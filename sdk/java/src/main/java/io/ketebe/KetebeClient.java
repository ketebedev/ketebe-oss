package io.ketebe;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class KetebeClient {
    private final ClientConfig config;
    private final HttpClient http;
    private final ObjectMapper json;

    public KetebeClient(String baseUrl) { this(ClientConfig.defaults(baseUrl)); }
    public KetebeClient(ClientConfig config) {
        this(config, HttpClient.newBuilder().connectTimeout(config.timeout()).build(), new ObjectMapper());
    }
    KetebeClient(ClientConfig config, HttpClient http, ObjectMapper json) {
        this.config = Objects.requireNonNull(config); this.http = Objects.requireNonNull(http); this.json = Objects.requireNonNull(json);
    }

    public JsonNode listCollections() { return request("GET", "/v0/collections", null, true, false); }
    public JsonNode createCollection(JsonNode body) { return request("POST", "/v0/collections", body, false, false); }
    public JsonNode getCollection(String id) { return request("GET", "/v0/collections/" + segment(id), null, true, false); }
    public void deleteCollection(String id) { request("DELETE", "/v0/collections/" + segment(id), null, true, true); }

    public Mutation upsertRecord(String collection, RecordId id, RecordUpsert record) {
        return mutation(request("PUT", recordsPath(collection, id), record.toJson(), true, false));
    }
    public Mutation deleteRecord(String collection, RecordId id) {
        return mutation(request("DELETE", recordsPath(collection, id), null, true, false));
    }
    public JsonNode batchUpsertRecords(String collection, List<BatchRecordUpsert> records) {
        return request("POST", "/v0/collections/" + segment(collection) + "/records:batchUpsert", BatchRecordUpsert.batchJson(records), true, false);
    }
    public JsonNode upsertDocument(String collection, RecordId id, DocumentUpsert document) {
        return request("PUT", "/v0/collections/" + segment(collection) + "/documents/" + segment(id.pathValue()), document.toJson(), true, false);
    }
    public JsonNode deleteDocument(String collection, RecordId id) {
        return request("DELETE", "/v0/collections/" + segment(collection) + "/documents/" + segment(id.pathValue()), null, true, false);
    }

    public QueryResponse query(String collection, QueryRequest query) {
        JsonNode root = request("POST", "/v1/collections/" + segment(collection) + "/query", query.toJson(), true, false);
        List<QueryHit> hits = new ArrayList<>();
        JsonNode array = root.path("hits");
        if (array.isArray()) for (JsonNode hit : array) {
            BigInteger seq = requiredU64(hit, "sequence_number");
            hits.add(new QueryHit(RecordId.fromJson(hit.get("id")), hit.path("score").asDouble(), seq, hit.get("metadata"), hit));
        }
        return new QueryResponse(root.path("api_version").asText("v1"), hits, root.get("explain"), root);
    }

    public JsonNode getJob(String jobId) { return request("GET", "/v0/jobs/" + segment(jobId), null, true, false); }
    public JsonNode cancelJob(String jobId) { return request("POST", "/v0/jobs/" + segment(jobId) + "/cancel", null, false, false); }
    public JsonNode getEmbeddingMigration(String collection) { return request("GET", migrationPath(collection), null, true, false); }
    public JsonNode startEmbeddingMigration(String collection, String targetProfile) {
        ObjectNode body = JsonNodeFactory.instance.objectNode().put("target_profile", targetProfile);
        return request("POST", migrationPath(collection), body, false, false);
    }
    public JsonNode catchUpEmbeddingMigration(String collection) { return request("POST", migrationPath(collection) + "/catch-up", null, false, false); }
    public JsonNode startEmbeddingMigrationCatchUpJob(String collection) { return request("POST", migrationPath(collection) + "/catch-up-job", null, false, false); }
    public JsonNode activateEmbeddingMigration(String collection) { return request("POST", migrationPath(collection) + "/activate", null, false, false); }

    private String recordsPath(String collection, RecordId id) { return "/v0/collections/" + segment(collection) + "/records/" + segment(id.pathValue()); }
    private String migrationPath(String collection) { return "/v0/collections/" + segment(collection) + "/embedding-migration"; }

    private JsonNode request(String method, String path, JsonNode body, boolean idempotent, boolean allowEmpty) {
        int attempts = idempotent ? config.maxRetries() + 1 : 1;
        Throwable last = null;
        for (int attempt = 0; attempt < attempts; attempt++) {
            try {
                HttpRequest.Builder builder = HttpRequest.newBuilder(resolve(path)).timeout(config.timeout());
                if (body == null) builder.method(method, HttpRequest.BodyPublishers.noBody());
                else builder.header("content-type", "application/json").method(method, HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body)));
                HttpResponse<String> response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    if (response.body().isEmpty()) return allowEmpty ? null : JsonNodeFactory.instance.nullNode();
                    return json.readTree(response.body());
                }
                if (idempotent && attempt + 1 < attempts && (response.statusCode() == 429 || response.statusCode() >= 500)) {
                    sleepBackoff(); continue;
                }
                throw apiError(response.statusCode(), response.body());
            } catch (ApiException e) {
                throw e;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TransportException("Ketebe request interrupted", e);
            } catch (IOException | RuntimeException e) {
                if (e instanceof KetebeException ke) throw ke;
                last = e;
                if (idempotent && attempt + 1 < attempts) { sleepBackoff(); continue; }
                throw new TransportException("Ketebe request failed", e);
            }
        }
        throw new TransportException("Ketebe request exhausted retries", last);
    }

    private ApiException apiError(int status, String body) {
        try {
            JsonNode root = body == null || body.isEmpty() ? null : json.readTree(body);
            JsonNode error = root == null ? null : root.get("error");
            String code = error != null ? error.path("code").asText("http_error") : "http_error";
            String message = error != null ? error.path("message").asText("HTTP " + status) : "HTTP " + status;
            return new ApiException(status, code, message);
        } catch (JsonProcessingException e) {
            return new ApiException(status, "http_error", "HTTP " + status);
        }
    }

    private Mutation mutation(JsonNode node) { return new Mutation(requiredU64(node, "sequence_number")); }
    private static BigInteger requiredU64(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isIntegralNumber()) throw new TransportException("missing or invalid u64 field: " + field);
        BigInteger n = value.bigIntegerValue();
        if (n.signum() < 0 || n.compareTo(RecordId.U64_MAX) > 0) throw new TransportException("u64 field out of range: " + field);
        return n;
    }
    private URI resolve(String path) { return URI.create(config.baseUri().toString() + path); }
    private static String segment(String value) { return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20"); }
    private void sleepBackoff() {
        Duration delay = config.retryBackoff();
        if (delay.isZero()) return;
        try { Thread.sleep(delay.toMillis()); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new TransportException("Ketebe retry interrupted", e); }
    }
}
