package io.ketebe;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class OpenApiContractTest {
    @Test void public_openapi_contains_java_sdk_surface() throws Exception {
        Path specPath = Path.of("..", "..", "api", "openapi", "v1.json").normalize();
        assertTrue(Files.exists(specPath), "missing public OpenAPI v1 contract");
        JsonNode spec = new ObjectMapper().readTree(Files.readString(specPath));
        List<String[]> operations = List.of(
            new String[]{"get", "/v0/collections"}, new String[]{"post", "/v0/collections"},
            new String[]{"put", "/v0/collections/{collection_id}/records/{record_id}"},
            new String[]{"post", "/v0/collections/{collection_id}/records:batchUpsert"},
            new String[]{"put", "/v0/collections/{collection_id}/documents/{record_id}"},
            new String[]{"post", "/v1/collections/{collection_id}/query"},
            new String[]{"get", "/v0/jobs/{job_id}"}, new String[]{"post", "/v0/jobs/{job_id}/cancel"},
            new String[]{"get", "/v0/collections/{collection_id}/embedding-migration"},
            new String[]{"post", "/v0/collections/{collection_id}/embedding-migration"},
            new String[]{"post", "/v0/collections/{collection_id}/embedding-migration/catch-up"},
            new String[]{"post", "/v0/collections/{collection_id}/embedding-migration/catch-up-job"},
            new String[]{"post", "/v0/collections/{collection_id}/embedding-migration/activate"}
        );
        for (String[] op : operations) assertTrue(spec.path("paths").path(op[1]).path(op[0]).isObject(), "missing " + op[0] + " " + op[1]);
        QueryRequest query = new QueryRequest().vector(List.of(1.0, 0.0)).text("vector database").topK(5).searchProfile("balanced").explain(true);
        assertEquals("balanced", query.toJson().path("search_profile").asText());
    }
}
