package io.ketebe;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
public record QueryResponse(String apiVersion, List<QueryHit> hits, JsonNode explain, JsonNode raw) {
    public QueryResponse { hits = List.copyOf(hits); }
}
