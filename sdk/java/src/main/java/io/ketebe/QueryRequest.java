package io.ketebe;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;

public final class QueryRequest {
    private List<Double> vector;
    private String text;
    private Integer topK;
    private JsonNode predicate;
    private String execution;
    private Integer denseCandidates;
    private Integer lexicalCandidates;
    private String searchProfile;
    private Long timeoutMs;
    private Boolean explain;

    public QueryRequest vector(List<Double> value) { vector = value == null ? null : List.copyOf(value); return this; }
    public QueryRequest text(String value) { text = value; return this; }
    public QueryRequest topK(int value) { topK = value; return this; }
    public QueryRequest predicate(JsonNode value) { predicate = value; return this; }
    public QueryRequest execution(String value) { execution = value; return this; }
    public QueryRequest denseCandidates(int value) { denseCandidates = value; return this; }
    public QueryRequest lexicalCandidates(int value) { lexicalCandidates = value; return this; }
    public QueryRequest searchProfile(String value) { searchProfile = value; return this; }
    public QueryRequest timeoutMs(long value) { timeoutMs = value; return this; }
    public QueryRequest explain(boolean value) { explain = value; return this; }

    public ObjectNode toJson() {
        ObjectNode n = JsonNodeFactory.instance.objectNode();
        if (vector != null) { ArrayNode a = n.putArray("vector"); vector.forEach(a::add); }
        if (text != null) n.put("text", text);
        if (topK != null) n.put("top_k", topK);
        if (predicate != null) n.set("predicate", predicate);
        if (execution != null) n.put("execution", execution);
        if (denseCandidates != null) n.put("dense_candidates", denseCandidates);
        if (lexicalCandidates != null) n.put("lexical_candidates", lexicalCandidates);
        if (searchProfile != null) n.put("search_profile", searchProfile);
        if (timeoutMs != null) n.put("timeout_ms", timeoutMs);
        if (explain != null) n.put("explain", explain);
        return n;
    }
}
