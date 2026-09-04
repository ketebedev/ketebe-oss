package io.ketebe;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Objects;

public record DocumentUpsert(String text, JsonNode metadata, JsonNode source, JsonNode chunking) {
    public DocumentUpsert { Objects.requireNonNull(text, "text"); }
    public ObjectNode toJson() {
        ObjectNode n = JsonNodeFactory.instance.objectNode(); n.put("text", text);
        if (metadata != null) n.set("metadata", metadata);
        if (source != null) n.set("source", source);
        if (chunking != null) n.set("chunking", chunking);
        return n;
    }
}
