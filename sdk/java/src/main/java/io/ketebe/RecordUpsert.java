package io.ketebe;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Objects;

public record RecordUpsert(List<Double> vector, JsonNode metadata) {
    public RecordUpsert {
        Objects.requireNonNull(vector, "vector");
        vector = List.copyOf(vector);
    }
    public ObjectNode toJson() {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        ArrayNode values = node.putArray("vector");
        vector.forEach(values::add);
        if (metadata != null) node.set("metadata", metadata);
        return node;
    }
}
