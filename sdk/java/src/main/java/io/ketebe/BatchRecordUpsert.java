package io.ketebe;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Objects;

public record BatchRecordUpsert(RecordId id, RecordUpsert record) {
    public BatchRecordUpsert { Objects.requireNonNull(id, "id"); Objects.requireNonNull(record, "record"); }
    ObjectNode toJson() {
        ObjectNode n = record.toJson(); n.set("id", id.toJson()); return n;
    }

    public static ObjectNode batchJson(List<BatchRecordUpsert> records) {
        Objects.requireNonNull(records, "records");
        ObjectNode root = JsonNodeFactory.instance.objectNode();
        ArrayNode array = root.putArray("records");
        records.forEach(value -> array.add(value.toJson()));
        return root;
    }
}
