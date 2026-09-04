package io.ketebe;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigInteger;
import java.util.Objects;

public sealed interface RecordId permits RecordId.StringId, RecordId.UnsignedId {
    BigInteger U64_MAX = new BigInteger("18446744073709551615");

    static RecordId string(String value) { return new StringId(value); }
    static RecordId unsigned(long value) {
        if (value < 0) throw new IllegalArgumentException("unsigned RecordId cannot be negative");
        return new UnsignedId(BigInteger.valueOf(value));
    }
    static RecordId unsigned(BigInteger value) { return new UnsignedId(value); }

    JsonNode toJson();
    String pathValue();

    static RecordId fromJson(JsonNode node) {
        if (node == null || !node.isObject()) throw new IllegalArgumentException("RecordId must be an object");
        String type = requiredText(node, "type");
        JsonNode value = node.get("value");
        if ("string".equals(type)) {
            if (value == null || !value.isTextual()) throw new IllegalArgumentException("string RecordId value must be text");
            return string(value.textValue());
        }
        if ("u64".equals(type)) {
            if (value == null || !value.isIntegralNumber()) throw new IllegalArgumentException("u64 RecordId value must be an integer");
            return unsigned(value.bigIntegerValue());
        }
        throw new IllegalArgumentException("unsupported RecordId type: " + type);
    }

    private static String requiredText(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual()) throw new IllegalArgumentException(field + " must be text");
        return value.textValue();
    }

    record StringId(String value) implements RecordId {
        public StringId { Objects.requireNonNull(value, "value"); if (value.isEmpty()) throw new IllegalArgumentException("RecordId string must not be empty"); }
        public JsonNode toJson() { ObjectNode n = JsonNodeFactory.instance.objectNode(); n.put("type", "string"); n.put("value", value); return n; }
        public String pathValue() { return value; }
    }

    record UnsignedId(BigInteger value) implements RecordId {
        public UnsignedId {
            Objects.requireNonNull(value, "value");
            if (value.signum() < 0 || value.compareTo(U64_MAX) > 0) throw new IllegalArgumentException("u64 RecordId must be in 0..2^64-1");
        }
        public JsonNode toJson() { ObjectNode n = JsonNodeFactory.instance.objectNode(); n.put("type", "u64"); n.put("value", value); return n; }
        public String pathValue() { return value.toString(); }
    }
}
