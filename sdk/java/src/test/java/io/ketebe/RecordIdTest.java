package io.ketebe;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RecordIdTest {
    private final ObjectMapper json = new ObjectMapper();

    @Test void typed_ids_are_distinct_and_full_u64_is_lossless() throws Exception {
        RecordId string = RecordId.string("42");
        RecordId numeric = RecordId.unsigned(BigInteger.valueOf(42));
        assertNotEquals(string, numeric);
        assertEquals(string, RecordId.fromJson(json.readTree(string.toJson().toString())));
        assertEquals(numeric, RecordId.fromJson(json.readTree(numeric.toJson().toString())));
        RecordId max = RecordId.unsigned(RecordId.U64_MAX);
        assertEquals(RecordId.U64_MAX, ((RecordId.UnsignedId) RecordId.fromJson(max.toJson())).value());
        assertThrows(IllegalArgumentException.class, () -> RecordId.unsigned(RecordId.U64_MAX.add(BigInteger.ONE)));
    }
}
