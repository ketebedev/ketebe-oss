package io.ketebe;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigInteger;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import static org.junit.jupiter.api.Assertions.*;

class RealServerTest {
    @Test
    @EnabledIfEnvironmentVariable(named = "KETEBE_BASE_URL", matches = ".+")
    void typed_ids_and_query_round_trip_against_real_server() {
        String base = System.getenv("KETEBE_BASE_URL");
        KetebeClient client = new KetebeClient(base);
        String collection = "java-sdk-" + System.nanoTime();
        ObjectNode create = new ObjectMapper().createObjectNode();
        create.put("id", collection); create.put("dimension", 2); create.put("metric", "dot");
        client.createCollection(create);
        try {
            client.batchUpsertRecords(collection, List.of(
                new BatchRecordUpsert(RecordId.string("42"), new RecordUpsert(List.of(1.0, 0.0), null)),
                new BatchRecordUpsert(RecordId.unsigned(BigInteger.valueOf(42)), new RecordUpsert(List.of(1.0, 0.0), null))
            ));
            QueryResponse response = client.query(collection, new QueryRequest().vector(List.of(1.0, 0.0)).topK(2).execution("exact"));
            assertEquals(2, response.hits().size());
            assertTrue(response.hits().stream().anyMatch(hit -> hit.id().equals(RecordId.string("42"))));
            assertTrue(response.hits().stream().anyMatch(hit -> hit.id().equals(RecordId.unsigned(BigInteger.valueOf(42)))));
            assertTrue(response.hits().stream().allMatch(hit -> hit.sequenceNumber().signum() >= 0));
        } finally { client.deleteCollection(collection); }
    }
}
