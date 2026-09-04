package io.ketebe;
import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigInteger;
public record QueryHit(RecordId id, double score, BigInteger sequenceNumber, JsonNode metadata, JsonNode raw) {}
