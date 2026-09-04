package io.ketebe;
import java.math.BigInteger;
public record Mutation(BigInteger sequenceNumber) {
    public Mutation {
        if (sequenceNumber == null || sequenceNumber.signum() < 0 || sequenceNumber.compareTo(RecordId.U64_MAX) > 0)
            throw new IllegalArgumentException("sequenceNumber must be a u64");
    }
}
