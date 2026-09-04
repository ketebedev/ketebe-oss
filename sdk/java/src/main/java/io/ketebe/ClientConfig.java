package io.ketebe;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;

public record ClientConfig(URI baseUri, Duration timeout, int maxRetries, Duration retryBackoff) {
    public ClientConfig {
        Objects.requireNonNull(baseUri, "baseUri");
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(retryBackoff, "retryBackoff");
        if (!baseUri.isAbsolute()) throw new IllegalArgumentException("baseUri must be absolute");
        if (timeout.isZero() || timeout.isNegative()) throw new IllegalArgumentException("timeout must be positive");
        if (maxRetries < 0) throw new IllegalArgumentException("maxRetries must be >= 0");
        if (retryBackoff.isNegative()) throw new IllegalArgumentException("retryBackoff must be >= 0");
    }

    public static ClientConfig defaults(String baseUrl) {
        String normalized = Objects.requireNonNull(baseUrl, "baseUrl").replaceAll("/+$", "");
        return new ClientConfig(URI.create(normalized), Duration.ofSeconds(10), 2, Duration.ofMillis(50));
    }

    public ClientConfig withTimeout(Duration value) { return new ClientConfig(baseUri, value, maxRetries, retryBackoff); }
    public ClientConfig withMaxRetries(int value) { return new ClientConfig(baseUri, timeout, value, retryBackoff); }
    public ClientConfig withRetryBackoff(Duration value) { return new ClientConfig(baseUri, timeout, maxRetries, value); }
}
