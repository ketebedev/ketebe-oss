package io.ketebe;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RetryTest {
    @Test void retries_only_idempotent_operations() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicInteger gets = new AtomicInteger();
        AtomicInteger posts = new AtomicInteger();
        server.createContext("/v0/collections", exchange -> {
            int count = exchange.getRequestMethod().equals("GET") ? gets.incrementAndGet() : posts.incrementAndGet();
            byte[] body = (count == 1 ? "{\"error\":{\"code\":\"temporary\",\"message\":\"retry\"}}" : "{\"collections\":[]}").getBytes();
            int status = count == 1 ? 500 : 200;
            exchange.sendResponseHeaders(status, body.length); exchange.getResponseBody().write(body); exchange.close();
        });
        server.start();
        try {
            ClientConfig cfg = ClientConfig.defaults("http://127.0.0.1:" + server.getAddress().getPort()).withMaxRetries(2).withRetryBackoff(Duration.ZERO);
            KetebeClient client = new KetebeClient(cfg);
            assertTrue(client.listCollections().path("collections").isArray());
            assertEquals(2, gets.get());
            assertThrows(ApiException.class, () -> client.createCollection(new ObjectMapper().createObjectNode().put("id", "x")));
            assertEquals(1, posts.get());
        } finally { server.stop(0); }
    }
}
