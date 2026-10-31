package com.riskmesh.ingestion.support;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public final class StubGatewayServer {

    private HttpServer server;
    private final AtomicInteger holdRequests = new AtomicInteger();
    private volatile int responseStatus = 200;

    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            server.createContext("/api/v1/holds", this::handle);
            server.setExecutor(Executors.newCachedThreadPool());
            server.start();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to start stub gateway", e);
        }
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    public String baseUrl() {
        return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort();
    }

    public void respondWith(int status) {
        this.responseStatus = status;
    }

    public int holdRequestCount() {
        return holdRequests.get();
    }

    public void reset() {
        holdRequests.set(0);
        responseStatus = 200;
    }

    private void handle(HttpExchange exchange) throws IOException {
        exchange.getRequestBody().readAllBytes();
        int count = holdRequests.incrementAndGet();
        int status = responseStatus;
        String body = switch (status) {
            case 200 -> "{\"holdReference\":\"hold-" + count + "\"}";
            case 422 -> "{\"reason\":\"Insufficient funds\"}";
            default -> "{\"error\":\"stub failure\"}";
        };
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
