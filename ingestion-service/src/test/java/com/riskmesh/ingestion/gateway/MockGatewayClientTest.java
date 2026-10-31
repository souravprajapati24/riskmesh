package com.riskmesh.ingestion.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.riskmesh.ingestion.config.GatewayProperties;
import com.riskmesh.ingestion.gateway.exception.GatewayRejectedException;
import com.riskmesh.ingestion.gateway.exception.GatewayUnavailableException;
import com.riskmesh.ingestion.support.StubGatewayServer;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MockGatewayClientTest {

    private final StubGatewayServer stub = new StubGatewayServer();
    private MockGatewayClient client;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        stub.start();
        client = new MockGatewayClient(propertiesFor(stub.baseUrl()), objectMapper );
    }

    @AfterEach
    void tearDown() {
        stub.stop();
    }

    @Test
    void acceptedHoldReturnsHoldReference() {
        stub.respondWith(200);

        HoldResult result = client.placeHold(UUID.randomUUID(), new BigDecimal("1200.00"), "INR");

        assertThat(result.holdReference()).isEqualTo("hold-1");
        assertThat(stub.holdRequestCount()).isEqualTo(1);
    }

    @Test
    void status422IsARejection() {
        stub.respondWith(422);

        assertThatThrownBy(() -> client.placeHold(UUID.randomUUID(), new BigDecimal("10.00"), "INR"))
                .isInstanceOf(GatewayRejectedException.class)
                .hasMessageContaining("HTTP 422");
    }

    @Test
    void status500IsATechnicalFailureNotARejection() {
        stub.respondWith(500);

        assertThatThrownBy(() -> client.placeHold(UUID.randomUUID(), new BigDecimal("10.00"), "INR"))
                .isInstanceOf(GatewayUnavailableException.class)
                .hasMessageContaining("500");
    }

    @Test
    void otherClientErrorsAreTechnicalFailuresNotRejections() {
        stub.respondWith(400);

        assertThatThrownBy(() -> client.placeHold(UUID.randomUUID(), new BigDecimal("10.00"), "INR"))
                .isInstanceOf(GatewayUnavailableException.class);
    }

    @Test
    void unreachableGatewayIsATechnicalFailure() {
        MockGatewayClient unreachable = new MockGatewayClient(propertiesFor("http://127.0.0.1:1") ,objectMapper);

        assertThatThrownBy(() -> unreachable.placeHold(UUID.randomUUID(), new BigDecimal("10.00"), "INR"))
                .isInstanceOf(GatewayUnavailableException.class);
    }

    private GatewayProperties propertiesFor(String baseUrl) {
        GatewayProperties properties = new GatewayProperties();
        properties.setBaseUrl(baseUrl);
        properties.setHoldPath("/api/v1/holds");
        properties.setConnectTimeout(Duration.ofSeconds(1));
        properties.setReadTimeout(Duration.ofSeconds(2));
        return properties;
    }
}
