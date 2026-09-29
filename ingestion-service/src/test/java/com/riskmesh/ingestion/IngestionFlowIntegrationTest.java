package com.riskmesh.ingestion;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import io.restassured.RestAssured;
import static io.restassured.RestAssured.given;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end integration test of the full ingestion pipeline: HMAC verification, rate limiting,
 * idempotency, persistence, and outbox writes - via a real embedded HTTP server and real
 * Testcontainers Postgres/Redis (RiskMesh_PRD.md §17.2's integration test requirement). Matches
 * the Phase 3 exit criteria in RiskMesh_Master_Implementation_Plan.md.
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class IngestionFlowIntegrationTest {

    private static final String SHARED_SECRET = "riskmesh-dev-shared-secret-change-me";

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>(DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"))
                    .withDatabaseName("ingestion_db_test3");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        registry.add("spring.data.redis.password", () -> "");
        registry.add("riskmesh.security.hmac.shared-secret", () -> SHARED_SECRET);
        // Kafka is not started in this test; OutboxPublisher's scheduled trigger is inert in
        // Phase 3 (see IngestionServiceApplication's Javadoc), so no bootstrap-servers override
        // is needed for the pipeline under test to complete successfully.
    }

    @LocalServerPort
    private int port;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @org.springframework.beans.factory.annotation.Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        RestAssured.port = port;
        jdbcTemplate.update("DELETE FROM outbox_events");
        jdbcTemplate.update("DELETE FROM saga_states");
        jdbcTemplate.update("DELETE FROM transactions");
        jdbcTemplate.update("DELETE FROM deduplication_records");
    }

    @Test
    void validSignedTransactionIsAcceptedAndPersisted() throws Exception {
        String externalTxnId = "e2e-" + UUID.randomUUID();
        UUID merchantId = UUID.randomUUID();
        String body = buildRequestBody(externalTxnId, merchantId);

        given()
                .contentType("application/json")
                .header("X-Merchant-Id", merchantId.toString())
                .header("X-Gateway-Signature", "sha256=" + sign(body))
                .body(body)
                .when()
                .post("/api/v1/transactions")
                .then()
                .statusCode(202)
                .body("status", equalTo("RECEIVED"))
                .body("transactionId", notNullValue());

        Integer txnCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM transactions WHERE external_txn_id = ?", Integer.class, externalTxnId);
        assertThatEquals(1, txnCount);

        Integer sagaCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM saga_states ss JOIN transactions t ON ss.transaction_id = t.transaction_id "
                        + "WHERE t.external_txn_id = ? AND ss.current_step = 'INITIATED'", Integer.class, externalTxnId);
        assertThatEquals(1, sagaCount);

        Integer outboxCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events oe "
                        + "JOIN transactions t ON oe.aggregate_id = t.transaction_id "
                        + "WHERE t.external_txn_id = ? AND oe.published_at IS NULL", Integer.class, externalTxnId);
        assertThatEquals(2, outboxCount);
    }

    @Test
    void duplicateSubmissionReturns200WithoutCreatingSecondRow() throws Exception {
        String externalTxnId = "dup-" + UUID.randomUUID();
        UUID merchantId = UUID.randomUUID();
        String body = buildRequestBody(externalTxnId, merchantId);
        String signature = "sha256=" + sign(body);

        given().contentType("application/json").header("X-Merchant-Id", merchantId.toString())
                .header("X-Gateway-Signature", signature).body(body)
                .post("/api/v1/transactions").then().statusCode(202);

        given().contentType("application/json").header("X-Merchant-Id", merchantId.toString())
                .header("X-Gateway-Signature", signature).body(body)
                .post("/api/v1/transactions")
                .then()
                .statusCode(200)
                .body("status", equalTo("ALREADY_PROCESSED"));

        Integer txnCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM transactions WHERE external_txn_id = ?", Integer.class, externalTxnId);
        assertThatEquals(1, txnCount);
    }

    @Test
    void invalidSignatureIsRejectedWithNoSideEffects() throws Exception {
        String externalTxnId = "bad-sig-" + UUID.randomUUID();
        UUID merchantId = UUID.randomUUID();
        String body = buildRequestBody(externalTxnId, merchantId);

        given()
                .contentType("application/json")
                .header("X-Merchant-Id", merchantId.toString())
                .header("X-Gateway-Signature", "sha256=" + "0".repeat(64))
                .body(body)
                .when()
                .post("/api/v1/transactions")
                .then()
                .statusCode(401);

        Integer txnCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM transactions WHERE external_txn_id = ?", Integer.class, externalTxnId);
        assertThatEquals(0, txnCount);
    }

    @Test
    void statusEndpointReturnsReceivedStatus() throws Exception {
        String externalTxnId = "status-" + UUID.randomUUID();
        UUID merchantId = UUID.randomUUID();
        String body = buildRequestBody(externalTxnId, merchantId);

        String transactionId = given()
                .contentType("application/json")
                .header("X-Merchant-Id", merchantId.toString())
                .header("X-Gateway-Signature", "sha256=" + sign(body))
                .body(body)
                .when()
                .post("/api/v1/transactions")
                .then()
                .statusCode(202)
                .extract().path("transactionId");

        given()
                .when()
                .get("/api/v1/transactions/{id}/status", transactionId)
                .then()
                .statusCode(200)
                .body("status", equalTo("RECEIVED"));
    }

    private void assertThatEquals(int expected, Integer actual) {
        org.assertj.core.api.Assertions.assertThat(actual).isEqualTo(expected);
    }

    private String buildRequestBody(String externalTxnId, UUID merchantId) throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("externalTxnId", externalTxnId);
        payload.put("merchantId", merchantId.toString());
        payload.put("payerId", UUID.randomUUID().toString());
        payload.put("payeeId", UUID.randomUUID().toString());
        payload.put("amount", "1200.00");
        payload.put("currency", "INR");
        payload.put("paymentMethod", "CARD");
        payload.put("cardBin", "411111");
        payload.put("cardLast4", "4242");
        payload.put("deviceFingerprint", "sha256:testfingerprint");
        payload.put("ipAddress", "203.0.113.42");
        payload.put("geoCountry", "IN");
        payload.put("geoCity", "Mumbai");
        payload.put("merchantCategory", "5411");
        payload.put("userAgent", "IntegrationTest/1.0");
        return objectMapper.writeValueAsString(payload);
    }

    private String sign(String body) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(SHARED_SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return HexFormat.of().formatHex(mac.doFinal(body.getBytes(StandardCharsets.UTF_8)));
    }
}
