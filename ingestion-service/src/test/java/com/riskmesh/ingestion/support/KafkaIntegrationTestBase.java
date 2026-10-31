package com.riskmesh.ingestion.support;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class KafkaIntegrationTestBase {

    protected static final String SHARED_SECRET = "riskmesh-dev-shared-secret";

    protected static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(
            DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("ingestion_db_phase4_kafka");
    protected static final GenericContainer<?> REDIS =
            new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);
    protected static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));
    protected static final StubGatewayServer GATEWAY = new StubGatewayServer();

    static {
        POSTGRES.start();
        REDIS.start();
        KAFKA.start();
        GATEWAY.start();
    }

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("spring.data.redis.password", () -> "");
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("riskmesh.kafka.enabled", () -> "true");
        registry.add("riskmesh.kafka.topics.replication-factor", () -> "1");
        registry.add("riskmesh.kafka.topics.min-insync-replicas", () -> "1");
        registry.add("riskmesh.gateway.base-url", GATEWAY::baseUrl);
        registry.add("riskmesh.security.hmac.shared-secret", () -> SHARED_SECRET);
    }

    @LocalServerPort
    protected int port;

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    protected final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    protected void resetState() {
        RestAssured.port = port;
        GATEWAY.reset();
        jdbcTemplate.update("DELETE FROM outbox_events");
        jdbcTemplate.update("DELETE FROM ingestion_dlq");
        jdbcTemplate.update("DELETE FROM saga_states");
        jdbcTemplate.update("DELETE FROM transactions");
        jdbcTemplate.update("DELETE FROM deduplication_records");
    }

    protected UUID submitTransaction(String externalTxnId) throws Exception {
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
        return UUID.fromString(transactionId);
    }

    private String buildRequestBody(String externalTxnId, UUID merchantId) throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("externalTxnId", externalTxnId);
        payload.put("merchantId", merchantId.toString());
        payload.put("payerId", UUID.randomUUID().toString());
        payload.put("payeeId", UUID.randomUUID().toString());
        payload.put("amount", "1200.00");
        payload.put("currency", "INR");
        payload.put("paymentMethod", "UPI");
        return objectMapper.writeValueAsString(payload);
    }

    private String sign(String body) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(SHARED_SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return HexFormat.of().formatHex(mac.doFinal(body.getBytes(StandardCharsets.UTF_8)));
    }

    protected String currentSagaStep(UUID transactionId) {
        List<String> steps = jdbcTemplate.queryForList(
                "SELECT current_step FROM saga_states WHERE transaction_id = ?", String.class, transactionId);
        return steps.isEmpty() ? null : steps.get(0);
    }

    protected void awaitSagaStep(UUID transactionId, String expectedStep) {
        await().atMost(Duration.ofSeconds(45)).pollInterval(Duration.ofMillis(200))
                .until(() -> expectedStep.equals(currentSagaStep(transactionId)));
    }

    protected List<String> outboxTopics(UUID transactionId) {
        return jdbcTemplate.queryForList(
                "SELECT topic FROM outbox_events WHERE aggregate_id = ? ORDER BY id", String.class, transactionId);
    }

    protected int count(String sql, Object... args) {
        Integer value = jdbcTemplate.queryForObject(sql, Integer.class, args);
        return value == null ? 0 : value;
    }

    protected List<ConsumerRecord<String, String>> awaitKafkaRecords(String topic, String key, int expected,
                                                                     Duration timeout) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "verify-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> matches = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            while (System.nanoTime() < deadline && matches.size() < expected) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    if (key.equals(record.key())) {
                        matches.add(record);
                    }
                }
            }
        }
        return matches;
    }

    protected void sendRaw(String topic, String key, String value) throws Exception {
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, key, value)).get(10, TimeUnit.SECONDS);
        }
    }
}
