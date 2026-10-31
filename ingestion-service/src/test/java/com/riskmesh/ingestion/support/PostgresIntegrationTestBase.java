package com.riskmesh.ingestion.support;

import com.riskmesh.ingestion.domain.Transaction;
import com.riskmesh.ingestion.domain.enums.SagaStep;
import com.riskmesh.ingestion.repository.TransactionRepository;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
public abstract class PostgresIntegrationTestBase {

    protected static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(
            DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("ingestion_db_pg");

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    protected TransactionRepository transactionRepository;

    @BeforeEach
    protected void cleanTables() {
        jdbcTemplate.update("DELETE FROM outbox_events");
        jdbcTemplate.update("DELETE FROM ingestion_dlq");
        jdbcTemplate.update("DELETE FROM saga_states");
        jdbcTemplate.update("DELETE FROM transactions");
        jdbcTemplate.update("DELETE FROM deduplication_records");
    }

    protected UUID insertTransactionWithSaga(SagaStep step, Instant updatedAt) {
        UUID transactionId = UUID.randomUUID();
        transactionRepository.insert(new Transaction(
                transactionId, "ext-" + transactionId, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("10.00"), "INR", "UPI",
                null, null, null, null, null, null, null, null,
                "RECEIVED", Instant.now()));
        jdbcTemplate.update("""
                INSERT INTO saga_states (transaction_id, current_step, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                """, transactionId, step.name(), Timestamp.from(updatedAt), Timestamp.from(updatedAt));
        return transactionId;
    }

    protected String sagaStep(UUID transactionId) {
        return jdbcTemplate.queryForObject(
                "SELECT current_step FROM saga_states WHERE transaction_id = ?", String.class, transactionId);
    }

    protected int outboxCount(UUID transactionId, String topic) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ? AND topic = ?",
                Integer.class, transactionId, topic);
        return count == null ? 0 : count;
    }
}
