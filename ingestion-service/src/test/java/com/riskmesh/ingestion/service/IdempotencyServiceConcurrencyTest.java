package com.riskmesh.ingestion.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;


@Testcontainers
@SpringBootTest
class IdempotencyServiceConcurrencyTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>(DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"))
                    .withDatabaseName("ingestion_db_test");

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
        registry.add("riskmesh.security.hmac.shared-secret",
                () -> "test-secret");
    }

    @Autowired
    private IdempotencyService idempotencyService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Test
    void concurrentReservation_onlyOneWins() throws InterruptedException {
        String idempotencyKey = "concurrency-test-key-" + UUID.randomUUID();
        int concurrentCallers = 10;

        ExecutorService executor = Executors.newFixedThreadPool(concurrentCallers);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(concurrentCallers);
        AtomicInteger reservedCount = new AtomicInteger(0);
        List<UUID> winningTransactionIds = new java.util.concurrent.CopyOnWriteArrayList<>();

        for (int i = 0; i < concurrentCallers; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    IdempotencyResult result = idempotencyService.checkAndReserve(idempotencyKey, UUID.randomUUID());
                    if (result.isNew()) {
                        reservedCount.incrementAndGet();
                    }
                    winningTransactionIds.add(result.transactionId());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(completed).isTrue();
        assertThat(reservedCount.get()).isEqualTo(1);
        assertThat(winningTransactionIds.stream().distinct().count()).isEqualTo(1);

        Integer rowCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM deduplication_records WHERE idempotency_key = ?",
                Integer.class, idempotencyKey);
        assertThat(rowCount).isEqualTo(1);
    }
}
