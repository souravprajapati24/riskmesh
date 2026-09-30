package com.riskmesh.ingestion.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.riskmesh.ingestion.repository.IngestionDlqRepository;
import com.riskmesh.ingestion.repository.OutboxEventRepository;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;


@Testcontainers
@SpringBootTest
class OutboxPublisherConcurrencyTest {

    @Autowired
    private OutboxPublisher outboxPublisher;
    @MockBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>(DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"))
                    .withDatabaseName("ingestion_db_test2");

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>("redis:7-alpine")
                    .withExposedPorts(6379);

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        registry.add("spring.data.redis.password", () -> "");
        registry.add("riskmesh.security.hmac.shared-secret", () -> "test-secret");
    }

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private IngestionDlqRepository ingestionDlqRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private java.util.concurrent.ConcurrentLinkedQueue<String> publishedKeys;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        jdbcTemplate.update("DELETE FROM outbox_events");
        publishedKeys = new java.util.concurrent.ConcurrentLinkedQueue<>();
    }

    @Test
    void concurrentPollersNeverDoublePublishTheSameRow() throws InterruptedException {
        Clock clock = Clock.fixed(Instant.now(), ZoneOffset.UTC);
        int rowCount = 50;
        for (int i = 0; i < rowCount; i++) {
            outboxEventRepository.insert(
                    UUID.randomUUID(), "transaction.received", UUID.randomUUID().toString(),
                    "{\"index\":" + i + "}", Instant.now(clock));
        }

        buildRecordingFakeKafkaTemplate();

        OutboxPublisher pollerOne = outboxPublisher;
        OutboxPublisher pollerTwo = outboxPublisher;

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);
        executor.submit(() -> {
            pollerOne.pollOnce();
            latch.countDown();
        });
        executor.submit(() -> {
            pollerTwo.pollOnce();
            latch.countDown();
        });

        boolean completed = latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(completed).isTrue();
        assertThat(publishedKeys).hasSize(rowCount);
        assertThat(publishedKeys.stream().distinct().count()).isEqualTo((long) rowCount);

        Integer unpublishedCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL", Integer.class);
        assertThat(unpublishedCount).isEqualTo(0);
    }

    @SuppressWarnings("unchecked")
    private void buildRecordingFakeKafkaTemplate() {
        when(kafkaTemplate.send(
                any(String.class),
                any(String.class),
                any(String.class)))
                .thenAnswer(invocation -> {
                    String key = invocation.getArgument(1);
                    publishedKeys.add(key);

                    SendResult<String, String> sendResult =
                            mock(SendResult.class);

                    return CompletableFuture.completedFuture(sendResult);
                });
    }
}
