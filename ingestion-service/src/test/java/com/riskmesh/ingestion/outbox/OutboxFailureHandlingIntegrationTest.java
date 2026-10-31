package com.riskmesh.ingestion.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.riskmesh.ingestion.repository.IngestionDlqRepository;
import com.riskmesh.ingestion.repository.OutboxEventRepository;
import com.riskmesh.ingestion.support.PostgresIntegrationTestBase;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.support.TransactionTemplate;

class OutboxFailureHandlingIntegrationTest extends PostgresIntegrationTestBase {

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private IngestionDlqRepository ingestionDlqRepository;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private OutboxEscalationJob escalationJob;

    @Autowired
    private MeterRegistry meterRegistry;

    private final Clock clock = Clock.systemUTC();

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);

    private OutboxPublisher publisher() {
        return new OutboxPublisher(outboxEventRepository, ingestionDlqRepository, kafka, clock);
    }

    private void poll(OutboxPublisher publisher) {
        transactionTemplate.executeWithoutResult(status -> publisher.pollOnce());
    }

    private int dlqRows(UUID txn) {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM ingestion_dlq WHERE transaction_id = ?",
                Integer.class, txn);
    }

    private int unpublished(UUID txn) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ? AND published_at IS NULL",
                Integer.class, txn);
    }

    @Test
    void repeatedBrokerFailuresProduceOneDlqRowWithIncrementingRetryCountThenResolveOnSuccess() {
        UUID txn = UUID.randomUUID();
        outboxEventRepository.insert(txn, "transaction.received", txn.toString(), "{\"n\":1}", Instant.now());
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(
                CompletableFuture.<SendResult<String, String>>failedFuture(new IllegalStateException("broker down")));
        OutboxPublisher publisher = publisher();

        poll(publisher);
        poll(publisher);
        poll(publisher);

        assertThat(dlqRows(txn)).isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject("SELECT retry_count FROM ingestion_dlq WHERE transaction_id = ?",
                Integer.class, txn)).isEqualTo(2);
        assertThat(unpublished(txn)).isEqualTo(1);

        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(
                CompletableFuture.completedFuture(mock(SendResult.class)));
        poll(publisher);

        assertThat(unpublished(txn)).isZero();
        assertThat(jdbcTemplate.queryForObject("SELECT resolved FROM ingestion_dlq WHERE transaction_id = ?",
                Boolean.class, txn)).isTrue();
        assertThat(dlqRows(txn)).isEqualTo(1);
    }

    @Test
    void infrastructureFailureStopsTheBatchAtTheFirstFailedRow() {
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        Instant base = Instant.now().minusSeconds(60);
        outboxEventRepository.insert(first, "transaction.received", first.toString(), "{\"n\":1}", base);
        outboxEventRepository.insert(second, "transaction.received", second.toString(), "{\"n\":2}", base.plusSeconds(1));
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(
                CompletableFuture.<SendResult<String, String>>failedFuture(new IllegalStateException("broker down")));

        poll(publisher());

        assertThat(dlqRows(first)).isEqualTo(1);
        assertThat(dlqRows(second)).isZero();
    }

    @Test
    void perRecordFailureSkipsOnlyThatRowAndLaterRowsStillPublish() {
        UUID poison = UUID.randomUUID();
        UUID healthy = UUID.randomUUID();
        Instant base = Instant.now().minusSeconds(60);
        outboxEventRepository.insert(poison, "transaction.received", poison.toString(), "{\"n\":1}", base);
        outboxEventRepository.insert(healthy, "transaction.received", healthy.toString(), "{\"n\":2}", base.plusSeconds(1));
        when(kafka.send(anyString(), eq(poison.toString()), anyString())).thenReturn(
                CompletableFuture.<SendResult<String, String>>failedFuture(new RecordTooLargeException("too large")));
        when(kafka.send(anyString(), eq(healthy.toString()), anyString())).thenReturn(
                CompletableFuture.completedFuture(mock(SendResult.class)));

        poll(publisher());

        assertThat(unpublished(poison)).isEqualTo(1);
        assertThat(dlqRows(poison)).isEqualTo(1);
        assertThat(unpublished(healthy)).isZero();
        assertThat(dlqRows(healthy)).isZero();
    }

    @Test
    void escalationJobAlertsOnlyForUnresolvedRowsOlderThanTheEscalationWindow() {
        UUID stale = UUID.randomUUID();
        UUID fresh = UUID.randomUUID();
        ingestionDlqRepository.insert(stale, "{\"a\":1}", "old failure", Instant.now().minus(Duration.ofHours(2)));
        ingestionDlqRepository.insert(fresh, "{\"a\":2}", "new failure", Instant.now());
        double before = meterRegistry.counter("riskmesh.ingestion.outbox.escalations.total").count();

        escalationJob.escalateStaleFailures();

        double after = meterRegistry.counter("riskmesh.ingestion.outbox.escalations.total").count();
        assertThat(after - before).isEqualTo(1.0);
    }
}
