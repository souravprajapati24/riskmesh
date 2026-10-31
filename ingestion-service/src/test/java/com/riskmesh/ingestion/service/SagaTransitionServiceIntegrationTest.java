package com.riskmesh.ingestion.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.riskmesh.common.events.FundHoldRequestedEvent;
import com.riskmesh.common.events.KafkaTopics;
import com.riskmesh.ingestion.domain.enums.SagaStep;
import com.riskmesh.ingestion.support.PostgresIntegrationTestBase;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class SagaTransitionServiceIntegrationTest extends PostgresIntegrationTestBase {

    @Autowired
    private SagaTransitionService service;

    @Test
    void beginHoldMovesInitiatedToHoldRequestedAndIsRepeatable() {
        UUID txn = insertTransactionWithSaga(SagaStep.INITIATED, Instant.now());

        assertThat(service.beginHold(txn)).isTrue();
        assertThat(sagaStep(txn)).isEqualTo("HOLD_REQUESTED");
        assertThat(service.beginHold(txn)).isTrue();
        assertThat(sagaStep(txn)).isEqualTo("HOLD_REQUESTED");
    }

    @Test
    void beginHoldIgnoresRedeliveryOnceTheSagaHasProgressed() {
        UUID txn = insertTransactionWithSaga(SagaStep.HOLD_CONFIRMED, Instant.now());

        assertThat(service.beginHold(txn)).isFalse();
        assertThat(sagaStep(txn)).isEqualTo("HOLD_CONFIRMED");
    }

    @Test
    void beginHoldFailsForUnknownTransaction() {
        assertThatThrownBy(() -> service.beginHold(UUID.randomUUID()))
                .isInstanceOf(SagaNotFoundException.class);
    }

    @Test
    void recordHoldConfirmedTransitionsStoresHoldAndEnqueuesEventOnlyOnce() {
        UUID txn = insertTransactionWithSaga(SagaStep.HOLD_REQUESTED, Instant.now());
        FundHoldRequestedEvent event = event(txn);

        assertThat(service.recordHoldConfirmed(event, "hold-42")).isTrue();
        assertThat(service.recordHoldConfirmed(event, "hold-42")).isFalse();

        assertThat(sagaStep(txn)).isEqualTo("HOLD_CONFIRMED");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT hold_reference FROM saga_states WHERE transaction_id = ?", String.class, txn))
                .isEqualTo("hold-42");
        assertThat(outboxCount(txn, KafkaTopics.FUND_HOLD_CONFIRMED)).isEqualTo(1);
    }

    @Test
    void recordHoldRejectedMovesToCompensatingWithReasonAndEnqueuesFailedEvent() {
        UUID txn = insertTransactionWithSaga(SagaStep.HOLD_REQUESTED, Instant.now());

        assertThat(service.recordHoldRejected(event(txn), "Gateway rejected the hold (HTTP 422)")).isTrue();

        assertThat(sagaStep(txn)).isEqualTo("COMPENSATING");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT failure_reason FROM saga_states WHERE transaction_id = ?", String.class, txn))
                .contains("HTTP 422");
        assertThat(outboxCount(txn, KafkaTopics.FUND_HOLD_FAILED)).isEqualTo(1);
        assertThat(outboxCount(txn, KafkaTopics.FUND_HOLD_CONFIRMED)).isZero();
    }

    @Test
    void concurrentConfirmationsProduceExactlyOneTransitionAndOneOutboxRow() throws Exception {
        UUID txn = insertTransactionWithSaga(SagaStep.HOLD_REQUESTED, Instant.now());
        FundHoldRequestedEvent event = event(txn);
        int threads = 2;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger applied = new AtomicInteger();

        Callable<Void> task = () -> {
            start.await();
            try {
                if (service.recordHoldConfirmed(event, "hold-race")) {
                    applied.incrementAndGet();
                }
            } catch (SagaConcurrentModificationException expected) {
            }
            return null;
        };
        Future<Void> first = executor.submit(task);
        Future<Void> second = executor.submit(task);
        start.countDown();
        first.get(30, TimeUnit.SECONDS);
        second.get(30, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(applied.get()).isEqualTo(1);
        assertThat(outboxCount(txn, KafkaTopics.FUND_HOLD_CONFIRMED)).isEqualTo(1);
        assertThat(sagaStep(txn)).isEqualTo("HOLD_CONFIRMED");
    }

    private FundHoldRequestedEvent event(UUID txn) {
        return new FundHoldRequestedEvent(UUID.randomUUID().toString(), "FUND_HOLD_REQUESTED", "1.0",
                Instant.now(), txn, new BigDecimal("1200.00"), "INR");
    }
}
