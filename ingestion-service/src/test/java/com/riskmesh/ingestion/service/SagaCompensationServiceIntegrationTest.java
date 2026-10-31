package com.riskmesh.ingestion.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.riskmesh.common.events.FundHoldFailedEvent;
import com.riskmesh.common.events.KafkaTopics;
import com.riskmesh.ingestion.domain.enums.SagaStep;
import com.riskmesh.ingestion.support.PostgresIntegrationTestBase;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class SagaCompensationServiceIntegrationTest extends PostgresIntegrationTestBase {

    @Autowired
    private SagaCompensationService service;

    @Test
    void compensateClosesSagaMarksTransactionAndEnqueuesSagaCompensated() {
        UUID txn = insertTransactionWithSaga(SagaStep.COMPENSATING, Instant.now());

        assertThat(service.compensate(failedEvent(txn))).isTrue();
        assertThat(sagaStep(txn)).isEqualTo("COMPENSATED");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM transactions WHERE transaction_id = ?", String.class, txn))
                .isEqualTo("COMPENSATED");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT compensation_steps::text FROM saga_states WHERE transaction_id = ?", String.class, txn))
                .contains("No funds were ever held");
        assertThat(outboxCount(txn, KafkaTopics.SAGA_COMPENSATED)).isEqualTo(1);
        assertThat(outboxCount(txn, KafkaTopics.FUND_RELEASED)).isZero();
        assertThat(outboxCount(txn, KafkaTopics.FUND_CAPTURED)).isZero();
    }

    @Test
    void replayedEventIsASafeNoOp() {
        UUID txn = insertTransactionWithSaga(SagaStep.COMPENSATING, Instant.now());
        FundHoldFailedEvent event = failedEvent(txn);

        assertThat(service.compensate(event)).isTrue();
        assertThat(service.compensate(event)).isFalse();

        assertThat(sagaStep(txn)).isEqualTo("COMPENSATED");
        assertThat(outboxCount(txn, KafkaTopics.SAGA_COMPENSATED)).isEqualTo(1);
    }

    @Test
    void markFailedMovesCompensatingToFailedWithReason() {
        UUID txn = insertTransactionWithSaga(SagaStep.COMPENSATING, Instant.now());

        assertThat(service.markFailed(txn, "Compensation failed: boom")).isTrue();

        assertThat(sagaStep(txn)).isEqualTo("FAILED");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT failure_reason FROM saga_states WHERE transaction_id = ?", String.class, txn))
                .contains("boom");
        assertThat(service.markFailed(txn, "again")).isFalse();
    }

    private FundHoldFailedEvent failedEvent(UUID txn) {
        return new FundHoldFailedEvent(UUID.randomUUID().toString(), "FUND_HOLD_FAILED", "1.0", Instant.now(),
                txn, "Gateway rejected the hold (HTTP 422)");
    }
}
