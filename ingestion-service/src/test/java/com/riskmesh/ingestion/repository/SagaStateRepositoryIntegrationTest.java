package com.riskmesh.ingestion.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.riskmesh.ingestion.domain.SagaState;
import com.riskmesh.ingestion.domain.enums.SagaStep;
import com.riskmesh.ingestion.support.PostgresIntegrationTestBase;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class SagaStateRepositoryIntegrationTest extends PostgresIntegrationTestBase {

    @Autowired
    private SagaStateRepository repository;

    @Test
    void transitionAppliesWhenStepAndVersionMatch() {
        UUID txn = insertTransactionWithSaga(SagaStep.HOLD_REQUESTED, Instant.now());
        SagaState saga = repository.findByTransactionId(txn).orElseThrow();

        int rows = repository.transition(saga.sagaId(), SagaStep.HOLD_REQUESTED, SagaStep.HOLD_CONFIRMED,
                saga.version(), "hold-9", new BigDecimal("55.50"), null, Instant.now());

        assertThat(rows).isEqualTo(1);
        SagaState updated = repository.findByTransactionId(txn).orElseThrow();
        assertThat(updated.currentStep()).isEqualTo(SagaStep.HOLD_CONFIRMED);
        assertThat(updated.version()).isEqualTo(saga.version() + 1);
        assertThat(updated.holdReference()).isEqualTo("hold-9");
        assertThat(updated.holdAmount()).isEqualByComparingTo("55.50");
    }

    @Test
    void transitionIsRejectedWhenVersionIsStale() {
        UUID txn = insertTransactionWithSaga(SagaStep.HOLD_REQUESTED, Instant.now());
        SagaState saga = repository.findByTransactionId(txn).orElseThrow();

        int rows = repository.transition(saga.sagaId(), SagaStep.HOLD_REQUESTED, SagaStep.HOLD_CONFIRMED,
                saga.version() + 5, null, null, null, Instant.now());

        assertThat(rows).isZero();
        assertThat(repository.findByTransactionId(txn).orElseThrow().currentStep()).isEqualTo(SagaStep.HOLD_REQUESTED);
    }

    @Test
    void transitionIsRejectedWhenStepDoesNotMatch() {
        UUID txn = insertTransactionWithSaga(SagaStep.COMPENSATED, Instant.now());
        SagaState saga = repository.findByTransactionId(txn).orElseThrow();

        int rows = repository.transition(saga.sagaId(), SagaStep.COMPENSATING, SagaStep.COMPENSATED,
                saga.version(), null, null, null, Instant.now());

        assertThat(rows).isZero();
    }

    @Test
    void nullOptionalFieldsDoNotOverwriteExistingValues() {
        UUID txn = insertTransactionWithSaga(SagaStep.HOLD_REQUESTED, Instant.now());
        SagaState saga = repository.findByTransactionId(txn).orElseThrow();
        repository.transition(saga.sagaId(), SagaStep.HOLD_REQUESTED, SagaStep.HOLD_CONFIRMED, saga.version(),
                "hold-keep", new BigDecimal("1.00"), null, Instant.now());
        SagaState confirmed = repository.findByTransactionId(txn).orElseThrow();

        repository.transition(confirmed.sagaId(), SagaStep.HOLD_CONFIRMED, SagaStep.SCORING_COMPLETE,
                confirmed.version(), null, null, null, Instant.now());

        SagaState after = repository.findByTransactionId(txn).orElseThrow();
        assertThat(after.holdReference()).isEqualTo("hold-keep");
        assertThat(after.holdAmount()).isEqualByComparingTo("1.00");
    }

    @Test
    void appendCompensationStepsConcatenatesJsonArrays() {
        UUID txn = insertTransactionWithSaga(SagaStep.COMPENSATING, Instant.now());
        SagaState saga = repository.findByTransactionId(txn).orElseThrow();

        repository.appendCompensationSteps(saga.sagaId(), "[\"one\",\"two\"]", Instant.now());
        repository.appendCompensationSteps(saga.sagaId(), "[\"three\"]", Instant.now());

        String steps = repository.findByTransactionId(txn).orElseThrow().compensationSteps();
        assertThat(steps).contains("one").contains("two").contains("three");
    }

    @Test
    void findStalledReturnsOnlyEarlySagaStepsOlderThanTheCutoff() {
        Instant old = Instant.now().minus(Duration.ofMinutes(10));
        UUID initiated = insertTransactionWithSaga(SagaStep.INITIATED, old);
        UUID holdRequested = insertTransactionWithSaga(SagaStep.HOLD_REQUESTED, old);
        UUID compensating = insertTransactionWithSaga(SagaStep.COMPENSATING, old);
        insertTransactionWithSaga(SagaStep.HOLD_CONFIRMED, old);
        insertTransactionWithSaga(SagaStep.COMPLETED, old);
        insertTransactionWithSaga(SagaStep.COMPENSATED, old);
        insertTransactionWithSaga(SagaStep.INITIATED, Instant.now());

        List<SagaState> stalled = repository.findStalled(Instant.now().minus(Duration.ofMinutes(5)));

        assertThat(stalled).extracting(SagaState::transactionId)
                .containsExactlyInAnyOrder(initiated, holdRequested, compensating);
    }
}
