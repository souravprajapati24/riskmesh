package com.riskmesh.ingestion.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.riskmesh.ingestion.domain.enums.SagaStep;
import com.riskmesh.ingestion.support.PostgresIntegrationTestBase;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class StalledSagaMonitorIntegrationTest extends PostgresIntegrationTestBase {

    @Autowired
    private StalledSagaMonitor monitor;

    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    void alertsOnlyForSagasStuckBeforeScoringCompleted() {
        Instant old = Instant.now().minus(Duration.ofMinutes(10));
        insertTransactionWithSaga(SagaStep.INITIATED, old);
        insertTransactionWithSaga(SagaStep.HOLD_REQUESTED, old);
        insertTransactionWithSaga(SagaStep.COMPENSATING, old);
        insertTransactionWithSaga(SagaStep.HOLD_CONFIRMED, old);
        insertTransactionWithSaga(SagaStep.COMPLETED, old);
        insertTransactionWithSaga(SagaStep.INITIATED, Instant.now().minus(Duration.ofMinutes(1)));
        double before = meterRegistry.counter("riskmesh.saga.stalled.total").count();

        monitor.checkStalledSagas();

        double after = meterRegistry.counter("riskmesh.saga.stalled.total").count();
        assertThat(after - before).isEqualTo(3.0);
    }
}
