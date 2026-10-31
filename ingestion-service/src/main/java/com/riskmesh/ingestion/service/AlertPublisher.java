package com.riskmesh.ingestion.service;

import com.riskmesh.ingestion.domain.SagaState;
import com.riskmesh.ingestion.outbox.IngestionDlqEntry;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class AlertPublisher {

    private static final Logger log = LoggerFactory.getLogger(AlertPublisher.class);

    private final MeterRegistry meterRegistry;

    public AlertPublisher(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void emitStalledSaga(SagaState saga) {
        log.warn("Stalled saga detected: transactionId={} step={} lastUpdated={}",
                saga.transactionId(), saga.currentStep(), saga.updatedAt());
        meterRegistry.counter("riskmesh.saga.stalled.total").increment();
    }

    public void pageOnCall(String message, Throwable cause) {
        log.error("PAGE ON-CALL: {}", message, cause);
        meterRegistry.counter("riskmesh.alerts.page.total").increment();
    }

    public void escalateOutboxFailure(IngestionDlqEntry entry) {
        log.error("Outbox publish failure escalated: transactionId={} retryCount={} firstFailedAt={} reason={}",
                entry.transactionId(), entry.retryCount(), entry.createdAt(), entry.errorReason());
        meterRegistry.counter("riskmesh.ingestion.outbox.escalations.total").increment();
    }
}
