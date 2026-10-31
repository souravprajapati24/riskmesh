package com.riskmesh.ingestion.service;

import com.riskmesh.common.events.FundHoldFailedEvent;
import com.riskmesh.common.events.KafkaTopics;
import com.riskmesh.common.events.SagaCompensatedEvent;
import com.riskmesh.ingestion.domain.SagaState;
import com.riskmesh.ingestion.domain.enums.SagaStep;
import com.riskmesh.ingestion.outbox.OutboxEventWriter;
import com.riskmesh.ingestion.repository.SagaStateRepository;
import com.riskmesh.ingestion.repository.TransactionRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


@Service
public class SagaCompensationService {

    private static final Logger log = LoggerFactory.getLogger(SagaCompensationService.class);
    private static final String COMPENSATION_STEPS_JSON =
            "[\"Marked transaction as COMPENSATED\",\"No funds were ever held\"]";

    private final SagaStateRepository sagaStateRepository;
    private final TransactionRepository transactionRepository;
    private final OutboxEventWriter outboxEventWriter;
    private final Clock clock;

    public SagaCompensationService(SagaStateRepository sagaStateRepository,
                                   TransactionRepository transactionRepository,
                                   OutboxEventWriter outboxEventWriter,
                                   Clock clock) {
        this.sagaStateRepository = sagaStateRepository;
        this.transactionRepository = transactionRepository;
        this.outboxEventWriter = outboxEventWriter;
        this.clock = clock;
    }

    @Transactional
    public boolean compensate(FundHoldFailedEvent event) {
        SagaState saga = load(event.transactionId());
        if (saga.currentStep() != SagaStep.COMPENSATING) {
            log.info("Compensation ignored for transaction {}: saga in step {}",
                    event.transactionId(), saga.currentStep());
            return false;
        }
        Instant now = Instant.now(clock);
        int rows = sagaStateRepository.transition(saga.sagaId(), SagaStep.COMPENSATING, SagaStep.COMPENSATED,
                saga.version(), null, null, null, now);
        if (rows == 0) {
            throw new SagaConcurrentModificationException(saga.sagaId());
        }
        sagaStateRepository.appendCompensationSteps(saga.sagaId(), COMPENSATION_STEPS_JSON, now);
        transactionRepository.updateStatus(event.transactionId(), "COMPENSATED", now);
        outboxEventWriter.enqueue(event.transactionId(), KafkaTopics.SAGA_COMPENSATED,
                new SagaCompensatedEvent(UUID.randomUUID().toString(), "SAGA_COMPENSATED", "1.0", now,
                        event.transactionId()));
        return true;
    }

    @Transactional
    public boolean markFailed(UUID transactionId, String reason) {
        SagaState saga = load(transactionId);
        if (saga.currentStep() != SagaStep.COMPENSATING) {
            return false;
        }
        int rows = sagaStateRepository.transition(saga.sagaId(), SagaStep.COMPENSATING, SagaStep.FAILED,
                saga.version(), null, null, reason, Instant.now(clock));
        if (rows == 0) {
            throw new SagaConcurrentModificationException(saga.sagaId());
        }
        return true;
    }

    private SagaState load(UUID transactionId) {
        return sagaStateRepository.findByTransactionId(transactionId)
                .orElseThrow(() -> new SagaNotFoundException(transactionId));
    }
}
