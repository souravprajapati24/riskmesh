package com.riskmesh.ingestion.service;

import com.riskmesh.common.events.FundHoldConfirmedEvent;
import com.riskmesh.common.events.FundHoldFailedEvent;
import com.riskmesh.common.events.FundHoldRequestedEvent;
import com.riskmesh.common.events.KafkaTopics;
import com.riskmesh.ingestion.domain.SagaState;
import com.riskmesh.ingestion.domain.enums.SagaStep;
import com.riskmesh.ingestion.outbox.OutboxEventWriter;
import com.riskmesh.ingestion.repository.SagaStateRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


@Service
public class SagaTransitionService {

    private static final Logger log = LoggerFactory.getLogger(SagaTransitionService.class);

    private final SagaStateRepository sagaStateRepository;
    private final OutboxEventWriter outboxEventWriter;
    private final Clock clock;

    public SagaTransitionService(SagaStateRepository sagaStateRepository, OutboxEventWriter outboxEventWriter,
                                 Clock clock) {
        this.sagaStateRepository = sagaStateRepository;
        this.outboxEventWriter = outboxEventWriter;
        this.clock = clock;
    }


    @Transactional
    public boolean beginHold(UUID transactionId) {
        SagaState saga = load(transactionId);
        if (saga.currentStep() == SagaStep.INITIATED) {
            int rows = sagaStateRepository.transition(saga.sagaId(), SagaStep.INITIATED, SagaStep.HOLD_REQUESTED,
                    saga.version(), null, null, null, Instant.now(clock));
            if (rows == 0) {
                throw new SagaConcurrentModificationException(saga.sagaId());
            }
            return true;
        }
        if (saga.currentStep() == SagaStep.HOLD_REQUESTED) {
            return true;
        }
        log.info("Ignoring fund.hold.requested for transaction {}: saga already in step {}",
                transactionId, saga.currentStep());
        return false;
    }

    @Transactional
    public boolean recordHoldConfirmed(FundHoldRequestedEvent event, String holdReference) {
        SagaState saga = load(event.transactionId());
        if (saga.currentStep() != SagaStep.HOLD_REQUESTED) {
            log.info("Hold confirmation ignored for transaction {}: saga in step {}",
                    event.transactionId(), saga.currentStep());
            return false;
        }
        Instant now = Instant.now(clock);
        int rows = sagaStateRepository.transition(saga.sagaId(), SagaStep.HOLD_REQUESTED, SagaStep.HOLD_CONFIRMED,
                saga.version(), holdReference, event.amount(), null, now);
        if (rows == 0) {
            throw new SagaConcurrentModificationException(saga.sagaId());
        }
        outboxEventWriter.enqueue(event.transactionId(), KafkaTopics.FUND_HOLD_CONFIRMED,
                new FundHoldConfirmedEvent(UUID.randomUUID().toString(), "FUND_HOLD_CONFIRMED", "1.0", now,
                        event.transactionId(), holdReference, event.amount(), event.currency()));
        return true;
    }

    @Transactional
    public boolean recordHoldRejected(FundHoldRequestedEvent event, String reason) {
        SagaState saga = load(event.transactionId());
        if (saga.currentStep() != SagaStep.HOLD_REQUESTED) {
            log.info("Hold rejection ignored for transaction {}: saga in step {}",
                    event.transactionId(), saga.currentStep());
            return false;
        }
        Instant now = Instant.now(clock);
        int rows = sagaStateRepository.transition(saga.sagaId(), SagaStep.HOLD_REQUESTED, SagaStep.COMPENSATING,
                saga.version(), null, null, reason, now);
        if (rows == 0) {
            throw new SagaConcurrentModificationException(saga.sagaId());
        }
        outboxEventWriter.enqueue(event.transactionId(), KafkaTopics.FUND_HOLD_FAILED,
                new FundHoldFailedEvent(UUID.randomUUID().toString(), "FUND_HOLD_FAILED", "1.0", now,
                        event.transactionId(), reason));
        return true;
    }

    private SagaState load(UUID transactionId) {
        return sagaStateRepository.findByTransactionId(transactionId)
                .orElseThrow(() -> new SagaNotFoundException(transactionId));
    }
}
