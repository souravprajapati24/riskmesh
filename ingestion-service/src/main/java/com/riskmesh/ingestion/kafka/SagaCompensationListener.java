package com.riskmesh.ingestion.kafka;

import com.riskmesh.common.events.FundHoldFailedEvent;
import com.riskmesh.common.events.KafkaTopics;
import com.riskmesh.ingestion.service.AlertPublisher;
import com.riskmesh.ingestion.service.SagaCompensationService;
import com.riskmesh.ingestion.service.SagaConcurrentModificationException;
import com.riskmesh.ingestion.service.SagaNotFoundException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "riskmesh.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class SagaCompensationListener {

    private static final Logger log = LoggerFactory.getLogger(SagaCompensationListener.class);

    private final KafkaEventReader eventReader;
    private final SagaCompensationService compensationService;
    private final AlertPublisher alertPublisher;

    public SagaCompensationListener(KafkaEventReader eventReader, SagaCompensationService compensationService,
                                    AlertPublisher alertPublisher) {
        this.eventReader = eventReader;
        this.compensationService = compensationService;
        this.alertPublisher = alertPublisher;
    }

    @KafkaListener(
            topics = KafkaTopics.FUND_HOLD_FAILED,
            groupId = "saga-compensation-group",
            containerFactory = "manualAckContainerFactory")
    public void onHoldFailed(ConsumerRecord<String, String> record, Acknowledgment ack) {
        FundHoldFailedEvent event = eventReader.read(record.value(), FundHoldFailedEvent.class);
        try {
            compensationService.compensate(event);
        } catch (SagaNotFoundException | SagaConcurrentModificationException e) {
            throw e;
        } catch (RuntimeException e) {
            log.error("Compensation failed for transaction {}", event.transactionId(), e);
            compensationService.markFailed(event.transactionId(), "Compensation failed: " + e.getMessage());
            alertPublisher.pageOnCall("Saga compensation failed for transaction " + event.transactionId(), e);
        }
        ack.acknowledge();
    }
}
