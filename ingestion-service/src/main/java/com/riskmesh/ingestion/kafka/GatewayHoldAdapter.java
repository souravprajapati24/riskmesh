package com.riskmesh.ingestion.kafka;

import com.riskmesh.common.events.FundHoldRequestedEvent;
import com.riskmesh.common.events.KafkaTopics;
import com.riskmesh.ingestion.gateway.exception.GatewayRejectedException;
import com.riskmesh.ingestion.gateway.HoldResult;
import com.riskmesh.ingestion.gateway.MockGatewayClient;
import com.riskmesh.ingestion.service.SagaTransitionService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;


@Component
@ConditionalOnProperty(prefix = "riskmesh.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class GatewayHoldAdapter {

    private static final Logger log = LoggerFactory.getLogger(GatewayHoldAdapter.class);

    private final KafkaEventReader eventReader;
    private final SagaTransitionService sagaTransitionService;
    private final MockGatewayClient gatewayClient;

    public GatewayHoldAdapter(KafkaEventReader eventReader, SagaTransitionService sagaTransitionService,
                              MockGatewayClient gatewayClient) {
        this.eventReader = eventReader;
        this.sagaTransitionService = sagaTransitionService;
        this.gatewayClient = gatewayClient;
    }

    @KafkaListener(
            topics = KafkaTopics.FUND_HOLD_REQUESTED,
            groupId = "gateway-adapter-group",
            containerFactory = "manualAckContainerFactory")
    public void onHoldRequested(ConsumerRecord<String, String> record, Acknowledgment ack) {
        FundHoldRequestedEvent event = eventReader.read(record.value(), FundHoldRequestedEvent.class);

        if (!sagaTransitionService.beginHold(event.transactionId())) {
            ack.acknowledge();
            return;
        }

        try {
            HoldResult result = gatewayClient.placeHold(event.transactionId(), event.amount(), event.currency());
            sagaTransitionService.recordHoldConfirmed(event, result.holdReference());
            log.info("Hold confirmed for transaction {} (holdReference={})", event.transactionId(), result.holdReference());
        } catch (GatewayRejectedException e) {
            sagaTransitionService.recordHoldRejected(event, e.getMessage());
            log.info("Hold rejected for transaction {}: {}", event.transactionId(), e.getMessage());
        }
        ack.acknowledge();
    }
}
