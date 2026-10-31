package com.riskmesh.common.events;

import java.time.Instant;
import java.util.UUID;

public record SagaCompensatedEvent(
        String eventId,
        String eventType,
        String eventVersion,
        Instant occurredAt,
        UUID transactionId
) implements RiskMeshEvent {

    @Override
    public String partitionKey() {
        return transactionId.toString();
    }
}
