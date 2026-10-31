package com.riskmesh.common.events;

import java.time.Instant;
import java.util.UUID;

public record FundHoldFailedEvent(
        String eventId,
        String eventType,
        String eventVersion,
        Instant occurredAt,
        UUID transactionId,
        String reason
) implements RiskMeshEvent {

    @Override
    public String partitionKey() {
        return transactionId.toString();
    }
}
