package com.riskmesh.common.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record FundHoldRequestedEvent(
        String eventId,
        String eventType,
        String eventVersion,
        Instant occurredAt,
        UUID transactionId,
        BigDecimal amount,
        String currency
) implements RiskMeshEvent {

    @Override
    public String partitionKey() {
        return transactionId.toString();
    }
}
