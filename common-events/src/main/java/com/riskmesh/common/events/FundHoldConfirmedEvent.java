package com.riskmesh.common.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record FundHoldConfirmedEvent(
        String eventId,
        String eventType,
        String eventVersion,
        Instant occurredAt,
        UUID transactionId,
        String holdReference,
        BigDecimal holdAmount,
        String currency
) implements RiskMeshEvent {

    @Override
    public String partitionKey() {
        return transactionId.toString();
    }
}
