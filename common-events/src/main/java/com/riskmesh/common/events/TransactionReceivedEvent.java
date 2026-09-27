package com.riskmesh.common.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record TransactionReceivedEvent(
        String eventId,
        String eventType,
        String eventVersion,
        Instant occurredAt,
        UUID transactionId,
        String externalTxnId,
        UUID merchantId,
        UUID payerId,
        UUID payeeId,
        BigDecimal amount,
        String currency,
        String paymentMethod,
        String cardBin,
        String cardLast4,
        String deviceFingerprint,
        String ipAddress,
        String geoCountry,
        String geoCity,
        String merchantCategory,
        String userAgent,
        Instant receivedAt
) implements RiskMeshEvent {

    @Override
    public String partitionKey() {
        return transactionId.toString();
    }
}
