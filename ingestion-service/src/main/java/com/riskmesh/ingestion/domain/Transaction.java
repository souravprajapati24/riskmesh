package com.riskmesh.ingestion.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;


public record Transaction(
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
        String status,
        Instant receivedAt
) {}
