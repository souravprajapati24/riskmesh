package com.riskmesh.ingestion.domain;

import java.math.BigDecimal;
import java.util.UUID;


public record MerchantProfile(
        UUID merchantId,
        String merchantName,
        String mcc,
        String riskTier,
        BigDecimal maxTransactionLimit,
        int velocityWindowSec,
        int maxVelocityCount,
        double stepUpThreshold,
        double declineThreshold,
        boolean active
) {}
