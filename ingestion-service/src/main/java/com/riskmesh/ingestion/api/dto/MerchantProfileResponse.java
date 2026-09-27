package com.riskmesh.ingestion.api.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record MerchantProfileResponse(
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
