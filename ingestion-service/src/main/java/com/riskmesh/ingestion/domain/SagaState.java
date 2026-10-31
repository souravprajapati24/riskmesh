package com.riskmesh.ingestion.domain;

import com.riskmesh.ingestion.domain.enums.SagaStep;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record SagaState(
        UUID sagaId,
        UUID transactionId,
        SagaStep currentStep,
        BigDecimal holdAmount,
        String holdReference,
        Instant createdAt,
        Instant updatedAt,
        String failureReason,
        String compensationSteps,
        int version,
        Instant stepUpDeadline
) {}
