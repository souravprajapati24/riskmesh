package com.riskmesh.ingestion.domain.enums;

public enum SagaStep {
    INITIATED,
    HOLD_REQUESTED,
    HOLD_CONFIRMED,
    SCORING_COMPLETE,
    COMPLETED,
    COMPENSATING,
    COMPENSATED,
    FAILED
}
