package com.riskmesh.ingestion.service;

import java.util.UUID;

public record IdempotencyResult(boolean isNew, UUID transactionId) {

    public static IdempotencyResult reserved(UUID transactionId) {
        return new IdempotencyResult(true, transactionId);
    }

    public static IdempotencyResult duplicate(UUID transactionId) {
        return new IdempotencyResult(false, transactionId);
    }
}
