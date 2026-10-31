package com.riskmesh.ingestion.outbox;

import java.time.Instant;
import java.util.UUID;

public record IngestionDlqEntry(
        UUID id,
        UUID transactionId,
        String errorReason,
        int retryCount,
        Instant createdAt
) {}
