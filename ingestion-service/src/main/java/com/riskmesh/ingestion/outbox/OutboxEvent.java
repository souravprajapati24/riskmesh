package com.riskmesh.ingestion.outbox;

import java.time.Instant;
import java.util.UUID;

public record OutboxEvent(
        long id,
        UUID aggregateId,
        String topic,
        String eventKey,
        String payload,
        Instant createdAt,
        Instant publishedAt
) {}
