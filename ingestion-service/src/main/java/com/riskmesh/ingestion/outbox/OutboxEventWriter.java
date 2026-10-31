package com.riskmesh.ingestion.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.riskmesh.common.events.RiskMeshEvent;
import com.riskmesh.ingestion.repository.OutboxEventRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class OutboxEventWriter {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public OutboxEventWriter(OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper, Clock clock) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    public void enqueue(UUID transactionId, String topic, RiskMeshEvent event) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            outboxEventRepository.insert(transactionId, topic, event.partitionKey(), payload, Instant.now(clock));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize outbox event payload for topic " + topic, e);
        }
    }
}
