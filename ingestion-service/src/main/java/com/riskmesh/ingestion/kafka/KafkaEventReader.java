package com.riskmesh.ingestion.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class KafkaEventReader {

    private final ObjectMapper objectMapper;

    public KafkaEventReader(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public <T> T read(String json, Class<T> type) {
        try {
            return objectMapper.readValue(json, type);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw new EventDeserializationException("Cannot deserialize " + type.getSimpleName() + " from Kafka record", e);
        }
    }
}
