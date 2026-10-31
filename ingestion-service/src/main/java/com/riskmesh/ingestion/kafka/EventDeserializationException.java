package com.riskmesh.ingestion.kafka;
public class EventDeserializationException extends RuntimeException {

    public EventDeserializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
