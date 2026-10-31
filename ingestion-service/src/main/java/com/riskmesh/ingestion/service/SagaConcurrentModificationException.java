package com.riskmesh.ingestion.service;

import java.util.UUID;


public class SagaConcurrentModificationException extends RuntimeException {

    public SagaConcurrentModificationException(UUID sagaId) {
        super("Saga " + sagaId + " was modified concurrently");
    }
}
