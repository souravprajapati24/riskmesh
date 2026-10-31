package com.riskmesh.ingestion.service;

import java.util.UUID;

public class SagaNotFoundException extends RuntimeException {

    public SagaNotFoundException(UUID transactionId) {
        super("No saga found for transaction " + transactionId);
    }
}
