package com.riskmesh.ingestion.service;

import java.util.UUID;

public record IngestResult(Status status, UUID transactionId) {

    public enum Status { NEW, DUPLICATE }

    public static IngestResult newTransaction(UUID transactionId) {
        return new IngestResult(Status.NEW, transactionId);
    }

    public static IngestResult duplicate(UUID transactionId) {
        return new IngestResult(Status.DUPLICATE, transactionId);
    }
}
