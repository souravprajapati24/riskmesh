package com.riskmesh.ingestion.api.dto;

import java.util.UUID;

public record TransactionStatusResponse(UUID transactionId, String status) {}
