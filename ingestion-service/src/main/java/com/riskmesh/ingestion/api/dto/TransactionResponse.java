package com.riskmesh.ingestion.api.dto;

import java.util.UUID;

public record TransactionResponse(UUID transactionId, String status) {}
