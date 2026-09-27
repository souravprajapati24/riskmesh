package com.riskmesh.ingestion.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.riskmesh.common.events.FundHoldRequestedEvent;
import com.riskmesh.common.events.TransactionReceivedEvent;
import com.riskmesh.ingestion.api.dto.TransactionRequest;
import com.riskmesh.ingestion.domain.Transaction;
import com.riskmesh.ingestion.repository.OutboxEventRepository;
import com.riskmesh.ingestion.repository.SagaStateRepository;
import com.riskmesh.ingestion.repository.TransactionRepository;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


@Service
public class TransactionIngestionService {

    private static final String TRANSACTION_RECEIVED_TOPIC = "transaction.received";
    private static final String FUND_HOLD_REQUESTED_TOPIC = "fund.hold.requested";

    private final IdempotencyService idempotencyService;
    private final TransactionRepository transactionRepository;
    private final SagaStateRepository sagaStateRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public TransactionIngestionService(IdempotencyService idempotencyService,
                                       TransactionRepository transactionRepository,
                                       SagaStateRepository sagaStateRepository,
                                       OutboxEventRepository outboxEventRepository,
                                       ObjectMapper objectMapper,
                                       Clock clock) {
        this.idempotencyService = idempotencyService;
        this.transactionRepository = transactionRepository;
        this.sagaStateRepository = sagaStateRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    @Transactional
    public IngestResult ingest(TransactionRequest request) {
        String idempotencyKey = computeIdempotencyKey(request.getExternalTxnId(), request.getMerchantId());
        UUID candidateTransactionId = UUID.randomUUID();

        IdempotencyResult idempotencyResult = idempotencyService.checkAndReserve(idempotencyKey, candidateTransactionId);
        if (!idempotencyResult.isNew()) {
            return IngestResult.duplicate(idempotencyResult.transactionId());
        }

        UUID transactionId = idempotencyResult.transactionId();
        Instant now = Instant.now(clock);

        Transaction transaction = new Transaction(
                transactionId,
                request.getExternalTxnId(),
                request.getMerchantId(),
                request.getPayerId(),
                request.getPayeeId(),
                request.getAmount(),
                request.getCurrency(),
                request.getPaymentMethod().name(),
                request.getCardBin(),
                request.getCardLast4(),
                request.getDeviceFingerprint(),
                request.getIpAddress(),
                request.getGeoCountry(),
                request.getGeoCity(),
                request.getMerchantCategory(),
                request.getUserAgent(),
                "RECEIVED",
                now);
        transactionRepository.insert(transaction);
        sagaStateRepository.insertInitiated(transactionId, now);

        TransactionReceivedEvent receivedEvent = new TransactionReceivedEvent(
                UUID.randomUUID().toString(), "TRANSACTION_RECEIVED", "1.0", now,
                transactionId, request.getExternalTxnId(), request.getMerchantId(), request.getPayerId(),
                request.getPayeeId(), request.getAmount(), request.getCurrency(),
                request.getPaymentMethod().name(), request.getCardBin(), request.getCardLast4(),
                request.getDeviceFingerprint(), request.getIpAddress(), request.getGeoCountry(),
                request.getGeoCity(), request.getMerchantCategory(), request.getUserAgent(), now);
        writeOutbox(transactionId, TRANSACTION_RECEIVED_TOPIC, receivedEvent, now);

        FundHoldRequestedEvent holdEvent = new FundHoldRequestedEvent(
                UUID.randomUUID().toString(), "FUND_HOLD_REQUESTED", "1.0", now,
                transactionId, request.getAmount(), request.getCurrency());
        writeOutbox(transactionId, FUND_HOLD_REQUESTED_TOPIC, holdEvent, now);

        return IngestResult.newTransaction(transactionId);
    }

    private void writeOutbox(UUID transactionId, String topic, Object event, Instant now) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            outboxEventRepository.insert(transactionId, topic, transactionId.toString(), payload, now);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize outbox event payload for topic " + topic, e);
        }
    }


    static String computeIdempotencyKey(String externalTxnId, UUID merchantId) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest((externalTxnId + "|" + merchantId).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
