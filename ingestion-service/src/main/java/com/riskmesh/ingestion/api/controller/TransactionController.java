package com.riskmesh.ingestion.api.controller;

import com.riskmesh.ingestion.api.dto.TransactionRequest;
import com.riskmesh.ingestion.api.dto.TransactionResponse;
import com.riskmesh.ingestion.api.dto.TransactionStatusResponse;
import com.riskmesh.ingestion.repository.TransactionRepository;
import com.riskmesh.ingestion.service.IngestResult;
import com.riskmesh.ingestion.service.TransactionIngestionService;
import jakarta.validation.Valid;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/api/v1/transactions")
public class TransactionController {

    private final TransactionIngestionService ingestionService;
    private final TransactionRepository transactionRepository;

    public TransactionController(TransactionIngestionService ingestionService,
                                 TransactionRepository transactionRepository) {
        this.ingestionService = ingestionService;
        this.transactionRepository = transactionRepository;
    }

    @PostMapping
    public ResponseEntity<TransactionResponse> submit(@Valid @RequestBody TransactionRequest request) {
        IngestResult result = ingestionService.ingest(request);
        return switch (result.status()) {
            case NEW -> ResponseEntity.status(HttpStatus.ACCEPTED)
                    .body(new TransactionResponse(result.transactionId(), "RECEIVED"));
            case DUPLICATE -> ResponseEntity.ok(
                    new TransactionResponse(result.transactionId(), "ALREADY_PROCESSED"));
        };
    }

    @GetMapping("/{transactionId}/status")
    public ResponseEntity<TransactionStatusResponse> status(@PathVariable UUID transactionId) {
        return transactionRepository.findStatus(transactionId)
                .map(status -> ResponseEntity.ok(new TransactionStatusResponse(transactionId, status)))
                .orElse(ResponseEntity.notFound().build());
    }
}
