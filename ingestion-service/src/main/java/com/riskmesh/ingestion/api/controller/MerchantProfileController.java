package com.riskmesh.ingestion.api.controller;

import com.riskmesh.ingestion.api.dto.MerchantProfileResponse;
import com.riskmesh.ingestion.domain.MerchantProfile;
import com.riskmesh.ingestion.repository.MerchantProfileRepository;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/internal/merchants")
public class MerchantProfileController {

    private final MerchantProfileRepository repository;

    public MerchantProfileController(MerchantProfileRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/{merchantId}")
    public ResponseEntity<MerchantProfileResponse> get(@PathVariable UUID merchantId) {
        return repository.findById(merchantId)
                .map(this::toResponse)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    private MerchantProfileResponse toResponse(MerchantProfile profile) {
        return new MerchantProfileResponse(
                profile.merchantId(), profile.merchantName(), profile.mcc(), profile.riskTier(),
                profile.maxTransactionLimit(), profile.velocityWindowSec(), profile.maxVelocityCount(),
                profile.stepUpThreshold(), profile.declineThreshold(), profile.active());
    }
}
