package com.riskmesh.ingestion.service;

import com.riskmesh.ingestion.config.SagaProperties;
import com.riskmesh.ingestion.domain.SagaState;
import com.riskmesh.ingestion.repository.SagaStateRepository;
import java.time.Clock;
import java.time.Instant;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class StalledSagaMonitor {

    private final SagaStateRepository sagaStateRepository;
    private final AlertPublisher alertPublisher;
    private final SagaProperties properties;
    private final Clock clock;

    public StalledSagaMonitor(SagaStateRepository sagaStateRepository, AlertPublisher alertPublisher,
                              SagaProperties properties, Clock clock) {
        this.sagaStateRepository = sagaStateRepository;
        this.alertPublisher = alertPublisher;
        this.properties = properties;
        this.clock = clock;
    }

    @Scheduled(fixedRate = 60_000)
    public void checkStalledSagas() {
        Instant cutoff = Instant.now(clock).minus(properties.getStalledThreshold());
        for (SagaState saga : sagaStateRepository.findStalled(cutoff)) {
            alertPublisher.emitStalledSaga(saga);
        }
    }
}
