package com.riskmesh.ingestion.outbox;

import com.riskmesh.ingestion.config.OutboxProperties;
import com.riskmesh.ingestion.repository.IngestionDlqRepository;
import com.riskmesh.ingestion.service.AlertPublisher;
import java.time.Clock;
import java.time.Instant;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OutboxEscalationJob {

    private final IngestionDlqRepository ingestionDlqRepository;
    private final AlertPublisher alertPublisher;
    private final OutboxProperties properties;
    private final Clock clock;

    public OutboxEscalationJob(IngestionDlqRepository ingestionDlqRepository, AlertPublisher alertPublisher,
                               OutboxProperties properties, Clock clock) {
        this.ingestionDlqRepository = ingestionDlqRepository;
        this.alertPublisher = alertPublisher;
        this.properties = properties;
        this.clock = clock;
    }

    @Scheduled(fixedRate = 300_000)
    public void escalateStaleFailures() {
        Instant cutoff = Instant.now(clock).minus(properties.getEscalationAfter());
        for (IngestionDlqEntry entry : ingestionDlqRepository.findUnresolvedCreatedBefore(cutoff)) {
            alertPublisher.escalateOutboxFailure(entry);
        }
    }
}
