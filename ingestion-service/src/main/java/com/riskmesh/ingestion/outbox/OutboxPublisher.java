package com.riskmesh.ingestion.outbox;

import com.riskmesh.ingestion.repository.IngestionDlqRepository;
import com.riskmesh.ingestion.repository.OutboxEventRepository;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final int BATCH_SIZE = 100;
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(5);

    private final OutboxEventRepository outboxEventRepository;
    private final IngestionDlqRepository ingestionDlqRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Clock clock;

    public OutboxPublisher(OutboxEventRepository outboxEventRepository,
                           IngestionDlqRepository ingestionDlqRepository,
                           KafkaTemplate<String, String> kafkaTemplate,
                           Clock clock) {
        this.outboxEventRepository = outboxEventRepository;
        this.ingestionDlqRepository = ingestionDlqRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.clock = clock;
    }

    @Scheduled(fixedDelay = 50)
    @Transactional
    public void pollOnce() {
        List<OutboxEvent> batch = outboxEventRepository.findUnpublishedBatchForUpdateSkipLocked(BATCH_SIZE);
        for (OutboxEvent event : batch) {
            try {
                kafkaTemplate.send(event.topic(), event.eventKey(), event.payload())
                        .get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                recordFailure(event, e);
                return;
            } catch (Exception e) {
                recordFailure(event, e);
                if (isPerRecordFailure(e)) {
                    continue;
                }
                return;
            }
            Instant now = Instant.now(clock);
            outboxEventRepository.markPublished(event.id(), now);
            ingestionDlqRepository.markResolved(event.aggregateId(), event.payload(), now);
        }
    }

    private boolean isPerRecordFailure(Exception e) {
        Throwable cause = (e instanceof ExecutionException && e.getCause() != null) ? e.getCause() : e;
        return cause instanceof RecordTooLargeException
                || cause instanceof InvalidTopicException
                || cause instanceof SerializationException;
    }

    private void recordFailure(OutboxEvent event, Exception e) {
        Instant now = Instant.now(clock);
        String reason = "Kafka publish failed: " + e;
        log.warn("Failed to publish outbox event id={} topic={}; leaving unpublished for retry",
                event.id(), event.topic(), e);
        Optional<UUID> existing = ingestionDlqRepository.findUnresolvedId(event.aggregateId(), event.payload());
        if (existing.isPresent()) {
            ingestionDlqRepository.incrementRetry(existing.get(), reason, now);
        } else {
            ingestionDlqRepository.insert(event.aggregateId(), event.payload(), reason, now);
        }
    }
}
