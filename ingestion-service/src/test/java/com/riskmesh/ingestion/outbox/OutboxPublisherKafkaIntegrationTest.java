package com.riskmesh.ingestion.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.riskmesh.common.events.KafkaTopics;
import com.riskmesh.ingestion.repository.OutboxEventRepository;
import com.riskmesh.ingestion.support.KafkaIntegrationTestBase;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class OutboxPublisherKafkaIntegrationTest extends KafkaIntegrationTestBase {

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Test
    void scheduledPollerPublishesPendingRowsToKafkaKeyedByTransactionId() {
        UUID aggregateId = UUID.randomUUID();
        outboxEventRepository.insert(aggregateId, KafkaTopics.TRANSACTION_RECEIVED, aggregateId.toString(),
                "{\"probe\":true}", Instant.now());

        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200)).until(() ->
                count("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ? AND published_at IS NOT NULL",
                        aggregateId) == 1);

        List<ConsumerRecord<String, String>> records = awaitKafkaRecords(
                KafkaTopics.TRANSACTION_RECEIVED, aggregateId.toString(), 1, Duration.ofSeconds(30));
        assertThat(records).hasSize(1);
        assertThat(records.get(0).key()).isEqualTo(aggregateId.toString());
        assertThat(records.get(0).value()).contains("probe");
    }

    @Test
    void ingestedTransactionReachesKafkaAsTransactionReceivedAndFundHoldRequested() throws Exception {
        UUID txn = submitTransaction("outbox-e2e-" + UUID.randomUUID());

        List<ConsumerRecord<String, String>> received = awaitKafkaRecords(
                KafkaTopics.TRANSACTION_RECEIVED, txn.toString(), 1, Duration.ofSeconds(30));
        List<ConsumerRecord<String, String>> requested = awaitKafkaRecords(
                KafkaTopics.FUND_HOLD_REQUESTED, txn.toString(), 1, Duration.ofSeconds(30));

        assertThat(received).hasSize(1);
        assertThat(requested).hasSize(1);
        assertThat(received.get(0).value()).contains("TRANSACTION_RECEIVED");
        assertThat(requested.get(0).value()).contains("FUND_HOLD_REQUESTED");
    }
}
