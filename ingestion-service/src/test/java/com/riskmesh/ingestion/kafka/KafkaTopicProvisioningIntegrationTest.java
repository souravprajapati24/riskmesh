package com.riskmesh.ingestion.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.riskmesh.common.events.KafkaTopics;
import com.riskmesh.ingestion.support.KafkaIntegrationTestBase;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;

class KafkaTopicProvisioningIntegrationTest extends KafkaIntegrationTestBase {

    @Test
    void allTopicsExistWithTheDocumentedPartitionCountsAndRetention() throws Exception {
        Map<String, Integer> expectedPartitions = Map.ofEntries(
                Map.entry(KafkaTopics.TRANSACTION_RECEIVED, 12),
                Map.entry(KafkaTopics.TRANSACTION_SCORED, 12),
                Map.entry(KafkaTopics.FUND_HOLD_REQUESTED, 6),
                Map.entry(KafkaTopics.FUND_RELEASED, 6),
                Map.entry(KafkaTopics.FUND_CAPTURED, 6),
                Map.entry(KafkaTopics.FUND_HOLD_FAILED, 6),
                Map.entry(KafkaTopics.FUND_HOLD_CONFIRMED, 6),
                Map.entry(KafkaTopics.FUND_HOLD_VOIDED, 6),
                Map.entry(KafkaTopics.SAGA_COMPENSATED, 6),
                Map.entry(KafkaTopics.STEP_UP_COMPLETED, 6),
                Map.entry(KafkaTopics.CASE_REVIEWED, 3),
                Map.entry(KafkaTopics.NOTIFICATION_SENT, 3),
                Map.entry(KafkaTopics.TRANSACTION_RECEIVED_DLQ, 3),
                Map.entry(KafkaTopics.TRANSACTION_SCORED_DLQ, 3));

        try (AdminClient admin = AdminClient.create(
                Map.<String, Object>of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            Map<String, TopicDescription> described =
                    admin.describeTopics(expectedPartitions.keySet()).allTopicNames().get(30, TimeUnit.SECONDS);
            expectedPartitions.forEach((name, partitions) ->
                    assertThat(described.get(name).partitions()).as(name).hasSize(partitions));

            assertRetention(admin, KafkaTopics.TRANSACTION_RECEIVED, "604800000");    // 7 days
            assertRetention(admin, KafkaTopics.TRANSACTION_SCORED, "2592000000");     // 30 days
            assertRetention(admin, KafkaTopics.FUND_HOLD_REQUESTED, "86400000");      // 1 day
            assertRetention(admin, KafkaTopics.CASE_REVIEWED, "2592000000");          // 30 days
            assertRetention(admin, KafkaTopics.NOTIFICATION_SENT, "604800000");       // 7 days
            assertRetention(admin, KafkaTopics.TRANSACTION_SCORED_DLQ, "1209600000"); // 14 days

            Config received = topicConfig(admin, KafkaTopics.TRANSACTION_RECEIVED);
            assertThat(received.get("compression.type").value()).isEqualTo("lz4");
            assertThat(received.get("min.insync.replicas").value()).isEqualTo("1");   // local test override
        }
    }

    private void assertRetention(AdminClient admin, String topic, String expectedMs) throws Exception {
        assertThat(topicConfig(admin, topic).get("retention.ms").value()).as(topic).isEqualTo(expectedMs);
    }

    private Config topicConfig(AdminClient admin, String topic) throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        return admin.describeConfigs(List.of(resource)).all().get(30, TimeUnit.SECONDS).get(resource);
    }
}
