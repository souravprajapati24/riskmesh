package com.riskmesh.ingestion.config;

import com.riskmesh.common.events.KafkaTopics;
import java.time.Duration;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
@ConditionalOnProperty(prefix = "riskmesh.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KafkaTopicConfig {

    private final KafkaTopicProperties properties;

    public KafkaTopicConfig(KafkaTopicProperties properties) {
        this.properties = properties;
    }

    @Bean
    public KafkaAdmin.NewTopics riskMeshTopics() {
        return new KafkaAdmin.NewTopics(
                topic(KafkaTopics.TRANSACTION_RECEIVED, 12, days(7), true, true),
                topic(KafkaTopics.TRANSACTION_SCORED, 12, days(30), true, true),
                topic(KafkaTopics.FUND_HOLD_REQUESTED, 6, days(1), true, false),
                topic(KafkaTopics.FUND_RELEASED, 6, days(1), true, false),
                topic(KafkaTopics.FUND_CAPTURED, 6, days(1), true, false),
                topic(KafkaTopics.FUND_HOLD_FAILED, 6, days(1), true, false),
                topic(KafkaTopics.CASE_REVIEWED, 3, days(30), false, false),
                topic(KafkaTopics.NOTIFICATION_SENT, 3, days(7), false, false),
                topic(KafkaTopics.TRANSACTION_RECEIVED_DLQ, 3, days(14), false, false),
                topic(KafkaTopics.TRANSACTION_SCORED_DLQ, 3, days(14), false, false),

                topic(KafkaTopics.FUND_HOLD_CONFIRMED, 6, days(1), true, false),
                topic(KafkaTopics.FUND_HOLD_VOIDED, 6, days(1), true, false),
                topic(KafkaTopics.SAGA_COMPENSATED, 6, days(1), true, false),
                topic(KafkaTopics.STEP_UP_COMPLETED, 6, days(1), true, false));
    }

    private NewTopic topic(String name, int partitions, long retentionMs, boolean minInsyncReplicas,
                           boolean lz4Compression) {
        TopicBuilder builder = TopicBuilder.name(name)
                .partitions(partitions)
                .replicas(properties.getReplicationFactor())
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retentionMs))
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
        if (minInsyncReplicas) {
            builder.config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(properties.getMinInsyncReplicas()));
        }
        if (lz4Compression) {
            builder.config(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        }
        return builder.build();
    }

    private static long days(int days) {
        return Duration.ofDays(days).toMillis();
    }
}
