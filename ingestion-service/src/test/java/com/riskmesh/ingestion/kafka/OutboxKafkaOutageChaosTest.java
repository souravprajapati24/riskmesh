package com.riskmesh.ingestion.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.github.dockerjava.api.DockerClient;
import com.riskmesh.common.events.KafkaTopics;
import com.riskmesh.ingestion.support.KafkaIntegrationTestBase;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OutboxKafkaOutageChaosTest extends KafkaIntegrationTestBase {

    @Autowired
    private KafkaTemplate<String , String> kafkaTemplate;

    @Test
    void kafkaOutageLeavesRowsUnpublishedWithASingleDlqRowThenRecoversWithoutLoss() throws Exception {
        GATEWAY.respondWith(200);
        DockerClient docker = KAFKA.getDockerClient();
        String kafkaContainerId = KAFKA.getContainerId();
        UUID txn;

        docker.pauseContainerCmd(kafkaContainerId).exec();
        try {
            txn = submitTransaction("chaos-" + UUID.randomUUID());

            await().atMost(Duration.ofSeconds(40)).pollInterval(Duration.ofMillis(500)).until(() ->
                    count("SELECT COUNT(*) FROM ingestion_dlq WHERE transaction_id = ?", txn) >= 1);

            Thread.sleep(15_000);

            assertThat(count("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ? AND published_at IS NULL", txn))
                    .isEqualTo(2);
            assertThat(count("SELECT COUNT(*) FROM ingestion_dlq WHERE transaction_id = ?", txn)).isEqualTo(1);
            assertThat(count("SELECT retry_count FROM ingestion_dlq WHERE transaction_id = ?", txn))
                    .isGreaterThanOrEqualTo(1);
            assertThat(currentSagaStep(txn)).isEqualTo("INITIATED");
        } finally {
            docker.unpauseContainerCmd(kafkaContainerId).exec();
            ((DefaultKafkaProducerFactory<?, ?>) kafkaTemplate.getProducerFactory()).reset();
        }

        awaitSagaStep(txn, "HOLD_CONFIRMED");
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500)).until(() ->
                count("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ? AND published_at IS NULL", txn) == 0);
        assertThat(count("SELECT COUNT(*) FROM ingestion_dlq WHERE transaction_id = ? AND resolved = FALSE", txn))
                .isZero();
        assertThat(outboxTopics(txn)).contains(
                KafkaTopics.TRANSACTION_RECEIVED, KafkaTopics.FUND_HOLD_REQUESTED, KafkaTopics.FUND_HOLD_CONFIRMED);
    }
}
