package com.riskmesh.ingestion.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.riskmesh.common.events.KafkaTopics;
import com.riskmesh.ingestion.support.KafkaIntegrationTestBase;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class SagaHoldFlowIntegrationTest extends KafkaIntegrationTestBase {

    @Test
    void acceptedHoldMovesSagaToHoldConfirmedAndPublishesFundHoldConfirmed() throws Exception {
        GATEWAY.respondWith(200);

        UUID txn = submitTransaction("hold-ok-" + UUID.randomUUID());
        awaitSagaStep(txn, "HOLD_CONFIRMED");

        assertThat(GATEWAY.holdRequestCount()).isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT hold_reference FROM saga_states WHERE transaction_id = ?", String.class, txn))
                .startsWith("hold-");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT hold_amount FROM saga_states WHERE transaction_id = ?", java.math.BigDecimal.class, txn))
                .isEqualByComparingTo("1200.00");
        assertThat(outboxTopics(txn)).containsExactlyInAnyOrder(
                KafkaTopics.TRANSACTION_RECEIVED, KafkaTopics.FUND_HOLD_REQUESTED, KafkaTopics.FUND_HOLD_CONFIRMED);

        List<ConsumerRecord<String, String>> confirmed = awaitKafkaRecords(
                KafkaTopics.FUND_HOLD_CONFIRMED, txn.toString(), 1, Duration.ofSeconds(30));
        assertThat(confirmed).hasSize(1);
        JsonNode json = objectMapper.readTree(confirmed.get(0).value());
        assertThat(json.get("eventType").asText()).isEqualTo("FUND_HOLD_CONFIRMED");
        assertThat(json.get("transactionId").asText()).isEqualTo(txn.toString());
        assertThat(json.get("holdReference").asText()).startsWith("hold-");
    }

    @Test
    void rejectedHoldCompensatesTheSagaAndPublishesSagaCompensated() throws Exception {
        GATEWAY.respondWith(422);

        UUID txn = submitTransaction("hold-rejected-" + UUID.randomUUID());
        awaitSagaStep(txn, "COMPENSATED");   // AC-05: COMPENSATED, never FAILED

        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM transactions WHERE transaction_id = ?", String.class, txn))
                .isEqualTo("COMPENSATED");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT failure_reason FROM saga_states WHERE transaction_id = ?", String.class, txn))
                .contains("HTTP 422");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT compensation_steps::text FROM saga_states WHERE transaction_id = ?", String.class, txn))
                .contains("No funds were ever held");

        assertThat(outboxTopics(txn)).containsExactlyInAnyOrder(
                KafkaTopics.TRANSACTION_RECEIVED, KafkaTopics.FUND_HOLD_REQUESTED,
                KafkaTopics.FUND_HOLD_FAILED, KafkaTopics.SAGA_COMPENSATED);
        assertThat(outboxTopics(txn)).doesNotContain(
                KafkaTopics.FUND_RELEASED, KafkaTopics.FUND_CAPTURED, KafkaTopics.FUND_HOLD_CONFIRMED);

        List<ConsumerRecord<String, String>> compensated = awaitKafkaRecords(
                KafkaTopics.SAGA_COMPENSATED, txn.toString(), 1, Duration.ofSeconds(30));
        assertThat(compensated).hasSize(1);
        assertThat(compensated.get(0).value()).contains("SAGA_COMPENSATED");
    }

    @Test
    void redeliveredHoldRequestDoesNotPlaceASecondHold() throws Exception {
        GATEWAY.respondWith(200);
        UUID txn = submitTransaction("hold-redelivery-" + UUID.randomUUID());
        awaitSagaStep(txn, "HOLD_CONFIRMED");
        assertThat(GATEWAY.holdRequestCount()).isEqualTo(1);

        List<ConsumerRecord<String, String>> original = awaitKafkaRecords(
                KafkaTopics.FUND_HOLD_REQUESTED, txn.toString(), 1, Duration.ofSeconds(30));
        assertThat(original).hasSize(1);
        sendRaw(KafkaTopics.FUND_HOLD_REQUESTED, txn.toString(), original.get(0).value());
        sendRaw(KafkaTopics.FUND_HOLD_REQUESTED, txn.toString(), original.get(0).value());

        await().during(Duration.ofSeconds(5)).atMost(Duration.ofSeconds(15))
                .until(() -> GATEWAY.holdRequestCount() == 1);
        assertThat(currentSagaStep(txn)).isEqualTo("HOLD_CONFIRMED");
        assertThat(outboxTopics(txn).stream().filter(KafkaTopics.FUND_HOLD_CONFIRMED::equals).count()).isEqualTo(1);
    }

    @Test
    void technicalGatewayFailureLeavesSagaInHoldRequestedAfterRetries() throws Exception {
        GATEWAY.respondWith(500);

        UUID txn = submitTransaction("hold-gateway-down-" + UUID.randomUUID());

        await().atMost(Duration.ofSeconds(60)).until(() -> GATEWAY.holdRequestCount() >= 4);
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10))
                .until(() -> "HOLD_REQUESTED".equals(currentSagaStep(txn)));

        assertThat(outboxTopics(txn)).doesNotContain(KafkaTopics.FUND_HOLD_FAILED, KafkaTopics.FUND_HOLD_CONFIRMED);
    }
}
