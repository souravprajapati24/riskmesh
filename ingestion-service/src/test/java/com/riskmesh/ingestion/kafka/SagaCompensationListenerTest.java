package com.riskmesh.ingestion.kafka;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.riskmesh.common.events.FundHoldFailedEvent;
import com.riskmesh.ingestion.service.AlertPublisher;
import com.riskmesh.ingestion.service.SagaCompensationService;
import com.riskmesh.ingestion.service.SagaConcurrentModificationException;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

class SagaCompensationListenerTest {

    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    private final SagaCompensationService compensation = mock(SagaCompensationService.class);
    private final AlertPublisher alerts = mock(AlertPublisher.class);
    private final Acknowledgment ack = mock(Acknowledgment.class);
    private final SagaCompensationListener listener =
            new SagaCompensationListener(new KafkaEventReader(mapper), compensation, alerts);

    private final UUID transactionId = UUID.randomUUID();
    private final FundHoldFailedEvent event = new FundHoldFailedEvent(
            UUID.randomUUID().toString(), "FUND_HOLD_FAILED", "1.0", Instant.parse("2026-01-01T00:00:00Z"),
            transactionId, "Gateway rejected the hold (HTTP 422)");

    @Test
    void successfulCompensationIsAcknowledged() throws Exception {
        listener.onHoldFailed(record(), ack);

        verify(compensation).compensate(event);
        verify(ack).acknowledge();
        verify(compensation, never()).markFailed(any(), anyString());
    }

    @Test
    void concurrentModificationIsRethrownForRetryAndNotAcknowledged() throws Exception {
        doThrow(new SagaConcurrentModificationException(UUID.randomUUID())).when(compensation).compensate(event);

        assertThatThrownBy(() -> listener.onHoldFailed(record(), ack))
                .isInstanceOf(SagaConcurrentModificationException.class);

        verify(ack, never()).acknowledge();
        verify(compensation, never()).markFailed(any(), anyString());
    }

    @Test
    void anyOtherCompensationFailureMarksSagaFailedAndPagesOnCall() throws Exception {
        RuntimeException failure = new IllegalStateException("boom");
        doThrow(failure).when(compensation).compensate(event);

        listener.onHoldFailed(record(), ack);

        verify(compensation).markFailed(eq(transactionId), anyString());
        verify(alerts).pageOnCall(anyString(), eq(failure));
        verify(ack).acknowledge();
    }

    private ConsumerRecord<String, String> record() throws Exception {
        return new ConsumerRecord<>("fund.hold.failed", 0, 0L, transactionId.toString(),
                mapper.writeValueAsString(event));
    }
}
