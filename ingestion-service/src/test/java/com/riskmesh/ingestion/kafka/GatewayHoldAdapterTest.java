package com.riskmesh.ingestion.kafka;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.riskmesh.common.events.FundHoldRequestedEvent;
import com.riskmesh.ingestion.gateway.HoldResult;
import com.riskmesh.ingestion.gateway.MockGatewayClient;
import com.riskmesh.ingestion.gateway.exception.GatewayRejectedException;
import com.riskmesh.ingestion.gateway.exception.GatewayUnavailableException;
import com.riskmesh.ingestion.service.SagaTransitionService;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

class GatewayHoldAdapterTest {

    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    private final SagaTransitionService transitions = mock(SagaTransitionService.class);
    private final MockGatewayClient gateway = mock(MockGatewayClient.class);
    private final Acknowledgment ack = mock(Acknowledgment.class);
    private final GatewayHoldAdapter adapter =
            new GatewayHoldAdapter(new KafkaEventReader(mapper), transitions, gateway);

    private final UUID transactionId = UUID.randomUUID();
    private final FundHoldRequestedEvent event = new FundHoldRequestedEvent(
            UUID.randomUUID().toString(), "FUND_HOLD_REQUESTED", "1.0", Instant.parse("2026-01-01T00:00:00Z"),
            transactionId, new BigDecimal("1200.00"), "INR");

    @Test
    void confirmedHoldIsRecordedAndAcknowledged() throws Exception {
        when(transitions.beginHold(transactionId)).thenReturn(true);
        when(gateway.placeHold(transactionId, event.amount(), "INR")).thenReturn(new HoldResult("hold-1"));

        adapter.onHoldRequested(record(), ack);

        verify(transitions).recordHoldConfirmed(event, "hold-1");
        verify(ack).acknowledge();
    }

    @Test
    void rejectedHoldIsRecordedAsRejectionAndAcknowledged() throws Exception {
        when(transitions.beginHold(transactionId)).thenReturn(true);
        when(gateway.placeHold(any(), any(), any()))
                .thenThrow(new GatewayRejectedException("Gateway rejected the hold (HTTP 422)", null));

        adapter.onHoldRequested(record(), ack);

        verify(transitions).recordHoldRejected(event, "Gateway rejected the hold (HTTP 422)");
        verify(transitions, never()).recordHoldConfirmed(any(), any());
        verify(ack).acknowledge();
    }

    @Test
    void redeliveryForAnAlreadyAdvancedSagaIsIgnoredWithoutCallingTheGateway() throws Exception {
        when(transitions.beginHold(transactionId)).thenReturn(false);

        adapter.onHoldRequested(record(), ack);

        verifyNoInteractions(gateway);
        verify(ack).acknowledge();
    }

    @Test
    void technicalGatewayFailurePropagatesWithoutAcknowledging(){
        when(transitions.beginHold(transactionId)).thenReturn(true);
        when(gateway.placeHold(any(), any(), any())).thenThrow(new GatewayUnavailableException("down", null));

        assertThatThrownBy(() -> adapter.onHoldRequested(record(), ack))
                .isInstanceOf(GatewayUnavailableException.class);

        verify(ack, never()).acknowledge();
        verify(transitions, never()).recordHoldConfirmed(any(), any());
        verify(transitions, never()).recordHoldRejected(any(), any());
    }

    @Test
    void malformedPayloadIsANonRetryableDeserializationFailure() {
        ConsumerRecord<String, String> bad = new ConsumerRecord<>("fund.hold.requested", 0, 0L, "k", "{not json");

        assertThatThrownBy(() -> adapter.onHoldRequested(bad, ack))
                .isInstanceOf(EventDeserializationException.class);

        verifyNoInteractions(transitions);
        verifyNoInteractions(gateway);
        verify(ack, never()).acknowledge();
    }

    private ConsumerRecord<String, String> record() throws Exception {
        return new ConsumerRecord<>("fund.hold.requested", 0, 0L, transactionId.toString(),
                mapper.writeValueAsString(event));
    }

}
