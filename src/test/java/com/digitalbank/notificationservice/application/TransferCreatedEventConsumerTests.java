package com.digitalbank.notificationservice.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.digitalbank.notificationservice.application.event.TransferCreatedEvent;
import com.digitalbank.notificationservice.application.event.TransferCreatedEventConsumerService;
import com.digitalbank.notificationservice.application.event.TransferEventConflictException;
import com.digitalbank.notificationservice.application.event.TransferEventConsumptionResult;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class TransferCreatedEventConsumerTests {

    private final TransferCreatedEventConsumerService consumer = new TransferCreatedEventConsumerService();

    @Test
    void acceptsTransferCreatedEventAndPreservesWorkflowMetadata() {
        var result = consumer.consume(event("evt-1", "transfer-1", "request-1", "{\"transferId\":\"transfer-1\"}"));

        assertThat(result.eventId()).isEqualTo("evt-1");
        assertThat(result.correlationId()).isEqualTo("transfer-1");
        assertThat(result.causationId()).isEqualTo("request-1");
        assertThat(result.replayed()).isFalse();
    }

    @Test
    void identicalEventReplayIsDeterministic() {
        var event = event("evt-1", "transfer-1", "request-1", "{\"transferId\":\"transfer-1\"}");

        var first = consumer.consume(event);
        var replay = consumer.consume(event);

        assertThat(replay)
                .isEqualTo(new TransferEventConsumptionResult(
                        first.eventId(), first.correlationId(), first.causationId(), true));
    }

    @Test
    void reusedEventIdWithChangedPayloadIsRejected() {
        consumer.consume(event("evt-1", "transfer-1", "request-1", "{\"transferId\":\"transfer-1\"}"));

        assertThatThrownBy(() ->
                        consumer.consume(event("evt-1", "transfer-1", "request-1", "{\"transferId\":\"transfer-2\"}")))
                .isInstanceOf(TransferEventConflictException.class);
    }

    @Test
    void samePayloadWithDifferentCorrelationIsRejected() {
        consumer.consume(event("evt-1", "transfer-1", "request-1", "{\"transferId\":\"transfer-1\"}"));

        assertThatThrownBy(() ->
                        consumer.consume(event("evt-1", "transfer-2", "request-1", "{\"transferId\":\"transfer-1\"}")))
                .isInstanceOf(TransferEventConflictException.class);
    }

    private TransferCreatedEvent event(String eventId, String correlationId, String causationId, String payload) {
        return new TransferCreatedEvent(
                eventId,
                correlationId,
                causationId,
                "transaction-service",
                "1",
                Instant.parse("2026-08-31T10:15:30Z"),
                payload);
    }
}
