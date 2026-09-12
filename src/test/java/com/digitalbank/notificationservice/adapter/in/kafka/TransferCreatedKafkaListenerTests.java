package com.digitalbank.notificationservice.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.digitalbank.notificationservice.application.event.TransferCreatedEventConsumer;
import com.digitalbank.notificationservice.application.event.TransferEventConsumptionResult;
import com.digitalbank.notificationservice.application.event.TransferEventQuarantine;
import com.digitalbank.notificationservice.application.event.TransferEventQuarantinePort;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

class TransferCreatedKafkaListenerTests {

    private static final String EVENT_ID = "00000000-0000-4000-8000-000000000001";
    private static final String TRANSFER_ID = "00000000-0000-4000-8000-000000000002";

    private final TransferCreatedKafkaListener listener =
            new TransferCreatedKafkaListener(new InMemoryConsumer(), Set.of("transaction-service"));

    @Test
    void mapsVersionedKafkaEnvelopeToTransportNeutralConsumer() {
        var result = listener.onMessage(record(EVENT_ID, "transfer-1", "request-1", validPayload()));

        assertThat(result).isEqualTo(new TransferEventConsumptionResult(EVENT_ID, "transfer-1", "request-1", false));
    }

    @Test
    void repeatedKafkaRecordIsReportedAsReplay() {
        var record = record(EVENT_ID, "transfer-1", "request-1", validPayload());

        listener.onMessage(record);
        assertThat(listener.onMessage(record).replayed()).isTrue();
    }

    @Test
    void untrustedProducerIsRejectedBeforeApplicationConsumption() {
        var record = record(EVENT_ID, "transfer-1", "request-1", validPayload());
        record.headers().remove("producer").add("producer", bytes("unknown-service"));

        assertThatThrownBy(() -> listener.onMessage(record)).isInstanceOf(InvalidTransferEventException.class);
    }

    @Test
    void missingRequiredMetadataIsRejectedAsTerminalInput() {
        var headers = new RecordHeaders()
                .add("event-id", bytes("evt-1"))
                .add("correlation-id", bytes("transfer-1"))
                .add("causation-id", bytes("request-1"))
                .add("producer", bytes("transaction-service"))
                .add("schema-version", bytes("1.0.0"));
        var record = new ConsumerRecord<String, String>(
                "events.transfer.created.v1",
                0,
                0L,
                0L,
                TimestampType.CREATE_TIME,
                -1,
                -1,
                "transfer-1",
                "{}",
                headers,
                Optional.empty());

        assertThatThrownBy(() -> listener.onMessage(record)).isInstanceOf(InvalidTransferEventException.class);
    }

    @Test
    void invalidRecordIsSentToQuarantineWithoutApplicationConsumption() {
        var quarantine = new InMemoryQuarantine();
        var listener =
                new TransferCreatedKafkaListener(new InMemoryConsumer(), Set.of("transaction-service"), quarantine);
        var record = record(EVENT_ID, "transfer-1", "request-1", "");

        assertThatThrownBy(() -> listener.onMessage(record)).isInstanceOf(InvalidTransferEventException.class);
        assertThat(quarantine.records()).singleElement().satisfies(entry -> {
            assertThat(entry.eventId()).isEqualTo(EVENT_ID);
            assertThat(entry.topic()).isEqualTo("events.transfer.created.v1");
            assertThat(entry.reason()).contains("payload");
        });
    }

    @Test
    void oversizedHeaderIsRejectedAndQuarantinedWithBoundedMetadata() {
        var quarantine = new InMemoryQuarantine();
        var listener =
                new TransferCreatedKafkaListener(new InMemoryConsumer(), Set.of("transaction-service"), quarantine);
        var record = record(EVENT_ID, "c".repeat(201), "request-1", validPayload());

        assertThatThrownBy(() -> listener.onMessage(record)).isInstanceOf(InvalidTransferEventException.class);
        assertThat(quarantine.records()).singleElement().satisfies(entry -> {
            assertThat(entry.eventId()).isEqualTo(EVENT_ID);
            assertThat(entry.correlationId()).isNull();
            assertThat(entry.reason()).contains("correlation-id must be at most 200 characters");
        });
    }

    @Test
    void payloadIdentityMismatchIsQuarantinedBeforeApplicationConsumption() {
        var consumer = new InMemoryConsumer();
        var quarantine = new InMemoryQuarantine();
        var listener = new TransferCreatedKafkaListener(consumer, Set.of("transaction-service"), quarantine);
        var mismatchedPayload = validPayload().replace(EVENT_ID, "00000000-0000-4000-8000-000000000099");

        assertThatThrownBy(() -> listener.onMessage(record(EVENT_ID, "transfer-1", "request-1", mismatchedPayload)))
                .isInstanceOf(InvalidTransferEventException.class);
        assertThat(consumer.events()).isEmpty();
        assertThat(quarantine.records()).singleElement().satisfies(entry -> {
            assertThat(entry.eventId()).isEqualTo(EVENT_ID);
            assertThat(entry.reason()).contains("eventId");
        });
    }

    @Test
    void malformedTransferPayloadIsQuarantinedBeforeApplicationConsumption() {
        var consumer = new InMemoryConsumer();
        var quarantine = new InMemoryQuarantine();
        var listener = new TransferCreatedKafkaListener(consumer, Set.of("transaction-service"), quarantine);

        assertThatThrownBy(() -> listener.onMessage(record(EVENT_ID, "transfer-1", "request-1", "not-json")))
                .isInstanceOf(InvalidTransferEventException.class);
        assertThat(consumer.events()).isEmpty();
        assertThat(quarantine.records()).singleElement().satisfies(entry -> {
            assertThat(entry.eventId()).isEqualTo(EVENT_ID);
            assertThat(entry.reason()).contains("valid JSON");
        });
    }

    private ConsumerRecord<String, String> record(
            String eventId, String correlationId, String causationId, String payload) {
        var headers = new RecordHeaders()
                .add("event-id", bytes(eventId))
                .add("correlation-id", bytes(correlationId))
                .add("causation-id", bytes(causationId))
                .add("producer", bytes("transaction-service"))
                .add("schema-version", bytes("1.0.0"))
                .add("occurred-at", bytes("2026-08-31T10:15:30Z"));
        return new ConsumerRecord<>(
                "events.transfer.created.v1",
                0,
                0L,
                0L,
                TimestampType.CREATE_TIME,
                -1,
                -1,
                TRANSFER_ID,
                payload,
                headers,
                Optional.empty());
    }

    private String validPayload() {
        return """
                {
                  "eventId":"%s",
                  "eventType":"TransferCreated.v1",
                  "schemaVersion":"1.0.0",
                  "producer":"transaction-service",
                  "occurredAt":"2026-08-31T10:15:30Z",
                  "aggregateId":"%s",
                  "correlationId":"transfer-1",
                  "causationId":"request-1",
                  "transactionId":"%s",
                  "sourceAccountId":"00000000-0000-4000-8000-000000000003",
                  "destinationAccountId":"00000000-0000-4000-8000-000000000004",
                  "amount":"125.5000",
                  "currency":"AED",
                  "transferRequestId":"transfer-request-1",
                  "reservationRequestId":"reservation-request-1",
                  "postingRequestId":"posting-request-1",
                  "status":"PENDING"
                }
                """.formatted(EVENT_ID, TRANSFER_ID, TRANSFER_ID);
    }

    private byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static class InMemoryConsumer implements TransferCreatedEventConsumer {

        private final Set<String> consumed = new java.util.HashSet<>();

        private final List<com.digitalbank.notificationservice.application.event.TransferCreatedEvent> events =
                new ArrayList<>();

        @Override
        public TransferEventConsumptionResult consume(
                com.digitalbank.notificationservice.application.event.TransferCreatedEvent event) {
            var replayed = !consumed.add(event.eventId());
            events.add(event);
            return new TransferEventConsumptionResult(
                    event.eventId(), event.correlationId(), event.causationId(), replayed);
        }

        List<com.digitalbank.notificationservice.application.event.TransferCreatedEvent> events() {
            return events;
        }
    }

    private static class InMemoryQuarantine implements TransferEventQuarantinePort {

        private final List<TransferEventQuarantine> records = new ArrayList<>();

        @Override
        public void quarantine(TransferEventQuarantine record) {
            records.add(record);
        }

        List<TransferEventQuarantine> records() {
            return records;
        }
    }
}
