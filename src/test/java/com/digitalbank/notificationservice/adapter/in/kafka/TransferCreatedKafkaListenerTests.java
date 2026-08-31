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

    private final TransferCreatedKafkaListener listener =
            new TransferCreatedKafkaListener(new InMemoryConsumer(), Set.of("transaction-service"));

    @Test
    void mapsVersionedKafkaEnvelopeToTransportNeutralConsumer() {
        var result = listener.onMessage(record("evt-1", "transfer-1", "request-1", "{\"transferId\":\"transfer-1\"}"));

        assertThat(result).isEqualTo(new TransferEventConsumptionResult("evt-1", "transfer-1", "request-1", false));
    }

    @Test
    void repeatedKafkaRecordIsReportedAsReplay() {
        var record = record("evt-1", "transfer-1", "request-1", "{\"transferId\":\"transfer-1\"}");

        listener.onMessage(record);
        assertThat(listener.onMessage(record).replayed()).isTrue();
    }

    @Test
    void untrustedProducerIsRejectedBeforeApplicationConsumption() {
        var record = record("evt-1", "transfer-1", "request-1", "{\"transferId\":\"transfer-1\"}");
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
        var record = record("evt-1", "transfer-1", "request-1", "");

        assertThatThrownBy(() -> listener.onMessage(record)).isInstanceOf(InvalidTransferEventException.class);
        assertThat(quarantine.records()).singleElement().satisfies(entry -> {
            assertThat(entry.eventId()).isEqualTo("evt-1");
            assertThat(entry.topic()).isEqualTo("events.transfer.created.v1");
            assertThat(entry.reason()).contains("payload");
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
                correlationId,
                payload,
                headers,
                Optional.empty());
    }

    private byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static class InMemoryConsumer implements TransferCreatedEventConsumer {

        private final Set<String> consumed = new java.util.HashSet<>();

        @Override
        public TransferEventConsumptionResult consume(
                com.digitalbank.notificationservice.application.event.TransferCreatedEvent event) {
            var replayed = !consumed.add(event.eventId());
            return new TransferEventConsumptionResult(
                    event.eventId(), event.correlationId(), event.causationId(), replayed);
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
