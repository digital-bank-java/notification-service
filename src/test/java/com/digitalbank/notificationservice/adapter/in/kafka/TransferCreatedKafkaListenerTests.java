package com.digitalbank.notificationservice.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.digitalbank.notificationservice.application.event.TransferCreatedEventConsumerService;
import com.digitalbank.notificationservice.application.event.TransferEventConsumptionResult;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

class TransferCreatedKafkaListenerTests {

    private final TransferCreatedKafkaListener listener =
            new TransferCreatedKafkaListener(new TransferCreatedEventConsumerService(), Set.of("transaction-service"));

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
                .add("schema-version", bytes("1"));
        var record = new ConsumerRecord<String, String>(
                "transfer.created.v1",
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

    private ConsumerRecord<String, String> record(
            String eventId, String correlationId, String causationId, String payload) {
        var headers = new RecordHeaders()
                .add("event-id", bytes(eventId))
                .add("correlation-id", bytes(correlationId))
                .add("causation-id", bytes(causationId))
                .add("producer", bytes("transaction-service"))
                .add("schema-version", bytes("1"))
                .add("occurred-at", bytes("2026-08-31T10:15:30Z"));
        return new ConsumerRecord<>(
                "transfer.created.v1",
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
}
