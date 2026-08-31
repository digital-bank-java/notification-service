package com.digitalbank.notificationservice.application.event;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

public record TransferEventQuarantine(
        String quarantineKey,
        String eventId,
        String fingerprint,
        String correlationId,
        String causationId,
        String producer,
        String schemaVersion,
        String topic,
        Integer partition,
        Long offset,
        String reason,
        Instant quarantinedAt) {

    public static TransferEventQuarantine fromEvent(TransferCreatedEvent event, String reason) {
        return create(
                event.eventId(),
                event.fingerprint(),
                event.correlationId(),
                event.causationId(),
                event.producer(),
                event.schemaVersion(),
                null,
                null,
                null,
                reason);
    }

    public static TransferEventQuarantine fromKafkaRecord(ConsumerRecord<String, String> record, String reason) {
        var fingerprint = digest(record.value() == null ? "" : record.value());
        return create(
                header(record, "event-id"),
                fingerprint,
                header(record, "correlation-id"),
                header(record, "causation-id"),
                header(record, "producer"),
                header(record, "schema-version"),
                record.topic(),
                record.partition(),
                record.offset(),
                reason);
    }

    private static TransferEventQuarantine create(
            String eventId,
            String fingerprint,
            String correlationId,
            String causationId,
            String producer,
            String schemaVersion,
            String topic,
            Integer partition,
            Long offset,
            String reason) {
        var quarantinedAt = Instant.now();
        var key = digest(String.join(
                "\u001f",
                String.valueOf(eventId),
                String.valueOf(fingerprint),
                String.valueOf(topic),
                String.valueOf(partition),
                String.valueOf(offset),
                reason));
        return new TransferEventQuarantine(
                key,
                eventId,
                fingerprint,
                correlationId,
                causationId,
                producer,
                schemaVersion,
                topic,
                partition,
                offset,
                reason,
                quarantinedAt);
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value() == null
                ? null
                : new String(header.value(), StandardCharsets.UTF_8).trim();
    }

    private static String digest(String value) {
        try {
            return HexFormat.of()
                    .formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is not available", exception);
        }
    }
}
