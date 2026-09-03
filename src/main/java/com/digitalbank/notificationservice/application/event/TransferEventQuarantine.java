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
        return fromEvent(event, reason, null);
    }

    public static TransferEventQuarantine fromEvent(
            TransferCreatedEvent event, String reason, TransferEventSource source) {
        return create(
                event.eventId(),
                event.fingerprint(),
                event.correlationId(),
                event.causationId(),
                event.producer(),
                event.schemaVersion(),
                source == null ? null : source.topic(),
                source == null ? null : source.partition(),
                source == null ? null : source.offset(),
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
                TransferEventMetadataLimits.bounded(record.topic(), TransferEventMetadataLimits.TOPIC),
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
                String.valueOf(reason)));
        return new TransferEventQuarantine(
                key,
                TransferEventMetadataLimits.bounded(eventId, TransferEventMetadataLimits.EVENT_ID),
                fingerprint,
                TransferEventMetadataLimits.bounded(correlationId, TransferEventMetadataLimits.CORRELATION_ID),
                TransferEventMetadataLimits.bounded(causationId, TransferEventMetadataLimits.CAUSATION_ID),
                TransferEventMetadataLimits.bounded(producer, TransferEventMetadataLimits.PRODUCER),
                TransferEventMetadataLimits.bounded(schemaVersion, TransferEventMetadataLimits.SCHEMA_VERSION),
                TransferEventMetadataLimits.bounded(topic, TransferEventMetadataLimits.TOPIC),
                partition,
                offset,
                boundedReason(reason),
                quarantinedAt);
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value() == null
                ? null
                : TransferEventMetadataLimits.bounded(
                        new String(header.value(), StandardCharsets.UTF_8).trim(),
                        TransferEventMetadataLimits.forHeader(name));
    }

    private static String boundedReason(String reason) {
        if (reason == null) {
            return "unknown quarantine reason";
        }
        return reason.length() <= TransferEventMetadataLimits.REASON
                ? reason
                : reason.substring(0, TransferEventMetadataLimits.REASON);
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
