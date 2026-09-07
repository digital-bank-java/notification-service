package com.digitalbank.notificationservice.application.payment;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

public record PaymentEventQuarantine(
        String quarantineKey,
        String eventId,
        String fingerprint,
        String eventType,
        String schemaVersion,
        String producer,
        String aggregateId,
        String instructionId,
        String correlationId,
        String causationId,
        BigDecimal amount,
        String currency,
        String status,
        String topic,
        Integer partition,
        Long offset,
        String reason,
        Instant quarantinedAt) {

    public static PaymentEventQuarantine fromEvent(PaymentInstructionStateChangedEvent event, String reason) {
        return fromEvent(event, reason, null);
    }

    public static PaymentEventQuarantine fromEvent(
            PaymentInstructionStateChangedEvent event, String reason, PaymentEventSource source) {
        var topic = source == null ? null : source.topic();
        var partition = source == null ? null : source.partition();
        var offset = source == null ? null : source.offset();
        var quarantinedAt = Instant.now();
        var key = digest(String.join(
                "\u001f",
                event.eventId(),
                event.fingerprint(),
                String.valueOf(topic),
                String.valueOf(partition),
                String.valueOf(offset),
                String.valueOf(reason)));
        return new PaymentEventQuarantine(
                key,
                event.eventId(),
                event.fingerprint(),
                event.eventType(),
                event.schemaVersion(),
                event.producer(),
                event.aggregateId(),
                event.instructionId(),
                event.correlationId(),
                event.causationId(),
                event.amount(),
                event.currency(),
                event.status().name(),
                topic,
                partition,
                offset,
                boundedReason(reason),
                quarantinedAt);
    }

    public static PaymentEventQuarantine fromKafkaRecord(ConsumerRecord<String, String> record, String reason) {
        var fingerprint = digest(record.value() == null ? "" : record.value());
        var eventId = bounded(header(record, "event-id"), 200);
        var topic = bounded(record.topic(), 250);
        var partition = record.partition();
        var offset = record.offset();
        var quarantinedAt = Instant.now();
        var key = digest(String.join(
                "\u001f",
                String.valueOf(eventId),
                fingerprint,
                String.valueOf(topic),
                String.valueOf(partition),
                String.valueOf(offset),
                String.valueOf(reason)));
        return new PaymentEventQuarantine(
                key,
                eventId,
                fingerprint,
                null,
                bounded(header(record, "schema-version"), 32),
                bounded(header(record, "producer"), 100),
                null,
                null,
                bounded(header(record, "correlation-id"), 200),
                bounded(header(record, "causation-id"), 200),
                null,
                null,
                null,
                topic,
                partition,
                offset,
                boundedReason(reason),
                quarantinedAt);
    }

    private static String boundedReason(String reason) {
        if (reason == null || reason.isBlank()) {
            return "unknown quarantine reason";
        }
        return reason.length() <= 500 ? reason : reason.substring(0, 500);
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value() == null
                ? null
                : new String(header.value(), StandardCharsets.UTF_8).trim();
    }

    private static String bounded(String value, int maxLength) {
        return value == null || value.length() > maxLength ? null : value;
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
