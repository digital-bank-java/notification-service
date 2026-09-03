package com.digitalbank.notificationservice.application.event;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;

public record TransferCreatedEvent(
        String eventId,
        String correlationId,
        String causationId,
        String producer,
        String schemaVersion,
        Instant occurredAt,
        String payload) {

    public TransferCreatedEvent {
        eventId = requireText(eventId, "eventId");
        correlationId = requireText(correlationId, "correlationId");
        causationId = requireText(causationId, "causationId");
        producer = requireText(producer, "producer");
        schemaVersion = requireText(schemaVersion, "schemaVersion");
        occurredAt = Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        payload = requireText(payload, "payload");
    }

    public String fingerprint() {
        var canonical = String.join(
                "\u001f", eventId, correlationId, causationId, producer, schemaVersion, occurredAt.toString(), payload);
        try {
            return HexFormat.of()
                    .formatHex(MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is not available", exception);
        }
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value.trim();
    }
}
