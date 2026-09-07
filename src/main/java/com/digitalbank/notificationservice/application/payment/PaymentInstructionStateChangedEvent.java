package com.digitalbank.notificationservice.application.payment;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;

public record PaymentInstructionStateChangedEvent(
        String eventId,
        String eventType,
        String schemaVersion,
        String producer,
        Instant occurredAt,
        String aggregateId,
        String correlationId,
        String causationId,
        String instructionId,
        String idempotencyKey,
        BigDecimal amount,
        String currency,
        PaymentInstructionState status,
        String failureReason,
        String payload) {

    public PaymentInstructionStateChangedEvent {
        eventId = requireText(eventId, "eventId");
        eventType = requireText(eventType, "eventType");
        schemaVersion = requireText(schemaVersion, "schemaVersion");
        producer = requireText(producer, "producer");
        occurredAt = Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        aggregateId = requireText(aggregateId, "aggregateId");
        correlationId = requireText(correlationId, "correlationId");
        causationId = requireText(causationId, "causationId");
        instructionId = requireText(instructionId, "instructionId");
        idempotencyKey = requireText(idempotencyKey, "idempotencyKey");
        amount = Objects.requireNonNull(amount, "amount must not be null");
        currency = requireText(currency, "currency");
        status = Objects.requireNonNull(status, "status must not be null");
        if (status == PaymentInstructionState.FAILED) {
            failureReason = requireText(failureReason, "failureReason");
        } else if (failureReason != null) {
            throw new IllegalArgumentException("failureReason must be null unless status is FAILED");
        }
        payload = requireText(payload, "payload");
    }

    public String fingerprint() {
        var canonical = String.join(
                "\u001f",
                eventId,
                eventType,
                schemaVersion,
                producer,
                occurredAt.toString(),
                aggregateId,
                correlationId,
                causationId,
                instructionId,
                idempotencyKey,
                amount.toPlainString(),
                currency,
                status.name(),
                String.valueOf(failureReason),
                payload);
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
