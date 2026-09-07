package com.digitalbank.notificationservice.application.payment;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentEventInbox(
        String eventId,
        String fingerprint,
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
        String status,
        Instant receivedAt) {

    public static PaymentEventInbox acceptedFrom(PaymentInstructionStateChangedEvent event) {
        return new PaymentEventInbox(
                event.eventId(),
                event.fingerprint(),
                event.eventType(),
                event.schemaVersion(),
                event.producer(),
                event.occurredAt(),
                event.aggregateId(),
                event.correlationId(),
                event.causationId(),
                event.instructionId(),
                event.idempotencyKey(),
                event.amount(),
                event.currency(),
                event.status().name(),
                Instant.now());
    }
}
