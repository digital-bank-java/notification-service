package com.digitalbank.notificationservice.application.event;

import java.time.Instant;

public record TransferEventInbox(
        String eventId,
        String fingerprint,
        String correlationId,
        String causationId,
        String producer,
        String schemaVersion,
        Instant occurredAt,
        String status,
        Instant receivedAt) {

    public static TransferEventInbox acceptedFrom(TransferCreatedEvent event) {
        return new TransferEventInbox(
                event.eventId(),
                event.fingerprint(),
                event.correlationId(),
                event.causationId(),
                event.producer(),
                event.schemaVersion(),
                event.occurredAt(),
                "ACCEPTED",
                Instant.now());
    }
}
