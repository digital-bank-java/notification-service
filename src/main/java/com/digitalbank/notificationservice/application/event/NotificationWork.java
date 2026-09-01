package com.digitalbank.notificationservice.application.event;

import java.time.Instant;

public record NotificationWork(
        String eventId, String correlationId, String causationId, String workType, String status, Instant createdAt) {

    public static NotificationWork from(TransferCreatedEvent event) {
        return new NotificationWork(
                event.eventId(),
                event.correlationId(),
                event.causationId(),
                "TRANSFER_CREATED",
                "PENDING",
                Instant.now());
    }
}
