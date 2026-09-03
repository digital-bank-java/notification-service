package com.digitalbank.notificationservice.domain;

import com.digitalbank.notificationservice.domain.exception.NotificationDeliveryStateConflictException;
import java.util.UUID;

public final class NotificationDelivery {

    private final UUID deliveryId;
    private final String idempotencyKey;
    private final String correlationId;
    private final String fingerprint;
    private NotificationDeliveryStatus status;
    private int attemptCount;

    private NotificationDelivery(UUID deliveryId, String idempotencyKey, String correlationId, String fingerprint) {
        this.deliveryId = deliveryId;
        this.idempotencyKey = idempotencyKey;
        this.correlationId = correlationId;
        this.fingerprint = fingerprint;
        this.status = NotificationDeliveryStatus.PENDING;
    }

    public static NotificationDelivery pending(
            UUID deliveryId, String idempotencyKey, String correlationId, String fingerprint) {
        return new NotificationDelivery(deliveryId, idempotencyKey, correlationId, fingerprint);
    }

    public void record(DeliveryOutcome outcome) {
        if (status == NotificationDeliveryStatus.DELIVERED || status == NotificationDeliveryStatus.TERMINAL_FAILURE) {
            throw new NotificationDeliveryStateConflictException(
                    "Delivery " + deliveryId + " already has a terminal outcome");
        }

        attemptCount++;
        status = switch (outcome) {
            case DELIVERED -> NotificationDeliveryStatus.DELIVERED;
            case RETRYABLE_FAILURE -> NotificationDeliveryStatus.RETRYABLE_FAILURE;
            case TERMINAL_FAILURE -> NotificationDeliveryStatus.TERMINAL_FAILURE;
        };
    }

    public boolean hasFingerprint(String candidateFingerprint) {
        return fingerprint.equals(candidateFingerprint);
    }

    public UUID deliveryId() {
        return deliveryId;
    }

    public String idempotencyKey() {
        return idempotencyKey;
    }

    public String correlationId() {
        return correlationId;
    }

    public NotificationDeliveryStatus status() {
        return status;
    }

    public int attemptCount() {
        return attemptCount;
    }
}
