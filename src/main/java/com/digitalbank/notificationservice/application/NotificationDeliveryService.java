package com.digitalbank.notificationservice.application;

import com.digitalbank.notificationservice.domain.DeliveryOutcome;
import com.digitalbank.notificationservice.domain.NotificationDelivery;
import com.digitalbank.notificationservice.domain.exception.NotificationDeliveryNotFoundException;
import com.digitalbank.notificationservice.domain.exception.NotificationIdempotencyConflictException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class NotificationDeliveryService implements NotificationDeliveryInputPort {

    private final ConcurrentMap<String, NotificationDelivery> deliveries = new ConcurrentHashMap<>();

    @Override
    public NotificationDeliveryResult requestDelivery(NotificationDeliveryCommand command) {
        var replayed = new AtomicBoolean(false);
        var delivery = deliveries.compute(command.idempotencyKey(), (key, existing) -> {
            if (existing == null) {
                return NotificationDelivery.pending(
                        UUID.randomUUID(), command.idempotencyKey(), command.correlationId(), command.fingerprint());
            }
            if (!existing.hasFingerprint(command.fingerprint())) {
                throw new NotificationIdempotencyConflictException(
                        "Idempotency key is already associated with a different notification request");
            }
            replayed.set(true);
            return existing;
        });
        return result(delivery, replayed.get());
    }

    @Override
    public NotificationDeliveryResult recordOutcome(String idempotencyKey, DeliveryOutcome outcome) {
        if (outcome == null) {
            throw new IllegalArgumentException("outcome must not be null");
        }
        var delivery = deliveries.computeIfPresent(idempotencyKey, (key, existing) -> {
            existing.record(outcome);
            return existing;
        });
        if (delivery == null) {
            throw new NotificationDeliveryNotFoundException(
                    "No notification delivery exists for idempotency key " + idempotencyKey);
        }
        return result(delivery, false);
    }

    private NotificationDeliveryResult result(NotificationDelivery delivery, boolean replayed) {
        return new NotificationDeliveryResult(
                delivery.deliveryId(), delivery.correlationId(), delivery.status(), delivery.attemptCount(), replayed);
    }
}
