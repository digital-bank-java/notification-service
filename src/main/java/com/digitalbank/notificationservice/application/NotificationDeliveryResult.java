package com.digitalbank.notificationservice.application;

import com.digitalbank.notificationservice.domain.NotificationDeliveryStatus;
import java.util.UUID;

public record NotificationDeliveryResult(
        UUID deliveryId, String correlationId, NotificationDeliveryStatus status, int attemptCount, boolean replayed) {}
