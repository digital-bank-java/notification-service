package com.digitalbank.notificationservice.application;

import com.digitalbank.notificationservice.domain.NotificationChannel;

public record NotificationDeliveryCommand(
        String idempotencyKey, String correlationId, NotificationChannel channel, String recipient, String templateId) {

    public NotificationDeliveryCommand {
        idempotencyKey = requireText(idempotencyKey, "idempotencyKey");
        correlationId = requireText(correlationId, "correlationId");
        channel = requireChannel(channel);
        recipient = requireText(recipient, "recipient");
        templateId = requireText(templateId, "templateId");
    }

    public String fingerprint() {
        return String.join(
                "|",
                fingerprintPart(idempotencyKey),
                fingerprintPart(correlationId),
                fingerprintPart(channel.name()),
                fingerprintPart(recipient),
                fingerprintPart(templateId));
    }

    private static String fingerprintPart(String value) {
        return value.length() + ":" + value;
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }

    private static NotificationChannel requireChannel(NotificationChannel value) {
        if (value == null) {
            throw new IllegalArgumentException("channel must not be null");
        }
        return value;
    }
}
