package com.digitalbank.notificationservice.domain;

public enum NotificationDeliveryStatus {
    PENDING,
    DELIVERED,
    RETRYABLE_FAILURE,
    TERMINAL_FAILURE
}
