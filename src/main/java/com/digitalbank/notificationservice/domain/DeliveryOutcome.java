package com.digitalbank.notificationservice.domain;

public enum DeliveryOutcome {
    DELIVERED,
    RETRYABLE_FAILURE,
    TERMINAL_FAILURE;

    public static DeliveryOutcome from(DeliveryFailureReason reason) {
        return switch (reason) {
            case PROVIDER_UNAVAILABLE, RATE_LIMITED, TIMEOUT -> RETRYABLE_FAILURE;
            case INVALID_RECIPIENT, TEMPLATE_REJECTED, UNSUPPORTED_CHANNEL -> TERMINAL_FAILURE;
        };
    }
}
