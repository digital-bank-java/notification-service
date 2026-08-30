package com.digitalbank.notificationservice.domain;

public enum DeliveryFailureReason {
    PROVIDER_UNAVAILABLE,
    RATE_LIMITED,
    TIMEOUT,
    INVALID_RECIPIENT,
    TEMPLATE_REJECTED,
    UNSUPPORTED_CHANNEL
}
