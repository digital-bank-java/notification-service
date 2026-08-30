package com.digitalbank.notificationservice.domain.exception;

public class NotificationDeliveryNotFoundException extends RuntimeException {

    public NotificationDeliveryNotFoundException(String message) {
        super(message);
    }
}
