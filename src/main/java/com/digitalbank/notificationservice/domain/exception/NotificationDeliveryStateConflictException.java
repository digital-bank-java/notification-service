package com.digitalbank.notificationservice.domain.exception;

public class NotificationDeliveryStateConflictException extends RuntimeException {

    public NotificationDeliveryStateConflictException(String message) {
        super(message);
    }
}
