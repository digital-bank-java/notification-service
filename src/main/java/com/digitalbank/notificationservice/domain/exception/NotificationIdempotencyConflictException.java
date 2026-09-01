package com.digitalbank.notificationservice.domain.exception;

public class NotificationIdempotencyConflictException extends RuntimeException {

    public NotificationIdempotencyConflictException(String message) {
        super(message);
    }
}
