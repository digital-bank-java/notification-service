package com.digitalbank.notificationservice.application.event;

public class TransferEventConflictException extends RuntimeException {

    public TransferEventConflictException(String message) {
        super(message);
    }
}
