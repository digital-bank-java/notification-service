package com.digitalbank.notificationservice.adapter.in.kafka;

public class InvalidTransferEventException extends RuntimeException {

    public InvalidTransferEventException(String message) {
        super(message);
    }

    public InvalidTransferEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
