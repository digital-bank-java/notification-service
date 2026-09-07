package com.digitalbank.notificationservice.application.payment;

public class PaymentEventConflictException extends RuntimeException {

    public PaymentEventConflictException(String message) {
        super(message);
    }
}
