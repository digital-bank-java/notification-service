package com.digitalbank.notificationservice.application.payment;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentNotificationWork(
        String eventId,
        String instructionId,
        String correlationId,
        String causationId,
        BigDecimal amount,
        String currency,
        String workType,
        String status,
        Instant createdAt) {

    public static PaymentNotificationWork from(PaymentInstructionStateChangedEvent event) {
        return new PaymentNotificationWork(
                event.eventId(),
                event.instructionId(),
                event.correlationId(),
                event.causationId(),
                event.amount(),
                event.currency(),
                "PAYMENT_INSTRUCTION_STATE_CHANGED",
                event.status().name(),
                Instant.now());
    }
}
