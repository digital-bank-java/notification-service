package com.digitalbank.notificationservice.application.payment;

public interface PaymentInstructionStateEventConsumer {

    PaymentEventConsumptionResult consume(PaymentInstructionStateChangedEvent event);

    default PaymentEventConsumptionResult consume(
            PaymentInstructionStateChangedEvent event, PaymentEventSource source) {
        return consume(event);
    }
}
