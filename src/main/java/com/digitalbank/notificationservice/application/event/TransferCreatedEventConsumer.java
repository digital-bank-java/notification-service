package com.digitalbank.notificationservice.application.event;

public interface TransferCreatedEventConsumer {

    TransferEventConsumptionResult consume(TransferCreatedEvent event);

    default TransferEventConsumptionResult consume(TransferCreatedEvent event, TransferEventSource source) {
        return consume(event);
    }
}
