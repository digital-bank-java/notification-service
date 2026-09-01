package com.digitalbank.notificationservice.application.event;

public interface TransferCreatedEventConsumer {

    TransferEventConsumptionResult consume(TransferCreatedEvent event);
}
