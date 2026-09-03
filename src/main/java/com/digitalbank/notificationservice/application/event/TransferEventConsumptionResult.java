package com.digitalbank.notificationservice.application.event;

public record TransferEventConsumptionResult(
        String eventId, String correlationId, String causationId, boolean replayed) {}
