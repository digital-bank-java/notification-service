package com.digitalbank.notificationservice.application.payment;

public record PaymentEventConsumptionResult(String eventId, String correlationId, boolean replayed) {}
