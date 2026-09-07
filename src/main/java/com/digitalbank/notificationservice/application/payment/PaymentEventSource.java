package com.digitalbank.notificationservice.application.payment;

public record PaymentEventSource(String topic, Integer partition, Long offset) {}
