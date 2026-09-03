package com.digitalbank.notificationservice.application.event;

public record TransferEventSource(String topic, Integer partition, Long offset) {}
