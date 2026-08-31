package com.digitalbank.notificationservice.application.event;

public final class TransferEventMetadataLimits {

    public static final int EVENT_ID = 200;
    public static final int CORRELATION_ID = 200;
    public static final int CAUSATION_ID = 200;
    public static final int PRODUCER = 100;
    public static final int SCHEMA_VERSION = 32;
    public static final int OCCURRED_AT = 64;
    public static final int TOPIC = 250;
    public static final int REASON = 500;

    private TransferEventMetadataLimits() {}

    public static int forHeader(String name) {
        return switch (name) {
            case "event-id" -> EVENT_ID;
            case "correlation-id" -> CORRELATION_ID;
            case "causation-id" -> CAUSATION_ID;
            case "producer" -> PRODUCER;
            case "schema-version" -> SCHEMA_VERSION;
            case "occurred-at" -> OCCURRED_AT;
            default -> throw new IllegalArgumentException("unknown transfer event header: " + name);
        };
    }

    public static String bounded(String value, int maxLength) {
        return value == null || value.length() > maxLength ? null : value;
    }
}
