package com.digitalbank.notificationservice.adapter.in.kafka;

import com.digitalbank.notificationservice.application.event.TransferCreatedEvent;
import com.digitalbank.notificationservice.application.event.TransferCreatedEventConsumer;
import com.digitalbank.notificationservice.application.event.TransferEventConsumptionResult;
import java.time.Instant;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "notification.events.transfer-created.enabled", havingValue = "true")
public class TransferCreatedKafkaListener {

    private static final String SUPPORTED_SCHEMA_VERSION = "1.0.0";
    private static final String EVENT_ID = "event-id";
    private static final String CORRELATION_ID = "correlation-id";
    private static final String CAUSATION_ID = "causation-id";
    private static final String PRODUCER = "producer";
    private static final String SCHEMA_VERSION = "schema-version";
    private static final String OCCURRED_AT = "occurred-at";

    private final TransferCreatedEventConsumer consumer;
    private final Set<String> allowedProducers;

    @Autowired
    public TransferCreatedKafkaListener(
            TransferCreatedEventConsumer consumer,
            @Value("${notification.events.allowed-producers:transaction-service}") String allowedProducers) {
        this(consumer, parseAllowedProducers(allowedProducers));
    }

    public TransferCreatedKafkaListener(TransferCreatedEventConsumer consumer, Set<String> allowedProducers) {
        this.consumer = consumer;
        this.allowedProducers = Set.copyOf(allowedProducers);
        if (this.allowedProducers.isEmpty()) {
            throw new IllegalArgumentException("at least one allowed producer is required");
        }
    }

    @KafkaListener(
            topics = "${notification.events.transfer-created.topic:events.transfer.created.v1}",
            groupId = "${spring.kafka.consumer.group-id:notification-service}",
            containerFactory = "transferCreatedKafkaListenerContainerFactory")
    public TransferEventConsumptionResult onMessage(ConsumerRecord<String, String> record) {
        if (record == null) {
            throw new InvalidTransferEventException("Kafka record must not be null");
        }

        var producer = header(record, PRODUCER);
        if (!allowedProducers.contains(producer)) {
            throw new InvalidTransferEventException("producer is not trusted for transfer-created events");
        }
        var schemaVersion = header(record, SCHEMA_VERSION);
        if (!SUPPORTED_SCHEMA_VERSION.equals(schemaVersion)) {
            throw new InvalidTransferEventException("unsupported transfer-created schema version: " + schemaVersion);
        }
        var event = new TransferCreatedEvent(
                header(record, EVENT_ID),
                header(record, CORRELATION_ID),
                header(record, CAUSATION_ID),
                producer,
                schemaVersion,
                occurredAt(record),
                payload(record));
        return consumer.consume(event);
    }

    private String payload(ConsumerRecord<String, String> record) {
        if (record.value() == null || record.value().isBlank()) {
            throw new InvalidTransferEventException("transfer-created payload must not be blank");
        }
        return record.value();
    }

    private Instant occurredAt(ConsumerRecord<String, String> record) {
        try {
            return Instant.parse(header(record, OCCURRED_AT));
        } catch (RuntimeException exception) {
            throw new InvalidTransferEventException("occurred-at must be an ISO-8601 timestamp");
        }
    }

    private String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null || header.value() == null) {
            throw new InvalidTransferEventException("missing required Kafka header: " + name);
        }
        var value = new String(header.value(), java.nio.charset.StandardCharsets.UTF_8).trim();
        if (value.isBlank()) {
            throw new InvalidTransferEventException("blank required Kafka header: " + name);
        }
        return value;
    }

    private static Set<String> parseAllowedProducers(String configured) {
        return Arrays.stream(configured.split(","))
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .collect(Collectors.toUnmodifiableSet());
    }
}
