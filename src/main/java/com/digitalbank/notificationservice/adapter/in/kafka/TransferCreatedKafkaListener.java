package com.digitalbank.notificationservice.adapter.in.kafka;

import com.digitalbank.notificationservice.application.event.TransferCreatedEvent;
import com.digitalbank.notificationservice.application.event.TransferCreatedEventConsumer;
import com.digitalbank.notificationservice.application.event.TransferEventConsumptionResult;
import com.digitalbank.notificationservice.application.event.TransferEventMetadataLimits;
import com.digitalbank.notificationservice.application.event.TransferEventQuarantine;
import com.digitalbank.notificationservice.application.event.TransferEventQuarantinePort;
import com.digitalbank.notificationservice.application.event.TransferEventSource;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
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

    private static final ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper().findAndRegisterModules();
    private static final String SUPPORTED_EVENT_TYPE = "TransferCreated.v1";
    private static final String SUPPORTED_SCHEMA_VERSION = "1.0.0";
    private static final String EVENT_ID = "event-id";
    private static final String CORRELATION_ID = "correlation-id";
    private static final String CAUSATION_ID = "causation-id";
    private static final String PRODUCER = "producer";
    private static final String SCHEMA_VERSION = "schema-version";
    private static final String OCCURRED_AT = "occurred-at";

    private final TransferCreatedEventConsumer consumer;
    private final ObjectMapper objectMapper;
    private final Set<String> allowedProducers;
    private final TransferEventQuarantinePort quarantine;

    @Autowired
    public TransferCreatedKafkaListener(
            TransferCreatedEventConsumer consumer,
            @Value("${notification.events.allowed-producers:transaction-service}") String allowedProducers,
            TransferEventQuarantinePort quarantine) {
        this(consumer, DEFAULT_OBJECT_MAPPER, parseAllowedProducers(allowedProducers), quarantine);
    }

    public TransferCreatedKafkaListener(TransferCreatedEventConsumer consumer, Set<String> allowedProducers) {
        this(consumer, DEFAULT_OBJECT_MAPPER, allowedProducers, record -> {});
    }

    TransferCreatedKafkaListener(
            TransferCreatedEventConsumer consumer,
            Set<String> allowedProducers,
            TransferEventQuarantinePort quarantine) {
        this(consumer, DEFAULT_OBJECT_MAPPER, allowedProducers, quarantine);
    }

    TransferCreatedKafkaListener(
            TransferCreatedEventConsumer consumer,
            ObjectMapper objectMapper,
            Set<String> allowedProducers,
            TransferEventQuarantinePort quarantine) {
        this.consumer = consumer;
        this.objectMapper = objectMapper;
        this.allowedProducers = Set.copyOf(allowedProducers);
        this.quarantine = quarantine;
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

        try {
            var producer = header(record, PRODUCER);
            if (!allowedProducers.contains(producer)) {
                throw new InvalidTransferEventException("producer is not trusted for transfer-created events");
            }
            var schemaVersion = header(record, SCHEMA_VERSION);
            if (!SUPPORTED_SCHEMA_VERSION.equals(schemaVersion)) {
                throw new InvalidTransferEventException(
                        "unsupported transfer-created schema version: " + schemaVersion);
            }
            var event = parse(record, producer, schemaVersion);
            return consumer.consume(
                    event, new TransferEventSource(record.topic(), record.partition(), record.offset()));
        } catch (InvalidTransferEventException exception) {
            quarantine.quarantine(TransferEventQuarantine.fromKafkaRecord(record, exception.getMessage()));
            throw exception;
        }
    }

    private TransferCreatedEvent parse(
            ConsumerRecord<String, String> record, String producerHeader, String schemaVersionHeader) {
        var eventIdHeader = header(record, EVENT_ID);
        var correlationIdHeader = header(record, CORRELATION_ID);
        var causationIdHeader = header(record, CAUSATION_ID);
        var occurredAtHeader = occurredAt(record);
        var payload = payload(record);
        final JsonNode root;
        try {
            root = objectMapper.readTree(payload);
        } catch (JsonProcessingException exception) {
            throw new InvalidTransferEventException("transfer-created payload must be valid JSON", exception);
        }
        if (root == null || !root.isObject()) {
            throw new InvalidTransferEventException("transfer-created payload must be a JSON object");
        }

        var eventId = uuid(payloadText(root, "eventId"), "eventId");
        if (!eventId.equals(uuid(eventIdHeader, EVENT_ID))) {
            throw new InvalidTransferEventException("eventId header does not match payload");
        }
        if (!SUPPORTED_EVENT_TYPE.equals(payloadText(root, "eventType"))) {
            throw new InvalidTransferEventException("unsupported transfer-created event type");
        }
        requirePayloadMatch(root, "schemaVersion", schemaVersionHeader);
        requirePayloadMatch(root, "producer", producerHeader);
        var occurredAt = instant(payloadText(root, "occurredAt"), "occurredAt");
        if (!occurredAt.equals(occurredAtHeader)) {
            throw new InvalidTransferEventException("occurred-at header does not match payload");
        }
        requirePayloadMatch(root, "correlationId", correlationIdHeader);
        requirePayloadMatch(root, "causationId", causationIdHeader);

        var aggregateId = uuid(payloadText(root, "aggregateId"), "aggregateId");
        var transactionId = uuid(payloadText(root, "transactionId"), "transactionId");
        if (!aggregateId.equals(transactionId)) {
            throw new InvalidTransferEventException("aggregateId and transactionId must identify the same transfer");
        }
        if (!aggregateId.equals(uuid(record.key(), "Kafka key"))) {
            throw new InvalidTransferEventException("Kafka key must match aggregateId");
        }
        uuid(payloadText(root, "sourceAccountId"), "sourceAccountId");
        uuid(payloadText(root, "destinationAccountId"), "destinationAccountId");
        amount(payloadText(root, "amount"));
        if (!payloadText(root, "currency").matches("[A-Z]{3}")) {
            throw new InvalidTransferEventException("currency must be a three-letter uppercase ISO code");
        }
        payloadText(root, "transferRequestId");
        payloadText(root, "reservationRequestId");
        payloadText(root, "postingRequestId");
        if (!"PENDING".equals(payloadText(root, "status"))) {
            throw new InvalidTransferEventException("transfer-created status must be PENDING");
        }

        return new TransferCreatedEvent(
                eventIdHeader,
                correlationIdHeader,
                causationIdHeader,
                producerHeader,
                schemaVersionHeader,
                occurredAtHeader,
                payload);
    }

    private String payload(ConsumerRecord<String, String> record) {
        if (record.value() == null || record.value().isBlank()) {
            throw new InvalidTransferEventException("transfer-created payload must not be blank");
        }
        return record.value();
    }

    private String payloadText(JsonNode root, String name) {
        var field = root.get(name);
        if (field == null || !field.isTextual() || field.asText().isBlank()) {
            throw new InvalidTransferEventException(name + " must be a non-blank string");
        }
        return field.asText().trim();
    }

    private void requirePayloadMatch(JsonNode root, String name, String expected) {
        if (!expected.equals(payloadText(root, name))) {
            throw new InvalidTransferEventException(name + " does not match Kafka headers");
        }
    }

    private UUID uuid(String value, String field) {
        try {
            return UUID.fromString(value);
        } catch (RuntimeException exception) {
            throw new InvalidTransferEventException(field + " must be a UUID", exception);
        }
    }

    private void amount(String value) {
        try {
            var amount = new java.math.BigDecimal(value);
            if (amount.signum() <= 0) {
                throw new InvalidTransferEventException("amount must be positive");
            }
        } catch (NumberFormatException exception) {
            throw new InvalidTransferEventException("amount must be a decimal number", exception);
        }
    }

    private Instant instant(String value, String field) {
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException exception) {
            throw new InvalidTransferEventException(field + " must be an ISO-8601 timestamp", exception);
        }
    }

    private Instant occurredAt(ConsumerRecord<String, String> record) {
        try {
            return Instant.parse(header(record, OCCURRED_AT));
        } catch (DateTimeParseException exception) {
            throw new InvalidTransferEventException("occurred-at must be an ISO-8601 timestamp", exception);
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
        var maxLength = TransferEventMetadataLimits.forHeader(name);
        if (value.length() > maxLength) {
            throw new InvalidTransferEventException(name + " must be at most " + maxLength + " characters");
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
