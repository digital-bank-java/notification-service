package com.digitalbank.notificationservice.adapter.in.kafka;

import com.digitalbank.notificationservice.application.payment.PaymentEventConsumptionResult;
import com.digitalbank.notificationservice.application.payment.PaymentEventQuarantine;
import com.digitalbank.notificationservice.application.payment.PaymentEventQuarantinePort;
import com.digitalbank.notificationservice.application.payment.PaymentEventSource;
import com.digitalbank.notificationservice.application.payment.PaymentInstructionState;
import com.digitalbank.notificationservice.application.payment.PaymentInstructionStateChangedEvent;
import com.digitalbank.notificationservice.application.payment.PaymentInstructionStateEventConsumer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
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
@ConditionalOnProperty(name = "notification.events.payment-state.enabled", havingValue = "true")
public class PaymentInstructionStateKafkaListener {

    static final String SUPPORTED_EVENT_TYPE = "PaymentInstructionStateChanged.v1";
    static final String SUPPORTED_SCHEMA_VERSION = "1.0.0";
    private static final String EVENT_ID = "event-id";
    private static final String CORRELATION_ID = "correlation-id";
    private static final String CAUSATION_ID = "causation-id";
    private static final String PRODUCER = "producer";
    private static final String SCHEMA_VERSION = "schema-version";
    private static final String OCCURRED_AT = "occurred-at";
    private static final int MAX_ID_LENGTH = 200;
    private static final int MAX_REASON_LENGTH = 500;

    private final PaymentInstructionStateEventConsumer consumer;
    private final ObjectMapper objectMapper;
    private final Set<String> allowedProducers;
    private final PaymentEventQuarantinePort quarantine;

    @Autowired
    public PaymentInstructionStateKafkaListener(
            PaymentInstructionStateEventConsumer consumer,
            ObjectMapper objectMapper,
            @Value("${notification.events.payment-state.allowed-producers:payment-service}") String allowedProducers,
            PaymentEventQuarantinePort quarantine) {
        this(consumer, objectMapper, parseAllowedProducers(allowedProducers), quarantine);
    }

    public PaymentInstructionStateKafkaListener(
            PaymentInstructionStateEventConsumer consumer, ObjectMapper objectMapper, Set<String> allowedProducers) {
        this(consumer, objectMapper, allowedProducers, record -> {});
    }

    PaymentInstructionStateKafkaListener(
            PaymentInstructionStateEventConsumer consumer,
            ObjectMapper objectMapper,
            Set<String> allowedProducers,
            PaymentEventQuarantinePort quarantine) {
        this.consumer = consumer;
        this.objectMapper = objectMapper;
        this.allowedProducers = Set.copyOf(allowedProducers);
        this.quarantine = quarantine;
        if (this.allowedProducers.isEmpty()) {
            throw new IllegalArgumentException("at least one allowed payment event producer is required");
        }
    }

    @KafkaListener(
            topics = "${notification.events.payment-state.topic:payment.instruction.state.v1}",
            groupId = "${spring.kafka.consumer.group-id:notification-service}",
            containerFactory = "paymentStateKafkaListenerContainerFactory")
    public PaymentEventConsumptionResult onMessage(ConsumerRecord<String, String> record) {
        if (record == null) {
            throw new InvalidPaymentEventException("Kafka record must not be null");
        }

        try {
            var event = parse(record);
            return consumer.consume(event, new PaymentEventSource(record.topic(), record.partition(), record.offset()));
        } catch (InvalidPaymentEventException exception) {
            quarantine.quarantine(PaymentEventQuarantine.fromKafkaRecord(record, exception.getMessage()));
            throw exception;
        }
    }

    private PaymentInstructionStateChangedEvent parse(ConsumerRecord<String, String> record) {
        var eventIdHeader = header(record, EVENT_ID);
        var correlationIdHeader = header(record, CORRELATION_ID);
        var causationIdHeader = header(record, CAUSATION_ID);
        var producerHeader = header(record, PRODUCER);
        var schemaVersionHeader = header(record, SCHEMA_VERSION);
        var occurredAtHeader = instant(header(record, OCCURRED_AT), OCCURRED_AT);
        if (!allowedProducers.contains(producerHeader)) {
            throw new InvalidPaymentEventException("producer is not trusted for payment state events");
        }
        if (!SUPPORTED_SCHEMA_VERSION.equals(schemaVersionHeader)) {
            throw new InvalidPaymentEventException("unsupported payment state schema version: " + schemaVersionHeader);
        }

        var payload = payload(record);
        final JsonNode root;
        try {
            root = objectMapper.readTree(payload);
        } catch (JsonProcessingException exception) {
            throw new InvalidPaymentEventException("payment state payload must be valid JSON", exception);
        }
        if (root == null || !root.isObject()) {
            throw new InvalidPaymentEventException("payment state payload must be a JSON object");
        }

        var eventId = uuid(payloadText(root, "eventId"), "eventId");
        if (!eventId.equals(uuid(eventIdHeader, EVENT_ID))) {
            throw new InvalidPaymentEventException("eventId header does not match payload");
        }
        var eventType = payloadText(root, "eventType");
        if (!SUPPORTED_EVENT_TYPE.equals(eventType)) {
            throw new InvalidPaymentEventException("unsupported payment state event type: " + eventType);
        }
        requirePayloadMatch(root, "schemaVersion", schemaVersionHeader);
        requirePayloadMatch(root, "producer", producerHeader);
        var occurredAt = instant(payloadText(root, "occurredAt"), "occurredAt");
        if (!occurredAt.equals(occurredAtHeader)) {
            throw new InvalidPaymentEventException("occurred-at header does not match payload");
        }
        requirePayloadMatch(root, "correlationId", correlationIdHeader);
        requirePayloadMatch(root, "causationId", causationIdHeader);

        var aggregateId = uuid(payloadText(root, "aggregateId"), "aggregateId");
        var instructionId = uuid(payloadText(root, "instructionId"), "instructionId");
        if (!aggregateId.equals(instructionId)) {
            throw new InvalidPaymentEventException("aggregateId and instructionId must identify the same instruction");
        }
        if (!aggregateId.equals(uuid(record.key(), "Kafka key"))) {
            throw new InvalidPaymentEventException("Kafka key must match aggregateId");
        }

        var idempotencyKey = payloadText(root, "idempotencyKey");
        var amount = amount(payloadText(root, "amount"));
        var currency = payloadText(root, "currency");
        if (!currency.matches("[A-Z]{3}")) {
            throw new InvalidPaymentEventException("currency must be a three-letter uppercase ISO code");
        }
        var status = status(payloadText(root, "status"));
        var failureReason = failureReason(root, status);
        return new PaymentInstructionStateChangedEvent(
                eventId.toString(),
                eventType,
                schemaVersionHeader,
                producerHeader,
                occurredAt,
                aggregateId.toString(),
                correlationIdHeader,
                causationIdHeader,
                instructionId.toString(),
                idempotencyKey,
                amount,
                currency,
                status,
                failureReason,
                payload);
    }

    private String failureReason(JsonNode root, PaymentInstructionState status) {
        var field = root.get("failureReason");
        if (status == PaymentInstructionState.FAILED) {
            if (field == null || field.isNull() || !field.isTextual()) {
                throw new InvalidPaymentEventException("failureReason is required for FAILED payment state");
            }
            var reason = field.asText().trim();
            if (reason.isBlank() || reason.length() > MAX_REASON_LENGTH) {
                throw new InvalidPaymentEventException("failureReason must be between 1 and 500 characters");
            }
            return reason;
        }
        if (field != null && !field.isNull()) {
            throw new InvalidPaymentEventException("failureReason is only allowed for FAILED payment state");
        }
        return null;
    }

    private BigDecimal amount(String value) {
        try {
            var amount = new BigDecimal(value);
            if (amount.signum() <= 0 || amount.scale() > 4 || amount.precision() > 19) {
                throw new InvalidPaymentEventException("amount must be positive with at most four decimal places");
            }
            return amount;
        } catch (NumberFormatException exception) {
            throw new InvalidPaymentEventException("amount must be a decimal number", exception);
        }
    }

    private PaymentInstructionState status(String value) {
        try {
            return PaymentInstructionState.valueOf(value);
        } catch (IllegalArgumentException exception) {
            throw new InvalidPaymentEventException("status must be PENDING, COMPLETED, or FAILED", exception);
        }
    }

    private String payload(ConsumerRecord<String, String> record) {
        if (record.value() == null || record.value().isBlank()) {
            throw new InvalidPaymentEventException("payment state payload must not be blank");
        }
        return record.value();
    }

    private String payloadText(JsonNode root, String name) {
        var field = root.get(name);
        if (field == null || !field.isTextual() || field.asText().isBlank()) {
            throw new InvalidPaymentEventException(name + " must be a non-blank string");
        }
        var value = field.asText().trim();
        if (value.length() > MAX_ID_LENGTH && !name.equals("eventType") && !name.equals("amount")) {
            throw new InvalidPaymentEventException(name + " must be at most 200 characters");
        }
        return value;
    }

    private void requirePayloadMatch(JsonNode root, String name, String expected) {
        if (!expected.equals(payloadText(root, name))) {
            throw new InvalidPaymentEventException(name + " does not match Kafka headers");
        }
    }

    private UUID uuid(String value, String field) {
        try {
            return UUID.fromString(value);
        } catch (RuntimeException exception) {
            throw new InvalidPaymentEventException(field + " must be a UUID", exception);
        }
    }

    private Instant instant(String value, String field) {
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException exception) {
            throw new InvalidPaymentEventException(field + " must be an ISO-8601 timestamp", exception);
        }
    }

    private String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null || header.value() == null) {
            throw new InvalidPaymentEventException("missing required Kafka header: " + name);
        }
        var value = new String(header.value(), StandardCharsets.UTF_8).trim();
        if (value.isBlank()) {
            throw new InvalidPaymentEventException("blank required Kafka header: " + name);
        }
        var maxLength = name.equals(OCCURRED_AT) ? 64 : MAX_ID_LENGTH;
        if (value.length() > maxLength) {
            throw new InvalidPaymentEventException(name + " must be at most " + maxLength + " characters");
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
