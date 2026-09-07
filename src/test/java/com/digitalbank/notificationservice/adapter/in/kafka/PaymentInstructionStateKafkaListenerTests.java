package com.digitalbank.notificationservice.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.digitalbank.notificationservice.application.payment.PaymentEventConsumptionResult;
import com.digitalbank.notificationservice.application.payment.PaymentEventQuarantine;
import com.digitalbank.notificationservice.application.payment.PaymentEventQuarantinePort;
import com.digitalbank.notificationservice.application.payment.PaymentInstructionStateChangedEvent;
import com.digitalbank.notificationservice.application.payment.PaymentInstructionStateEventConsumer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

class PaymentInstructionStateKafkaListenerTests {

    private static final String EVENT_ID = "22222222-2222-2222-2222-222222222222";
    private static final String INSTRUCTION_ID = "11111111-1111-1111-1111-111111111111";

    @Test
    void mapsGovernedPaymentStatePayloadAndHeadersToApplicationConsumer() {
        var consumer = new InMemoryConsumer();
        var listener = new PaymentInstructionStateKafkaListener(
                consumer, new ObjectMapper().findAndRegisterModules(), Set.of("payment-service"));

        var result = listener.onMessage(record(validPayload(), validHeaders()));

        assertThat(result).isEqualTo(new PaymentEventConsumptionResult(EVENT_ID, "corr-1", false));
        assertThat(consumer.events()).singleElement().satisfies(event -> {
            assertThat(event.eventId()).isEqualTo(EVENT_ID);
            assertThat(event.aggregateId()).isEqualTo(INSTRUCTION_ID);
            assertThat(event.instructionId()).isEqualTo(INSTRUCTION_ID);
            assertThat(event.amount()).isEqualByComparingTo("125.50");
            assertThat(event.currency()).isEqualTo("AED");
            assertThat(event.status().name()).isEqualTo("PENDING");
        });
    }

    @Test
    void payloadAndHeaderIdentityMismatchIsQuarantinedBeforeApplicationConsumption() {
        var consumer = new InMemoryConsumer();
        var quarantine = new InMemoryQuarantine();
        var listener = new PaymentInstructionStateKafkaListener(
                consumer, new ObjectMapper().findAndRegisterModules(), Set.of("payment-service"), quarantine);
        var headers = validHeaders();
        headers.remove("event-id").add("event-id", bytes("33333333-3333-3333-3333-333333333333"));

        assertThatThrownBy(() -> listener.onMessage(record(validPayload(), headers)))
                .isInstanceOf(InvalidPaymentEventException.class);
        assertThat(consumer.events()).isEmpty();
        assertThat(quarantine.records()).singleElement().satisfies(entry -> {
            assertThat(entry.eventId()).isEqualTo("33333333-3333-3333-3333-333333333333");
            assertThat(entry.topic()).isEqualTo("payment.instruction.state.v1");
            assertThat(entry.reason()).contains("eventId");
        });
    }

    @Test
    void untrustedProducerAndInvalidBusinessFieldsAreQuarantined() {
        var quarantine = new InMemoryQuarantine();
        var listener = new PaymentInstructionStateKafkaListener(
                new InMemoryConsumer(),
                new ObjectMapper().findAndRegisterModules(),
                Set.of("payment-service"),
                quarantine);

        var untrustedHeaders = validHeaders();
        untrustedHeaders.remove("producer").add("producer", bytes("unknown-service"));
        assertThatThrownBy(() -> listener.onMessage(record(validPayload(), untrustedHeaders)))
                .isInstanceOf(InvalidPaymentEventException.class);

        var invalidPayload = validPayload()
                .replace("\"amount\":\"125.50\"", "\"amount\":\"0\"")
                .replace("\"currency\":\"AED\"", "\"currency\":\"aed\"");
        assertThatThrownBy(() -> listener.onMessage(record(invalidPayload, validHeaders())))
                .isInstanceOf(InvalidPaymentEventException.class);

        var invalidSchemaHeaders = validHeaders();
        invalidSchemaHeaders.remove("schema-version").add("schema-version", bytes("2.0.0"));
        assertThatThrownBy(() -> listener.onMessage(record(validPayload(), invalidSchemaHeaders)))
                .isInstanceOf(InvalidPaymentEventException.class);

        assertThat(quarantine.records()).hasSize(3);
    }

    @Test
    void failedStateRequiresFailureReasonAndOccurredAtMustBeValid() {
        var quarantine = new InMemoryQuarantine();
        var listener = new PaymentInstructionStateKafkaListener(
                new InMemoryConsumer(),
                new ObjectMapper().findAndRegisterModules(),
                Set.of("payment-service"),
                quarantine);
        var failedWithoutReason = validPayload().replace("\"status\":\"PENDING\"", "\"status\":\"FAILED\"");
        var invalidTimeHeaders = validHeaders();
        invalidTimeHeaders.remove("occurred-at").add("occurred-at", bytes("not-a-timestamp"));

        assertThatThrownBy(() -> listener.onMessage(record(failedWithoutReason, validHeaders())))
                .isInstanceOf(InvalidPaymentEventException.class);
        assertThatThrownBy(() -> listener.onMessage(record(validPayload(), invalidTimeHeaders)))
                .isInstanceOf(InvalidPaymentEventException.class);

        assertThat(quarantine.records()).hasSize(2);
    }

    private ConsumerRecord<String, String> record(String payload, RecordHeaders headers) {
        return new ConsumerRecord<>(
                "payment.instruction.state.v1",
                1,
                7L,
                0L,
                TimestampType.CREATE_TIME,
                -1,
                -1,
                INSTRUCTION_ID,
                payload,
                headers,
                Optional.empty());
    }

    private RecordHeaders validHeaders() {
        var headers = new RecordHeaders();
        headers.add("event-id", bytes(EVENT_ID));
        headers.add("correlation-id", bytes("corr-1"));
        headers.add("causation-id", bytes("cause-1"));
        headers.add("producer", bytes("payment-service"));
        headers.add("schema-version", bytes("1.0.0"));
        headers.add("occurred-at", bytes("2026-09-06T19:00:00Z"));
        return headers;
    }

    private String validPayload() {
        return """
                {
                  "eventId":"%s",
                  "eventType":"PaymentInstructionStateChanged.v1",
                  "schemaVersion":"1.0.0",
                  "producer":"payment-service",
                  "occurredAt":"2026-09-06T19:00:00Z",
                  "aggregateId":"%s",
                  "correlationId":"corr-1",
                  "causationId":"cause-1",
                  "instructionId":"%s",
                  "idempotencyKey":"idem-1",
                  "amount":"125.50",
                  "currency":"AED",
                  "status":"PENDING"
                }
                """.formatted(EVENT_ID, INSTRUCTION_ID, INSTRUCTION_ID);
    }

    private byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static class InMemoryConsumer implements PaymentInstructionStateEventConsumer {

        private final List<PaymentInstructionStateChangedEvent> events = new ArrayList<>();

        @Override
        public PaymentEventConsumptionResult consume(PaymentInstructionStateChangedEvent event) {
            events.add(event);
            return new PaymentEventConsumptionResult(event.eventId(), event.correlationId(), false);
        }

        List<PaymentInstructionStateChangedEvent> events() {
            return events;
        }
    }

    private static class InMemoryQuarantine implements PaymentEventQuarantinePort {

        private final List<PaymentEventQuarantine> records = new ArrayList<>();

        @Override
        public void quarantine(PaymentEventQuarantine record) {
            records.add(record);
        }

        List<PaymentEventQuarantine> records() {
            return records;
        }
    }
}
