package com.digitalbank.notificationservice.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.digitalbank.notificationservice.application.payment.PaymentEventConflictException;
import com.digitalbank.notificationservice.application.payment.PaymentEventConsumptionResult;
import com.digitalbank.notificationservice.application.payment.PaymentEventInbox;
import com.digitalbank.notificationservice.application.payment.PaymentEventInboxPort;
import com.digitalbank.notificationservice.application.payment.PaymentEventQuarantine;
import com.digitalbank.notificationservice.application.payment.PaymentEventQuarantinePort;
import com.digitalbank.notificationservice.application.payment.PaymentEventSource;
import com.digitalbank.notificationservice.application.payment.PaymentInstructionState;
import com.digitalbank.notificationservice.application.payment.PaymentInstructionStateChangedEvent;
import com.digitalbank.notificationservice.application.payment.PaymentInstructionStateEventConsumerService;
import com.digitalbank.notificationservice.application.payment.PaymentNotificationWork;
import com.digitalbank.notificationservice.application.payment.PaymentNotificationWorkPort;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PaymentInstructionStateEventConsumerTests {

    private static final UUID INSTRUCTION_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private InMemoryInbox inbox;
    private InMemoryNotificationWork work;
    private InMemoryQuarantine quarantine;
    private PaymentInstructionStateEventConsumerService consumer;

    @BeforeEach
    void setUp() {
        inbox = new InMemoryInbox();
        work = new InMemoryNotificationWork();
        quarantine = new InMemoryQuarantine();
        consumer = new PaymentInstructionStateEventConsumerService(inbox, work, quarantine);
    }

    @Test
    void acceptsPaymentStateAndCreatesDurableNotificationWork() {
        var event = event("22222222-2222-2222-2222-222222222222", PaymentInstructionState.PENDING);

        var result = consumer.consume(event, new PaymentEventSource("payment.instruction.state.v1", 1, 7L));

        assertThat(result).isEqualTo(new PaymentEventConsumptionResult(event.eventId(), event.correlationId(), false));
        assertThat(inbox.events()).containsOnlyKeys(event.eventId());
        assertThat(work.events()).containsOnlyKeys(event.eventId());
        assertThat(work.events().get(event.eventId())).satisfies(notification -> {
            assertThat(notification.instructionId()).isEqualTo(event.instructionId());
            assertThat(notification.status()).isEqualTo("PENDING");
            assertThat(notification.amount()).isEqualByComparingTo(event.amount());
            assertThat(notification.currency()).isEqualTo(event.currency());
        });
    }

    @Test
    void identicalEventReplayIsIgnoredAfterConsumerRestart() {
        var event = event("33333333-3333-3333-3333-333333333333", PaymentInstructionState.COMPLETED);
        var first = consumer.consume(event);

        var replay = new PaymentInstructionStateEventConsumerService(inbox, work, quarantine).consume(event);

        assertThat(replay).isEqualTo(new PaymentEventConsumptionResult(first.eventId(), first.correlationId(), true));
        assertThat(inbox.events()).hasSize(1);
        assertThat(work.events()).hasSize(1);
        assertThat(quarantine.records()).isEmpty();
    }

    @Test
    void reusedEventIdWithChangedFingerprintIsRejectedAndQuarantined() {
        var original = event("44444444-4444-4444-4444-444444444444", PaymentInstructionState.PENDING);
        var changed = new PaymentInstructionStateChangedEvent(
                original.eventId(),
                original.eventType(),
                original.schemaVersion(),
                original.producer(),
                original.occurredAt(),
                original.aggregateId(),
                original.correlationId(),
                original.causationId(),
                original.instructionId(),
                original.idempotencyKey(),
                new BigDecimal("125.51"),
                original.currency(),
                original.status(),
                original.failureReason(),
                original.payload());
        consumer.consume(original);

        assertThatThrownBy(
                        () -> consumer.consume(changed, new PaymentEventSource("payment.instruction.state.v1", 2, 13L)))
                .isInstanceOf(PaymentEventConflictException.class);

        assertThat(inbox.events().get(original.eventId()).fingerprint()).isEqualTo(original.fingerprint());
        assertThat(work.events()).hasSize(1);
        assertThat(quarantine.records()).singleElement().satisfies(record -> {
            assertThat(record.eventId()).isEqualTo(original.eventId());
            assertThat(record.topic()).isEqualTo("payment.instruction.state.v1");
            assertThat(record.partition()).isEqualTo(2);
            assertThat(record.offset()).isEqualTo(13L);
            assertThat(record.reason()).contains("different fingerprint");
        });
    }

    @Test
    void eachPaymentStateCreatesDistinctDurableWorkStatus() {
        consumer.consume(event("55555555-5555-5555-5555-555555555551", PaymentInstructionState.PENDING));
        consumer.consume(event("55555555-5555-5555-5555-555555555552", PaymentInstructionState.COMPLETED));
        consumer.consume(event("55555555-5555-5555-5555-555555555553", PaymentInstructionState.FAILED));

        assertThat(work.events().values().stream()
                        .map(PaymentNotificationWork::status)
                        .toList())
                .containsExactlyInAnyOrder("PENDING", "COMPLETED", "FAILED");
    }

    private PaymentInstructionStateChangedEvent event(String eventId, PaymentInstructionState status) {
        return new PaymentInstructionStateChangedEvent(
                eventId,
                "PaymentInstructionStateChanged.v1",
                "1.0.0",
                "payment-service",
                Instant.parse("2026-09-06T19:00:00Z"),
                INSTRUCTION_ID.toString(),
                "corr-1",
                "cause-1",
                INSTRUCTION_ID.toString(),
                "idem-1",
                new BigDecimal("125.50"),
                "AED",
                status,
                status == PaymentInstructionState.FAILED ? "provider rejected payment" : null,
                "payload");
    }

    private static class InMemoryInbox implements PaymentEventInboxPort {

        private final Map<String, PaymentEventInbox> events = new HashMap<>();

        @Override
        public void lock(String eventId) {}

        @Override
        public Optional<PaymentEventInbox> findByEventId(String eventId) {
            return Optional.ofNullable(events.get(eventId));
        }

        @Override
        public void save(PaymentEventInbox event) {
            events.put(event.eventId(), event);
        }

        Map<String, PaymentEventInbox> events() {
            return events;
        }
    }

    private static class InMemoryNotificationWork implements PaymentNotificationWorkPort {

        private final Map<String, PaymentNotificationWork> events = new HashMap<>();

        @Override
        public void createIfAbsent(PaymentNotificationWork notificationWork) {
            events.putIfAbsent(notificationWork.eventId(), notificationWork);
        }

        Map<String, PaymentNotificationWork> events() {
            return events;
        }
    }

    private static class InMemoryQuarantine implements PaymentEventQuarantinePort {

        private final List<PaymentEventQuarantine> records = new ArrayList<>();

        @Override
        public void quarantine(PaymentEventQuarantine quarantine) {
            records.add(quarantine);
        }

        List<PaymentEventQuarantine> records() {
            return records;
        }
    }
}
