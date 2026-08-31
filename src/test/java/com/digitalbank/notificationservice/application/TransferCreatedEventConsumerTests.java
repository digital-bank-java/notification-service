package com.digitalbank.notificationservice.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.digitalbank.notificationservice.application.event.NotificationWork;
import com.digitalbank.notificationservice.application.event.NotificationWorkPort;
import com.digitalbank.notificationservice.application.event.TransferCreatedEvent;
import com.digitalbank.notificationservice.application.event.TransferCreatedEventConsumerService;
import com.digitalbank.notificationservice.application.event.TransferEventConflictException;
import com.digitalbank.notificationservice.application.event.TransferEventConsumptionResult;
import com.digitalbank.notificationservice.application.event.TransferEventInbox;
import com.digitalbank.notificationservice.application.event.TransferEventInboxPort;
import com.digitalbank.notificationservice.application.event.TransferEventQuarantine;
import com.digitalbank.notificationservice.application.event.TransferEventQuarantinePort;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TransferCreatedEventConsumerTests {

    private InMemoryInbox inbox;
    private InMemoryNotificationWork work;
    private InMemoryQuarantine quarantine;
    private TransferCreatedEventConsumerService consumer;

    @BeforeEach
    void setUp() {
        inbox = new InMemoryInbox();
        work = new InMemoryNotificationWork();
        quarantine = new InMemoryQuarantine();
        consumer = new TransferCreatedEventConsumerService(inbox, work, quarantine);
    }

    @Test
    void acceptsEventAndCreatesDurableNotificationWork() {
        var result = consumer.consume(event("evt-1", "transfer-1", "request-1", "{\"transferId\":\"transfer-1\"}"));

        assertThat(result.eventId()).isEqualTo("evt-1");
        assertThat(result.correlationId()).isEqualTo("transfer-1");
        assertThat(result.causationId()).isEqualTo("request-1");
        assertThat(result.replayed()).isFalse();
        assertThat(inbox.events()).containsOnlyKeys("evt-1");
        assertThat(work.events()).containsOnlyKeys("evt-1");
    }

    @Test
    void identicalEventReplayRemainsIdempotentAfterConsumerRestart() {
        var event = event("evt-1", "transfer-1", "request-1", "{\"transferId\":\"transfer-1\"}");
        var first = consumer.consume(event);

        var restartedConsumer = new TransferCreatedEventConsumerService(inbox, work, quarantine);
        var replay = restartedConsumer.consume(event);

        assertThat(replay)
                .isEqualTo(new TransferEventConsumptionResult(
                        first.eventId(), first.correlationId(), first.causationId(), true));
        assertThat(inbox.events()).hasSize(1);
        assertThat(work.events()).hasSize(1);
        assertThat(quarantine.records()).isEmpty();
    }

    @Test
    void reusedEventIdWithChangedFingerprintIsRejectedWithoutOverwritingInbox() {
        var original = event("evt-1", "transfer-1", "request-1", "{\"transferId\":\"transfer-1\"}");
        var changed = event("evt-1", "transfer-1", "request-1", "{\"transferId\":\"transfer-2\"}");
        consumer.consume(original);

        assertThatThrownBy(() -> consumer.consume(changed)).isInstanceOf(TransferEventConflictException.class);

        assertThat(inbox.events().get("evt-1").fingerprint()).isEqualTo(original.fingerprint());
        assertThat(work.events()).hasSize(1);
        assertThat(quarantine.records()).singleElement().satisfies(record -> {
            assertThat(record.eventId()).isEqualTo("evt-1");
            assertThat(record.fingerprint()).isEqualTo(changed.fingerprint());
            assertThat(record.correlationId()).isEqualTo("transfer-1");
        });
    }

    @Test
    void notificationWorkIsNotCreatedWhenInboxPersistenceFails() {
        var failingInbox = new InMemoryInbox() {
            @Override
            public void save(TransferEventInbox event) {
                throw new IllegalStateException("inbox unavailable");
            }
        };
        var failingConsumer = new TransferCreatedEventConsumerService(failingInbox, work, quarantine);

        assertThatThrownBy(() -> failingConsumer.consume(event("evt-1", "transfer-1", "request-1", "payload")))
                .isInstanceOf(IllegalStateException.class);
        assertThat(work.events()).isEmpty();
    }

    private TransferCreatedEvent event(String eventId, String correlationId, String causationId, String payload) {
        return new TransferCreatedEvent(
                eventId,
                correlationId,
                causationId,
                "transaction-service",
                "1.0.0",
                Instant.parse("2026-08-31T10:15:30Z"),
                payload);
    }

    private static class InMemoryInbox implements TransferEventInboxPort {

        private final Map<String, TransferEventInbox> events = new HashMap<>();

        @Override
        public void lock(String eventId) {}

        @Override
        public Optional<TransferEventInbox> findByEventId(String eventId) {
            return Optional.ofNullable(events.get(eventId));
        }

        @Override
        public void save(TransferEventInbox event) {
            events.put(event.eventId(), event);
        }

        Map<String, TransferEventInbox> events() {
            return events;
        }
    }

    private static class InMemoryNotificationWork implements NotificationWorkPort {

        private final Map<String, NotificationWork> events = new HashMap<>();

        @Override
        public void createIfAbsent(NotificationWork notificationWork) {
            events.putIfAbsent(notificationWork.eventId(), notificationWork);
        }

        Map<String, NotificationWork> events() {
            return events;
        }
    }

    private static class InMemoryQuarantine implements TransferEventQuarantinePort {

        private final List<TransferEventQuarantine> records = new ArrayList<>();

        @Override
        public void quarantine(TransferEventQuarantine quarantine) {
            records.add(quarantine);
        }

        List<TransferEventQuarantine> records() {
            return records;
        }
    }
}
