package com.digitalbank.notificationservice.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.digitalbank.notificationservice.application.event.TransferEventQuarantine;
import com.digitalbank.notificationservice.application.event.TransferEventQuarantinePort;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ListenerExecutionFailedException;

class TransferEventKafkaRecovererTests {

    @Test
    void persistsExhaustedRetryableRecordThroughDurableQuarantine() {
        var quarantine = new InMemoryQuarantine();
        var recoverer = new TransferEventKafkaRecoverer(quarantine);
        var record = record();

        recoverer.accept(record, new IllegalStateException("database unavailable"));

        assertThat(quarantine.records()).singleElement().satisfies(entry -> {
            assertThat(entry.eventId()).isEqualTo("evt-1");
            assertThat(entry.topic()).isEqualTo("events.transfer.created.v1");
            assertThat(entry.partition()).isEqualTo(2);
            assertThat(entry.offset()).isEqualTo(17L);
            assertThat(entry.reason()).contains("retry exhaustion", "IllegalStateException", "database unavailable");
        });
    }

    @Test
    void doesNotQuarantineTerminalCauseWrappedByListenerExecutionException() {
        var quarantine = new InMemoryQuarantine();
        var recoverer = new TransferEventKafkaRecoverer(quarantine);
        var terminal = new InvalidTransferEventException("payload invalid");

        recoverer.accept(record(), new ListenerExecutionFailedException("listener failed", terminal));

        assertThat(quarantine.records()).isEmpty();
    }

    @Test
    void handlesCyclicCauseChainWithoutHanging() {
        var quarantine = new InMemoryQuarantine();
        var recoverer = new TransferEventKafkaRecoverer(quarantine);
        var first = new IllegalStateException("first failure");
        var second = new IllegalArgumentException("root failure");
        first.initCause(second);
        second.initCause(first);

        recoverer.accept(record(), first);

        assertThat(quarantine.records()).singleElement().satisfies(entry -> {
            assertThat(entry.reason()).contains("IllegalArgumentException", "root failure");
        });
    }

    private ConsumerRecord<String, String> record() {
        var headers = new RecordHeaders()
                .add("event-id", bytes("evt-1"))
                .add("correlation-id", bytes("transfer-1"))
                .add("causation-id", bytes("request-1"))
                .add("producer", bytes("transaction-service"))
                .add("schema-version", bytes("1.0.0"))
                .add("occurred-at", bytes("2026-08-31T10:15:30Z"));
        return new ConsumerRecord<>(
                "events.transfer.created.v1",
                2,
                17L,
                0L,
                TimestampType.CREATE_TIME,
                -1,
                -1,
                "transfer-1",
                "{\"transferId\":\"transfer-1\"}",
                headers,
                Optional.empty());
    }

    private byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static class InMemoryQuarantine implements TransferEventQuarantinePort {

        private final List<TransferEventQuarantine> records = new ArrayList<>();

        @Override
        public void quarantine(TransferEventQuarantine record) {
            records.add(record);
        }

        List<TransferEventQuarantine> records() {
            return records;
        }
    }
}
