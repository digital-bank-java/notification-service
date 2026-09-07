package com.digitalbank.notificationservice.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.digitalbank.notificationservice.application.payment.PaymentEventConflictException;
import com.digitalbank.notificationservice.application.payment.PaymentEventQuarantine;
import com.digitalbank.notificationservice.application.payment.PaymentEventQuarantinePort;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ListenerExecutionFailedException;

class PaymentEventKafkaRecovererTests {

    @Test
    void persistsExhaustedRetryablePaymentRecordThroughQuarantine() {
        var quarantine = new InMemoryQuarantine();
        var recoverer = new PaymentEventKafkaRecoverer(quarantine);

        recoverer.accept(record(), new IllegalStateException("database unavailable"));

        assertThat(quarantine.records()).singleElement().satisfies(entry -> {
            assertThat(entry.eventId()).isEqualTo("22222222-2222-2222-2222-222222222222");
            assertThat(entry.topic()).isEqualTo("payment.instruction.state.v1");
            assertThat(entry.partition()).isEqualTo(1);
            assertThat(entry.offset()).isEqualTo(7L);
            assertThat(entry.reason()).contains("retry exhaustion", "database unavailable");
        });
    }

    @Test
    void doesNotQuarantineTerminalPaymentConflict() {
        var quarantine = new InMemoryQuarantine();
        var recoverer = new PaymentEventKafkaRecoverer(quarantine);

        recoverer.accept(
                record(),
                new ListenerExecutionFailedException("listener failed", new PaymentEventConflictException("conflict")));

        assertThat(quarantine.records()).isEmpty();
    }

    private ConsumerRecord<String, String> record() {
        var headers = new RecordHeaders();
        headers.add("event-id", bytes("22222222-2222-2222-2222-222222222222"));
        headers.add("correlation-id", bytes("corr-1"));
        headers.add("causation-id", bytes("cause-1"));
        headers.add("producer", bytes("payment-service"));
        headers.add("schema-version", bytes("1.0.0"));
        headers.add("occurred-at", bytes("2026-09-06T19:00:00Z"));
        return new ConsumerRecord<>(
                "payment.instruction.state.v1",
                1,
                7L,
                0L,
                TimestampType.CREATE_TIME,
                -1,
                -1,
                "11111111-1111-1111-1111-111111111111",
                "{}",
                headers,
                Optional.empty());
    }

    private byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
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
