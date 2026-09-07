package com.digitalbank.notificationservice;

import static org.assertj.core.api.Assertions.assertThat;

import com.digitalbank.notificationservice.application.payment.PaymentEventConsumptionResult;
import com.digitalbank.notificationservice.application.payment.PaymentEventSource;
import com.digitalbank.notificationservice.application.payment.PaymentInstructionState;
import com.digitalbank.notificationservice.application.payment.PaymentInstructionStateChangedEvent;
import com.digitalbank.notificationservice.application.payment.PaymentInstructionStateEventConsumer;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@SpringBootTest
@Testcontainers
class PaymentInstructionStateEventPersistenceIT {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16-alpine");

    private static final String INSTRUCTION_ID = "11111111-1111-1111-1111-111111111111";

    @Autowired
    private PaymentInstructionStateEventConsumer consumer;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void storesOnePaymentInboxAndWorkRowAcrossReplay() {
        var event = event("22222222-2222-2222-2222-222222222222", PaymentInstructionState.COMPLETED);

        var first = consumer.consume(event, new PaymentEventSource("payment.instruction.state.v1", 1, 7L));
        var replay = consumer.consume(event);

        assertThat(first.replayed()).isFalse();
        assertThat(replay).isEqualTo(new PaymentEventConsumptionResult(event.eventId(), event.correlationId(), true));
        assertThat(jdbcTemplate.queryForObject(
                        "select count(*) from payment_event_inbox where event_id = ?", Integer.class, event.eventId()))
                .isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject(
                        "select count(*) from payment_notification_work where event_id = ?",
                        Integer.class,
                        event.eventId()))
                .isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject(
                        "select status from payment_notification_work where event_id = ?",
                        String.class,
                        event.eventId()))
                .isEqualTo("COMPLETED");
        assertThat(jdbcTemplate.queryForObject(
                        "select amount from payment_notification_work where event_id = ?",
                        BigDecimal.class,
                        event.eventId()))
                .isEqualByComparingTo("125.50");
    }

    private PaymentInstructionStateChangedEvent event(String eventId, PaymentInstructionState status) {
        return new PaymentInstructionStateChangedEvent(
                eventId,
                "PaymentInstructionStateChanged.v1",
                "1.0.0",
                "payment-service",
                Instant.parse("2026-09-06T19:00:00Z"),
                INSTRUCTION_ID,
                "corr-it-1",
                "cause-it-1",
                INSTRUCTION_ID,
                "idem-it-1",
                new BigDecimal("125.50"),
                "AED",
                status,
                status == PaymentInstructionState.FAILED ? "provider rejected payment" : null,
                "payload-" + UUID.randomUUID());
    }
}
