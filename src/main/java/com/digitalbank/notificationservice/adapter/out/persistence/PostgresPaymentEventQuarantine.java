package com.digitalbank.notificationservice.adapter.out.persistence;

import com.digitalbank.notificationservice.application.payment.PaymentEventQuarantine;
import com.digitalbank.notificationservice.application.payment.PaymentEventQuarantinePort;
import java.sql.Timestamp;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class PostgresPaymentEventQuarantine implements PaymentEventQuarantinePort {

    private static final String INSERT = """
            insert into payment_event_quarantine (
                id, quarantine_key, event_id, fingerprint, event_type, schema_version, producer,
                aggregate_id, instruction_id, correlation_id, causation_id, amount, currency,
                event_status, topic, kafka_partition, kafka_offset, reason, quarantined_at)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict (quarantine_key) do nothing
            """;

    private final JdbcTemplate jdbcTemplate;

    public PostgresPaymentEventQuarantine(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void quarantine(PaymentEventQuarantine record) {
        jdbcTemplate.update(
                INSERT,
                UUID.randomUUID(),
                record.quarantineKey(),
                record.eventId(),
                record.fingerprint(),
                record.eventType(),
                record.schemaVersion(),
                record.producer(),
                record.aggregateId(),
                record.instructionId(),
                record.correlationId(),
                record.causationId(),
                record.amount(),
                record.currency(),
                record.status(),
                record.topic(),
                record.partition(),
                record.offset(),
                record.reason(),
                Timestamp.from(record.quarantinedAt()));
    }
}
