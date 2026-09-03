package com.digitalbank.notificationservice.adapter.out.persistence;

import com.digitalbank.notificationservice.application.event.TransferEventQuarantine;
import com.digitalbank.notificationservice.application.event.TransferEventQuarantinePort;
import java.sql.Timestamp;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class PostgresTransferEventQuarantine implements TransferEventQuarantinePort {

    private static final String INSERT = """
            insert into transfer_event_quarantine (
                id, quarantine_key, event_id, fingerprint, correlation_id, causation_id,
                producer, schema_version, topic, kafka_partition, kafka_offset, reason, quarantined_at)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict (quarantine_key) do nothing
            """;

    private final JdbcTemplate jdbcTemplate;

    public PostgresTransferEventQuarantine(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void quarantine(TransferEventQuarantine record) {
        jdbcTemplate.update(
                INSERT,
                UUID.randomUUID(),
                record.quarantineKey(),
                record.eventId(),
                record.fingerprint(),
                record.correlationId(),
                record.causationId(),
                record.producer(),
                record.schemaVersion(),
                record.topic(),
                record.partition(),
                record.offset(),
                record.reason(),
                Timestamp.from(record.quarantinedAt()));
    }
}
