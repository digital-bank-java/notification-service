package com.digitalbank.notificationservice.adapter.out.persistence;

import com.digitalbank.notificationservice.application.event.TransferEventQuarantine;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "transfer_event_quarantine")
public class TransferEventQuarantineJpaEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "quarantine_key", nullable = false, unique = true, length = 64)
    private String quarantineKey;

    @Column(name = "event_id", length = 200)
    private String eventId;

    @Column(length = 64)
    private String fingerprint;

    @Column(name = "correlation_id", length = 200)
    private String correlationId;

    @Column(name = "causation_id", length = 200)
    private String causationId;

    @Column(length = 100)
    private String producer;

    @Column(name = "schema_version", length = 32)
    private String schemaVersion;

    @Column(length = 250)
    private String topic;

    @Column(name = "kafka_partition")
    private Integer partition;

    @Column(name = "kafka_offset")
    private Long offset;

    @Column(nullable = false, length = 500)
    private String reason;

    @Column(name = "quarantined_at", nullable = false)
    private Instant quarantinedAt;

    protected TransferEventQuarantineJpaEntity() {}

    TransferEventQuarantineJpaEntity(TransferEventQuarantine record) {
        this.quarantineKey = record.quarantineKey();
        this.eventId = record.eventId();
        this.fingerprint = record.fingerprint();
        this.correlationId = record.correlationId();
        this.causationId = record.causationId();
        this.producer = record.producer();
        this.schemaVersion = record.schemaVersion();
        this.topic = record.topic();
        this.partition = record.partition();
        this.offset = record.offset();
        this.reason = record.reason();
        this.quarantinedAt = record.quarantinedAt();
    }
}
