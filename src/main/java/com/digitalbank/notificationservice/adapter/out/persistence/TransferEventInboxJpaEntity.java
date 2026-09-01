package com.digitalbank.notificationservice.adapter.out.persistence;

import com.digitalbank.notificationservice.application.event.TransferEventInbox;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

@Entity
@Table(name = "transfer_event_inbox")
public class TransferEventInboxJpaEntity {

    @Id
    @Column(name = "event_id", nullable = false, length = 200)
    private String eventId;

    @Column(nullable = false, length = 64)
    private String fingerprint;

    @Column(name = "correlation_id", nullable = false, length = 200)
    private String correlationId;

    @Column(name = "causation_id", nullable = false, length = 200)
    private String causationId;

    @Column(nullable = false, length = 100)
    private String producer;

    @Column(name = "schema_version", nullable = false, length = 32)
    private String schemaVersion;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    @Column(nullable = false, length = 32)
    private String status;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    protected TransferEventInboxJpaEntity() {}

    TransferEventInboxJpaEntity(TransferEventInbox inbox) {
        this.eventId = inbox.eventId();
        this.fingerprint = inbox.fingerprint();
        this.correlationId = inbox.correlationId();
        this.causationId = inbox.causationId();
        this.producer = inbox.producer();
        this.schemaVersion = inbox.schemaVersion();
        this.occurredAt = inbox.occurredAt();
        this.status = inbox.status();
        this.receivedAt = inbox.receivedAt();
    }

    TransferEventInbox toDomain() {
        return new TransferEventInbox(
                eventId,
                fingerprint,
                correlationId,
                causationId,
                producer,
                schemaVersion,
                occurredAt,
                status,
                receivedAt);
    }
}
