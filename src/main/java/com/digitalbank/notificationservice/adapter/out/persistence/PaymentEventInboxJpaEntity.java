package com.digitalbank.notificationservice.adapter.out.persistence;

import com.digitalbank.notificationservice.application.payment.PaymentEventInbox;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "payment_event_inbox")
public class PaymentEventInboxJpaEntity {

    @Id
    @Column(name = "event_id", nullable = false, length = 200)
    private String eventId;

    @Column(nullable = false, length = 64)
    private String fingerprint;

    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;

    @Column(name = "schema_version", nullable = false, length = 32)
    private String schemaVersion;

    @Column(nullable = false, length = 100)
    private String producer;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    @Column(name = "aggregate_id", nullable = false, length = 200)
    private String aggregateId;

    @Column(name = "correlation_id", nullable = false, length = 200)
    private String correlationId;

    @Column(name = "causation_id", nullable = false, length = 200)
    private String causationId;

    @Column(name = "instruction_id", nullable = false, length = 200)
    private String instructionId;

    @Column(name = "idempotency_key", nullable = false, length = 200)
    private String idempotencyKey;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Column(name = "event_status", nullable = false, length = 32)
    private String status;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    protected PaymentEventInboxJpaEntity() {}

    PaymentEventInboxJpaEntity(PaymentEventInbox inbox) {
        this.eventId = inbox.eventId();
        this.fingerprint = inbox.fingerprint();
        this.eventType = inbox.eventType();
        this.schemaVersion = inbox.schemaVersion();
        this.producer = inbox.producer();
        this.occurredAt = inbox.occurredAt();
        this.aggregateId = inbox.aggregateId();
        this.correlationId = inbox.correlationId();
        this.causationId = inbox.causationId();
        this.instructionId = inbox.instructionId();
        this.idempotencyKey = inbox.idempotencyKey();
        this.amount = inbox.amount();
        this.currency = inbox.currency();
        this.status = inbox.status();
        this.receivedAt = inbox.receivedAt();
    }

    PaymentEventInbox toDomain() {
        return new PaymentEventInbox(
                eventId,
                fingerprint,
                eventType,
                schemaVersion,
                producer,
                occurredAt,
                aggregateId,
                correlationId,
                causationId,
                instructionId,
                idempotencyKey,
                amount,
                currency,
                status,
                receivedAt);
    }
}
