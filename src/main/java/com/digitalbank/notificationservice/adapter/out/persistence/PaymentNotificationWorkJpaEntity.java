package com.digitalbank.notificationservice.adapter.out.persistence;

import com.digitalbank.notificationservice.application.payment.PaymentNotificationWork;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "payment_notification_work")
public class PaymentNotificationWorkJpaEntity {

    @Id
    @Column(name = "event_id", nullable = false, length = 200)
    private String eventId;

    @Column(name = "instruction_id", nullable = false, length = 200)
    private String instructionId;

    @Column(name = "correlation_id", nullable = false, length = 200)
    private String correlationId;

    @Column(name = "causation_id", nullable = false, length = 200)
    private String causationId;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Column(name = "work_type", nullable = false, length = 64)
    private String workType;

    @Column(nullable = false, length = 32)
    private String status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected PaymentNotificationWorkJpaEntity() {}

    PaymentNotificationWorkJpaEntity(PaymentNotificationWork work) {
        this.eventId = work.eventId();
        this.instructionId = work.instructionId();
        this.correlationId = work.correlationId();
        this.causationId = work.causationId();
        this.amount = work.amount();
        this.currency = work.currency();
        this.workType = work.workType();
        this.status = work.status();
        this.createdAt = work.createdAt();
    }
}
