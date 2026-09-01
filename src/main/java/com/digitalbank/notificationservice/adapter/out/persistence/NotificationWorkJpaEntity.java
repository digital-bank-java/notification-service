package com.digitalbank.notificationservice.adapter.out.persistence;

import com.digitalbank.notificationservice.application.event.NotificationWork;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

@Entity
@Table(name = "notification_work")
public class NotificationWorkJpaEntity {

    @Id
    @Column(name = "event_id", nullable = false, length = 200)
    private String eventId;

    @Column(name = "correlation_id", nullable = false, length = 200)
    private String correlationId;

    @Column(name = "causation_id", nullable = false, length = 200)
    private String causationId;

    @Column(name = "work_type", nullable = false, length = 64)
    private String workType;

    @Column(nullable = false, length = 32)
    private String status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected NotificationWorkJpaEntity() {}

    NotificationWorkJpaEntity(NotificationWork work) {
        this.eventId = work.eventId();
        this.correlationId = work.correlationId();
        this.causationId = work.causationId();
        this.workType = work.workType();
        this.status = work.status();
        this.createdAt = work.createdAt();
    }
}
