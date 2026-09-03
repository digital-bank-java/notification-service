package com.digitalbank.notificationservice.adapter.out.persistence;

import com.digitalbank.notificationservice.application.event.NotificationWork;
import com.digitalbank.notificationservice.application.event.NotificationWorkPort;
import org.springframework.stereotype.Repository;

@Repository
public class PostgresNotificationWork implements NotificationWorkPort {

    private final NotificationWorkRepository repository;

    public PostgresNotificationWork(NotificationWorkRepository repository) {
        this.repository = repository;
    }

    @Override
    public void createIfAbsent(NotificationWork work) {
        if (repository.existsById(work.eventId())) {
            return;
        }
        repository.save(new NotificationWorkJpaEntity(work));
    }
}
