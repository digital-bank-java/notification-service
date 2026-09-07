package com.digitalbank.notificationservice.adapter.out.persistence;

import com.digitalbank.notificationservice.application.payment.PaymentNotificationWork;
import com.digitalbank.notificationservice.application.payment.PaymentNotificationWorkPort;
import org.springframework.stereotype.Repository;

@Repository
public class PostgresPaymentNotificationWork implements PaymentNotificationWorkPort {

    private final PaymentNotificationWorkRepository repository;

    public PostgresPaymentNotificationWork(PaymentNotificationWorkRepository repository) {
        this.repository = repository;
    }

    @Override
    public void createIfAbsent(PaymentNotificationWork work) {
        if (repository.existsById(work.eventId())) {
            return;
        }
        repository.save(new PaymentNotificationWorkJpaEntity(work));
    }
}
