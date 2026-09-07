package com.digitalbank.notificationservice.adapter.out.persistence;

import com.digitalbank.notificationservice.application.payment.PaymentEventInbox;
import com.digitalbank.notificationservice.application.payment.PaymentEventInboxPort;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class PostgresPaymentEventInbox implements PaymentEventInboxPort {

    private final PaymentEventInboxRepository repository;
    private final JdbcTemplate jdbcTemplate;

    public PostgresPaymentEventInbox(PaymentEventInboxRepository repository, JdbcTemplate jdbcTemplate) {
        this.repository = repository;
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void lock(String eventId) {
        jdbcTemplate.queryForList("select pg_advisory_xact_lock(hashtextextended(?, 1))", eventId);
    }

    @Override
    public Optional<PaymentEventInbox> findByEventId(String eventId) {
        return repository.findById(eventId).map(PaymentEventInboxJpaEntity::toDomain);
    }

    @Override
    public void save(PaymentEventInbox event) {
        repository.save(new PaymentEventInboxJpaEntity(event));
    }
}
