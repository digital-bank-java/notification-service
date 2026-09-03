package com.digitalbank.notificationservice.adapter.out.persistence;

import com.digitalbank.notificationservice.application.event.TransferEventInbox;
import com.digitalbank.notificationservice.application.event.TransferEventInboxPort;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class PostgresTransferEventInbox implements TransferEventInboxPort {

    private final TransferEventInboxRepository repository;
    private final JdbcTemplate jdbcTemplate;

    public PostgresTransferEventInbox(TransferEventInboxRepository repository, JdbcTemplate jdbcTemplate) {
        this.repository = repository;
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void lock(String eventId) {
        jdbcTemplate.queryForList("select pg_advisory_xact_lock(hashtextextended(?, 0))", eventId);
    }

    @Override
    public Optional<TransferEventInbox> findByEventId(String eventId) {
        return repository.findById(eventId).map(TransferEventInboxJpaEntity::toDomain);
    }

    @Override
    public void save(TransferEventInbox event) {
        repository.save(new TransferEventInboxJpaEntity(event));
    }
}
