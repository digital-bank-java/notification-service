package com.digitalbank.notificationservice.adapter.out.persistence;

import com.digitalbank.notificationservice.application.event.TransferEventQuarantine;
import com.digitalbank.notificationservice.application.event.TransferEventQuarantinePort;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class PostgresTransferEventQuarantine implements TransferEventQuarantinePort {

    private final TransferEventQuarantineRepository repository;

    public PostgresTransferEventQuarantine(TransferEventQuarantineRepository repository) {
        this.repository = repository;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void quarantine(TransferEventQuarantine record) {
        if (!repository.existsByQuarantineKey(record.quarantineKey())) {
            repository.save(new TransferEventQuarantineJpaEntity(record));
        }
    }
}
