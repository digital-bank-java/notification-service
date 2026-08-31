package com.digitalbank.notificationservice.adapter.out.persistence;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

interface TransferEventQuarantineRepository extends JpaRepository<TransferEventQuarantineJpaEntity, UUID> {

    boolean existsByQuarantineKey(String quarantineKey);
}
