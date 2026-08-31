package com.digitalbank.notificationservice.adapter.out.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

interface NotificationWorkRepository extends JpaRepository<NotificationWorkJpaEntity, String> {}
