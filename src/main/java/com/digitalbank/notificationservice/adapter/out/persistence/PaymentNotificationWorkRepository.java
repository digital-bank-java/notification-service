package com.digitalbank.notificationservice.adapter.out.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

interface PaymentNotificationWorkRepository extends JpaRepository<PaymentNotificationWorkJpaEntity, String> {}
