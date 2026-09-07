package com.digitalbank.notificationservice.application.payment;

import java.util.Optional;

public interface PaymentEventInboxPort {

    void lock(String eventId);

    Optional<PaymentEventInbox> findByEventId(String eventId);

    void save(PaymentEventInbox event);
}
