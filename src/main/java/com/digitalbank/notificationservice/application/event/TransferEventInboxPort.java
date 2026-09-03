package com.digitalbank.notificationservice.application.event;

import java.util.Optional;

public interface TransferEventInboxPort {

    void lock(String eventId);

    Optional<TransferEventInbox> findByEventId(String eventId);

    void save(TransferEventInbox event);
}
