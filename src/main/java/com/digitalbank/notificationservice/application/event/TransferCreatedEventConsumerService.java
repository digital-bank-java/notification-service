package com.digitalbank.notificationservice.application.event;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TransferCreatedEventConsumerService implements TransferCreatedEventConsumer {

    private final TransferEventInboxPort inbox;
    private final NotificationWorkPort notificationWork;
    private final TransferEventQuarantinePort quarantine;

    public TransferCreatedEventConsumerService(
            TransferEventInboxPort inbox,
            NotificationWorkPort notificationWork,
            TransferEventQuarantinePort quarantine) {
        this.inbox = inbox;
        this.notificationWork = notificationWork;
        this.quarantine = quarantine;
    }

    @Override
    @Transactional
    public TransferEventConsumptionResult consume(TransferCreatedEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }

        inbox.lock(event.eventId());
        var existing = inbox.findByEventId(event.eventId());
        if (existing.isPresent()) {
            var accepted = existing.get();
            if (!accepted.fingerprint().equals(event.fingerprint())) {
                quarantine.quarantine(TransferEventQuarantine.fromEvent(
                        event, "eventId is already associated with a different fingerprint"));
                throw new TransferEventConflictException("eventId is already associated with a different event");
            }
            return new TransferEventConsumptionResult(
                    accepted.eventId(), accepted.correlationId(), accepted.causationId(), true);
        }

        inbox.save(TransferEventInbox.acceptedFrom(event));
        notificationWork.createIfAbsent(NotificationWork.from(event));
        return new TransferEventConsumptionResult(event.eventId(), event.correlationId(), event.causationId(), false);
    }
}
