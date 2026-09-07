package com.digitalbank.notificationservice.application.payment;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentInstructionStateEventConsumerService implements PaymentInstructionStateEventConsumer {

    private final PaymentEventInboxPort inbox;
    private final PaymentNotificationWorkPort notificationWork;
    private final PaymentEventQuarantinePort quarantine;

    public PaymentInstructionStateEventConsumerService(
            PaymentEventInboxPort inbox,
            PaymentNotificationWorkPort notificationWork,
            PaymentEventQuarantinePort quarantine) {
        this.inbox = inbox;
        this.notificationWork = notificationWork;
        this.quarantine = quarantine;
    }

    @Override
    @Transactional
    public PaymentEventConsumptionResult consume(PaymentInstructionStateChangedEvent event) {
        return consume(event, null);
    }

    @Override
    @Transactional
    public PaymentEventConsumptionResult consume(PaymentInstructionStateChangedEvent event, PaymentEventSource source) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }

        inbox.lock(event.eventId());
        var existing = inbox.findByEventId(event.eventId());
        if (existing.isPresent()) {
            var accepted = existing.get();
            if (!accepted.fingerprint().equals(event.fingerprint())) {
                quarantine.quarantine(PaymentEventQuarantine.fromEvent(
                        event, "eventId is already associated with a different fingerprint", source));
                throw new PaymentEventConflictException("eventId is already associated with a different event");
            }
            return new PaymentEventConsumptionResult(accepted.eventId(), accepted.correlationId(), true);
        }

        inbox.save(PaymentEventInbox.acceptedFrom(event));
        notificationWork.createIfAbsent(PaymentNotificationWork.from(event));
        return new PaymentEventConsumptionResult(event.eventId(), event.correlationId(), false);
    }
}
