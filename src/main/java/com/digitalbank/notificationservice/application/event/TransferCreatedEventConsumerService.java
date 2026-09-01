package com.digitalbank.notificationservice.application.event;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.stereotype.Service;

@Service
public class TransferCreatedEventConsumerService implements TransferCreatedEventConsumer {

    private final ConcurrentMap<String, ConsumedEvent> consumedEvents = new ConcurrentHashMap<>();

    @Override
    public TransferEventConsumptionResult consume(TransferCreatedEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }

        var replayed = new AtomicBoolean(false);
        var consumed = consumedEvents.compute(event.eventId(), (eventId, existing) -> {
            if (existing == null) {
                return new ConsumedEvent(event.fingerprint(), result(event, false));
            }
            if (!existing.fingerprint().equals(event.fingerprint())) {
                throw new TransferEventConflictException("eventId is already associated with a different event");
            }
            replayed.set(true);
            return existing;
        });
        var original = consumed.result();
        return new TransferEventConsumptionResult(
                original.eventId(), original.correlationId(), original.causationId(), replayed.get());
    }

    private TransferEventConsumptionResult result(TransferCreatedEvent event, boolean replayed) {
        return new TransferEventConsumptionResult(
                event.eventId(), event.correlationId(), event.causationId(), replayed);
    }

    private record ConsumedEvent(String fingerprint, TransferEventConsumptionResult result) {}
}
