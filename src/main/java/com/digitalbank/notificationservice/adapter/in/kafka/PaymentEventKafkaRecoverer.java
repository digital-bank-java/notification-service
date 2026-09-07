package com.digitalbank.notificationservice.adapter.in.kafka;

import com.digitalbank.notificationservice.application.payment.PaymentEventConflictException;
import com.digitalbank.notificationservice.application.payment.PaymentEventQuarantine;
import com.digitalbank.notificationservice.application.payment.PaymentEventQuarantinePort;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;

public final class PaymentEventKafkaRecoverer implements ConsumerRecordRecoverer {

    private static final int ROOT_CAUSE_DIAGNOSTIC_LIMIT = 200;

    private final PaymentEventQuarantinePort quarantine;

    public PaymentEventKafkaRecoverer(PaymentEventQuarantinePort quarantine) {
        this.quarantine = quarantine;
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        if (hasCause(exception, InvalidPaymentEventException.class)
                || hasCause(exception, PaymentEventConflictException.class)) {
            return;
        }
        var kafkaRecord = cast(record);
        quarantine.quarantine(
                PaymentEventQuarantine.fromKafkaRecord(kafkaRecord, "retry exhaustion: " + diagnostic(exception)));
    }

    private static ConsumerRecord<String, String> cast(ConsumerRecord<?, ?> record) {
        @SuppressWarnings("unchecked")
        var kafkaRecord = (ConsumerRecord<String, String>) record;
        return kafkaRecord;
    }

    private static boolean hasCause(Throwable exception, Class<? extends Throwable> type) {
        var visited = identitySet();
        for (var current = exception; current != null && visited.add(current); current = current.getCause()) {
            if (type.isInstance(current)) {
                return true;
            }
        }
        return false;
    }

    private static String diagnostic(Throwable exception) {
        if (exception == null) {
            return "unknown failure";
        }
        var rootCause = exception;
        var visited = identitySet();
        visited.add(rootCause);
        while (rootCause.getCause() != null && visited.add(rootCause.getCause())) {
            rootCause = rootCause.getCause();
        }
        var type = bound(rootCause.getClass().getName());
        var message = rootCause.getMessage();
        return message == null || message.isBlank() ? type : type + ": " + bound(message);
    }

    private static String bound(String value) {
        return value.length() <= ROOT_CAUSE_DIAGNOSTIC_LIMIT ? value : value.substring(0, ROOT_CAUSE_DIAGNOSTIC_LIMIT);
    }

    private static Set<Throwable> identitySet() {
        return Collections.newSetFromMap(new IdentityHashMap<>());
    }
}
