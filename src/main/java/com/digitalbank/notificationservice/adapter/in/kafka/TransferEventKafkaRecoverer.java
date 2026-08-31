package com.digitalbank.notificationservice.adapter.in.kafka;

import com.digitalbank.notificationservice.application.event.TransferEventConflictException;
import com.digitalbank.notificationservice.application.event.TransferEventQuarantine;
import com.digitalbank.notificationservice.application.event.TransferEventQuarantinePort;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;

public final class TransferEventKafkaRecoverer implements ConsumerRecordRecoverer {

    private static final int ROOT_CAUSE_DIAGNOSTIC_LIMIT = 200;

    private final TransferEventQuarantinePort quarantine;

    public TransferEventKafkaRecoverer(TransferEventQuarantinePort quarantine) {
        this.quarantine = quarantine;
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        if (hasCause(exception, InvalidTransferEventException.class)
                || hasCause(exception, TransferEventConflictException.class)) {
            return;
        }
        var detail = diagnostic(exception);
        @SuppressWarnings("unchecked")
        var kafkaRecord = (ConsumerRecord<String, String>) record;
        quarantine.quarantine(TransferEventQuarantine.fromKafkaRecord(kafkaRecord, "retry exhaustion: " + detail));
    }

    private static boolean hasCause(Throwable exception, Class<? extends Throwable> type) {
        for (var current = exception; current != null; current = current.getCause()) {
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
        while (rootCause.getCause() != null) {
            rootCause = rootCause.getCause();
        }
        var type = bound(rootCause.getClass().getName());
        var message = rootCause.getMessage();
        return message == null || message.isBlank() ? type : type + ": " + bound(message);
    }

    private static String bound(String value) {
        return value.length() <= ROOT_CAUSE_DIAGNOSTIC_LIMIT ? value : value.substring(0, ROOT_CAUSE_DIAGNOSTIC_LIMIT);
    }
}
