package com.digitalbank.notificationservice.adapter.in.kafka;

import com.digitalbank.notificationservice.application.event.TransferEventConflictException;
import com.digitalbank.notificationservice.application.event.TransferEventQuarantine;
import com.digitalbank.notificationservice.application.event.TransferEventQuarantinePort;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;

public final class TransferEventKafkaRecoverer implements ConsumerRecordRecoverer {

    private final TransferEventQuarantinePort quarantine;

    public TransferEventKafkaRecoverer(TransferEventQuarantinePort quarantine) {
        this.quarantine = quarantine;
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        if (exception instanceof InvalidTransferEventException || exception instanceof TransferEventConflictException) {
            return;
        }
        var detail =
                exception == null ? "unknown failure" : exception.getClass().getSimpleName();
        @SuppressWarnings("unchecked")
        var kafkaRecord = (ConsumerRecord<String, String>) record;
        quarantine.quarantine(TransferEventQuarantine.fromKafkaRecord(kafkaRecord, "retry exhaustion: " + detail));
    }
}
