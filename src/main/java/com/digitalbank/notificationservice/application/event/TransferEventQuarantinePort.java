package com.digitalbank.notificationservice.application.event;

public interface TransferEventQuarantinePort {

    void quarantine(TransferEventQuarantine record);
}
