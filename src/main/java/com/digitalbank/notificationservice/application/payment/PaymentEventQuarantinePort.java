package com.digitalbank.notificationservice.application.payment;

public interface PaymentEventQuarantinePort {

    void quarantine(PaymentEventQuarantine record);
}
