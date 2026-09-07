package com.digitalbank.notificationservice.application.payment;

public interface PaymentNotificationWorkPort {

    void createIfAbsent(PaymentNotificationWork work);
}
