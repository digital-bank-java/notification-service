package com.digitalbank.notificationservice.application.event;

public interface NotificationWorkPort {

    void createIfAbsent(NotificationWork work);
}
