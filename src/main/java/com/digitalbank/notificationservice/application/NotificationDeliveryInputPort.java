package com.digitalbank.notificationservice.application;

import com.digitalbank.notificationservice.domain.DeliveryOutcome;

public interface NotificationDeliveryInputPort {

    NotificationDeliveryResult requestDelivery(NotificationDeliveryCommand command);

    NotificationDeliveryResult recordOutcome(String idempotencyKey, DeliveryOutcome outcome);
}
