package com.digitalbank.notificationservice.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.digitalbank.notificationservice.domain.DeliveryFailureReason;
import com.digitalbank.notificationservice.domain.DeliveryOutcome;
import com.digitalbank.notificationservice.domain.NotificationChannel;
import com.digitalbank.notificationservice.domain.NotificationDeliveryStatus;
import com.digitalbank.notificationservice.domain.exception.NotificationDeliveryNotFoundException;
import com.digitalbank.notificationservice.domain.exception.NotificationDeliveryStateConflictException;
import com.digitalbank.notificationservice.domain.exception.NotificationIdempotencyConflictException;
import org.junit.jupiter.api.Test;

class NotificationDeliveryServiceTests {

    private final NotificationDeliveryService service = new NotificationDeliveryService();

    @Test
    void acceptsDeliveryWithCorrelationAndPendingStatus() {
        var result = service.requestDelivery(command("notify-1", "transfer-1", "alice@example.test"));

        assertThat(result.deliveryId()).isNotNull();
        assertThat(result.correlationId()).isEqualTo("transfer-1");
        assertThat(result.status()).isEqualTo(NotificationDeliveryStatus.PENDING);
        assertThat(result.attemptCount()).isZero();
        assertThat(result.replayed()).isFalse();
    }

    @Test
    void equivalentRequestWithSameIdempotencyKeyReturnsOriginalDeliveryAsReplay() {
        var first = service.requestDelivery(command(" notify-1 ", " transfer-1 ", " alice@example.test "));
        var replay = service.requestDelivery(command("notify-1", "transfer-1", "alice@example.test"));

        assertThat(replay.deliveryId()).isEqualTo(first.deliveryId());
        assertThat(replay.replayed()).isTrue();
        assertThat(replay.status()).isEqualTo(NotificationDeliveryStatus.PENDING);
    }

    @Test
    void changedRequestWithSameIdempotencyKeyIsRejected() {
        service.requestDelivery(command("notify-1", "transfer-1", "alice@example.test"));

        assertThatThrownBy(() -> service.requestDelivery(command("notify-1", "transfer-1", "bob@example.test")))
                .isInstanceOf(NotificationIdempotencyConflictException.class);
    }

    @Test
    void retryableAndTerminalFailuresAreClassifiedDeterministically() {
        assertThat(DeliveryOutcome.from(DeliveryFailureReason.PROVIDER_UNAVAILABLE))
                .isEqualTo(DeliveryOutcome.RETRYABLE_FAILURE);
        assertThat(DeliveryOutcome.from(DeliveryFailureReason.RATE_LIMITED))
                .isEqualTo(DeliveryOutcome.RETRYABLE_FAILURE);
        assertThat(DeliveryOutcome.from(DeliveryFailureReason.INVALID_RECIPIENT))
                .isEqualTo(DeliveryOutcome.TERMINAL_FAILURE);
        assertThat(DeliveryOutcome.from(DeliveryFailureReason.TEMPLATE_REJECTED))
                .isEqualTo(DeliveryOutcome.TERMINAL_FAILURE);
    }

    @Test
    void recordsRetryableOutcomeThenSuccessfulRetry() {
        service.requestDelivery(command("notify-1", "transfer-1", "alice@example.test"));

        var retryable = service.recordOutcome("notify-1", DeliveryOutcome.RETRYABLE_FAILURE);
        var delivered = service.recordOutcome("notify-1", DeliveryOutcome.DELIVERED);

        assertThat(retryable.status()).isEqualTo(NotificationDeliveryStatus.RETRYABLE_FAILURE);
        assertThat(retryable.attemptCount()).isOne();
        assertThat(delivered.status()).isEqualTo(NotificationDeliveryStatus.DELIVERED);
        assertThat(delivered.attemptCount()).isEqualTo(2);
    }

    @Test
    void terminalOutcomeCannotBeChangedByAnotherAttempt() {
        service.requestDelivery(command("notify-1", "transfer-1", "alice@example.test"));
        service.recordOutcome("notify-1", DeliveryOutcome.TERMINAL_FAILURE);

        assertThatThrownBy(() -> service.recordOutcome("notify-1", DeliveryOutcome.DELIVERED))
                .isInstanceOf(NotificationDeliveryStateConflictException.class);
    }

    @Test
    void outcomeForUnknownDeliveryIsRejected() {
        assertThatThrownBy(() -> service.recordOutcome("unknown", DeliveryOutcome.DELIVERED))
                .isInstanceOf(NotificationDeliveryNotFoundException.class);
    }

    private NotificationDeliveryCommand command(String idempotencyKey, String correlationId, String recipient) {
        return new NotificationDeliveryCommand(
                idempotencyKey, correlationId, NotificationChannel.EMAIL, recipient, "transfer-completed");
    }
}
