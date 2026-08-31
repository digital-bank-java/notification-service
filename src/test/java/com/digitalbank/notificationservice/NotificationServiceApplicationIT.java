package com.digitalbank.notificationservice;

import static org.assertj.core.api.Assertions.assertThat;

import com.digitalbank.notificationservice.application.event.TransferCreatedEvent;
import com.digitalbank.notificationservice.application.event.TransferCreatedEventConsumer;
import com.digitalbank.notificationservice.application.event.TransferEventConflictException;
import com.digitalbank.notificationservice.application.event.TransferEventConsumptionResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class NotificationServiceApplicationIT {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16-alpine");

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @LocalServerPort
    private int port;

    @Autowired
    private TransferCreatedEventConsumer consumer;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void healthEndpointReportsUp() throws Exception {
        var response = get("/actuator/health");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(objectMapper.readTree(response.body()).path("status").asText())
                .isEqualTo("UP");
    }

    @Test
    void openApiEndpointReturnsExplicitMetadata() throws Exception {
        var response = get("/v3/api-docs");
        JsonNode document = objectMapper.readTree(response.body());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(document.path("info").path("title").asText()).isEqualTo("Digital Bank Notification Service API");
        assertThat(document.path("info").path("version").asText()).isEqualTo("1.0.0");
        assertThat(document.path("info").path("description").asText()).contains("Internal notification");
    }

    @Test
    void transferEventInboxAndNotificationWorkAreDurable() {
        var event = new TransferCreatedEvent(
                "it-evt-1",
                "it-transfer-1",
                "it-request-1",
                "transaction-service",
                "1.0.0",
                Instant.parse("2026-08-31T10:15:30Z"),
                "{\"transferId\":\"it-transfer-1\"}");

        consumer.consume(event);
        var replay = consumer.consume(event);

        assertThat(replay.replayed()).isTrue();
        assertThat(jdbcTemplate.queryForObject(
                        "select count(*) from transfer_event_inbox where event_id = ?", Integer.class, event.eventId()))
                .isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject(
                        "select count(*) from notification_work where event_id = ?", Integer.class, event.eventId()))
                .isEqualTo(1);
    }

    @Test
    void concurrentDuplicateDeliveryCreatesOneInboxAndOneNotificationWorkRow() throws Exception {
        var event = new TransferCreatedEvent(
                "it-concurrent-1",
                "it-transfer-concurrent-1",
                "it-request-concurrent-1",
                "transaction-service",
                "1.0.0",
                Instant.parse("2026-08-31T10:15:30Z"),
                "{\"transferId\":\"it-transfer-concurrent-1\"}");
        var executor = Executors.newFixedThreadPool(2);

        try {
            List<TransferEventConsumptionResult> results = executor
                    .invokeAll(List.of(
                            (Callable<TransferEventConsumptionResult>) () -> consumer.consume(event),
                            (Callable<TransferEventConsumptionResult>) () -> consumer.consume(event)))
                    .stream()
                    .map(future -> {
                        try {
                            return future.get();
                        } catch (Exception exception) {
                            throw new AssertionError(exception);
                        }
                    })
                    .toList();

            assertThat(results)
                    .extracting(TransferEventConsumptionResult::replayed)
                    .containsExactlyInAnyOrder(false, true);
            assertThat(jdbcTemplate.queryForObject(
                            "select count(*) from transfer_event_inbox where event_id = ?",
                            Integer.class,
                            event.eventId()))
                    .isEqualTo(1);
            assertThat(jdbcTemplate.queryForObject(
                            "select count(*) from notification_work where event_id = ?",
                            Integer.class,
                            event.eventId()))
                    .isEqualTo(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void changedFingerprintIsRejectedAndDurablyQuarantined() {
        var original = new TransferCreatedEvent(
                "it-conflict-1",
                "it-transfer-conflict-1",
                "it-request-conflict-1",
                "transaction-service",
                "1.0.0",
                Instant.parse("2026-08-31T10:15:30Z"),
                "{\"transferId\":\"it-transfer-conflict-1\"}");
        var changed = new TransferCreatedEvent(
                original.eventId(),
                original.correlationId(),
                original.causationId(),
                original.producer(),
                original.schemaVersion(),
                original.occurredAt(),
                "{\"transferId\":\"different-transfer\"}");

        consumer.consume(original);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> consumer.consume(changed))
                .isInstanceOf(TransferEventConflictException.class);

        assertThat(jdbcTemplate.queryForObject(
                        "select fingerprint from transfer_event_inbox where event_id = ?",
                        String.class,
                        original.eventId()))
                .isEqualTo(original.fingerprint());
        assertThat(jdbcTemplate.queryForObject(
                        "select count(*) from transfer_event_quarantine where event_id = ?",
                        Integer.class,
                        original.eventId()))
                .isEqualTo(1);
    }

    private HttpResponse<String> get(String path) throws Exception {
        var request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .GET()
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
