package com.digitalbank.notificationservice;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class NotificationServiceApplicationIT {

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @LocalServerPort
    private int port;

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

    private HttpResponse<String> get(String path) throws Exception {
        var request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .GET()
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
