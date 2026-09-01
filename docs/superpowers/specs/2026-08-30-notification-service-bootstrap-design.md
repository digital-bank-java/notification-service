# Notification Service Bootstrap Design

## Goal

Create a minimal, independently deployable notification-service foundation on Java 21 and Spring Boot 4.0.7, aligned with the existing auth-service and MFA-service delivery conventions.

## Scope

The service will provide only runtime and delivery foundations:

- Spring Cloud Config Client integration.
- Actuator health, liveness, and readiness probes.
- Spring MVC and Validation dependencies for future API work.
- Service-owned Springdoc OpenAPI metadata.
- Maven quality configuration for Spotless, JaCoCo, Surefire, and Failsafe.
- Non-root Docker packaging using UID/GID `10001:10001`.
- Helm packaging for the `digital-bank-sit` environment on port `8088`.
- Integration tests for the health and OpenAPI endpoints.
- CI validation for Maven, Helm, and container smoke behavior.
- Repository guidance through `AGENTS.md`, `README.md`, and `CODEOWNERS`.

## Explicit Non-Goals

This bootstrap does not add Kafka consumers, provider integrations, notification delivery, retry behavior, persistence, database migrations, gateway routes, config-repo entries, or business endpoints.

## Architecture

`NotificationServiceApplication` is the only application entry point. Runtime configuration is imported from Config Server using `CONFIG_SERVER_URL`, with service defaults in the application resource file. The Spring context exposes only framework-provided operational endpoints and OpenAPI metadata; no notification domain or adapter layer is introduced until a later story defines business behavior.

## Verification

`NotificationServiceApplicationIT` starts the real Spring Boot application on a random port with Config Client disabled for isolation. It verifies that `/actuator/health` returns HTTP 200 and `UP`, and that `/v3/api-docs` returns the required title, contract version, and internal description. Maven `verify` runs unit and integration-test phases, while CI also validates Helm rendering and builds/runs the container against a mocked Config Server.

## Delivery

The feature branch is `feature/153-notification-service-bootstrap`, based on `main`. The PR targets `main`, closes `digital-bank-java/.github#153`, relates `#60` and `#98`, states that no merge dependency or waiting period exists, and remains open without merging.
