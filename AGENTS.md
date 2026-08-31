# Notification Service Agent Guide

## Repository Purpose

This repository owns the deployable notification-service runtime for the Digital
Bank Java platform. It exposes framework health endpoints, service-owned
OpenAPI metadata, and an opt-in transfer-created Kafka consumer with a durable
PostgreSQL inbox, notification work row, and quarantine path.

## Scope

Notification templates, delivery providers, provider-backed retries, gateway
routes, and business endpoints are tracked follow-up work and must not be added
to this service slice. The transfer-created consumer owns its validated inbound
Kafka boundary, durable inbox/work persistence, conflict quarantine, and
retry-exhaustion recovery; it must not invent notification content or call an
external provider.

Keep provider credentials, tokens, and secrets out of source control.

## Environments

Use `sit`, `uat`, and `prod` as the formal runtime profiles. SIT runs on local Docker Desktop Kubernetes; UAT and PROD are future AWS environments. Workstation execution is a debugging technique, not a fourth environment.

## Architecture

Keep notification policy and orchestration in application/domain code. HTTP and
Kafka consumers are inbound adapters. Provider clients and persistence are
outbound adapters. The transfer-created consumer validates the governed event
envelope, then delegates to the application boundary.

The service name and Config Server application name are
`notification-service`. The default and SIT HTTP port is `8088`. Helm passes
its `service.port` as `SERVER_PORT`, so the deployment port is authoritative
even when Config Server is reachable; config-repo should keep its matching
`server.port` default for non-Helm startup.

The current delivery foundation is transport-neutral. Use
`NotificationDeliveryInputPort` and `NotificationDeliveryService` for
correlation, idempotency-key replay/conflict handling, and deterministic
retryable versus terminal outcome classification. The current registry is
process-local and intentionally temporary; do not treat it as durable
production state. Provider adapters, credentials, message content, and provider
retry execution belong to later tracked tasks. The transfer-created consumer
uses PostgreSQL as the durable inbox and quarantine state. Its Kafka error
handler retries retryable failures and sends exhausted records through the
quarantine port before allowing offset recovery. Quarantine stores bounded
metadata and a payload hash, never the raw payload; payload re-drive is a
tracked follow-up.

## Local Commands

Normal startup requires Config Server and the `sit` profile. Automated tests
disable Config Client and must not depend on an external service.

```bash
./mvnw test
./mvnw verify
helm lint helm --strict --values helm/values-sit.yaml
helm template notification-service helm --namespace digital-bank-sit --values helm/values-sit.yaml
docker build -t digital-bank-java/notification-service:<tag> .
```

The Maven Wrapper is included; Java 21, Docker, Helm 4, and `kubectl` are
required for the full local delivery workflow.

## Container And Helm Rules

The image and pod run as numeric user/group `10001:10001`. Helm sets the
Config Server URL and `sit` profile, disables service-account token mounting,
uses a read-only root filesystem with writable `/tmp`, drops all capabilities,
and probes the Actuator health groups.

## Verification And Delivery

Run `./mvnw verify`, Helm lint/template validation, the container smoke test,
and `git diff --check` before opening or updating a PR. Use a tracked issue, a
dedicated branch, and a non-draft PR. Never commit secrets or merge directly
to `main`.
