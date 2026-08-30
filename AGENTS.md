# Notification Service Agent Guide

## Repository Purpose

This repository owns the deployable notification-service runtime foundation
for the Digital Bank Java platform. The current service exposes only framework
health endpoints and service-owned OpenAPI metadata.

## Scope

Notification templates, delivery providers, Kafka consumers, retries,
persistence, database migrations, gateway routes, and business endpoints are
tracked follow-up work and must not be added to this bootstrap.

Keep provider credentials, tokens, and secrets out of source control.

## Environments

Use `sit`, `uat`, and `prod` as the formal runtime profiles. SIT runs on local Docker Desktop Kubernetes; UAT and PROD are future AWS environments. Workstation execution is a debugging technique, not a fourth environment.

## Architecture

Keep notification policy and orchestration in application/domain code. HTTP and future Kafka consumers are inbound adapters. Provider clients and persistence are outbound adapters. Kafka consumer behavior is separate from this bootstrap.

The service name and Config Server application name are
`notification-service`. The default and SIT HTTP port is `8088`; Config Server
may override it for a formal runtime environment.

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
