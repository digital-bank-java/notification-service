# Notification Service

Notification delivery service foundation for the Digital Bank Java platform. The service is built with Spring Boot 4.0.7 and Java 21 and is prepared for future event-driven notification workflows.

## Current Scope

This slice establishes a deployable service boundary only:

- Config Client integration for externalized runtime configuration;
- Actuator health, liveness, and readiness endpoints;
- service-owned OpenAPI metadata;
- an opt-in Kafka consumer boundary for governed transfer-created events;
- container and Helm packaging for local Kubernetes SIT;
- Maven verification with Spotless, JaCoCo, Surefire, and Failsafe.

Notification templates, delivery providers, provider-backed retry execution,
and user-facing notification behavior are future work. No notification
business endpoint is exposed by this scaffold.

## Delivery Lifecycle Foundation

The transport-neutral application boundary is exposed by
`NotificationDeliveryInputPort` and implemented by `NotificationDeliveryService`.
It currently accepts a delivery request containing:

- a correlation ID that links the notification to a business workflow such as a transfer;
- an idempotency key that identifies one logical notification request;
- a channel, recipient, and template ID.

The service normalizes the request before comparing it. Repeating the same
request with the same idempotency key returns the original delivery ID and
marks the result as a replay. Reusing that key for a different request is
rejected as an idempotency conflict. This is process-local foundation logic;
durable idempotency storage will be added with the future notification
persistence and event-consumer work.

Delivery outcomes are explicit:

- `DELIVERED` is terminal success;
- `RETRYABLE_FAILURE` represents provider unavailability, throttling, or timeout;
- `TERMINAL_FAILURE` represents an invalid recipient, rejected template, or unsupported channel.

A retryable failure may be followed by another attempt. A delivered or
terminally failed delivery cannot be changed by a later attempt. Provider
adapters will classify real provider responses and call this boundary in a
future slice. No provider credentials, message content, or external delivery
call belongs in this delivery foundation.

## Transfer Event Consumer Foundation

The service has an opt-in inbound Kafka adapter for the versioned
`events.transfer.created.v1` topic. It validates the event envelope before
handing the event to the application boundary:

- `event-id` identifies the event for replay detection;
- `correlation-id` links the event to the transfer workflow;
- `causation-id` identifies the command or event that caused it;
- `producer` is checked against the configured producer allowlist;
- `schema-version` is checked against the supported version;
- `occurred-at` must be an ISO-8601 timestamp.

The consumer reports an exact duplicate as a replay and rejects a reused event
ID with different content as a conflict. The accepted event identity and
fingerprint are stored in PostgreSQL, so replay detection survives process
restarts. Accepted events also create durable notification work in the same
transaction. Conflicting or invalid events cross an explicit quarantine
boundary that stores identifiers, hashes, Kafka metadata, and a reason, but
never the raw event payload.

Retryable listener failures are retried twice with a one-second fixed backoff.
After retry exhaustion, an explicit recoverer writes the Kafka record metadata,
payload hash, and failure reason to the same PostgreSQL quarantine table in an
independent transaction. Kafka offset recovery occurs only after that durable
write succeeds, so a database or application failure is not silently
acknowledged. Invalid and conflict records are already quarantined by their
originating path and are not written a second time by the recoverer.

Quarantine is an evidence and operator-review path, not a replay queue. The
current contract intentionally does not persist raw event payloads. A durable
payload store and re-drive workflow are deferred to a follow-up issue before
operators need to replay exhausted records from this service.

Notification construction, provider delivery, and provider-backed retry
execution remain later tracked work after the governed event schema is
finalized.

Enable consumption explicitly with the Helm SIT values or equivalent
environment variables:

```properties
NOTIFICATION_TRANSFER_EVENTS_ENABLED=true
NOTIFICATION_TRANSFER_EVENTS_TOPIC=events.transfer.created.v1
NOTIFICATION_EVENTS_ALLOWED_PRODUCERS=transaction-service
KAFKA_BOOTSTRAP_SERVERS=kafka.digital-bank-sit.svc.cluster.local:9092
KAFKA_CONSUMER_GROUP_ID=notification-service
```

The Kafka listener is disabled by default. Enabling it requires the PostgreSQL
connection used by the service and a reachable Kafka broker. Invalid input is
recorded through the durable quarantine port before the listener error is
returned to Kafka error handling.

## Responsibilities And Boundaries

This repository owns application startup, runtime configuration consumption,
operational health endpoints, service-owned OpenAPI metadata, and the
container/Helm delivery foundation.

It does not own customer data, transfer orchestration, Kafka infrastructure,
provider credentials, notification delivery, or configuration repository
changes. Public access will flow through API Gateway after a separately
tracked notification contract exists.

## Runtime Configuration

The service reads runtime configuration from the external Config Server:

```properties
spring.application.name=notification-service
spring.config.import=configserver:${CONFIG_SERVER_URL:http://localhost:8888}
server.port=8088
```

The formal environments are `sit`, `uat`, and `prod`. Local Kubernetes SIT uses the `sit` profile. Credentials, provider keys, and tokens must be supplied by deployment secrets and must never be committed here.

The service defaults to port `8088` so the Helm package remains compatible
before service-specific Config Server files are added. Helm passes its
`service.port` value to the process as `SERVER_PORT`; that deployment value is
authoritative over Config Server `server.port`, keeping the process, Service,
and probes aligned. Config-repo should retain the same `server.port` default
for non-Helm startup. Config Server remains a required dependency for normal
startup and may provide other runtime properties.

## Prerequisites

- Java 21.
- Docker.
- Helm 4.
- `kubectl` for Kubernetes dry-run and rollout checks.
- A running Config Server for normal application startup.
- PostgreSQL for normal runtime startup; Flyway creates the inbox, notification
  work, and quarantine tables.

The Maven Wrapper is included, so a global Maven installation is not needed.

## Verify Locally

```bash
./mvnw verify
helm lint helm --strict --values helm/values-sit.yaml
helm template notification-service helm \
  --namespace digital-bank-sit \
  --values helm/values-sit.yaml \
  | kubectl apply --dry-run=client -f -
git diff --check
```

CI runs the unit phase, Failsafe integration/package verification, Helm lint
and rendering, formatting validation, and a container health/OpenAPI smoke
test against a mocked Config Server response that deliberately conflicts on
`server.port` while the Helm-equivalent `SERVER_PORT=8088` is set.

The integration suite starts PostgreSQL with Testcontainers and verifies
durable replay, concurrent duplicate delivery, conflict quarantine, health,
and OpenAPI metadata. Testcontainers is a test dependency only; production
uses the PostgreSQL service supplied by the deployment environment.

## Container

Build the image:

```bash
docker build -t digital-bank-java/notification-service:0.0.1 .
```

Verify its runtime user:

```bash
docker image inspect \
  --format '{{.Config.User}}' \
  digital-bank-java/notification-service:0.0.1
```

Expected value:

```text
10001:10001
```

## Kubernetes SIT

```bash
helm upgrade --install notification-service helm \
  --namespace digital-bank-sit \
  --create-namespace \
  --values helm/values-sit.yaml \
  --wait \
  --timeout 5m

kubectl rollout status deployment/notification-service \
  --namespace digital-bank-sit \
  --timeout=180s
```

The chart creates a ClusterIP service on port `8088`. API Gateway routing will be added only with a separately tracked contract and routing change.

## OpenAPI

After deploying to SIT, forward the service port:

```bash
kubectl port-forward --namespace digital-bank-sit \
  service/notification-service 8088:8088
```

The service-owned contract is then available at:

```text
GET http://localhost:8088/v3/api-docs
```

Centralized internal documentation will be added after the notification contract and access policy are reviewed.

## Development Workflow

Use a dedicated branch and pull request for every change. Before opening a pull request:

```bash
git status
./mvnw verify
helm lint helm --strict --values helm/values-sit.yaml
git diff --check
```

Relevant organization story: [`.github#60`](https://github.com/digital-bank-java/.github/issues/60).
