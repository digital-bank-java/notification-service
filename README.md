# Notification Service

Notification delivery service foundation for the Digital Bank Java platform. The service is built with Spring Boot 4.0.7 and Java 21 and is prepared for future event-driven notification workflows.

## Current Scope

This slice establishes a deployable service boundary only:

- Config Client integration for externalized runtime configuration;
- Actuator health, liveness, and readiness endpoints;
- service-owned OpenAPI metadata;
- container and Helm packaging for local Kubernetes SIT;
- Maven verification with Spotless, JaCoCo, Surefire, and Failsafe.

Notification templates, delivery providers, Kafka consumers, retries, persistence, and user-facing notification behavior are future work. No notification business endpoint is exposed by this scaffold.

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
before service-specific Config Server files are added. Config Server remains a
required dependency for normal startup and may override runtime properties.

## Prerequisites

- Java 21.
- Docker.
- Helm 4.
- `kubectl` for Kubernetes dry-run and rollout checks.
- A running Config Server for normal application startup.

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
test against a mocked Config Server response.

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
