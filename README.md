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

## Runtime Configuration

The service reads runtime configuration from the external Config Server:

```properties
spring.application.name=notification-service
spring.config.import=configserver:${CONFIG_SERVER_URL:http://localhost:8888}
```

The formal environments are `sit`, `uat`, and `prod`. Local Kubernetes SIT uses the `sit` profile. Credentials, provider keys, and tokens must be supplied by deployment secrets and must never be committed here.

## Verify Locally

```bash
./mvnw verify
helm lint helm --strict --values helm/values-sit.yaml
helm template notification-service helm \
  --namespace digital-bank-sit \
  --values helm/values-sit.yaml \
  | kubectl apply --dry-run=client -f -
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

The service-owned contract is available at:

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
