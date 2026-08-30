# Notification Service Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and publish a Java 21 Spring Boot 4.0.7 notification-service deployment foundation on port 8088.

**Architecture:** Keep the service as a minimal Spring Boot application with Config Client runtime import, operational Actuator endpoints, and service-owned OpenAPI metadata. Package the same application as a non-root container and a ClusterIP Helm release; defer all notification business behavior.

**Tech Stack:** Java 21, Spring Boot 4.0.7, Spring Cloud Config 2025.1.2, Spring MVC, Validation, Actuator, Springdoc OpenAPI 3.0.2, Maven Wrapper, Spotless, JaCoCo, Surefire, Failsafe, Docker, Helm, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-30-notification-service-bootstrap-design.md`

## Global Constraints

- Service name and Config Client name are `notification-service`.
- Java version is `21` and Spring Boot version is `4.0.7`.
- Default HTTP port is `8088`.
- OpenAPI title is `Digital Bank Notification Service API` and contract version is `1.0.0`.
- Runtime configuration is imported from `configserver:${CONFIG_SERVER_URL:http://localhost:8888}`.
- Container and pod run as non-root numeric user/group `10001:10001`.
- No Kafka consumption, provider integration, notification delivery, retries, persistence, business endpoint, gateway route, or config-repo change.
- CI must run Maven verification, Helm validation, and a container health/OpenAPI smoke test.

---

### Task 1: Establish Service Build and Runtime Contract

**Files:**
- Modify: `pom.xml`
- Create: `.mvn/wrapper/maven-wrapper.properties`, `mvnw`, `mvnw.cmd`
- Create: `src/main/java/com/digitalbank/notificationservice/NotificationServiceApplication.java`
- Create: `src/main/java/com/digitalbank/notificationservice/configuration/OpenApiConfiguration.java`
- Create: `src/main/resources/application.properties`

**Interfaces:**
- Produces a Spring Boot executable artifact named `notification-service-0.0.1-SNAPSHOT.jar`.
- Produces OpenAPI metadata through a package-local `OpenAPI notificationServiceOpenApi()` bean.

- [ ] **Step 1: Write the failing integration contract**

Create `src/test/java/com/digitalbank/notificationservice/NotificationServiceApplicationIT.java` with real `@SpringBootTest(webEnvironment = RANDOM_PORT)` HTTP checks for `/actuator/health` and `/v3/api-docs`; configure `src/test/resources/application.properties` to disable Config Client and use port `0`.

- [ ] **Step 2: Run the integration test to verify it fails**

Run `./mvnw --batch-mode --no-transfer-progress -DskipTests=false -Dtest=NotificationServiceApplicationIT test`.

Expected result: the test cannot compile or load the missing application because the service build and runtime classes are not yet present.

- [ ] **Step 3: Add the minimal Maven and Spring Boot implementation**

Configure the Spring Boot parent, Java 21, Spring Cloud dependency management, Actuator, Validation, WebMVC, Springdoc WebMVC UI, Config Client, and the corresponding Spring Boot test starters. Add Spotless validation, JaCoCo prepare-agent/report, Surefire includes/excludes for `*Test` and Failsafe includes/executions for `*IT`. Add the application entry point, OpenAPI bean with the exact title/version/internal description, and runtime properties for the service name, port, Config Server import, health probes, and OpenAPI paths.

- [ ] **Step 4: Run the integration test to verify it passes**

Run `./mvnw --batch-mode --no-transfer-progress -DskipTests=false -Dtest=NotificationServiceApplicationIT test` and confirm both health and OpenAPI tests pass.

- [ ] **Step 5: Commit the service foundation**

Run `git add pom.xml .mvn mvnw mvnw.cmd src && git commit -m "feat: bootstrap notification service"`.

### Task 2: Add Container and Helm Delivery

**Files:**
- Create: `.dockerignore`, `.gitattributes`, `.gitignore`
- Create: `Dockerfile`
- Create: `helm/Chart.yaml`, `helm/values.yaml`, `helm/values-sit.yaml`
- Create: `helm/templates/_helpers.tpl`, `helm/templates/deployment.yaml`, `helm/templates/service.yaml`

**Interfaces:**
- Container exposes and serves HTTP on port `8088` as `10001:10001`.
- Helm renders a `Deployment` and `ClusterIP` `Service` named `notification-service` with port `8088`.

- [ ] **Step 1: Add the minimal two-stage non-root Dockerfile**

Build with Eclipse Temurin 21 JDK, run Maven package without tests in the builder, copy the executable JAR into an Eclipse Temurin 21 JRE image, create numeric-equivalent user/group `10001`, set `USER 10001:10001`, expose `8088`, and keep `/tmp` available for a read-only-root-filesystem deployment.

- [ ] **Step 2: Add the Helm chart and SIT override**

Define chart metadata, image/runtime/service defaults, health probes, resource limits, pod/container security contexts, temporary storage, and termination settings. Set `values-sit.yaml` to profile `sit` and Config Server URL `http://config-server:8888`; set the service port and target port to `8088`.

- [ ] **Step 3: Validate the package definitions**

Run `helm lint helm --strict --values helm/values-sit.yaml` and `helm template notification-service helm --namespace digital-bank-sit --values helm/values-sit.yaml > /tmp/notification-service-rendered.yaml`; confirm the rendered file is non-empty and contains port `8088`, the Config Server environment variables, and non-root security settings.

- [ ] **Step 4: Commit delivery packaging**

Run `git add .dockerignore .gitattributes .gitignore Dockerfile helm && git commit -m "build: package notification service for sit"`.

### Task 3: Add Repository Documentation and CI

**Files:**
- Create: `AGENTS.md`
- Modify: `README.md`
- Create: `.github/CODEOWNERS`
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- README documents local Maven, Helm, container, and SIT verification commands.
- CI runs the standard Maven unit/integration verification, formatting check, Helm lint/render, and image smoke test.

- [ ] **Step 1: Document the repository boundary and workflow**

Write `AGENTS.md` with the service purpose, explicit future scope, port `8088`, Config Server/SIT conventions, non-root runtime, commands, and branch/issue/PR rules. Update `README.md` with current scope, non-goals, prerequisites, Maven verification, Helm validation, container build, SIT deployment, OpenAPI access, and the `.github#153` reference.

- [ ] **Step 2: Add ownership and CI**

Set `.github/CODEOWNERS` to `* @digital-bank-java/maintainers`. Configure CI jobs for Maven verification, Helm lint/render, and a dependent container build/smoke job that asserts Docker user `10001:10001`, serves a mocked Config Server response for `notification-service`, checks `/actuator/health`, and checks `/v3/api-docs` on port `8088`.

- [ ] **Step 3: Run repository quality checks**

Run `./mvnw --batch-mode --no-transfer-progress verify`, `helm lint helm --strict --values helm/values-sit.yaml`, `helm template notification-service helm --namespace digital-bank-sit --values helm/values-sit.yaml > /tmp/notification-service-rendered.yaml`, and `git diff --check`.

- [ ] **Step 4: Commit documentation and CI**

Run `git add AGENTS.md README.md .github && git commit -m "ci: add notification service delivery checks"`.

### Task 4: Full Verification, Review, and PR

**Files:**
- Review: all tracked files in the feature branch

- [ ] **Step 1: Run the complete local verification matrix**

Run `./mvnw --batch-mode --no-transfer-progress verify`, `helm lint helm --strict --values helm/values-sit.yaml`, `helm template notification-service helm --namespace digital-bank-sit --values helm/values-sit.yaml > /tmp/notification-service-rendered.yaml`, `git diff --check`, and the Docker build/smoke flow equivalent to CI.

- [ ] **Step 2: Review the final diff against main**

Run `git diff --stat main..HEAD`, `git diff --check main..HEAD`, inspect every changed file, and confirm no Kafka, provider, delivery, retry, persistence, gateway, or config-repo logic was added.

- [ ] **Step 3: Commit any final corrections and request review**

Commit final corrections with a scoped message, then review the final commit range for requirements, security, tests, container user, Helm configuration, and documentation completeness.

- [ ] **Step 4: Push and create the non-draft PR**

Run `git push -u origin feature/153-notification-service-bootstrap`, then open a non-draft PR against `main`. Use correctly rendered Markdown that closes `digital-bank-java/.github#153`, relates `#60` and `#98`, states there is no merge dependency or wait, and explicitly says not to merge.
