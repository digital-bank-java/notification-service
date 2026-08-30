# Notification Service Agent Guide

## Scope

This repository owns notification delivery boundaries. Keep provider credentials, tokens, and secrets out of source control.

## Environments

Use `sit`, `uat`, and `prod` as the formal runtime profiles. SIT runs on local Docker Desktop Kubernetes; UAT and PROD are future AWS environments. Workstation execution is a debugging technique, not a fourth environment.

## Architecture

Keep notification policy and orchestration in application/domain code. HTTP and future Kafka consumers are inbound adapters. Provider clients and persistence are outbound adapters. Kafka consumer behavior is separate from this bootstrap.

## Verification

Run `./mvnw verify`, Helm lint/template validation, and `git diff --check` before opening a PR. Never commit secrets or merge directly to `main`.
