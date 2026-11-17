# ADR-0019: Testing: unit tests for classification and routing, Testcontainers for end-to-end, explicit chaos cases

Status: accepted; the load test bullet is amended below: the generator is `loadtest/`, not
Testcontainers, and results are in `docs/LOADTEST.md` · Date: 2026-11-07

## Decision

- Unit: routing rules and index patterns, timestamp parsing per format, decoder (object / array /
  rejects), bulk body builder, ES error classification (recorded responses),
  offset tracker (out-of-order completion, revoke), config diff.
- Integration (Testcontainers Kafka + Elasticsearch): happy path, JSON array
  explosion, mapping conflict → DLQ, 429 storm → retry then success, ES down
  mid-batch → no loss, rebalance mid-batch → no loss, SIGTERM mid-batch → no loss,
  hot reload of a running pipeline, missing DLQ topic → `failed_preflight`, wrong
  ES credential → pipeline paused with auth-failure metric, SCRAM-SHA-256 and basic
  auth identities against secured containers.
- Load: a Testcontainers-based generator against the compose stack; results recorded in `docs/perf.md`.
  *(Amended 2026-11-17: neither exists. The load test is the hand-run harness in
  [`loadtest/`](../../loadtest/README.md) (`run.sh`, `Produce.java`, `compose.yaml`, issue
  #184), outside `./mvnw verify` and CI, with no Testcontainers; results are recorded in
  [`docs/LOADTEST.md`](../LOADTEST.md).)*

## Consequences

CI runs unit + integration on every PR (Docker available on GitHub runners).
