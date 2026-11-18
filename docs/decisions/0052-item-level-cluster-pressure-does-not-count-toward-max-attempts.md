# ADR-0052: Item-level cluster pressure does not count toward `max-attempts`

Status: accepted · Date: 2026-11-18 · Refines the item-retry bound of
[ADR-0026](0026-bulk-retry-loop-in-the-sink.md) for pressure errors;
[ADR-0011](0011-dlq-on-source-kafka-cluster.md) stays

## Context

ADR-0026 retries every retryable item error up to `dlq.max-attempts` (default 10) or
`dlq.max-elapsed`, then dead-letters it. With the default backoff the ten attempts take about
51 seconds. An item answered `429 es_rejected_execution_exception` (write queue full) or
`503 unavailable_shards_exception` (rolling restart, relocation) says the cluster is overloaded
or moving shards, not that the document is bad. A sustained overload therefore dead-lettered
healthy documents after about a minute, which contradicts the rule that an Elasticsearch
problem shows as lag, not as DLQ records (#397).

## Decision

- The classifier gives item 429 of any error type (`es_rejected_execution_exception`,
  `circuit_breaking_exception`, ...) and item 503 `unavailable_shards_exception` their own
  verdict, `RETRY_PRESSURE`. A pressure attempt does not increment the item's `max-attempts`
  counter and is not checked against `max-elapsed`. The exception is `cluster_block_exception`
  (any status): the closed-index and cluster-block rule comes first and keeps the bounded retry.
- The delay comes from the configured backoff on a separate pressure counter, capped at 30
  seconds.
- A fixed safety valve of one hour since the item's first pressure failure sends it to the
  DLQ as `es_retries_exhausted`, so a document that Elasticsearch mislabels as pressure cannot
  retry forever. It is a constant, not a setting: kafkafim stays light.
- Item 404 `index_not_found_exception`, other 5xx items and unexpected item statuses keep the
  `max-attempts`/`max-elapsed` bound.

## Consequences

- Sustained item-level overload stalls the partition (lag) instead of filling the DLQ. An
  operator watches `kafkafim_retries_total{scope=item}` and Elasticsearch's write queues.
- Pressure items still do not feed the circuit breaker: the response is a 200, so the breaker
  sees a success. Reporting a pressure share to the breaker is a possible follow-up.
- DESIGN §7 and RUNBOOK (item-level paragraph) are updated.
