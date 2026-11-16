# Load test results

Results of the repeatable load test in [loadtest/](../loadtest/README.md) (issue #184, the 1.0
roadmap condition in [DESIGN §16](DESIGN.md#16-roadmap)). Method and commands are in the
harness README; this page records what was measured, on what, and what it means for sizing.

Every number below was produced by `loadtest/run.sh` on one laptop. They are **a floor for
one pipeline on one small Elasticsearch node, not a production capacity plan**: the load
generator, Kafka, Elasticsearch and kafkafim all shared the machine, Elasticsearch had two
CPUs and a 1 GiB heap, and documents were small and uniform. Re-run the harness on
production-like hardware before sizing a real cluster.

## Setup

| | |
|---|---|
| Date | 2026-11-15 |
| kafkafim | 0.1.0-SNAPSHOT at `8658634` (plus the harness), jar run with `java -jar` on Temurin 27, profile `kafkafim`, OTLP export off |
| Host | Apple M4, 10 cores, 24 GiB; Docker Desktop VM with 10 CPUs and 8 GiB |
| Kafka | `apache/kafka:4.1.0`, one KRaft node, 1 GiB heap, container limit 2 CPUs / 2 GiB |
| Elasticsearch | `8.19.0`, one node, security off, 1 GiB heap, container limit 2 CPUs / 2 GiB; one shard, no replica |
| Documents | JSON object, about 1 KiB, unique `id`, one route to one index per pipeline |
| Pipeline | 3 partitions per topic unless stated; defaults otherwise: `max-poll-records` 500, bulk 5000 docs / 5 MB / 200 ms, `bulk.in-flight` 4 |
| Load | topic backlog written first (throughput, memory), or a fixed rate while kafkafim runs (latency, outage, rebalance) |

Sizes in configuration (`64MB`) are Spring `DataSize` values, i.e. MiB; the tables use MiB.
Load average on the host was 3 to 9 during the runs (other sessions' work shared the
machine), so treat differences of a few tens of percent as noise.

## Throughput

Backlog drain: the topics were filled first, then kafkafim started; documents per second is
the growth of the Elasticsearch document count from the first non-empty one-second sample to
the last. Elasticsearch ended each run with exactly the produced number of documents.

| Pipelines × documents | Heap | `max-in-flight-bytes` per pipeline | Documents/s (total) |
|---|---|---|---|
| 1 × 500,000 | 512 MiB | 64 MB | 43,700 |
| 1 × 1,000,000 | 512 MiB | 64 MB | 26,500 and 25,800 (two runs) |
| 1 × 1,000,000 | 1 GiB | 256 MB | 47,200 |
| 4 × 250,000 | 1 GiB | 64 MB | 41,000 |
| 4 × 250,000 | 768 MiB | 64 MB | 37,000 |
| 4 × 250,000 | 512 MiB | 16 MB | 41,700 |

About 1 KiB per document, so 25,000 to 47,000 documents/s is about 25 to 47 MB/s of source
data. The spread between the 500,000- and 1,000,000-document single-pipeline runs
(44,000 against 26,000) is larger than the effect of any setting changed in this test; the
likely cause is Elasticsearch merge and refresh work on the larger index and host load, but
that was not isolated. With four pipelines the total did not grow beyond what one pipeline
reaches, so on this setup **Elasticsearch, not kafkafim, was the limit**: adding pipelines
splits the same throughput, it does not add to it. No effect of the budget
(`max-in-flight-bytes`, 16 to 256 MB) on throughput was visible above that noise.

## Latency

`kafkafim_e2e_latency_seconds` (Elasticsearch ack time minus the Kafka record timestamp,
which the load generator sets at send time). The histogram's buckets are fixed
(0.05 s … 3600 s), so percentiles are bucket upper bounds, not interpolated values.
Steady rate, topics with 3 partitions, 600,000 documents per run:

| Load | Mean | p50 | p95 | p99 | Max (last window) |
|---|---|---|---|---|---|
| 1 pipeline, 10,000 docs/s | 0.22 s | ≤ 0.25 s | ≤ 0.5 s | ≤ 2.5 s | 2.0 s |
| 4 pipelines, 2,500 docs/s each (10,000 docs/s total) | 0.31 s | ≤ 0.25 s | ≤ 2.5 s | ≤ 5 s | 3.6 s |

The maximum is `kafkafim_e2e_latency_seconds_max`, a decaying window, so it can understate the run's
true maximum. The pipeline's own floor is the batch age: the default bulk `max-age` of 200 ms plus the
Elasticsearch bulk time. The first seconds after a start show more (one-second mean latency
of 6 to 14 s in the first two samples of the rebalance run, while the consumers ramped up
on a topic already receiving 5,000 docs/s), so quote steady-state numbers only after the
first minute. Latency in the backlog runs (mean 24 to 32 s) measures the backlog's age, not
the pipeline, and is not a latency result.

## Memory under `max-in-flight-bytes`

Four pipelines on one instance, 250,000 documents each (a 1 GB backlog, far above the
budgets), fixed `-Xmx`. "In flight" is `kafkafim_in_flight_bytes` summed over the pipelines;
"heap in use" is the highest sampled `jvm_memory_used_bytes{area="heap"}`, which includes
garbage not yet collected, so it overstates what is retained.

| Heap | Budget per pipeline (total) | Peak in flight | Peak heap in use | Peak RSS | Outcome |
|---|---|---|---|---|---|
| 1 GiB | 64 MB (256 MB) | 247 MiB | 947 MiB | 1,315 MiB | completed |
| 768 MiB | 64 MB (256 MB) | 250 MiB | 758 MiB | 1,045 MiB | completed |
| 512 MiB | 16 MB (64 MB) | 66 MiB | 435 MiB | 760 MiB | completed |
| 512 MiB | 64 MB (256 MB) | n/a | n/a | 780 MiB | **`OutOfMemoryError` in three of three runs**, consumers stopped, stalled at 60,479 and 115,467 of 1,000,000 documents in the two runs that recorded the count |
| 1 GiB (one pipeline) | 256 MB | 256 MiB | 1,006 MiB | 1,266 MiB | completed, heap nearly full |

What this shows:

- The budget is honoured: in-flight bytes peaked at the configured total plus at most one
  poll (4 × 64 MB gave 247 to 250 MiB, 4 × 16 MB gave 66 MiB).
- With about 1 KiB documents the heap needed is a small multiple of the **sum** of the
  budgets of all pipelines in the instance. Completed: 3× (768 MiB for 256 MB), 4× (1 GiB for
  256 MB), 8× (512 MiB for 64 MB). Failed: 2× (512 MiB for 256 MB). One point at 4× with a
  single pipeline completed with the heap almost full.
- Resident memory was the heap plus about 250 to 300 MiB (JVM, threads, Netty and Kafka
  buffers).
- In a container the JVM exits on `OutOfMemoryError` (`-XX:+ExitOnOutOfMemoryError` in the
  image and the chart) and Kubernetes restarts the pod. The harness runs the jar without that
  flag, which is why the failed runs stalled instead of restarting.

The decode peak of very large records ([DESIGN §4.5](DESIGN.md#45-decoding), `Memory`) is
not part of this test, which used 1 KiB documents.

## Elasticsearch outage

One pipeline, 5,000 docs/s (600,000 documents over 120 s), heap 1 GiB. Elasticsearch
was stopped 26 s in and started again 63 s later (measured times; the harness defaults are
`OUTAGE_AT=30` and `OUTAGE_FOR=60`, this run used 20 and 60 plus start-up time); kafkafim ran
unchanged throughout.

| What | Measured |
|---|---|
| Circuit breaker opened, pipeline `paused` | 1.5 s after Elasticsearch stopped |
| In-flight bytes while paused | flat at 4.2 MiB (the byte budget was not reached) |
| Breaker probes while down | half-open every 10 s, re-opened each time |
| Elasticsearch answering again → pipeline `running` | 7 s later (the next half-open probe) |
| Catch-up | the document count grew by about 495,000 between two samples 20 s apart (the resumption fell inside them), in-flight bytes peaked at 63.5 MiB (the budget is 64 MiB) |
| Documents in Elasticsearch at the end | 600,000 of 600,000 produced; none dead-lettered |
| Peak end-to-end latency | 102 s (the outage plus the catch-up) |

During the outage the breaker, not the byte budget, stopped consumption: it opened within
about a second, so the budget only came into play during the catch-up. Memory stayed flat
while paused (heap 60 to 260 MiB), and Kafka held the backlog, as designed (ADR-0010).

## Rebalance

One pipeline on a topic with 24 partitions, 5,000 docs/s, heap 1 GiB per instance. A second
instance of the same configuration joined about 20 s after the producer started and was
stopped (SIGTERM) 47 s later.

| What | Measured |
|---|---|
| Instance A's partitions | 24 → 14 when B joined, back to 24 when B stopped |
| One-second mean latency during both rebalances | at most 0.7 s (no window above it) |
| B's shutdown | every revoked partition reported 0 incomplete records |
| Documents in Elasticsearch at the end | 600,000 of 600,000 |
| Documents indexed more than once | about 36,000 at least (A's counter 554,115 plus B's last sampled counter 81,505, less 600,000); the redelivered documents overwrote themselves by `id` |
| Slowest documents | 457 of A's 554,115 took more than 30 s (maximum 44 s; not attributed to a cause), and about 36,000 more took 10 to 30 s, which matches the two slow samples at the start of the run rather than either rebalance |

Redelivery after a rebalance is the documented at-least-once behaviour (ADR-0006); the
count above is an estimate from two counters, not a per-document audit. A
second instance only receives work if the topic has more partitions than the first
instance's consumers: with 6 partitions the first instance already ran one consumer per
partition (concurrency is capped at the partition count) and the second received no
partition at all.

## Sizing guidance

These follow from the runs above, for small (about 1 KiB) documents; they are starting
points to confirm on production-like hardware.

- **Heap against budgets.** This is an empirical minimum for 1 KiB documents; the
  [RUNBOOK](RUNBOOK.md) gives the fuller rule (budget plus overshoot, per-copy, DLQ, decode and
  bulk-body terms), which takes precedence. Size the heap at no less than three times the sum of
  `max-in-flight-bytes` over all pipelines of an instance, and more when documents are larger
  than 1 KiB or when records approach `max-record-bytes` (the decode peak in DESIGN §4.5 comes
  on top). The measured run used the former chart default (`1Gi` memory, `MaxRAMPercentage` 75, about
  768 MiB of heap): four pipelines at the default 64 MB completed on that heap, but resident
  memory peaked at 1,045 MiB, over a 1 GiB limit, so an OOMKill was to be expected. The chart
  default is now `1536Mi` (heap about 1.1 GiB), per the sizing rule in [HELM](HELM.md#sizing-memory)
  (container limits were not tested here). More than four were not tested. Lower
  `max-in-flight-bytes` rather than raising the heap when pipelines are added: 16 MB per
  pipeline cost no throughput in this test.
- **Container memory.** Plan on heap plus about 300 MiB resident.
- **Throughput.** Plan for the sink (Elasticsearch) to be the limit. Per instance, this
  setup sustained 25,000 to 47,000 documents/s of 1 KiB documents into a two-CPU Elasticsearch
  node. Spreading the same load over more pipelines does not raise the total.
- **Partitions.** An instance runs `min(concurrency, partitions)` consumers per pipeline, where `concurrency`
  is the pipeline's `source.concurrency` or, unset, the CPU count of the pod.
  A second replica adds capacity and takes part of the load only while the topic has more
  partitions than all replicas' consumers together.
- **Outage.** The breaker pauses fetching within seconds of an outage, so memory does not
  grow during it. Recovery after Elasticsearch is back takes about one half-open probe
  interval (10 s) plus the catch-up, which ran at several times the steady rate. Alert on
  `kafkafim_pipeline_state{state="paused"}` and on consumer lag in the Kafka team's
  monitoring (ADR-0034).
- **Latency.** Expect a steady-state mean end-to-end latency in the order of 0.2 to 0.5 s with
  the default 200 ms `max-age`, and a tail of a few seconds (p99 bounded by 2.5 s with one pipeline
  and 5 s with four); lowering `latency.max-age` per pipeline is the knob.

## Not covered

- Larger documents, routes with `all` fan-out, DLQ-heavy traffic, SASL/TLS, and multiple
  Elasticsearch nodes or shards.
- More than four pipelines per instance, and more than one instance at a time except in the
  rebalance run.
- CPU-limited pods (the jar ran on the host, unlimited).
- Duration: every run was under two minutes of steady load, so slow leaks and long-term
  Elasticsearch merge behaviour are not covered. For a soak test run
  `loadtest/run.sh latency` with `DOCS` set for an hour of load and watch the samples.
