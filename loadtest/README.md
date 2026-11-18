# Load test

A repeatable load test for kafkafim: throughput and latency per pipeline, memory under
`max-in-flight-bytes`, behaviour during an Elasticsearch outage and during a rebalance.
Results and sizing guidance are in [docs/LOADTEST.md](../docs/LOADTEST.md).

Not part of `./mvnw verify`, CI, or the release. It needs Docker and a built jar, and it is
meant to be run by hand on a machine you can leave alone while it runs.

## What it starts

- [compose.yaml](compose.yaml): one Kafka broker (`apache/kafka:4.3.1`, 1 GiB heap, 2 CPUs) on
  `localhost:19092` and one Elasticsearch node (`9.5.6`, 1 GiB heap, 2 CPUs, security off) on
  `localhost:19200`. Ports differ from the development stack, so both can run side by side.
- kafkafim itself runs from the jar on the host (`java -jar`) with the production `kafkafim`
  profile and a generated `application-kafkafim.yaml` in the run directory. OTLP export is
  switched off; the numbers come from the actuator's `/actuator/metrics/<name>` and Elasticsearch `_count`.
  The actuator has no histogram buckets, so a run reports the e2e latency mean and maximum, not percentiles.
- [Produce.java](Produce.java) runs on the host (JDK 27, Kafka client jars taken from the kafkafim
  jar) and writes JSON documents with
  a unique id, so Elasticsearch ends with exactly one document per produced record and a
  redelivery overwrites instead of adding.

## Run

```bash
./mvnw -DskipTests package                      # builds target/kafkafim-*.jar (JDK 27)
loadtest/run.sh throughput                      # also: latency, memory, outage, rebalance
loadtest/run.sh down                            # remove the containers and their data
```

Run `down` when you change `PIPELINES` between runs: a run deletes only the indices of its own
pipelines, and leftovers from a run with more pipelines would count as documents.

Settings are environment variables (defaults in [run.sh](run.sh)): `PIPELINES`, `PARTITIONS`,
`DOCS` (per pipeline), `DOC_BYTES`, `RATE` (docs/s per pipeline), `MAX_IN_FLIGHT`, `HEAP`,
`BULK_IN_FLIGHT`, `OUTAGE_AT`, `OUTAGE_FOR`. Example: `PIPELINES=4 DOCS=500000 loadtest/run.sh throughput`.

## Scenarios

| Scenario | What it does | Result |
|---|---|---|
| `throughput` | Fills the topics with `DOCS` documents per pipeline, then starts kafkafim and times the drain. | documents/s into Elasticsearch, per run and per pipeline |
| `latency` | Produces at `RATE` docs/s per pipeline while kafkafim runs. | `kafkafim.e2e.latency`: mean and maximum |
| `memory` | Same as `throughput` with a backlog far larger than `MAX_IN_FLIGHT`; vary `MAX_IN_FLIGHT` and `HEAP`. | peak `kafkafim.in.flight.bytes`, peak RSS, throughput |
| `outage` | Produces at `RATE`; stops Elasticsearch `OUTAGE_AT` seconds in, for `OUTAGE_FOR` seconds. | in-flight bytes while down, recovery time, final document count |
| `rebalance` | Produces at `RATE`; a second kafkafim instance joins after `OUTAGE_AT` seconds and leaves `OUTAGE_FOR` seconds later. | latency spike, redelivered documents, final document count |

Each run writes `loadtest/results/<time>-<scenario>/` (git-ignored): `run.log`,
`samples.csv` (one line per second), `metrics-final.txt`, the application logs and the
generated configuration. `loadtest/summary.sh <run-dir>` prints the summary again.

## Reading the numbers

A single machine runs the load generator, Kafka, Elasticsearch, and kafkafim, so absolute
numbers are a floor for what the pipeline can do, not a capacity plan. Compare runs on the
same machine, and record its CPU and memory next to the numbers.
