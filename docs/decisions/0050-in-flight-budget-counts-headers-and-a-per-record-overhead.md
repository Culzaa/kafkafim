# ADR-0050: The in-flight budget counts headers and a fixed per-record overhead

Status: accepted · Date: 2026-11-18 · Supersedes the budget bullet of
[ADR-0025](0025-own-offset-tracker-instead-of-async-acks.md)

## Context

ADR-0025 charged each pending record its key and value bytes against `max-in-flight-bytes`.
A tombstone (null key and value) was charged 0 and a one-byte record 1, yet every pending
record costs a few hundred bytes of heap, and headers (copied to the DLQ, ADR-0027) were not
counted. When Elasticsearch or the DLQ stalls and records stay incomplete, the budget never
tripped for such records while the consumer kept polling, so the real bound was record count
times per-record heap, not `max-in-flight-bytes` (ADR-0010).

## Decision

Each pending record is charged `key + value + header bytes (key and value of each header) +
256` bytes against the budget. The 256 is a fixed estimate of the record object, its
coordinates, the completion handle and the tracker's bookkeeping; it is not configurable.
`SourceRecord.sizeBytes()` stays key + value (it feeds `kafkafim_consumed_bytes`), and
`SourceRecord.budgetBytes()` is the charged amount, acquired when the record is tracked and
released once when it completes or is abandoned. `kafkafim_in_flight_bytes` reports the
budget's sum.

## Alternatives

- A separate record-count limit: another knob for operators; the byte budget with an
  overhead term bounds both.
- A configurable overhead: false precision for an estimate; revisit if sizing shows it matters.

## Consequences

For small records the budget now trips earlier (a 100-byte record counts about 3.5 times as
much), so a pipeline of tiny records holds fewer in flight at the same setting; sizing
guidance counts the overhead. `kafkafim_in_flight_bytes` is no longer a payload byte count.
