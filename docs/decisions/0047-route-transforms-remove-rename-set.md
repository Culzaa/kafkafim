# ADR-0047: Routes can transform documents with `remove`, `rename`, and `set`

Status: accepted · Date: 2026-11-17 · Supersedes the transformation parts of
[ADR-0009](0009-declarative-routing-no-expression-language.md) and
[ADR-0020](0020-mvp-scope.md) · Refines
[ADR-0037](0037-routes-can-drop-documents.md)

## Context

ADR-0009 ruled transformations out of scope (enrichment lives in ES ingest pipelines or
in the producer), and ADR-0020 repeated it, adding that none of the excluded items
changes the data path. The second claim is wrong for transformations.
`DocumentSource` is a zero-copy view of the Kafka value, and the parsed tree is shared
and read-only. Changing a document means giving it its own bytes.

The need is small. Teams migrating from Logstash drop a secret field
before indexing, rename a field to match an index template, or stamp a constant such as
`event.dataset`. An ES ingest pipeline does these, but only where the team can create
one (ADR-0021: kafkafim provisions nothing), and each pipeline is a second system to
version. Three operations cover this need without turning kafkafim into a pipeline
engine, and they fit the rule-set style ADR-0009 chose and ADR-0037 extended.

## Decision

### Placement

Transformation is **per route**, not per pipeline: copies of one document in
`route-mode: all` often go to different clusters with different needs. The order for
one document is:

1. Decode, and extract `_id`, version, and timestamp from the **original** document.
2. Route on the original (conditions, drop routes, index pattern resolution).
3. For each selected indexing route that has `transform`, apply its steps, in order, to
   that route's own copy.
4. Serialize and hand the bytes to the sink.

A document that never gets a copy to send (a held `_id`, version, or timestamp error,
no matching route, an `unresolved_index`, or a drop route that cancelled the other copies
in `route-mode: all`) never reaches step 3: no transform runs for it. The route's ES
ingest `pipeline:` runs in Elasticsearch, after the transform.

Everything that reads the document (`when` conditions, `_id`, version, timestamp, the
`%{field}` references of `index`) sees the original. A `remove` of `/id` therefore does
not change the `_id`, and a `set` of `/level` does not change which route was selected.
Fan-out copies are independent: one route's steps never show in another route's copy.

### Closed op set

Exactly three operations. There is no expression language, no conditions inside steps,
no computed values, and no arithmetic. Conditional behaviour comes from routing: a route
with `when` has its own `transform`. A fourth operation needs its own ADR, as a new
condition type does under ADR-0009.

### Configuration

`transform` is an optional list on a route. Each step is a map with an `op` and the
fields that op takes. These are plain Spring properties that Helm renders from values:

```yaml
routes:
  - name: eu
    when: [ { field: /region, equals: eu } ]
    cluster: search-eu
    index: "orders-eu-%{+yyyy.MM}"
    transform:
      - { op: remove, field: /card/number }
      - { op: rename, field: /usr, to: /user }
      - { op: set, field: /event/dataset, value: orders }
      - { op: set, field: /event/version, json: "2" }
```

| Op | Fields | Effect |
|---|---|---|
| `remove` | `field` | Deletes the member at `field`. |
| `rename` | `field`, `to` | Moves the member at `field` to `to`. |
| `set` | `field`, and exactly one of `value`, `json` | Writes a value at `field`. `value` is a JSON string. `json` is the text of one JSON value (number, boolean, `null`, string, object or array), parsed at bind. |

- `field` and `to` are JSON pointers (RFC 6901) that name **object members**. Unlike `when`
  conditions, they cannot index arrays. A pointer is non-empty (the root cannot be a target),
  and a pointer token is always an object key, so `/a/0` is the member `"0"` of `/a`, never an
  array element. Steps cannot address array elements.
- `set` overwrites an existing member and creates any missing parent objects. `rename`
  overwrites an existing target and creates missing parents of `to`.
- A **missing source is a no-op**: `remove` and `rename` do nothing when `field` is
  absent, or when a parent on the way is absent or not an object. A `null` member is
  present: `remove` deletes it, `rename` moves it.
- Steps run in order, each on the result of the previous one. A later step can read
  what an earlier one wrote.
- **Key order.** Replacing an existing member keeps its position. A new member is
  appended at the end of its object. `rename` removes the source and writes the target
  by the same rule.
- Limits: at most 32 steps per route; a `json` or `value` text of at most 4 KiB; a
  document that would nest deeper than 500 after a step is a `transform` failure
  (below). A `json` text with a duplicate key at any depth is rejected at bind, as the
  decoder rejects it in a record.
- Spring binds a bare YAML 1.1 word such as `no`, `off`, or `on` as a boolean, so a
  `value` that must be that text is quoted in the configuration.
- Validation runs at bind, on startup, on reload, and under the `validate` profile, like
  conditions (ADR-0009, ADR-0012), and fails the whole configuration with the route
  and step position: unknown `op`; a field that does not belong to the op; a missing
  required field; both or neither of `value` and `json`; an invalid pointer or `json`
  text; `rename` with `field` equal to `to`, or one a prefix of the other; `transform`
  on a `drop: true` route (it has no copy to change; ADR-0037).
- A change to a route's `transform` is a route change: reload restarts only its
  pipeline (ADR-0028), and the change applies to records not yet committed.

### Data path

- A route **without** `transform` keeps the zero-copy path of DESIGN §4.5, byte for
  byte, and pays nothing for the feature.
- A route **with** `transform` gets a copy-on-write document that **owns its bytes**.
  The decoder's tree is built for routing and is not exact for decimals (it parses
  fractions and exponents as `double`), so it is never serialized for a transformed copy.
  Instead, the copy is parsed from the document's original bytes by a second mapper with
  `USE_BIG_DECIMAL_FOR_FLOATS` (exact decimals, big integers). Steps copy only the object
  nodes along the changed paths and share every other node within that parse. The result
  is serialized once, as compact JSON on one line, so it is NDJSON-safe, and a new
  `DocumentSource` that owns those bytes replaces the view of the Kafka value for this
  copy. The work lives in `decode` (a `DocumentSource` factory for owned bytes) and is
  called from the supervisor's copy planning; `config` and `route` hold only the
  validated step model (ADR-0003: JDK and Jackson only).
- **Number spelling, key order and whitespace on transformed routes.** Key order is
  preserved, except as the rules above state for new and moved members. Insignificant
  whitespace is dropped, and string escapes are written in Jackson's minimal form.
  Numbers keep their **exact value**: the exact parse above never goes through
  `double`, so integers beyond 64 bits and decimals with many digits survive. Their
  **spelling** is not guaranteed (`1e3` is written `1E+3`, and `-0` is written `0`).
  Elasticsearch reads the same number either
  way. Byte-for-byte output is a property of routes without `transform` only.
- Memory: a transformed copy holds its own serialized bytes until its part completes,
  instead of sharing the one byte copy of its routes. `max-in-flight-bytes` still counts
  the raw record only, so the "held in flight" note of DESIGN §4.5 grows by up to one
  document size per transformed copy, and the exact parse is transient decode memory.

### Failures

A new `error.type=transform` joins `decode`, `missing_id`, `bad_timestamp`, `no_route`,
and `unresolved_index` (DESIGN §7, §9). It is raised for one copy, when a `set` or
`rename` needs a parent that exists but is not an object (`/a/b` when `/a` is a string
or an array), or when a step would nest the document deeper than 500.

- The DLQ record carries the **original bytes** of the Kafka value, never the
  transformed ones, so a replay sees what was produced. The headers are the usual
  ones, including `kafkafim.doc-index` for an array element, `kafkafim.target.cluster`
  and, because the index is resolved before the steps run, `kafkafim.target.index`.
- `kafkafim.error.reason` names the route, the step position, the op, and the pointer.
  It never contains a document value (the reasoning of ADR-0030).
- In `route-mode: all`, only the copy whose transform failed is dead-lettered; the
  other copies go ahead. It is one part, as `unresolved_index` is.
- The error is counted in `kafkafim_dlq_records_total{error_type="transform"}`. There
  is no new metric. `kafkafim_docs_total{outcome="dlq"}` counts it under the route.
- A missing source field is not a failure (see above), so a pipeline whose producers
  omit a field does not fill the DLQ.

### Not in scope

Type coercion, array operations, copy-from-field, templated values, string functions,
conditions in steps, changing `_id`, version, or the index, and transforms that read
Kafka headers. Each needs its own ADR if it is ever wanted.

## Alternatives

- **ES ingest pipelines (`pipeline:` on the route).** Still valid, and still the answer
  for conditional logic, parsing, enrichment, and anything beyond the three ops. The
  cost is that a team needs rights to create them (ADR-0021), and a field removed
  by an ingest pipeline has already crossed the network. Both stay supported.
- **CEL, jq, or JSLT.** Expressive, but a second language in configuration, hard to
  validate at bind time, and the reason ADR-0009 stayed with rules. A closed op set
  can be validated completely before the first record arrives.
- **A Java plugin SPI.** Arbitrary code in the shipper breaks the air-gapped delivery
  model (ADR-0022), the package boundaries (ADR-0003), and the light-footprint goal.
- **Per-pipeline transform.** Simpler to configure, but copies bound for different
  clusters often need different fields removed, and it would run the work for routes
  that need none.
- **Transform before routing.** Conditions would see mutated documents, and a failed
  step could not be separated from a routing failure. Routing on the original keeps
  `when` and `_id` predictable.

## Consequences

- ADR-0009 and ADR-0020 stay as history, with their status lines marked superseded for
  the transformation parts. The expression-language ban and the CLI ban stand.
- The data path now has two shapes (view, and owned copy), and tests must cover both.
- Documents on transformed routes lose byte-for-byte fidelity. That is a deliberate
  trade, visible in this ADR and in DESIGN §4.5.
- Migrating Logstash pipelines that only used `mutate { remove_field, rename,
  add_field }` no longer needs an ingest pipeline first (ADR-0017).
- The `DocumentSource` Javadoc, which says DESIGN §8 indexes documents byte-for-byte,
  is updated in the framework issue.
- Follow-up issues implement it in order: the framework (config, validation, copy-on-
  write document, `transform` error type), then each op.
