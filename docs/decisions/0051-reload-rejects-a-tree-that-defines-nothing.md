# ADR-0051: A reload rejects a tree that defines nothing while pipelines run

Status: accepted · Date: 2026-11-18 · Supersedes the scope of [ADR-0043](0043-validate-profile-fails-on-an-empty-configuration.md)

## Context

ADR-0043 made the `validate` profile fail on a tree that defines no Kafka cluster, no
Elasticsearch cluster and no pipeline, because that almost always means the configuration
was not read. It left a normal start and a reload unchanged: both only warn.

A reload re-reads the config data (ADR-0028). If the mounted `application-kafkafim.yaml`
is momentarily empty or absent under an optional location, or a Helm render produced an
empty group, the new tree binds with nothing in it, validates (with a warning), and the
diff removes every running pipeline. The pod then consumes nothing, and the reload
counts as a success (issue #391). The reasoning of ADR-0043 applies more strongly to a
running pod, where the damage is a silent stop.

## Decision

(Matan, 2026-11-18, issue #391: reject.)

- `ConfigReloader.reload` rejects a tree that defines no Kafka cluster, no Elasticsearch
  cluster and no pipeline while the running tree has at least one pipeline. The running
  configuration stays, `kafkafim_config_reload_total{result="failure"}` increments, and
  the response is `rejected` (HTTP 400) with one error that says what was found and how
  to wipe on purpose.
- The predicate is the one of the `validate` profile, shared as
  `ValidateProfile.definesNothing`; the bound tree is judged, not property names.
- A deliberate wipe stays possible: keep the clusters defined and drop the pipelines
  (ADR-0043's "deliberately empty group"). That tree applies and drains every pipeline.
- A reload whose running tree has no pipelines is unchanged: an empty tree is accepted.
- A normal start is unchanged: no pipelines stays a warning.

## Consequences

- An empty or unread file can no longer drain a pod. The watcher does not retry a
  rejected tree until the files change again (RUNBOOK §4.2).
- Removing the last pipeline of a group together with all its clusters is no longer one
  reload. The group keeps its cluster maps (the chart renders them shared), so the
  Helm path is unaffected.

## Alternatives

- Accept and only warn loudly, with a metric: the pod still stops consuming until
  someone notices.
- Reject any reload that removes every pipeline: blocks the deliberate wipe.
