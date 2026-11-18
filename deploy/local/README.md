# Local Kubernetes demo

One `helm install` brings up everything on a local Kubernetes cluster: a Kafka broker,
Elasticsearch, an APM Server that receives kafkafim's OTLP, Kibana on that Elasticsearch,
Grafana LGTM (dashboards), and kafkafim, installed by the real chart ([charts/kafkafim](../../charts/kafkafim)). Secrets take the
production path: Kubernetes `Secret`s mounted as files and read through
`configtree:/etc/kafkafim/secrets/` ([HELM.md](../../docs/HELM.md#secrets-and-configtree-naming)).
On Kubernetes kafkafim refuses the `local` profile and inline secrets, so this is the
way to see a chart install work before production. For day-to-day development use
`./mvnw spring-boot:run` with Compose instead ([README](../../README.md#develop)).

Local only. In production Kafka, Elasticsearch, Grafana, and the APM Server that
receives OTLP are external, and `charts/kafkafim` deploys only kafkafim (ADR-0015). The
demo is a separate, demo-only chart, [kafkafim-demo](kafkafim-demo), which is never
packaged or released
([ADR-0042](../../docs/decisions/0042-local-kubernetes-demo-is-one-umbrella-chart.md)).
In production the dashboards and alert rules come from the release (its air-gap bundle
or its assets) and are loaded by hand
([deploy/grafana](../grafana/README.md#where-they-ship)); the demo loads the same
dashboards from this checkout. It does not load the alert rules: otel-lgtm's Prometheus
has no `rule_files`, as in the Compose demo.

## What it deploys

One release, `kafkafim`, in namespace `kafkafim-demo`:

| Piece | From | Notes |
|---|---|---|
| `kafka` | [templates/kafka.yaml](kafkafim-demo/templates/kafka.yaml) | `apache/kafka:4.3.1`, one KRaft node, data in the pod's `emptyDir` only. SASL/PLAIN on `kafka:9092` for kafkafim; plaintext on the pod's loopback for the demo's own `kubectl exec` commands. Topic auto-creation off. |
| Topics `demo.events`, `demo.events.kafkafim-dlq` | the `topics` container of the `kafka` pod | kafkafim never creates topics (ADR-0021). The pod becomes ready, and the `kafka` Service gets its endpoint, only once they exist, so kafkafim never sees the broker without them. |
| `elasticsearch` | [templates/elasticsearch.yaml](kafkafim-demo/templates/elasticsearch.yaml) | `elasticsearch:9.5.6`, single node, security on (basic auth), TLS off, no persistence. |
| `apm-server` | [templates/apm-server.yaml](kafkafim-demo/templates/apm-server.yaml) | `apm-server:9.5.6`, OTLP/HTTP on port 8200, writing to the demo Elasticsearch as `elastic`. No secret token, TLS off. This is kafkafim's OTLP endpoint, as the APM Server is in production. |
| `kibana` | [templates/kibana.yaml](kafkafim-demo/templates/kibana.yaml) | `kibana:9.5.6` on port 5601, connecting to Elasticsearch as `kibana_system`. An init container sets that user's password from its secret on every start. |
| `lgtm` | [templates/lgtm.yaml](kafkafim-demo/templates/lgtm.yaml) | `grafana/otel-lgtm:0.41.0` (the Compose version): OpenTelemetry Collector, Prometheus, Tempo, Loki, and Grafana in one container, no persistence. Both dashboards of [deploy/grafana](../grafana/README.md) are loaded into the *kafkafim* folder. kafkafim does not send to it (see below). |
| Secrets `kafka-demo-kafkafim`, `es-demo-kafkafim`, `kibana-demo-system` | [templates/secrets.yaml](kafkafim-demo/templates/secrets.yaml) | `username` + a random `password`, generated on install and kept on upgrade. The broker and Elasticsearch read them as env vars; kafkafim mounts them as configtree files; Kibana reads `kibana-demo-system`. |
| kafkafim (`deploy/kafkafim-demo`) | `charts/kafkafim` as a subchart, values under `kafkafim:` in [values.yaml](kafkafim-demo/values.yaml) | Group `demo` (one replica) running the Compose demo's `demo` pipeline; image `kafkafim:dev` built from this checkout. OTLP export goes to `http://apm-server:8200` through the chart's one endpoint value, `otel.endpoint`. |

Nothing is copied into the demo chart. `kafkafim-demo/charts/kafkafim` is a symlink to
the production chart, so there is no `helm dependency build` step. `kafkafim-demo/files/`
holds symlinks to `deploy/grafana/dashboards` and `deploy/grafana/provisioning`; the
chart renders them into ConfigMaps mounted where `compose.yaml` mounts the same
directories. On Windows, check out with `git config core.symlinks true`.

## Requirements

Docker, `kubectl`, `helm`, and either [kind](https://kind.sigs.k8s.io/) or Docker
Desktop's Kubernetes (kubeadm mode, which shares Docker's images). JDK 27 to build the
jar, as for the rest of the repository. With Nix:
`nix shell nixpkgs#kind nixpkgs#kubectl nixpkgs#kubernetes-helm`.

Memory: the pods request about 5.5 GiB in total (Kafka 768 Mi + 256 Mi for its topics
container, Elasticsearch 1.5 Gi requested and 2 Gi limit with a 1 GiB heap, LGTM 1 Gi,
kafkafim 768 Mi, Kibana 768 Mi requested and 1.5 Gi limit, APM Server 256 Mi). Give Docker
at least 8 GiB.

## Run it

```bash
deploy/local/demo.sh up       # jar + kafkafim:dev image, kind cluster, then the one helm install
```

```bash
deploy/local/demo.sh smoke    # one record into demo.events; waits until it is in demo-eu-<date>
```

```bash
deploy/local/demo.sh down     # deletes the kind cluster
```

`demo.sh up` only prepares what Helm cannot (the image and the cluster). The install
itself is one command, which also works on its own against any cluster that can run the
`kafkafim:dev` image:

```bash
helm upgrade --install kafkafim deploy/local/kafkafim-demo -n kafkafim-demo --create-namespace --wait --timeout 10m
```

`CLUSTER=docker-desktop` uses Docker Desktop's cluster instead (`down` then removes
the release and the namespace). `SKIP_BUILD=1` reuses an existing `kafkafim:dev`
image. `up` is safe to re-run: after a code change, run it again and restart the pods
(`kubectl --context kind-kafkafim-demo -n kafkafim-demo rollout restart deploy/kafkafim-demo`),
since the tag stays `dev`. The cluster pulls the Kafka, Elasticsearch, and LGTM images
itself on the first install.

## Look around

The commands below use the kind context; with `CLUSTER=docker-desktop` replace
`--context kind-kafkafim-demo` with `--context docker-desktop`.

Grafana, with anonymous admin access:

```bash
kubectl --context kind-kafkafim-demo -n kafkafim-demo port-forward svc/lgtm 3000:3000
```

Then open <http://localhost:3000>:

Grafana no longer receives kafkafim's metrics, traces, or logs, because the demo sends
them to the APM Server. Its dashboards and Explore views stay empty unless
`otel.endpoint` points at `http://lgtm:4318` again. The Tempo and Loki queries that
used to work there are now in Kibana (below).

Kibana, through the port-forward:

```bash
kubectl --context kind-kafkafim-demo -n kafkafim-demo port-forward svc/kibana 5601:5601
```

Then open <http://localhost:5601> and sign in as `elastic`. Its password is the
`es-demo-kafkafim` secret (generated on install):

```bash
kubectl --context kind-kafkafim-demo -n kafkafim-demo get secret es-demo-kafkafim -o jsonpath='{.data.password}' | base64 -d; echo
```

- *Observability → APM → Services → kafkafim*: transactions `demo.events process` (the
  Kafka span for one record, which is microseconds long) and `pipeline.batch`, each with
  its `es.bulk` child. The application default keeps 1% of batches and every failed one;
  the demo traces every batch (`MANAGEMENT_TRACING_SAMPLING_PROBABILITY=1.0` in the demo
  values). Health probes are traced too, so filter by `transaction.name` to see the
  pipeline.
- Metrics, in the APM metrics indices (`metrics-apm.app.kafkafim-*`). Some `NaN` values
  are rejected by Elasticsearch (see [#475](https://github.com/culzaa/kafkafim/issues/475)).
- *Discover*, data view `demo-*`: the documents `demo.sh smoke` indexes.

kafkafim itself:

```bash
kubectl --context kind-kafkafim-demo -n kafkafim-demo logs deploy/kafkafim-demo -f
```

```bash
kubectl --context kind-kafkafim-demo -n kafkafim-demo port-forward svc/kafkafim-demo 8080:8080
```

With the port-forward running, `curl -s localhost:8080/actuator/health` and
`curl -s localhost:8080/actuator/metrics/kafkafim.pipeline.state` show the
pipeline. Hot reload works as in production: edit the pipeline under `kafkafim.config`
in `deploy/local/kafkafim-demo/values.yaml` (for example a route's `index`), run the
`helm upgrade --install` above again, and the pod reloads within about a minute without
a restart (HELM.md, *Operating notes*).

## Verified

On 2026-11-14, on the branch with main `34accd9` merged, with kind 0.33.0 (node image
`kindest/node:v1.37.0`), Helm 4.3.1 and Docker Desktop for Mac (7.7 GiB):

- `demo.sh up` on a new cluster: the one `helm upgrade --install` finished in about a
  minute with images already pulled; all four pods ready with no restarts
  (`kafka` 2/2, the others 1/1). The kind node used 4.0 GiB with everything running.
- kafkafim went `stopped` → `starting` → `running` on its own. (On 2026-11-13 it was
  also seen starting while the broker pod was still creating the topics, staying in
  `starting` until they existed.)
- `demo.sh smoke`, ten times: each record indexed in `demo-eu-<date>`.
- Grafana 11.6.0 through the port-forward: *kafkafim / Overview* and *kafkafim /
  Elasticsearch* are in the *kafkafim* folder. Overview: 21 of 22 panels have data
  (`running` = 1 for pipeline `demo`, end-to-end latency, records, documents by route,
  one DLQ record from a deliberately bad timestamp); *Pause reasons by pod* is empty
  because the pipeline never paused. Elasticsearch: 9 of 11 panels have
  data (breaker `closed`, bulk requests with status 200, bulk latency); *Retries by
  scope* and *Item errors by type* are empty because the demo had no retry or item error.
- Tempo: five `smoke` records gave five `pipeline.batch` traces of service `kafkafim`,
  each with its `es.bulk` child span (`http.response.status_code` 200,
  `kafkafim.pipeline` `demo`). Before the demo sampled every batch, the same run with
  ten records had no batch trace.
- Loki (on 2026-11-14, a new cluster on main `c29bf90`, which exports logs over OTLP,
  two `smoke` runs): the `service_name` label had the value `kafkafim`, and the query
  `{service_name="kafkafim"}` through Grafana's Loki data source returned kafkafim's
  log lines (Kafka consumer subscription messages, for example offset resets). Checked through the
  data source's HTTP API, not in the *Explore* page.
- Running the same `helm upgrade --install` again kept both generated passwords, and
  `smoke` passed after it.
- After a restart of the broker container alone (SIGTERM to the broker; same pod,
  restart count 1) the topics were still there and `smoke` passed (run on 2026-11-13,
  not repeated).
- `demo.sh down`. `helm lint` passes and the rendered chart passes `kubeconform -strict`.

The sections above are from kind and the Grafana/LGTM setup.

On 2026-11-18, on Docker Desktop's Kubernetes (`CLUSTER=docker-desktop`, node
`containerd` 2.2.1, Docker Desktop 29.7.2, 8 GiB VM), with this branch:

- `helm upgrade --install` with all six pods ready: kafkafim, Kafka (2/2),
  Elasticsearch, APM Server, Kibana, LGTM.
- The image had to be loaded by hand, since `demo.sh up` does not do it yet
  ([#476](https://github.com/culzaa/kafkafim/issues/476)):
  `docker save kafkafim:dev | docker exec -i desktop-control-plane ctr -n k8s.io images import -`.
- `demo.sh smoke`: records indexed in `demo-eu-<date>` and traces `demo.events process` →
  `pipeline.batch` → `es.bulk` in the APM indices.
- Kibana `/api/status` returns 200 and signs in as `elastic`.
- Elasticsearch was OOM-killed at its former 1 Gi limit. That is why the limit is now 2 Gi.

`CLUSTER=kind` with the APM Server and Kibana has not been run.
