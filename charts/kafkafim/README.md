# kafkafim Helm chart

Deploys [kafkafim](https://github.com/matabar/kafkafim) (Kafka → Elasticsearch
ingest). One release per Kubernetes cluster; each entry under `groups` becomes a
`Deployment`, `ConfigMap`, `PodDisruptionBudget`, `Service`, and optional CPU
`HorizontalPodAutoscaler`. The chart creates no `Secret` and needs no CRDs.

The design, the values shape, and every validation rule are documented in
[docs/HELM.md](https://github.com/matabar/kafkafim/blob/main/docs/HELM.md); `values.yaml` is
commented field by field.

```bash
helm upgrade --install kafkafim oci://ghcr.io/matabar/charts/kafkafim --version <ver> -f values-<cluster>.yaml
```

Requires Kubernetes 1.27+ (`matchLabelKeys` in the zone topology spread). Development:
`helm lint`, `helm template -f ci/<name>-values.yaml | kubeconform -strict`, and
`helm unittest .` (helm-unittest 1.2.1) from this directory.

## Extra objects

`extraObjects` deploys additional Kubernetes resources with the release, for example an
ExternalSecret that creates a Secret listed under `secrets`. Each entry is a string, rendered
through `tpl` (so it can use release values), or a map:

```yaml
extraObjects:
  - apiVersion: v1
    kind: ConfigMap
    metadata:
      name: extra
    data: { key: value }
  - |
    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: {{ .Release.Name }}-extra
```

Trade-offs: the objects bypass `values.schema.json` validation, and a `tpl` error points at
the template rather than at the offending entry. A map entry is not templated.
