#!/usr/bin/env bash
# Local Kubernetes demo of kafkafim (deploy/local/README.md).
#
#   deploy/local/demo.sh up      build the image, create the cluster, then one `helm install`
#                                of deploy/local/kafkafim-demo (Kafka, Elasticsearch, APM
#                                Server, Kibana, Grafana LGTM and kafkafim)
#   deploy/local/demo.sh smoke   send one record to demo.events and wait for it in Elasticsearch
#   deploy/local/demo.sh down    delete the kind cluster (Docker Desktop: the namespace)
#
# CLUSTER=kind (default) or CLUSTER=docker-desktop. SKIP_BUILD=1 reuses kafkafim:dev.
set -euo pipefail

readonly CLUSTER="${CLUSTER:-kind}"
readonly KIND_NAME="${KIND_NAME:-kafkafim-demo}"
readonly NS=kafkafim-demo
readonly IMAGE=kafkafim:dev
readonly RELEASE=kafkafim
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly HERE
ROOT="$(cd "$HERE/../.." && pwd)"
readonly ROOT
CONTEXT="" # kubectl context; set by set_context

#######################################
# Derives the kubectl context from CLUSTER; exits 2 for an unknown CLUSTER.
# Globals:
#   CLUSTER, KIND_NAME (read), CONTEXT (set)
# Arguments:
#   None
#######################################
set_context() {
  case "$CLUSTER" in
    kind) CONTEXT="kind-$KIND_NAME" ;;
    docker-desktop) CONTEXT=docker-desktop ;;
    *)
      echo "CLUSTER must be kind or docker-desktop, not '$CLUSTER'" >&2
      exit 2
      ;;
  esac
}

#######################################
# Runs kubectl against the demo cluster and namespace.
# Globals:
#   CONTEXT, NS (read)
# Arguments:
#   kubectl arguments, passed through
#######################################
k() { kubectl --context "$CONTEXT" -n "$NS" "$@"; }

#######################################
# Exits 2 with a message on stderr unless every named tool is on the PATH.
# Arguments:
#   Tool names to check
#######################################
need() {
  for tool in "$@"; do
    command -v "$tool" >/dev/null || {
      echo "missing tool: $tool (see deploy/local/README.md)" >&2
      exit 2
    }
  done
}

#######################################
# Builds the image (unless SKIP_BUILD=1), creates the cluster (kind) and installs the demo chart.
# Globals:
#   CLUSTER, KIND_NAME, IMAGE, RELEASE, NS, CONTEXT, HERE, ROOT (read); SKIP_BUILD (read)
# Arguments:
#   None
#######################################
up() {
  need docker kubectl helm
  if [[ "${SKIP_BUILD:-}" != 1 ]]; then
    (cd "$ROOT" && ./mvnw -B -ntp -q -DskipTests package && docker build -t "$IMAGE" .)
  fi

  if [[ "$CLUSTER" == kind ]]; then
    need kind
    kind get clusters 2>/dev/null | grep -x "$KIND_NAME" >/dev/null || kind create cluster --name "$KIND_NAME" --wait 120s
    kind load docker-image "$IMAGE" --name "$KIND_NAME"
  fi
  # Docker Desktop (kubeadm mode) runs on the same Docker engine, so it sees kafkafim:dev.

  # The whole demo is this one install: Kafka, Elasticsearch, APM Server, Kibana, Grafana LGTM
  # and kafkafim.
  helm --kube-context "$CONTEXT" upgrade --install "$RELEASE" "$HERE/kafkafim-demo" \
    -n "$NS" --create-namespace --wait --timeout 10m
  k get pods
}

#######################################
# Sends one record to demo.events and waits up to 60 s for it in Elasticsearch.
# Globals:
#   CONTEXT, NS (read)
# Arguments:
#   None
# Returns:
#   0 when the record is indexed, 1 when it is not
#######################################
smoke() {
  need kubectl
  local id record now
  id="smoke-$(date +%s)"
  now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  record="{\"id\":\"$id\",\"region\":\"eu\",\"@timestamp\":\"$now\"}"
  echo "$record" | k exec -i deploy/kafka -c kafka -- /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server 127.0.0.1:19092 --topic demo.events
  echo "sent $record"
  for _ in $(seq 1 30); do
    local hits
    hits="$(k exec deploy/elasticsearch -- sh -c \
      "curl -fsS -u \"elastic:\$ELASTIC_PASSWORD\" 'http://127.0.0.1:9200/demo-eu-*/_count?q=_id:$id'" \
      2>/dev/null | sed -n 's/.*"count":\([0-9]*\).*/\1/p')" || hits=0 # retry on any hiccup
    if [[ "${hits:-0}" -ge 1 ]]; then
      echo "OK: $id is in Elasticsearch (demo-eu-*)"
      return 0
    fi
    sleep 2
  done
  echo "FAIL: $id did not reach Elasticsearch within 60s; see: kubectl --context $CONTEXT -n $NS logs deploy/kafkafim-demo" >&2
  return 1
}

#######################################
# Deletes the kind cluster, or on Docker Desktop uninstalls the release and deletes the namespace.
# Globals:
#   CLUSTER, KIND_NAME, RELEASE, NS, CONTEXT (read)
# Arguments:
#   None
#######################################
down() {
  if [[ "$CLUSTER" == kind ]]; then
    need kind
    kind delete cluster --name "$KIND_NAME"
  else
    need kubectl helm
    helm --kube-context "$CONTEXT" uninstall "$RELEASE" -n "$NS" --ignore-not-found
    kubectl --context "$CONTEXT" delete namespace "$NS" --ignore-not-found
  fi
}

#######################################
# Entry point: resolves the context, then runs the subcommand.
# Arguments:
#   up | smoke | down
#######################################
main() {
  set_context
  case "${1:-}" in
    up) up ;;
    smoke) smoke ;;
    down) down ;;
    *)
      echo "usage: $0 up|smoke|down" >&2
      exit 2
      ;;
  esac
}

main "$@"
