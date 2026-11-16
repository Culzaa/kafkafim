#!/usr/bin/env bash
# kafkafim load test driver (loadtest/README.md). Usage: ./run.sh <up|down|throughput|latency|memory|outage|rebalance>
# Every knob is an environment variable with a default below. Output: loadtest/results/<run>/.
set -euo pipefail
cd "$(dirname "$0")"

: "${PIPELINES:=1}"        # pipelines, each on its own topic lt.p<n> and its own ES index
: "${PARTITIONS:=3}"       # partitions per topic
: "${DOCS:=1000000}"       # documents per pipeline
: "${DOC_BYTES:=1024}"     # approximate size of one JSON document
: "${RATE:=5000}"          # docs/s per pipeline (latency, outage, rebalance)
: "${MAX_IN_FLIGHT:=64MB}" # source.max-in-flight-bytes
: "${HEAP:=512m}"          # kafkafim -Xmx
: "${BULK_IN_FLIGHT:=4}"   # elasticsearch bulk.in-flight
: "${OUTAGE_AT:=30}"       # outage: seconds into the run when ES stops
: "${OUTAGE_FOR:=60}"      # outage: seconds ES stays down
default_jar() {            # first built kafkafim jar that is not the pre-repackage "-original" one; empty if none
  local jar
  for jar in ../target/kafkafim-*.jar; do
    [[ "$jar" == *original* ]] || {
      echo "$jar"
      return 0
    }
  done
}
: "${JAR:=$(default_jar)}"
BOOT=localhost:19092
LIB="$PWD/results/.lib"
ES=http://localhost:19200
RUN="results/$(date +%Y%m%d-%H%M%S)-${1:-run}"
DC="docker compose -f compose.yaml"

log() { printf '%s %s\n' "$(date +%T)" "$*" | tee -a "$RUN/run.log"; }
prod() { # topic firstSeq count rate; runs on the host with the Kafka client jars taken from the kafkafim jar
  if [[ ! -d "$LIB" ]]; then
    mkdir -p "$LIB"
    unzip -qj -o "$JAR" 'BOOT-INF/lib/kafka-clients-*' 'BOOT-INF/lib/lz4-java-*' 'BOOT-INF/lib/slf4j-api-*' -d "$LIB"
  fi
  java -cp "$LIB/*" Produce.java "$BOOT" "$1" "$2" "$3" "$4" "$DOC_BYTES" 2>&1 | grep -v '^SLF4J\|^WARNING'
}
es_count() { curl -s -m 5 "$ES/lt-*/_count" | sed -n 's/.*"count":\([0-9]*\).*/\1/p'; }

up() {
  $DC up -d --wait
  for n in $(seq 1 "$PIPELINES"); do
    $DC exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --delete --topic "lt.p$n" --topic "lt.p$n.dlq" >/dev/null 2>&1 || true
  done
  sleep 2
  for n in $(seq 1 "$PIPELINES"); do
    $DC exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create \
      --topic "lt.p$n" --partitions "$PARTITIONS" --replication-factor 1 >/dev/null
    $DC exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create \
      --topic "lt.p$n.dlq" --partitions 1 --replication-factor 1 >/dev/null # kafkafim never creates topics (ADR-0021)
  done
  for n in $(seq 1 "$PIPELINES"); do curl -s -XDELETE "$ES/lt-p$n" >/dev/null || true; done # wildcard deletes are refused by default
  # one shard, no replicas, 1s refresh: the same for every run
  curl -s -XPUT "$ES/_index_template/lt" -H 'content-type: application/json' \
    -d '{"index_patterns":["lt-*"],"template":{"settings":{"number_of_shards":1,"number_of_replicas":0}}}' >/dev/null
}

gen_config() { # writes $RUN/config/application-kafkafim.yaml
  mkdir -p "$RUN/config"
  {
    cat <<YAML
kafkafim:
  kafka:
    clusters:
      lt: { bootstrap-servers: [$BOOT], security-protocol: PLAINTEXT }
  elasticsearch:
    clusters:
      lt:
        uris: [$ES]
        bulk: { max-docs: 5000, max-bytes: 5MB, max-age: 200ms, in-flight: $BULK_IN_FLIGHT }
  pipelines:
YAML
    for n in $(seq 1 "$PIPELINES"); do
      cat <<YAML
    p$n:
      source:
        cluster: lt
        topics: [lt.p$n]
        group-id: lt.p$n
        offset-reset: earliest
        max-in-flight-bytes: $MAX_IN_FLIGHT
      document: { id-field: /id }
      routes:
        - default: true
          cluster: lt
          index: lt-p$n
      dlq: { topic: lt.p$n.dlq }
YAML
    done
  } >"$RUN/config/application-kafkafim.yaml"
}

start_app() { # name port [partitions that must be assigned before it counts as ready]
  if curl -s -m 2 -o /dev/null "localhost:$2/actuator/health"; then
    log "port $2 is in use: a kafkafim from an earlier run is still alive"
    return 1
  fi
  JAVA_TOOL_OPTIONS="-Xmx$HEAP" SPRING_PROFILES_ACTIVE=kafkafim \
    SPRING_CONFIG_ADDITIONAL_LOCATION="$PWD/$RUN/config/" SERVER_PORT="$2" \
    MANAGEMENT_OTLP_METRICS_EXPORT_ENABLED=false MANAGEMENT_OTLP_TRACING_EXPORT_ENABLED=false \
    MANAGEMENT_LOGGING_EXPORT_OTLP_ENABLED=false MANAGEMENT_TRACING_SAMPLING_PROBABILITY=0 \
    java -jar "$JAR" >"$RUN/app-$1.log" 2>&1 &
  echo $! >"$RUN/app-$1.pid"
  for _ in $(seq 1 90); do
    curl -sf -m 3 "localhost:$2/actuator/health/readiness" >/dev/null 2>&1 &&
      [[ "$(metric "$2" '^kafkafim_assigned_partitions')" -ge "${3:-1}" ]] && return 0
    sleep 1
  done
  log "kafkafim $1 did not become ready"
  return 1
}
stop_app() { # name: SIGTERM (graceful), SIGKILL after 20 s; only the pid this run started
  if [[ -f "$RUN/app-$1.pid" ]]; then
    local pid
    pid=$(cat "$RUN/app-$1.pid")
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 20); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 1
    done
    kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$RUN/app-$1.pid"
}
metric() { # port name-regex  -> sum of all matching series (0 when absent)
  curl -sf -m 3 "localhost:$1/actuator/prometheus" 2>/dev/null |
    awk -v re="$2" '$0 ~ re { s += $NF } END { printf "%.0f\n", s }'
}

sampler() { # one CSV line per second: time, ES count, indexed A, indexed B, in-flight A, assigned A, e2e sum A, e2e count A, RSS KB A, JVM heap used A
  echo "t,es_count,indexed_a,indexed_b,in_flight_a,assigned_a,e2e_sum_a,e2e_count_a,rss_kb_a,heap_used_a" >"$RUN/samples.csv"
  while :; do
    pid=$(cat "$RUN/app-a.pid" 2>/dev/null || echo 0)
    a=$(curl -sf -m 2 localhost:8080/actuator/prometheus || true)
    b=$(curl -sf -m 2 localhost:8081/actuator/prometheus || true)
    sum() { awk -v re="$1" '$0 ~ re { s += $NF } END { printf "%.3f", s }'; }
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' "$(date +%s)" "$(es_count || echo 0)" \
      "$(sum '^kafkafim_docs_total.*outcome="indexed"' <<<"$a")" "$(sum '^kafkafim_docs_total.*outcome="indexed"' <<<"$b")" \
      "$(sum '^kafkafim_in_flight_bytes' <<<"$a")" "$(sum '^kafkafim_assigned_partitions' <<<"$a")" \
      "$(sum '^kafkafim_e2e_latency_seconds_sum' <<<"$a")" "$(sum '^kafkafim_e2e_latency_seconds_count' <<<"$a")" \
      "$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ')" "$(sum '^jvm_memory_used_bytes.*area="heap"' <<<"$a")" >>"$RUN/samples.csv"
    sleep 1
  done
}

prefill() { # all pipelines in parallel, returns when every topic is filled
  produce_all 0
  wait_produce
}
steady() { # produce DOCS docs per pipeline at RATE, in parallel, in the background
  produce_all "$RATE"
}
produce_all() { # rate
  PRODS=()
  for n in $(seq 1 "$PIPELINES"); do
    prod "lt.p$n" 0 "$DOCS" "$1" >>"$RUN/produce.log" &
    PRODS+=("$!")
  done
}
wait_produce() { wait "${PRODS[@]}"; } # not a bare `wait`: that would also wait for the sampler
wait_total() {                         # until ES holds PIPELINES*DOCS documents; gives up after $1 seconds, or 180 s without progress
  local want=$((PIPELINES * DOCS)) c last=-1 idle=0
  for _ in $(seq 1 "$1"); do
    c=$(es_count || echo 0)
    [[ "${c:-0}" -ge "$want" ]] && return 0
    if [[ "${c:-0}" == "$last" ]]; then idle=$((idle + 1)); else idle=0; fi
    [[ "$idle" -ge 180 ]] && {
      log "STALLED at ${c:-0} documents"
      return 1
    }
    last=${c:-0}
    sleep 1
  done
  return 1
}
finish() { # final metrics snapshot and summary
  curl -sf -m 5 localhost:8080/actuator/prometheus | grep '^kafkafim_' >"$RUN/metrics-final.txt" || true
  curl -s "$ES/_refresh" >/dev/null || true
  grep -c OutOfMemoryError "$RUN"/app-*.log | awk -F: '$2>0 { print "OutOfMemoryError in " $1 }' | tee -a "$RUN/run.log" || true
  log "ES documents: $(es_count) of $((PIPELINES * DOCS))"
  ./summary.sh "$RUN" | tee -a "$RUN/run.log"
}

main() {
  local scenario="${1:-}"
  mkdir -p "$RUN"
  case "$scenario" in up | down) ;; *) [[ -f "$JAR" ]] || {
    echo "no jar: run ./mvnw -DskipTests package first" >&2
    exit 1
  } ;; esac
  cleanup() {
    kill "${SAMPLER:-0}" 2>/dev/null || true
    stop_app a
    stop_app b
    $DC start elasticsearch >/dev/null 2>&1 || true
  }
  trap cleanup EXIT
  log "scenario=$scenario pipelines=$PIPELINES partitions=$PARTITIONS docs=$DOCS doc_bytes=$DOC_BYTES rate=$RATE max_in_flight=$MAX_IN_FLIGHT heap=$HEAP bulk_in_flight=$BULK_IN_FLIGHT jar=$JAR"

  case "$scenario" in
    up) $DC up -d --wait ;;
    down) $DC down -v ;;
    throughput | memory) # backlog first, then kafkafim drains it as fast as it can
      up
      gen_config
      log "prefilling $((PIPELINES * DOCS)) documents"
      prefill
      sampler &
      SAMPLER=$!
      start_app a 8080 $((PIPELINES * PARTITIONS)) || log "kafkafim never reported its partitions assigned"
      log "kafkafim started, draining"
      wait_total 1800 || log "TIMEOUT waiting for the backlog"
      finish
      ;;
    latency) # steady rate: the e2e histogram is the result
      up
      gen_config
      start_app a 8080 $((PIPELINES * PARTITIONS))
      sampler &
      SAMPLER=$!
      log "producing at $RATE docs/s per pipeline"
      steady
      wait_produce
      wait_total 120 || log "TIMEOUT"
      finish
      ;;
    outage) # steady rate; Elasticsearch stopped OUTAGE_FOR seconds, OUTAGE_AT seconds in
      up
      gen_config
      start_app a 8080 $((PIPELINES * PARTITIONS))
      sampler &
      SAMPLER=$!
      steady
      sleep "$OUTAGE_AT"
      log "STOP elasticsearch"
      $DC stop elasticsearch >/dev/null
      sleep "$OUTAGE_FOR"
      log "START elasticsearch"
      $DC start elasticsearch >/dev/null
      until curl -sf "$ES/_cluster/health" >/dev/null; do sleep 1; done
      log "elasticsearch answers"
      wait_produce
      wait_total 900 || log "TIMEOUT"
      finish
      ;;
    rebalance) # second instance joins after OUTAGE_AT seconds and leaves OUTAGE_FOR seconds later
      up
      gen_config
      start_app a 8080 $((PIPELINES * PARTITIONS))
      sampler &
      SAMPLER=$!
      steady
      sleep "$OUTAGE_AT"
      log "START instance b"
      start_app b 8081
      sleep "$OUTAGE_FOR"
      log "STOP instance b"
      stop_app b
      wait_produce
      wait_total 300 || log "TIMEOUT"
      finish
      ;;
    *)
      echo "usage: $0 <up|down|throughput|latency|memory|outage|rebalance>" >&2
      exit 2
      ;;
  esac
}

main "$@"
