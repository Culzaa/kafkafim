#!/usr/bin/env bash
# kafkafim load test driver (loadtest/README.md). Usage: ./run.sh <up|down|throughput|latency|memory|outage|rebalance>
# Every knob is an environment variable with a default below. Output: loadtest/results/<run>/.
set -euo pipefail

readonly PIPELINES="${PIPELINES:-1}"            # pipelines, each on its own topic lt.p<n> and its own ES index
readonly PARTITIONS="${PARTITIONS:-3}"          # partitions per topic
readonly DOCS="${DOCS:-1000000}"                # documents per pipeline
readonly DOC_BYTES="${DOC_BYTES:-1024}"         # approximate size of one JSON document
readonly RATE="${RATE:-5000}"                   # docs/s per pipeline (latency, outage, rebalance)
readonly MAX_IN_FLIGHT="${MAX_IN_FLIGHT:-64MB}" # source.max-in-flight-bytes
readonly HEAP="${HEAP:-512m}"                   # kafkafim -Xmx
readonly BULK_IN_FLIGHT="${BULK_IN_FLIGHT:-4}"  # elasticsearch bulk.in-flight
readonly OUTAGE_AT="${OUTAGE_AT:-30}"           # outage: seconds into the run when ES stops
readonly OUTAGE_FOR="${OUTAGE_FOR:-60}"         # outage: seconds ES stays down
readonly BOOT=localhost:19092
readonly ES=http://localhost:19200
readonly DC="docker compose -f compose.yaml"

# Set by init, which runs first in main (they depend on the script's directory and the scenario).
JAR="${JAR:-}" # kafkafim jar; the environment may override
LIB=""
RUN=""
SAMPLER="" # pid of the background sampler
PRODS=()   # pids of the background producers

#######################################
# Prints the first built kafkafim jar that is not the pre-repackage "-original" one; nothing if none.
# Arguments:
#   None
# Outputs:
#   Writes the jar path to stdout
#######################################
default_jar() {
  local jar
  for jar in ../target/kafkafim-*.jar; do
    [[ "$jar" == *original* ]] || {
      echo "$jar"
      return 0
    }
  done
}

#######################################
# Changes into the script's directory and sets the per-run constants JAR, LIB and RUN.
# Globals:
#   JAR (read from the environment, set), LIB, RUN (set)
# Arguments:
#   Scenario name, used in the run directory name (default "run")
#######################################
init() {
  cd "$(dirname "$0")"
  JAR="${JAR:-$(default_jar)}"
  LIB="$PWD/results/.lib"
  RUN="results/$(date +%Y%m%d-%H%M%S)-${1:-run}"
  readonly JAR LIB RUN
}

#######################################
# Prints a timestamped line to stdout and appends it to the run log.
# Globals:
#   RUN (read)
# Arguments:
#   Message words
#######################################
log() { printf '%s %s\n' "$(date +%T)" "$*" | tee -a "$RUN/run.log"; }

#######################################
# Like log, but to stderr (and the run log): for failures.
# Globals:
#   RUN (read)
# Arguments:
#   Message words
#######################################
err() { printf '%s %s\n' "$(date +%T)" "$*" | tee -a "$RUN/run.log" >&2; }

#######################################
# Runs Produce.java on the host, with the Kafka client jars taken from the kafkafim jar.
# Globals:
#   LIB, JAR, BOOT, DOC_BYTES (read)
# Arguments:
#   Topic, first sequence number, count, rate in docs/s (0 = unthrottled)
#######################################
prod() {
  if [[ ! -d "$LIB" ]]; then
    mkdir -p "$LIB"
    unzip -qj -o "$JAR" 'BOOT-INF/lib/kafka-clients-*' 'BOOT-INF/lib/lz4-java-*' 'BOOT-INF/lib/slf4j-api-*' -d "$LIB"
  fi
  java -cp "$LIB/*" Produce.java "$BOOT" "$1" "$2" "$3" "$4" "$DOC_BYTES" 2>&1 | grep -v '^SLF4J\|^WARNING'
}

#######################################
# Prints the number of documents in the lt-* indices.
# Globals:
#   ES (read)
#######################################
es_count() { curl -s -m 5 "$ES/lt-*/_count" | sed -n 's/.*"count":\([0-9]*\).*/\1/p'; }

#######################################
# Starts the compose stack, recreates the topics and index template: the same state for every run.
# Globals:
#   DC, PIPELINES, PARTITIONS, ES (read)
#######################################
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

#######################################
# Writes $RUN/config/application-kafkafim.yaml.
# Globals:
#   RUN, BOOT, ES, BULK_IN_FLIGHT, PIPELINES, MAX_IN_FLIGHT (read)
#######################################
gen_config() {
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

#######################################
# Starts a kafkafim instance in the background and waits up to 90 s until it is ready.
# Globals:
#   RUN, JAR, HEAP (read)
# Arguments:
#   Name (a or b), port, optionally the partitions that must be assigned before it counts as ready
# Returns:
#   0 when ready, 1 when the port is in use or readiness timed out
#######################################
start_app() {
  if curl -s -m 2 -o /dev/null "localhost:$2/actuator/health"; then
    err "port $2 is in use: a kafkafim from an earlier run is still alive"
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
      [[ "$(metric "$2" kafkafim.assigned.partitions VALUE | cut -d. -f1)" -ge "${3:-1}" ]] && return 0
    sleep 1
  done
  err "kafkafim $1 did not become ready"
  return 1
}

#######################################
# Stops an instance this run started: SIGTERM (graceful), SIGKILL after 20 s.
# Globals:
#   RUN (read)
# Arguments:
#   Name (a or b)
#######################################
stop_app() {
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

#######################################
# Prints one statistic of a meter, summed over all its tag combinations, read from the actuator
# (kafkafim exports metrics over OTLP only; /actuator/metrics/<name> serves the same meters as
# JSON). Prints 0 when the meter does not exist yet or the instance is down.
# Arguments:
#   Port, meter name (dotted, e.g. kafkafim.docs), statistic (COUNT, VALUE, TOTAL_TIME, MAX),
#   optionally a tag filter as key:value
#######################################
metric() {
  local url="localhost:$1/actuator/metrics/$2"
  [[ -z "${4:-}" ]] || url+="?tag=$4"
  curl -sf -m 3 "$url" 2>/dev/null |
    tr '{' '\n' | sed -n 's/.*"statistic":"'"$3"'","value":\([-0-9.eE+]*\).*/\1/p' |
    awk '{ s += $1 } END { printf "%.3f\n", s }'
}

#######################################
# Appends one CSV line per second to $RUN/samples.csv, forever (run it in the background):
# time, ES count, indexed A, indexed B, in-flight A, assigned A, e2e sum A, e2e count A,
# RSS KB A, JVM heap used A.
# Globals:
#   RUN (read)
#######################################
sampler() {
  local pid
  echo "t,es_count,indexed_a,indexed_b,in_flight_a,assigned_a,e2e_sum_a,e2e_count_a,rss_kb_a,heap_used_a" >"$RUN/samples.csv"
  while :; do
    pid=$(cat "$RUN/app-a.pid" 2>/dev/null || echo 0)
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' "$(date +%s)" "$(es_count || echo 0)" \
      "$(metric 8080 kafkafim.docs COUNT outcome:indexed)" "$(metric 8081 kafkafim.docs COUNT outcome:indexed)" \
      "$(metric 8080 kafkafim.in.flight.bytes VALUE)" "$(metric 8080 kafkafim.assigned.partitions VALUE)" \
      "$(metric 8080 kafkafim.e2e.latency TOTAL_TIME)" "$(metric 8080 kafkafim.e2e.latency COUNT)" \
      "$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ')" "$(metric 8080 jvm.memory.used VALUE area:heap)" >>"$RUN/samples.csv"
    sleep 1
  done
}

#######################################
# Fills every topic with DOCS documents, all pipelines in parallel; returns when they are filled.
# Globals:
#   PRODS (set via produce_all)
#######################################
prefill() {
  produce_all 0
  wait_produce
}

#######################################
# Produces DOCS documents per pipeline at RATE, in parallel, in the background.
# Globals:
#   RATE (read), PRODS (set via produce_all)
#######################################
steady() {
  produce_all "$RATE"
}

#######################################
# Starts one background producer per pipeline.
# Globals:
#   PIPELINES, DOCS, RUN (read), PRODS (set)
# Arguments:
#   Rate in docs/s (0 = unthrottled)
#######################################
produce_all() {
  PRODS=()
  for n in $(seq 1 "$PIPELINES"); do
    prod "lt.p$n" 0 "$DOCS" "$1" >>"$RUN/produce.log" &
    PRODS+=("$!")
  done
}

#######################################
# Waits for the producers (not a bare `wait`: that would also wait for the sampler).
# Globals:
#   PRODS (read)
#######################################
wait_produce() { wait "${PRODS[@]}"; }

#######################################
# Waits until ES holds PIPELINES*DOCS documents; gives up after a timeout, or 180 s without progress.
# Globals:
#   PIPELINES, DOCS (read)
# Arguments:
#   Timeout in seconds
# Returns:
#   0 when all documents arrived, 1 on timeout or stall
#######################################
wait_total() {
  local want=$((PIPELINES * DOCS)) c last=-1 idle=0
  for _ in $(seq 1 "$1"); do
    c=$(es_count || echo 0)
    [[ "${c:-0}" -ge "$want" ]] && return 0
    if [[ "${c:-0}" == "$last" ]]; then idle=$((idle + 1)); else idle=0; fi
    [[ "$idle" -ge 180 ]] && {
      err "STALLED at ${c:-0} documents"
      return 1
    }
    last=${c:-0}
    sleep 1
  done
  return 1
}

#######################################
# Writes the final e2e latency snapshot and prints the summary.
# Globals:
#   RUN, ES, PIPELINES, DOCS (read)
#######################################
finish() {
  {
    echo "kafkafim.e2e.latency COUNT $(metric 8080 kafkafim.e2e.latency COUNT)"
    echo "kafkafim.e2e.latency TOTAL_TIME $(metric 8080 kafkafim.e2e.latency TOTAL_TIME)"
    echo "kafkafim.e2e.latency MAX $(metric 8080 kafkafim.e2e.latency MAX)"
  } >"$RUN/metrics-final.txt"
  curl -s "$ES/_refresh" >/dev/null || true
  grep -c OutOfMemoryError "$RUN"/app-*.log | awk -F: '$2>0 { print "OutOfMemoryError in " $1 }' | tee -a "$RUN/run.log" || true
  log "ES documents: $(es_count) of $((PIPELINES * DOCS))"
  ./summary.sh "$RUN" | tee -a "$RUN/run.log"
}

#######################################
# EXIT trap: stops the sampler and both instances, and makes sure Elasticsearch is running again.
# Globals:
#   SAMPLER, DC (read)
#######################################
cleanup() {
  [[ -z "$SAMPLER" ]] || kill "$SAMPLER" 2>/dev/null || true
  stop_app a
  stop_app b
  $DC start elasticsearch >/dev/null 2>&1 || true
}

#######################################
# Entry point: runs one scenario.
# Arguments:
#   Scenario: up | down | throughput | latency | memory | outage | rebalance
#######################################
main() {
  local scenario="${1:-}"
  init "$scenario"
  mkdir -p "$RUN"
  if [[ "$scenario" != up && "$scenario" != down && ! -f "$JAR" ]]; then
    echo "no jar: run ./mvnw -DskipTests package first" >&2
    exit 1
  fi
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
