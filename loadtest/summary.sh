#!/usr/bin/env bash
# Summarises one run directory: ./summary.sh results/<run>
# Reads <run>/samples.csv (one line per second, written by run.sh) and <run>/metrics-final.txt
# (the final kafkafim.e2e.latency COUNT / TOTAL_TIME / MAX read from the actuator), and prints three lines on stdout.
set -euo pipefail

#######################################
# Prints the throughput and peak-memory line from samples.csv.
# Arguments:
#   Run directory
#######################################
print_throughput_and_peaks() {
  # Documents/s between the first non-empty sample and the last; peaks of in-flight bytes, RSS and heap.
  awk -F, 'NR>1 && $2+0>0 { if (!s) { s=$1; c0=$2 } e=$1; c1=$2; n++
            if ($5>m) m=$5; if ($9>r) r=$9; if ($10>h) h=$10 }
    END { if (e>s) printf "ES documents/s over the run (first non-empty sample to last): %.0f\n", (c1-c0)/(e-s);
          printf "peak kafkafim.in.flight.bytes: %.1f MiB; peak kafkafim RSS: %.0f MiB; peak sampled JVM heap in use: %.0f MiB\n", m/1048576, r/1024, h/1048576 }' "$1/samples.csv"
}

#######################################
# Prints the count and mean of the e2e latency timer. The actuator serves no histogram buckets,
# so there are no percentiles (use an OTLP backend for those).
# Arguments:
#   Run directory
#######################################
print_latency_mean() {
  awk '$2=="COUNT" { n=$3 } $2=="TOTAL_TIME" { sum=$3 }
       END { if (!n) { print "no e2e latency samples"; exit }
             printf "e2e latency: n=%d mean=%.3fs\n", n, sum/n }' "$1/metrics-final.txt"
}

#######################################
# Prints the e2e latency maximum, a decaying-window value (it can understate the run's maximum).
# Arguments:
#   Run directory
#######################################
print_latency_max() {
  awk '$2=="MAX" { m=$3 } END { printf "e2e latency max (kafkafim.e2e.latency MAX, decaying window): %.3fs\n", m }' "$1/metrics-final.txt"
}

#######################################
# Prints the worst one-second mean e2e latency (from the sum and count columns) and when it occurred.
# Arguments:
#   Run directory
#######################################
print_worst_second() {
  awk -F, 'NR>1 && p8>0 && $8>p8 { w=($7-p7)/($8-p8); if (w>mx) { mx=w; at=$1-first } }
           NR==2 { first=$1 } { p7=$7; p8=$8 }
           END { printf "worst one-second mean e2e latency: %.3fs (at +%ds); samples: %d\n", mx, at, NR-1 }' "$1/samples.csv"
}

#######################################
# Entry point.
# Arguments:
#   Run directory, e.g. results/<run>
#######################################
main() {
  if [[ $# -ne 1 ]]; then
    echo "usage: $0 results/<run>" >&2
    exit 2
  fi
  local -r run_dir="$1"
  print_throughput_and_peaks "$run_dir"
  print_latency_mean "$run_dir"
  print_latency_max "$run_dir"
  print_worst_second "$run_dir"
}

main "$@"
