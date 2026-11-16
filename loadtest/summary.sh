#!/usr/bin/env bash
# Summarises one run directory: ./summary.sh results/<run>
set -euo pipefail
readonly run_dir="$1"
awk -F, 'NR>1 && $2+0>0 { if (!s) { s=$1; c0=$2 } e=$1; c1=$2; n++
          if ($5>m) m=$5; if ($9>r) r=$9; if ($10>h) h=$10 }
  END { if (e>s) printf "ES documents/s over the run (first non-empty sample to last): %.0f\n", (c1-c0)/(e-s);
        printf "peak kafkafim_in_flight_bytes: %.1f MiB; peak kafkafim RSS: %.0f MiB; peak sampled JVM heap in use: %.0f MiB\n", m/1048576, r/1024, h/1048576 }' "$run_dir/samples.csv"
awk '/^kafkafim_e2e_latency_seconds_bucket/ {
       match($0, /le="[^"]*"/); le=substr($0, RSTART+4, RLENGTH-5); v[le]+=$NF; if (le!="+Inf") k[le]=le+0 }
     /^kafkafim_e2e_latency_seconds_count/ { tot+=$NF }
     /^kafkafim_e2e_latency_seconds_sum/ { sum+=$NF }
     END { if (!tot) { print "no e2e histogram buckets"; exit }
       printf "e2e latency: n=%d mean=%.3fs", tot, sum/tot
       split("0.5 0.95 0.99", q, " ")
       for (i=1;i<=3;i++) { best=""; for (l in k) if (v[l] >= q[i]*tot && (best=="" || k[l]<k[best])) best=l
         printf "  p%d<=%ss", q[i]*100, (best==""?"+Inf":best) }
       print "" }' "$run_dir/metrics-final.txt"
awk '/^kafkafim_e2e_latency_seconds_max/ { if ($NF>m) m=$NF } END { printf "e2e latency max (kafkafim_e2e_latency_seconds_max, decaying window): %.3fs\n", m }' "$run_dir/metrics-final.txt"
awk -F, 'NR>1 && p8>0 && $8>p8 { w=($7-p7)/($8-p8); if (w>mx) { mx=w; at=$1-first } }
         NR==2 { first=$1 } { p7=$7; p8=$8 }
         END { printf "worst one-second mean e2e latency: %.3fs (at +%ds); samples: %d\n", mx, at, NR-1 }' "$run_dir/samples.csv"
