#!/usr/bin/env bash
# Times a fixed set of searches against a running rustymagnet (`serve` or `run`).
# Each query runs twice: first with the database evicted from the OS page cache
# (posix_fadvise DONTNEED, no root needed; approximates a cold NAS disk), then warm.
# Prints the server-side took_ms for both, plus the server's RSS afterwards.
#
#   bench_search.sh <db path> <http base, e.g. http://127.0.0.1:3399> [pid]
#
# The default words come from the synthetic generator in src/store.rs
# (`bench_fill`): Quibe = most frequent word, Kaoknema = rank 10,
# Doelmael = rank 1000, Rasuten = rank 100000. Override with WORDS="a b c d".
set -euo pipefail
db=$1 base=$2 pid=${3:-}
read -r w0 w10 w1k w100k <<<"${WORDS:-Quibe Kaoknema Doelmael Rasuten}"
queries=(
  "q=$w100k"
  "q=$w1k"
  "q=$w10"
  "q=$w0"
  "q=1080p"
  "q=1080p&sort=newest"
  "q=1080p&sort=size"
  "q=1080p&type=movie"
  "q=$w0+$w10"
  "q=$w0&offset=5000"
  "q=ka*"
  "sort=newest"
  "type=tv_show"
  "year=1999"
  "sort=size"
)
evict() {
  python3 -I -c 'import os,sys
for p in sys.argv[1:]:
    if os.path.exists(p):
        fd=os.open(p,os.O_RDONLY); os.posix_fadvise(fd,0,0,os.POSIX_FADV_DONTNEED); os.close(fd)' "$db" "$db-wal"
}
took() { curl -sf "$base/api/search?$1&limit=50" | python3 -I -c 'import json,sys; d=json.load(sys.stdin); print(d["took_ms"], d["total"])'; }
printf '%-28s %10s %10s %8s\n' query cold_ms warm_ms total
for q in "${queries[@]}"; do
  evict
  read -r cold total < <(took "$q")
  read -r warm _ < <(took "$q")
  printf '%-28s %10s %10s %8s\n' "$q" "$cold" "$warm" "$total"
done
if [ -n "$pid" ]; then grep -E 'VmRSS|VmHWM|RssAnon|RssFile' "/proc/$pid/status"; fi
