# Scaling to a 50–80 GB index

Goal: grow the database from today's ~10 000 torrents (42.8 MB, 140 MB container RAM)
to 50–80 GB while keeping the NAS responsive and search usable.

Everything below was measured on a synthetic database built to look like crawled data,
not estimated from the schema. The generator is `bench_fill` in `src/store.rs`, the query
timings come from `.claude-task/tools/bench_search.sh`, and the sections below say how to
reproduce them. The machine was a 16-core container with 7.7 GB RAM and SSD-backed storage.
**Cold** means the database file was evicted from the OS page cache before the query.
**Warm** means the same query was run again straight away.

## Short answers

| Question | Answer |
|---|---|
| Will the size hold? | **Yes.** At 15 M torrents the file is 65 GB and the writer still inserts ~6 600 torrents/s, about 4 000× what the crawler delivers. SQLite's limits are far away. |
| How long to get there? | At ~130 000 torrents/day (README, `--rate 150`): **~90 days to 50 GB, ~145 days to 80 GB**. bitmagnet users report ~5 000 torrents/hour and 12 M torrents after about 4 months, which agrees. Expect the rate to fall once the popular part of the DHT is known. For comparison, bitmagnet's FAQ plans for ~80 GB per 10 M torrents; here that is ~45 GB. |
| RAM | **The process itself stays small: 59 MB at start-up with 15 M torrents known, 92 MB after 3 minutes of crawling.** Broad searches push it higher: text plus a type filter took ~260 MB of heap, and a 2-letter prefix search peaked at ~630 MB RSS (plan steps 3–5 fix both). `docker stats` also counts the database file's page cache, which is why today's 140 MB looks bigger than the process. Set the container limit to **1 GB**. |
| Will SQLite manage? | **Writes, yes, easily. Reads, yes, if queries are bounded.** The danger is queries whose cost grows with the number of *matches* (relevance ranking, size sorts, prefix searches). Those took 5–80 s at 15 M before. This change fixes the most common one; the plan covers the rest. |
| Will search be reasonable? | **Plain text search: yes**, 15–250 ms warm at 15 M after this change (4.5–13.7 s before). **Text + filter, size sorts, `ab*` prefixes: not yet**, 1–12 s warm and up to 80 s cold. See plan steps 2–5. |

## Where the bytes go

The current database: 42.8 MB / 9 867 torrents ≈ **4.3 KB per torrent**. The synthetic
data was tuned to match that (4.57 KB). At 15 M torrents:

| Table | Size | Per torrent | Read when |
|---|---:|---:|---|
| `torrent_files` (file-list blobs) | 51.5 GB (79 %) | 3 601 B | opening one torrent |
| `torrents_fts_data` (search index) | 10.7 GB (16 %) | 746 B | every text search |
| `torrents` (rows) | 2.2 GB | 155 B | every result, browse, filters |
| info-hash index | 0.47 GB | 33 B | start-up, torrent page |
| type index + FTS docsize | 0.45 GB | 31 B | type browse, ranking |
| **Total** | **65.3 GB** | **4 567 B** | |

So **50 GB ≈ 11–12 M torrents and 80 GB ≈ 18–19 M torrents** at today's mix. Two
consequences:

- **The search working set is ~13.4 GB, not 65 GB.** That is the FTS index, the torrents
  table and the hash index. File lists are only read when a torrent is opened. How much of
  those 13.4 GB the NAS can keep in its page cache decides whether searches see the warm or
  the cold numbers below.
- `--no-file-index` only removes file *names* from the 10.7 GB search index. The 51.5 GB of
  file lists stay. Expect it to save maybe 10–15 %; I did not measure it. To halve the
  database you would have to stop storing file lists (for example, only for torrents with
  more than N files), which is a product decision.

## RAM

Measured with the 15 M database:

| State | RSS |
|---|---|
| `run`, right after start-up (Bloom filter for 15 M hashes is 56 MiB, loaded in 19 s) | 59 MB anon |
| `run`, crawling for 3 min (50 000 nodes, 64 fetching, 259 torrents inserted) | 87 MB anon, 92 MB RSS |
| `serve`, typical text search | ~35 MB anon |
| text + type filter (`1080p` + `type=movie`) | ~260 MB anon, 414 MB peak RSS |
| 2-letter prefix (`ka*`) | 630 MB peak RSS |
| old code, after the whole query suite | 641 MB anon, 1.23 GB peak |
| new code, after the whole query suite | 66 MB anon, 812 MB peak |

Fixed costs that grow with the index:

- **Bloom filter**: about 2 bytes × 2 × known torrents, at least 20 MB: **~40 MB at 10 M,
  ~75 MB at 18 M**. It is sized at start-up for twice the known count. If you crawl past that
  without restarting, the false-positive rate climbs from 0.05 % towards ~1 %, which means
  ~1 % of new torrents are skipped. A restart resizes it.
- **SQLite page cache**: up to `--cache-mb` (64) for the writer + 32 MB × up to 4 readers =
  **192 MB maximum**, filled lazily.
- **mmap**: 256 MB per connection, file-backed and shared. The kernel can drop it, but
  `docker stats` may count it.

Peaks come from FTS5 holding every match in memory when it can't stream the result
(rank-sorted queries joined to filters, prefix expansion). With the default 50 000 DHT
nodes and 64 fetchers the crawler's own share stays in the tens of MB.

## Search at 15 M torrents (65 GB)

`took_ms` reported by the server, limit 50. Word frequencies come from the generator:
"rank 0" is the most frequent word, which appears in a large share of names and file names.

| Query | Old cold | Old warm | **New cold** | **New warm** |
|---|---:|---:|---:|---:|
| rare word (~9 800 hits) | 2 867 | 28 | 1 390 | **14** |
| mid word (rank 1 000) | 7 505 | 116 | 5 100 | **77** |
| common word (rank 10) | 12 302 | 4 469 | 208 | **83** |
| most common word (rank 0) | 21 073 | 13 685 | 211 | **163** |
| `1080p` | 12 035 | 4 662 | 139 | **84** |
| two common words | 13 129 | 5 541 | 365 | **243** |
| most common word, `offset=5000` | 21 358 | 12 654 | 210 | **169** |
| `1080p`, sort newest | 3 | 1 | 0 | 0 |
| `1080p`, sort size | 80 283 | 1 117 | 79 685 | 1 089 |
| `1080p` + `type=movie` | 12 432 | 4 623 | 12 184 | 4 587 |
| `ka*` (prefix) | 29 807 | 20 334 | 12 080 | 10 600 |
| browse newest / type facet | 0–8 | 0 | 0–5 | 0 |
| `year=1999`, no text | 8 931 | 92 | 8 817 | 98 |
| browse, sort size | 78 998 | 860 | 79 190 | 873 |

The cold numbers are on SSD. **On a spinning-disk NAS, cold reads are 10–50× slower per
random read**, so anything that is "cold seconds" here becomes tens of seconds. Put the
database on SSD if the NAS has one.

At 3 M torrents (13 GB) the old code already took 1.8 s warm for the most common word, and
the new code 57 ms. The old cost grew linearly with the index; the new cost is roughly flat.

### What changed in this PR

`Reader::search` in `src/store.rs`: when a query has text and no other filters and is sorted
by relevance, newest or oldest:

1. Ranking, paging and the "10 000+" count run inside the FTS index. Only the 50 rows on the
   page are looked up in `torrents`, instead of one row per match.
2. Relevance ranks only the **newest 100 000 matches** (`RANK_WINDOW`). Words with fewer
   matches are ranked exactly, as before. For very common words, an old torrent that would
   have ranked highly may now be missing from the relevance view. It is still found with
   more words, a filter, or `sort=oldest`. Most of the speed-up comes from this, because bm25
   used to score every match, millions of rows for a common word.

Queries with filters (type, year, resolution, season, size) or sorted by size or peers take
the old path unchanged, so Torznab and filtered results are exactly as before.

## Plan

In order of value per effort. Step 1 is in this PR; the rest is not started.

1. **Done: bounded text search** (above).
2. **Indexes for size/peers sorting.** `CREATE INDEX torrents_size ON torrents(size)` and
   `torrents_peers ON torrents(peers, id)`. Browse by size goes from 0.9 s warm / 79 s cold to
   milliseconds. Cost: ~0.5 GB each at 15 M, and creating them on an existing large database
   blocks start-up for a few minutes once. Text + size sort (`1080p&sort=size`) needs the
   same window treatment as relevance (sort the newest 100 k matches), because an index
   doesn't help there.
3. **Filters inside the FTS index.** Text + type is the most common UI action (the chips) and
   costs 4.6 s warm at 15 M, with ~260 MB of heap. Add a third FTS column with tokens like
   `t_movie`, `r_1080p`, `y_1999`, `s_02`. Filtering then becomes an FTS `AND` and can use
   the bounded path from step 1. Needs a one-off `reindex` command that rebuilds the FTS
   index: hours at 15 M, so do it **while the database is still small**. The cheaper
   alternative is to apply the rank window to filtered queries too. It's a one-line change,
   but a common word plus a rare filter could then show no results while the count says
   there are some, so I did not do it.
4. **Prefix searches.** Require at least 3 characters before `*`. That's trivial, and `ab*`
   accounts for most of the 10–30 s and the 630 MB peak. Optionally add `prefix='3'` to the
   FTS table later, for about +30–50 % index size (estimate).
5. **Guard rails for SQLite.** Install a progress handler on reader connections that aborts
   a query after ~3 s ("query too broad, add words"). Set `PRAGMA hard_heap_limit` (for
   example 256 MB) so that a pathological query fails instead of growing the container. Even
   if steps 2–4 are skipped, this one keeps the NAS safe.
6. **Filter-only browsing** (`year=1999` with no text): it is the "10 000+" count that scans
   the table, 9 s cold. Either lower the count cap for filter-only queries or index `year`
   and `resolution`. Cheap; decide after step 3.
7. **Operations.**
   - Keep ≥ 1.5× the database size free if you ever want to `VACUUM`. Otherwise never run
     it. The file is 65 GB and VACUUM rewrites all of it.
   - Back up with `sqlite3 rustymagnet.db ".backup /backup/rm.db"` (works online), or stop
     the container and copy the file. Don't copy a live `.db` without its `-wal`.
   - Plan for 100 GB of disk, not 80 GB: the WAL, a backup copy and the index additions above.
   - Watch `db_bytes` in `GET /api/stats` and `docker stats`. A memory limit of 1 GB is safe.
     512 MB is enough for crawling, but broad searches can hit it until step 5 is done.

## Reproducing

```sh
# ~4.5 KB per torrent, ~7 000 torrents/s; 15 M takes ~35 min and 65 GB.
RUSTYMAGNET_BENCH_DB=/tmp/bench/big.db RUSTYMAGNET_BENCH_N=3000000 \
    cargo test --release bench_fill -- --ignored --nocapture
target/release/rustymagnet serve --db /tmp/bench/big.db --http 127.0.0.1:3399 &
.claude-task/tools/bench_search.sh /tmp/bench/big.db http://127.0.0.1:3399 $!
```

Running `bench_fill` again on the same file adds N more torrents. Its last lines show the
per-table breakdown (via SQLite's `dbstat`).

Real-world bitmagnet figures (crawl rate, its FAQ's disk estimate, 12 M after ~4 months) come from
user reports, not from measurements here:
[Lemmy thread quoting the bitmagnet FAQ](https://lemmy.imagisphe.re/post/62981/210601),
[TorrentFreak article on bitmagnet](https://torrentfreak.com/bitmagnet-allows-people-to-run-their-own-decentralized-torrent-indexer-locally-240218/).
