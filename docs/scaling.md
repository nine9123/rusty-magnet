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
| RAM | **The process itself stays small: 59 MB at start-up with 15 M torrents known, 92 MB after 3 minutes of crawling.** Broad searches used to push it higher (text plus a type filter ~260 MB of heap, a 2-letter prefix search ~630 MB peak RSS). Since plan steps 2–6, the whole query suite at 3 M peaks at 146 MB RSS (397 MB before), and SQLite's heap is capped at 448 MB. `docker stats` also counts the database file's page cache, which is why today's 140 MB looks bigger than the process. Set the container limit to **1 GB**. |
| Will SQLite manage? | **Writes, yes, easily. Reads, yes, if queries are bounded.** The danger is queries whose cost grows with the number of *matches* (relevance ranking, size sorts, prefix searches). Those took 5–80 s at 15 M. Plan steps 1–6 bound all the common ones, and anything left over is stopped after 3 s. |
| Will search be reasonable? | **Yes.** Plain text search: 15–250 ms warm at 15 M (4.5–13.7 s before step 1). Text + filter, size/peers sorts and filter-only browsing took 1.4–6.5 s cold and up to 1.9 s warm at 3 M. After a one-off `reindex` they take 0–0.5 s cold (two at 1.5–2 s) and 0–0.13 s warm (see [Filters, sorts and guard rails](#filters-sorts-and-guard-rails-plan-steps-27)). I did not re-measure at 15 M. |

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

### Bounded text search (plan step 1)

`Reader::search` in `src/store.rs`: when a query has text and no other filters and is sorted
by relevance, newest or oldest:

1. Ranking, paging and the "10 000+" count run inside the FTS index. Only the 50 rows on the
   page are looked up in `torrents`, instead of one row per match.
2. Relevance ranks only the **newest 100 000 matches** (`RANK_WINDOW`). Words with fewer
   matches are ranked exactly, as before. For very common words, an old torrent that would
   have ranked highly may now be missing from the relevance view. It is still found with
   more words, a filter that narrows the matches, or `sort=oldest`. Most of the speed-up comes from this, because bm25
   used to score every match, millions of rows for a common word.

When step 1 shipped, queries with filters or sorted by size or peers still took the old
path. Steps 2–7 below cover those.

## Filters, sorts and guard rails (plan steps 2–7)

Measured on a 3 M torrent database (13.8 GB, same generator) that was built by the previous
version, so the same file gives all three columns. "Before" is the previous binary. "No tags"
is the new binary before `reindex`, which is what a large existing database runs until someone
reindexes it; there, ✗ means stopped by the 3 s limit. "After" is after `reindex`, run with
the limit off so the real cost shows. Times in ms, cold / warm, limit 50.

| Query | Before | No tags | **After** |
|---|---:|---:|---:|
| `1080p`, sort size | 6 542 / 225 | 473 / 33 | **424 / 34** |
| `1080p`, sort peers | 4 188 / 230 | 470 / 31 | **468 / 30** |
| `1080p` + `type=movie` | 1 359 / 645 | 1 061 / 645 | **208 / 67** |
| most common word + `type=movie` | 2 222 / 1 877 | 2 362 / 1 904 | **264 / 131** |
| common word + `year=1999` | 6 516 / 840 | ✗ | **202 / 34** |
| common word + `resolution=720p`, sort size | 3 947 / 226 | ✗ | **1 998 / 87** |
| `ka*` (2-letter prefix) | 2 533 / 2 299 | 0 / 0 | **0 / 0** |
| `kao*` (3-letter prefix) | 242 / 181 | 266 / 219 | **315 / 214** |
| `year=1999`, no text | 2 503 / 92 | 1 905 / 95 | **4 / 0** |
| `year=1999`, sort size | 4 013 / 267 | ✗ | **316 / 12** |
| `type=movie`, sort size | 4 548 / 221 | 16 / 0 | **8 / 0** |
| `type=tv_show&season=2&episode=3` | 4 517 / 177 | ✗ | **1 467 / 61** |
| browse, sort size | 4 203 / 184 | 6 / 0 | **4 / 0** |
| browse, sort peers | 5 325 / 217 | 0 / 0 | **1 / 0** |
| peak RSS after the whole suite | 397 MB | 391 MB | **146 MB** |

Plain text searches are unchanged (within noise). The "No tags" warm values after a ✗ are
not really warm, because the interrupted cold run left part of the data unread.

I compared the results of 15 filtered and sorted queries between the old and new binary on this
file. Totals and the set of rows are identical everywhere. The only difference is the order of
rows with equal relevance in text + filter queries: those are now newest first, like text-only
searches.

What changed, in `src/store.rs` unless noted:

- **Step 2, sort indexes.** `torrents_size` and `torrents_peers (peers, id)`.
  They are ~42 MiB each at 3 M, so ~0.2 GB each at 15 M, not the 0.5 GB estimated. Creating them
  on the existing 3 M database took 4 s at start-up (logged). Expect about a minute at 15 M on
  SSD, longer on spinning disks. Text sorted by size or peers sorts the newest
  100 000 matches (`RANK_WINDOW`), the same trade-off as relevance. Without text, the type
  filter is kept off its index (`+t.content_type`) so that SQLite walks the size index instead of
  sorting every movie.
- **Step 3, filters in the search index.** The FTS table has a third column, `tags`, with
  tokens like `ct1` (type id, `ct0` = unknown), `y1999`, `r1080p`, `s2`. Its bm25 weight is 0, and
  the user's text is restricted to `{name files}`, so neither ranking nor matching changes. Type,
  year, resolution and season become `AND tags : ...` in the FTS query, and the bounded path from
  step 1 handles them (relevance and newest/oldest; size and peers use the window above). Episode and size filters are still checked on the rows. The column adds
  30 MiB (1.4 %) to the 2.1 GB index at 3 M.
  - New databases get the column straight away. On an existing database, `run`/`serve` rebuild
    the index at start-up up to 1 M torrents (`AUTO_REINDEX_MAX`). Above that they log a warning
    and keep working the old way until you stop the container and run `rustymagnet reindex`.
  - `reindex` built the new index for 3 M torrents in **2 min 50 s** (~17 500 torrents/s, 377 MB
    peak RSS). That is **~15 min at 15 M on SSD**, much less than the "hours" I had estimated. Expect
    several times that on spinning disks. It writes into a new table and swaps it in at the end,
    so an interrupted run leaves the old index in place, and the next `reindex` starts over. It needs free disk
    space for a second copy of the index (~11 GB at 15 M). The old index's pages stay in the file
    as free pages (2.2 GB here), and new torrents reuse them.
  - `reclassify` rewrites a torrent's index entry when its tags change.
- **Step 4, prefix searches.** `ab*` needs at least 3 characters before `*` (`MIN_PREFIX`). Shorter
  ones are searched as the plain word. FTS `prefix=` indexes were not added.
- **Step 5, guard rails.** A watchdog thread interrupts reader queries after 3 s, configurable with
  `--query-timeout` / `RUSTYMAGNET_QUERY_TIMEOUT` (0 = off). The API answers 503 with "the search
  took too long: add more words or filters", and the UI shows that text. The bundled rusqlite has
  no progress-handler API without the `hooks` feature, so this uses `sqlite3_interrupt` instead.
  `PRAGMA hard_heap_limit` caps SQLite's heap at 3 × `--cache-mb` + 256 MB (448 MB by default:
  the writer's page cache, 4 readers at half of it, and headroom). The limit is process-wide, so
  a query that hits it can also make one writer batch fail. That is logged as `batch insert
  failed`, and those torrents are not fetched again until the next restart, because the Bloom
  filter already counts them as known. With the 3 s limit in place I could not get a search
  anywhere near the cap.
  **On a spinning-disk NAS, raise the timeout** (for example 10–15 s). Cold reads there are much
  slower and the 3 s limit would also stop ordinary filtered searches on a cold cache.
- **Step 6, filter-only browsing.** `year=1999` with no text counts its matches in the search index
  instead of scanning the table: 2.5 s → 4 ms cold.

## Plan

In order of value per effort. Steps 1–6 are done and step 7 is advice; details are in the
sections above.

1. **Done: bounded text search.**
2. **Done: indexes for size/peers sorting**, plus the newest-100 k window for text sorted by
   size or peers.
3. **Done: filters inside the FTS index.** Existing databases over 1 M torrents need one
   `rustymagnet reindex`, so run it **while the database is still small** if you can. Still open:
   episode and size filters are checked on the rows, so text + `min_size` on a very common word
   is not bounded except by the time limit.
4. **Done: prefix searches need 3 characters.** Not done: `prefix='3'` on the FTS table
   (about +30–50 % index size, estimate). Measure `kao*`-style searches on the real data first.
5. **Done: guard rails** (3 s query limit, heap cap).
6. **Done: filter-only browsing** counts in the FTS index (needs step 3's `reindex`).
7. **Operations.**
   - **Disk: plan for 100 GB, not 80 GB.** That covers the WAL, the sort indexes (~0.4 GB
     together at 15 M), a `reindex` (a second copy of the search index, ~11 GB at 15 M, while it
     runs) and free pages left behind afterwards.
   - **Never run `VACUUM`.** It rewrites the whole file (65 GB at 15 M), needs that much free
     space again, and blocks the crawler for the whole time. Free pages from deletes or a
     `reindex` are reused by new torrents anyway.
   - **Backups:** `sqlite3 rustymagnet.db ".backup /backup/rm.db"` works while rustymagnet runs
     and gives a consistent copy. Or stop the container and copy the file. Never copy a live
     `.db` without its `-wal`.
   - Watch `db_bytes` in `GET /api/stats` and `docker stats`. A memory limit of 1 GB is safe,
     and 512 MB should now be enough too, since the heap cap is 448 MB at the default
     `--cache-mb 64` (not measured under a 512 MB limit).
   - On a spinning-disk NAS, set `RUSTYMAGNET_QUERY_TIMEOUT` to 10–15.

## Reproducing

```sh
# ~4.5 KB per torrent, ~7 000 torrents/s; 15 M takes ~35 min and 65 GB.
RUSTYMAGNET_BENCH_DB=/tmp/bench/big.db RUSTYMAGNET_BENCH_N=3000000 \
    cargo test --release bench_fill -- --ignored --nocapture
target/release/rustymagnet serve --db /tmp/bench/big.db --http 127.0.0.1:3399 &
.claude-task/tools/bench_search.sh /tmp/bench/big.db http://127.0.0.1:3399 $!
```

To see what an existing database gains from `reindex`, build it with the previous version first,
time the searches, then run `target/release/rustymagnet reindex --db /tmp/bench/big.db` and time
them again. Start `serve` with `RUSTYMAGNET_QUERY_TIMEOUT=0` to measure queries that would
otherwise be stopped after 3 s.

Running `bench_fill` again on the same file adds N more torrents. Its last lines show the
per-table breakdown (via SQLite's `dbstat`).

Real-world bitmagnet figures (crawl rate, its FAQ's disk estimate, 12 M after ~4 months) come from
user reports, not from measurements here:
[Lemmy thread quoting the bitmagnet FAQ](https://lemmy.imagisphe.re/post/62981/210601),
[TorrentFreak article on bitmagnet](https://torrentfreak.com/bitmagnet-allows-people-to-run-their-own-decentralized-torrent-indexer-locally-240218/).
