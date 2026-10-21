//! SQLite storage: one row per torrent, a compact file-list blob in a side
//! table, and a contentless FTS5 index for search.
//!
//! All writes go through a single writer thread that commits in batches; reads
//! use a small pool of read-only connections (WAL mode lets them run concurrently).

use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rusqlite::types::Value as SqlValue;
use rusqlite::{params, params_from_iter, Connection, OpenFlags, OptionalExtension};

use crate::bloom::Bloom;
use crate::classify::{self, Classification, ContentType};
use crate::torrent::{self, File, InfoHash, TorrentInfo};

pub type Result<T> = rusqlite::Result<T>;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS torrents (
    id            INTEGER PRIMARY KEY,
    info_hash     BLOB NOT NULL UNIQUE,
    name          TEXT NOT NULL,
    size          INTEGER NOT NULL,
    files_count   INTEGER NOT NULL,
    private       INTEGER NOT NULL DEFAULT 0,
    content_type  INTEGER,
    title         TEXT,
    year          INTEGER,
    season        INTEGER,
    episodes      TEXT,
    resolution    TEXT,
    source        TEXT,
    video_codec   TEXT,
    modifiers     TEXT,
    languages     TEXT,
    release_group TEXT,
    peers         INTEGER NOT NULL DEFAULT 0,
    discovered_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS torrents_type_id ON torrents(content_type, id);
CREATE INDEX IF NOT EXISTS torrents_size ON torrents(size);
CREATE INDEX IF NOT EXISTS torrents_peers ON torrents(peers, id);
CREATE TABLE IF NOT EXISTS torrent_files (
    torrent_id INTEGER PRIMARY KEY,
    data       BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS type_counts (
    content_type INTEGER PRIMARY KEY,  -- 0 = unknown
    n            INTEGER NOT NULL
);
"#;

#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub path: PathBuf,
    /// SQLite page cache per connection, in MiB.
    pub cache_mb: u32,
    /// Index file names (not just torrent names) for full-text search.
    pub index_files: bool,
    /// Reader queries running longer are interrupted; `None` = no limit.
    pub query_timeout: Option<Duration>,
}

fn configure(conn: &Connection, cfg: &StoreConfig) -> Result<()> {
    conn.busy_timeout(Duration::from_secs(10))?;
    conn.execute_batch(&format!(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         PRAGMA temp_store=MEMORY;
         PRAGMA cache_size=-{};
         PRAGMA mmap_size=268435456;",
        u64::from(cfg.cache_mb.max(2)) * 1024
    ))
}

pub fn open_rw(cfg: &StoreConfig) -> Result<Connection> {
    let conn = Connection::open(&cfg.path)?;
    configure(&conn, cfg)?;
    if table_exists(&conn, "torrents")?
        && !(table_exists(&conn, "torrents_size")? && table_exists(&conn, "torrents_peers")?)
    {
        crate::log_info!("db: building the size and peers sort indexes (a few minutes on a large database)");
    }
    conn.execute_batch(SCHEMA)?;
    if !table_exists(&conn, "torrents_fts")? {
        create_fts(&conn, "torrents_fts")?;
    }
    Ok(conn)
}

fn table_exists(conn: &Connection, name: &str) -> Result<bool> {
    conn.query_row("SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE name = ?1)", [name], |r| r.get(0))
}

/// The search index. `tags` holds filter tokens (see `fts_tags`) so that type,
/// year, resolution and season filters run inside the index; it has no weight
/// in relevance ranking.
fn create_fts(conn: &Connection, table: &str) -> Result<()> {
    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE {table} USING fts5(
            name, files, tags,
            content='', contentless_delete=1,
            tokenize='unicode61 remove_diacritics 2'
         );
         INSERT INTO {table} ({table}, rank) VALUES ('rank', 'bm25(1.0, 1.0, 0.0)');"
    ))
}

/// Databases created before the `tags` column have a two-column search index
/// until `reindex` rebuilds it. Everything still works on them, filters just
/// run outside the index.
fn fts_has_tags(conn: &Connection) -> Result<bool> {
    Ok(conn.prepare("SELECT * FROM torrents_fts LIMIT 0")?.column_count() >= 3)
}

/// Filter tokens for the `tags` column: `ct<type id>` (`ct0` = unknown),
/// `y<year>`, `r<resolution>`, `s<season>`.
fn fts_tags(content_type: Option<i64>, year: Option<i64>, resolution: Option<&str>, season: Option<i64>) -> String {
    let mut s = format!("ct{}", content_type.unwrap_or(0));
    if let Some(y) = year {
        s.push_str(&format!(" y{y}"));
    }
    if let Some(r) = resolution {
        s.push_str(&format!(" r{r}"));
    }
    if let Some(n) = season {
        s.push_str(&format!(" s{n}"));
    }
    s
}

/// Databases up to this many torrents get their search index rebuilt at
/// start-up (well under a minute); larger ones need an explicit `reindex`.
const AUTO_REINDEX_MAX: u64 = 1_000_000;

/// Brings an older database up to date before it is served.
pub fn upgrade(cfg: &StoreConfig) -> Result<()> {
    let conn = open_rw(cfg)?;
    if fts_has_tags(&conn)? {
        return Ok(());
    }
    let n = conn.query_row("SELECT coalesce(sum(n), 0) FROM type_counts", [], |r| r.get::<_, i64>(0))? as u64;
    drop(conn);
    if n <= AUTO_REINDEX_MAX {
        crate::log_info!("db: rebuilding the search index with filter tags ({n} torrents)");
        reindex(cfg)?;
    } else {
        crate::log_warn!(
            "db: the search index has no filter tags yet, so filtered searches stay slow on {n} torrents. \
             Stop rustymagnet and run `rustymagnet reindex` once (see docs/scaling.md)"
        );
    }
    Ok(())
}

/// Rebuilds the search index from the stored names and file lists, into a new
/// table that replaces the old one at the end. Run it with nothing else
/// writing to the database. Needs free disk space for a second copy of the index.
pub fn reindex(cfg: &StoreConfig) -> Result<u64> {
    let mut conn = open_rw(cfg)?;
    conn.execute_batch("DROP TABLE IF EXISTS torrents_fts_new;")?;
    create_fts(&conn, "torrents_fts_new")?;
    let mut last_id = 0i64;
    let mut total = 0u64;
    loop {
        let tx = conn.transaction()?;
        type ReindexRow = (i64, String, Option<Vec<u8>>, Option<i64>, Option<i64>, Option<String>, Option<i64>);
        let rows: Vec<ReindexRow> = {
            let mut stmt = tx.prepare(
                "SELECT t.id, t.name, f.data, t.content_type, t.year, t.resolution, t.season
                 FROM torrents t LEFT JOIN torrent_files f ON f.torrent_id = t.id
                 WHERE t.id > ?1 ORDER BY t.id LIMIT 10000",
            )?;
            let rows = stmt.query_map([last_id], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?))
            })?;
            rows.collect::<Result<_>>()?
        };
        let Some(last) = rows.last() else {
            tx.commit()?;
            break;
        };
        last_id = last.0;
        {
            let mut ins =
                tx.prepare_cached("INSERT INTO torrents_fts_new (rowid, name, files, tags) VALUES (?1, ?2, ?3, ?4)")?;
            for (id, name, data, ct, year, res, season) in &rows {
                // Only multi-file torrents have a file list, as in `insert`.
                let files = match data {
                    Some(d) if cfg.index_files => files_fts_text(&torrent::decode_files(d)),
                    _ => String::new(),
                };
                ins.execute(params![id, name, files, fts_tags(*ct, *year, res.as_deref(), *season)])?;
            }
        }
        tx.commit()?;
        let before = total;
        total += rows.len() as u64;
        if before / 100_000 != total / 100_000 {
            crate::log_info!("reindex: {} torrents processed", total);
        }
    }
    conn.execute_batch(
        "BEGIN;
         DROP TABLE torrents_fts;
         ALTER TABLE torrents_fts_new RENAME TO torrents_fts;
         COMMIT;",
    )?;
    Ok(total)
}

pub struct NewTorrent {
    pub info_hash: InfoHash,
    pub info: TorrentInfo,
    pub class: Classification,
    pub peers: u32,
    pub discovered_at: i64,
}

impl NewTorrent {
    pub fn new(info_hash: InfoHash, info: TorrentInfo, peers: u32) -> Self {
        let class = classify::classify(&info.name, &info.files);
        NewTorrent { info_hash, info, class, peers, discovered_at: now() }
    }
}

pub fn now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

fn files_fts_text(files: &[File]) -> String {
    // Index at most a few hundred file names; huge packs add little search value.
    let mut s = String::new();
    for f in files.iter().take(300) {
        s.push_str(&f.path);
        s.push('\n');
    }
    s
}

fn join(v: &[&str]) -> Option<String> {
    (!v.is_empty()).then(|| v.join(","))
}

/// Inserts one torrent; returns false if it already existed. `tags` says
/// whether the search index has the `tags` column.
fn insert(tx: &rusqlite::Transaction<'_>, t: &NewTorrent, index_files: bool, tags: bool) -> Result<bool> {
    let c = &t.class;
    if c.banned {
        return Ok(false);
    }
    let mut stmt = tx.prepare_cached(
        "INSERT OR IGNORE INTO torrents (info_hash, name, size, files_count, private, content_type, title, year,
            season, episodes, resolution, source, video_codec, modifiers, languages, release_group, peers, discovered_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
    )?;
    let n = stmt.execute(params![
        &t.info_hash[..],
        t.info.name,
        t.info.size as i64,
        t.info.files_count as i64,
        t.info.private,
        c.content_type.map(ContentType::id),
        c.title,
        c.year,
        c.season,
        c.episodes,
        c.resolution,
        c.source,
        c.video_codec,
        join(&c.modifiers),
        join(&c.languages),
        c.release_group,
        t.peers,
        t.discovered_at,
    ])?;
    if n == 0 {
        return Ok(false);
    }
    let id = tx.last_insert_rowid();
    // Single-file torrents are fully described by the row itself.
    let multi = t.info.files.len() > 1 || t.info.files.first().is_some_and(|f| f.path != t.info.name);
    if multi {
        tx.prepare_cached("INSERT OR REPLACE INTO torrent_files (torrent_id, data) VALUES (?1, ?2)")?
            .execute(params![id, torrent::encode_files(&t.info.files)])?;
    }
    let files_text = if index_files && multi { files_fts_text(&t.info.files) } else { String::new() };
    if tags {
        let tags =
            fts_tags(c.content_type.map(ContentType::id), c.year.map(i64::from), c.resolution, c.season.map(i64::from));
        tx.prepare_cached("INSERT INTO torrents_fts (rowid, name, files, tags) VALUES (?1, ?2, ?3, ?4)")?
            .execute(params![id, t.info.name, files_text, tags])?;
    } else {
        tx.prepare_cached("INSERT INTO torrents_fts (rowid, name, files) VALUES (?1, ?2, ?3)")?.execute(params![
            id,
            t.info.name,
            files_text
        ])?;
    }
    tx.prepare_cached(
        "INSERT INTO type_counts (content_type, n) VALUES (?1, 1) ON CONFLICT(content_type) DO UPDATE SET n = n + 1",
    )?
    .execute(params![c.content_type.map_or(0, ContentType::id)])?;
    Ok(true)
}

pub enum WriteMsg {
    Insert(Box<NewTorrent>),
}

#[derive(Default)]
pub struct WriterStats {
    pub inserted: std::sync::atomic::AtomicU64,
}

/// Starts the writer thread. Inserts are committed in batches of up to
/// `batch` rows or every `max_delay`, whichever comes first.
pub fn spawn_writer(
    cfg: StoreConfig,
    stats: Arc<WriterStats>,
) -> Result<(SyncSender<WriteMsg>, std::thread::JoinHandle<()>)> {
    let mut conn = open_rw(&cfg)?;
    let tags = fts_has_tags(&conn)?;
    let (tx, rx) = std::sync::mpsc::sync_channel::<WriteMsg>(4096);
    let handle = std::thread::Builder::new()
        .name("db-writer".into())
        .spawn(move || writer_loop(&mut conn, rx, &cfg, tags, &stats))
        .expect("spawn writer thread");
    Ok((tx, handle))
}

fn writer_loop(conn: &mut Connection, rx: Receiver<WriteMsg>, cfg: &StoreConfig, tags: bool, stats: &WriterStats) {
    const BATCH: usize = 1000;
    const MAX_DELAY: Duration = Duration::from_millis(1000);
    let mut pending: Vec<Box<NewTorrent>> = Vec::with_capacity(BATCH);
    let mut closed = false;
    while !closed {
        let deadline = Instant::now() + MAX_DELAY;
        // Block for the first message, then gather until full or deadline.
        match rx.recv() {
            Ok(WriteMsg::Insert(t)) => pending.push(t),
            Err(_) => closed = true,
        }
        while !closed && pending.len() < BATCH {
            match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(WriteMsg::Insert(t)) => pending.push(t),
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => closed = true,
            }
        }
        if !pending.is_empty() {
            match write_batch(conn, &pending, cfg.index_files, tags) {
                Ok(n) => {
                    stats.inserted.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
                    crate::log_debug!("db: committed {} new torrents", n);
                }
                Err(e) => crate::log_error!("db: batch insert failed: {e}"),
            }
            pending.clear();
        }
    }
    let _ = conn.execute_batch("PRAGMA optimize;");
}

fn write_batch(conn: &mut Connection, batch: &[Box<NewTorrent>], index_files: bool, tags: bool) -> Result<u64> {
    let tx = conn.transaction()?;
    let mut n = 0;
    for t in batch {
        if insert(&tx, t, index_files, tags)? {
            n += 1;
        }
    }
    tx.commit()?;
    Ok(n)
}

/// Fills the Bloom filter with every stored info hash.
pub fn load_known(cfg: &StoreConfig, bloom: &Bloom) -> Result<u64> {
    let conn = open_rw(cfg)?;
    let mut stmt = conn.prepare("SELECT info_hash FROM torrents")?;
    let mut rows = stmt.query([])?;
    let mut n = 0;
    while let Some(row) = rows.next()? {
        let h: Vec<u8> = row.get(0)?;
        if let Ok(h) = <[u8; 20]>::try_from(h.as_slice()) {
            bloom.insert(&h);
            n += 1;
        }
    }
    Ok(n)
}

pub fn count_torrents(cfg: &StoreConfig) -> Result<u64> {
    let conn = open_rw(cfg)?;
    conn.query_row("SELECT coalesce(sum(n), 0) FROM type_counts", [], |r| r.get::<_, i64>(0)).map(|n| n as u64)
}

/// Re-runs the classifier over the whole database (after classifier updates).
/// Torrents that now match the blocklist are deleted.
pub fn reclassify(cfg: &StoreConfig) -> Result<u64> {
    let mut conn = open_rw(cfg)?;
    let tags = fts_has_tags(&conn)?;
    let mut last_id = 0i64;
    let mut total = 0u64;
    loop {
        let tx = conn.transaction()?;
        type ReclassifyRow = (i64, String, Option<Vec<u8>>, i64, String);
        let rows: Vec<ReclassifyRow> = {
            let mut stmt = tx.prepare(
                "SELECT t.id, t.name, f.data, t.size, t.content_type, t.year, t.resolution, t.season
                 FROM torrents t LEFT JOIN torrent_files f ON f.torrent_id = t.id
                 WHERE t.id > ?1 ORDER BY t.id LIMIT 5000",
            )?;
            let rows = stmt.query_map([last_id], |r| {
                let old_tags = fts_tags(r.get(4)?, r.get(5)?, r.get::<_, Option<String>>(6)?.as_deref(), r.get(7)?);
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, old_tags))
            })?;
            rows.collect::<Result<_>>()?
        };
        if rows.is_empty() {
            tx.commit()?;
            break;
        }
        {
            let mut upd = tx.prepare_cached(
                "UPDATE torrents SET content_type=?2, title=?3, year=?4, season=?5, episodes=?6, resolution=?7,
                 source=?8, video_codec=?9, modifiers=?10, languages=?11, release_group=?12 WHERE id=?1",
            )?;
            for (id, name, data, size, old_tags) in &rows {
                let files = match data {
                    Some(d) => torrent::decode_files(d),
                    None => vec![File { path: name.clone(), size: *size as u64 }],
                };
                let c = classify::classify(name, &files);
                if c.banned {
                    tx.execute("DELETE FROM torrents_fts WHERE rowid = ?1", [id])?;
                    tx.execute("DELETE FROM torrent_files WHERE torrent_id = ?1", [id])?;
                    tx.execute("DELETE FROM torrents WHERE id = ?1", [id])?;
                    continue;
                }
                let new_tags = fts_tags(
                    c.content_type.map(ContentType::id),
                    c.year.map(i64::from),
                    c.resolution,
                    c.season.map(i64::from),
                );
                if tags && new_tags != *old_tags {
                    // A contentless index row can only be replaced as a whole.
                    let files_text =
                        if cfg.index_files && data.is_some() { files_fts_text(&files) } else { String::new() };
                    tx.prepare_cached("UPDATE torrents_fts SET name = ?2, files = ?3, tags = ?4 WHERE rowid = ?1")?
                        .execute(params![id, name, files_text, new_tags])?;
                }
                upd.execute(params![
                    id,
                    c.content_type.map(ContentType::id),
                    c.title,
                    c.year,
                    c.season,
                    c.episodes,
                    c.resolution,
                    c.source,
                    c.video_codec,
                    join(&c.modifiers),
                    join(&c.languages),
                    c.release_group
                ])?;
            }
        }
        tx.commit()?;
        last_id = rows.last().map_or(last_id, |r| r.0);
        total += rows.len() as u64;
        crate::log_info!("reclassify: {} torrents processed", total);
    }
    conn.execute_batch(
        "DELETE FROM type_counts;
         INSERT INTO type_counts (content_type, n) SELECT coalesce(content_type, 0), count(*) FROM torrents GROUP BY 1;",
    )?;
    Ok(total)
}

// ---------------------------------------------------------------- reads

#[derive(Debug, Clone)]
pub struct Row {
    pub info_hash: InfoHash,
    pub name: String,
    pub size: u64,
    pub files_count: u64,
    pub private: bool,
    pub content_type: Option<ContentType>,
    pub title: Option<String>,
    pub year: Option<u16>,
    pub season: Option<u16>,
    pub episodes: Option<String>,
    pub resolution: Option<String>,
    pub source: Option<String>,
    pub video_codec: Option<String>,
    pub modifiers: Option<String>,
    pub languages: Option<String>,
    pub release_group: Option<String>,
    pub peers: u32,
    pub discovered_at: i64,
}

const ROW_COLS: &str = "t.info_hash, t.name, t.size, t.files_count, t.private, t.content_type, t.title, t.year, t.season,
    t.episodes, t.resolution, t.source, t.video_codec, t.modifiers, t.languages, t.release_group, t.peers, t.discovered_at";

fn map_row(r: &rusqlite::Row<'_>) -> Result<Row> {
    let h: Vec<u8> = r.get(0)?;
    Ok(Row {
        info_hash: h.as_slice().try_into().unwrap_or([0; 20]),
        name: r.get(1)?,
        size: r.get::<_, i64>(2)? as u64,
        files_count: r.get::<_, i64>(3)? as u64,
        private: r.get(4)?,
        content_type: r.get::<_, Option<i64>>(5)?.and_then(ContentType::from_id),
        title: r.get(6)?,
        year: r.get(7)?,
        season: r.get(8)?,
        episodes: r.get(9)?,
        resolution: r.get(10)?,
        source: r.get(11)?,
        video_codec: r.get(12)?,
        modifiers: r.get(13)?,
        languages: r.get(14)?,
        release_group: r.get(15)?,
        peers: r.get(16)?,
        discovered_at: r.get(17)?,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Sort {
    #[default]
    Relevance,
    Newest,
    Oldest,
    Largest,
    Smallest,
    Peers,
}

impl Sort {
    pub fn parse(s: &str) -> Option<Sort> {
        Some(match s {
            "relevance" => Sort::Relevance,
            "newest" => Sort::Newest,
            "oldest" => Sort::Oldest,
            "size" | "largest" => Sort::Largest,
            "smallest" => Sort::Smallest,
            "peers" => Sort::Peers,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct Query {
    pub text: String,
    /// Content types to include; `None` inside means "unknown". Empty = all.
    pub types: Vec<Option<ContentType>>,
    pub year: Option<u16>,
    pub resolution: Option<String>,
    pub season: Option<u16>,
    pub episode: Option<u16>,
    pub min_size: Option<u64>,
    pub max_size: Option<u64>,
    pub sort: Sort,
    pub limit: u32,
    pub offset: u32,
}

/// Maximum number of matches counted for the "total" figure.
pub const COUNT_CAP: u64 = 10_000;

/// Text-only searches sorted by relevance rank only the newest this many
/// matches, so their cost stops growing with the index. Words with fewer
/// matches are ranked exactly.
pub const RANK_WINDOW: u64 = 100_000;

pub struct SearchResult {
    pub rows: Vec<Row>,
    /// Number of matches, capped at `COUNT_CAP`.
    pub total: u64,
    pub total_capped: bool,
}

/// Characters needed before `*` for a prefix search.
pub const MIN_PREFIX: usize = 3;

/// Turns user input into a safe FTS5 expression: every word is quoted and
/// AND-ed, `-word` excludes, `word*` is a prefix match.
pub fn fts_query(input: &str) -> Option<String> {
    let mut pos = Vec::new();
    let mut neg = Vec::new();
    for raw in input.split_whitespace() {
        let (negate, raw) = match raw.strip_prefix('-') {
            Some(r) if !r.is_empty() => (true, r),
            _ => (false, raw),
        };
        let parts: Vec<&str> = raw.split(|c: char| !c.is_alphanumeric()).filter(|w| !w.is_empty()).collect();
        let Some(last) = parts.last() else {
            continue;
        };
        // A short prefix like `ab*` expands to a large part of the vocabulary;
        // it is searched as a plain word instead.
        let prefix = raw.ends_with('*') && last.chars().count() >= MIN_PREFIX;
        let words: Vec<String> = parts.iter().map(|w| format!("\"{}\"", w)).collect();
        // "s01e02" or "x.y.z" stay together as a phrase.
        let mut term = if words.len() > 1 { words.join(" + ") } else { words[0].clone() };
        if prefix {
            term.push('*');
        }
        if negate {
            neg.push(term)
        } else {
            pos.push(term)
        }
    }
    if pos.is_empty() {
        return None;
    }
    let mut q = pos.join(" AND ");
    for n in neg {
        q = format!("({q}) NOT {n}");
    }
    Some(q)
}

/// SQLite's heap is capped at the page caches' budget plus this much, so that
/// a pathological query fails instead of growing the container.
const HEAP_HEADROOM_MB: u64 = 256;

/// Whether a query was stopped after `StoreConfig::query_timeout`.
pub fn is_timeout(e: &rusqlite::Error) -> bool {
    e.sqlite_error_code() == Some(rusqlite::ErrorCode::OperationInterrupted)
}

/// One search filter: an SQL condition on `torrents t` and, where the search
/// index can answer it, the same condition as an FTS expression on `tags`.
struct Filter {
    sql: String,
    args: Vec<SqlValue>,
    tag: Option<String>,
}

fn filters(q: &Query) -> Vec<Filter> {
    let mut out = Vec::new();
    let mut add = |sql: &str, args: Vec<SqlValue>, tag: Option<String>| out.push(Filter { sql: sql.into(), args, tag });
    if !q.types.is_empty() {
        let mut parts = Vec::new();
        let ids: Vec<String> = q.types.iter().flatten().map(|t| t.id().to_string()).collect();
        if !ids.is_empty() {
            parts.push(format!("t.content_type IN ({})", ids.join(",")));
        }
        if q.types.contains(&None) {
            parts.push("t.content_type IS NULL".to_string());
        }
        let tags: Vec<String> = q.types.iter().map(|t| format!("\"ct{}\"", t.map_or(0, ContentType::id))).collect();
        add(&format!("({})", parts.join(" OR ")), vec![], Some(format!("tags : ({})", tags.join(" OR "))));
    }
    if let Some(y) = q.year {
        add("t.year = ?", vec![SqlValue::Integer(y.into())], Some(format!("tags : \"y{y}\"")));
    }
    if let Some(r) = &q.resolution {
        // Stored resolutions are tokens like `1080p`; anything else can only be compared in SQL.
        let tag = (!r.is_empty() && r.chars().all(|c| c.is_ascii_alphanumeric())).then(|| format!("tags : \"r{r}\""));
        add("t.resolution = ?", vec![SqlValue::Text(r.clone())], tag);
    }
    if let Some(s) = q.season {
        add("t.season = ?", vec![SqlValue::Integer(s.into())], Some(format!("tags : \"s{s}\"")));
        if let Some(e) = q.episode {
            // Exact episode, a range, or a full-season pack.
            add(
                "(t.episodes = ? OR t.episodes NOT LIKE '%E%' OR t.episodes LIKE '%-E%')",
                vec![SqlValue::Text(format!("S{s:02}E{e:02}"))],
                None,
            );
        }
    }
    if let Some(m) = q.min_size {
        add("t.size >= ?", vec![SqlValue::Integer(m as i64)], None);
    }
    if let Some(m) = q.max_size {
        add("t.size <= ?", vec![SqlValue::Integer(m as i64)], None);
    }
    out
}

/// Joins the filters' SQL with AND, numbering placeholders from `?{first}`.
/// `no_type_index` keeps SQLite from filtering by the type index, so that it
/// walks a sort index instead of sorting every torrent of that type.
fn render(filters: &[&Filter], first: usize, no_type_index: bool) -> (String, Vec<SqlValue>) {
    let mut n = first;
    let mut conds = Vec::new();
    let mut args = Vec::new();
    for f in filters {
        let mut sql = String::new();
        for c in f.sql.chars() {
            if c == '?' {
                sql.push_str(&format!("?{n}"));
                n += 1;
            } else {
                sql.push(c);
            }
        }
        if no_type_index {
            sql = sql.replace("t.content_type", "+t.content_type");
        }
        conds.push(sql);
        args.extend(f.args.iter().cloned());
    }
    (conds.join(" AND "), args)
}

/// Lets the watchdog thread interrupt a connection whose query has run past
/// its deadline.
struct Watch {
    deadline: Mutex<Option<Instant>>,
    interrupt: rusqlite::InterruptHandle,
}

struct Pooled {
    conn: Connection,
    watch: Arc<Watch>,
}

type Watches = Mutex<Vec<std::sync::Weak<Watch>>>;

/// Interrupts reader queries past their deadline; exits once the `Reader` is gone.
fn watchdog(watches: std::sync::Weak<Watches>) {
    while let Some(list) = watches.upgrade() {
        let now = Instant::now();
        list.lock().unwrap().retain(|w| {
            let Some(w) = w.upgrade() else {
                return false;
            };
            // The lock keeps the query from finishing and the connection from
            // starting its next query between the check and the interrupt.
            let deadline = w.deadline.lock().unwrap();
            if deadline.is_some_and(|d| now >= d) {
                w.interrupt.interrupt();
            }
            true
        });
        drop(list);
        std::thread::sleep(Duration::from_millis(100));
    }
}

pub struct Reader {
    cfg: StoreConfig,
    /// The search index has the `tags` column.
    tags: bool,
    timeout: Option<Duration>,
    pool: Mutex<Vec<Pooled>>,
    watches: Arc<Watches>,
}

impl Reader {
    pub fn new(cfg: StoreConfig) -> Result<Self> {
        // Ensure the schema exists before read-only connections are opened.
        let conn = open_rw(&cfg)?;
        let tags = fts_has_tags(&conn)?;
        // Process-wide: the writer's page cache, up to 4 readers at half of it, and headroom.
        let limit_mb = u64::from(cfg.cache_mb.max(2)) * 3 + HEAP_HEADROOM_MB;
        conn.execute_batch(&format!("PRAGMA hard_heap_limit={};", limit_mb << 20))?;
        drop(conn);
        let watches = Arc::new(Watches::default());
        let w = Arc::downgrade(&watches);
        std::thread::Builder::new()
            .name("db-watchdog".into())
            .spawn(move || watchdog(w))
            .expect("spawn watchdog thread");
        Ok(Reader { timeout: cfg.query_timeout, cfg, tags, pool: Mutex::new(Vec::new()), watches })
    }

    fn with_conn<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let p = self.pool.lock().unwrap().pop();
        let p = match p {
            Some(p) => p,
            None => {
                let c = Connection::open_with_flags(
                    &self.cfg.path,
                    OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX | OpenFlags::SQLITE_OPEN_URI,
                )?;
                c.busy_timeout(Duration::from_secs(10))?;
                c.execute_batch(&format!(
                    "PRAGMA cache_size=-{}; PRAGMA mmap_size=268435456; PRAGMA temp_store=MEMORY;",
                    u64::from(self.cfg.cache_mb.max(2)) * 1024 / 2
                ))?;
                let watch = Arc::new(Watch { deadline: Mutex::new(None), interrupt: c.get_interrupt_handle() });
                self.watches.lock().unwrap().push(Arc::downgrade(&watch));
                Pooled { conn: c, watch }
            }
        };
        *p.watch.deadline.lock().unwrap() = self.timeout.map(|t| Instant::now() + t);
        let out = f(&p.conn);
        *p.watch.deadline.lock().unwrap() = None;
        let mut pool = self.pool.lock().unwrap();
        if pool.len() < 4 {
            pool.push(p);
        }
        out
    }

    pub fn search(&self, q: &Query) -> Result<SearchResult> {
        let text = fts_query(&q.text);
        // Without text there is nothing to rank.
        let sort = if text.is_none() && q.sort == Sort::Relevance { Sort::Newest } else { q.sort };
        let by_column = matches!(sort, Sort::Largest | Sort::Smallest | Sort::Peers);
        let filters = filters(q);
        let unfiltered = text.is_none() && filters.is_empty();
        let (tagged, residual): (Vec<&Filter>, Vec<&Filter>) =
            filters.iter().partition(|f| self.tags && f.tag.is_some());
        // Text never matches the filter tokens.
        let mut terms: Vec<String> = text.iter().map(|t| format!("{{name files}} : ({t})")).collect();
        terms.extend(tagged.iter().filter_map(|f| f.tag.clone()));
        let matched = (!terms.is_empty()).then(|| terms.join(" AND "));
        let page = format!("LIMIT {} OFFSET {}", q.limit.clamp(1, 500), q.offset);
        let fts_order = match sort {
            // Newest first among equal ranks, so pages don't overlap or skip.
            Sort::Relevance => Some(("rank, rowid DESC", "f.rank, t.id DESC")),
            Sort::Newest => Some(("rowid DESC", "t.id DESC")),
            Sort::Oldest => Some(("rowid ASC", "t.id ASC")),
            _ => None,
        };
        let fts_count = format!(
            "SELECT count(*) FROM (SELECT 1 FROM torrents_fts WHERE torrents_fts MATCH ?1 LIMIT {})",
            COUNT_CAP + 1
        );
        let count_of =
            |from_where: &str| format!("SELECT count(*) FROM (SELECT 1 FROM {from_where} LIMIT {})", COUNT_CAP + 1);
        let (sql, args, count_sql, count_args) = match &matched {
            // Every condition is in the search index: rank, page and count there
            // and only look up the `torrents` rows that are returned, instead of
            // one (often uncached) row per match.
            Some(m) if residual.is_empty() && fts_order.is_some() => {
                let (inner, outer) = fts_order.unwrap_or_default();
                let source = if sort == Sort::Relevance {
                    // bm25 costs a little per match; very common words would
                    // otherwise score millions of rows. Rank the newest ones.
                    format!(
                        "(SELECT rowid, rank FROM torrents_fts WHERE torrents_fts MATCH ?1
                         ORDER BY rowid DESC LIMIT {RANK_WINDOW})"
                    )
                } else {
                    "torrents_fts WHERE torrents_fts MATCH ?1".to_string()
                };
                let sql = format!(
                    "SELECT {ROW_COLS} FROM (SELECT rowid, rank FROM {source} ORDER BY {inner} {page}) f
                     JOIN torrents t ON t.id = f.rowid ORDER BY {outer}"
                );
                let args = vec![SqlValue::Text(m.clone())];
                (sql, args.clone(), fts_count, args)
            }
            // Text sorted by size or peers: an index on the column doesn't help
            // with a text match, so sort the newest matches, as for relevance.
            Some(m) if residual.is_empty() && text.is_some() => {
                let col = match sort {
                    Sort::Largest => "t.size DESC",
                    Sort::Smallest => "t.size ASC",
                    _ => "t.peers DESC",
                };
                let sql = format!(
                    "SELECT {ROW_COLS} FROM (SELECT rowid FROM torrents_fts WHERE torrents_fts MATCH ?1
                     ORDER BY rowid DESC LIMIT {RANK_WINDOW}) f
                     JOIN torrents t ON t.id = f.rowid ORDER BY {col}, t.id DESC {page}"
                );
                let args = vec![SqlValue::Text(m.clone())];
                (sql, args.clone(), fts_count, args)
            }
            // Some conditions can only be checked on the rows.
            Some(m) if text.is_some() || !by_column => {
                let (conds, mut args) = render(&residual, 2, false);
                args.insert(0, SqlValue::Text(m.clone()));
                let from_where =
                    format!("torrents_fts f JOIN torrents t ON t.id = f.rowid WHERE torrents_fts MATCH ?1 AND {conds}");
                let order = match sort {
                    Sort::Relevance => "f.rank",
                    Sort::Newest => "f.rowid DESC",
                    Sort::Oldest => "t.id ASC",
                    Sort::Largest => "t.size DESC",
                    Sort::Smallest => "t.size ASC",
                    Sort::Peers => "t.peers DESC, t.id DESC",
                };
                let sql = format!("SELECT {ROW_COLS} FROM {from_where} ORDER BY {order} {page}");
                (sql, args.clone(), count_of(&from_where), args)
            }
            // No text: walk the table in sort order and check the filters.
            _ => {
                let all: Vec<&Filter> = filters.iter().collect();
                let (conds, args) = render(&all, 1, by_column);
                let wh = if conds.is_empty() { String::new() } else { format!("WHERE {conds}") };
                let order = match sort {
                    Sort::Relevance | Sort::Newest => "t.id DESC",
                    Sort::Oldest => "t.id ASC",
                    Sort::Largest => "t.size DESC",
                    Sort::Smallest => "t.size ASC",
                    Sort::Peers => "t.peers DESC, t.id DESC",
                };
                let sql = format!("SELECT {ROW_COLS} FROM torrents t {wh} ORDER BY {order} {page}");
                match &matched {
                    // Filter-only counts are cheap in the index, a table scan otherwise.
                    Some(m) if residual.is_empty() => (sql, args, fts_count, vec![SqlValue::Text(m.clone())]),
                    _ => {
                        let (conds, count_args) = render(&all, 1, false);
                        let wh = if conds.is_empty() { String::new() } else { format!("WHERE {conds}") };
                        (sql, args, count_of(&format!("torrents t {wh}")), count_args)
                    }
                }
            }
        };
        self.with_conn(|c| {
            let rows = c
                .prepare_cached(&sql)?
                .query_map(params_from_iter(args.iter()), map_row)?
                .collect::<Result<Vec<_>>>()?;
            let total = if unfiltered {
                c.query_row("SELECT coalesce(sum(n), 0) FROM type_counts", [], |r| r.get::<_, i64>(0))? as u64
            } else {
                c.prepare_cached(&count_sql)?.query_row(params_from_iter(count_args.iter()), |r| r.get::<_, i64>(0))?
                    as u64
            };
            let capped = !unfiltered && total > COUNT_CAP;
            Ok(SearchResult { rows, total: if capped { COUNT_CAP } else { total }, total_capped: capped })
        })
    }

    pub fn get(&self, info_hash: &InfoHash) -> Result<Option<(Row, Vec<File>)>> {
        self.with_conn(|c| {
            let sql = format!(
                "SELECT {ROW_COLS}, f.data FROM torrents t LEFT JOIN torrent_files f ON f.torrent_id = t.id WHERE t.info_hash = ?1"
            );
            c.query_row(&sql, [&info_hash[..]], |r| {
                let row = map_row(r)?;
                let files = match r.get::<_, Option<Vec<u8>>>(18)? {
                    Some(d) => torrent::decode_files(&d),
                    None => vec![File { path: row.name.clone(), size: row.size }],
                };
                Ok((row, files))
            })
            .optional()
        })
    }

    /// Torrent counts per content type slug (`unknown` for unclassified).
    pub fn counts(&self) -> Result<Vec<(&'static str, u64)>> {
        self.with_conn(|c| {
            let mut stmt = c.prepare_cached("SELECT content_type, n FROM type_counts ORDER BY n DESC")?;
            let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?;
            rows.map(|r| {
                r.map(|(t, n)| (ContentType::from_id(t).map_or(classify::UNKNOWN_SLUG, ContentType::slug), n as u64))
            })
            .collect()
        })
    }

    pub fn db_size_bytes(&self) -> u64 {
        let wal = PathBuf::from(format!("{}-wal", self.cfg.path.display()));
        [self.cfg.path.as_path(), wal.as_path()].iter().filter_map(|p| std::fs::metadata(p).ok()).map(|m| m.len()).sum()
    }

    pub fn path(&self) -> &Path {
        &self.cfg.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::torrent::File;

    fn temp_cfg(name: &str) -> StoreConfig {
        let dir = std::env::temp_dir().join(format!("rustymagnet-test-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        StoreConfig { path: dir.join("db.sqlite"), cache_mb: 8, index_files: true, query_timeout: None }
    }

    fn mk(i: u8, name: &str, files: &[(&str, u64)]) -> Box<NewTorrent> {
        let files: Vec<File> = files.iter().map(|(p, s)| File { path: p.to_string(), size: *s }).collect();
        let info = TorrentInfo {
            name: name.into(),
            size: files.iter().map(|f| f.size).sum(),
            files_count: files.len() as u64,
            files,
            private: false,
        };
        Box::new(NewTorrent::new([i; 20], info, 3))
    }

    #[test]
    fn fts_query_escaping() {
        assert_eq!(fts_query("matrix 1999").unwrap(), "\"matrix\" AND \"1999\"");
        assert_eq!(fts_query("ubun* -iso").unwrap(), "(\"ubun\"*) NOT \"iso\"");
        assert_eq!(fts_query("S01E02").unwrap(), "\"S01E02\"");
        assert_eq!(fts_query("a\"b OR").unwrap(), "\"a\" + \"b\" AND \"OR\"");
        assert!(fts_query(" -x ").is_none());
        assert!(fts_query("\"\"").is_none());
        // Prefixes need MIN_PREFIX characters, otherwise the word is matched as is.
        assert_eq!(fts_query("ab*").unwrap(), "\"ab\"");
        assert_eq!(fts_query("abc*").unwrap(), "\"abc\"*");
        assert_eq!(fts_query("x.ab*").unwrap(), "\"x\" + \"ab\"");
        assert_eq!(fts_query("ab.cdé*").unwrap(), "\"ab\" + \"cdé\"*");
    }

    fn fill(cfg: &StoreConfig) {
        let stats = Arc::new(WriterStats::default());
        let (tx, handle) = spawn_writer(cfg.clone(), stats).unwrap();
        let names = [
            "The.Matrix.1999.1080p.BluRay.x264-GRP",
            "The.Matrix.Reloaded.2003.720p.BluRay.x264-GRP",
            "Matrix.Documentary.1999.2160p.WEB-DL.x265-GRP",
            "Severance.S02E03.1080p.WEB.h264-ETHEL",
            "Severance.S02E04.1080p.WEB.h264-ETHEL",
            "Severance.S01E01.720p.WEB.h264-ETHEL",
            "matrix-notes",
        ];
        for (i, name) in names.iter().enumerate() {
            let i = i as u8 + 1;
            let ext = if name.contains('.') { "mkv" } else { "txt" };
            let file = format!("dir/{name}.{ext}");
            tx.send(WriteMsg::Insert(mk(i, name, &[(&file, u64::from(i) << 30), ("dir/x.nfo", 1)]))).unwrap();
        }
        drop(tx);
        handle.join().unwrap();
    }

    /// Names and totals for queries that cover every search path.
    fn suite(r: &Reader) -> Vec<(Vec<String>, u64)> {
        let base = Query { limit: 10, sort: Sort::Newest, ..Default::default() };
        let movie = vec![Some(ContentType::Movie)];
        let tv = vec![Some(ContentType::TvShow)];
        let queries = [
            Query { text: "matrix".into(), ..base.clone() },
            Query { text: "matrix".into(), types: movie.clone(), ..base.clone() },
            Query { text: "matrix".into(), types: vec![None], ..base.clone() },
            Query { text: "matrix".into(), year: Some(1999), ..base.clone() },
            Query { text: "matrix".into(), resolution: Some("1080p".into()), ..base.clone() },
            Query { text: "matrix".into(), types: movie.clone(), sort: Sort::Largest, ..base.clone() },
            Query { text: "matrix".into(), min_size: Some(2 << 30), ..base.clone() },
            Query { text: "severance".into(), season: Some(2), episode: Some(3), ..base.clone() },
            Query { types: movie.clone(), ..base.clone() },
            Query { types: vec![Some(ContentType::Movie), None], sort: Sort::Oldest, ..base.clone() },
            Query { year: Some(1999), ..base.clone() },
            Query { year: Some(1999), sort: Sort::Largest, ..base.clone() },
            Query { types: tv.clone(), season: Some(2), ..base.clone() },
            Query { types: tv.clone(), sort: Sort::Peers, ..base.clone() },
            Query { types: tv, resolution: Some("720p".into()), ..base.clone() },
            Query { resolution: Some("1080p".into()), max_size: Some(4 << 30), ..base.clone() },
            Query { sort: Sort::Smallest, ..base },
        ];
        queries
            .iter()
            .map(|q| r.search(q).map(|res| (res.rows.into_iter().map(|r| r.name).collect(), res.total)).unwrap())
            .collect()
    }

    #[test]
    fn filters_in_search_index() {
        let cfg = temp_cfg("tags");
        fill(&cfg);
        let r = Reader::new(cfg.clone()).unwrap();
        assert!(r.tags);
        let got = suite(&r);
        let names = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(got[0].1, 4);
        assert_eq!(got[1].1, 3);
        assert_eq!(got[2].0, names(&["matrix-notes"]));
        assert_eq!(got[3].1, 2);
        assert_eq!(got[5].0[0], "Matrix.Documentary.1999.2160p.WEB-DL.x265-GRP");
        assert_eq!(got[7].0, names(&["Severance.S02E03.1080p.WEB.h264-ETHEL"]));
        assert_eq!(got[12].1, 2);
        assert_eq!(got[14].0, names(&["Severance.S01E01.720p.WEB.h264-ETHEL"]));
        assert_eq!(got[16].1, 7);

        // The same database with the search index of older versions gives the
        // same results without tags, and again after `reindex`.
        let legacy = temp_cfg("tags-legacy");
        drop(open_rw(&legacy).unwrap());
        Connection::open(&legacy.path)
            .unwrap()
            .execute_batch(
                "DROP TABLE torrents_fts;
                 CREATE VIRTUAL TABLE torrents_fts USING fts5(name, files, content='', contentless_delete=1,
                     tokenize='unicode61 remove_diacritics 2');",
            )
            .unwrap();
        fill(&legacy);
        let old = Reader::new(legacy.clone()).unwrap();
        assert!(!old.tags);
        assert_eq!(suite(&old), got);
        drop(old);
        upgrade(&legacy).unwrap();
        let new = Reader::new(legacy.clone()).unwrap();
        assert!(new.tags);
        assert_eq!(suite(&new), got);

        // reclassify replaces stale tags.
        let c = Connection::open(&cfg.path).unwrap();
        c.execute_batch(
            "UPDATE torrents SET content_type = 7 WHERE id = 1;
             UPDATE torrents_fts SET name = 'The.Matrix.1999.1080p.BluRay.x264-GRP', files = '', tags = 'ct7' WHERE rowid = 1;",
        )
        .unwrap();
        let games =
            Query { text: "matrix".into(), types: vec![Some(ContentType::Game)], limit: 10, ..Default::default() };
        assert_eq!(r.search(&games).unwrap().total, 1);
        reclassify(&cfg).unwrap();
        assert_eq!(r.search(&games).unwrap().total, 0);
        assert_eq!(suite(&r), got);

        for c in [cfg, legacy] {
            let _ = std::fs::remove_dir_all(c.path.parent().unwrap());
        }
    }

    #[test]
    fn slow_queries_time_out() {
        let cfg = temp_cfg("timeout");
        let mut r = Reader::new(cfg.clone()).unwrap();
        r.timeout = Some(Duration::from_millis(200));
        let t0 = Instant::now();
        let err = r
            .with_conn(|c| {
                c.query_row(
                    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n) SELECT count(*) FROM n",
                    [],
                    |r| r.get::<_, i64>(0),
                )
            })
            .unwrap_err();
        assert!(is_timeout(&err), "{err}");
        assert!(t0.elapsed() < Duration::from_secs(2));
        // The connection is fine for the next query.
        assert_eq!(r.with_conn(|c| c.query_row("SELECT 1", [], |r| r.get::<_, i64>(0))).unwrap(), 1);
        // Every test store has an 8 MiB cache.
        let limit = r.with_conn(|c| c.query_row("PRAGMA hard_heap_limit", [], |r| r.get::<_, i64>(0))).unwrap();
        assert_eq!(limit, (8 * 3 + 256) << 20);
        let _ = std::fs::remove_dir_all(cfg.path.parent().unwrap());
    }

    #[test]
    fn write_and_search() {
        let cfg = temp_cfg("search");
        let stats = Arc::new(WriterStats::default());
        let (tx, handle) = spawn_writer(cfg.clone(), stats.clone()).unwrap();
        tx.send(WriteMsg::Insert(mk(
            1,
            "The.Matrix.1999.1080p.BluRay.x264-GRP",
            &[("The.Matrix.1999.1080p.BluRay.x264-GRP.mkv", 8 << 30)],
        )))
        .unwrap();
        tx.send(WriteMsg::Insert(mk(
            2,
            "Severance.S02E03.720p.WEB.h264-ETHEL",
            &[("a/Severance.S02E03.mkv", 1 << 30), ("a/readme.txt", 10)],
        )))
        .unwrap();
        // Blocklisted content is never stored.
        tx.send(WriteMsg::Insert(mk(4, "pthc", &[("a.mp4", 1)]))).unwrap();
        tx.send(WriteMsg::Insert(mk(
            3,
            "ubuntu-24.04-desktop-amd64.iso",
            &[("ubuntu-24.04-desktop-amd64.iso", 6 << 30)],
        )))
        .unwrap();
        // Duplicate must be ignored.
        tx.send(WriteMsg::Insert(mk(1, "dupe", &[("dupe", 1)]))).unwrap();
        // Closing the channel makes the writer commit and exit.
        drop(tx);
        handle.join().unwrap();
        assert_eq!(stats.inserted.load(std::sync::atomic::Ordering::Relaxed), 3);

        let r = Reader::new(cfg.clone()).unwrap();
        let res = r.search(&Query { text: "matrix".into(), limit: 10, ..Default::default() }).unwrap();
        assert_eq!(res.rows.len(), 1);
        assert_eq!(res.rows[0].year, Some(1999));
        assert_eq!(res.rows[0].content_type, Some(ContentType::Movie));

        // File names are searchable too.
        let res = r.search(&Query { text: "readme".into(), limit: 10, ..Default::default() }).unwrap();
        assert_eq!(res.rows.len(), 1);

        let res = r
            .search(&Query {
                types: vec![Some(ContentType::TvShow)],
                season: Some(2),
                episode: Some(3),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(res.rows.len(), 1);
        assert_eq!(res.rows[0].episodes.as_deref(), Some("S02E03"));

        let res = r.search(&Query { sort: Sort::Largest, limit: 1, ..Default::default() }).unwrap();
        assert_eq!(res.rows[0].name, "The.Matrix.1999.1080p.BluRay.x264-GRP");
        assert_eq!(res.total, 3);

        let (row, files) = r.get(&[2; 20]).unwrap().unwrap();
        assert_eq!(row.name, "Severance.S02E03.720p.WEB.h264-ETHEL");
        assert_eq!(files.len(), 2);
        let (_, files) = r.get(&[3; 20]).unwrap().unwrap();
        assert_eq!(files.len(), 1);

        let counts = r.counts().unwrap();
        assert_eq!(counts.iter().map(|c| c.1).sum::<u64>(), 3);

        let bloom = Bloom::new(1000, 0.001);
        assert_eq!(load_known(&cfg, &bloom).unwrap(), 3);
        assert!(bloom.contains(&[1; 20]));

        assert_eq!(reclassify(&cfg).unwrap(), 3);
        assert_eq!(r.counts().unwrap().iter().map(|c| c.1).sum::<u64>(), 3);

        let _ = std::fs::remove_dir_all(cfg.path.parent().unwrap());
    }

    #[test]
    fn text_only_search_pages_in_fts() {
        let cfg = temp_cfg("fts-only");
        let stats = Arc::new(WriterStats::default());
        let (tx, handle) = spawn_writer(cfg.clone(), stats).unwrap();
        for i in 1..=5u8 {
            let name = format!("Common.Show.Part{i}.{}", if i == 3 { "common.common" } else { "x" });
            tx.send(WriteMsg::Insert(mk(i, &name, &[(&name, u64::from(i) << 20)]))).unwrap();
        }
        tx.send(WriteMsg::Insert(mk(9, "unrelated", &[("unrelated", 1)]))).unwrap();
        drop(tx);
        handle.join().unwrap();
        let r = Reader::new(cfg.clone()).unwrap();
        let names = |q: Query| -> (Vec<String>, u64) {
            let res = r.search(&q).unwrap();
            (res.rows.into_iter().map(|r| r.name).collect(), res.total)
        };
        let base = Query { text: "common".into(), limit: 2, ..Default::default() };

        let (rows, total) = names(Query { sort: Sort::Newest, ..base.clone() });
        assert_eq!(total, 5);
        assert_eq!(rows, ["Common.Show.Part5.x", "Common.Show.Part4.x"]);
        let (rows, _) = names(Query { sort: Sort::Newest, offset: 4, ..base.clone() });
        assert_eq!(rows, ["Common.Show.Part1.x"]);
        let (rows, _) = names(Query { sort: Sort::Oldest, ..base.clone() });
        assert_eq!(rows, ["Common.Show.Part1.x", "Common.Show.Part2.x"]);
        // The name mentioning the word three times ranks first.
        let (rows, total) = names(Query { sort: Sort::Relevance, ..base.clone() });
        assert_eq!(total, 5);
        assert_eq!(rows, ["Common.Show.Part3.common.common", "Common.Show.Part5.x"]);
        // Equal ranks page newest first without overlap.
        let (rows, _) = names(Query { sort: Sort::Relevance, offset: 2, ..base.clone() });
        assert_eq!(rows, ["Common.Show.Part4.x", "Common.Show.Part2.x"]);
        // Other sorts and filters still go through the join.
        let (rows, _) = names(Query { sort: Sort::Largest, ..base.clone() });
        assert_eq!(rows, ["Common.Show.Part5.x", "Common.Show.Part4.x"]);
        let (_, total) = names(Query { min_size: Some(3 << 20), ..base });
        assert_eq!(total, 3);

        let _ = std::fs::remove_dir_all(cfg.path.parent().unwrap());
    }
}

/// Synthetic scale benchmark: fills a database with crawler-like torrents
/// (names, file lists, file-name FTS) and prints where the bytes go.
///
/// RUSTYMAGNET_BENCH_DB=/tmp/bench.db RUSTYMAGNET_BENCH_N=1000000 \
///     cargo test --release bench_fill -- --ignored --nocapture
///
/// Re-running with the same path appends N more torrents. Query timings are
/// taken against `rustymagnet serve --db <path>` afterwards.
#[cfg(test)]
mod bench {
    use super::*;
    use crate::torrent::File;

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            // xorshift64*
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n.max(1)
        }
        fn unit(&mut self) -> f64 {
            (self.next() >> 11) as f64 / (1u64 << 53) as f64
        }
        /// Roughly Zipf(1)-distributed rank in `0..n` (log-uniform).
        fn zipf(&mut self, n: usize) -> usize {
            ((n as f64).powf(self.unit()) as usize).saturating_sub(1).min(n - 1)
        }
    }

    fn vocabulary(rng: &mut Rng, n: usize) -> Vec<String> {
        const SYL: [&str; 24] = [
            "ka", "lo", "mi", "ra", "ten", "su", "vor", "an", "el", "is", "ta", "ne", "do", "ri", "ma", "go", "be",
            "zu", "ok", "ul", "pre", "sto", "fa", "qui",
        ];
        (0..n)
            .map(|_| {
                let k = 2 + rng.below(3);
                let mut w: String = (0..k).map(|_| SYL[rng.below(SYL.len() as u64) as usize]).collect();
                if let Some(c) = w.get_mut(0..1) {
                    c.make_ascii_uppercase();
                }
                w
            })
            .collect()
    }

    fn words(rng: &mut Rng, vocab: &[String], n: u64) -> Vec<String> {
        (0..n).map(|_| vocab[rng.zipf(vocab.len())].clone()).collect()
    }

    fn torrent(rng: &mut Rng, vocab: &[String], hash_seed: u64) -> Box<NewTorrent> {
        const RES: [&str; 4] = ["1080p", "720p", "2160p", "480p"];
        const SRC: [&str; 4] = ["WEB-DL", "BluRay", "WEBRip", "HDTV"];
        const CODEC: [&str; 3] = ["x264", "x265", "H264"];
        const EXT: [&str; 8] = ["mkv", "mp4", "flac", "mp3", "jpg", "txt", "pdf", "srt"];
        let k = 2 + rng.below(5);
        let mut parts = words(rng, vocab, k);
        match rng.below(10) {
            0..=3 => parts.push(format!("{}", 1960 + rng.below(66))),
            4..=6 => parts.push(format!("S{:02}E{:02}", 1 + rng.below(12), 1 + rng.below(24))),
            _ => {}
        }
        if rng.below(10) < 6 {
            parts.push(RES[rng.zipf(RES.len())].into());
            parts.push(SRC[rng.below(4) as usize].into());
            parts.push(format!("{}-{}", CODEC[rng.below(3) as usize], vocab[rng.zipf(2000)]));
        }
        let name = parts.join(".");
        // Mostly single files or small packs, with a long tail of big ones.
        let nfiles = match rng.below(100) {
            0..=44 => 1,
            45..=74 => 2 + rng.below(9),
            75..=93 => 11 + rng.below(140),
            _ => 151 + rng.below(1850),
        } as usize;
        let files: Vec<File> = if nfiles == 1 {
            vec![File { path: name.clone(), size: rng.below(20 << 30) }]
        } else {
            let sub = words(rng, vocab, 1).join(" ");
            (0..nfiles)
                .map(|i| {
                    let k = 1 + rng.below(4);
                    let title = words(rng, vocab, k).join(" ");
                    let ext = EXT[rng.below(EXT.len() as u64) as usize];
                    File { path: format!("{sub}/{:02} - {title}.{ext}", i + 1), size: rng.below(2 << 30) }
                })
                .collect()
        };
        let info = TorrentInfo {
            name,
            size: files.iter().map(|f| f.size).sum(),
            files_count: files.len() as u64,
            files,
            private: false,
        };
        let mut h = [0u8; 20];
        h.copy_from_slice(&sha1_smol::Sha1::from(hash_seed.to_le_bytes()).digest().bytes());
        Box::new(NewTorrent::new(h, info, rng.below(200) as u32))
    }

    #[test]
    #[ignore]
    fn bench_fill() {
        let path: PathBuf = std::env::var("RUSTYMAGNET_BENCH_DB").unwrap_or("/tmp/rustymagnet-bench.db".into()).into();
        let n: u64 = std::env::var("RUSTYMAGNET_BENCH_N").ok().and_then(|v| v.parse().ok()).unwrap_or(100_000);
        let cfg = StoreConfig { path, cache_mb: 64, index_files: true, query_timeout: None };
        let mut conn = open_rw(&cfg).unwrap();
        let tags = fts_has_tags(&conn).unwrap();
        let start: u64 =
            conn.query_row("SELECT coalesce(max(id), 0) FROM torrents", [], |r| r.get::<_, i64>(0)).unwrap() as u64;
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        let vocab = vocabulary(&mut rng, 300_000);
        eprintln!(
            "words by frequency rank 0/10/1000/100000: {} {} {} {}",
            vocab[0], vocab[10], vocab[1000], vocab[100_000]
        );
        let mut rng = Rng(0x1234_5678 ^ start.wrapping_mul(0x9e37_79b9));
        let t0 = Instant::now();
        let mut done = 0;
        while done < n {
            let batch: Vec<Box<NewTorrent>> =
                (0..1000.min(n - done)).map(|i| torrent(&mut rng, &vocab, start + done + i)).collect();
            write_batch(&mut conn, &batch, true, tags).unwrap();
            done += batch.len() as u64;
            if done % 100_000 == 0 {
                eprintln!("{done} torrents, {:.0} s", t0.elapsed().as_secs_f64());
            }
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        let total: i64 = conn.query_row("SELECT count(*) FROM torrents", [], |r| r.get(0)).unwrap();
        let mut stmt = conn.prepare("SELECT name, sum(pgsize) FROM dbstat GROUP BY name ORDER BY 2 DESC").unwrap();
        let rows: Vec<(String, i64)> =
            stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().collect::<Result<_>>().unwrap();
        let sum: i64 = rows.iter().map(|r| r.1).sum();
        eprintln!(
            "{total} torrents, {:.1} MiB, {:.0} bytes/torrent",
            sum as f64 / 1048576.0,
            sum as f64 / total as f64
        );
        for (name, bytes) in rows {
            eprintln!(
                "  {name:<28} {:>10.1} MiB {:>6.0} B/torrent",
                bytes as f64 / 1048576.0,
                bytes as f64 / total as f64
            );
        }
    }
}
