//! rustymagnet — a lightweight, self-hosted BitTorrent DHT crawler, classifier
//! and search engine in the spirit of bitmagnet.

mod bencode;
mod bloom;
mod classify;
mod crawler;
mod http;
mod krpc;
mod log;
mod metadata;
mod store;
mod torrent;
mod util;

use std::io::BufRead;
use std::path::PathBuf;
use std::sync::Arc;

use crate::bloom::Bloom;
use crate::crawler::{Crawler, CrawlerConfig, CrawlerStats};
use crate::store::{NewTorrent, StoreConfig, WriteMsg, WriterStats};
use crate::torrent::{File, TorrentInfo};

const USAGE: &str = "\
rustymagnet — self-hosted BitTorrent DHT crawler & search engine

USAGE:
    rustymagnet [COMMAND] [OPTIONS]

COMMANDS:
    run          Crawl the DHT and serve the web UI / API (default)
    serve        Serve the web UI / API only, no crawling
    import       Import `info_hash<TAB>name<TAB>size[<TAB>unix_time]` lines from stdin
    reclassify   Re-run the classifier over every stored torrent
    help         Show this message

OPTIONS (environment variable in brackets):
    --db PATH          SQLite database file              [RUSTYMAGNET_DB]        (./rustymagnet.db)
    --http ADDR        Web UI / API listen address       [RUSTYMAGNET_HTTP]      (0.0.0.0:3333)
    --dht-port PORT    UDP port for the DHT              [RUSTYMAGNET_DHT_PORT]  (3334)
    --rate N           Outgoing DHT queries per second   [RUSTYMAGNET_RATE]      (150)
    --fetchers N       Torrents fetched concurrently     [RUSTYMAGNET_FETCHERS]  (64)
    --max-nodes N      DHT nodes kept in memory          [RUSTYMAGNET_MAX_NODES] (50000)
    --cache-mb N       SQLite page cache per connection  [RUSTYMAGNET_CACHE_MB]  (64)
    --no-file-index    Only index torrent names for search, not file names
                                                         [RUSTYMAGNET_NO_FILE_INDEX=1]
    --skip-xxx         Do not store adult content        [RUSTYMAGNET_SKIP_XXX=1]

Log level: RUSTYMAGNET_LOG=error|warn|info|debug
";

struct Options {
    command: String,
    db: PathBuf,
    http: String,
    crawler: CrawlerConfig,
    cache_mb: u32,
    index_files: bool,
}

fn parse_args() -> Result<Options, String> {
    let env = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
    let mut crawler = CrawlerConfig::default();
    let mut o = Options {
        command: "run".into(),
        db: env("RUSTYMAGNET_DB").unwrap_or_else(|| "rustymagnet.db".into()).into(),
        http: env("RUSTYMAGNET_HTTP").unwrap_or_else(|| "0.0.0.0:3333".into()),
        crawler: CrawlerConfig::default(),
        cache_mb: 64,
        index_files: !matches!(env("RUSTYMAGNET_NO_FILE_INDEX").as_deref(), Some("1" | "true" | "yes")),
    };
    crawler.skip_xxx = matches!(env("RUSTYMAGNET_SKIP_XXX").as_deref(), Some("1" | "true" | "yes"));
    let set = |key: &str, val: &str, o: &mut Options, crawler: &mut CrawlerConfig| -> Result<(), String> {
        let num = |v: &str| v.parse::<u64>().map_err(|_| format!("invalid value for {key}: {v}"));
        match key {
            "db" => o.db = val.into(),
            "http" => o.http = val.into(),
            "dht-port" => crawler.port = u16::try_from(num(val)?).map_err(|_| "invalid port")?,
            "rate" => crawler.rate = num(val)?.clamp(1, 100_000) as u32,
            "fetchers" => crawler.fetch_concurrency = num(val)?.clamp(1, 10_000) as usize,
            "max-nodes" => crawler.max_nodes = num(val)?.clamp(100, 10_000_000) as usize,
            "cache-mb" => o.cache_mb = num(val)?.clamp(2, 65_536) as u32,
            _ => return Err(format!("unknown option --{key}")),
        }
        Ok(())
    };
    for (k, var) in [
        ("dht-port", "RUSTYMAGNET_DHT_PORT"),
        ("rate", "RUSTYMAGNET_RATE"),
        ("fetchers", "RUSTYMAGNET_FETCHERS"),
        ("max-nodes", "RUSTYMAGNET_MAX_NODES"),
        ("cache-mb", "RUSTYMAGNET_CACHE_MB"),
    ] {
        if let Some(v) = env(var) {
            set(k, &v, &mut o, &mut crawler)?;
        }
    }
    let mut args = std::env::args().skip(1);
    let mut first = true;
    while let Some(a) = args.next() {
        if let Some(opt) = a.strip_prefix("--") {
            if opt == "no-file-index" {
                o.index_files = false;
            } else if opt == "skip-xxx" {
                crawler.skip_xxx = true;
            } else if opt == "help" {
                o.command = "help".into();
            } else {
                let (k, v) = match opt.split_once('=') {
                    Some((k, v)) => (k.to_string(), v.to_string()),
                    None => (opt.to_string(), args.next().ok_or_else(|| format!("--{opt} needs a value"))?),
                };
                set(&k, &v, &mut o, &mut crawler)?;
            }
        } else if first {
            o.command = a;
        } else {
            return Err(format!("unexpected argument: {a}"));
        }
        first = false;
    }
    o.crawler = crawler;
    Ok(o)
}

fn main() {
    log::init_from_env();
    let opts = match parse_args() {
        Ok(o) => o,
        Err(e) => {
            eprintln!("error: {e}\n\n{USAGE}");
            std::process::exit(2);
        }
    };
    let store_cfg = StoreConfig { path: opts.db.clone(), cache_mb: opts.cache_mb, index_files: opts.index_files };
    let result = match opts.command.as_str() {
        "run" => run(&opts, store_cfg, true),
        "serve" => run(&opts, store_cfg, false),
        "import" => import(store_cfg, opts.crawler.skip_xxx),
        "reclassify" => {
            store::reclassify(&store_cfg).map(|n| log_info!("reclassified {n} torrents")).map_err(|e| e.to_string())
        }
        "help" | "-h" => {
            print!("{USAGE}");
            Ok(())
        }
        c => Err(format!("unknown command {c}\n\n{USAGE}")),
    };
    if let Err(e) = result {
        log_error!("{e}");
        std::process::exit(1);
    }
}

fn run(opts: &Options, store_cfg: StoreConfig, crawl: bool) -> Result<(), String> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(std::thread::available_parallelism().map_or(2, |n| n.get().min(4)))
        .max_blocking_threads(16)
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    rt.block_on(async {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let reader =
            store::Reader::new(store_cfg.clone()).map_err(|e| format!("opening {}: {e}", store_cfg.path.display()))?;
        log_info!("database: {}", reader.path().display());

        let mut writer_handle = None;
        let mut crawler_task = None;
        let crawler_stats = if crawl {
            let known = store::count_torrents(&store_cfg).map_err(|e| e.to_string())?;
            let bloom = Arc::new(Bloom::new((known as usize * 2).max(10_000_000), 0.0005));
            let cfg = store_cfg.clone();
            let b = bloom.clone();
            let loaded = tokio::task::spawn_blocking(move || store::load_known(&cfg, &b))
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?;
            log_info!("loaded {loaded} known info hashes (bloom filter {} MiB)", bloom.memory_bytes() >> 20);
            let (writer, handle) =
                store::spawn_writer(store_cfg.clone(), Arc::new(WriterStats::default())).map_err(|e| e.to_string())?;
            writer_handle = Some(handle);
            let stats = Arc::new(CrawlerStats::default());
            let crawler = Crawler::bind(opts.crawler.clone(), bloom, writer, stats.clone())
                .await
                .map_err(|e| format!("binding DHT port {}: {e}", opts.crawler.port))?;
            crawler_task = Some(tokio::spawn(crawler.run(shutdown_rx.clone())));
            Some(stats)
        } else {
            None
        };

        let listener =
            tokio::net::TcpListener::bind(&opts.http).await.map_err(|e| format!("binding {}: {e}", opts.http))?;
        let state = Arc::new(http::AppState { reader, crawler: crawler_stats, started: std::time::Instant::now() });
        let http_task = tokio::spawn(http::serve(listener, state, shutdown_rx));

        wait_for_signal().await;
        log_info!("shutting down");
        let _ = shutdown_tx.send(true);
        if let Some(t) = crawler_task {
            let _ = t.await;
        }
        let _ = http_task.await;
        if let Some(h) = writer_handle {
            // The crawler (and with it the last writer sender) is gone: the writer
            // commits what is left and exits.
            let _ = tokio::task::spawn_blocking(move || h.join()).await;
        }
        Ok(())
    })
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        let mut term = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Undoes PostgreSQL COPY text-format escaping.
fn unescape_copy(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some(o) => out.push(o),
            None => out.push('\\'),
        }
    }
    out
}

fn parse_import_line(line: &str) -> Option<NewTorrent> {
    let mut cols = line.split('\t');
    let hash = cols.next()?.trim();
    let hash = torrent::parse_hex(hash.strip_prefix("\\\\x").or_else(|| hash.strip_prefix("\\x")).unwrap_or(hash))?;
    let name = unescape_copy(cols.next()?).trim().to_string();
    let size: u64 = cols.next()?.trim().parse().ok()?;
    if name.is_empty() {
        return None;
    }
    let ts = cols.next().and_then(|t| t.trim().split('.').next()?.parse::<i64>().ok());
    let info =
        TorrentInfo { files: vec![File { path: name.clone(), size }], name, size, files_count: 1, private: false };
    let mut t = NewTorrent::new(hash, info, 0);
    if let Some(ts) = ts {
        t.discovered_at = ts;
    }
    Some(t)
}

fn import(store_cfg: StoreConfig, skip_xxx: bool) -> Result<(), String> {
    let stats = Arc::new(WriterStats::default());
    let (writer, handle) = store::spawn_writer(store_cfg, stats.clone()).map_err(|e| e.to_string())?;
    let stdin = std::io::stdin();
    let (mut read, mut bad, mut filtered) = (0u64, 0u64, 0u64);
    for line in stdin.lock().lines() {
        let line = line.map_err(|e| e.to_string())?;
        if line.trim().is_empty() {
            continue;
        }
        read += 1;
        match parse_import_line(&line) {
            Some(t) if t.class.banned || (skip_xxx && t.class.content_type == Some(classify::ContentType::Xxx)) => {
                filtered += 1
            }
            Some(t) => writer.send(WriteMsg::Insert(Box::new(t))).map_err(|_| "writer stopped")?,
            None => bad += 1,
        }
        if read % 100_000 == 0 {
            log_info!("import: {read} lines read");
        }
    }
    drop(writer);
    let _ = handle.join();
    log_info!(
        "import: {read} lines, {} new torrents, {filtered} filtered out, {bad} unparseable",
        stats.inserted.load(std::sync::atomic::Ordering::Relaxed)
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn import_lines() {
        let t = parse_import_line(
            "\\\\x0123456789abcdef0123456789abcdef01234567\tThe.Matrix.1999.1080p\\tx\t123\t1700000000.5",
        )
        .unwrap();
        assert_eq!(t.info.name, "The.Matrix.1999.1080p\tx");
        assert_eq!(t.info.size, 123);
        assert_eq!(t.discovered_at, 1_700_000_000);
        assert!(parse_import_line("abc\tname\t1").is_none());
        assert!(parse_import_line("0123456789abcdef0123456789abcdef01234567\tname\tnope").is_none());
    }
}
