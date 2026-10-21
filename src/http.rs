//! A deliberately small HTTP/1.1 server (GET only, one request per
//! connection) serving the web UI, a JSON API and a Torznab endpoint.

use std::net::SocketAddr;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

use crate::classify::{self, ContentType};
use crate::crawler::CrawlerStats;
use crate::store::{self, Query, Reader, Row, Sort};
use crate::torrent::{self, File};
use crate::{log_debug, log_info};

const UI: &str = include_str!("ui.html");
const MAX_REQUEST: usize = 16 * 1024;

pub struct AppState {
    pub reader: Reader,
    pub crawler: Option<Arc<CrawlerStats>>,
    pub started: std::time::Instant,
}

pub struct Response {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
}

impl Response {
    fn new(status: u16, content_type: &'static str, body: impl Into<Vec<u8>>) -> Self {
        Response { status, content_type, body: body.into() }
    }

    fn json(body: String) -> Self {
        Response::new(200, "application/json; charset=utf-8", body)
    }

    fn error(status: u16, msg: &str) -> Self {
        let mut s = String::from("{\"error\":");
        json_str(&mut s, msg);
        s.push('}');
        Response::new(status, "application/json; charset=utf-8", s)
    }
}

pub async fn serve(listener: TcpListener, state: Arc<AppState>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
    log_info!("http: listening on http://{}", listener.local_addr().map(|a| a.to_string()).unwrap_or_default());
    // Bound concurrent connections so a burst cannot exhaust the NAS.
    let conns = Arc::new(Semaphore::new(64));
    loop {
        tokio::select! {
            r = listener.accept() => {
                let Ok((stream, peer)) = r else { continue };
                let Ok(permit) = conns.clone().try_acquire_owned() else { continue };
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_conn(stream, peer, state).await {
                        log_debug!("http: {peer}: {e}");
                    }
                    drop(permit);
                });
            }
            _ = shutdown.changed() => break,
        }
    }
}

async fn handle_conn(mut stream: TcpStream, _peer: SocketAddr, state: Arc<AppState>) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(1024);
    let head_end = tokio::time::timeout(Duration::from_secs(10), async {
        let mut chunk = [0u8; 2048];
        loop {
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                return Ok(None);
            }
            buf.extend_from_slice(&chunk[..n]);
            if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                return Ok(Some(p));
            }
            if buf.len() > MAX_REQUEST {
                return Ok::<_, std::io::Error>(None);
            }
        }
    })
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "request timeout"))??;
    let Some(head_end) = head_end else { return Ok(()) };
    let head = String::from_utf8_lossy(&buf[..head_end]);
    let mut parts = head.lines().next().unwrap_or("").split(' ');
    let (method, target) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    let resp = match method {
        "GET" | "HEAD" => {
            let (path, query) = target.split_once('?').unwrap_or((target, ""));
            route(path.to_string(), parse_query(query), state).await
        }
        _ => Response::error(405, "method not allowed"),
    };
    let reason = match resp.status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        503 => "Service Unavailable",
        _ => "Internal Server Error",
    };
    let header = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n",
        resp.status,
        reason,
        resp.content_type,
        resp.body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    if method != "HEAD" {
        stream.write_all(&resp.body).await?;
    }
    stream.shutdown().await
}

pub type Params = Vec<(String, String)>;

fn param<'a>(p: &'a Params, key: &str) -> Option<&'a str> {
    p.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str()).filter(|v| !v.is_empty())
}

pub fn parse_query(q: &str) -> Params {
    q.split('&')
        .filter(|s| !s.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (percent_decode(k), percent_decode(v))
        })
        .collect()
}

pub fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() && b[i + 1].is_ascii_hexdigit() && b[i + 2].is_ascii_hexdigit() => {
                // Both bytes are ASCII hex digits, so this cannot fail.
                let hex = std::str::from_utf8(&b[i + 1..i + 3]).unwrap_or("0");
                out.push(u8::from_str_radix(hex, 16).unwrap_or(0));
                i += 3;
                continue;
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

async fn route(path: String, params: Params, state: Arc<AppState>) -> Response {
    match path.as_str() {
        "/" | "/index.html" => Response::new(200, "text/html; charset=utf-8", UI),
        "/healthz" => Response::new(200, "text/plain", "ok"),
        "/api/stats" => blocking(state, move |s| Ok(stats_json(s))).await,
        "/api/search" => match build_query(&params) {
            Ok(q) => blocking(state, move |s| search_json(s, &q)).await,
            Err(e) => Response::error(400, &e),
        },
        "/torznab" | "/torznab/api" | "/api/torznab" | "/api/torznab/api" => {
            blocking(state, move |s| Ok(torznab(s, &params))).await
        }
        p if p.starts_with("/api/torrent/") => match torrent::parse_hex(&p["/api/torrent/".len()..]) {
            Some(h) => blocking(state, move |s| torrent_json(s, &h)).await,
            None => Response::error(400, "invalid info hash"),
        },
        _ => Response::error(404, "not found"),
    }
}

async fn blocking(
    state: Arc<AppState>,
    f: impl FnOnce(&AppState) -> rusqlite::Result<Response> + Send + 'static,
) -> Response {
    match tokio::task::spawn_blocking(move || f(&state)).await {
        Ok(Ok(r)) => r,
        Ok(Err(e)) if store::is_timeout(&e) => {
            Response::error(503, "the search took too long: add more words or filters")
        }
        Ok(Err(e)) => {
            log_info!("http: database error: {e}");
            Response::error(500, "database error")
        }
        Err(_) => Response::error(500, "internal error"),
    }
}

fn build_query(p: &Params) -> Result<Query, String> {
    let num = |k: &str| -> Result<Option<u64>, String> {
        param(p, k).map(|v| v.parse::<u64>().map_err(|_| format!("invalid {k}"))).transpose()
    };
    let mut types = Vec::new();
    if let Some(t) = param(p, "type") {
        for slug in t.split(',') {
            if slug == classify::UNKNOWN_SLUG {
                types.push(None);
            } else {
                types.push(Some(ContentType::from_slug(slug).ok_or_else(|| format!("unknown type {slug}"))?));
            }
        }
    }
    let text = param(p, "q").unwrap_or("").to_string();
    let sort = match param(p, "sort") {
        Some(s) => Sort::parse(s).ok_or("invalid sort")?,
        None if text.trim().is_empty() => Sort::Newest,
        None => Sort::Relevance,
    };
    Ok(Query {
        text,
        types,
        year: num("year")?.map(|y| y as u16),
        resolution: param(p, "resolution").map(str::to_string),
        season: num("season")?.map(|s| s as u16),
        episode: num("episode")?.map(|s| s as u16),
        min_size: num("min_size")?,
        max_size: num("max_size")?,
        sort,
        limit: num("limit")?.unwrap_or(50).min(500) as u32,
        offset: num("offset")?.unwrap_or(0).min(100_000) as u32,
    })
}

// ------------------------------------------------------------------ JSON

pub fn json_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 || c == '\u{2028}' || c == '\u{2029}' => {
                out.push_str(&format!("\\u{:04x}", c as u32))
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

struct Obj<'a> {
    out: &'a mut String,
    first: bool,
}

impl<'a> Obj<'a> {
    fn new(out: &'a mut String) -> Self {
        out.push('{');
        Obj { out, first: true }
    }
    fn key(&mut self, k: &str) -> &mut String {
        if !self.first {
            self.out.push(',');
        }
        self.first = false;
        json_str(self.out, k);
        self.out.push(':');
        self.out
    }
    fn str(&mut self, k: &str, v: &str) -> &mut Self {
        let o = self.key(k);
        json_str(o, v);
        self
    }
    fn opt_str(&mut self, k: &str, v: Option<&str>) -> &mut Self {
        match v {
            Some(v) => self.str(k, v),
            None => self.raw(k, "null"),
        }
    }
    fn num(&mut self, k: &str, v: impl std::fmt::Display) -> &mut Self {
        let s = v.to_string();
        self.raw(k, &s)
    }
    fn opt_num(&mut self, k: &str, v: Option<impl std::fmt::Display>) -> &mut Self {
        match v {
            Some(v) => self.num(k, v),
            None => self.raw(k, "null"),
        }
    }
    fn raw(&mut self, k: &str, v: &str) -> &mut Self {
        self.key(k).push_str(v);
        self
    }
    fn end(&mut self) {
        self.out.push('}');
    }
}

fn row_json(out: &mut String, r: &Row) {
    let mut o = Obj::new(out);
    o.str("info_hash", &torrent::hex(&r.info_hash))
        .str("name", &r.name)
        .num("size", r.size)
        .num("files_count", r.files_count)
        .raw("private", if r.private { "true" } else { "false" })
        .str("content_type", r.content_type.map_or(classify::UNKNOWN_SLUG, ContentType::slug))
        .opt_str("title", r.title.as_deref())
        .opt_num("year", r.year)
        .opt_num("season", r.season)
        .opt_str("episodes", r.episodes.as_deref())
        .opt_str("resolution", r.resolution.as_deref())
        .opt_str("source", r.source.as_deref())
        .opt_str("video_codec", r.video_codec.as_deref())
        .opt_str("modifiers", r.modifiers.as_deref())
        .opt_str("languages", r.languages.as_deref())
        .opt_str("release_group", r.release_group.as_deref())
        .num("peers", r.peers)
        .str("discovered_at", &rfc3339(r.discovered_at))
        .str("magnet", &torrent::magnet(&r.info_hash, &r.name))
        .end();
}

fn search_json(s: &AppState, q: &Query) -> rusqlite::Result<Response> {
    let t = std::time::Instant::now();
    let res = s.reader.search(q)?;
    let mut out = String::with_capacity(512 * res.rows.len() + 128);
    let mut o = Obj::new(&mut out);
    o.num("total", res.total)
        .raw("total_capped", if res.total_capped { "true" } else { "false" })
        .num("took_ms", t.elapsed().as_millis());
    let buf = o.key("results");
    buf.push('[');
    for (i, r) in res.rows.iter().enumerate() {
        if i > 0 {
            buf.push(',');
        }
        row_json(buf, r);
    }
    buf.push(']');
    o.end();
    Ok(Response::json(out))
}

fn torrent_json(s: &AppState, h: &torrent::InfoHash) -> rusqlite::Result<Response> {
    let Some((row, files)) = s.reader.get(h)? else { return Ok(Response::error(404, "not found")) };
    let mut out = String::new();
    let mut o = Obj::new(&mut out);
    let buf = o.key("torrent");
    row_json(buf, &row);
    let buf = o.key("files");
    files_json(buf, &files);
    o.end();
    Ok(Response::json(out))
}

fn files_json(out: &mut String, files: &[File]) {
    out.push('[');
    for (i, f) in files.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        Obj::new(out).str("path", &f.path).num("size", f.size).end();
    }
    out.push(']');
}

fn stats_json(s: &AppState) -> Response {
    let counts = s.reader.counts().unwrap_or_default();
    let mut out = String::new();
    let mut o = Obj::new(&mut out);
    o.num("torrents", counts.iter().map(|c| c.1).sum::<u64>());
    let buf = o.key("content_types");
    let mut t = Obj::new(buf);
    for (slug, n) in &counts {
        t.num(slug, n);
    }
    t.end();
    o.num("db_bytes", s.reader.db_size_bytes()).num("uptime_secs", s.started.elapsed().as_secs());
    match &s.crawler {
        Some(c) => {
            let buf = o.key("crawler");
            Obj::new(buf)
                .num("nodes", c.nodes.load(Relaxed))
                .num("lookups", c.lookups.load(Relaxed))
                .num("fetching", c.fetching.load(Relaxed))
                .num("packets_in", c.packets_in.load(Relaxed))
                .num("packets_out", c.packets_out.load(Relaxed))
                .num("hashes_seen", c.hashes_seen.load(Relaxed))
                .num("hashes_new", c.hashes_new.load(Relaxed))
                .num("fetched", c.fetched.load(Relaxed))
                .num("fetch_failed", c.fetch_failed.load(Relaxed))
                .end();
        }
        None => {
            o.raw("crawler", "null");
        }
    }
    o.end();
    Response::json(out)
}

// ------------------------------------------------------------------ Torznab

fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c if (c as u32) < 0x20 && !matches!(c, '\t' | '\n' | '\r') => {}
            c => out.push(c),
        }
    }
    out
}

const TORZNAB_CAPS: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<caps>
  <server version="1.0" title="rustymagnet"/>
  <limits max="100" default="100"/>
  <searching>
    <search available="yes" supportedParams="q"/>
    <tv-search available="yes" supportedParams="q,season,ep"/>
    <movie-search available="yes" supportedParams="q"/>
    <music-search available="yes" supportedParams="q"/>
    <audio-search available="yes" supportedParams="q"/>
    <book-search available="yes" supportedParams="q"/>
  </searching>
  <categories>
    <category id="2000" name="Movies"><subcat id="2030" name="Movies/SD"/><subcat id="2040" name="Movies/HD"/><subcat id="2045" name="Movies/UHD"/></category>
    <category id="3000" name="Audio"><subcat id="3010" name="Audio/MP3"/><subcat id="3030" name="Audio/Audiobook"/><subcat id="3040" name="Audio/Lossless"/></category>
    <category id="4000" name="PC"><subcat id="4050" name="PC/Games"/></category>
    <category id="5000" name="TV"><subcat id="5030" name="TV/SD"/><subcat id="5040" name="TV/HD"/><subcat id="5045" name="TV/UHD"/></category>
    <category id="6000" name="XXX"/>
    <category id="7000" name="Books"><subcat id="7020" name="Books/EBook"/><subcat id="7030" name="Books/Comics"/></category>
    <category id="8000" name="Other"/>
  </categories>
</caps>
"#;

fn torznab_error(code: u32, msg: &str) -> Response {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<error code=\"{code}\" description=\"{}\"/>\n",
        xml_escape(msg)
    );
    Response::new(200, "application/xml; charset=utf-8", body)
}

fn torznab(s: &AppState, p: &Params) -> Response {
    let t = param(p, "t").unwrap_or("search");
    if t == "caps" {
        return Response::new(200, "application/xml; charset=utf-8", TORZNAB_CAPS);
    }
    let mut types: Vec<Option<ContentType>> = Vec::new();
    if let Some(cats) = param(p, "cat") {
        for c in cats.split(',').filter_map(|c| c.trim().parse::<u32>().ok()) {
            if c == 8000 {
                types.push(None);
            }
            for ty in classify::types_for_torznab_category(c) {
                if !types.contains(&Some(ty)) {
                    types.push(Some(ty));
                }
            }
        }
        if types.is_empty() {
            return torznab_rss(&[]);
        }
    }
    let default_types: &[ContentType] = match t {
        "search" => &[],
        "tvsearch" => &[ContentType::TvShow],
        "movie" => &[ContentType::Movie],
        "music" | "audio" => &[ContentType::Music],
        "book" => &[ContentType::Ebook, ContentType::Audiobook, ContentType::Comic],
        _ => return torznab_error(202, "no such function"),
    };
    if types.is_empty() {
        types = default_types.iter().map(|t| Some(*t)).collect();
    }
    let num = |k: &str| param(p, k).and_then(|v| v.parse::<u64>().ok());
    let mut text = param(p, "q").unwrap_or("").to_string();
    // Common *arr extras that can be matched as text.
    for k in ["artist", "album", "title", "author"] {
        if let Some(v) = param(p, k) {
            text.push(' ');
            text.push_str(v);
        }
    }
    let season = num("season").map(|v| v as u16);
    let q = Query {
        sort: if text.trim().is_empty() { Sort::Newest } else { Sort::Relevance },
        text,
        types,
        season,
        episode: season.and(num("ep").map(|v| v as u16)),
        limit: num("limit").unwrap_or(100).min(100) as u32,
        offset: num("offset").unwrap_or(0).min(100_000) as u32,
        ..Default::default()
    };
    match s.reader.search(&q) {
        Ok(r) => torznab_rss(&r.rows),
        Err(e) => torznab_error(900, &e.to_string()),
    }
}

fn torznab_rss(rows: &[Row]) -> Response {
    let mut out = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<rss version=\"2.0\" xmlns:atom=\"http://www.w3.org/2005/Atom\" xmlns:torznab=\"http://torznab.com/schemas/2015/feed\">\n<channel>\n<title>rustymagnet</title>\n",
    );
    for r in rows {
        let magnet = xml_escape(&torrent::magnet(&r.info_hash, &r.name));
        let hash = torrent::hex(&r.info_hash);
        let cat = classify::torznab_category(r.content_type, r.resolution.as_deref(), &r.name);
        let parent = cat / 1000 * 1000;
        out.push_str(&format!(
            "<item>\n<title>{title}</title>\n<guid>{hash}</guid>\n<link>{magnet}</link>\n<pubDate>{date}</pubDate>\n<size>{size}</size>\n\
             <enclosure url=\"{magnet}\" length=\"{size}\" type=\"application/x-bittorrent;x-scheme-handler/magnet\"/>\n\
             <category>{parent}</category>\n<category>{cat}</category>\n\
             <torznab:attr name=\"category\" value=\"{parent}\"/>\n<torznab:attr name=\"category\" value=\"{cat}\"/>\n\
             <torznab:attr name=\"infohash\" value=\"{hash}\"/>\n<torznab:attr name=\"magneturl\" value=\"{magnet}\"/>\n\
             <torznab:attr name=\"size\" value=\"{size}\"/>\n<torznab:attr name=\"files\" value=\"{files}\"/>\n\
             <torznab:attr name=\"seeders\" value=\"{peers}\"/>\n<torznab:attr name=\"peers\" value=\"{peers}\"/>\n\
             <torznab:attr name=\"downloadvolumefactor\" value=\"0\"/>\n<torznab:attr name=\"uploadvolumefactor\" value=\"1\"/>\n",
            title = xml_escape(&r.name),
            date = rfc2822(r.discovered_at),
            size = r.size,
            files = r.files_count,
            peers = r.peers,
        ));
        if let Some(y) = r.year {
            out.push_str(&format!("<torznab:attr name=\"year\" value=\"{y}\"/>\n"));
        }
        if let Some(s) = r.season {
            out.push_str(&format!("<torznab:attr name=\"season\" value=\"{s}\"/>\n"));
        }
        out.push_str("</item>\n");
    }
    out.push_str("</channel>\n</rss>\n");
    Response::new(200, "application/rss+xml; charset=utf-8", out)
}

// ------------------------------------------------------------------ dates

/// (year, month, day, hour, min, sec, weekday 0=Sunday) in UTC.
fn civil(ts: i64) -> (i64, u32, u32, u32, u32, u32, u32) {
    let days = ts.div_euclid(86400);
    let secs = ts.rem_euclid(86400) as u32;
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    let wd = (days + 4).rem_euclid(7) as u32;
    (y, m, d, secs / 3600, secs / 60 % 60, secs % 60, wd)
}

pub fn rfc3339(ts: i64) -> String {
    let (y, mo, d, h, mi, s, _) = civil(ts);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

pub fn rfc2822(ts: i64) -> String {
    const DAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MONTHS: [&str; 12] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let (y, mo, d, h, mi, s, wd) = civil(ts);
    format!("{}, {d:02} {} {y:04} {h:02}:{mi:02}:{s:02} +0000", DAYS[wd as usize], MONTHS[mo as usize - 1])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339(1_700_000_000), "2023-11-14T22:13:20Z");
        assert_eq!(rfc2822(1_700_000_000), "Tue, 14 Nov 2023 22:13:20 +0000");
        assert_eq!(rfc3339(951_782_400), "2000-02-29T00:00:00Z");
    }

    #[test]
    fn query_parsing() {
        let p = parse_query("q=the+matrix%20199%39&type=movie,tv_show&x=%zz&y=%4&z=%+4");
        assert_eq!(param(&p, "q"), Some("the matrix 1999"));
        assert_eq!(param(&p, "x"), Some("%zz"));
        assert_eq!(param(&p, "y"), Some("%4"));
        assert_eq!(param(&p, "z"), Some("% 4"));
        let q = build_query(&p).unwrap();
        assert_eq!(q.types, vec![Some(ContentType::Movie), Some(ContentType::TvShow)]);
        assert_eq!(q.sort, Sort::Relevance);
        assert!(build_query(&parse_query("type=nope")).is_err());
        assert!(build_query(&parse_query("limit=abc")).is_err());
    }

    #[test]
    fn json_escaping() {
        let mut s = String::new();
        json_str(&mut s, "a\"b\\c\n\u{1}");
        assert_eq!(s, "\"a\\\"b\\\\c\\n\\u0001\"");
        assert_eq!(xml_escape("<a&b>"), "&lt;a&amp;b&gt;");
    }
}
