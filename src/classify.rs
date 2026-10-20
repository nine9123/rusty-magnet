//! Rule-based torrent classifier, modelled on bitmagnet's content types and
//! release-name attributes (resolution, source, codec, episodes, languages...).
//!
//! It is purely local: no external metadata lookups (TMDB etc.) are performed.

use crate::torrent::File;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentType {
    Movie = 1,
    TvShow = 2,
    Music = 3,
    Ebook = 4,
    Comic = 5,
    Audiobook = 6,
    Game = 7,
    Software = 8,
    Xxx = 9,
}

pub const ALL_TYPES: [ContentType; 9] = [
    ContentType::Movie,
    ContentType::TvShow,
    ContentType::Music,
    ContentType::Ebook,
    ContentType::Comic,
    ContentType::Audiobook,
    ContentType::Game,
    ContentType::Software,
    ContentType::Xxx,
];

impl ContentType {
    pub fn id(self) -> i64 {
        self as i64
    }

    pub fn from_id(id: i64) -> Option<Self> {
        ALL_TYPES.into_iter().find(|t| t.id() == id)
    }

    pub fn slug(self) -> &'static str {
        match self {
            ContentType::Movie => "movie",
            ContentType::TvShow => "tv_show",
            ContentType::Music => "music",
            ContentType::Ebook => "ebook",
            ContentType::Comic => "comic",
            ContentType::Audiobook => "audiobook",
            ContentType::Game => "game",
            ContentType::Software => "software",
            ContentType::Xxx => "xxx",
        }
    }

    pub fn from_slug(s: &str) -> Option<Self> {
        ALL_TYPES.into_iter().find(|t| t.slug() == s)
    }
}

/// Slug used for torrents the classifier could not place.
pub const UNKNOWN_SLUG: &str = "unknown";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Classification {
    pub content_type: Option<ContentType>,
    pub title: String,
    pub year: Option<u16>,
    pub season: Option<u16>,
    /// Human readable episode spec, e.g. `S01E02`, `S01E01-E03`, `S01-S03`, `S02`.
    pub episodes: Option<String>,
    pub resolution: Option<&'static str>,
    pub source: Option<&'static str>,
    pub video_codec: Option<&'static str>,
    pub modifiers: Vec<&'static str>,
    pub languages: Vec<&'static str>,
    pub release_group: Option<String>,
    /// Matches the child-abuse blocklist: must never be stored or served.
    pub banned: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Video,
    Audio,
    Audiobook,
    Ebook,
    Comic,
    Software,
    Other,
}

fn ext_kind(path: &str) -> Kind {
    let ext = match path.rsplit_once('.') {
        Some((_, e)) if e.len() <= 8 => e.to_ascii_lowercase(),
        _ => return Kind::Other,
    };
    match ext.as_str() {
        "mkv" | "mp4" | "avi" | "m4v" | "mov" | "wmv" | "flv" | "m2ts" | "mts" | "ts" | "webm" | "mpg" | "mpeg"
        | "vob" | "rmvb" | "rm" | "3gp" | "divx" | "ogv" | "f4v" => Kind::Video,
        "mp3" | "flac" | "m4a" | "aac" | "ogg" | "opus" | "wav" | "ape" | "alac" | "wma" | "dsf" | "dff" | "wv"
        | "aiff" => Kind::Audio,
        "m4b" | "aax" => Kind::Audiobook,
        "epub" | "mobi" | "azw" | "azw3" | "pdf" | "djvu" | "fb2" | "lit" | "chm" => Kind::Ebook,
        "cbz" | "cbr" | "cb7" | "cbt" => Kind::Comic,
        "exe" | "msi" | "dmg" | "apk" | "deb" | "rpm" | "pkg" | "appimage" | "iso" | "bin" | "xapk" | "nsp" | "xci" => {
            Kind::Software
        }
        _ => Kind::Other,
    }
}

const XXX_WORDS: &[&str] = &[
    "xxx",
    "porn",
    "porno",
    "brazzers",
    "bangbros",
    "realitykings",
    "naughtyamerica",
    "blacked",
    "tushy",
    "vixen",
    "pornhub",
    "onlyfans",
    "milf",
    "hentai",
    "fakehub",
    "bangbus",
    "nubiles",
    "teamskeet",
    "mofos",
    "evilangel",
    "julesjordan",
    "povd",
    "dorcel",
    "private.com",
    "sexart",
    "metart",
    "x-art",
    "jav",
    "18+",
    "anal",
    "creampie",
    "blowjob",
    "gangbang",
    "deepthroat",
    "cumshot",
    "lesbian",
    "nsfw",
    "pornstar",
];

/// Adult-content markers for names without word boundaries (CJK).
const XXX_SUBSTRINGS: &[&str] = &[
    "无码",
    "有码",
    "自慰",
    "性爱",
    "啪啪",
    "探花",
    "第一会所",
    "巨乳",
    "人妻",
    "中出",
    "痴漢",
    "素人",
    "熟女",
    "约炮",
    "国产av",
    "av女优",
    "無修正",
    "エロ",
];

/// Terms that on their own identify child sexual abuse material.
const BANNED_WORDS: &[&str] = &[
    "pthc",
    "ptsc",
    "pedo",
    "pedofilia",
    "pedophile",
    "pedophilia",
    "paedo",
    "preteen",
    "preteens",
    "hussyfan",
    "kiddy",
    "kiddie",
    "kiddieporn",
    "childporn",
    "jailbait",
    "underage",
    "r@ygold",
    "babyshivid",
];

/// Terms that indicate minors; banned when the torrent is adult content.
const MINOR_WORDS: &[&str] = &[
    "child",
    "children",
    "kid",
    "kids",
    "bata",
    "menor",
    "menores",
    "minor",
    "minors",
    "nina",
    "ninas",
    "niña",
    "niñas",
    "schoolgirl",
    "toddler",
    "infant",
    "lolita",
    "loli",
];

const GAME_WORDS: &[&str] = &[
    "fitgirl",
    "dodi",
    "codex",
    "skidrow",
    "plaza",
    "cpy",
    "elamigos",
    "empress",
    "tenoke",
    "rune",
    "razor1911",
    "flt",
    "gog",
    "reloaded",
    "prophet",
    "darksiders",
    "tinyiso",
    "kaos",
    "xatab",
    "repack",
    "steamrip",
    "rg",
    "nsw",
    "switch",
    "ps4",
    "ps3",
    "ps2",
    "psp",
    "xbox360",
    "xbox",
    "wii",
    "3ds",
    "nds",
    "gba",
    "ppsspp",
];

const AUDIOBOOK_WORDS: &[&str] =
    &["audiobook", "audiobooks", "audible", "unabridged", "abridged", "audiolibro", "hörbuch", "horbuch"];

const BOOK_WORDS: &[&str] = &["epub", "ebook", "ebooks", "mobi", "azw3", "pdf"];

const COMIC_WORDS: &[&str] = &["comic", "comics", "manga", "cbr", "cbz"];

const MUSIC_WORDS: &[&str] = &["flac", "mp3", "discography", "album", "320kbps", "lossless", "24bit", "vinyl", "ost"];

/// (token, ISO 639-1)
const LANGS: &[(&str, &str)] = &[
    ("english", "en"),
    ("eng", "en"),
    ("french", "fr"),
    ("truefrench", "fr"),
    ("vff", "fr"),
    ("vf", "fr"),
    ("vostfr", "fr"),
    ("german", "de"),
    ("ger", "de"),
    ("deutsch", "de"),
    ("spanish", "es"),
    ("spa", "es"),
    ("esp", "es"),
    ("castellano", "es"),
    ("latino", "es"),
    ("italian", "it"),
    ("ita", "it"),
    ("russian", "ru"),
    ("rus", "ru"),
    ("japanese", "ja"),
    ("jpn", "ja"),
    ("korean", "ko"),
    ("kor", "ko"),
    ("chinese", "zh"),
    ("chi", "zh"),
    ("mandarin", "zh"),
    ("cantonese", "zh"),
    ("hindi", "hi"),
    ("tamil", "ta"),
    ("telugu", "te"),
    ("malayalam", "ml"),
    ("dutch", "nl"),
    ("nld", "nl"),
    ("polish", "pl"),
    ("pol", "pl"),
    ("portuguese", "pt"),
    ("por", "pt"),
    ("dublado", "pt"),
    ("swedish", "sv"),
    ("swe", "sv"),
    ("danish", "da"),
    ("norwegian", "no"),
    ("finnish", "fi"),
    ("turkish", "tr"),
    ("tur", "tr"),
    ("arabic", "ar"),
    ("ara", "ar"),
    ("hebrew", "he"),
    ("greek", "el"),
    ("czech", "cs"),
    ("hungarian", "hu"),
    ("hun", "hu"),
    ("ukrainian", "uk"),
    ("ukr", "uk"),
    ("romanian", "ro"),
    ("rom", "ro"),
    ("thai", "th"),
    ("vietnamese", "vi"),
    ("indonesian", "id"),
    ("persian", "fa"),
    ("multi", "multi"),
    ("dual", "multi"),
];

fn resolution(tok: &str) -> Option<&'static str> {
    Some(match tok {
        "360p" => "360p",
        "480p" | "480i" => "480p",
        "540p" => "540p",
        "576p" | "576i" => "576p",
        "720p" | "720i" => "720p",
        "1080p" | "1080i" | "fhd" => "1080p",
        "1440p" | "2k" => "1440p",
        "2160p" | "4k" | "uhd" => "2160p",
        "4320p" | "8k" => "4320p",
        _ => return None,
    })
}

fn source(tok: &str) -> Option<&'static str> {
    Some(match tok {
        "bluray" | "bdrip" | "brrip" | "bdremux" | "bd25" | "bd50" | "bdmv" | "blu" => "BluRay",
        "webdl" | "web" | "amzn" | "nf" | "dsnp" | "hmax" | "atvp" => "WEB-DL",
        "webrip" => "WEBRip",
        "hdtv" | "pdtv" | "sdtv" | "dsr" | "tvrip" | "hdtvrip" | "satrip" => "TV",
        "dvdrip" | "dvd" | "dvd5" | "dvd9" | "dvdr" | "dvdscr" => "DVD",
        "cam" | "camrip" | "hdcam" => "CAM",
        "ts" | "telesync" | "hdts" | "pdvd" => "TELESYNC",
        "tc" | "telecine" | "hdtc" => "TELECINE",
        "workprint" | "wp" => "WORKPRINT",
        _ => return None,
    })
}

fn codec(tok: &str) -> Option<&'static str> {
    Some(match tok {
        "x264" | "h264" | "avc" => "H.264",
        "x265" | "h265" | "hevc" => "H.265",
        "av1" => "AV1",
        "xvid" => "XviD",
        "divx" => "DivX",
        "mpeg2" => "MPEG-2",
        "vp9" => "VP9",
        _ => return None,
    })
}

fn modifier(tok: &str) -> Option<&'static str> {
    Some(match tok {
        "remux" | "bdremux" => "REMUX",
        "3d" | "hsbs" | "sbs" | "hou" => "3D",
        "screener" | "scr" | "dvdscr" => "SCREENER",
        "hdr" | "hdr10" | "dv" | "dovi" => "HDR",
        "rawhd" => "RAWHD",
        "brdisk" | "bdmv" | "bd25" | "bd50" => "BRDISK",
        _ => return None,
    })
}

/// A token with its byte offset in the original name.
struct Token<'a> {
    lower: String,
    raw: &'a str,
    start: usize,
}

fn tokenize(name: &str) -> Vec<Token<'_>> {
    let mut out = Vec::new();
    let mut start = None;
    for (i, c) in name.char_indices() {
        let word = c.is_alphanumeric() || c == '+';
        match (word, start) {
            (true, None) => start = Some(i),
            (false, Some(s)) => {
                out.push(Token { lower: name[s..i].to_lowercase(), raw: &name[s..i], start: s });
                start = None;
            }
            _ => {}
        }
    }
    if let Some(s) = start {
        out.push(Token { lower: name[s..].to_lowercase(), raw: &name[s..], start: s });
    }
    out
}

/// Parses `s01e02e03`, `s01e01-e05` (as split tokens), `s01`, `1x02`.
fn parse_episode_token(tok: &str) -> Option<(u16, Vec<u16>)> {
    let b = tok.as_bytes();
    if b.first() == Some(&b's') && b.len() >= 2 && b[1].is_ascii_digit() {
        let mut i = 1;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i - 1 > 2 {
            return None;
        }
        let season: u16 = tok[1..i].parse().ok()?;
        let mut eps = Vec::new();
        while i < b.len() {
            if b[i] != b'e' {
                return None;
            }
            i += 1;
            let st = i;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            if i == st || i - st > 4 {
                return None;
            }
            eps.push(tok[st..i].parse().ok()?);
        }
        return Some((season, eps));
    }
    // 1x02
    let (a, e) = tok.split_once('x')?;
    if (1..=2).contains(&a.len())
        && (2..=3).contains(&e.len())
        && a.bytes().chain(e.bytes()).all(|c| c.is_ascii_digit())
    {
        return Some((a.parse().ok()?, vec![e.parse().ok()?]));
    }
    None
}

fn is_year(tok: &str) -> Option<u16> {
    if tok.len() != 4 {
        return None;
    }
    let y: u16 = tok.parse().ok()?;
    (1900..=2099).contains(&y).then_some(y)
}

/// Strips a trailing file extension from a single-file torrent name.
fn strip_ext(name: &str) -> &str {
    match name.rsplit_once('.') {
        Some((base, ext))
            if (2..=4).contains(&ext.len())
                && ext.chars().all(|c| c.is_ascii_alphanumeric())
                && ext_kind(name) != Kind::Other =>
        {
            base
        }
        _ => name,
    }
}

fn release_group(name: &str) -> Option<String> {
    let base = strip_ext(name).trim_end_matches([']', ')', ' ']);
    let (_, grp) = base.rsplit_once('-')?;
    let grp = grp.trim();
    if grp.is_empty() || grp.len() > 20 || !grp.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return None;
    }
    if grp.chars().all(|c| c.is_ascii_digit()) || resolution(&grp.to_lowercase()).is_some() {
        return None;
    }
    Some(grp.to_string())
}

/// Parses the release name into attributes, without deciding the content type.
pub fn parse_name(name: &str) -> Classification {
    let toks = tokenize(name);
    let mut c = Classification::default();
    // Offset where the "title" part of the name ends.
    let mut title_end: Option<usize> = None;
    let mark = |end: &mut Option<usize>, at: usize| {
        if end.is_none() || end.is_some_and(|e| at < e) {
            *end = Some(at);
        }
    };
    let mut seasons: Vec<u16> = Vec::new();
    let mut episodes: Vec<u16> = Vec::new();

    for (i, t) in toks.iter().enumerate() {
        let tok = t.lower.as_str();
        if let Some((s, eps)) = parse_episode_token(tok) {
            if i > 0 || toks.len() == 1 {
                mark(&mut title_end, t.start);
            }
            if !seasons.contains(&s) {
                seasons.push(s);
            }
            episodes.extend(eps);
            continue;
        }
        // "S01E01-E03" / "S01E01-03" ranges.
        if !episodes.is_empty() && i > 0 && name[toks[i - 1].start + toks[i - 1].raw.len()..t.start] == *"-" {
            let num = tok.strip_prefix('e').unwrap_or(tok);
            if (1..=3).contains(&num.len()) && num.bytes().all(|b| b.is_ascii_digit()) {
                if let (Some(&last), Ok(n)) = (episodes.last(), num.parse::<u16>()) {
                    if n > last && n - last < 500 {
                        episodes.extend(last + 1..=n);
                        continue;
                    }
                }
            }
        }
        // Spanish "Cap.104" = season 1, episode 4.
        if tok == "cap" || tok == "capitulo" {
            if let Some(n) =
                toks.get(i + 1).filter(|n| (3..=4).contains(&n.lower.len())).and_then(|n| n.lower.parse::<u16>().ok())
            {
                mark(&mut title_end, t.start);
                if !seasons.contains(&(n / 100)) {
                    seasons.push(n / 100);
                }
                episodes.push(n % 100);
                continue;
            }
        }
        if matches!(tok, "season" | "saison" | "temporada" | "staffel" | "stagione") {
            if let Some(n) = toks.get(i + 1).and_then(|n| n.lower.parse::<u16>().ok()).filter(|n| *n < 100) {
                mark(&mut title_end, t.start);
                if !seasons.contains(&n) {
                    seasons.push(n);
                }
                continue;
            }
        }
        if matches!(tok, "complete" | "integrale")
            && toks.get(i + 1).is_some_and(|n| matches!(n.lower.as_str(), "series" | "season" | "seasons"))
        {
            mark(&mut title_end, t.start);
        }
        if let Some(y) = is_year(tok) {
            // A year as the very first token is probably part of the title ("1917", "2012").
            if i > 0 {
                c.year = Some(y);
                mark(&mut title_end, t.start);
                continue;
            }
        }
        let mut hit = false;
        if let Some(r) = resolution(tok) {
            c.resolution.get_or_insert(r);
            hit = true;
        }
        if let Some(s) = source(tok) {
            // "web" alone is weak; "webrip" wins over it, "web" "dl" stays WEB-DL.
            if c.source.is_none() || c.source == Some("WEB-DL") && s == "WEBRip" {
                c.source = Some(s);
            }
            hit = hit || !matches!(tok, "ts" | "tc" | "wp" | "web" | "nf" | "blu" | "dvd");
        }
        if let Some(v) = codec(tok) {
            c.video_codec.get_or_insert(v);
            hit = true;
        }
        if let Some(m) = modifier(tok) {
            if !c.modifiers.contains(&m) {
                c.modifiers.push(m);
            }
            hit = hit || !matches!(tok, "dv" | "sbs" | "hou" | "scr");
        }
        if hit && i > 0 {
            mark(&mut title_end, t.start);
        }
    }

    // Languages only after the title, to avoid catching words of the title itself.
    let lang_from = title_end.unwrap_or(usize::MAX);
    for t in toks.iter().filter(|t| t.start >= lang_from) {
        if let Some((_, code)) = LANGS.iter().find(|(k, _)| *k == t.lower) {
            if !c.languages.contains(code) {
                c.languages.push(code);
            }
        }
    }

    if !seasons.is_empty() {
        seasons.sort_unstable();
        c.season = Some(seasons[0]);
        episodes.sort_unstable();
        episodes.dedup();
        c.episodes = Some(match (seasons.len(), episodes.as_slice()) {
            (1, []) => format!("S{:02}", seasons[0]),
            (1, [e]) => format!("S{:02}E{:02}", seasons[0], e),
            (1, [first, .., last]) => format!("S{:02}E{:02}-E{:02}", seasons[0], first, last),
            _ => format!("S{:02}-S{:02}", seasons[0], seasons[seasons.len() - 1]),
        });
    }

    // Anime-style absolute numbering: "[Group] Title - 1074 [1080p]".
    if c.episodes.is_none() {
        if let Some((at, ep)) = absolute_episode(name) {
            mark(&mut title_end, at);
            c.episodes = Some(format!("E{ep:02}"));
        }
    }

    let base = strip_ext(name);
    let title_raw = &base[..title_end.unwrap_or(base.len()).min(base.len())];
    c.title = tokenize(title_raw).iter().map(|t| t.raw).collect::<Vec<_>>().join(" ");
    if c.title.is_empty() {
        c.title = base.to_string();
    }
    if title_end.is_some() {
        c.release_group = release_group(name);
    }
    c
}

fn absolute_episode(name: &str) -> Option<(usize, u16)> {
    let mut from = 0;
    while let Some(p) = name[from..].find(" - ") {
        let at = from + p;
        let rest = &name[at + 3..];
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        let after = rest[digits..].chars().next();
        if (2..=4).contains(&digits)
            && after.is_none_or(|c| matches!(c, ' ' | '[' | '(' | '.' | 'v'))
            && is_year(&rest[..digits]).is_none()
        {
            return Some((at, rest[..digits].parse().ok()?));
        }
        from = at + 3;
    }
    None
}

/// JAV catalogue codes like `ABP-123`, `SHKD-746`, optionally behind a site prefix.
fn is_jav_code(name: &str) -> bool {
    let mut s = name.trim();
    // Drop "[site]" and "site@" prefixes.
    while let Some(rest) = s.strip_prefix('[').and_then(|r| r.split_once(']')).map(|(_, r)| r.trim_start()) {
        s = rest;
    }
    if let Some((_, rest)) = s.split_once('@') {
        s = rest;
    }
    let b = s.as_bytes();
    let letters = b.iter().take_while(|c| c.is_ascii_alphabetic()).count();
    if !(2..=6).contains(&letters) || b.get(letters) != Some(&b'-') {
        return false;
    }
    let digits = b[letters + 1..].iter().take_while(|c| c.is_ascii_digit()).count();
    (3..=5).contains(&digits) && b.get(letters + 1 + digits).is_none_or(|c| !c.is_ascii_alphanumeric())
}

fn is_banned(toks: &[Token<'_>], lname: &str, xxx: bool) -> bool {
    if has_word(toks, BANNED_WORDS) || lname.contains("r@ygold") {
        return true;
    }
    // Ages under 18 written as "12yo" or "12 yo".
    let minor_age = toks.iter().enumerate().any(|(i, t)| {
        let age = |s: &str| s.parse::<u8>().is_ok_and(|n| (1..18).contains(&n));
        t.lower.strip_suffix("yo").is_some_and(age) || (t.lower == "yo" && i > 0 && age(&toks[i - 1].lower))
    });
    xxx && (minor_age || has_word(toks, MINOR_WORDS))
}

fn has_word(toks: &[Token<'_>], words: &[&str]) -> bool {
    toks.iter().any(|t| words.contains(&t.lower.as_str()))
}

/// Classifies a torrent from its name and (possibly empty) file list.
pub fn classify(name: &str, files: &[File]) -> Classification {
    let mut c = parse_name(name);
    let toks = tokenize(name);
    let lname = name.to_lowercase();

    // Size per kind; single-file torrents are represented by the name itself.
    let mut sizes = [0u64; 7];
    let mut video_files = 0usize;
    let mut episode_like_files = 0usize;
    for f in files {
        let k = ext_kind(&f.path);
        // Saturating: sizes come straight from untrusted metadata.
        sizes[k as usize] = sizes[k as usize].saturating_add(f.size.max(1));
        if k == Kind::Video && f.size > 50 << 20 {
            video_files += 1;
            let base = f.path.rsplit('/').next().unwrap_or(&f.path);
            if tokenize(base).iter().any(|t| parse_episode_token(&t.lower).is_some_and(|(_, e)| !e.is_empty())) {
                episode_like_files += 1;
            }
        }
    }
    if files.is_empty() {
        sizes[ext_kind(name) as usize] = 1;
    }
    let kinds = [Kind::Video, Kind::Audio, Kind::Audiobook, Kind::Ebook, Kind::Comic, Kind::Software];
    let dominant = kinds.into_iter().filter(|k| sizes[*k as usize] > 0).max_by_key(|k| sizes[*k as usize]);

    let first_video =
        files.iter().find(|f| ext_kind(&f.path) == Kind::Video).map(|f| f.path.rsplit('/').next().unwrap_or(&f.path));
    let xxx = has_word(&toks, XXX_WORDS)
        || XXX_WORDS.iter().any(|w| w.contains('.') && lname.contains(w))
        || XXX_SUBSTRINGS.iter().any(|w| lname.contains(w))
        || (matches!(dominant, Some(Kind::Video)) && (is_jav_code(name) || first_video.is_some_and(is_jav_code)));
    c.banned = is_banned(&toks, &lname, xxx);
    let is_video_release =
        c.season.is_some() || c.resolution.is_some() || c.video_codec.is_some() || c.source.is_some();

    c.content_type = match dominant {
        _ if xxx => Some(ContentType::Xxx),
        Some(Kind::Video) => {
            if c.episodes.is_some() || episode_like_files >= 1 && (episode_like_files > 1 || video_files == 1) {
                Some(ContentType::TvShow)
            } else {
                Some(ContentType::Movie)
            }
        }
        Some(Kind::Audiobook) => Some(ContentType::Audiobook),
        Some(Kind::Audio) if has_word(&toks, AUDIOBOOK_WORDS) => Some(ContentType::Audiobook),
        Some(Kind::Audio) => Some(ContentType::Music),
        Some(Kind::Ebook) if has_word(&toks, COMIC_WORDS) => Some(ContentType::Comic),
        Some(Kind::Ebook) => Some(ContentType::Ebook),
        Some(Kind::Comic) => Some(ContentType::Comic),
        Some(Kind::Software) if has_word(&toks, GAME_WORDS) => Some(ContentType::Game),
        Some(Kind::Software) if is_video_release && lname.contains("iso") => Some(ContentType::Movie),
        Some(Kind::Software) => Some(ContentType::Software),
        _ => {
            // Archives or unknown files: fall back to the name only.
            if c.episodes.is_some() {
                Some(ContentType::TvShow)
            } else if is_video_release && c.year.is_some() {
                Some(ContentType::Movie)
            } else if has_word(&toks, GAME_WORDS) {
                Some(ContentType::Game)
            } else if has_word(&toks, AUDIOBOOK_WORDS) {
                Some(ContentType::Audiobook)
            } else if has_word(&toks, COMIC_WORDS) {
                Some(ContentType::Comic)
            } else if has_word(&toks, BOOK_WORDS) {
                Some(ContentType::Ebook)
            } else if has_word(&toks, MUSIC_WORDS) {
                Some(ContentType::Music)
            } else if is_video_release {
                Some(ContentType::Movie)
            } else {
                None
            }
        }
    };

    // Video attributes are noise for non-video content.
    if !matches!(c.content_type, Some(ContentType::Movie | ContentType::TvShow | ContentType::Xxx)) {
        c.resolution = None;
        c.video_codec = None;
        c.source = None;
        c.modifiers.clear();
    }
    if !matches!(c.content_type, Some(ContentType::TvShow | ContentType::Xxx)) {
        c.season = None;
        c.episodes = None;
    }
    c
}

/// Torznab / Newznab category for a classified torrent.
pub fn torznab_category(c: Option<ContentType>, resolution: Option<&str>, name: &str) -> u32 {
    // Children of a parent category are parent + 30 (SD), 40 (HD), 45 (UHD).
    let hd = |parent: u32| match resolution {
        Some("2160p" | "4320p") => parent + 45,
        Some("720p" | "1080p" | "1440p") => parent + 40,
        Some(_) => parent + 30,
        None => parent,
    };
    match c {
        Some(ContentType::Movie) => hd(2000),
        Some(ContentType::TvShow) => hd(5000),
        Some(ContentType::Music) if name.to_lowercase().contains("flac") => 3040,
        Some(ContentType::Music) => 3010,
        Some(ContentType::Audiobook) => 3030,
        Some(ContentType::Ebook) => 7020,
        Some(ContentType::Comic) => 7030,
        Some(ContentType::Game) => 4050,
        Some(ContentType::Software) => 4000,
        Some(ContentType::Xxx) => 6000,
        None => 8000,
    }
}

/// Content types matching a Torznab category (parent categories match all children).
pub fn types_for_torznab_category(cat: u32) -> Vec<ContentType> {
    match cat {
        2000..=2999 => vec![ContentType::Movie],
        5000..=5999 => vec![ContentType::TvShow],
        3030 => vec![ContentType::Audiobook],
        3000 => vec![ContentType::Music, ContentType::Audiobook],
        3001..=3999 => vec![ContentType::Music],
        7020 => vec![ContentType::Ebook],
        7030 => vec![ContentType::Comic],
        7000..=7999 => vec![ContentType::Ebook, ContentType::Comic],
        4050 => vec![ContentType::Game],
        4000 => vec![ContentType::Software, ContentType::Game],
        4001..=4999 => vec![ContentType::Software],
        6000..=6999 => vec![ContentType::Xxx],
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(path: &str, mb: u64) -> File {
        File { path: path.into(), size: mb << 20 }
    }

    #[test]
    fn movie() {
        let c = classify(
            "The.Matrix.1999.1080p.BluRay.x264-SPARKS",
            &[f("The.Matrix.1999.1080p.BluRay.x264-SPARKS.mkv", 8000)],
        );
        assert_eq!(c.content_type, Some(ContentType::Movie));
        assert_eq!(c.title, "The Matrix");
        assert_eq!(c.year, Some(1999));
        assert_eq!(c.resolution, Some("1080p"));
        assert_eq!(c.source, Some("BluRay"));
        assert_eq!(c.video_codec, Some("H.264"));
        assert_eq!(c.release_group.as_deref(), Some("SPARKS"));
    }

    #[test]
    fn huge_file_sizes_do_not_overflow() {
        let big = File { path: "a.mkv".into(), size: i64::MAX as u64 };
        let c = classify("Huge.2020.1080p", &[big.clone(), big.clone(), big]);
        assert_eq!(c.content_type, Some(ContentType::Movie));
    }

    #[test]
    fn movie_with_numeric_title_and_languages() {
        let c = classify("1917 (2019) [2160p] [4K] MULTI FRENCH HEVC REMUX", &[f("a.mkv", 30000), f("a.srt", 1)]);
        assert_eq!(c.content_type, Some(ContentType::Movie));
        assert_eq!(c.title, "1917");
        assert_eq!(c.year, Some(2019));
        assert_eq!(c.resolution, Some("2160p"));
        assert_eq!(c.languages, vec!["multi", "fr"]);
        assert!(c.modifiers.contains(&"REMUX"));
    }

    #[test]
    fn tv_episode_and_season() {
        let c = classify("Severance.S02E03.720p.WEB.h264-ETHEL", &[f("x.mkv", 900)]);
        assert_eq!(c.content_type, Some(ContentType::TvShow));
        assert_eq!(c.title, "Severance");
        assert_eq!(c.episodes.as_deref(), Some("S02E03"));
        assert_eq!(c.season, Some(2));

        let c = classify("Breaking Bad Season 3 Complete 1080p", &[f("e1.mkv", 900), f("e2.mkv", 900)]);
        assert_eq!(c.content_type, Some(ContentType::TvShow));
        assert_eq!(c.title, "Breaking Bad");
        assert_eq!(c.episodes.as_deref(), Some("S03"));

        let c = classify("Show.S01E01-E03.1080p", &[f("x.mkv", 900)]);
        assert_eq!(c.episodes.as_deref(), Some("S01E01-E03"));

        let c = classify("Friends.S01-S10", &[]);
        assert_eq!(c.content_type, Some(ContentType::TvShow));
        assert_eq!(c.episodes.as_deref(), Some("S01-S10"));
    }

    #[test]
    fn tv_from_files() {
        let files = [f("Show/Show.S01E01.mkv", 500), f("Show/Show.S01E02.mkv", 500)];
        assert_eq!(classify("Some Show", &files).content_type, Some(ContentType::TvShow));
    }

    #[test]
    fn music_audiobook_books() {
        let c = classify("Pink Floyd - The Wall (1979) [FLAC]", &[f("01.flac", 40), f("cover.jpg", 1)]);
        assert_eq!(c.content_type, Some(ContentType::Music));
        assert_eq!(c.resolution, None);
        assert_eq!(classify("Dune Unabridged", &[f("1.mp3", 40)]).content_type, Some(ContentType::Audiobook));
        assert_eq!(classify("Something", &[f("1.m4b", 400)]).content_type, Some(ContentType::Audiobook));
        assert_eq!(classify("Rust Book", &[f("rust.epub", 4)]).content_type, Some(ContentType::Ebook));
        assert_eq!(classify("Batman 001", &[f("b.cbz", 40)]).content_type, Some(ContentType::Comic));
    }

    #[test]
    fn software_and_games() {
        assert_eq!(classify("ubuntu-24.04-desktop-amd64.iso", &[]).content_type, Some(ContentType::Software));
        assert_eq!(
            classify("Elden Ring [FitGirl Repack]", &[f("setup.exe", 5), f("data.bin", 40000)]).content_type,
            Some(ContentType::Game)
        );
    }

    #[test]
    fn xxx_and_unknown() {
        assert_eq!(classify("Brazzers - Something 1080p", &[f("a.mp4", 1000)]).content_type, Some(ContentType::Xxx));
        assert_eq!(classify("random stuff", &[f("a.zip", 10)]).content_type, None);
    }

    #[test]
    fn anime_jav_and_spanish_episodes() {
        let c = classify("[Erai-raws] One Piece - 1074 [1080p][Multiple Subtitle].mkv", &[f("x.mkv", 1400)]);
        assert_eq!(c.content_type, Some(ContentType::TvShow));
        assert_eq!(c.episodes.as_deref(), Some("E1074"));
        assert_eq!(c.title, "Erai raws One Piece");
        let c = classify("Crystal Lake [HDTV 1080p][Cap.104]", &[f("x.mkv", 1400)]);
        assert_eq!(c.episodes.as_deref(), Some("S01E04"));
        assert_eq!(classify("[Thz.la]shkd-746", &[f("shkd-746.mp4", 4000)]).content_type, Some(ContentType::Xxx));
        assert_eq!(classify("START-651", &[f("START-651.mp4", 4000)]).content_type, Some(ContentType::Xxx));
        assert_eq!(classify("Blade Runner 2049 - 2017", &[f("a.mkv", 9000)]).content_type, Some(ContentType::Movie));
        assert_eq!(classify("X-Men 2000", &[f("a.mkv", 9000)]).content_type, Some(ContentType::Movie));
    }

    #[test]
    fn banned() {
        assert!(classify("something pthc", &[]).banned);
        assert!(classify("porn 12yo clip", &[f("a.mp4", 100)]).banned);
        assert!(classify("child porno clip", &[f("a.mp4", 100)]).banned);
        assert!(!classify("Children of Men 2006 1080p", &[f("a.mkv", 9000)]).banned);
        assert!(!classify("Brazzers Teen 1080p", &[f("a.mp4", 1000)]).banned);
    }

    #[test]
    fn torznab_cats() {
        assert_eq!(torznab_category(Some(ContentType::Movie), Some("1080p"), ""), 2040);
        assert_eq!(torznab_category(Some(ContentType::TvShow), None, ""), 5000);
        assert_eq!(types_for_torznab_category(5040), vec![ContentType::TvShow]);
    }
}
