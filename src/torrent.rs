//! Parsing of the BitTorrent `info` dictionary and compact file-list encoding.

use crate::bencode::{self, Value};

pub type InfoHash = [u8; 20];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct File {
    pub path: String,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct TorrentInfo {
    pub name: String,
    pub size: u64,
    /// Number of files in the torrent (may exceed `files.len()` if truncated).
    pub files_count: u64,
    pub files: Vec<File>,
    pub private: bool,
}

/// Maximum number of files we keep per torrent; the rest is only counted.
pub const MAX_FILES_STORED: usize = 2000;

pub fn hex(h: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(h.len() * 2);
    for b in h {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 15) as usize] as char);
    }
    s
}

pub fn parse_hex(s: &str) -> Option<InfoHash> {
    let s = s.trim();
    if s.len() != 40 {
        return None;
    }
    let mut out = [0u8; 20];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hi = (chunk[0] as char).to_digit(16)?;
        let lo = (chunk[1] as char).to_digit(16)?;
        out[i] = (hi * 16 + lo) as u8;
    }
    Some(out)
}

fn text(v: &Value, key: &[u8], utf8_key: &[u8]) -> Option<String> {
    v.get_bytes(utf8_key).or_else(|| v.get_bytes(key)).map(|b| String::from_utf8_lossy(b).into_owned())
}

fn clean(s: &str) -> String {
    s.chars().map(|c| if c.is_control() { ' ' } else { c }).collect::<String>().trim().to_string()
}

/// Parses a raw (already hash-verified) info dictionary.
pub fn parse_info(raw: &[u8]) -> Option<TorrentInfo> {
    let info = bencode::decode(raw).ok()?;
    let name = clean(&text(&info, b"name", b"name.utf-8")?);
    if name.is_empty() {
        return None;
    }
    let private = info.get_int(b"private") == Some(1);
    if let Some(len) = info.get_int(b"length") {
        let size = u64::try_from(len).ok()?;
        return Some(TorrentInfo {
            files: vec![File { path: name.clone(), size }],
            name,
            size,
            files_count: 1,
            private,
        });
    }
    let list = info.get(b"files")?.as_list()?;
    let mut files = Vec::with_capacity(list.len().min(MAX_FILES_STORED));
    let mut size = 0u64;
    let mut count = 0u64;
    for f in list {
        let len = u64::try_from(f.get_int(b"length")?).ok()?;
        // BEP 47 padding files are an artefact, not content.
        if f.get_bytes(b"attr").is_some_and(|a| a.contains(&b'p')) {
            continue;
        }
        size = size.saturating_add(len);
        count += 1;
        if files.len() < MAX_FILES_STORED {
            let parts = f.get(b"path.utf-8").or_else(|| f.get(b"path"))?.as_list()?;
            let path = parts
                .iter()
                .filter_map(|p| p.as_bytes())
                .map(|p| clean(&String::from_utf8_lossy(p)))
                .collect::<Vec<_>>()
                .join("/");
            files.push(File { path, size: len });
        }
    }
    Some(TorrentInfo { name, size, files_count: count, files, private })
}

/// Compact on-disk file list: repeated `varint(size) varint(len) path-bytes`.
pub fn encode_files(files: &[File]) -> Vec<u8> {
    let mut out = Vec::new();
    for f in files {
        put_varint(&mut out, f.size);
        put_varint(&mut out, f.path.len() as u64);
        out.extend_from_slice(f.path.as_bytes());
    }
    out
}

pub fn decode_files(mut buf: &[u8]) -> Vec<File> {
    let mut files = Vec::new();
    while !buf.is_empty() {
        let Some(size) = get_varint(&mut buf) else { break };
        let Some(len) = get_varint(&mut buf) else { break };
        let len = len as usize;
        if len > buf.len() {
            break;
        }
        let path = String::from_utf8_lossy(&buf[..len]).into_owned();
        buf = &buf[len..];
        files.push(File { path, size });
    }
    files
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn get_varint(buf: &mut &[u8]) -> Option<u64> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let (&b, rest) = buf.split_first()?;
        *buf = rest;
        v |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Some(v);
        }
    }
    None
}

pub fn magnet(info_hash: &InfoHash, name: &str) -> String {
    format!("magnet:?xt=urn:btih:{}&dn={}", hex(info_hash), url_encode(name))
}

pub fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bencode::{bytes, dict};

    #[test]
    fn hex_roundtrip() {
        let h = [0xabu8; 20];
        assert_eq!(parse_hex(&hex(&h)), Some(h));
        assert_eq!(parse_hex("zz"), None);
    }

    #[test]
    fn parse_multi_file() {
        let info = dict([
            (b"name", bytes("Show S01")),
            (
                b"files",
                Value::List(vec![
                    dict([(b"length", Value::Int(100)), (b"path", Value::List(vec![bytes("a"), bytes("e1.mkv")]))]),
                    dict([
                        (b"length", Value::Int(5)),
                        (b"attr", bytes("p")),
                        (b"path", Value::List(vec![bytes(".pad")])),
                    ]),
                    dict([(b"length", Value::Int(200)), (b"path", Value::List(vec![bytes("e2.mkv")]))]),
                ]),
            ),
            (b"piece length", Value::Int(16384)),
        ]);
        let t = parse_info(&info.encode()).unwrap();
        assert_eq!(t.name, "Show S01");
        assert_eq!(t.size, 300);
        assert_eq!(t.files_count, 2);
        assert_eq!(t.files[0].path, "a/e1.mkv");
        let enc = encode_files(&t.files);
        assert_eq!(decode_files(&enc), t.files);
    }

    #[test]
    fn parse_single_file() {
        let info = dict([(b"name", bytes("x.iso")), (b"length", Value::Int(1 << 40)), (b"private", Value::Int(1))]);
        let t = parse_info(&info.encode()).unwrap();
        assert_eq!(t.size, 1 << 40);
        assert!(t.private);
    }

    #[test]
    fn magnet_link() {
        let m = magnet(&[0; 20], "a b&c");
        assert!(m.ends_with("&dn=a%20b%26c"));
    }
}
