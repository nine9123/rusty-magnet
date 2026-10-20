//! Fetching torrent metadata from a peer (BEP 3 handshake, BEP 10 extension
//! protocol, BEP 9 ut_metadata), verified against the info hash.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::bencode::{self, bytes, dict, Value};
use crate::torrent::InfoHash;

const PSTR: &[u8] = b"BitTorrent protocol";
const PIECE: usize = 16 * 1024;
/// Largest metadata we accept (huge packs with tens of thousands of files).
pub const MAX_METADATA: usize = 8 * 1024 * 1024;
const MAX_MESSAGE: usize = 1024 * 1024;
/// Our ut_metadata extension id, announced in the extended handshake.
const OUR_UT_METADATA: u8 = 1;

#[derive(Debug)]
pub enum FetchError {
    Io(std::io::Error),
    Timeout,
    Protocol(&'static str),
}

impl From<std::io::Error> for FetchError {
    fn from(e: std::io::Error) -> Self {
        FetchError::Io(e)
    }
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FetchError::Io(e) => write!(f, "io: {e}"),
            FetchError::Timeout => f.write_str("timeout"),
            FetchError::Protocol(m) => write!(f, "protocol: {m}"),
        }
    }
}

/// Connects to `addr` and downloads the info dictionary of `info_hash`.
pub async fn fetch(
    addr: SocketAddr,
    info_hash: InfoHash,
    peer_id: [u8; 20],
    timeout: Duration,
) -> Result<Vec<u8>, FetchError> {
    match tokio::time::timeout(timeout, fetch_inner(addr, info_hash, peer_id)).await {
        Ok(r) => r,
        Err(_) => Err(FetchError::Timeout),
    }
}

async fn fetch_inner(addr: SocketAddr, info_hash: InfoHash, peer_id: [u8; 20]) -> Result<Vec<u8>, FetchError> {
    let mut s = match tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(addr)).await {
        Ok(r) => r?,
        Err(_) => return Err(FetchError::Timeout),
    };
    s.set_nodelay(true)?;

    let mut hs = Vec::with_capacity(68);
    hs.push(PSTR.len() as u8);
    hs.extend_from_slice(PSTR);
    let mut reserved = [0u8; 8];
    reserved[5] |= 0x10; // extension protocol
    hs.extend_from_slice(&reserved);
    hs.extend_from_slice(&info_hash);
    hs.extend_from_slice(&peer_id);
    s.write_all(&hs).await?;

    let mut resp = [0u8; 68];
    s.read_exact(&mut resp).await?;
    if resp[0] as usize != PSTR.len() || &resp[1..20] != PSTR {
        return Err(FetchError::Protocol("bad handshake"));
    }
    if resp[25] & 0x10 == 0 {
        return Err(FetchError::Protocol("no extension support"));
    }
    if resp[28..48] != info_hash {
        return Err(FetchError::Protocol("info hash mismatch"));
    }

    let ext_hs = dict([
        (b"m", dict([(b"ut_metadata", Value::Int(OUR_UT_METADATA.into()))])),
        (b"v", bytes(concat!("rustymagnet/", env!("CARGO_PKG_VERSION")))),
    ]);
    send_ext(&mut s, 0, &ext_hs.encode()).await?;

    let mut peer_ut: Option<u8> = None;
    let mut buf: Vec<u8> = Vec::new();
    let mut have: Vec<bool> = Vec::new();
    let mut remaining = 0usize;
    loop {
        let mut len = [0u8; 4];
        s.read_exact(&mut len).await?;
        let len = u32::from_be_bytes(len) as usize;
        if len == 0 {
            continue; // keep-alive
        }
        if len > MAX_MESSAGE {
            return Err(FetchError::Protocol("message too large"));
        }
        let mut msg = vec![0u8; len];
        s.read_exact(&mut msg).await?;
        if msg[0] != 20 || msg.len() < 2 {
            continue; // bitfield, have, unchoke... irrelevant here
        }
        let payload = &msg[2..];
        match msg[1] {
            0 if peer_ut.is_none() => {
                let v = bencode::decode_prefix(payload).map_err(|_| FetchError::Protocol("bad ext handshake"))?.0;
                let id = v
                    .get(b"m")
                    .and_then(|m| m.get_int(b"ut_metadata"))
                    .filter(|i| (1..=255).contains(i))
                    .ok_or(FetchError::Protocol("no ut_metadata"))? as u8;
                let size = v.get_int(b"metadata_size").ok_or(FetchError::Protocol("no metadata_size"))?;
                if size <= 0 || size as usize > MAX_METADATA {
                    return Err(FetchError::Protocol("bad metadata_size"));
                }
                let size = size as usize;
                peer_ut = Some(id);
                buf = vec![0u8; size];
                let pieces = size.div_ceil(PIECE);
                have = vec![false; pieces];
                remaining = pieces;
                for p in 0..pieces {
                    let req = dict([(b"msg_type", Value::Int(0)), (b"piece", Value::Int(p as i64))]);
                    send_ext(&mut s, id, &req.encode()).await?;
                }
            }
            OUR_UT_METADATA if peer_ut.is_some() => {
                let (v, used) =
                    bencode::decode_prefix(payload).map_err(|_| FetchError::Protocol("bad metadata message"))?;
                match v.get_int(b"msg_type") {
                    Some(1) => {}
                    Some(2) => return Err(FetchError::Protocol("metadata request rejected")),
                    _ => continue,
                }
                let piece = v.get_int(b"piece").ok_or(FetchError::Protocol("no piece"))?;
                let piece = usize::try_from(piece).map_err(|_| FetchError::Protocol("bad piece"))?;
                if piece >= have.len() || have[piece] {
                    continue;
                }
                let data = &payload[used..];
                let start = piece * PIECE;
                let expected = PIECE.min(buf.len() - start);
                if data.len() != expected {
                    return Err(FetchError::Protocol("bad piece length"));
                }
                buf[start..start + expected].copy_from_slice(data);
                have[piece] = true;
                remaining -= 1;
                if remaining == 0 {
                    if sha1_smol::Sha1::from(&buf).digest().bytes() != info_hash {
                        return Err(FetchError::Protocol("hash mismatch"));
                    }
                    return Ok(buf);
                }
            }
            _ => {}
        }
    }
}

async fn send_ext(s: &mut TcpStream, ext_id: u8, payload: &[u8]) -> std::io::Result<()> {
    let mut msg = Vec::with_capacity(payload.len() + 6);
    msg.extend_from_slice(&((payload.len() + 2) as u32).to_be_bytes());
    msg.push(20);
    msg.push(ext_id);
    msg.extend_from_slice(payload);
    s.write_all(&msg).await
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// A minimal seeding peer that only serves metadata.
    pub async fn serve_metadata(listener: TcpListener, info: Vec<u8>) {
        let info_hash: InfoHash = sha1_smol::Sha1::from(&info).digest().bytes();
        let (mut s, _) = listener.accept().await.unwrap();
        let mut hs = [0u8; 68];
        s.read_exact(&mut hs).await.unwrap();
        assert_eq!(&hs[28..48], &info_hash);
        let mut reply = hs;
        reply[48..68].copy_from_slice(&[7u8; 20]);
        s.write_all(&reply).await.unwrap();
        // Send some noise first (bitfield), as real peers do.
        s.write_all(&[0, 0, 0, 2, 5, 0xff]).await.unwrap();
        let their_ext =
            dict([(b"m", dict([(b"ut_metadata", Value::Int(3))])), (b"metadata_size", Value::Int(info.len() as i64))]);
        send_ext(&mut s, 0, &their_ext.encode()).await.unwrap();
        let mut our_id = 0;
        loop {
            let mut len = [0u8; 4];
            if s.read_exact(&mut len).await.is_err() {
                return;
            }
            let mut msg = vec![0u8; u32::from_be_bytes(len) as usize];
            s.read_exact(&mut msg).await.unwrap();
            let v = bencode::decode(&msg[2..]).unwrap();
            if msg[1] == 0 {
                our_id = v.get(b"m").unwrap().get_int(b"ut_metadata").unwrap() as u8;
                continue;
            }
            assert_eq!(msg[1], 3);
            let piece = v.get_int(b"piece").unwrap() as usize;
            let chunk = &info[piece * PIECE..((piece + 1) * PIECE).min(info.len())];
            let mut payload = dict([
                (b"msg_type", Value::Int(1)),
                (b"piece", Value::Int(piece as i64)),
                (b"total_size", Value::Int(info.len() as i64)),
            ])
            .encode();
            payload.extend_from_slice(chunk);
            send_ext(&mut s, our_id, &payload).await.unwrap();
        }
    }

    #[tokio::test]
    async fn fetch_from_fake_peer() {
        // Multi-piece info dict.
        let files: Vec<Value> = (0..2000)
            .map(|i| dict([(b"length", Value::Int(i)), (b"path", Value::List(vec![bytes(format!("file-{i}.mkv"))]))]))
            .collect();
        let info =
            dict([(b"name", bytes("Test")), (b"files", Value::List(files)), (b"piece length", Value::Int(16384))])
                .encode();
        assert!(info.len() > PIECE * 2);
        let info_hash: InfoHash = sha1_smol::Sha1::from(&info).digest().bytes();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_metadata(listener, info.clone()));
        let got = fetch(addr, info_hash, [1; 20], Duration::from_secs(5)).await.unwrap();
        assert_eq!(got, info);
        let parsed = crate::torrent::parse_info(&got).unwrap();
        assert_eq!(parsed.files_count, 2000);
    }

    #[tokio::test]
    async fn rejects_wrong_hash() {
        let info = dict([(b"name", bytes("x")), (b"length", Value::Int(1))]).encode();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut wrong = sha1_smol::Sha1::from(&info).digest().bytes();
        tokio::spawn(async move {
            // Server answers with the hash it was asked for, but serves other data.
            let (mut s, _) = listener.accept().await.unwrap();
            let mut hs = [0u8; 68];
            s.read_exact(&mut hs).await.unwrap();
            s.write_all(&hs).await.unwrap();
            let ext = dict([
                (b"m", dict([(b"ut_metadata", Value::Int(2))])),
                (b"metadata_size", Value::Int(info.len() as i64)),
            ]);
            send_ext(&mut s, 0, &ext.encode()).await.unwrap();
            // Answer the (single) piece request without waiting for it.
            let mut payload = dict([(b"msg_type", Value::Int(1)), (b"piece", Value::Int(0))]).encode();
            payload.extend_from_slice(&info);
            send_ext(&mut s, 1, &payload).await.unwrap();
            let mut buf = [0u8; 256];
            while s.read(&mut buf).await.is_ok_and(|n| n > 0) {}
        });
        wrong[0] ^= 1;
        let err = fetch(addr, wrong, [1; 20], Duration::from_secs(5)).await.unwrap_err();
        assert!(matches!(err, FetchError::Protocol("hash mismatch")), "{err}");
    }
}
