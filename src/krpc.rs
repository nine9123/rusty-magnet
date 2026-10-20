//! KRPC message encoding/decoding for the Mainline DHT (BEP 5, BEP 51).

use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};

use crate::bencode::{self, bytes, dict, Value};

pub type NodeId = [u8; 20];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    pub id: NodeId,
    pub addr: SocketAddr,
}

#[derive(Debug)]
pub enum Message {
    Query { tid: Vec<u8>, method: Vec<u8>, args: Value },
    Response { tid: Vec<u8>, body: Value },
    Error { tid: Vec<u8> },
}

pub fn parse(buf: &[u8]) -> Option<Message> {
    let v = bencode::decode(buf).ok()?;
    let tid = v.get_bytes(b"t")?.to_vec();
    match v.get_bytes(b"y")? {
        b"q" => Some(Message::Query { tid, method: v.get_bytes(b"q")?.to_vec(), args: v.get(b"a")?.clone() }),
        b"r" => Some(Message::Response { tid, body: v.get(b"r")?.clone() }),
        b"e" => Some(Message::Error { tid }),
        _ => None,
    }
}

pub fn query(tid: &[u8], method: &[u8], args: Value) -> Vec<u8> {
    dict([(b"t", bytes(tid)), (b"y", bytes("q")), (b"q", bytes(method)), (b"a", args)]).encode()
}

pub fn response(tid: &[u8], body: Value) -> Vec<u8> {
    dict([(b"t", bytes(tid)), (b"y", bytes("r")), (b"r", body)]).encode()
}

pub fn error(tid: &[u8], code: i64, msg: &str) -> Vec<u8> {
    dict([(b"t", bytes(tid)), (b"y", bytes("e")), (b"e", Value::List(vec![Value::Int(code), bytes(msg)]))]).encode()
}

pub fn id_of(v: &Value) -> Option<NodeId> {
    v.get_bytes(b"id")?.try_into().ok()
}

pub fn decode_nodes(b: &[u8]) -> Vec<NodeInfo> {
    b.as_chunks::<26>()
        .0
        .iter()
        .map(|c| NodeInfo {
            id: c[..20].try_into().unwrap(),
            addr: SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::new(c[20], c[21], c[22], c[23]),
                u16::from_be_bytes([c[24], c[25]]),
            )),
        })
        .collect()
}

pub fn encode_nodes<'a>(nodes: impl Iterator<Item = (&'a NodeId, &'a SocketAddr)>) -> Vec<u8> {
    let mut out = Vec::new();
    for (id, addr) in nodes {
        if let SocketAddr::V4(a) = addr {
            out.extend_from_slice(id);
            out.extend_from_slice(&a.ip().octets());
            out.extend_from_slice(&a.port().to_be_bytes());
        }
    }
    out
}

pub fn decode_peers(values: &[Value]) -> Vec<SocketAddr> {
    values
        .iter()
        .filter_map(|v| v.as_bytes())
        .filter(|b| b.len() == 6)
        .map(|b| SocketAddr::new(IpAddr::V4(Ipv4Addr::new(b[0], b[1], b[2], b[3])), u16::from_be_bytes([b[4], b[5]])))
        .collect()
}

pub fn decode_samples(b: &[u8]) -> Vec<NodeId> {
    b.as_chunks::<20>().0.to_vec()
}

pub fn distance(a: &NodeId, b: &NodeId) -> NodeId {
    let mut d = [0u8; 20];
    for i in 0..20 {
        d[i] = a[i] ^ b[i];
    }
    d
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_roundtrip() {
        let q = query(b"aa", b"ping", dict([(b"id", bytes([1u8; 20].to_vec()))]));
        match parse(&q).unwrap() {
            Message::Query { tid, method, args } => {
                assert_eq!(tid, b"aa");
                assert_eq!(method, b"ping");
                assert_eq!(id_of(&args), Some([1u8; 20]));
            }
            m => panic!("{m:?}"),
        }
        assert!(matches!(parse(&error(b"x", 204, "nope")), Some(Message::Error { .. })));
    }

    #[test]
    fn compact_nodes() {
        let id = [9u8; 20];
        let addr: SocketAddr = "1.2.3.4:6881".parse().unwrap();
        let enc = encode_nodes([(&id, &addr)].into_iter());
        assert_eq!(enc.len(), 26);
        assert_eq!(decode_nodes(&enc), vec![NodeInfo { id, addr }]);
        let peers = decode_peers(&[bytes(vec![1, 2, 3, 4, 0x1a, 0xe1]), bytes("short")]);
        assert_eq!(peers, vec![addr]);
    }
}
