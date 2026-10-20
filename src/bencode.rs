//! Minimal bencode decoder/encoder used by KRPC (DHT) and the metadata exchange.

use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Dict(BTreeMap<Vec<u8>, Value>),
}

#[derive(Debug, PartialEq, Eq)]
pub struct DecodeError;

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid bencode")
    }
}

impl std::error::Error for DecodeError {}

const MAX_DEPTH: usize = 64;

impl Value {
    pub fn get(&self, key: &[u8]) -> Option<&Value> {
        match self {
            Value::Dict(d) => d.get(key),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn get_bytes(&self, key: &[u8]) -> Option<&[u8]> {
        self.get(key).and_then(Value::as_bytes)
    }

    pub fn get_int(&self, key: &[u8]) -> Option<i64> {
        self.get(key).and_then(Value::as_int)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        self.encode_into(&mut out);
        out
    }

    pub fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            Value::Int(i) => {
                out.push(b'i');
                out.extend_from_slice(i.to_string().as_bytes());
                out.push(b'e');
            }
            Value::Bytes(b) => {
                out.extend_from_slice(b.len().to_string().as_bytes());
                out.push(b':');
                out.extend_from_slice(b);
            }
            Value::List(l) => {
                out.push(b'l');
                for v in l {
                    v.encode_into(out);
                }
                out.push(b'e');
            }
            Value::Dict(d) => {
                out.push(b'd');
                for (k, v) in d {
                    out.extend_from_slice(k.len().to_string().as_bytes());
                    out.push(b':');
                    out.extend_from_slice(k);
                    v.encode_into(out);
                }
                out.push(b'e');
            }
        }
    }
}

/// Convenience builder for dictionaries.
pub fn dict<const N: usize>(entries: [(&[u8], Value); N]) -> Value {
    Value::Dict(entries.into_iter().map(|(k, v)| (k.to_vec(), v)).collect())
}

pub fn bytes(b: impl Into<Vec<u8>>) -> Value {
    Value::Bytes(b.into())
}

/// Decodes exactly one value occupying the whole buffer.
pub fn decode(buf: &[u8]) -> Result<Value, DecodeError> {
    let (v, used) = decode_prefix(buf)?;
    if used != buf.len() {
        return Err(DecodeError);
    }
    Ok(v)
}

/// Decodes one value from the start of the buffer and returns it with the number
/// of bytes consumed (the ut_metadata data message appends raw bytes after the dict).
pub fn decode_prefix(buf: &[u8]) -> Result<(Value, usize), DecodeError> {
    let mut pos = 0;
    let v = parse(buf, &mut pos, 0)?;
    Ok((v, pos))
}

fn parse(buf: &[u8], pos: &mut usize, depth: usize) -> Result<Value, DecodeError> {
    if depth > MAX_DEPTH {
        return Err(DecodeError);
    }
    match *buf.get(*pos).ok_or(DecodeError)? {
        b'i' => {
            *pos += 1;
            let end = find(buf, *pos, b'e')?;
            let s = std::str::from_utf8(&buf[*pos..end]).map_err(|_| DecodeError)?;
            let i = s.parse::<i64>().map_err(|_| DecodeError)?;
            *pos = end + 1;
            Ok(Value::Int(i))
        }
        b'l' => {
            *pos += 1;
            let mut list = Vec::new();
            loop {
                if *buf.get(*pos).ok_or(DecodeError)? == b'e' {
                    *pos += 1;
                    return Ok(Value::List(list));
                }
                list.push(parse(buf, pos, depth + 1)?);
            }
        }
        b'd' => {
            *pos += 1;
            let mut map = BTreeMap::new();
            loop {
                if *buf.get(*pos).ok_or(DecodeError)? == b'e' {
                    *pos += 1;
                    return Ok(Value::Dict(map));
                }
                let key = parse_bytes(buf, pos)?;
                let val = parse(buf, pos, depth + 1)?;
                map.insert(key, val);
            }
        }
        b'0'..=b'9' => Ok(Value::Bytes(parse_bytes(buf, pos)?)),
        _ => Err(DecodeError),
    }
}

fn parse_bytes(buf: &[u8], pos: &mut usize) -> Result<Vec<u8>, DecodeError> {
    let colon = find(buf, *pos, b':')?;
    let len_str = std::str::from_utf8(&buf[*pos..colon]).map_err(|_| DecodeError)?;
    if len_str.is_empty() || !len_str.bytes().all(|b| b.is_ascii_digit()) {
        return Err(DecodeError);
    }
    let len: usize = len_str.parse().map_err(|_| DecodeError)?;
    let start = colon + 1;
    let end = start.checked_add(len).ok_or(DecodeError)?;
    if end > buf.len() {
        return Err(DecodeError);
    }
    *pos = end;
    Ok(buf[start..end].to_vec())
}

fn find(buf: &[u8], from: usize, byte: u8) -> Result<usize, DecodeError> {
    // Integers and length prefixes are short; bound the scan.
    let limit = buf.len().min(from + 32);
    buf[from.min(limit)..limit].iter().position(|&b| b == byte).map(|p| from + p).ok_or(DecodeError)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let v = dict([
            (b"a", Value::Int(-42)),
            (b"b", bytes("spam")),
            (b"c", Value::List(vec![Value::Int(1), bytes("x")])),
        ]);
        let enc = v.encode();
        assert_eq!(enc, b"d1:ai-42e1:b4:spam1:cli1e1:xee");
        assert_eq!(decode(&enc).unwrap(), v);
    }

    #[test]
    fn prefix_and_trailing() {
        let buf = b"d8:msg_typei1e5:piecei0eeRAWDATA";
        let (v, used) = decode_prefix(buf).unwrap();
        assert_eq!(v.get_int(b"msg_type"), Some(1));
        assert_eq!(&buf[used..], b"RAWDATA");
        assert!(decode(buf).is_err());
    }

    #[test]
    fn rejects_garbage() {
        for bad in [&b"i12"[..], b"5:abc", b"d1:a", b"x", b"l", b"-1:a", b"99999999999999999999:a"] {
            assert!(decode(bad).is_err(), "{:?}", bad);
        }
        // Deep nesting must not blow the stack.
        let deep = vec![b'l'; 10_000];
        assert!(decode(&deep).is_err());
    }
}
