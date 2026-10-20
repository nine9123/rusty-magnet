//! Small helpers: a fast non-cryptographic PRNG and address filtering.

use std::cell::Cell;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

thread_local! {
    static RNG: Cell<u64> = Cell::new(seed());
}

fn seed() -> u64 {
    let t =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(1);
    let local = 0u8;
    let addr = &local as *const u8 as u64;
    let s = t ^ (u64::from(std::process::id()) << 32) ^ addr.rotate_left(17);
    if s == 0 {
        0x9E37_79B9_7F4A_7C15
    } else {
        s
    }
}

/// xorshift64* — plenty for transaction ids and random DHT targets.
pub fn rand_u64() -> u64 {
    RNG.with(|r| {
        let mut x = r.get();
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        r.set(x);
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    })
}

pub fn rand_bytes<const N: usize>() -> [u8; N] {
    let mut out = [0u8; N];
    for chunk in out.chunks_mut(8) {
        let r = rand_u64().to_le_bytes();
        chunk.copy_from_slice(&r[..chunk.len()]);
    }
    out
}

/// True for addresses worth talking to on the public internet.
pub fn routable(addr: &SocketAddr) -> bool {
    if addr.port() == 0 {
        return false;
    }
    match addr.ip() {
        IpAddr::V4(ip) => routable_v4(ip),
        IpAddr::V6(_) => false,
    }
}

fn routable_v4(ip: Ipv4Addr) -> bool {
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_multicast()
        || ip.is_documentation()
        || ip.octets()[0] == 0
        || ip.octets()[0] >= 240
        // 100.64.0.0/10 carrier-grade NAT
        || (ip.octets()[0] == 100 && (ip.octets()[1] & 0xc0) == 64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rng_varies() {
        let a = rand_u64();
        let b = rand_u64();
        assert_ne!(a, b);
        let x: [u8; 20] = rand_bytes();
        assert_ne!(x, [0u8; 20]);
    }

    #[test]
    fn routable_filter() {
        assert!(routable(&"8.8.8.8:6881".parse().unwrap()));
        assert!(!routable(&"192.168.1.1:6881".parse().unwrap()));
        assert!(!routable(&"8.8.8.8:0".parse().unwrap()));
        assert!(!routable(&"100.64.1.1:1".parse().unwrap()));
    }
}
