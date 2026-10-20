//! The DHT crawler.
//!
//! One task owns all crawler state (no locks): it discovers nodes, asks them
//! for info hash samples (BEP 51), answers incoming DHT queries (collecting
//! `announce_peer`/`get_peers` info hashes), looks up peers for unknown hashes
//! with `get_peers`, and hands them to a bounded pool of metadata fetchers.
//! Outgoing traffic is paced by a packets-per-second budget.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Semaphore};

use crate::bencode::{bytes, dict, Value};
use crate::bloom::Bloom;
use crate::classify::ContentType;
use crate::krpc::{self, Message, NodeId};
use crate::metadata;
use crate::store::{NewTorrent, WriteMsg};
use crate::torrent::{self, InfoHash};
use crate::util::{rand_bytes, rand_u64, routable};
use crate::{log_debug, log_info, log_warn};

#[derive(Debug, Clone)]
pub struct CrawlerConfig {
    pub port: u16,
    /// Outgoing DHT query budget (packets per second).
    pub rate: u32,
    /// Concurrent metadata downloads.
    pub fetch_concurrency: usize,
    /// Maximum number of DHT nodes remembered.
    pub max_nodes: usize,
    /// Maximum concurrent get_peers lookups.
    pub max_lookups: usize,
    /// Do not store adult content.
    pub skip_xxx: bool,
    pub bootstrap: Vec<String>,
}

impl Default for CrawlerConfig {
    fn default() -> Self {
        CrawlerConfig {
            port: 3334,
            rate: 150,
            fetch_concurrency: 64,
            max_nodes: 50_000,
            max_lookups: 1_000,
            skip_xxx: false,
            bootstrap: vec![
                "router.bittorrent.com:6881".into(),
                "dht.transmissionbt.com:6881".into(),
                "router.utorrent.com:6881".into(),
                "dht.libtorrent.org:25401".into(),
                "dht.aelitis.com:6881".into(),
            ],
        }
    }
}

#[derive(Default)]
pub struct CrawlerStats {
    pub packets_in: AtomicU64,
    pub packets_out: AtomicU64,
    pub nodes: AtomicU64,
    pub hashes_seen: AtomicU64,
    pub hashes_new: AtomicU64,
    pub lookups: AtomicU64,
    pub fetching: AtomicU64,
    pub fetched: AtomicU64,
    pub fetch_failed: AtomicU64,
}

const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(12);
const RETRY_FAILED_AFTER: Duration = Duration::from_secs(3600);
const MAX_PEERS_PER_HASH: usize = 12;
const MAX_QUERIES_PER_LOOKUP: usize = 16;
const MAX_PENDING: usize = 100_000;

struct Node {
    id: NodeId,
    last_seen: Instant,
    failures: u8,
    /// Has answered at least one of our queries.
    responded: bool,
    /// None = unknown, Some(false) = doesn't answer sample_infohashes.
    bep51: Option<bool>,
}

#[derive(Clone, Copy)]
enum Kind {
    Sample,
    FindNode,
    GetPeers(InfoHash),
}

struct Pending {
    kind: Kind,
    addr: SocketAddr,
    sent: Instant,
}

struct Lookup {
    started: Instant,
    peers: Vec<SocketAddr>,
    queried: HashSet<SocketAddr>,
    /// Unqueried candidates, sorted by distance to the hash (closest last).
    candidates: Vec<(NodeId, SocketAddr)>,
    in_flight: usize,
}

struct FetchDone {
    hash: InfoHash,
    /// Metadata was retrieved (the torrent may still be filtered out).
    ok: bool,
    torrent: Option<Box<NewTorrent>>,
}

pub struct Crawler {
    cfg: CrawlerConfig,
    id: NodeId,
    sock: Arc<UdpSocket>,
    token_secret: [u8; 16],
    nodes: HashMap<SocketAddr, Node>,
    by_id: BTreeMap<NodeId, SocketAddr>,
    /// Never-queried nodes, sampled first.
    fresh: VecDeque<SocketAddr>,
    /// Nodes to sample again once their advertised interval has passed.
    revisit: BinaryHeap<Reverse<(Instant, SocketAddr)>>,
    /// Nodes known to answer, cycled through with `find_node` when idle.
    good: VecDeque<SocketAddr>,
    /// Addresses that never answered; not worth querying again soon.
    dead: HashSet<SocketAddr>,
    pending: HashMap<u32, Pending>,
    next_tid: u32,
    /// get_peers queries waiting for send budget (higher priority than sampling).
    lookup_queue: VecDeque<(InfoHash, SocketAddr)>,
    lookups: HashMap<InfoHash, Lookup>,
    /// Hashes currently looked up or being fetched.
    in_flight: HashSet<InfoHash>,
    failed: HashMap<InfoHash, Instant>,
    bloom: Arc<Bloom>,
    writer: SyncSender<WriteMsg>,
    stats: Arc<CrawlerStats>,
    fetch_tx: mpsc::Sender<(InfoHash, Vec<SocketAddr>)>,
    done_rx: mpsc::Receiver<FetchDone>,
    budget: f64,
    last_bootstrap: Option<Instant>,
}

impl Crawler {
    pub async fn bind(
        cfg: CrawlerConfig,
        bloom: Arc<Bloom>,
        writer: SyncSender<WriteMsg>,
        stats: Arc<CrawlerStats>,
    ) -> std::io::Result<Crawler> {
        let sock = Arc::new(UdpSocket::bind(("0.0.0.0", cfg.port)).await?);
        let (fetch_tx, fetch_rx) = mpsc::channel(cfg.fetch_concurrency * 4);
        let (done_tx, done_rx) = mpsc::channel(1024);
        tokio::spawn(fetch_dispatcher(fetch_rx, done_tx, cfg.fetch_concurrency, cfg.skip_xxx, stats.clone()));
        Ok(Crawler {
            id: rand_bytes(),
            sock,
            token_secret: rand_bytes(),
            nodes: HashMap::new(),
            by_id: BTreeMap::new(),
            fresh: VecDeque::new(),
            revisit: BinaryHeap::new(),
            good: VecDeque::new(),
            dead: HashSet::new(),
            pending: HashMap::new(),
            next_tid: rand_u64() as u32,
            lookup_queue: VecDeque::new(),
            lookups: HashMap::new(),
            in_flight: HashSet::new(),
            failed: HashMap::new(),
            bloom,
            writer,
            stats,
            fetch_tx,
            done_rx,
            budget: 0.0,
            last_bootstrap: None,
            cfg,
        })
    }

    pub async fn run(mut self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let sock = self.sock.clone();
        let mut buf = vec![0u8; 4096];
        let mut tick = tokio::time::interval(Duration::from_millis(20));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut maint = tokio::time::interval(Duration::from_secs(5));
        let mut report =
            tokio::time::interval(Duration::from_secs(if crate::log::enabled(crate::log::DEBUG) { 10 } else { 60 }));
        let mut last_tick = Instant::now();
        log_info!("dht: listening on udp/{}", self.cfg.port);
        self.bootstrap().await;
        loop {
            tokio::select! {
                r = sock.recv_from(&mut buf) => match r {
                    Ok((n, from)) => {
                        self.stats.packets_in.fetch_add(1, Relaxed);
                        self.on_packet(&buf[..n], from);
                    }
                    Err(e) => log_debug!("dht: recv error: {e}"),
                },
                _ = tick.tick() => {
                    let now = Instant::now();
                    let dt = now.duration_since(last_tick).as_secs_f64();
                    last_tick = now;
                    self.budget = (self.budget + dt * f64::from(self.cfg.rate)).min(f64::from(self.cfg.rate) / 5.0);
                    self.send_queued();
                }
                Some(done) = self.done_rx.recv() => self.on_fetch_done(done),
                _ = maint.tick() => self.maintenance().await,
                _ = report.tick() => self.report(),
                _ = shutdown.changed() => break,
            }
        }
        log_info!("dht: stopped");
    }

    fn report(&self) {
        let s = &self.stats;
        log_info!(
            "dht: nodes={} (queued {}, revisit {}, pending {}) lookups={} fetching={} | pkts in={} out={} | hashes seen={} new={} | fetched={} failed={}",
            self.nodes.len(),
            self.fresh.len(),
            self.revisit.len(),
            self.pending.len(),
            self.lookups.len(),
            s.fetching.load(Relaxed),
            s.packets_in.load(Relaxed),
            s.packets_out.load(Relaxed),
            s.hashes_seen.load(Relaxed),
            s.hashes_new.load(Relaxed),
            s.fetched.load(Relaxed),
            s.fetch_failed.load(Relaxed),
        );
    }

    async fn bootstrap(&mut self) {
        self.last_bootstrap = Some(Instant::now());
        for host in self.cfg.bootstrap.clone() {
            match tokio::net::lookup_host(host.as_str()).await {
                Ok(addrs) => {
                    for addr in addrs.filter(SocketAddr::is_ipv4) {
                        let target: NodeId = rand_bytes();
                        self.send_query(
                            addr,
                            Kind::FindNode,
                            b"find_node",
                            dict([(b"id", bytes(self.id.to_vec())), (b"target", bytes(target.to_vec()))]),
                        );
                    }
                }
                Err(e) => log_warn!("dht: cannot resolve bootstrap node {host}: {e}"),
            }
        }
    }

    // ------------------------------------------------------------ sending

    fn send_raw(&self, addr: SocketAddr, msg: &[u8]) {
        if self.sock.try_send_to(msg, addr).is_ok() {
            self.stats.packets_out.fetch_add(1, Relaxed);
        }
    }

    fn send_query(&mut self, addr: SocketAddr, kind: Kind, method: &[u8], args: Value) {
        if self.pending.len() >= MAX_PENDING {
            return;
        }
        self.next_tid = self.next_tid.wrapping_add(1);
        let tid = self.next_tid;
        let msg = krpc::query(&tid.to_be_bytes(), method, args);
        self.send_raw(addr, &msg);
        self.pending.insert(tid, Pending { kind, addr, sent: Instant::now() });
    }

    fn send_get_peers(&mut self, hash: InfoHash, addr: SocketAddr) {
        let args = dict([(b"id", bytes(self.id.to_vec())), (b"info_hash", bytes(hash.to_vec()))]);
        self.send_query(addr, Kind::GetPeers(hash), b"get_peers", args);
    }

    /// Spends the packet budget: lookups first, then sampling new nodes,
    /// then revisiting nodes whose sample interval has passed.
    fn send_queued(&mut self) {
        while self.budget >= 1.0 {
            if let Some((hash, addr)) = self.lookup_queue.pop_front() {
                if self.lookups.contains_key(&hash) {
                    self.send_get_peers(hash, addr);
                    self.budget -= 1.0;
                }
                continue;
            }
            let (addr, explore) = match self.fresh.pop_front() {
                Some(a) => (a, false),
                None => match self.revisit.peek() {
                    Some(Reverse((due, _))) if *due <= Instant::now() => (self.revisit.pop().unwrap().0 .1, false),
                    // Nothing due: use the spare budget to discover new nodes.
                    _ => match self.good.pop_front() {
                        Some(a) if self.nodes.contains_key(&a) => {
                            self.good.push_back(a);
                            (a, true)
                        }
                        Some(_) => continue,
                        None => break,
                    },
                },
            };
            let Some(node) = self.nodes.get(&addr) else { continue };
            let target: NodeId = rand_bytes();
            let args = dict([(b"id", bytes(self.id.to_vec())), (b"target", bytes(target.to_vec()))]);
            if explore || node.bep51 == Some(false) {
                self.send_query(addr, Kind::FindNode, b"find_node", args);
            } else {
                self.send_query(addr, Kind::Sample, b"sample_infohashes", args);
            }
            self.budget -= 1.0;
        }
    }

    // ------------------------------------------------------------ receiving

    fn on_packet(&mut self, buf: &[u8], from: SocketAddr) {
        match krpc::parse(buf) {
            Some(Message::Query { tid, method, args }) => self.on_query(&tid, &method, &args, from),
            Some(Message::Response { tid, body }) => self.on_response(&tid, &body, from),
            Some(Message::Error { tid }) => {
                if let Some(p) = take_pending(&mut self.pending, &tid, from) {
                    self.mark_responded(from);
                    if let (Kind::Sample, Some(n)) = (p.kind, self.nodes.get_mut(&from)) {
                        n.bep51 = Some(false);
                        // No BEP 51 support: ask for its neighbours instead, right away.
                        self.fresh.push_front(from);
                    }
                }
            }
            None => {}
        }
    }

    fn token(&self, addr: &SocketAddr) -> [u8; 8] {
        let mut h = sha1_smol::Sha1::new();
        h.update(&self.token_secret);
        if let SocketAddr::V4(a) = addr {
            h.update(&a.ip().octets());
        }
        h.digest().bytes()[..8].try_into().unwrap()
    }

    /// Our id as seen by `other`: close to theirs, so we receive more traffic.
    fn neighbor_id(&self, other: &NodeId) -> Vec<u8> {
        let mut id = self.id;
        id[..15].copy_from_slice(&other[..15]);
        id.to_vec()
    }

    fn closest_nodes(&self, target: &NodeId, n: usize) -> Vec<(NodeId, SocketAddr)> {
        // Nodes sharing a long prefix with the target are adjacent in id order.
        let mut out: Vec<(NodeId, SocketAddr)> = self
            .by_id
            .range(..*target)
            .rev()
            .take(n)
            .chain(self.by_id.range(*target..).take(n))
            .map(|(id, a)| (*id, *a))
            .collect();
        out.sort_by_key(|(id, _)| krpc::distance(id, target));
        out.truncate(n);
        out
    }

    fn on_query(&mut self, tid: &[u8], method: &[u8], args: &Value, from: SocketAddr) {
        let Some(qid) = krpc::id_of(args) else { return };
        if !routable(&from) {
            return;
        }
        self.add_node(qid, from);
        let id = self.neighbor_id(&qid);
        let reply = match method {
            b"ping" => krpc::response(tid, dict([(b"id", bytes(id))])),
            b"find_node" | b"sample_infohashes" => {
                let target: NodeId = args.get_bytes(b"target").and_then(|t| t.try_into().ok()).unwrap_or(qid);
                let nodes = self.closest_nodes(&target, 8);
                let nodes = krpc::encode_nodes(nodes.iter().map(|(i, a)| (i, a)));
                if method == b"find_node" {
                    krpc::response(tid, dict([(b"id", bytes(id)), (b"nodes", bytes(nodes))]))
                } else {
                    krpc::response(
                        tid,
                        dict([
                            (b"id", bytes(id)),
                            (b"nodes", bytes(nodes)),
                            (b"interval", Value::Int(21600)),
                            (b"num", Value::Int(0)),
                            (b"samples", bytes(Vec::new())),
                        ]),
                    )
                }
            }
            b"get_peers" => {
                let Some(hash) = args.get_bytes(b"info_hash").and_then(|h| <InfoHash>::try_from(h).ok()) else {
                    return self.send_raw(from, &krpc::error(tid, 203, "missing info_hash"));
                };
                let nodes = self.closest_nodes(&hash, 8);
                let nodes = krpc::encode_nodes(nodes.iter().map(|(i, a)| (i, a)));
                let token = self.token(&from).to_vec();
                // Someone is searching for this torrent: worth a look, at lower priority.
                if self.lookups.len() < self.cfg.max_lookups / 2 {
                    self.discovered(hash, None);
                }
                krpc::response(tid, dict([(b"id", bytes(id)), (b"nodes", bytes(nodes)), (b"token", bytes(token))]))
            }
            b"announce_peer" => {
                let hash = args.get_bytes(b"info_hash").and_then(|h| <InfoHash>::try_from(h).ok());
                let token_ok = args.get_bytes(b"token") == Some(&self.token(&from)[..]);
                if let (Some(hash), true) = (hash, token_ok) {
                    let port = if args.get_int(b"implied_port") == Some(1) {
                        Some(from.port())
                    } else {
                        args.get_int(b"port").and_then(|p| u16::try_from(p).ok())
                    };
                    let peer = port.map(|p| SocketAddr::new(from.ip(), p)).filter(routable);
                    self.discovered(hash, peer.map(|p| (None, p)));
                }
                krpc::response(tid, dict([(b"id", bytes(id))]))
            }
            _ => krpc::error(tid, 204, "method unknown"),
        };
        self.send_raw(from, &reply);
    }

    fn on_response(&mut self, tid: &[u8], body: &Value, from: SocketAddr) {
        let Some(p) = take_pending(&mut self.pending, tid, from) else { return };
        let Some(rid) = krpc::id_of(body) else { return };
        self.add_node(rid, from);
        let now = Instant::now();
        self.mark_responded(from);
        let nodes = body.get_bytes(b"nodes").map(krpc::decode_nodes).unwrap_or_default();
        match p.kind {
            Kind::FindNode => {
                for n in &nodes {
                    self.add_node(n.id, n.addr);
                }
                if self.nodes.get(&from).is_some_and(|n| n.bep51 == Some(false)) {
                    self.revisit.push(Reverse((now + Duration::from_secs(900), from)));
                }
            }
            Kind::Sample => {
                for n in &nodes {
                    self.add_node(n.id, n.addr);
                }
                match body.get_bytes(b"samples") {
                    Some(samples) => {
                        let interval = body.get_int(b"interval").unwrap_or(300).clamp(60, 6 * 3600) as u64;
                        if let Some(n) = self.nodes.get_mut(&from) {
                            n.bep51 = Some(true);
                        }
                        self.revisit.push(Reverse((now + Duration::from_secs(interval), from)));
                        for h in krpc::decode_samples(samples) {
                            // The sampled node stores peers for this hash: ask it first.
                            self.discovered(h, Some((Some(rid), from)));
                        }
                    }
                    None => {
                        if let Some(n) = self.nodes.get_mut(&from) {
                            n.bep51 = Some(false);
                        }
                        self.revisit.push(Reverse((now + Duration::from_secs(900), from)));
                    }
                }
            }
            Kind::GetPeers(hash) => {
                let peers = body.get(b"values").and_then(Value::as_list).map(krpc::decode_peers).unwrap_or_default();
                for n in &nodes {
                    self.add_node(n.id, n.addr);
                }
                let Some(l) = self.lookups.get_mut(&hash) else { return };
                l.in_flight = l.in_flight.saturating_sub(1);
                for peer in peers.into_iter().filter(routable) {
                    if l.peers.len() < MAX_PEERS_PER_HASH && !l.peers.contains(&peer) {
                        l.peers.push(peer);
                    }
                }
                for n in nodes.into_iter().filter(|n| routable(&n.addr)) {
                    if !l.queried.contains(&n.addr) && !l.candidates.iter().any(|c| c.1 == n.addr) {
                        l.candidates.push((n.id, n.addr));
                    }
                }
                l.candidates.sort_by_key(|(id, _)| Reverse(krpc::distance(id, &hash)));
                l.candidates.truncate(32);
                self.advance_lookup(hash);
            }
        }
    }

    fn mark_responded(&mut self, addr: SocketAddr) {
        if let Some(n) = self.nodes.get_mut(&addr) {
            n.last_seen = Instant::now();
            n.failures = 0;
            if !n.responded {
                n.responded = true;
                if self.good.len() < 10_000 {
                    self.good.push_back(addr);
                }
            }
        }
    }

    fn add_node(&mut self, id: NodeId, addr: SocketAddr) {
        if id == self.id
            || !routable(&addr)
            || self.nodes.contains_key(&addr)
            || self.nodes.len() >= self.cfg.max_nodes
            || self.dead.contains(&addr)
        {
            return;
        }
        if let Some(old) = self.by_id.insert(id, addr) {
            if old != addr {
                // Same id from a different address: keep the newest only.
                self.nodes.remove(&old);
            }
        }
        self.nodes.insert(addr, Node { id, last_seen: Instant::now(), failures: 0, responded: false, bep51: None });
        if self.fresh.len() < self.cfg.max_nodes {
            self.fresh.push_back(addr);
        }
        self.stats.nodes.store(self.nodes.len() as u64, Relaxed);
    }

    // ------------------------------------------------------------ lookups

    /// A hash was seen; `source` is a node to ask (`Some(id)`) or a peer (`None` id).
    fn discovered(&mut self, hash: InfoHash, source: Option<(Option<NodeId>, SocketAddr)>) {
        self.stats.hashes_seen.fetch_add(1, Relaxed);
        if self.bloom.contains(&hash) || self.in_flight.contains(&hash) {
            return;
        }
        if let Some(t) = self.failed.get(&hash) {
            if t.elapsed() < RETRY_FAILED_AFTER {
                return;
            }
            self.failed.remove(&hash);
        }
        if self.lookups.len() >= self.cfg.max_lookups {
            return;
        }
        self.stats.hashes_new.fetch_add(1, Relaxed);
        self.in_flight.insert(hash);
        let mut l = Lookup {
            started: Instant::now(),
            peers: Vec::new(),
            queried: HashSet::new(),
            candidates: Vec::new(),
            in_flight: 0,
        };
        match source {
            Some((None, peer)) => {
                // An announcing peer has (or is downloading) the torrent.
                l.peers.push(peer);
            }
            Some((Some(id), node)) => l.candidates.push((id, node)),
            None => {}
        }
        let mut close = self.closest_nodes(&hash, 8);
        close.retain(|c| !l.candidates.iter().any(|x| x.1 == c.1));
        close.reverse();
        // Keep the explicit source last so it is queried first.
        close.append(&mut l.candidates);
        l.candidates = close;
        self.lookups.insert(hash, l);
        self.stats.lookups.store(self.lookups.len() as u64, Relaxed);
        self.advance_lookup(hash);
    }

    fn advance_lookup(&mut self, hash: InfoHash) {
        let Some(l) = self.lookups.get_mut(&hash) else { return };
        if l.peers.len() >= 4 || (!l.peers.is_empty() && l.in_flight == 0) {
            return self.dispatch(hash);
        }
        while l.in_flight < 3 && l.queried.len() < MAX_QUERIES_PER_LOOKUP {
            let Some((_, addr)) = l.candidates.pop() else { break };
            if l.queried.insert(addr) {
                l.in_flight += 1;
                self.lookup_queue.push_back((hash, addr));
            }
        }
        if l.in_flight == 0 {
            // Nothing more to ask and no peers.
            self.lookups.remove(&hash);
            self.in_flight.remove(&hash);
            self.failed.insert(hash, Instant::now());
        }
    }

    fn dispatch(&mut self, hash: InfoHash) {
        let Some(l) = self.lookups.remove(&hash) else { return };
        self.stats.lookups.store(self.lookups.len() as u64, Relaxed);
        if self.fetch_tx.try_send((hash, l.peers)).is_err() {
            // Fetchers saturated: forget it, it will be sampled again later.
            self.in_flight.remove(&hash);
        }
    }

    fn on_fetch_done(&mut self, done: FetchDone) {
        self.in_flight.remove(&done.hash);
        if done.ok {
            // Remembered even when filtered out, so it is never fetched again.
            self.bloom.insert(&done.hash);
        }
        match done.torrent {
            Some(t) => {
                let w = self.writer.clone();
                if let Err(std::sync::mpsc::TrySendError::Full(msg)) = w.try_send(WriteMsg::Insert(t)) {
                    tokio::task::spawn_blocking(move || {
                        let _ = w.send(msg);
                    });
                }
            }
            None if !done.ok => {
                self.failed.insert(done.hash, Instant::now());
            }
            None => {}
        }
    }

    async fn maintenance(&mut self) {
        let now = Instant::now();
        // Expire unanswered queries.
        let mut timed_out = Vec::new();
        self.pending.retain(|_, p| {
            let keep = now.duration_since(p.sent) < QUERY_TIMEOUT;
            if !keep {
                timed_out.push((p.kind, p.addr));
            }
            keep
        });
        for (kind, addr) in timed_out {
            if let Some(n) = self.nodes.get_mut(&addr) {
                n.failures = n.failures.saturating_add(1);
                if !n.responded {
                    // Never answered: free the slot for nodes that might.
                    let id = n.id;
                    self.nodes.remove(&addr);
                    self.dead.insert(addr);
                    if self.by_id.get(&id) == Some(&addr) {
                        self.by_id.remove(&id);
                    }
                } else if n.failures < 3 && matches!(kind, Kind::Sample | Kind::FindNode) {
                    self.revisit.push(Reverse((now + Duration::from_secs(300), addr)));
                }
            }
            if let Kind::GetPeers(hash) = kind {
                if let Some(l) = self.lookups.get_mut(&hash) {
                    l.in_flight = l.in_flight.saturating_sub(1);
                    self.advance_lookup(hash);
                }
            }
        }
        // Expire slow lookups: fetch with whatever peers were found.
        let stale: Vec<InfoHash> = self
            .lookups
            .iter()
            .filter(|(_, l)| now.duration_since(l.started) > LOOKUP_TIMEOUT)
            .map(|(h, _)| *h)
            .collect();
        for h in stale {
            if self.lookups.get(&h).is_some_and(|l| !l.peers.is_empty()) {
                self.dispatch(h);
            } else {
                self.lookups.remove(&h);
                self.in_flight.remove(&h);
                self.failed.insert(h, now);
            }
        }
        self.lookup_queue.retain(|(h, _)| self.lookups.contains_key(h));
        // Forget dead nodes.
        let before = self.nodes.len();
        let by_id = &mut self.by_id;
        self.nodes.retain(|addr, n| {
            let idle = now.duration_since(n.last_seen);
            // Nodes without BEP 51 only help discovery; keep them while they are fresh.
            let dead = n.failures >= 3
                || idle > Duration::from_secs(4 * 3600)
                || (n.bep51 == Some(false) && idle > Duration::from_secs(1800));
            if dead && by_id.get(&n.id) == Some(addr) {
                by_id.remove(&n.id);
            }
            !dead
        });
        if self.nodes.len() != before {
            self.stats.nodes.store(self.nodes.len() as u64, Relaxed);
        }
        if self.revisit.len() > self.cfg.max_nodes * 4 {
            let nodes = &self.nodes;
            self.revisit.retain(|Reverse((_, a))| nodes.contains_key(a));
        }
        if self.dead.len() > 500_000 {
            self.dead.clear();
        }
        if self.failed.len() > 500_000 {
            self.failed.retain(|_, t| now.duration_since(*t) < RETRY_FAILED_AFTER);
        }
        self.stats.lookups.store(self.lookups.len() as u64, Relaxed);
        // Running dry: ask random known nodes (or the routers) for more nodes.
        if self.fresh.is_empty() {
            if self.nodes.len() < 200 && self.last_bootstrap.is_none_or(|t| t.elapsed() > Duration::from_secs(30)) {
                self.bootstrap().await;
            } else {
                let addrs: Vec<SocketAddr> =
                    self.nodes.keys().skip((rand_u64() as usize) % self.nodes.len().max(1)).take(16).copied().collect();
                for addr in addrs {
                    let target: NodeId = rand_bytes();
                    self.send_query(
                        addr,
                        Kind::FindNode,
                        b"find_node",
                        dict([(b"id", bytes(self.id.to_vec())), (b"target", bytes(target.to_vec()))]),
                    );
                }
            }
        }
    }
}

fn take_pending(pending: &mut HashMap<u32, Pending>, tid: &[u8], from: SocketAddr) -> Option<Pending> {
    let tid = u32::from_be_bytes(tid.try_into().ok()?);
    match pending.get(&tid) {
        Some(p) if p.addr == from => pending.remove(&tid),
        _ => None,
    }
}

/// Peers contacted at the same time for one torrent, and in total.
const PARALLEL_PEERS: usize = 4;
const MAX_PEERS_TRIED: usize = 8;

/// Races metadata downloads from several peers; the first valid one wins.
async fn fetch_any(hash: InfoHash, peers: &[SocketAddr], peer_id: [u8; 20]) -> Option<torrent::TorrentInfo> {
    let mut queue = peers.iter().take(MAX_PEERS_TRIED).copied();
    let mut set = tokio::task::JoinSet::new();
    let spawn = |set: &mut tokio::task::JoinSet<_>, peer: SocketAddr| {
        set.spawn(async move { (peer, metadata::fetch(peer, hash, peer_id, Duration::from_secs(12)).await) });
    };
    for peer in queue.by_ref().take(PARALLEL_PEERS) {
        spawn(&mut set, peer);
    }
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((_, Ok(raw))) => {
                if let Some(info) = torrent::parse_info(&raw) {
                    // Dropping the set aborts the other attempts.
                    return Some(info);
                }
            }
            Ok((peer, Err(e))) => log_debug!("fetch {} from {peer}: {e}", torrent::hex(&hash)),
            Err(_) => {}
        }
        if let Some(peer) = queue.next() {
            spawn(&mut set, peer);
        }
    }
    None
}

/// Runs metadata downloads with bounded concurrency.
async fn fetch_dispatcher(
    mut rx: mpsc::Receiver<(InfoHash, Vec<SocketAddr>)>,
    done: mpsc::Sender<FetchDone>,
    concurrency: usize,
    skip_xxx: bool,
    stats: Arc<CrawlerStats>,
) {
    let sem = Arc::new(Semaphore::new(concurrency.max(1)));
    let mut peer_id = *b"-RM0100-000000000000";
    peer_id[8..].copy_from_slice(&rand_bytes::<12>());
    while let Some((hash, peers)) = rx.recv().await {
        let Ok(permit) = sem.clone().acquire_owned().await else { break };
        let done = done.clone();
        let stats = stats.clone();
        stats.fetching.fetch_add(1, Relaxed);
        tokio::spawn(async move {
            let result = fetch_any(hash, &peers, peer_id).await;
            let ok = result.is_some();
            let torrent = result.map(|info| Box::new(NewTorrent::new(hash, info, peers.len() as u32))).filter(|t| {
                let keep = !t.class.banned && !(skip_xxx && t.class.content_type == Some(ContentType::Xxx));
                if !keep {
                    log_debug!("dropping filtered torrent {}", torrent::hex(&hash));
                }
                keep
            });
            if ok {
                stats.fetched.fetch_add(1, Relaxed);
            } else {
                stats.fetch_failed.fetch_add(1, Relaxed);
            }
            stats.fetching.fetch_sub(1, Relaxed);
            drop(permit);
            let _ = done.send(FetchDone { hash, ok, torrent }).await;
        });
    }
}
