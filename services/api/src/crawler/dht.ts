/**
 * Passive DHT listener — BEP-5 (Kademlia-based DHT) implementation
 * using Node.js built-in dgram (UDP). Zero external dependencies.
 *
 * How it works:
 * 1. Joins the BitTorrent DHT network by pinging bootstrap nodes
 * 2. Builds a small routing table of known nodes
 * 3. Periodically sends find_node queries to expand our view
 * 4. Intercepts get_peers and announce_peer messages from other nodes
 * 5. Extracts infohashes (these are torrents being actively sought/seeded)
 *
 * Resource usage: ~50MB RAM, minimal CPU. One UDP socket.
 */

import { createSocket, type Socket, type RemoteInfo } from 'dgram';
import { randomBytes } from 'crypto';
import { logger } from '../utils/logger.js';

const BOOTSTRAP_NODES = [
  { host: 'router.bittorrent.com', port: 6881 },
  { host: 'dht.transmissionbt.com', port: 6881 },
  { host: 'router.utorrent.com', port: 6881 },
  { host: 'dht.libtorrent.org', port: 25401 },
];

const MAX_ROUTING_TABLE = 3000;
const FIND_NODE_INTERVAL = 5000; // 5s between find_node bursts
const CRAWL_INTERVAL = 2000; // 2s between active crawl rounds (reduced from 1s for memory)
const MAX_INFOHASH_BUFFER = 20000;

interface DHTNode {
  id: Buffer;
  host: string;
  port: number;
  lastSeen: number;
}

// Bencode encoder/decoder (minimal, inline — no deps)
function bencode(val: any): Buffer {
  if (typeof val === 'number') {
    return Buffer.from(`i${val}e`);
  }
  if (Buffer.isBuffer(val)) {
    return Buffer.concat([Buffer.from(`${val.length}:`), val]);
  }
  if (typeof val === 'string') {
    const buf = Buffer.from(val);
    return Buffer.concat([Buffer.from(`${buf.length}:`), buf]);
  }
  if (Array.isArray(val)) {
    const parts = [Buffer.from('l'), ...val.map(bencode), Buffer.from('e')];
    return Buffer.concat(parts);
  }
  if (typeof val === 'object' && val !== null) {
    const keys = Object.keys(val).sort();
    const parts: Buffer[] = [Buffer.from('d')];
    for (const k of keys) {
      if (val[k] === undefined) continue;
      parts.push(bencode(k), bencode(val[k]));
    }
    parts.push(Buffer.from('e'));
    return Buffer.concat(parts);
  }
  throw new Error(`Cannot bencode type: ${typeof val}`);
}

function bdecode(buf: Buffer, offset = 0): { value: any; offset: number } {
  const c = buf[offset];

  // Integer: i<number>e
  if (c === 0x69) { // 'i'
    const end = buf.indexOf(0x65, offset + 1); // 'e'
    if (end === -1) throw new Error('Unterminated integer');
    const num = parseInt(buf.subarray(offset + 1, end).toString(), 10);
    return { value: num, offset: end + 1 };
  }

  // List: l...e
  if (c === 0x6c) { // 'l'
    const items: any[] = [];
    let pos = offset + 1;
    while (buf[pos] !== 0x65) { // 'e'
      const { value, offset: newPos } = bdecode(buf, pos);
      items.push(value);
      pos = newPos;
    }
    return { value: items, offset: pos + 1 };
  }

  // Dictionary: d...e
  if (c === 0x64) { // 'd'
    const dict: Record<string, any> = {};
    let pos = offset + 1;
    while (buf[pos] !== 0x65) { // 'e'
      const { value: key, offset: keyEnd } = bdecode(buf, pos);
      const { value: val, offset: valEnd } = bdecode(buf, keyEnd);
      dict[key.toString()] = val;
      pos = valEnd;
    }
    return { value: dict, offset: pos + 1 };
  }

  // String: <length>:<data>
  if (c >= 0x30 && c <= 0x39) { // '0'-'9'
    const colonIdx = buf.indexOf(0x3a, offset); // ':'
    if (colonIdx === -1) throw new Error('Unterminated string length');
    const len = parseInt(buf.subarray(offset, colonIdx).toString(), 10);
    const start = colonIdx + 1;
    const data = buf.subarray(start, start + len);
    return { value: data, offset: start + len };
  }

  throw new Error(`Invalid bencode at offset ${offset}: 0x${c.toString(16)}`);
}

export class DHTListener {
  private socket: Socket;
  private nodeId: Buffer;
  private routingTable: Map<string, DHTNode> = new Map();
  private transactionCounter = 0;
  private infohashBuffer: Set<string> = new Set();
  private onInfohash: (infohash: string, hasRealPeers: boolean) => void;
  // Track pending get_peers transactions → infohash they were looking for
  private pendingGetPeers: Map<string, string> = new Map();
  private crawlPosition: number = 0; // For systematic keyspace walking
  // Cache of infohash → actual peers (from get_peers values + announce_peer)
  private peerCache: Map<string, { host: string; port: number; ts: number }[]> = new Map();
  private static readonly PEER_CACHE_MAX = 20_000;
  private static readonly PEER_CACHE_TTL = 5 * 60_000; // 5 minutes
  // Per-transaction callbacks for iterative lookups
  private txCallbacks: Map<string, (r: any, rinfo: RemoteInfo) => void> = new Map();

  constructor(port: number, onInfohash: (infohash: string, hasRealPeers: boolean) => void) {
    this.nodeId = randomBytes(20);
    this.onInfohash = onInfohash;

    this.socket = createSocket('udp4');
    this.socket.on('message', (msg, rinfo) => this.handleMessage(msg, rinfo));
    this.socket.on('error', (err) => logger.warn('DHT socket error', { error: err.message }));

    this.socket.bind(port, () => {
      logger.info('DHT listener started', { port, nodeId: this.nodeId.toString('hex').substring(0, 8) });
      this.bootstrap();
      setInterval(() => this.expandRoutingTable(), FIND_NODE_INTERVAL);
      setInterval(() => this.activeCrawl(), CRAWL_INTERVAL);
      setInterval(() => { this.cleanPeerCache(); this.cleanTxCallbacks(); }, 60_000);
    });
  }

  private nextTxId(): Buffer {
    this.transactionCounter = (this.transactionCounter + 1) & 0xffff;
    const buf = Buffer.alloc(2);
    buf.writeUInt16BE(this.transactionCounter);
    return buf;
  }

  private send(msg: Record<string, any>, host: string, port: number): void {
    try {
      const encoded = bencode(msg);
      this.socket.send(encoded, 0, encoded.length, port, host);
    } catch { /* ignore send errors */ }
  }

  private bootstrap(): void {
    for (const node of BOOTSTRAP_NODES) {
      this.sendFindNode(node.host, node.port, randomBytes(20));
    }
  }

  private sendFindNode(host: string, port: number, target: Buffer): void {
    this.send({
      t: this.nextTxId(),
      y: 'q',
      q: 'find_node',
      a: { id: this.nodeId, target },
    }, host, port);
  }

  private sendPing(host: string, port: number): void {
    this.send({
      t: this.nextTxId(),
      y: 'q',
      q: 'ping',
      a: { id: this.nodeId },
    }, host, port);
  }

  private expandRoutingTable(): void {
    // Send find_node to random nodes in our table with a random target
    const nodes = [...this.routingTable.values()];
    const count = Math.min(nodes.length, 8);
    for (let i = 0; i < count; i++) {
      const node = nodes[Math.floor(Math.random() * nodes.length)];
      this.sendFindNode(node.host, node.port, randomBytes(20));
    }
  }

  private sendGetPeers(host: string, port: number, infohash: Buffer): void {
    const txId = this.nextTxId();
    const txKey = txId.toString('hex');
    this.pendingGetPeers.set(txKey, infohash.toString('hex'));
    // Clean up old pending entries (prevent memory leak)
    if (this.pendingGetPeers.size > 10000) {
      const first = this.pendingGetPeers.keys().next().value!;
      this.pendingGetPeers.delete(first);
    }
    this.send({
      t: txId,
      y: 'q',
      q: 'get_peers',
      a: { id: this.nodeId, info_hash: infohash },
    }, host, port);
  }

  /**
   * Active DHT crawling — systematically walk the keyspace.
   * Generates targets across the full 160-bit hash space and sends
   * get_peers queries to discover infohashes being actively used.
   */
  private activeCrawl(): void {
    const nodes = [...this.routingTable.values()];
    if (nodes.length < 10) return; // Wait until we have enough nodes

    // Walk the keyspace: generate a target with the high byte set to crawlPosition
    // This ensures we systematically cover different regions of the hash space
    const target = randomBytes(20);
    target[0] = this.crawlPosition & 0xff;
    target[1] = (this.crawlPosition >> 8) & 0xff;
    this.crawlPosition = (this.crawlPosition + 1) % 65536;

    // Send get_peers to the closest nodes we know
    const sorted = this.closestNodes(target, 6);
    for (const node of sorted) {
      this.sendGetPeers(node.host, node.port, target);
    }
  }

  /**
   * Find peers for a specific infohash (used by probe pipeline).
   * Returns found peers via the onInfohash callback.
   */
  crawlTargeted(infohash: string): void {
    const target = Buffer.from(infohash, 'hex');
    if (target.length !== 20) return;
    const closest = this.closestNodes(target, 8);
    for (const node of closest) {
      this.sendGetPeers(node.host, node.port, target);
    }
  }

  private closestNodes(target: Buffer, count: number): DHTNode[] {
    const nodes = [...this.routingTable.values()];
    // Sort by XOR distance to target
    nodes.sort((a, b) => {
      for (let i = 0; i < 20; i++) {
        const da = a.id[i] ^ target[i];
        const db = b.id[i] ^ target[i];
        if (da !== db) return da - db;
      }
      return 0;
    });
    return nodes.slice(0, count);
  }

  private handleMessage(msg: Buffer, rinfo: RemoteInfo): void {
    let decoded: any;
    try {
      decoded = bdecode(msg).value;
    } catch {
      return; // Ignore malformed messages
    }

    if (!decoded || typeof decoded !== 'object') return;

    const type = decoded.y?.toString?.();

    if (type === 'r') {
      // Response — extract nodes for routing table
      this.handleResponse(decoded, rinfo);
    } else if (type === 'q') {
      // Query from another node — this is where we discover infohashes
      this.handleQuery(decoded, rinfo);
    }
  }

  private handleResponse(msg: any, rinfo: RemoteInfo): void {
    const r = msg.r;
    if (!r) return;

    // Add responding node to routing table
    if (Buffer.isBuffer(r.id) && r.id.length === 20) {
      this.addNode(r.id, rinfo.address, rinfo.port);
    }

    // Parse compact node info from "nodes" field
    if (Buffer.isBuffer(r.nodes) && r.nodes.length >= 26) {
      this.parseCompactNodes(r.nodes);
    }

    // Check for iterative lookup callbacks first
    const txKey = Buffer.isBuffer(msg.t) ? msg.t.toString('hex') : '';
    const cb = this.txCallbacks.get(txKey);
    if (cb) {
      cb(r, rinfo);
      // Don't return — still process nodes/values for background caching
    }

    // Handle get_peers response — if values present, the infohash is active
    const pendingHash = this.pendingGetPeers.get(txKey);
    if (pendingHash) {
      this.pendingGetPeers.delete(txKey);
      // If response contains "values", peers exist for this infohash — it's active
      if (Array.isArray(r.values) && r.values.length > 0) {
        this.recordInfohash(pendingHash, true);
        // Parse compact peer info (6 bytes each: 4 IP + 2 port) and cache
        const now = Date.now();
        const peers = this.peerCache.get(pendingHash) || [];
        for (const val of r.values) {
          if (Buffer.isBuffer(val) && val.length === 6) {
            const host = `${val[0]}.${val[1]}.${val[2]}.${val[3]}`;
            const port = val.readUInt16BE(4);
            if (port > 0 && port < 65536 && host !== '0.0.0.0') {
              // Avoid duplicates
              if (!peers.some(p => p.host === host && p.port === port)) {
                peers.push({ host, port, ts: now });
              }
            }
          }
        }
        if (peers.length > 0) {
          this.peerCache.set(pendingHash, peers);
          this.evictPeerCacheIfNeeded();
        }
      }
    }
  }

  private handleQuery(msg: any, rinfo: RemoteInfo): void {
    const q = msg.q?.toString?.();
    const a = msg.a;
    if (!a) return;

    // Add querying node to routing table
    if (Buffer.isBuffer(a.id) && a.id.length === 20) {
      this.addNode(a.id, rinfo.address, rinfo.port);
    }

    if (q === 'get_peers' && Buffer.isBuffer(a.info_hash) && a.info_hash.length === 20) {
      // Someone is looking for peers for this infohash — this torrent is actively wanted
      const infohash = a.info_hash.toString('hex');
      this.recordInfohash(infohash);

      // Reply with our node ID and a token (required by protocol)
      this.send({
        t: msg.t,
        y: 'r',
        r: {
          id: this.nodeId,
          token: randomBytes(4),
          nodes: Buffer.alloc(0), // We don't have peers
        },
      }, rinfo.address, rinfo.port);
    }

    if (q === 'announce_peer' && Buffer.isBuffer(a.info_hash) && a.info_hash.length === 20) {
      // Someone is announcing they have this torrent — confirmed active torrent with real peer
      const infohash = a.info_hash.toString('hex');
      this.recordInfohash(infohash, true);

      // The announcing node IS a peer — cache it
      // BEP-5: if implied_port is set, use UDP source port; otherwise use a.port
      const peerPort = a.implied_port ? rinfo.port : (typeof a.port === 'number' ? a.port : rinfo.port);
      if (peerPort > 0 && peerPort < 65536) {
        const peers = this.peerCache.get(infohash) || [];
        if (!peers.some(p => p.host === rinfo.address && p.port === peerPort)) {
          peers.push({ host: rinfo.address, port: peerPort, ts: Date.now() });
          this.peerCache.set(infohash, peers);
          this.evictPeerCacheIfNeeded();
        }
      }

      // Acknowledge
      this.send({
        t: msg.t,
        y: 'r',
        r: { id: this.nodeId },
      }, rinfo.address, rinfo.port);
    }

    if (q === 'ping') {
      this.send({
        t: msg.t,
        y: 'r',
        r: { id: this.nodeId },
      }, rinfo.address, rinfo.port);
    }

    if (q === 'find_node') {
      // Reply with nodes from our routing table
      const nodes = this.getCompactNodes(8);
      this.send({
        t: msg.t,
        y: 'r',
        r: { id: this.nodeId, nodes },
      }, rinfo.address, rinfo.port);
    }
  }

  private recordInfohash(infohash: string, hasRealPeers = false): void {
    if (this.infohashBuffer.has(infohash)) return;
    this.infohashBuffer.add(infohash);

    // Prevent unbounded growth
    if (this.infohashBuffer.size > MAX_INFOHASH_BUFFER) {
      const first = this.infohashBuffer.values().next().value!;
      this.infohashBuffer.delete(first);
    }

    this.onInfohash(infohash, hasRealPeers);
  }

  private addNode(id: Buffer, host: string, port: number): void {
    if (port <= 0 || port > 65535) return;
    if (host === '0.0.0.0' || host === '127.0.0.1') return;

    const key = `${host}:${port}`;
    if (this.routingTable.size >= MAX_ROUTING_TABLE && !this.routingTable.has(key)) {
      // Evict oldest
      let oldest: string | null = null;
      let oldestTime = Infinity;
      for (const [k, v] of this.routingTable) {
        if (v.lastSeen < oldestTime) {
          oldestTime = v.lastSeen;
          oldest = k;
        }
      }
      if (oldest) this.routingTable.delete(oldest);
    }

    this.routingTable.set(key, { id, host, port, lastSeen: Date.now() });
  }

  private parseCompactNodes(buf: Buffer): void {
    // Each node is 26 bytes: 20 byte node ID + 4 byte IP + 2 byte port
    for (let i = 0; i + 26 <= buf.length; i += 26) {
      const id = buf.subarray(i, i + 20);
      const ip = `${buf[i + 20]}.${buf[i + 21]}.${buf[i + 22]}.${buf[i + 23]}`;
      const port = buf.readUInt16BE(i + 24);
      if (port > 0 && port < 65536) {
        this.addNode(id, ip, port);
      }
    }
  }

  private getCompactNodes(count: number): Buffer {
    const nodes = [...this.routingTable.values()].slice(0, count);
    const buf = Buffer.alloc(nodes.length * 26);
    for (let i = 0; i < nodes.length; i++) {
      const n = nodes[i];
      n.id.copy(buf, i * 26);
      const parts = n.host.split('.');
      buf[i * 26 + 20] = parseInt(parts[0]);
      buf[i * 26 + 21] = parseInt(parts[1]);
      buf[i * 26 + 22] = parseInt(parts[2]);
      buf[i * 26 + 23] = parseInt(parts[3]);
      buf.writeUInt16BE(n.port, i * 26 + 24);
    }
    return buf;
  }

  getStats(): { routingTableSize: number; infohashesDiscovered: number } {
    return {
      routingTableSize: this.routingTable.size,
      infohashesDiscovered: this.infohashBuffer.size,
    };
  }

  /** Get routing table nodes as potential peers for BEP-9 metadata fetching */
  getRoutingNodes(count: number): { host: string; port: number }[] {
    const nodes = [...this.routingTable.values()];
    const shuffled = nodes.sort(() => Math.random() - 0.5);
    return shuffled.slice(0, count).map(n => ({ host: n.host, port: n.port }));
  }

  /**
   * Iterative DHT lookup — proper BEP-5 Kademlia-style peer discovery.
   * Sends get_peers, follows nodes responses to get closer, collects peers.
   * Typically takes 3-5 rounds, ~10-20 seconds.
   */
  findPeersForHash(infohash: string): Promise<{ host: string; port: number }[]> {
    const target = Buffer.from(infohash, 'hex');
    if (target.length !== 20) return Promise.resolve([]);

    const ALPHA = 8;       // Parallel queries per round
    const MAX_ROUNDS = 5;
    const ROUND_TIMEOUT = 3000; // 3s per round
    const foundPeers: Map<string, { host: string; port: number }> = new Map();
    const queried = new Set<string>();      // "host:port" we already sent to
    // Candidate nodes sorted by distance, to query in next round
    let candidates: { host: string; port: number; id: Buffer }[] =
      this.closestNodes(target, ALPHA * 2).map(n => ({ host: n.host, port: n.port, id: n.id }));

    return new Promise<{ host: string; port: number }[]>(resolve => {
      let round = 0;

      const runRound = () => {
        if (round >= MAX_ROUNDS || foundPeers.size > 0) {
          // We either have peers or exhausted rounds
          if (foundPeers.size > 0) {
            // Also cache them
            const now = Date.now();
            const cached = this.peerCache.get(infohash) || [];
            for (const p of foundPeers.values()) {
              if (!cached.some(c => c.host === p.host && c.port === p.port)) {
                cached.push({ host: p.host, port: p.port, ts: now });
              }
            }
            this.peerCache.set(infohash, cached);
            this.evictPeerCacheIfNeeded();
          }
          resolve([...foundPeers.values()]);
          return;
        }

        // Pick up to ALPHA unqueried candidates
        const toQuery = candidates
          .filter(c => !queried.has(`${c.host}:${c.port}`))
          .slice(0, ALPHA);

        if (toQuery.length === 0) {
          resolve([...foundPeers.values()]);
          return;
        }

        let responsesLeft = toQuery.length;
        const newNodes: { host: string; port: number; id: Buffer }[] = [];

        const onAllDone = () => {
          // Sort new candidates by XOR distance and merge
          for (const n of newNodes) {
            const key = `${n.host}:${n.port}`;
            if (!queried.has(key) && !candidates.some(c => c.host === n.host && c.port === n.port)) {
              candidates.push(n);
            }
          }
          candidates.sort((a, b) => {
            for (let i = 0; i < 20; i++) {
              const da = a.id[i] ^ target[i];
              const db = b.id[i] ^ target[i];
              if (da !== db) return da - db;
            }
            return 0;
          });
          round++;
          // If we found peers this round, resolve immediately
          if (foundPeers.size > 0) {
            runRound(); // Will resolve at the top
          } else {
            runRound();
          }
        };

        let roundDone = false;
        const roundTimer = setTimeout(() => {
          if (!roundDone) {
            roundDone = true;
            onAllDone();
          }
        }, ROUND_TIMEOUT);

        for (const node of toQuery) {
          queried.add(`${node.host}:${node.port}`);

          const txId = this.nextTxId();
          const txKey = txId.toString('hex');

          this.txCallbacks.set(txKey, (r: any, _rinfo: RemoteInfo) => {
            this.txCallbacks.delete(txKey);

            // Parse peers from values
            if (Array.isArray(r.values)) {
              for (const val of r.values) {
                if (Buffer.isBuffer(val) && val.length === 6) {
                  const host = `${val[0]}.${val[1]}.${val[2]}.${val[3]}`;
                  const port = val.readUInt16BE(4);
                  if (port > 0 && port < 65536 && host !== '0.0.0.0') {
                    foundPeers.set(`${host}:${port}`, { host, port });
                  }
                }
              }
            }

            // Parse closer nodes
            if (Buffer.isBuffer(r.nodes) && r.nodes.length >= 26) {
              for (let i = 0; i + 26 <= r.nodes.length; i += 26) {
                const id = Buffer.from(r.nodes.subarray(i, i + 20));
                const ip = `${r.nodes[i + 20]}.${r.nodes[i + 21]}.${r.nodes[i + 22]}.${r.nodes[i + 23]}`;
                const p = r.nodes.readUInt16BE(i + 24);
                if (p > 0 && p < 65536 && ip !== '0.0.0.0' && ip !== '127.0.0.1') {
                  newNodes.push({ host: ip, port: p, id });
                  // Also add to our routing table
                  this.addNode(id, ip, p);
                }
              }
            }

            responsesLeft--;
            if (responsesLeft <= 0 && !roundDone) {
              roundDone = true;
              clearTimeout(roundTimer);
              onAllDone();
            }
          });

          // Send get_peers with the callback-tracked txId
          this.send({
            t: txId,
            y: 'q',
            q: 'get_peers',
            a: { id: this.nodeId, info_hash: target },
          }, node.host, node.port);
        }
      };

      runRound();
    });
  }

  /** Get cached peers for a specific infohash (from get_peers values + announce_peer) */
  getPeersForHash(infohash: string): { host: string; port: number }[] {
    const peers = this.peerCache.get(infohash);
    if (!peers) return [];
    const now = Date.now();
    // Filter out stale entries
    return peers
      .filter(p => now - p.ts < DHTListener.PEER_CACHE_TTL)
      .map(p => ({ host: p.host, port: p.port }));
  }

  private cleanTxCallbacks(): void {
    // txCallbacks from abandoned iterative lookups — just clear any stragglers
    if (this.txCallbacks.size > 1000) {
      this.txCallbacks.clear();
    }
  }

  private cleanPeerCache(): void {
    const now = Date.now();
    for (const [hash, peers] of this.peerCache) {
      const fresh = peers.filter(p => now - p.ts < DHTListener.PEER_CACHE_TTL);
      if (fresh.length === 0) {
        this.peerCache.delete(hash);
      } else {
        this.peerCache.set(hash, fresh);
      }
    }
  }

  private evictPeerCacheIfNeeded(): void {
    if (this.peerCache.size <= DHTListener.PEER_CACHE_MAX) return;
    // Evict oldest entries
    const first = this.peerCache.keys().next().value!;
    this.peerCache.delete(first);
  }

  close(): void {
    this.socket.close();
  }
}
