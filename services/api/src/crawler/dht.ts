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

const MAX_ROUTING_TABLE = 1000;
const FIND_NODE_INTERVAL = 5000; // 5s between find_node bursts
const MAX_INFOHASH_BUFFER = 10000;

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
  private onInfohash: (infohash: string) => void;

  constructor(port: number, onInfohash: (infohash: string) => void) {
    this.nodeId = randomBytes(20);
    this.onInfohash = onInfohash;

    this.socket = createSocket('udp4');
    this.socket.on('message', (msg, rinfo) => this.handleMessage(msg, rinfo));
    this.socket.on('error', (err) => logger.warn('DHT socket error', { error: err.message }));

    this.socket.bind(port, () => {
      logger.info('DHT listener started', { port, nodeId: this.nodeId.toString('hex').substring(0, 8) });
      this.bootstrap();
      setInterval(() => this.expandRoutingTable(), FIND_NODE_INTERVAL);
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
      // Someone is announcing they have this torrent — confirmed active torrent
      const infohash = a.info_hash.toString('hex');
      this.recordInfohash(infohash);

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

  private recordInfohash(infohash: string): void {
    if (this.infohashBuffer.has(infohash)) return;
    this.infohashBuffer.add(infohash);

    // Prevent unbounded growth
    if (this.infohashBuffer.size > MAX_INFOHASH_BUFFER) {
      const first = this.infohashBuffer.values().next().value!;
      this.infohashBuffer.delete(first);
    }

    this.onInfohash(infohash);
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

  close(): void {
    this.socket.close();
  }
}
