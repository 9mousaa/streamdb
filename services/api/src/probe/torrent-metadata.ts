/**
 * BEP-9 Metadata Fetcher — fetches torrent metadata (file list, name, sizes)
 * from peers using the BitTorrent Extension Protocol (BEP-10 + BEP-9).
 *
 * Zero external dependencies. Uses Node.js built-in `net` module.
 *
 * Flow:
 * 1. TCP connect to peer
 * 2. BitTorrent handshake (with extension protocol support bit)
 * 3. BEP-10 extension handshake (exchange ut_metadata ID)
 * 4. BEP-9 metadata request (piece by piece)
 * 5. Reassemble + bdecode metadata → extract file list
 */

import { createConnection, type Socket } from 'net';
import { randomBytes, createHash } from 'crypto';

export interface TorrentFile {
  path: string;
  size: number;
}

export interface TorrentMetadata {
  name: string;
  files: TorrentFile[];
  pieceLength: number;
  totalSize: number;
}

export interface Peer {
  host: string;
  port: number;
}

const PROTOCOL = Buffer.from('BitTorrent protocol');
const HANDSHAKE_LEN = 68; // 1 + 19 + 8 + 20 + 20
const METADATA_PIECE_SIZE = 16384; // 16KB per BEP-9 spec
const CONNECT_TIMEOUT = 10_000;
const METADATA_TIMEOUT = 30_000;

// Minimal bencode/bdecode for metadata parsing
function bencodeEncode(val: any): Buffer {
  if (typeof val === 'number') return Buffer.from(`i${val}e`);
  if (Buffer.isBuffer(val)) return Buffer.concat([Buffer.from(`${val.length}:`), val]);
  if (typeof val === 'string') {
    const buf = Buffer.from(val);
    return Buffer.concat([Buffer.from(`${buf.length}:`), buf]);
  }
  if (Array.isArray(val)) {
    return Buffer.concat([Buffer.from('l'), ...val.map(bencodeEncode), Buffer.from('e')]);
  }
  if (typeof val === 'object' && val !== null) {
    const keys = Object.keys(val).sort();
    const parts: Buffer[] = [Buffer.from('d')];
    for (const k of keys) {
      parts.push(bencodeEncode(k), bencodeEncode(val[k]));
    }
    parts.push(Buffer.from('e'));
    return Buffer.concat(parts);
  }
  throw new Error('Cannot bencode value');
}

function bencodeDecode(buf: Buffer, offset = 0): { value: any; offset: number } {
  const c = buf[offset];
  if (c === 0x69) { // 'i' — integer
    const end = buf.indexOf(0x65, offset + 1); // 'e'
    return { value: parseInt(buf.subarray(offset + 1, end).toString(), 10), offset: end + 1 };
  }
  if (c === 0x6c) { // 'l' — list
    const list: any[] = [];
    let pos = offset + 1;
    while (buf[pos] !== 0x65) {
      const r = bencodeDecode(buf, pos);
      list.push(r.value);
      pos = r.offset;
    }
    return { value: list, offset: pos + 1 };
  }
  if (c === 0x64) { // 'd' — dict
    const dict: Record<string, any> = {};
    let pos = offset + 1;
    while (buf[pos] !== 0x65) {
      const keyR = bencodeDecode(buf, pos);
      const key = Buffer.isBuffer(keyR.value) ? keyR.value.toString() : String(keyR.value);
      const valR = bencodeDecode(buf, keyR.offset);
      dict[key] = valR.value;
      pos = valR.offset;
    }
    return { value: dict, offset: pos + 1 };
  }
  if (c >= 0x30 && c <= 0x39) { // string
    const colonIdx = buf.indexOf(0x3a, offset);
    const len = parseInt(buf.subarray(offset, colonIdx).toString(), 10);
    const start = colonIdx + 1;
    return { value: buf.subarray(start, start + len), offset: start + len };
  }
  throw new Error(`Invalid bencode at ${offset}`);
}

/**
 * Fetch torrent metadata from a list of peers.
 * Tries each peer sequentially until one succeeds.
 */
export async function fetchMetadata(infohash: string, peers: Peer[], maxPeers = 5): Promise<TorrentMetadata | null> {
  const hashBuf = Buffer.from(infohash, 'hex');
  if (hashBuf.length !== 20) return null;

  const peersToTry = peers.slice(0, maxPeers);
  for (const peer of peersToTry) {
    try {
      const result = await fetchFromPeer(hashBuf, peer);
      if (result) return result;
    } catch {
      // Try next peer
    }
  }
  return null;
}

function fetchFromPeer(infohash: Buffer, peer: Peer): Promise<TorrentMetadata | null> {
  return new Promise((resolve) => {
    const peerId = Buffer.concat([Buffer.from('-SD0100-'), randomBytes(12)]);
    let sock: Socket | null = null;
    let buffer = Buffer.alloc(0);
    let handshakeDone = false;
    let utMetadataId = 0; // Remote peer's ut_metadata extension ID
    let metadataSize = 0;
    let metadataPieces: Map<number, Buffer> = new Map();
    let totalPieces = 0;
    let resolved = false;

    const cleanup = () => {
      if (sock) {
        sock.removeAllListeners();
        sock.destroy();
        sock = null;
      }
    };

    const done = (result: TorrentMetadata | null) => {
      if (resolved) return;
      resolved = true;
      cleanup();
      resolve(result);
    };

    const timer = setTimeout(() => done(null), METADATA_TIMEOUT);

    try {
      sock = createConnection({ host: peer.host, port: peer.port, timeout: CONNECT_TIMEOUT });
    } catch {
      done(null);
      return;
    }

    sock.on('connect', () => {
      // Send BitTorrent handshake with extension protocol bit (reserved byte 5, bit 4)
      const reserved = Buffer.alloc(8);
      reserved[5] = 0x10; // Extension protocol support
      const handshake = Buffer.concat([
        Buffer.from([19]), // Protocol name length
        PROTOCOL,
        reserved,
        infohash,
        peerId,
      ]);
      sock!.write(handshake);
    });

    sock.on('data', (data: Buffer) => {
      buffer = Buffer.concat([buffer, data]);

      if (!handshakeDone) {
        if (buffer.length < HANDSHAKE_LEN) return;

        // Verify handshake
        const pstrlen = buffer[0];
        if (pstrlen !== 19) { done(null); return; }

        const peerReserved = buffer.subarray(20, 28);
        const peerInfohash = buffer.subarray(28, 48);

        // Check infohash matches
        if (!peerInfohash.equals(infohash)) { done(null); return; }

        // Check extension protocol support
        if (!(peerReserved[5] & 0x10)) { done(null); return; }

        buffer = buffer.subarray(HANDSHAKE_LEN);
        handshakeDone = true;

        // Send BEP-10 extension handshake (message ID 20, extension ID 0)
        const extHandshake = bencodeEncode({ m: { ut_metadata: 1 } });
        const msg = Buffer.alloc(4 + 1 + 1 + extHandshake.length);
        msg.writeUInt32BE(2 + extHandshake.length, 0); // length prefix
        msg[4] = 20; // Extended message
        msg[5] = 0;  // Handshake extension ID
        extHandshake.copy(msg, 6);
        sock!.write(msg);
      }

      // Process BT messages
      while (buffer.length >= 4) {
        const msgLen = buffer.readUInt32BE(0);
        if (msgLen === 0) { buffer = buffer.subarray(4); continue; } // keep-alive
        if (buffer.length < 4 + msgLen) break; // Incomplete message

        const msgId = buffer[4];
        const payload = buffer.subarray(5, 4 + msgLen);
        buffer = buffer.subarray(4 + msgLen);

        if (msgId === 20) { // Extended message
          handleExtended(payload);
        }
      }
    });

    sock.on('error', () => done(null));
    sock.on('close', () => done(null));
    sock.on('timeout', () => done(null));

    function handleExtended(payload: Buffer) {
      if (payload.length === 0) return;
      const extId = payload[0];
      const extPayload = payload.subarray(1);

      if (extId === 0) {
        // Extension handshake response
        try {
          const decoded = bencodeDecode(extPayload).value;
          if (decoded.m?.ut_metadata) {
            utMetadataId = typeof decoded.m.ut_metadata === 'number'
              ? decoded.m.ut_metadata
              : Buffer.isBuffer(decoded.m.ut_metadata)
                ? decoded.m.ut_metadata.readUInt8(0)
                : 0;
          }
          if (decoded.metadata_size) {
            metadataSize = typeof decoded.metadata_size === 'number'
              ? decoded.metadata_size
              : 0;
          }

          if (utMetadataId > 0 && metadataSize > 0) {
            totalPieces = Math.ceil(metadataSize / METADATA_PIECE_SIZE);
            // Request all pieces
            for (let i = 0; i < totalPieces; i++) {
              requestMetadataPiece(i);
            }
          } else {
            done(null); // Peer doesn't support ut_metadata
          }
        } catch {
          done(null);
        }
      } else if (extId === 1) {
        // ut_metadata response (our local ID is 1)
        try {
          const decoded = bencodeDecode(extPayload);
          const msg = decoded.value;
          const msgType = typeof msg.msg_type === 'number' ? msg.msg_type : -1;
          const piece = typeof msg.piece === 'number' ? msg.piece : -1;

          if (msgType === 1 && piece >= 0) { // data
            const pieceData = extPayload.subarray(decoded.offset);
            metadataPieces.set(piece, pieceData);

            if (metadataPieces.size === totalPieces) {
              assembleMetadata();
            }
          } else if (msgType === 2) { // reject
            done(null);
          }
        } catch {
          done(null);
        }
      }
    }

    function requestMetadataPiece(piece: number) {
      const req = bencodeEncode({ msg_type: 0, piece });
      const msg = Buffer.alloc(4 + 1 + 1 + req.length);
      msg.writeUInt32BE(2 + req.length, 0);
      msg[4] = 20; // Extended message
      msg[5] = utMetadataId;
      req.copy(msg, 6);
      sock?.write(msg);
    }

    function assembleMetadata() {
      clearTimeout(timer);
      // Concatenate all pieces in order
      const parts: Buffer[] = [];
      for (let i = 0; i < totalPieces; i++) {
        const p = metadataPieces.get(i);
        if (!p) { done(null); return; }
        parts.push(p);
      }
      const raw = Buffer.concat(parts);

      // Verify: SHA1 of raw metadata should equal the infohash
      const hash = createHash('sha1').update(raw).digest();
      if (!hash.equals(infohash)) { done(null); return; }

      // Decode the info dictionary
      try {
        const info = bencodeDecode(raw).value;
        const result = parseInfoDict(info);
        done(result);
      } catch {
        done(null);
      }
    }
  });
}

function parseInfoDict(info: Record<string, any>): TorrentMetadata {
  const name = Buffer.isBuffer(info.name) ? info.name.toString('utf8') : String(info.name || 'unknown');
  const pieceLength = typeof info['piece length'] === 'number' ? info['piece length'] : 0;
  const files: TorrentFile[] = [];
  let totalSize = 0;

  if (Array.isArray(info.files)) {
    // Multi-file torrent
    for (const f of info.files) {
      const pathParts = Array.isArray(f.path)
        ? f.path.map((p: any) => Buffer.isBuffer(p) ? p.toString('utf8') : String(p))
        : [];
      const path = pathParts.join('/');
      const size = typeof f.length === 'number' ? f.length : 0;
      files.push({ path, size });
      totalSize += size;
    }
  } else {
    // Single-file torrent
    const size = typeof info.length === 'number' ? info.length : 0;
    files.push({ path: name, size });
    totalSize = size;
  }

  return { name, files, pieceLength, totalSize };
}
