/**
 * Partial Piece Downloader — downloads specific pieces from a torrent
 * via BitTorrent protocol (BEP-3) for OS hash computation.
 *
 * Only downloads the first 64KB and last 64KB of the largest video file,
 * which is enough for OpenSubtitles hash computation.
 *
 * Zero external dependencies. Uses Node.js built-in `net` module.
 */

import { createConnection, type Socket } from 'net';
import { randomBytes } from 'crypto';
import type { TorrentMetadata, Peer } from './torrent-metadata.js';

const PROTOCOL = Buffer.from('BitTorrent protocol');
const CONNECT_TIMEOUT = 10_000;
const DOWNLOAD_TIMEOUT = 30_000;
const BLOCK_SIZE = 16384; // 16KB — standard BT block size
const OSHASH_CHUNK_SIZE = 65536; // 64KB for OS hash

export interface PartialData {
  firstChunk: Buffer; // First 64KB of the file
  lastChunk: Buffer;  // Last 64KB of the file
  fileSize: number;
}

// BT message IDs
const MSG_CHOKE = 0;
const MSG_UNCHOKE = 1;
const MSG_INTERESTED = 2;
const MSG_BITFIELD = 5;
const MSG_REQUEST = 6;
const MSG_PIECE = 7;

/**
 * Find the largest video file in a torrent's file list.
 * Returns its index and byte offset within the torrent.
 */
function findVideoFile(meta: TorrentMetadata): { fileIndex: number; offset: number; size: number } | null {
  const videoExts = /\.(mkv|mp4|avi|wmv|flv|mov|webm|m4v|ts|mpg|mpeg)$/i;
  let best = -1;
  let bestSize = 0;
  let offset = 0;
  let bestOffset = 0;

  for (let i = 0; i < meta.files.length; i++) {
    const f = meta.files[i];
    if (videoExts.test(f.path) && f.size > bestSize) {
      best = i;
      bestSize = f.size;
      bestOffset = offset;
    }
    offset += f.size;
  }

  if (best < 0) return null;
  return { fileIndex: best, offset: bestOffset, size: bestSize };
}

/**
 * Calculate which torrent pieces contain specific byte ranges of a file.
 */
function piecesForRange(
  fileOffset: number, rangeStart: number, rangeLength: number, pieceLength: number
): { pieceIndex: number; begin: number; length: number }[] {
  const absStart = fileOffset + rangeStart;
  const absEnd = absStart + rangeLength;
  const requests: { pieceIndex: number; begin: number; length: number }[] = [];

  const firstPiece = Math.floor(absStart / pieceLength);
  const lastPiece = Math.floor((absEnd - 1) / pieceLength);

  for (let p = firstPiece; p <= lastPiece; p++) {
    const pieceStart = p * pieceLength;
    const overlapStart = Math.max(absStart, pieceStart);
    const overlapEnd = Math.min(absEnd, pieceStart + pieceLength);
    requests.push({
      pieceIndex: p,
      begin: overlapStart - pieceStart,
      length: overlapEnd - overlapStart,
    });
  }

  return requests;
}

/**
 * Download the first and last 64KB of the largest video file in a torrent.
 * Tries each peer until one succeeds.
 */
export async function downloadPartial(
  infohash: string, meta: TorrentMetadata, peers: Peer[], maxPeers = 5
): Promise<PartialData | null> {
  const video = findVideoFile(meta);
  if (!video || video.size < OSHASH_CHUNK_SIZE * 2) return null;

  const hashBuf = Buffer.from(infohash, 'hex');
  if (hashBuf.length !== 20) return null;

  // Calculate which pieces we need
  const firstRange = piecesForRange(video.offset, 0, OSHASH_CHUNK_SIZE, meta.pieceLength);
  const lastStart = video.size - OSHASH_CHUNK_SIZE;
  const lastRange = piecesForRange(video.offset, lastStart, OSHASH_CHUNK_SIZE, meta.pieceLength);

  const allRequests = [...firstRange, ...lastRange];

  for (const peer of peers.slice(0, maxPeers)) {
    try {
      const pieces = await downloadPieces(hashBuf, peer, allRequests);
      if (!pieces) continue;

      // Reassemble the first and last chunks
      const firstChunk = reassembleRange(pieces, firstRange, video.offset, 0, meta.pieceLength);
      const lastChunk = reassembleRange(pieces, lastRange, video.offset, lastStart, meta.pieceLength);

      if (firstChunk.length >= OSHASH_CHUNK_SIZE && lastChunk.length >= OSHASH_CHUNK_SIZE) {
        return {
          firstChunk: firstChunk.subarray(0, OSHASH_CHUNK_SIZE),
          lastChunk: lastChunk.subarray(0, OSHASH_CHUNK_SIZE),
          fileSize: video.size,
        };
      }
    } catch {
      // Try next peer
    }
  }
  return null;
}

function reassembleRange(
  pieces: Map<string, Buffer>,
  requests: { pieceIndex: number; begin: number; length: number }[],
  fileOffset: number, rangeStart: number, pieceLength: number
): Buffer {
  const absStart = fileOffset + rangeStart;
  const totalLen = requests.reduce((sum, r) => sum + r.length, 0);
  const result = Buffer.alloc(totalLen);
  let writePos = 0;

  for (const req of requests) {
    const key = `${req.pieceIndex}:${req.begin}`;
    const data = pieces.get(key);
    if (!data) return Buffer.alloc(0);
    data.copy(result, writePos, 0, req.length);
    writePos += req.length;
  }

  return result;
}

function downloadPieces(
  infohash: Buffer,
  peer: Peer,
  requests: { pieceIndex: number; begin: number; length: number }[]
): Promise<Map<string, Buffer> | null> {
  return new Promise((resolve) => {
    const peerId = Buffer.concat([Buffer.from('-SD0100-'), randomBytes(12)]);
    let sock: Socket | null = null;
    let buffer = Buffer.alloc(0);
    let handshakeDone = false;
    let unchoked = false;
    let resolved = false;
    const receivedPieces = new Map<string, Buffer>();
    let pendingRequests: { pieceIndex: number; begin: number; length: number }[] = [];

    const cleanup = () => {
      if (sock) {
        sock.removeAllListeners();
        sock.destroy();
        sock = null;
      }
    };

    const done = (result: Map<string, Buffer> | null) => {
      if (resolved) return;
      resolved = true;
      clearTimeout(timer);
      cleanup();
      resolve(result);
    };

    const timer = setTimeout(() => done(null), DOWNLOAD_TIMEOUT);

    try {
      sock = createConnection({ host: peer.host, port: peer.port, timeout: CONNECT_TIMEOUT });
    } catch {
      done(null);
      return;
    }

    sock.on('connect', () => {
      // BitTorrent handshake
      const reserved = Buffer.alloc(8);
      const handshake = Buffer.concat([
        Buffer.from([19]),
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
        if (buffer.length < 68) return;
        const peerInfohash = buffer.subarray(28, 48);
        if (!peerInfohash.equals(infohash)) { done(null); return; }
        buffer = buffer.subarray(68);
        handshakeDone = true;

        // Send interested
        const interested = Buffer.alloc(5);
        interested.writeUInt32BE(1, 0);
        interested[4] = MSG_INTERESTED;
        sock!.write(interested);
      }

      // Process messages
      while (buffer.length >= 4) {
        const msgLen = buffer.readUInt32BE(0);
        if (msgLen === 0) { buffer = buffer.subarray(4); continue; }
        if (buffer.length < 4 + msgLen) break;

        const msgId = buffer[4];
        const payload = buffer.subarray(5, 4 + msgLen);
        buffer = buffer.subarray(4 + msgLen);

        handleMessage(msgId, payload);
      }
    });

    sock.on('error', () => done(null));
    sock.on('close', () => done(null));
    sock.on('timeout', () => done(null));

    function handleMessage(msgId: number, payload: Buffer) {
      switch (msgId) {
        case MSG_UNCHOKE:
          unchoked = true;
          sendRequests();
          break;
        case MSG_CHOKE:
          unchoked = false;
          break;
        case MSG_BITFIELD:
          // We assume peer has all pieces (if not, we'll get rejects)
          if (unchoked) sendRequests();
          break;
        case MSG_PIECE: {
          if (payload.length < 8) break;
          const index = payload.readUInt32BE(0);
          const begin = payload.readUInt32BE(4);
          const block = payload.subarray(8);
          const key = `${index}:${begin}`;
          receivedPieces.set(key, Buffer.from(block));

          // Check if all done
          if (receivedPieces.size >= requests.length) {
            done(receivedPieces);
          }
          break;
        }
      }
    }

    function sendRequests() {
      if (!unchoked || !sock) return;

      // Break each request into BLOCK_SIZE sub-requests
      for (const req of requests) {
        let offset = req.begin;
        let remaining = req.length;
        while (remaining > 0) {
          const len = Math.min(remaining, BLOCK_SIZE);
          const msg = Buffer.alloc(17);
          msg.writeUInt32BE(13, 0); // length
          msg[4] = MSG_REQUEST;
          msg.writeUInt32BE(req.pieceIndex, 5);
          msg.writeUInt32BE(offset, 9);
          msg.writeUInt32BE(len, 13);
          sock!.write(msg);
          // Track sub-request with original begin for reassembly
          if (offset !== req.begin) {
            // Store sub-blocks that need to be combined later
          }
          offset += len;
          remaining -= len;
        }
      }
    }
  });
}
