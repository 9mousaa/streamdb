/**
 * Perceptual hash (pHash) computation for video frames.
 *
 * Algorithm:
 * 1. Use ffmpeg to extract grayscale 32x32 frames from a video
 * 2. Compute 2D DCT on each 32x32 block
 * 3. Take top-left 8x8 DCT coefficients (low frequencies)
 * 4. Threshold at median → 64-bit hash
 *
 * Also provides Hamming distance comparison.
 * Zero external dependencies (DCT computed from scratch).
 */

import { execFile } from 'child_process';
import { logger } from '../utils/logger.js';

const FRAME_SIZE = 32; // 32x32 grayscale pixels per frame
const DCT_SIZE = 8;    // 8x8 low-frequency coefficients

/**
 * Extract grayscale 32x32 frames from a video URL at regular intervals.
 * Uses ffmpeg to decode, resize, and output raw grayscale pixels.
 */
export function extractFrames(videoUrl: string, intervalSec = 3, maxFrames = 30): Promise<Buffer[]> {
  return new Promise((resolve, reject) => {
    const args = [
      '-i', videoUrl,
      '-vf', `fps=1/${intervalSec},scale=${FRAME_SIZE}:${FRAME_SIZE}:flags=lanczos`,
      '-pix_fmt', 'gray',
      '-f', 'rawvideo',
      '-frames:v', String(maxFrames),
      'pipe:1',
    ];

    const proc = execFile('/usr/bin/ffmpeg', args, {
      maxBuffer: FRAME_SIZE * FRAME_SIZE * maxFrames * 2, // generous buffer
      timeout: 60000,
      encoding: 'buffer',
    }, (err, stdout) => {
      if (err) {
        reject(err);
        return;
      }

      const raw = stdout as unknown as Buffer;
      const frameBytes = FRAME_SIZE * FRAME_SIZE;
      const frames: Buffer[] = [];

      for (let i = 0; i + frameBytes <= raw.length; i += frameBytes) {
        frames.push(raw.subarray(i, i + frameBytes));
      }

      resolve(frames);
    });
  });
}

/**
 * Compute perceptual hash of a 32x32 grayscale frame.
 * Returns an 8-byte Buffer (64-bit hash).
 */
export function computePhash(frame: Buffer): Buffer {
  if (frame.length !== FRAME_SIZE * FRAME_SIZE) {
    throw new Error(`Frame must be ${FRAME_SIZE}x${FRAME_SIZE} grayscale`);
  }

  // Convert to 2D float array
  const pixels: number[][] = [];
  for (let y = 0; y < FRAME_SIZE; y++) {
    const row: number[] = [];
    for (let x = 0; x < FRAME_SIZE; x++) {
      row.push(frame[y * FRAME_SIZE + x]);
    }
    pixels.push(row);
  }

  // Compute 2D DCT
  const dct = dct2d(pixels);

  // Take top-left 8x8 coefficients (excluding DC component at [0,0])
  const coeffs: number[] = [];
  for (let y = 0; y < DCT_SIZE; y++) {
    for (let x = 0; x < DCT_SIZE; x++) {
      if (y === 0 && x === 0) continue; // Skip DC
      coeffs.push(dct[y][x]);
    }
  }

  // Compute median
  const sorted = [...coeffs].sort((a, b) => a - b);
  const median = sorted[Math.floor(sorted.length / 2)];

  // Threshold: 1 if above median, 0 if below → 63 bits (pad to 64)
  const hash = Buffer.alloc(8);
  let bitIndex = 0;
  for (let y = 0; y < DCT_SIZE; y++) {
    for (let x = 0; x < DCT_SIZE; x++) {
      if (y === 0 && x === 0) continue;
      if (bitIndex >= 64) break;
      if (dct[y][x] > median) {
        const byteIdx = Math.floor(bitIndex / 8);
        const bitIdx = 7 - (bitIndex % 8);
        hash[byteIdx] |= (1 << bitIdx);
      }
      bitIndex++;
    }
  }

  return hash;
}

/**
 * Hamming distance between two phashes.
 * 0 = identical, 64 = completely different.
 */
export function hammingDistance(a: Buffer, b: Buffer): number {
  if (a.length !== 8 || b.length !== 8) return 64;

  let dist = 0;
  for (let i = 0; i < 8; i++) {
    let xor = a[i] ^ b[i];
    while (xor) {
      dist += xor & 1;
      xor >>= 1;
    }
  }
  return dist;
}

// ---- 2D DCT implementation (from scratch) ----

// Precompute cosine table
const cosTable: number[][] = [];
for (let i = 0; i < FRAME_SIZE; i++) {
  cosTable[i] = [];
  for (let j = 0; j < FRAME_SIZE; j++) {
    cosTable[i][j] = Math.cos((Math.PI / FRAME_SIZE) * (j + 0.5) * i);
  }
}

function dct1d(row: number[]): number[] {
  const N = row.length;
  const result: number[] = new Array(N);
  for (let k = 0; k < N; k++) {
    let sum = 0;
    for (let n = 0; n < N; n++) {
      sum += row[n] * cosTable[k][n];
    }
    result[k] = sum * (k === 0 ? 1 / Math.sqrt(N) : Math.sqrt(2 / N));
  }
  return result;
}

function dct2d(pixels: number[][]): number[][] {
  const N = pixels.length;

  // DCT on rows
  const rowDct: number[][] = [];
  for (let y = 0; y < N; y++) {
    rowDct.push(dct1d(pixels[y]));
  }

  // DCT on columns
  const result: number[][] = [];
  for (let y = 0; y < N; y++) {
    result.push(new Array(N));
  }

  for (let x = 0; x < N; x++) {
    const col: number[] = [];
    for (let y = 0; y < N; y++) {
      col.push(rowDct[y][x]);
    }
    const dctCol = dct1d(col);
    for (let y = 0; y < N; y++) {
      result[y][x] = dctCol[y];
    }
  }

  return result;
}
