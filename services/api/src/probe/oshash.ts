/**
 * OpenSubtitles Hash (os_hash) computation.
 * Algorithm: sum of uint64 little-endian values from first 64KB + last 64KB + file size.
 * Result: 16-char hex string.
 *
 * Zero dependencies.
 */

const CHUNK_SIZE = 65536; // 64KB

/**
 * Compute OpenSubtitles hash from first/last 64KB chunks and file size.
 */
export function computeOsHash(firstChunk: Buffer, lastChunk: Buffer, fileSize: number): string {
  if (firstChunk.length < CHUNK_SIZE || lastChunk.length < CHUNK_SIZE) {
    throw new Error(`Chunks must be at least ${CHUNK_SIZE} bytes`);
  }

  // Start with file size as a 64-bit value
  let lo = fileSize & 0xffffffff;
  let hi = Math.floor(fileSize / 0x100000000) & 0xffffffff;

  // Sum uint64 LE values from first 64KB (8192 uint64s)
  for (let i = 0; i < CHUNK_SIZE; i += 8) {
    const vLo = firstChunk.readUInt32LE(i);
    const vHi = firstChunk.readUInt32LE(i + 4);
    lo = (lo + vLo) >>> 0;
    // Carry
    if (lo < vLo) hi = (hi + 1) >>> 0;
    hi = (hi + vHi) >>> 0;
  }

  // Sum uint64 LE values from last 64KB
  for (let i = 0; i < CHUNK_SIZE; i += 8) {
    const vLo = lastChunk.readUInt32LE(i);
    const vHi = lastChunk.readUInt32LE(i + 4);
    lo = (lo + vLo) >>> 0;
    if (lo < vLo) hi = (hi + 1) >>> 0;
    hi = (hi + vHi) >>> 0;
  }

  // Format as 16-char hex: high 32 bits first, then low 32 bits
  return hi.toString(16).padStart(8, '0') + lo.toString(16).padStart(8, '0');
}
