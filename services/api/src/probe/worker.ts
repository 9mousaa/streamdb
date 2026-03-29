import { pool } from '../db/pool.js';
import { logger } from '../utils/logger.js';
import { fetchMetadata, type Peer, type TorrentMetadata } from './torrent-metadata.js';
import { downloadPartial, downloadMiddleChunk } from './piece-downloader.js';
import { computeOsHash } from './oshash.js';
import { getDHTListener } from '../crawler/processor.js';
import { extractFrames, computePhash, hammingDistance } from '../recognition/phash.js';
import { probeUrl } from './ffprobe.js';
import { unrestrictTorBox } from '../debrid/manager.js';
import { config } from '../config.js';
import { writeFileSync, unlinkSync } from 'fs';
import type { PoolClient } from 'pg';

const VIDEO_EXTS = /\.(mkv|mp4|avi|wmv|flv|mov|webm|m4v|ts|mpg|mpeg)$/i;

export function startProbeWorker(intervalMs: number, concurrency = 1): void {
  logger.info('Probe worker started (torrent-based)', { intervalMs, concurrency });
  setInterval(() => {
    const jobs = Array.from({ length: concurrency }, () =>
      processNextJob().catch(err =>
        logger.error('Probe worker tick failed', { error: err.message })
      )
    );
    Promise.allSettled(jobs);
  }, intervalMs);
}

async function processNextJob(): Promise<void> {
  // Claim one job using SKIP LOCKED, respecting backoff via next_attempt_at
  const claim = await pool.query(`
    UPDATE probe_jobs SET status = 'processing', claimed_at = NOW()
    WHERE id = (
      SELECT id FROM probe_jobs
      WHERE status = 'pending' AND attempts < max_attempts
        AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
      ORDER BY priority DESC, created_at
      LIMIT 1
      FOR UPDATE SKIP LOCKED
    ) RETURNING *
  `);

  if (!claim.rows.length) return;

  const job = claim.rows[0];
  logger.debug('Claimed probe job', { id: job.id, infohash: job.infohash });

  try {
    // Step 1: Find peers via DHT
    const peers = await findPeers(job.infohash);
    if (peers.length === 0) {
      throw new Error('No peers found via DHT');
    }

    // Step 2: Fetch metadata via BEP-9
    const metadata = await fetchMetadata(job.infohash, peers);
    if (!metadata) {
      throw new Error('Failed to fetch torrent metadata from peers');
    }

    logger.debug('Got torrent metadata', {
      infohash: job.infohash,
      name: metadata.name,
      files: metadata.files.length,
      totalSize: metadata.totalSize,
    });

    // Find the best video file
    const videoFile = findBestVideoFile(metadata.files);

    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      // Find the file record
      const fileRes = await client.query(
        'SELECT id FROM files WHERE infohash = $1 ORDER BY file_idx LIMIT 1',
        [job.infohash]
      );
      if (!fileRes.rows.length) {
        throw new Error(`No file record for infohash ${job.infohash}`);
      }
      const fileId = fileRes.rows[0].id;

      // Update file with metadata from torrent
      const filename = videoFile ? videoFile.path : metadata.name;
      const fileSize = videoFile ? videoFile.size : metadata.totalSize;
      const parsed = parseTitleMetadata(filename);

      await client.query(`
        UPDATE files SET
          filename = COALESCE($1, filename),
          file_size = COALESCE($2, file_size),
          torrent_name = COALESCE($3, torrent_name),
          resolution = COALESCE($4, resolution),
          video_codec = COALESCE($5, video_codec),
          hdr = COALESCE($6, hdr),
          metadata_src = CASE WHEN metadata_src = 'dht' THEN 'torrent_meta' ELSE metadata_src END,
          confidence = GREATEST(confidence, 0.6),
          probed_at = NOW()
        WHERE id = $7
      `, [filename, fileSize, metadata.name, parsed.resolution, parsed.codec, parsed.hdr, fileId]);

      // Step 3: Real ffprobe via TorBox URL (only for files with content edges — avoid token waste)
      const hasEdge = await client.query(
        'SELECT 1 FROM content_files cf JOIN files f ON f.id = cf.file_id WHERE f.infohash = $1 LIMIT 1',
        [job.infohash]
      );
      const alreadyProbed = await client.query(
        "SELECT 1 FROM file_tracks WHERE file_id = $1 LIMIT 1", [fileId]
      );
      if (config.torboxApiKey && hasEdge.rows.length > 0 && alreadyProbed.rows.length === 0) {
        try {
          const tbUrl = await unrestrictTorBox(job.infohash, 0, config.torboxApiKey);
          if (tbUrl) {
            logger.debug('Running ffprobe via TorBox URL', { infohash: job.infohash });
            const probe = await probeUrl(tbUrl);

            // Update file with real probed metadata
            await client.query(`
              UPDATE files SET
                resolution = COALESCE($1, resolution),
                video_codec = COALESCE($2, video_codec),
                hdr = COALESCE($3, hdr),
                bit_depth = COALESCE($4, bit_depth),
                bitrate = COALESCE($5, bitrate),
                duration = COALESCE($6, duration),
                container = COALESCE($7, container),
                metadata_src = 'ffprobe',
                confidence = GREATEST(confidence, 0.95),
                probed_at = NOW()
              WHERE id = $8
            `, [probe.resolution, probe.video_codec, probe.hdr, probe.bit_depth,
                probe.bitrate, probe.duration, probe.container, fileId]);

            // Delete old tracks and insert real ones
            await client.query('DELETE FROM file_tracks WHERE file_id = $1', [fileId]);

            for (let ti = 0; ti < probe.audioTracks.length; ti++) {
              const t = probe.audioTracks[ti];
              await client.query(`
                INSERT INTO file_tracks (file_id, track_type, codec, channels, language, forced, is_default, track_index)
                VALUES ($1, 'audio', $2, $3, $4, $5, $6, $7)
              `, [fileId, t.codec, t.channels, t.language, t.forced, t.is_default, ti]);
            }

            for (let ti = 0; ti < probe.subtitleTracks.length; ti++) {
              const t = probe.subtitleTracks[ti];
              await client.query(`
                INSERT INTO file_tracks (file_id, track_type, codec, channels, language, sub_format, forced, is_default, track_index)
                VALUES ($1, 'subtitle', $2, $3, $4, $5, $6, $7, $8)
              `, [fileId, t.codec, t.channels, t.language, t.sub_format, t.forced, t.is_default, ti]);
            }

            logger.info('ffprobe completed', {
              infohash: job.infohash,
              resolution: probe.resolution,
              audioTracks: probe.audioTracks.length,
              subtitleTracks: probe.subtitleTracks.length,
            });
          }
        } catch (err: any) {
          logger.debug('ffprobe via TorBox failed (non-fatal)', { error: err.message });
        }
      }

      // Step 4: Try to download partial pieces for OS hash
      let osHash: string | null = null;
      try {
        const partial = await downloadPartial(job.infohash, metadata, peers);
        if (partial) {
          osHash = computeOsHash(partial.firstChunk, partial.lastChunk, partial.fileSize);
          await client.query('UPDATE files SET os_hash = $1 WHERE id = $2', [osHash, fileId]);
          logger.debug('Computed OS hash', { infohash: job.infohash, osHash });
        }
      } catch (err: any) {
        logger.debug('OS hash computation failed (non-fatal)', { error: err.message });
      }

      // Step 4: Check if file has a content_files edge — if not, try matching
      const edgeCheck = await client.query(
        'SELECT 1 FROM content_files cf JOIN files f ON f.id = cf.file_id WHERE f.infohash = $1 LIMIT 1',
        [job.infohash]
      );

      if (edgeCheck.rows.length === 0) {
        // Try phash matching against trailer reference frames
        await tryPhashMatch(client, fileId, job.infohash, metadata, peers);

        // Try duration matching as fallback
        const duration = fileSize && parsed.resolution ? estimateDuration(fileSize, parsed.resolution) : null;
        if (duration) {
          await tryDurationMatch(client, fileId, duration);
        }
      }

      // Mark job completed
      await client.query(
        "UPDATE probe_jobs SET status = 'completed', completed_at = NOW() WHERE id = $1",
        [job.id]
      );

      await client.query('COMMIT');
      logger.info('Probe completed', { infohash: job.infohash, name: metadata.name, osHash });
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  } catch (err: any) {
    logger.warn('Probe job failed', { id: job.id, infohash: job.infohash, error: err.message, attempts: job.attempts });
    // Exponential backoff: 5min, 10min, 20min, 40min...
    const backoffMinutes = Math.pow(2, job.attempts || 0) * 5;
    await pool.query(
      `UPDATE probe_jobs SET status = 'pending', attempts = attempts + 1,
       error = $1, next_attempt_at = NOW() + ($3 || ' minutes')::interval
       WHERE id = $2`,
      [err.message, job.id, String(backoffMinutes)]
    );

    // If max attempts reached, mark as failed
    await pool.query(
      "UPDATE probe_jobs SET status = 'failed' WHERE id = $1 AND attempts >= max_attempts",
      [job.id]
    );
  }
}

/**
 * Find peers for an infohash using the DHT network.
 * First checks cache, then does an iterative BEP-5 lookup (~10-20s).
 */
async function findPeers(infohash: string): Promise<Peer[]> {
  const dht = getDHTListener();
  if (!dht) return [];

  // Check if we already have cached peers from prior DHT activity
  const cached = dht.getPeersForHash(infohash);
  if (cached.length > 0) {
    logger.info('Found cached peers', { infohash: infohash.substring(0, 8), count: cached.length });
    return cached;
  }

  // Do a proper iterative DHT lookup (3-5 rounds, ~10-20s)
  const peers = await dht.findPeersForHash(infohash);
  if (peers.length > 0) {
    logger.info('Found peers via DHT lookup', { infohash: infohash.substring(0, 8), count: peers.length });
  } else {
    logger.info('No peers found for hash', { infohash: infohash.substring(0, 8) });
  }
  return peers;
}

function findBestVideoFile(files: { path: string; size: number }[]): { path: string; size: number } | null {
  let best: { path: string; size: number } | null = null;
  for (const f of files) {
    if (VIDEO_EXTS.test(f.path) && (!best || f.size > best.size)) {
      best = f;
    }
  }
  return best || (files.length === 1 ? files[0] : null);
}

function parseTitleMetadata(filename: string): {
  resolution: string | null;
  codec: string | null;
  hdr: string | null;
} {
  const t = filename;

  let resolution: string | null = null;
  if (/2160p|4k|uhd/i.test(t)) resolution = '2160p';
  else if (/1080p/i.test(t)) resolution = '1080p';
  else if (/720p/i.test(t)) resolution = '720p';
  else if (/480p/i.test(t)) resolution = '480p';

  let codec: string | null = null;
  if (/x265|hevc|h\.?265/i.test(t)) codec = 'x265';
  else if (/x264|h\.?264|avc/i.test(t)) codec = 'x264';
  else if (/av1/i.test(t)) codec = 'AV1';

  let hdr: string | null = null;
  if (/dolby[\.\s-]?vision|[\.\s]dv[\.\s]|dovi/i.test(t)) hdr = 'DV';
  else if (/hdr10\+|hdr10plus/i.test(t)) hdr = 'HDR10+';
  else if (/hdr10|hdr/i.test(t)) hdr = 'HDR10';

  return { resolution, codec, hdr };
}

/**
 * Try to match a file against trailer reference frames using phash.
 * Downloads a few frames from the torrent and compares against stored trailer phashes.
 */
async function tryPhashMatch(
  client: PoolClient, fileId: number, infohash: string,
  metadata: TorrentMetadata, peers: Peer[]
): Promise<void> {
  try {
    // Check if we have any reference frames at all
    const refCount = await client.query('SELECT COUNT(*)::int as c FROM trailer_frames WHERE source != $1', ['no_trailer']);
    if (refCount.rows[0].c === 0) return;

    // We need to get video data to extract frames. For now, try to find
    // a peer that can stream enough data for a few frames.
    // This is best-effort — will work when peers are available and responsive.
    logger.debug('Attempting phash match', { infohash });

    // Load all reference phashes into memory (they're small: 8 bytes each)
    const refs = await client.query(`
      SELECT tf.content_id, tf.phash FROM trailer_frames tf
      WHERE tf.source != 'no_trailer'
    `);

    if (refs.rows.length === 0) return;

    // Build a map of content_id → phash buffers
    const refMap = new Map<number, Buffer[]>();
    for (const row of refs.rows) {
      const list = refMap.get(row.content_id) || [];
      list.push(row.phash);
      refMap.set(row.content_id, list);
    }

    // Download ~2MB from the middle of the video for frame extraction
    const videoFile = findBestVideoFile(metadata.files);
    if (!videoFile || videoFile.size < 10_000_000) {
      logger.debug('Phash match: video too small or not found', { infohash });
      return;
    }

    let midData: Buffer | null = null;
    try {
      midData = await downloadMiddleChunk(infohash, metadata, peers);
    } catch (err: any) {
      logger.debug('Phash match: failed to download middle chunk', { error: err.message });
      return;
    }
    if (!midData || midData.length < 100_000) return;

    // Write to temp file, extract frames with ffmpeg, compare phashes
    const tmpFile = `/tmp/probe-${infohash.substring(0, 12)}.bin`;
    writeFileSync(tmpFile, midData);
    try {
      const frames = await extractFrames(tmpFile, 1, 10);
      if (frames.length === 0) return;

      for (const frame of frames) {
        const phash = computePhash(frame);
        let bestMatch: { contentId: number; distance: number } | null = null;
        for (const [contentId, refPhashes] of refMap) {
          for (const ref of refPhashes) {
            const dist = hammingDistance(phash, ref);
            if (dist < 10 && (!bestMatch || dist < bestMatch.distance)) {
              bestMatch = { contentId, distance: dist };
            }
          }
        }
        if (bestMatch) {
          const confidence = Math.max(0.5, 0.85 - bestMatch.distance * 0.01);
          await client.query(`
            INSERT INTO content_files (content_id, file_id, match_method, confidence)
            VALUES ($1, $2, 'phash', $3)
            ON CONFLICT DO NOTHING
          `, [bestMatch.contentId, fileId, confidence]);
          logger.info('Phash match found', {
            infohash, contentId: bestMatch.contentId, distance: bestMatch.distance,
          });
          return;
        }
      }
    } finally {
      try { unlinkSync(tmpFile); } catch {}
    }
  } catch (err: any) {
    logger.debug('Phash match failed (non-fatal)', { error: err.message });
  }
}

/**
 * Estimate video duration from file size and resolution.
 * Very rough — used as a signal, not a definitive match.
 */
function estimateDuration(fileSize: number, resolution: string): number | null {
  // Rough bitrate estimates (bytes/sec)
  const bitrates: Record<string, number> = {
    '2160p': 2_500_000, // ~20 Mbps
    '1080p': 1_000_000, // ~8 Mbps
    '720p':    500_000,  // ~4 Mbps
    '480p':    250_000,  // ~2 Mbps
  };
  const bps = bitrates[resolution];
  if (!bps) return null;
  return Math.round(fileSize / bps); // duration in seconds
}

/**
 * Try to match a file to content based on estimated duration.
 * Only creates edges with low confidence (0.3) since this is speculative.
 */
async function tryDurationMatch(client: PoolClient, fileId: number, estimatedDuration: number): Promise<void> {
  try {
    const durationMin = Math.round(estimatedDuration / 60);
    if (durationMin < 30 || durationMin > 300) return;

    // Match against content with known runtime (from TMDB enrichment)
    const matches = await client.query(`
      SELECT id FROM content
      WHERE type = 'movie' AND runtime_minutes IS NOT NULL
        AND ABS(runtime_minutes - $1) <= 3
      LIMIT 5
    `, [durationMin]);

    if (matches.rows.length === 1) {
      // Only create edge if exactly one match (unambiguous)
      await client.query(`
        INSERT INTO content_files (content_id, file_id, match_method, confidence)
        VALUES ($1, $2, 'duration', 0.3)
        ON CONFLICT DO NOTHING
      `, [matches.rows[0].id, fileId]);
      logger.info('Duration match found', { fileId, durationMin, contentId: matches.rows[0].id });
    }
  } catch (err: any) {
    logger.debug('Duration match failed (non-fatal)', { error: err.message });
  }
}

export async function getProbeStats(): Promise<Record<string, number>> {
  const result = await pool.query(`
    SELECT status, COUNT(*)::int as count FROM probe_jobs GROUP BY status
  `);
  const stats: Record<string, number> = { pending: 0, processing: 0, completed: 0, failed: 0 };
  for (const row of result.rows) {
    stats[row.status] = row.count;
  }
  return stats;
}
