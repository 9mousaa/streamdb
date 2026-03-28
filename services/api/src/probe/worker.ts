import { pool } from '../db/pool.js';
import { config } from '../config.js';
import { logger } from '../utils/logger.js';
import { probeUrl } from './ffprobe.js';
import { unrestrictRealDebrid } from '../debrid/manager.js';

export function startProbeWorker(intervalMs: number): void {
  logger.info('Probe worker started', { intervalMs });
  setInterval(() => processNextJob().catch(err =>
    logger.error('Probe worker tick failed', { error: err.message })
  ), intervalMs);
}

async function processNextJob(): Promise<void> {
  // Claim one job using SKIP LOCKED (safe for concurrent workers)
  const claim = await pool.query(`
    UPDATE probe_jobs SET status = 'processing', claimed_at = NOW()
    WHERE id = (
      SELECT id FROM probe_jobs
      WHERE status = 'pending' AND attempts < max_attempts
      ORDER BY priority DESC, created_at
      LIMIT 1
      FOR UPDATE SKIP LOCKED
    ) RETURNING *
  `);

  if (!claim.rows.length) return;

  const job = claim.rows[0];
  logger.debug('Claimed probe job', { id: job.id, infohash: job.infohash });

  try {
    // Get a CDN URL via RealDebrid
    const cdnUrl = await unrestrictRealDebrid(job.infohash, config.probeRdApiKey);
    if (!cdnUrl) {
      throw new Error('Failed to get CDN URL from RealDebrid');
    }

    // Probe the file
    const result = await probeUrl(cdnUrl);
    logger.debug('Probe result', { infohash: job.infohash, resolution: result.resolution, codec: result.video_codec });

    // Update file metadata in a transaction
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

      // Update file with probed metadata
      await client.query(`
        UPDATE files SET
          resolution = COALESCE($1, resolution),
          video_codec = COALESCE($2, video_codec),
          hdr = COALESCE($3, hdr),
          bit_depth = COALESCE($4, bit_depth),
          bitrate = COALESCE($5, bitrate),
          duration = COALESCE($6, duration),
          container = COALESCE($7, container),
          file_size = COALESCE($8, file_size),
          metadata_src = 'probe',
          confidence = 1.0,
          probed_at = NOW()
        WHERE id = $9
      `, [
        result.resolution, result.video_codec, result.hdr, result.bit_depth,
        result.bitrate, result.duration, result.container, result.file_size,
        fileId,
      ]);

      // Replace tracks with probed data
      await client.query('DELETE FROM file_tracks WHERE file_id = $1', [fileId]);

      for (const track of result.audioTracks) {
        await client.query(`
          INSERT INTO file_tracks (file_id, track_type, codec, channels, language, forced, is_default)
          VALUES ($1, 'audio', $2, $3, $4, $5, $6)
        `, [fileId, track.codec, track.channels, track.language, track.forced, track.is_default]);
      }

      for (const track of result.subtitleTracks) {
        await client.query(`
          INSERT INTO file_tracks (file_id, track_type, sub_format, language, forced, is_default)
          VALUES ($1, 'subtitle', $2, $3, $4, $5)
        `, [fileId, track.sub_format, track.language, track.forced, track.is_default]);
      }

      // Mark job completed
      await client.query(
        "UPDATE probe_jobs SET status = 'completed', completed_at = NOW() WHERE id = $1",
        [job.id]
      );

      await client.query('COMMIT');
      logger.info('Probe completed', { infohash: job.infohash, resolution: result.resolution });
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  } catch (err: any) {
    logger.warn('Probe job failed', { id: job.id, infohash: job.infohash, error: err.message });
    await pool.query(
      "UPDATE probe_jobs SET status = 'pending', attempts = attempts + 1, error = $1 WHERE id = $2",
      [err.message, job.id]
    );

    // If max attempts reached, mark as failed
    await pool.query(
      "UPDATE probe_jobs SET status = 'failed' WHERE id = $1 AND attempts >= max_attempts",
      [job.id]
    );
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
