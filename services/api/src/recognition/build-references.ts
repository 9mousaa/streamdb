/**
 * Reference frame builder — extracts phash frames from trailers
 * and stores them in the trailer_frames table for content matching.
 *
 * Runs as a background worker, processing content nodes that don't yet
 * have reference frames.
 */

import { pool } from '../db/pool.js';
import { logger } from '../utils/logger.js';
import { fetchTrailerUrl } from './trailerio.js';
import { extractFrames, computePhash } from './phash.js';

const PROCESS_INTERVAL = 60_000; // Process one content item per minute
let running = false;

export function startReferenceBuilder(): void {
  logger.info('Reference frame builder started');
  setInterval(() => processNext().catch(err =>
    logger.error('Reference builder tick failed', { error: err.message })
  ), PROCESS_INTERVAL);
}

async function processNext(): Promise<void> {
  if (running) return;
  running = true;

  try {
    // Find a content node with an IMDB ID but no trailer_frames
    const result = await pool.query(`
      SELECT c.id, c.imdb_id, c.type, c.title
      FROM content c
      LEFT JOIN trailer_frames tf ON tf.content_id = c.id
      WHERE c.imdb_id IS NOT NULL
        AND c.imdb_id LIKE 'tt%'
        AND tf.id IS NULL
      ORDER BY RANDOM()
      LIMIT 1
    `);

    if (!result.rows.length) return;

    const content = result.rows[0];
    const type = content.type === 'series' ? 'series' : 'movie';

    // Fetch trailer URL from Trailerio
    const trailerUrl = await fetchTrailerUrl(content.imdb_id, type);
    if (!trailerUrl) {
      // Insert a dummy record so we don't retry this content
      await pool.query(`
        INSERT INTO trailer_frames (content_id, phash, frame_ts_ms, source)
        VALUES ($1, $2, -1, 'no_trailer')
      `, [content.id, Buffer.alloc(8)]);
      logger.debug('No trailer found', { imdbId: content.imdb_id, title: content.title });
      return;
    }

    // Extract frames and compute phashes
    logger.debug('Extracting trailer frames', { imdbId: content.imdb_id, url: trailerUrl });
    const frames = await extractFrames(trailerUrl, 3, 30);

    if (frames.length === 0) {
      logger.debug('No frames extracted from trailer', { imdbId: content.imdb_id });
      return;
    }

    // Store phashes in database
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      for (let i = 0; i < frames.length; i++) {
        const phash = computePhash(frames[i]);
        const timestampMs = i * 3000; // 3 seconds apart

        await client.query(`
          INSERT INTO trailer_frames (content_id, phash, frame_ts_ms, source)
          VALUES ($1, $2, $3, 'trailerio')
        `, [content.id, phash, timestampMs]);
      }

      await client.query('COMMIT');
      logger.info('Built reference frames', {
        imdbId: content.imdb_id,
        title: content.title,
        frameCount: frames.length,
      });
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  } finally {
    running = false;
  }
}
