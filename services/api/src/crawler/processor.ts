/**
 * DHT infohash processor — takes discovered infohashes from the DHT listener
 * and records them in the database for future probing.
 *
 * Since we only get infohashes from DHT (no metadata), we can only:
 * 1. Record the infohash in the files table
 * 2. Queue a probe job to extract metadata later via debrid
 *
 * Content matching happens after probing (or when the user requests a stream
 * and their debrid service recognizes the hash).
 */

import { pool } from '../db/pool.js';
import { logger } from '../utils/logger.js';
import { DHTListener } from './dht.js';
import { config } from '../config.js';

const FLUSH_INTERVAL = 30_000; // Flush to DB every 30s
const MAX_BATCH = 500;

let pendingHashes: string[] = [];
let dhtListener: DHTListener | null = null;

export function startDHTCrawler(): void {
  logger.info('Starting DHT crawler', { port: config.dhtPort });

  dhtListener = new DHTListener(config.dhtPort, (infohash) => {
    pendingHashes.push(infohash);
  });

  // Periodically flush discovered hashes to database
  setInterval(() => flushToDatabase().catch(err =>
    logger.error('DHT flush failed', { error: err.message })
  ), FLUSH_INTERVAL);

  // Log stats periodically
  setInterval(() => {
    if (dhtListener) {
      const stats = dhtListener.getStats();
      logger.info('DHT stats', stats);
    }
  }, 60_000);
}

async function flushToDatabase(): Promise<void> {
  if (!pendingHashes.length) return;

  // Take up to MAX_BATCH hashes
  const batch = pendingHashes.splice(0, MAX_BATCH);
  const unique = [...new Set(batch)];

  if (!unique.length) return;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    let inserted = 0;
    for (const hash of unique) {
      // Only insert if this infohash doesn't already exist
      const result = await client.query(`
        INSERT INTO files (infohash, file_idx, metadata_src, confidence)
        VALUES ($1, 0, 'dht', 0.0)
        ON CONFLICT (infohash, file_idx) DO NOTHING
        RETURNING id
      `, [hash]);

      if (result.rows.length > 0) {
        // New file — queue for probing
        await client.query(`
          INSERT INTO probe_jobs (infohash, priority, source)
          VALUES ($1, 0, 'dht')
          ON CONFLICT (infohash) DO NOTHING
        `, [hash]);
        inserted++;
      }
    }

    await client.query('COMMIT');

    if (inserted > 0) {
      logger.debug('DHT flush', { checked: unique.length, newFiles: inserted });
    }
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

export function getDHTStats(): { routingTableSize: number; infohashesDiscovered: number; pendingFlush: number } | null {
  if (!dhtListener) return null;
  return {
    ...dhtListener.getStats(),
    pendingFlush: pendingHashes.length,
  };
}
