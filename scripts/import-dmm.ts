#!/usr/bin/env npx tsx
/**
 * DMM (DebridMediaManager) hashlist importer
 * Imports JSON hashlists with structure: { "tt1234567": [{ "hash": "...", "filename": "..." }] }
 *
 * Usage: npx tsx scripts/import-dmm.ts ./dmm-hashlists/*.json
 *
 * DMM hashlists map IMDB IDs directly to infohashes with filenames.
 */

import { readFileSync } from 'fs';
import pg from 'pg';

const BATCH_SIZE = 500;

interface DMMEntry {
  hash?: string;
  filename?: string;
  filesize?: number;
  rd?: number; // 1 if cached on RD
}

// Simple title parser (no external deps)
function parseFromFilename(name: string): {
  resolution: string | null;
  codec: string | null;
  hdr: string | null;
  title: string;
  year: number | null;
  season: number | null;
  episode: number | null;
} {
  const t = name.toLowerCase();

  // Resolution
  let resolution: string | null = null;
  if (/2160p|4k|uhd/i.test(name)) resolution = '2160p';
  else if (/1080p/i.test(name)) resolution = '1080p';
  else if (/720p/i.test(name)) resolution = '720p';
  else if (/480p/i.test(name)) resolution = '480p';

  // Codec
  let codec: string | null = null;
  if (/x265|hevc|h\.?265/i.test(name)) codec = 'x265';
  else if (/x264|h\.?264|avc/i.test(name)) codec = 'x264';
  else if (/av1/i.test(name)) codec = 'AV1';

  // HDR
  let hdr: string | null = 'SDR';
  if (/dolby[\.\s-]?vision|[\.\s]dv[\.\s]|dovi/i.test(name)) hdr = 'DV';
  else if (/hdr10\+|hdr10plus/i.test(name)) hdr = 'HDR10+';
  else if (/hdr10|hdr/i.test(name)) hdr = 'HDR10';

  // Year
  const yearMatch = name.match(/[\.\s\(]?((?:19|20)\d{2})[\.\s\)]/);
  const year = yearMatch ? parseInt(yearMatch[1]) : null;

  // Season/Episode
  const seMatch = name.match(/S(\d{1,2})E(\d{1,3})/i);
  const season = seMatch ? parseInt(seMatch[1]) : null;
  const episode = seMatch ? parseInt(seMatch[2]) : null;

  // Clean title: take everything before year or resolution/quality tags
  let title = name.split(/[\.\s](?:(?:19|20)\d{2}|S\d{2}|2160p|1080p|720p|480p)/i)[0] || name;
  title = title.replace(/\./g, ' ').trim();

  return { resolution, codec, hdr, title, year, season, episode };
}

async function main() {
  const files = process.argv.slice(2);
  if (!files.length) {
    console.error('Usage: npx tsx scripts/import-dmm.ts <json-files...>');
    process.exit(1);
  }

  const dbUrl = process.env.DATABASE_URL || 'postgres://streamdb:streamdb_secret@localhost:5432/streamdb';
  const pool = new pg.Pool({ connectionString: dbUrl });
  const client = await pool.connect();

  let totalImported = 0;
  let totalSkipped = 0;

  try {
    for (const file of files) {
      console.log(`Processing: ${file}`);
      const raw = readFileSync(file, 'utf-8');
      let data: Record<string, DMMEntry[]>;
      try {
        data = JSON.parse(raw);
      } catch {
        console.error(`  Skipping invalid JSON: ${file}`);
        continue;
      }

      const imdbIds = Object.keys(data).filter(k => k.startsWith('tt'));
      console.log(`  Found ${imdbIds.length} IMDB entries`);

      await client.query('BEGIN');
      let batchCount = 0;

      for (const imdbId of imdbIds) {
        const entries = data[imdbId];
        if (!Array.isArray(entries)) continue;

        for (const entry of entries) {
          const hash = entry.hash?.toLowerCase();
          if (!hash || hash.length !== 40) { totalSkipped++; continue; }

          const filename = entry.filename || '';
          const parsed = parseFromFilename(filename);
          const isEpisode = parsed.season !== null && parsed.episode !== null;
          const contentType = isEpisode ? 'episode' : 'movie';

          // Upsert file
          await client.query(`
            INSERT INTO files (infohash, file_idx, filename, file_size, torrent_name, resolution, video_codec, hdr, metadata_src, confidence)
            VALUES ($1, 0, $2, $3, $4, $5, $6, $7, 'dmm_hashlist', 0.3)
            ON CONFLICT (infohash, file_idx) DO NOTHING
          `, [hash, filename, entry.filesize || null, filename, parsed.resolution, parsed.codec, parsed.hdr]);

          // Upsert content
          const contentKey = isEpisode ? `${imdbId}:${parsed.season}:${parsed.episode}` : imdbId;
          const contentResult = await client.query(`
            INSERT INTO content (imdb_id, type, title, year, season, episode, parent_imdb)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW()
            RETURNING id
          `, [contentKey, contentType, parsed.title, parsed.year, parsed.season, parsed.episode, isEpisode ? imdbId : null]);

          // Create edge
          const fileResult = await client.query('SELECT id FROM files WHERE infohash = $1 AND file_idx = 0', [hash]);
          if (fileResult.rows[0]) {
            await client.query(`
              INSERT INTO content_files (content_id, file_id, match_method, confidence)
              VALUES ($1, $2, 'dmm_hashlist', 0.7)
              ON CONFLICT DO NOTHING
            `, [contentResult.rows[0].id, fileResult.rows[0].id]);
          }

          // Queue probe job
          await client.query(`
            INSERT INTO probe_jobs (infohash, priority, source)
            VALUES ($1, 0, 'dmm_import')
            ON CONFLICT (infohash) DO NOTHING
          `, [hash]);

          totalImported++;
          batchCount++;

          if (batchCount >= BATCH_SIZE) {
            await client.query('COMMIT');
            await client.query('BEGIN');
            batchCount = 0;
            process.stdout.write(`\r  Imported: ${totalImported.toLocaleString()}`);
          }
        }
      }

      await client.query('COMMIT');
      console.log(`\n  File done. Running total: ${totalImported.toLocaleString()}`);
    }
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }

  console.log(`\nDone! Imported: ${totalImported.toLocaleString()}, Skipped: ${totalSkipped.toLocaleString()}`);

  const stats = await pool.query(`
    SELECT
      (SELECT COUNT(*) FROM content) as content_count,
      (SELECT COUNT(*) FROM files) as file_count,
      (SELECT COUNT(*) FROM content_files) as edge_count
  `);
  console.log('DB Stats:', stats.rows[0]);
  await pool.end();
}

main().catch(err => {
  console.error('Import failed:', err);
  process.exit(1);
});
