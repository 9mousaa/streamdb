#!/usr/bin/env npx tsx
/**
 * Torrentio-style hashlist importer
 * Imports newline-delimited JSON (NDJSON) files with structure:
 * { "imdb": "tt1234567", "hash": "abc123...", "title": "Movie.Name.2024.1080p.BluRay.x264", "size": 12345678 }
 *
 * Usage: npx tsx scripts/import-torrentio.ts ./hashlist.ndjson
 *
 * Also supports CSV format: imdb_id,infohash,title,size
 */

import { createReadStream } from 'fs';
import { createInterface } from 'readline';
import pg from 'pg';

const BATCH_SIZE = 500;

function parseFromTitle(name: string): {
  resolution: string | null;
  codec: string | null;
  hdr: string | null;
  cleanTitle: string;
  year: number | null;
  season: number | null;
  episode: number | null;
} {
  let resolution: string | null = null;
  if (/2160p|4k|uhd/i.test(name)) resolution = '2160p';
  else if (/1080p/i.test(name)) resolution = '1080p';
  else if (/720p/i.test(name)) resolution = '720p';
  else if (/480p/i.test(name)) resolution = '480p';

  let codec: string | null = null;
  if (/x265|hevc|h\.?265/i.test(name)) codec = 'x265';
  else if (/x264|h\.?264|avc/i.test(name)) codec = 'x264';
  else if (/av1/i.test(name)) codec = 'AV1';

  let hdr: string | null = 'SDR';
  if (/dolby[\.\s-]?vision|[\.\s]dv[\.\s]|dovi/i.test(name)) hdr = 'DV';
  else if (/hdr10\+|hdr10plus/i.test(name)) hdr = 'HDR10+';
  else if (/hdr10|hdr/i.test(name)) hdr = 'HDR10';

  const yearMatch = name.match(/[\.\s\(]?((?:19|20)\d{2})[\.\s\)]/);
  const year = yearMatch ? parseInt(yearMatch[1]) : null;

  const seMatch = name.match(/S(\d{1,2})E(\d{1,3})/i);
  const season = seMatch ? parseInt(seMatch[1]) : null;
  const episode = seMatch ? parseInt(seMatch[2]) : null;

  let cleanTitle = name.split(/[\.\s](?:(?:19|20)\d{2}|S\d{2}|2160p|1080p|720p|480p)/i)[0] || name;
  cleanTitle = cleanTitle.replace(/\./g, ' ').trim();

  return { resolution, codec, hdr, cleanTitle, year, season, episode };
}

interface HashEntry {
  imdb: string;
  hash: string;
  title: string;
  size?: number;
}

function parseLine(line: string): HashEntry | null {
  line = line.trim();
  if (!line) return null;

  // Try JSON
  if (line.startsWith('{')) {
    try {
      const obj = JSON.parse(line);
      const imdb = obj.imdb || obj.imdb_id || obj.imdbId;
      const hash = obj.hash || obj.infohash || obj.infoHash;
      const title = obj.title || obj.name || obj.filename || '';
      const size = obj.size || obj.filesize || null;
      if (imdb && hash) return { imdb, hash, title, size };
    } catch {}
    return null;
  }

  // Try CSV: imdb_id,infohash,title,size
  const parts = line.split(',');
  if (parts.length >= 2 && parts[0].startsWith('tt')) {
    return {
      imdb: parts[0].trim(),
      hash: parts[1].trim(),
      title: parts[2]?.trim() || '',
      size: parts[3] ? parseInt(parts[3]) : undefined,
    };
  }

  return null;
}

async function main() {
  const filePath = process.argv[2];
  if (!filePath) {
    console.error('Usage: npx tsx scripts/import-torrentio.ts <ndjson-or-csv-file>');
    process.exit(1);
  }

  const dbUrl = process.env.DATABASE_URL || 'postgres://streamdb:streamdb_secret@localhost:5432/streamdb';
  const pool = new pg.Pool({ connectionString: dbUrl });
  const client = await pool.connect();

  let imported = 0;
  let skipped = 0;
  let batchCount = 0;

  const rl = createInterface({ input: createReadStream(filePath), crlfDelay: Infinity });

  try {
    await client.query('BEGIN');

    for await (const line of rl) {
      const entry = parseLine(line);
      if (!entry) { skipped++; continue; }

      const hash = entry.hash.toLowerCase();
      if (hash.length !== 40) { skipped++; continue; }
      if (!entry.imdb.startsWith('tt')) { skipped++; continue; }

      const parsed = parseFromTitle(entry.title);
      const isEpisode = parsed.season !== null && parsed.episode !== null;
      const contentType = isEpisode ? 'episode' : 'movie';

      // Upsert file
      await client.query(`
        INSERT INTO files (infohash, file_idx, filename, file_size, torrent_name, resolution, video_codec, hdr, metadata_src, confidence)
        VALUES ($1, 0, $2, $3, $4, $5, $6, $7, 'torrentio', 0.3)
        ON CONFLICT (infohash, file_idx) DO NOTHING
      `, [hash, entry.title, entry.size || null, entry.title, parsed.resolution, parsed.codec, parsed.hdr]);

      // Upsert content
      const contentKey = isEpisode ? `${entry.imdb}:${parsed.season}:${parsed.episode}` : entry.imdb;
      const contentResult = await client.query(`
        INSERT INTO content (imdb_id, type, title, year, season, episode, parent_imdb)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW()
        RETURNING id
      `, [contentKey, contentType, parsed.cleanTitle, parsed.year, parsed.season, parsed.episode, isEpisode ? entry.imdb : null]);

      // Create edge
      const fileResult = await client.query('SELECT id FROM files WHERE infohash = $1 AND file_idx = 0', [hash]);
      if (fileResult.rows[0]) {
        await client.query(`
          INSERT INTO content_files (content_id, file_id, match_method, confidence)
          VALUES ($1, $2, 'torrentio', 0.7)
          ON CONFLICT DO NOTHING
        `, [contentResult.rows[0].id, fileResult.rows[0].id]);
      }

      // Queue probe job
      await client.query(`
        INSERT INTO probe_jobs (infohash, priority, source)
        VALUES ($1, 0, 'torrentio_import')
        ON CONFLICT (infohash) DO NOTHING
      `, [hash]);

      imported++;
      batchCount++;

      if (batchCount >= BATCH_SIZE) {
        await client.query('COMMIT');
        await client.query('BEGIN');
        batchCount = 0;
        process.stdout.write(`\r  Imported: ${imported.toLocaleString()}, Skipped: ${skipped.toLocaleString()}`);
      }
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }

  console.log(`\nDone! Imported: ${imported.toLocaleString()}, Skipped: ${skipped.toLocaleString()}`);

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
