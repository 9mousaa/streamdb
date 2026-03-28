#!/usr/bin/env npx tsx
/**
 * RARBG SQLite dump importer
 * Reads rarbg_db.sqlite and imports into StreamDB PostgreSQL
 *
 * Usage: npx tsx scripts/import-rarbg.ts ./rarbg_db.sqlite
 *
 * The RARBG dump has a table `items` with columns:
 *   hash, title, dt, cat, size, ext_ids (JSON with imdb), imdb
 *
 * ~2.8M entries, ~826K with IMDB IDs
 */

import Database from 'better-sqlite3';
import pg from 'pg';
import { parseTitle } from 'parse-torrent-title';

const BATCH_SIZE = 1000;

// RARBG categories that are video content
const VIDEO_CATS = new Set([
  'Movies/XVID',
  'Movies/XVID/720',
  'Movies/x264',
  'Movies/x264/1080',
  'Movies/x264/720',
  'Movies/x264/3D',
  'Movies/x264/4k',
  'Movies/x265/4k',
  'Movies/x265/4k/HDR',
  'Movies/BD Remux',
  'Movies/Full BD',
  'Movs/x264',
  'Movs/x264/1080',
  'Movs/x264/720',
  'Movs/x264/3D',
  'Movs/x264/4k',
  'Movs/x265/4k',
  'Movs/x265/4k/HDR',
  'TV Episodes',
  'TV HD Episodes',
  'TV UHD Episodes',
]);

interface RarbgItem {
  hash: string;
  title: string;
  dt: string;
  cat: string;
  size: number;
  imdb: string | null;
}

function detectResolution(title: string, cat: string): string | null {
  const parsed = parseTitle(title);
  if (parsed.resolution) return parsed.resolution;
  if (cat.includes('4k') || cat.includes('UHD')) return '2160p';
  if (cat.includes('1080')) return '1080p';
  if (cat.includes('720')) return '720p';
  return null;
}

function detectCodec(title: string): string | null {
  const parsed = parseTitle(title);
  if (parsed.codec) return parsed.codec;
  const t = title.toLowerCase();
  if (t.includes('x265') || t.includes('hevc') || t.includes('h.265') || t.includes('h265')) return 'x265';
  if (t.includes('x264') || t.includes('h.264') || t.includes('h264') || t.includes('avc')) return 'x264';
  if (t.includes('av1')) return 'AV1';
  if (t.includes('xvid')) return 'XviD';
  return null;
}

function detectHDR(title: string, cat: string): string | null {
  const t = title.toLowerCase();
  if (t.includes('dolby.vision') || t.includes('dv') || t.includes('dovi')) return 'DV';
  if (t.includes('hdr10+') || t.includes('hdr10plus')) return 'HDR10+';
  if (t.includes('hdr10') || t.includes('hdr')) return 'HDR10';
  if (cat.includes('HDR')) return 'HDR10';
  return 'SDR';
}

function detectType(cat: string): 'movie' | 'series' {
  if (cat.startsWith('TV')) return 'series';
  return 'movie';
}

async function main() {
  const sqlitePath = process.argv[2];
  if (!sqlitePath) {
    console.error('Usage: npx tsx scripts/import-rarbg.ts <path-to-rarbg_db.sqlite>');
    process.exit(1);
  }

  const dbUrl = process.env.DATABASE_URL || 'postgres://streamdb:streamdb_secret@localhost:5432/streamdb';
  const pool = new pg.Pool({ connectionString: dbUrl });

  console.log(`Opening SQLite: ${sqlitePath}`);
  const sqlite = new Database(sqlitePath, { readonly: true });

  // Count total rows
  const { total } = sqlite.prepare('SELECT COUNT(*) as total FROM items').get() as { total: number };
  console.log(`Total RARBG items: ${total.toLocaleString()}`);

  const countWithImdb = sqlite.prepare("SELECT COUNT(*) as c FROM items WHERE imdb IS NOT NULL AND imdb != ''").get() as { c: number };
  console.log(`Items with IMDB: ${countWithImdb.c.toLocaleString()}`);

  // Process in batches
  const stmt = sqlite.prepare('SELECT hash, title, dt, cat, size, imdb FROM items LIMIT ? OFFSET ?');

  let imported = 0;
  let skipped = 0;
  let offset = 0;

  const client = await pool.connect();

  try {
    // Prepare upsert statements
    await client.query('BEGIN');

    while (offset < total) {
      const rows = stmt.all(BATCH_SIZE, offset) as RarbgItem[];
      if (rows.length === 0) break;

      for (const row of rows) {
        const hash = row.hash?.toLowerCase();
        if (!hash || hash.length !== 40) { skipped++; continue; }

        const type = detectType(row.cat);
        const resolution = detectResolution(row.title, row.cat);
        const codec = detectCodec(row.title);
        const hdr = detectHDR(row.title, row.cat);
        const parsed = parseTitle(row.title);

        // Insert file
        await client.query(`
          INSERT INTO files (infohash, file_idx, filename, file_size, torrent_name, resolution, video_codec, hdr, metadata_src, confidence)
          VALUES ($1, 0, $2, $3, $4, $5, $6, $7, 'rarbg_dump', 0.3)
          ON CONFLICT (infohash, file_idx) DO NOTHING
        `, [hash, row.title, row.size, row.title, resolution, codec, hdr]);

        // If we have IMDB, create content + edge
        if (row.imdb && row.imdb.startsWith('tt')) {
          const title = parsed.title || row.title.split('.').join(' ').substring(0, 200);
          const year = parsed.year || null;
          const season = parsed.season || null;
          const episode = parsed.episode || null;

          let contentType: string = type;
          let parentImdb: string | null = null;

          if (type === 'series' && season && episode) {
            contentType = 'episode';
            parentImdb = row.imdb;
          }

          // Upsert content
          const contentResult = await client.query(`
            INSERT INTO content (imdb_id, type, title, year, season, episode, parent_imdb)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW()
            RETURNING id
          `, [
            contentType === 'episode' ? `${row.imdb}:${season}:${episode}` : row.imdb,
            contentType, title, year, season, episode, parentImdb
          ]);

          const contentId = contentResult.rows[0].id;

          // Get file id
          const fileResult = await client.query(
            'SELECT id FROM files WHERE infohash = $1 AND file_idx = 0',
            [hash]
          );

          if (fileResult.rows[0]) {
            await client.query(`
              INSERT INTO content_files (content_id, file_id, match_method, confidence)
              VALUES ($1, $2, 'rarbg_dump', 0.7)
              ON CONFLICT DO NOTHING
            `, [contentId, fileResult.rows[0].id]);
          }
        }

        imported++;
      }

      // Commit every 10K rows
      if (imported % 10000 < BATCH_SIZE) {
        await client.query('COMMIT');
        await client.query('BEGIN');
        const pct = ((offset / total) * 100).toFixed(1);
        console.log(`Progress: ${imported.toLocaleString()} imported, ${skipped.toLocaleString()} skipped (${pct}%)`);
      }

      offset += BATCH_SIZE;
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }

  console.log(`\nDone! Imported: ${imported.toLocaleString()}, Skipped: ${skipped.toLocaleString()}`);

  // Print stats
  const stats = await pool.query(`
    SELECT
      (SELECT COUNT(*) FROM content) as content_count,
      (SELECT COUNT(*) FROM files) as file_count,
      (SELECT COUNT(*) FROM content_files) as edge_count
  `);
  console.log('DB Stats:', stats.rows[0]);

  sqlite.close();
  await pool.end();
}

main().catch(err => {
  console.error('Import failed:', err);
  process.exit(1);
});
