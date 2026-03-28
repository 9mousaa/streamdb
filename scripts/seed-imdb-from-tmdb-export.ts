#!/usr/bin/env npx tsx
/**
 * Fast IMDB Content Seeder — Uses TMDB daily file exports to get ALL movie and TV
 * show IDs, then fetches IMDB IDs in bulk.
 *
 * TMDB publishes daily exports at:
 *   https://files.tmdb.org/p/exports/movie_ids_MM_DD_YYYY.json.gz
 *   https://files.tmdb.org/p/exports/tv_series_ids_MM_DD_YYYY.json.gz
 *
 * Each line: {"id":123,"original_title":"...","popularity":45.6,"adult":false,"video":false}
 *
 * Strategy:
 * 1. Download the export, sort by popularity (highest first)
 * 2. Take top N items
 * 3. Batch fetch IMDB IDs via /movie/{id} or /tv/{id}
 * 4. Insert into content table
 *
 * Usage:
 *   TMDB_API_KEY=xxx npx tsx scripts/seed-imdb-from-tmdb-export.ts [--limit 500000]
 */

import pg from 'pg';
import { createGunzip } from 'zlib';
import { Readable } from 'stream';
import { createInterface } from 'readline';

const TMDB_BASE = 'https://api.themoviedb.org/3';
const TMDB_KEY = process.env.TMDB_API_KEY;
if (!TMDB_KEY) {
  console.error('TMDB_API_KEY required');
  process.exit(1);
}

const args = process.argv.slice(2);
const limitArg = args.indexOf('--limit');
const LIMIT = limitArg >= 0 ? parseInt(args[limitArg + 1]) : 500000;

const BATCH_SIZE = 40; // TMDB allows 40 req/s
const RATE_MS = 1100; // Slightly over 1s per batch to stay under limit

function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }

interface ExportItem {
  id: number;
  popularity: number;
  adult: boolean;
}

async function downloadExport(type: 'movie' | 'tv_series'): Promise<ExportItem[]> {
  // Try today and yesterday's exports
  const now = new Date();
  const dates = [now, new Date(now.getTime() - 86400000)];

  for (const d of dates) {
    const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(d.getUTCDate()).padStart(2, '0');
    const yyyy = d.getUTCFullYear();
    const url = `https://files.tmdb.org/p/exports/${type}_ids_${mm}_${dd}_${yyyy}.json.gz`;

    console.log(`Downloading: ${url}`);

    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(120000) });
      if (!res.ok) {
        console.log(`  HTTP ${res.status}, trying previous day...`);
        continue;
      }

      const items: ExportItem[] = [];
      const gunzip = createGunzip();
      // @ts-ignore - ReadableStream to Node stream
      const nodeStream = Readable.fromWeb(res.body as any);
      nodeStream.pipe(gunzip);

      const rl = createInterface({ input: gunzip });
      for await (const line of rl) {
        try {
          const obj = JSON.parse(line);
          if (!obj.adult) {
            items.push({ id: obj.id, popularity: obj.popularity || 0, adult: false });
          }
        } catch { /* skip malformed lines */ }
      }

      console.log(`  Loaded ${items.length.toLocaleString()} ${type} items`);
      return items;
    } catch (err: any) {
      console.log(`  Failed: ${err.message}`);
      continue;
    }
  }

  return [];
}

async function fetchTmdbDetails(type: 'movie' | 'tv', id: number): Promise<{
  imdb_id: string | null;
  title: string;
  year: number | null;
  runtime: number | null;
} | null> {
  try {
    const url = `${TMDB_BASE}/${type}/${id}?api_key=${TMDB_KEY}`;
    const res = await fetch(url, { signal: AbortSignal.timeout(10000) });
    if (!res.ok) return null;
    const data = await res.json() as any;

    const title = data.title || data.name || '';
    const dateStr = data.release_date || data.first_air_date || '';
    const year = dateStr ? parseInt(dateStr.substring(0, 4)) : null;
    const runtime = data.runtime || (data.episode_run_time?.[0]) || null;

    // For movies, imdb_id is in the response directly
    // For TV, need to fetch external_ids
    let imdb_id = data.imdb_id || null;
    if (!imdb_id && type === 'tv') {
      const extRes = await fetch(`${TMDB_BASE}/tv/${id}/external_ids?api_key=${TMDB_KEY}`, {
        signal: AbortSignal.timeout(10000),
      });
      if (extRes.ok) {
        const ext = await extRes.json() as any;
        imdb_id = ext.imdb_id || null;
      }
    }

    return { imdb_id, title, year, runtime };
  } catch {
    return null;
  }
}

async function main() {
  const dbUrl = process.env.DATABASE_URL || 'postgres://streamdb:streamdb_secret@localhost:5432/streamdb';
  const pool = new pg.Pool({ connectionString: dbUrl, max: 5 });

  // Download exports
  const movies = await downloadExport('movie');
  const shows = await downloadExport('tv_series');

  // Sort by popularity (descending) and take top items
  movies.sort((a, b) => b.popularity - a.popularity);
  shows.sort((a, b) => b.popularity - a.popularity);

  const halfLimit = Math.floor(LIMIT / 2);
  const movieTargets = movies.slice(0, halfLimit);
  const showTargets = shows.slice(0, halfLimit);

  console.log(`\nTargeting: ${movieTargets.length.toLocaleString()} movies + ${showTargets.length.toLocaleString()} shows`);

  const client = await pool.connect();
  let seeded = 0;
  let skipped = 0;

  try {
    await client.query('BEGIN');

    // Process movies in batches
    console.log('\n=== Processing Movies ===');
    for (let i = 0; i < movieTargets.length; i += BATCH_SIZE) {
      const batch = movieTargets.slice(i, i + BATCH_SIZE);
      const results = await Promise.all(batch.map(m => fetchTmdbDetails('movie', m.id)));

      for (let j = 0; j < results.length; j++) {
        const r = results[j];
        if (!r || !r.imdb_id || !r.imdb_id.startsWith('tt')) { skipped++; continue; }

        await client.query(`
          INSERT INTO content (imdb_id, tmdb_id, type, title, year)
          VALUES ($1, $2, 'movie', $3, $4)
          ON CONFLICT (imdb_id) DO UPDATE SET
            tmdb_id = COALESCE(EXCLUDED.tmdb_id, content.tmdb_id),
            title = COALESCE(EXCLUDED.title, content.title),
            year = COALESCE(EXCLUDED.year, content.year),
            updated_at = NOW()
        `, [r.imdb_id, batch[j].id, r.title, r.year]);
        seeded++;
      }

      await sleep(RATE_MS);

      if ((i / BATCH_SIZE) % 25 === 0) {
        await client.query('COMMIT');
        await client.query('BEGIN');
        const pct = ((i / movieTargets.length) * 100).toFixed(1);
        console.log(`  Movies: ${seeded.toLocaleString()} seeded, ${skipped.toLocaleString()} skipped (${pct}%)`);
      }
    }

    // Process shows in batches
    console.log('\n=== Processing TV Shows ===');
    for (let i = 0; i < showTargets.length; i += BATCH_SIZE) {
      const batch = showTargets.slice(i, i + BATCH_SIZE);
      const results = await Promise.all(batch.map(s => fetchTmdbDetails('tv', s.id)));

      for (let j = 0; j < results.length; j++) {
        const r = results[j];
        if (!r || !r.imdb_id || !r.imdb_id.startsWith('tt')) { skipped++; continue; }

        await client.query(`
          INSERT INTO content (imdb_id, tmdb_id, type, title, year)
          VALUES ($1, $2, 'series', $3, $4)
          ON CONFLICT (imdb_id) DO UPDATE SET
            tmdb_id = COALESCE(EXCLUDED.tmdb_id, content.tmdb_id),
            title = COALESCE(EXCLUDED.title, content.title),
            year = COALESCE(EXCLUDED.year, content.year),
            updated_at = NOW()
        `, [r.imdb_id, batch[j].id, r.title, r.year]);
        seeded++;
      }

      await sleep(RATE_MS);

      if ((i / BATCH_SIZE) % 25 === 0) {
        await client.query('COMMIT');
        await client.query('BEGIN');
        const pct = ((i / showTargets.length) * 100).toFixed(1);
        console.log(`  Shows: ${seeded.toLocaleString()} seeded (${pct}%)`);
      }
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }

  const stats = await pool.query(`
    SELECT
      (SELECT COUNT(*) FROM content WHERE type = 'movie') as movies,
      (SELECT COUNT(*) FROM content WHERE type = 'series') as shows,
      (SELECT COUNT(*) FROM content) as total,
      (SELECT COUNT(*) FROM content WHERE tmdb_id IS NOT NULL) as with_tmdb
  `);

  console.log('\n═══════════════════════════════════════');
  console.log(`Seeded: ${seeded.toLocaleString()}, Skipped: ${skipped.toLocaleString()}`);
  console.log('DB Stats:', stats.rows[0]);
  console.log('═══════════════════════════════════════');
  console.log('Reference frame builder will automatically process these content items.');

  await pool.end();
}

main().catch(err => {
  console.error('Seed failed:', err);
  process.exit(1);
});
