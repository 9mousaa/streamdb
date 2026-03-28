#!/usr/bin/env npx tsx
/**
 * IMDB Content Seeder — Populates the content table with top-rated and newest
 * movies and TV shows from TMDB, including their IMDB IDs.
 *
 * This creates content nodes that the reference frame builder will then
 * automatically process (fetching trailers, computing phashes).
 *
 * Uses TMDB discover API with multiple sort strategies:
 * - popularity.desc (most popular)
 * - primary_release_date.desc (newest)
 * - vote_count.desc (most voted = most well-known)
 *
 * TMDB allows 40 req/s, page limit is 500 (10,000 per query).
 * With different year ranges + sort combos, we can cover ~500K items.
 *
 * Usage:
 *   TMDB_API_KEY=xxx npx tsx scripts/seed-imdb-content.ts [--movies] [--shows] [--limit N]
 */

import pg from 'pg';

const TMDB_BASE = 'https://api.themoviedb.org/3';
const RESULTS_PER_PAGE = 20;
const MAX_PAGE = 500; // TMDB hard limit
const RATE_LIMIT_MS = 30; // ~33 req/s, under 40 limit

const TMDB_KEY = process.env.TMDB_API_KEY;
if (!TMDB_KEY) {
  console.error('TMDB_API_KEY required');
  process.exit(1);
}

const args = process.argv.slice(2);
const doMovies = args.includes('--movies') || (!args.includes('--shows'));
const doShows = args.includes('--shows') || (!args.includes('--movies'));
const limitArg = args.indexOf('--limit');
const targetLimit = limitArg >= 0 ? parseInt(args[limitArg + 1]) : 500000;

function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }

async function tmdbFetch(path: string): Promise<any> {
  await sleep(RATE_LIMIT_MS);
  const url = `${TMDB_BASE}${path}${path.includes('?') ? '&' : '?'}api_key=${TMDB_KEY}`;
  const res = await fetch(url, {
    headers: { 'User-Agent': 'StreamDB/1.0' },
    signal: AbortSignal.timeout(15000),
  });
  if (res.status === 429) {
    console.log('Rate limited, waiting 10s...');
    await sleep(10000);
    return tmdbFetch(path);
  }
  if (!res.ok) return null;
  return res.json();
}

interface DiscoverResult {
  id: number;
  title?: string;
  name?: string;
  release_date?: string;
  first_air_date?: string;
  vote_average?: number;
  popularity?: number;
}

async function discoverAndSeed(
  client: pg.PoolClient,
  type: 'movie' | 'tv',
  sortBy: string,
  yearFrom: number,
  yearTo: number,
  maxPages: number,
): Promise<number> {
  let seeded = 0;
  const dateField = type === 'movie' ? 'primary_release_date' : 'first_air_date';

  for (let page = 1; page <= maxPages; page++) {
    const path = `/discover/${type}?sort_by=${sortBy}&${dateField}.gte=${yearFrom}-01-01&${dateField}.lte=${yearTo}-12-31&page=${page}`;
    const data = await tmdbFetch(path);
    if (!data?.results?.length) break;

    for (const item of data.results as DiscoverResult[]) {
      // Get IMDB ID via TMDB external IDs
      const extPath = `/${type}/${item.id}/external_ids`;
      const ext = await tmdbFetch(extPath);
      const imdbId = ext?.imdb_id;

      if (!imdbId || !imdbId.startsWith('tt')) continue;

      const title = item.title || item.name || '';
      const dateStr = item.release_date || item.first_air_date || '';
      const year = dateStr ? parseInt(dateStr.substring(0, 4)) : null;
      const contentType = type === 'tv' ? 'series' : 'movie';

      await client.query(`
        INSERT INTO content (imdb_id, tmdb_id, type, title, year)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (imdb_id) DO UPDATE SET
          tmdb_id = COALESCE(EXCLUDED.tmdb_id, content.tmdb_id),
          title = COALESCE(EXCLUDED.title, content.title),
          year = COALESCE(EXCLUDED.year, content.year),
          updated_at = NOW()
      `, [imdbId, item.id, contentType, title, year]);

      seeded++;
    }

    if (page % 10 === 0) {
      await client.query('COMMIT');
      await client.query('BEGIN');
      console.log(`  ${type} ${sortBy} ${yearFrom}-${yearTo}: page ${page}/${maxPages}, seeded ${seeded}`);
    }

    // Check if we've reached the end
    if (page >= (data.total_pages || 0)) break;
  }

  return seeded;
}

async function main() {
  const dbUrl = process.env.DATABASE_URL || 'postgres://streamdb:streamdb_secret@localhost:5432/streamdb';
  const pool = new pg.Pool({ connectionString: dbUrl, max: 3 });
  const client = await pool.connect();

  let totalSeeded = 0;

  try {
    await client.query('BEGIN');

    if (doMovies) {
      console.log('\n=== MOVIES ===');

      // Strategy 1: Most popular movies (all time)
      console.log('\n-- Most Popular (all time) --');
      totalSeeded += await discoverAndSeed(client, 'movie', 'popularity.desc', 1900, 2026, MAX_PAGE);

      // Strategy 2: Most voted movies by decade (covers "top" content)
      for (const decade of [2020, 2010, 2000, 1990, 1980, 1970]) {
        console.log(`\n-- Most Voted ${decade}s --`);
        const from = decade;
        const to = Math.min(decade + 9, 2026);
        totalSeeded += await discoverAndSeed(client, 'movie', 'vote_count.desc', from, to, MAX_PAGE);
        if (totalSeeded >= targetLimit / 2) break;
      }

      // Strategy 3: Newest movies (2024-2026)
      console.log('\n-- Newest (2024-2026) --');
      totalSeeded += await discoverAndSeed(client, 'movie', 'primary_release_date.desc', 2024, 2026, MAX_PAGE);
    }

    if (doShows) {
      console.log('\n=== TV SHOWS ===');

      // Strategy 1: Most popular shows
      console.log('\n-- Most Popular (all time) --');
      totalSeeded += await discoverAndSeed(client, 'tv', 'popularity.desc', 1950, 2026, MAX_PAGE);

      // Strategy 2: Most voted shows by decade
      for (const decade of [2020, 2010, 2000, 1990]) {
        console.log(`\n-- Most Voted ${decade}s --`);
        const from = decade;
        const to = Math.min(decade + 9, 2026);
        totalSeeded += await discoverAndSeed(client, 'tv', 'vote_count.desc', from, to, MAX_PAGE);
        if (totalSeeded >= targetLimit) break;
      }

      // Strategy 3: Newest shows
      console.log('\n-- Newest (2024-2026) --');
      totalSeeded += await discoverAndSeed(client, 'tv', 'first_air_date.desc', 2024, 2026, MAX_PAGE);
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }

  // Stats
  const stats = await pool.query(`
    SELECT
      (SELECT COUNT(*) FROM content WHERE type = 'movie') as movies,
      (SELECT COUNT(*) FROM content WHERE type = 'series') as shows,
      (SELECT COUNT(*) FROM content) as total,
      (SELECT COUNT(*) FROM content WHERE tmdb_id IS NOT NULL) as with_tmdb
  `);
  console.log('\n═══════════════════════════════════════');
  console.log(`Total seeded: ${totalSeeded.toLocaleString()}`);
  console.log('DB Stats:', stats.rows[0]);
  console.log('═══════════════════════════════════════');

  await pool.end();
}

main().catch(err => {
  console.error('Seed failed:', err);
  process.exit(1);
});
