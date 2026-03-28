#!/usr/bin/env npx tsx
/**
 * StreamDB Seeder — fetches infohashes from Torrentio's public API
 * for popular movies and TV shows, then imports them into the database.
 *
 * Usage:
 *   npx tsx scripts/seed-from-torrentio.ts                    # Use TMDB to discover popular titles
 *   npx tsx scripts/seed-from-torrentio.ts --imdb-file ids.txt # Use a file with one IMDB ID per line
 *   npx tsx scripts/seed-from-torrentio.ts --pages 50          # Fetch 50 pages from TMDB (1000 titles)
 *
 * Env:
 *   DATABASE_URL  — Postgres connection string
 *   TMDB_API_KEY  — TMDB API key (optional if using --imdb-file)
 */

import pg from 'pg';
import { readFileSync } from 'fs';

const TORRENTIO_BASE = 'https://torrentio.strem.fun/qualityfilter=brremux,hdrall,dolbyvision,4k,1080p,720p,480p,other,scr,cam,unknown';
const TMDB_BASE = 'https://api.themoviedb.org/3';
const DELAY_MS = 800; // Rate limit: ~1.2 req/s to Torrentio
const TMDB_DELAY_MS = 100;

// ── Title parser (no external deps) ────────────────────────────

function parseStreamTitle(title: string): {
  resolution: string | null;
  codec: string | null;
  hdr: string | null;
  size: number | null;
} {
  let resolution: string | null = null;
  if (/2160p|4k|uhd/i.test(title)) resolution = '2160p';
  else if (/1080p/i.test(title)) resolution = '1080p';
  else if (/720p/i.test(title)) resolution = '720p';
  else if (/480p/i.test(title)) resolution = '480p';

  let codec: string | null = null;
  if (/x265|hevc|h\.?265/i.test(title)) codec = 'x265';
  else if (/x264|h\.?264|avc/i.test(title)) codec = 'x264';
  else if (/av1/i.test(title)) codec = 'AV1';

  let hdr: string | null = 'SDR';
  if (/dolby[\.\s-]?vision|[\.\s]dv[\.\s]|dovi/i.test(title)) hdr = 'DV';
  else if (/hdr10\+|hdr10plus/i.test(title)) hdr = 'HDR10+';
  else if (/hdr10|hdr/i.test(title)) hdr = 'HDR10';

  // Parse size like "💾 45.2 GB" or "45.2 GB"
  let size: number | null = null;
  const sizeMatch = title.match(/([\d.]+)\s*GB/i);
  if (sizeMatch) size = Math.round(parseFloat(sizeMatch[1]) * 1024 * 1024 * 1024);
  else {
    const mbMatch = title.match(/([\d.]+)\s*MB/i);
    if (mbMatch) size = Math.round(parseFloat(mbMatch[1]) * 1024 * 1024);
  }

  return { resolution, codec, hdr, size };
}

// ── Fetch helpers ──────────────────────────────────────────────

async function fetchJson(url: string, retries = 3): Promise<any> {
  for (let i = 0; i < retries; i++) {
    try {
      const res = await fetch(url, {
        headers: { 'User-Agent': 'StreamDB/1.0', 'Accept': 'application/json' },
        signal: AbortSignal.timeout(15000),
      });
      if (res.status === 429) {
        console.log('  Rate limited, waiting 5s...');
        await sleep(5000);
        continue;
      }
      if (!res.ok) return null;
      return await res.json();
    } catch (err: any) {
      if (i < retries - 1) {
        await sleep(2000 * (i + 1));
        continue;
      }
      return null;
    }
  }
  return null;
}

function sleep(ms: number) {
  return new Promise(r => setTimeout(r, ms));
}

// ── TMDB: Discover popular IMDB IDs ───────────────────────────

interface TmdbTitle {
  imdbId: string;
  type: 'movie' | 'series';
  title: string;
  year: number | null;
  seasons?: number;
}

async function fetchPopularFromTmdb(apiKey: string, pages: number): Promise<TmdbTitle[]> {
  const titles: TmdbTitle[] = [];
  const seenImdb = new Set<string>();

  // Fetch popular + top rated movies and TV
  const endpoints = [
    { path: '/movie/popular', type: 'movie' as const },
    { path: '/movie/top_rated', type: 'movie' as const },
    { path: '/tv/popular', type: 'series' as const },
    { path: '/tv/top_rated', type: 'series' as const },
    { path: '/trending/movie/week', type: 'movie' as const },
    { path: '/trending/tv/week', type: 'series' as const },
  ];

  for (const ep of endpoints) {
    const maxPages = Math.min(pages, ep.path.includes('trending') ? 10 : pages);
    for (let page = 1; page <= maxPages; page++) {
      const data = await fetchJson(`${TMDB_BASE}${ep.path}?api_key=${apiKey}&page=${page}`);
      if (!data?.results) break;

      for (const item of data.results) {
        const tmdbId = item.id;
        await sleep(TMDB_DELAY_MS);

        // Get external IDs for IMDB
        const extPath = ep.type === 'movie' ? `/movie/${tmdbId}/external_ids` : `/tv/${tmdbId}/external_ids`;
        const ext = await fetchJson(`${TMDB_BASE}${extPath}?api_key=${apiKey}`);
        const imdbId = ext?.imdb_id;
        if (!imdbId || seenImdb.has(imdbId)) continue;
        seenImdb.add(imdbId);

        const title: TmdbTitle = {
          imdbId,
          type: ep.type,
          title: item.title || item.name || '',
          year: (item.release_date || item.first_air_date || '').substring(0, 4) ? parseInt((item.release_date || item.first_air_date || '').substring(0, 4)) || null : null,
        };

        if (ep.type === 'series') {
          title.seasons = item.number_of_seasons || null;
          // Also get season/episode details
          const details = await fetchJson(`${TMDB_BASE}/tv/${tmdbId}?api_key=${apiKey}`);
          if (details?.number_of_seasons) {
            title.seasons = details.number_of_seasons;
          }
          await sleep(TMDB_DELAY_MS);
        }

        titles.push(title);
      }

      if (!data.results.length) break;
      process.stdout.write(`\r  TMDB: ${titles.length} titles discovered (${ep.path} page ${page}/${maxPages})`);
    }
  }

  console.log(`\n  Total unique IMDB IDs from TMDB: ${titles.length}`);
  return titles;
}

// ── Torrentio: Fetch streams for an IMDB ID ──────────────────

interface TorrentioStream {
  infoHash: string;
  title: string;
  fileIdx?: number;
}

async function fetchTorrentioStreams(type: string, id: string): Promise<TorrentioStream[]> {
  const data = await fetchJson(`${TORRENTIO_BASE}/stream/${type}/${id}.json`);
  if (!data?.streams) return [];

  return data.streams
    .filter((s: any) => s.infoHash)
    .map((s: any) => ({
      infoHash: s.infoHash.toLowerCase(),
      title: s.title || '',
      fileIdx: s.fileIdx ?? 0,
    }));
}

// ── DB Import ─────────────────────────────────────────────────

async function importStreams(
  client: pg.PoolClient,
  imdbId: string,
  type: string,
  title: string,
  year: number | null,
  streams: TorrentioStream[],
  season?: number,
  episode?: number,
): Promise<number> {
  let imported = 0;

  for (const stream of streams) {
    if (stream.infoHash.length !== 40) continue;
    const parsed = parseStreamTitle(stream.title);
    const cleanTitle = stream.title.split('\n')[0].replace(/[^\x20-\x7E]/g, '').trim();

    const isEpisode = season !== undefined && episode !== undefined;
    const contentType = isEpisode ? 'episode' : type;
    const contentKey = isEpisode ? `${imdbId}:${season}:${episode}` : imdbId;

    // Upsert file
    await client.query(`
      INSERT INTO files (infohash, file_idx, filename, file_size, torrent_name, resolution, video_codec, hdr, metadata_src, confidence)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'torrentio_seed', 0.4)
      ON CONFLICT (infohash, file_idx) DO NOTHING
    `, [stream.infoHash, stream.fileIdx, cleanTitle, parsed.size, cleanTitle, parsed.resolution, parsed.codec, parsed.hdr]);

    // Upsert content
    const contentResult = await client.query(`
      INSERT INTO content (imdb_id, type, title, year, season, episode, parent_imdb)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW()
      RETURNING id
    `, [contentKey, contentType, title, year, season || null, episode || null, isEpisode ? imdbId : null]);

    // Create edge
    const fileResult = await client.query(
      'SELECT id FROM files WHERE infohash = $1 AND file_idx = $2',
      [stream.infoHash, stream.fileIdx]
    );

    if (fileResult.rows[0] && contentResult.rows[0]) {
      await client.query(`
        INSERT INTO content_files (content_id, file_id, match_method, confidence)
        VALUES ($1, $2, 'torrentio_seed', 0.8)
        ON CONFLICT DO NOTHING
      `, [contentResult.rows[0].id, fileResult.rows[0].id]);
    }

    // Queue probe job
    await client.query(`
      INSERT INTO probe_jobs (infohash, priority, source)
      VALUES ($1, 10, 'torrentio_seed')
      ON CONFLICT (infohash) DO NOTHING
    `, [stream.infoHash]);

    imported++;
  }

  return imported;
}

// ── Hardcoded popular IMDB IDs (fallback if no TMDB key) ─────

const POPULAR_MOVIES = [
  'tt0111161','tt0068646','tt0468569','tt0071562','tt0050083','tt0108052','tt0167260','tt0110912',
  'tt0120737','tt0060196','tt0137523','tt0109830','tt1375666','tt0167261','tt0080684','tt0133093',
  'tt0099685','tt0073486','tt0114369','tt0038650','tt0102926','tt0076759','tt0317248','tt0118799',
  'tt0245429','tt0120815','tt0816692','tt6751668','tt1853728','tt0172495','tt0407887','tt0482571',
  'tt0078788','tt0209144','tt0078748','tt0032138','tt0110413','tt0434409','tt0253474','tt0047478',
  'tt0082971','tt0114814','tt0120689','tt0088763','tt0054215','tt0064116','tt0021749','tt0095327',
  'tt0027977','tt1675434','tt0095765','tt0090605','tt0169547','tt0910970','tt0087843','tt0119698',
  'tt0086190','tt0435761','tt2582802','tt15398776','tt0062622','tt0105236','tt0180093','tt0086879',
  'tt0057012','tt8503618','tt0056058','tt0051201','tt0364569','tt0112573','tt0093058','tt1745960',
  'tt0211915','tt0082096','tt0057565','tt0036775','tt0081505','tt0338013','tt0040522','tt0056172',
  'tt0119488','tt0075314','tt0103064','tt0245712','tt0047396','tt0208092','tt0053125','tt0372784',
  'tt0042192','tt0066921','tt0053604','tt0086250','tt0361748','tt0993846','tt7286456','tt1187043',
  // Recent popular
  'tt1517268','tt9362722','tt6263850','tt10872600','tt13238346','tt14539740','tt15239678',
  'tt11866324','tt14230458','tt21692408','tt5433140','tt9603212','tt14998742',
];

const POPULAR_SERIES = [
  { imdb: 'tt0903747', seasons: 5, title: 'Breaking Bad' },
  { imdb: 'tt0944947', seasons: 8, title: 'Game of Thrones' },
  { imdb: 'tt0306414', seasons: 5, title: 'The Wire' },
  { imdb: 'tt0386676', seasons: 5, title: 'The Office' },
  { imdb: 'tt5491994', seasons: 4, title: 'Planet Earth II' },
  { imdb: 'tt0141842', seasons: 7, title: 'The Sopranos' },
  { imdb: 'tt2861424', seasons: 3, title: 'Rick and Morty' },
  { imdb: 'tt0773262', seasons: 5, title: 'Dexter' },
  { imdb: 'tt2085059', seasons: 6, title: 'Black Mirror' },
  { imdb: 'tt0108778', seasons: 10, title: 'Friends' },
  { imdb: 'tt0475784', seasons: 2, title: 'Westworld' },
  { imdb: 'tt4574334', seasons: 4, title: 'Stranger Things' },
  { imdb: 'tt7366338', seasons: 3, title: 'Chernobyl' },
  { imdb: 'tt5180504', seasons: 2, title: 'The Witcher' },
  { imdb: 'tt0185906', seasons: 6, title: 'Band of Brothers' },
  { imdb: 'tt3032476', seasons: 6, title: 'Better Call Saul' },
  { imdb: 'tt1190634', seasons: 5, title: 'The Boys' },
  { imdb: 'tt2442560', seasons: 2, title: 'Peaky Blinders' },
  { imdb: 'tt11198330', seasons: 2, title: 'House of the Dragon' },
  { imdb: 'tt13443470', seasons: 2, title: 'Wednesday' },
  { imdb: 'tt2356777', seasons: 3, title: 'True Detective' },
  { imdb: 'tt1856010', seasons: 3, title: 'House of Cards' },
  { imdb: 'tt0460649', seasons: 9, title: 'How I Met Your Mother' },
  { imdb: 'tt2575988', seasons: 6, title: 'Silicon Valley' },
  { imdb: 'tt0804503', seasons: 3, title: 'Mad Men' },
  { imdb: 'tt1632701', seasons: 4, title: 'Suits' },
  { imdb: 'tt0413573', seasons: 5, title: 'Grey\'s Anatomy' },
  { imdb: 'tt7660850', seasons: 3, title: 'Succession' },
  { imdb: 'tt14688458', seasons: 2, title: 'Shogun' },
  { imdb: 'tt15384528', seasons: 2, title: 'Fallout' },
];

// ── Main ──────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const imdbFile = args.includes('--imdb-file') ? args[args.indexOf('--imdb-file') + 1] : null;
  const pagesArg = args.includes('--pages') ? parseInt(args[args.indexOf('--pages') + 1]) : 10;

  const dbUrl = process.env.DATABASE_URL || 'postgres://streamdb:streamdb_secret@localhost:5432/streamdb';
  const tmdbKey = process.env.TMDB_API_KEY || '';

  const pool = new pg.Pool({ connectionString: dbUrl });
  const client = await pool.connect();

  let totalImported = 0;
  let totalTitles = 0;

  try {
    // ── Gather IMDB IDs ──

    let movieIds: string[] = [];
    let seriesEntries: { imdb: string; seasons: number; title: string }[] = [];

    if (imdbFile) {
      console.log(`Reading IMDB IDs from ${imdbFile}...`);
      const lines = readFileSync(imdbFile, 'utf-8').split('\n').map(l => l.trim()).filter(l => l.startsWith('tt'));
      movieIds = lines; // Treat all as movies for simplicity
      console.log(`  Found ${movieIds.length} IMDB IDs`);
    } else if (tmdbKey) {
      console.log(`Discovering popular titles from TMDB (${pagesArg} pages per category)...`);
      const tmdbTitles = await fetchPopularFromTmdb(tmdbKey, pagesArg);
      movieIds = tmdbTitles.filter(t => t.type === 'movie').map(t => t.imdbId);
      seriesEntries = tmdbTitles.filter(t => t.type === 'series').map(t => ({
        imdb: t.imdbId,
        seasons: t.seasons || 2,
        title: t.title,
      }));
    } else {
      console.log('No TMDB_API_KEY set, using hardcoded popular titles...');
      movieIds = POPULAR_MOVIES;
      seriesEntries = POPULAR_SERIES;
    }

    console.log(`\nSeeding: ${movieIds.length} movies + ${seriesEntries.length} series\n`);

    // ── Seed movies ──

    console.log('=== MOVIES ===');
    await client.query('BEGIN');
    let batchCount = 0;

    for (let i = 0; i < movieIds.length; i++) {
      const imdbId = movieIds[i];
      const streams = await fetchTorrentioStreams('movie', imdbId);

      if (streams.length > 0) {
        const count = await importStreams(client, imdbId, 'movie', '', null, streams);
        totalImported += count;
        totalTitles++;
        batchCount += count;
      }

      if (batchCount >= 500) {
        await client.query('COMMIT');
        await client.query('BEGIN');
        batchCount = 0;
      }

      process.stdout.write(`\r  Movies: ${i + 1}/${movieIds.length} | Streams: ${totalImported.toLocaleString()} | Hit: ${totalTitles}`);
      await sleep(DELAY_MS);
    }

    await client.query('COMMIT');
    console.log(`\n  Movies done: ${totalImported.toLocaleString()} streams from ${totalTitles} titles\n`);

    // ── Seed series (episodes) ──

    console.log('=== SERIES ===');
    let seriesImported = 0;

    for (const series of seriesEntries) {
      console.log(`\n  ${series.title} (${series.imdb}) — ${series.seasons} seasons`);
      await client.query('BEGIN');
      batchCount = 0;

      // Fetch S01E01 through each season
      for (let s = 1; s <= Math.min(series.seasons, 10); s++) {
        // Fetch first 15 episodes per season
        for (let e = 1; e <= 15; e++) {
          const id = `${series.imdb}:${s}:${e}`;
          const streams = await fetchTorrentioStreams('series', id);

          if (streams.length === 0 && e > 2) break; // No more episodes this season

          if (streams.length > 0) {
            const count = await importStreams(client, series.imdb, 'series', series.title, null, streams, s, e);
            seriesImported += count;
            totalImported += count;
            batchCount += count;
          }

          if (batchCount >= 500) {
            await client.query('COMMIT');
            await client.query('BEGIN');
            batchCount = 0;
          }

          process.stdout.write(`\r    S${String(s).padStart(2, '0')}E${String(e).padStart(2, '0')} — ${streams.length} streams (total: ${seriesImported.toLocaleString()})`);
          await sleep(DELAY_MS);
        }
      }

      await client.query('COMMIT');
      totalTitles++;
    }

    console.log(`\n\n  Series done: ${seriesImported.toLocaleString()} streams\n`);

  } catch (err) {
    try { await client.query('ROLLBACK'); } catch {}
    throw err;
  } finally {
    client.release();
  }

  // Print final stats
  const stats = await pool.query(`
    SELECT
      (SELECT COUNT(*) FROM content) as content,
      (SELECT COUNT(*) FROM files) as files,
      (SELECT COUNT(*) FROM content_files) as mappings,
      (SELECT COUNT(*) FROM probe_jobs) as probe_jobs
  `);
  console.log('═══════════════════════════════════════');
  console.log(`Total imported: ${totalImported.toLocaleString()} streams from ${totalTitles} titles`);
  console.log('DB Stats:', stats.rows[0]);
  console.log('═══════════════════════════════════════');

  await pool.end();
}

main().catch(err => {
  console.error('Seed failed:', err);
  process.exit(1);
});
