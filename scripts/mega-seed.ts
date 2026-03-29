#!/usr/bin/env npx tsx
/**
 * StreamDB Mega Seeder — Continuous pipeline to reach 1M+ content↔file edges.
 *
 * Strategy (in priority order):
 *   1. Zilean API — direct IMDB→infohash mappings (highest quality, no guessing)
 *   2. TMDB content discovery — populate content table (500K popular + newest)
 *   3. DMM hashlists — re-fetch with title matching fallback
 *   4. Loop until 1M edges or Ctrl+C
 *
 * Usage:
 *   DATABASE_URL=postgres://... TMDB_API_KEY=xxx npx tsx scripts/mega-seed.ts
 */

import pg from 'pg';
import { createGunzip } from 'zlib';
import { Readable } from 'stream';
import { createInterface } from 'readline';

// ── Config ───────────────────────────────────────────────────
const DB_URL = process.env.DATABASE_URL || 'postgres://streamdb:streamdb_secret@localhost:5432/streamdb';
const TMDB_KEY = process.env.TMDB_API_KEY || '';
const ZILEAN_URL = process.env.ZILEAN_URL || 'http://zilean:8181';
const TARGET_EDGES = 1_000_000;
const BATCH_SIZE = 500;
const TMDB_BASE = 'https://api.themoviedb.org/3';

const pool = new pg.Pool({ connectionString: DB_URL, max: 15 });
function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }
let totalEdgesCreated = 0;
let startTime = Date.now();

// ── Title Parser ─────────────────────────────────────────────
function parseTitle(name: string) {
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

  const seMatch = name.match(/S(\d{1,2})E(\d{1,3})/i);
  const season = seMatch ? parseInt(seMatch[1]) : null;
  const episode = seMatch ? parseInt(seMatch[2]) : null;

  return { resolution, codec, hdr, season, episode };
}

// ── Fetch Helpers ────────────────────────────────────────────
async function fetchJson(url: string, retries = 3): Promise<any> {
  for (let i = 0; i < retries; i++) {
    try {
      const res = await fetch(url, {
        headers: { 'User-Agent': 'StreamDB/1.0', 'Accept': 'application/json' },
        signal: AbortSignal.timeout(30000),
      });
      if (res.status === 429 || res.status === 403) {
        await sleep(5000 * (i + 1));
        continue;
      }
      if (!res.ok) return null;
      return await res.json();
    } catch {
      await sleep(2000 * (i + 1));
    }
  }
  return null;
}

// ── Zilean DB Direct Import ──────────────────────────────────
const ZILEAN_DB = process.env.ZILEAN_DB || '';

async function phase0_zileanDirect(): Promise<boolean> {
  if (!ZILEAN_DB) return false;

  let zileanPool: pg.Pool;
  try {
    zileanPool = new pg.Pool({ connectionString: ZILEAN_DB, max: 5 });
    await zileanPool.query('SELECT 1');
  } catch (err: any) {
    console.log(`  Zilean DB not available: ${err.message}`);
    return false;
  }

  console.log('\n═══ PHASE 0: Zilean Direct DB Import ═══\n');

  const countResult = await zileanPool.query('SELECT COUNT(*)::int as total FROM "Torrents" WHERE "ImdbId" IS NOT NULL');
  const total = countResult.rows[0].total;
  console.log(`  Zilean DB has ${total.toLocaleString()} torrents with IMDB IDs`);

  if (total === 0) {
    console.log('  Zilean DB empty (still ingesting?). Skipping.\n');
    await zileanPool.end();
    return false;
  }

  // Load IMDB metadata
  const imdbResult = await zileanPool.query('SELECT "ImdbId", "Category", "Title", "Year" FROM "ImdbFiles"');
  const imdbMap = new Map<string, { category: string; title: string; year: number | null }>();
  for (const row of imdbResult.rows) {
    imdbMap.set(row.ImdbId, { category: row.Category || 'movie', title: row.Title || '', year: row.Year || null });
  }
  console.log(`  IMDB metadata: ${imdbMap.size.toLocaleString()} entries`);

  const BATCH = 2000;
  let offset = 0, imported = 0, newEdges = 0;
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    while (offset < total) {
      const batch = await zileanPool.query(
        `SELECT "InfoHash", "ImdbId", "RawTitle", "Resolution", "Codec", "Seasons", "Episodes", "Size", "Year"
         FROM "Torrents" WHERE "ImdbId" IS NOT NULL
         ORDER BY "InfoHash" LIMIT $1 OFFSET $2`, [BATCH, offset]
      );

      if (batch.rows.length === 0) break;

      for (const t of batch.rows) {
        const hash = (t.InfoHash || '').toLowerCase().trim();
        if (!hash || hash.length !== 40) continue;

        const imdbId = t.ImdbId;
        if (!imdbId || !imdbId.startsWith('tt')) continue;

        const rawTitle = t.RawTitle || '';
        const parsed = parseTitle(rawTitle);
        const resolution = t.Resolution || parsed.resolution;
        const codec = t.Codec || parsed.codec;
        const hdr = parsed.hdr;
        const fileSize = t.Size ? Number(t.Size) : null;
        const seasons = Array.isArray(t.Seasons) ? t.Seasons : [];
        const episodes = Array.isArray(t.Episodes) ? t.Episodes : [];

        // Upsert file
        await client.query(`
          INSERT INTO files (infohash, file_idx, filename, file_size, torrent_name, resolution, video_codec, hdr, metadata_src, confidence)
          VALUES ($1, 0, $2, $3, $4, $5, $6, $7, 'zilean', 0.5)
          ON CONFLICT (infohash, file_idx) DO UPDATE SET
            resolution = COALESCE(EXCLUDED.resolution, files.resolution),
            video_codec = COALESCE(EXCLUDED.video_codec, files.video_codec),
            file_size = COALESCE(EXCLUDED.file_size, files.file_size)
        `, [hash, rawTitle, fileSize, rawTitle, resolution, codec, hdr]);

        const imdbInfo = imdbMap.get(imdbId);
        const title = imdbInfo?.title || rawTitle.split(/[\.\s](?:(?:19|20)\d{2})/)[0]?.replace(/\./g, ' ').trim() || imdbId;
        const year = t.Year || imdbInfo?.year || null;

        if (seasons.length > 0 && episodes.length > 0) {
          for (let si = 0; si < seasons.length; si++) {
            const s = seasons[si], e = episodes[si] || episodes[0] || 1;
            const cr = await client.query(`
              INSERT INTO content (imdb_id, type, title, year, season, episode, parent_imdb)
              VALUES ($1, 'episode', $2, $3, $4, $5, $6)
              ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW() RETURNING id
            `, [`${imdbId}:${s}:${e}`, title, year, s, e, imdbId]);
            const fr = await client.query('SELECT id FROM files WHERE infohash = $1 AND file_idx = 0', [hash]);
            if (fr.rows[0] && cr.rows[0]) {
              const er = await client.query(`
                INSERT INTO content_files (content_id, file_id, match_method, confidence)
                VALUES ($1, $2, 'zilean', 0.85)
                ON CONFLICT DO NOTHING RETURNING 1
              `, [cr.rows[0].id, fr.rows[0].id]);
              if (er.rowCount && er.rowCount > 0) { newEdges++; totalEdgesCreated++; }
            }
          }
        } else {
          const type = (imdbInfo?.category === 'tv' || imdbInfo?.category === 'tvSeries') ? 'series' : 'movie';
          const cr = await client.query(`
            INSERT INTO content (imdb_id, type, title, year)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW() RETURNING id
          `, [imdbId, type, title, year]);
          const fr = await client.query('SELECT id FROM files WHERE infohash = $1 AND file_idx = 0', [hash]);
          if (fr.rows[0] && cr.rows[0]) {
            const er = await client.query(`
              INSERT INTO content_files (content_id, file_id, match_method, confidence)
              VALUES ($1, $2, 'zilean', 0.85)
              ON CONFLICT DO NOTHING RETURNING 1
            `, [cr.rows[0].id, fr.rows[0].id]);
            if (er.rowCount && er.rowCount > 0) { newEdges++; totalEdgesCreated++; }
          }
        }

        // Queue probe for metadata enrichment
        await client.query(`
          INSERT INTO probe_jobs (infohash, priority, source) VALUES ($1, 8, 'zilean')
          ON CONFLICT (infohash) DO NOTHING
        `, [hash]);

        imported++;
      }

      if (imported % 10000 < BATCH) {
        await client.query('COMMIT');
        await client.query('BEGIN');
        process.stdout.write(`\r  Zilean DB: ${imported.toLocaleString()} / ${total.toLocaleString()} | ${newEdges.toLocaleString()} new edges`);
      }

      offset += BATCH;

      // Check target periodically
      if (offset % 50000 < BATCH) {
        const edges = (await getStats()).edges;
        if (edges >= TARGET_EDGES) { console.log('\n  TARGET REACHED!'); break; }
      }
    }

    await client.query('COMMIT');
  } finally {
    client.release();
  }

  console.log(`\n  Phase 0 complete: ${imported.toLocaleString()} imported | ${newEdges.toLocaleString()} edges\n`);
  await zileanPool.end();
  return true;
}

// ── Stats ────────────────────────────────────────────────────
async function getStats(): Promise<{ edges: number; content: number; files: number }> {
  const res = await pool.query(`
    SELECT
      (SELECT count(*)::int FROM content_files) as edges,
      (SELECT count(*)::int FROM content) as content,
      (SELECT count(*)::int FROM files) as files
  `);
  return res.rows[0];
}

async function printStats() {
  const s = await getStats();
  const elapsed = Math.round((Date.now() - startTime) / 60000);
  const rate = elapsed > 0 ? Math.round(totalEdgesCreated / elapsed) : 0;
  console.log(`\n[PROGRESS] Edges: ${s.edges.toLocaleString()} / ${TARGET_EDGES.toLocaleString()} (${(s.edges / TARGET_EDGES * 100).toFixed(1)}%) | Content: ${s.content.toLocaleString()} | Files: ${s.files.toLocaleString()} | +${totalEdgesCreated.toLocaleString()} this run | ${rate}/min | ${elapsed}min\n`);
  return s.edges;
}

// ═════════════════════════════════════════════════════════════
// PHASE 1: ZILEAN API — Direct IMDB→infohash (PRIMARY SOURCE)
// ═════════════════════════════════════════════════════════════
async function phase1_zileanAPI() {
  console.log('\n═══ PHASE 1: Zilean API — IMDB→infohash import ═══\n');

  // Get all IMDB IDs from our content table
  const imdbResult = await pool.query(`
    SELECT imdb_id, type, title, year FROM content
    WHERE imdb_id IS NOT NULL AND imdb_id LIKE 'tt%'
    ORDER BY imdb_id
  `);

  const imdbIds = imdbResult.rows;
  console.log(`  Querying Zilean for ${imdbIds.length.toLocaleString()} IMDB IDs...\n`);

  // Track which IMDB IDs already have Zilean edges (skip on repeat runs)
  const alreadyQueried = new Set<string>();
  const existingEdges = await pool.query(`
    SELECT DISTINCT c.imdb_id FROM content_files cf
    JOIN content c ON c.id = cf.content_id
    WHERE cf.match_method IN ('zilean_api', 'zilean') AND c.imdb_id IS NOT NULL
  `);
  for (const row of existingEdges.rows) alreadyQueried.add(row.imdb_id);
  console.log(`  Already have Zilean edges: ${alreadyQueried.size.toLocaleString()} (skipping)\n`);

  const client = await pool.connect();
  let queried = 0, totalHashes = 0, newEdges = 0;

  try {
    await client.query('BEGIN');

    for (let i = 0; i < imdbIds.length; i++) {
      const { imdb_id, type, title, year } = imdbIds[i];
      if (alreadyQueried.has(imdb_id)) continue;

      // Query Zilean filtered endpoint
      const data = await fetchJson(`${ZILEAN_URL}/dmm/filtered?ImdbId=${imdb_id}`);

      if (Array.isArray(data) && data.length > 0) {
        for (const torrent of data) {
          const hash = (torrent.info_hash || '').toLowerCase();
          if (!hash || hash.length !== 40) continue;

          const rawTitle = torrent.raw_title || '';
          const parsed = parseTitle(rawTitle);
          const resolution = torrent.resolution || parsed.resolution;
          const codec = torrent.codec || parsed.codec;
          const hdr = parsed.hdr;
          const fileSize = torrent.size ? Number(torrent.size) : null;
          const seasons = torrent.seasons || [];
          const episodes = torrent.episodes || [];

          // Insert file
          await client.query(`
            INSERT INTO files (infohash, file_idx, filename, file_size, torrent_name, resolution, video_codec, hdr, metadata_src, confidence)
            VALUES ($1, 0, $2, $3, $4, $5, $6, $7, 'zilean_api', 0.5)
            ON CONFLICT (infohash, file_idx) DO UPDATE SET
              resolution = COALESCE(EXCLUDED.resolution, files.resolution),
              video_codec = COALESCE(EXCLUDED.video_codec, files.video_codec),
              file_size = COALESCE(EXCLUDED.file_size, files.file_size)
          `, [hash, rawTitle, fileSize, rawTitle, resolution, codec, hdr]);

          // Handle episodes vs movies
          if (Array.isArray(seasons) && seasons.length > 0 && Array.isArray(episodes) && episodes.length > 0) {
            for (let si = 0; si < seasons.length; si++) {
              const s = seasons[si];
              const e = episodes[si] || episodes[0] || 1;
              const epKey = `${imdb_id}:${s}:${e}`;

              const cr = await client.query(`
                INSERT INTO content (imdb_id, type, title, year, season, episode, parent_imdb)
                VALUES ($1, 'episode', $2, $3, $4, $5, $6)
                ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW()
                RETURNING id
              `, [epKey, title, year, s, e, imdb_id]);

              const fr = await client.query('SELECT id FROM files WHERE infohash = $1 AND file_idx = 0', [hash]);
              if (fr.rows[0] && cr.rows[0]) {
                const er = await client.query(`
                  INSERT INTO content_files (content_id, file_id, match_method, confidence)
                  VALUES ($1, $2, 'zilean_api', 0.85)
                  ON CONFLICT DO NOTHING RETURNING 1
                `, [cr.rows[0].id, fr.rows[0].id]);
                if (er.rowCount && er.rowCount > 0) { newEdges++; totalEdgesCreated++; }
              }
            }
          } else {
            // Movie or unspecified — link to parent content
            const cr = await client.query(`
              INSERT INTO content (imdb_id, type, title, year)
              VALUES ($1, $2, $3, $4)
              ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW()
              RETURNING id
            `, [imdb_id, type || 'movie', title, year]);

            const fr = await client.query('SELECT id FROM files WHERE infohash = $1 AND file_idx = 0', [hash]);
            if (fr.rows[0] && cr.rows[0]) {
              const er = await client.query(`
                INSERT INTO content_files (content_id, file_id, match_method, confidence)
                VALUES ($1, $2, 'zilean_api', 0.85)
                ON CONFLICT DO NOTHING RETURNING 1
              `, [cr.rows[0].id, fr.rows[0].id]);
              if (er.rowCount && er.rowCount > 0) { newEdges++; totalEdgesCreated++; }
            }
          }

          totalHashes++;

          // Queue probe job for hashing/metadata enrichment
          await client.query(`
            INSERT INTO probe_jobs (infohash, priority, source)
            VALUES ($1, 8, 'zilean_api')
            ON CONFLICT (infohash) DO NOTHING
          `, [hash]);
        }
      }

      queried++;

      // Commit every 50 IMDB IDs
      if (queried % 50 === 0) {
        await client.query('COMMIT');
        await client.query('BEGIN');
        process.stdout.write(`\r  Zilean: ${queried.toLocaleString()} / ${imdbIds.length.toLocaleString()} queried | ${totalHashes.toLocaleString()} hashes | ${newEdges.toLocaleString()} new edges`);
      }

      // Rate limit: local instance = fast, remote = slower
      await sleep(ZILEAN_URL.includes('localhost') || ZILEAN_URL.includes('zilean:') ? 50 : 200);

      // Early termination: if hit rate < 2% after 500 queries, move on
      if (queried >= 500 && queried % 500 === 0) {
        const hitRate = totalHashes / queried;
        if (hitRate < 0.02) {
          console.log(`\n  Low hit rate (${(hitRate * 100).toFixed(1)}%) after ${queried} queries. Moving to next phase.`);
          break;
        }
        const edges = (await getStats()).edges;
        if (edges >= TARGET_EDGES) {
          console.log('\n  TARGET REACHED!');
          break;
        }
      }
    }

    await client.query('COMMIT');
  } finally {
    client.release();
  }

  console.log(`\n  Phase 1 complete: ${queried.toLocaleString()} IMDB IDs queried | ${totalHashes.toLocaleString()} hashes | ${newEdges.toLocaleString()} edges\n`);
}

// ═════════════════════════════════════════════════════════════
// PHASE 2: TMDB Content Discovery (500K popular + newest)
// ═════════════════════════════════════════════════════════════
async function phase2_tmdbDiscovery() {
  if (!TMDB_KEY) { console.log('\n  Skipping Phase 2: No TMDB_API_KEY\n'); return; }

  console.log('\n═══ PHASE 2: TMDB content discovery ═══\n');

  for (const type of ['movie', 'tv_series'] as const) {
    const now = new Date();
    const dates = [now, new Date(now.getTime() - 86400000)];
    let items: { id: number; popularity: number }[] = [];

    for (const d of dates) {
      const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
      const dd = String(d.getUTCDate()).padStart(2, '0');
      const yyyy = d.getUTCFullYear();
      const url = `https://files.tmdb.org/p/exports/${type}_ids_${mm}_${dd}_${yyyy}.json.gz`;
      console.log(`  Downloading: ${url}`);

      try {
        const res = await fetch(url, { signal: AbortSignal.timeout(120000) });
        if (!res.ok) { console.log(`  HTTP ${res.status}, trying previous day...`); continue; }

        const gunzip = createGunzip();
        const nodeStream = Readable.fromWeb(res.body as any);
        nodeStream.pipe(gunzip);
        const rl = createInterface({ input: gunzip });

        for await (const line of rl) {
          try {
            const obj = JSON.parse(line);
            if (!obj.adult) items.push({ id: obj.id, popularity: obj.popularity || 0 });
          } catch {}
        }
        console.log(`  Got ${items.length.toLocaleString()} ${type} items`);
        break;
      } catch (err: any) {
        console.log(`  Failed: ${err.message}`);
      }
    }

    if (!items.length) continue;

    // Sort by popularity, take top 500K
    items.sort((a, b) => b.popularity - a.popularity);
    items = items.slice(0, 500000);
    console.log(`  Processing top ${items.length.toLocaleString()} by popularity...`);

    const client = await pool.connect();
    let inserted = 0;
    try {
      await client.query('BEGIN');
      const apiType = type === 'movie' ? 'movie' : 'tv';

      for (let i = 0; i < items.length; i += 40) {
        const batch = items.slice(i, i + 40);
        const promises = batch.map(async item => {
          try {
            const res = await fetch(`${TMDB_BASE}/${apiType}/${item.id}?api_key=${TMDB_KEY}&append_to_response=external_ids`, {
              signal: AbortSignal.timeout(10000),
            });
            if (!res.ok) return;
            const data = await res.json() as any;
            const imdbId = data.external_ids?.imdb_id || data.imdb_id;
            if (!imdbId) return;

            const title = data.title || data.name || '';
            const yearStr = (data.release_date || data.first_air_date || '').substring(0, 4);
            const runtime = data.runtime || (data.episode_run_time?.[0]) || null;

            await client.query(`
              INSERT INTO content (imdb_id, tmdb_id, type, title, year, runtime_minutes)
              VALUES ($1, $2, $3, $4, $5, $6)
              ON CONFLICT (imdb_id) DO UPDATE SET
                tmdb_id = COALESCE(EXCLUDED.tmdb_id, content.tmdb_id),
                title = CASE WHEN length(EXCLUDED.title) > length(content.title) THEN EXCLUDED.title ELSE content.title END,
                runtime_minutes = COALESCE(EXCLUDED.runtime_minutes, content.runtime_minutes),
                updated_at = NOW()
            `, [imdbId, item.id, apiType === 'movie' ? 'movie' : 'series', title, yearStr ? parseInt(yearStr) : null, runtime]);

            inserted++;
          } catch {}
        });
        await Promise.all(promises);

        if (inserted % 1000 < 40) {
          await client.query('COMMIT');
          await client.query('BEGIN');
        }
        if ((i + 40) % 4000 < 40) {
          process.stdout.write(`\r  TMDB ${apiType}: ${inserted.toLocaleString()} / ${items.length.toLocaleString()}`);
        }
        await sleep(1100);
      }

      await client.query('COMMIT');
      console.log(`\n  TMDB ${apiType}: ${inserted.toLocaleString()} content items\n`);
    } finally {
      client.release();
    }
  }
}

// ═════════════════════════════════════════════════════════════
// PHASE 3: Backfill existing unmatched files via pg_trgm
// ═════════════════════════════════════════════════════════════
async function phase3_titleMatchBackfill() {
  console.log('\n═══ PHASE 3: Title-match backfill for unmatched files ═══\n');

  const client = await pool.connect();
  let matched = 0, processed = 0;

  try {
    while (true) {
      const unmatched = await client.query(`
        SELECT f.id, f.filename, f.infohash, f.file_idx
        FROM files f
        LEFT JOIN content_files cf ON cf.file_id = f.id
        WHERE cf.file_id IS NULL AND f.filename IS NOT NULL AND f.filename != ''
        ORDER BY f.file_size DESC NULLS LAST
        LIMIT $1
      `, [BATCH_SIZE]);

      if (!unmatched.rows.length) break;

      await client.query('BEGIN');

      for (const row of unmatched.rows) {
        processed++;
        const name = row.filename || '';

        // Extract clean title
        const yearMatch = name.match(/[\.\s\(]?((?:19|20)\d{2})[\.\s\)]/);
        const year = yearMatch ? parseInt(yearMatch[1]) : null;
        let cleanTitle = name.split(/[\.\s](?:(?:19|20)\d{2}|S\d{2}|2160p|1080p|720p|480p)/i)[0] || name;
        cleanTitle = cleanTitle.replace(/\./g, ' ').replace(/\s+/g, ' ').trim();
        if (cleanTitle.length < 3) continue;

        // pg_trgm similarity search against content table
        const match = year
          ? await client.query(`SELECT id, imdb_id FROM content WHERE similarity(title, $1) > 0.35 AND (year = $2 OR year IS NULL) AND imdb_id IS NOT NULL ORDER BY similarity(title, $1) DESC LIMIT 1`, [cleanTitle, year])
          : await client.query(`SELECT id, imdb_id FROM content WHERE similarity(title, $1) > 0.45 AND imdb_id IS NOT NULL ORDER BY similarity(title, $1) DESC LIMIT 1`, [cleanTitle]);

        if (match.rows[0]) {
          const parsed = parseTitle(name);
          await client.query(`
            INSERT INTO content_files (content_id, file_id, match_method, confidence)
            VALUES ($1, $2, 'title_match', 0.6)
            ON CONFLICT DO NOTHING
          `, [match.rows[0].id, row.id]);

          // Queue probe for enrichment
          await client.query(`
            INSERT INTO probe_jobs (infohash, priority, source)
            VALUES ($1, 5, 'title_match')
            ON CONFLICT (infohash) DO NOTHING
          `, [row.infohash]);

          matched++;
          totalEdgesCreated++;
        }

        if (processed % 500 === 0) {
          await client.query('COMMIT');
          await client.query('BEGIN');
          process.stdout.write(`\r  Title match: ${processed.toLocaleString()} processed | ${matched.toLocaleString()} matched`);
        }
      }

      await client.query('COMMIT');
      process.stdout.write(`\r  Title match: ${processed.toLocaleString()} processed | ${matched.toLocaleString()} matched`);

      const edges = (await getStats()).edges;
      if (edges >= TARGET_EDGES) { console.log('\n  TARGET REACHED!'); return; }
    }
  } finally {
    client.release();
  }

  console.log(`\n  Phase 3 complete: ${matched.toLocaleString()} edges from ${processed.toLocaleString()} files\n`);
}

// ═════════════════════════════════════════════════════════════
// MAIN LOOP
// ═════════════════════════════════════════════════════════════
async function main() {
  console.log('╔══════════════════════════════════════════════════════╗');
  console.log('║  StreamDB Mega Seeder — Target: 1M edges            ║');
  console.log('║  Zilean API + TMDB + Title Matching + Probing       ║');
  console.log('╚══════════════════════════════════════════════════════╝\n');

  let edges = await printStats();
  if (edges >= TARGET_EDGES) { console.log('TARGET ALREADY REACHED!'); return; }

  let round = 0;
  while (true) {
    round++;
    console.log(`\n━━━━━━━━━━━━━━━━━━━━━ ROUND ${round} ━━━━━━━━━━━━━━━━━━━━━`);

    // Phase 0: Zilean Direct DB (fastest — reads PostgreSQL directly)
    const usedDB = await phase0_zileanDirect();
    edges = await printStats();
    if (edges >= TARGET_EDGES) break;

    // Phase 1: Zilean API — direct IMDB→infohash (fallback if no DB)
    if (!usedDB) {
      await phase1_zileanAPI();
      edges = await printStats();
      if (edges >= TARGET_EDGES) break;
    }

    // Phase 2: TMDB discovery (expand content table for more Zilean queries)
    const contentCount = (await pool.query('SELECT count(*)::int as c FROM content')).rows[0].c;
    if (contentCount < 500000) {
      await phase2_tmdbDiscovery();
      edges = await printStats();
      if (edges >= TARGET_EDGES) break;

      // Re-run Zilean with expanded content table
      console.log('\n  Re-running Zilean with expanded content...');
      await phase1_zileanAPI();
      edges = await printStats();
      if (edges >= TARGET_EDGES) break;
    }

    // Phase 3: Title-match backfill for remaining unmatched files
    await phase3_titleMatchBackfill();
    edges = await printStats();
    if (edges >= TARGET_EDGES) break;

    console.log(`\nRound ${round} complete. Sleeping 120s before next round...`);
    await sleep(120000);
  }

  console.log('\n╔══════════════════════════════════════════════════════╗');
  console.log('║  TARGET REACHED: 1M+ content_files edges!           ║');
  console.log('╚══════════════════════════════════════════════════════╝');
  await printStats();
  await pool.end();
}

main().catch(err => {
  console.error('Mega seed failed:', err);
  process.exit(1);
});
