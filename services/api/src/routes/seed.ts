import { Router } from 'express';
import { pool } from '../db/pool.js';
import { logger } from '../utils/logger.js';
import { decompressFromBase64 } from '../utils/lz-string.js';
import { config } from '../config.js';

const router = Router();

let seedingInProgress = false;
let seedProgress = { phase: '', processed: 0, total: 0, files: 0, matched: 0 };

router.post('/api/seed', async (req, res) => {
  if (seedingInProgress) {
    return res.json({ status: 'already_running', progress: seedProgress });
  }

  // Accept TMDB key via POST body (overrides env)
  const tmdbKey = req.body?.tmdb_api_key || config.tmdbApiKey;

  seedingInProgress = true;
  seedProgress = { phase: 'starting', processed: 0, total: 0, files: 0, matched: 0 };
  res.json({ status: 'started' });

  runSeedPipeline(tmdbKey).finally(() => { seedingInProgress = false; });
});

// Phase 2 only — match already-imported files to IMDB via TMDB
router.post('/api/seed/match', async (req, res) => {
  if (seedingInProgress) {
    return res.json({ status: 'already_running', progress: seedProgress });
  }

  const tmdbKey = req.body?.tmdb_api_key || config.tmdbApiKey;
  if (!tmdbKey) {
    return res.status(400).json({ error: 'TMDB API key required (pass tmdb_api_key in body or set TMDB_API_KEY env)' });
  }

  seedingInProgress = true;
  seedProgress = { phase: 'matching_imdb', processed: 0, total: 0, files: 0, matched: 0 };
  res.json({ status: 'started', phase: 'match_only' });

  runMatchPhase(tmdbKey).finally(() => { seedingInProgress = false; });
});

// Accept bulk hash data via POST
router.post('/api/seed/bulk', async (req, res) => {
  const entries = req.body?.entries;
  if (!Array.isArray(entries)) {
    return res.status(400).json({ error: 'Expected { entries: [{ imdb, hash, title, size? }] }' });
  }

  const client = await pool.connect();
  let imported = 0;

  try {
    await client.query('BEGIN');
    for (const entry of entries) {
      const hash = (entry.hash || '').toLowerCase();
      if (!hash || hash.length !== 40) continue;
      const imdb = entry.imdb || '';
      if (!imdb.startsWith('tt')) continue;

      const parsed = parseTitle(entry.title || '');
      await client.query(`INSERT INTO files (infohash, file_idx, filename, file_size, torrent_name, resolution, video_codec, hdr, metadata_src, confidence) VALUES ($1, 0, $2, $3, $4, $5, $6, $7, 'bulk_seed', 0.4) ON CONFLICT (infohash, file_idx) DO NOTHING`,
        [hash, entry.title || '', entry.size || null, entry.title || '', parsed.resolution, parsed.codec, parsed.hdr]);

      const cr = await client.query(`INSERT INTO content (imdb_id, type, title) VALUES ($1, $2, $3) ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW() RETURNING id`,
        [imdb, parsed.isEpisode ? 'episode' : 'movie', entry.title || imdb]);

      const fr = await client.query('SELECT id FROM files WHERE infohash = $1 AND file_idx = 0', [hash]);
      if (fr.rows[0] && cr.rows[0]) {
        await client.query(`INSERT INTO content_files (content_id, file_id, match_method, confidence) VALUES ($1, $2, 'bulk_seed', 0.7) ON CONFLICT DO NOTHING`,
          [cr.rows[0].id, fr.rows[0].id]);
      }
      await client.query(`INSERT INTO probe_jobs (infohash, priority, source) VALUES ($1, 1, 'bulk_seed') ON CONFLICT (infohash) DO NOTHING`, [hash]);
      imported++;
    }
    await client.query('COMMIT');
    res.json({ imported });
  } catch (err: any) {
    try { await client.query('ROLLBACK'); } catch {}
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

router.get('/api/seed/status', async (_req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        (SELECT COUNT(*)::int FROM content) as content,
        (SELECT COUNT(*)::int FROM files) as files,
        (SELECT COUNT(*)::int FROM content_files) as mappings,
        (SELECT COUNT(*)::int FROM probe_jobs) as probe_jobs
    `);
    res.json({ seeding: seedingInProgress, progress: seedProgress, ...result.rows[0] });
  } catch {
    res.json({ seeding: seedingInProgress, progress: seedProgress, error: 'db_error' });
  }
});

// ── Helpers ───────────────────────────────────────────────────

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
  const isEpisode = seMatch !== null;

  // Extract clean title and year
  const yearMatch = name.match(/[\.\s\(]?((?:19|20)\d{2})[\.\s\)]/);
  const year = yearMatch ? parseInt(yearMatch[1]) : null;
  let cleanTitle = name.split(/[\.\s](?:(?:19|20)\d{2}|S\d{2}|2160p|1080p|720p|480p)/i)[0] || name;
  cleanTitle = cleanTitle.replace(/\./g, ' ').trim();

  return { resolution, codec, hdr, isEpisode, cleanTitle, year };
}

function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }

async function fetchText(url: string): Promise<string | null> {
  for (let i = 0; i < 3; i++) {
    try {
      const res = await fetch(url, {
        headers: { 'User-Agent': 'StreamDB/1.0', 'Accept': '*/*' },
        signal: AbortSignal.timeout(30000),
      });
      if (res.status === 429 || res.status === 403) {
        await sleep(10000 * (i + 1));
        continue;
      }
      if (!res.ok) return null;
      return await res.text();
    } catch {
      await sleep(3000 * (i + 1));
    }
  }
  return null;
}

async function fetchJson(url: string): Promise<any> {
  const text = await fetchText(url);
  if (!text) return null;
  try { return JSON.parse(text); } catch { return null; }
}

// ── DMM Decompression ─────────────────────────────────────────

interface RawHashEntry {
  filename: string;
  hash: string;
  bytes: number;
}

function decompressDMMHtml(html: string): RawHashEntry[] {
  const match = html.match(/src="[^"]*#([^"]+)"/);
  if (!match) return [];

  const decompressed = decompressFromBase64(match[1]);
  if (!decompressed) return [];

  try {
    const data = JSON.parse(decompressed);
    // DMM format: array or pseudo-array of { filename, hash, bytes }
    const items = Array.isArray(data)
      ? data
      : (typeof data === 'object' && data['0'] !== undefined)
        ? Object.values(data)
        : [];

    const results: RawHashEntry[] = [];
    for (const item of items) {
      if (!item || typeof item !== 'object') continue;
      const hash = (item.hash || item.infohash || '').toLowerCase();
      if (!hash || hash.length !== 40) continue;
      results.push({
        filename: item.filename || item.title || item.name || '',
        hash,
        bytes: item.bytes || item.filesize || item.size || 0,
      });
    }
    return results;
  } catch {
    return [];
  }
}

// ── TMDB Title Search ─────────────────────────────────────────

async function searchTMDB(title: string, year: number | null, type: 'movie' | 'tv', apiKey: string): Promise<string | null> {
  const params = new URLSearchParams({
    api_key: apiKey,
    query: title,
    ...(year ? { year: year.toString() } : {}),
  });

  const endpoint = type === 'tv' ? 'search/tv' : 'search/movie';
  const data = await fetchJson(`https://api.themoviedb.org/3/${endpoint}?${params}`);
  if (!data?.results?.length) return null;

  const tmdbId = data.results[0].id;
  const extPath = type === 'tv' ? `/tv/${tmdbId}/external_ids` : `/movie/${tmdbId}/external_ids`;
  const ext = await fetchJson(`https://api.themoviedb.org/3${extPath}?api_key=${apiKey}`);
  return ext?.imdb_id || null;
}

// ── Main Seed Pipeline ────────────────────────────────────────

const DMM_GITHUB_API = 'https://api.github.com/repos/debridmediamanager/hashlists/contents';
const DMM_RAW_BASE = 'https://raw.githubusercontent.com/debridmediamanager/hashlists/main';

async function runMatchPhase(tmdbKey: string) {
  const client = await pool.connect();
  let totalMatched = 0;

  try {
    logger.info('Seed Match: Starting TMDB matching for unmatched files');
    const totalFiles = (await pool.query('SELECT COUNT(*)::int as c FROM files')).rows[0].c;

    const unmatched = await client.query(`
      SELECT f.id, f.filename, f.infohash
      FROM files f
      LEFT JOIN content_files cf ON cf.file_id = f.id
      WHERE cf.file_id IS NULL AND f.filename != ''
      ORDER BY f.file_size DESC NULLS LAST
      LIMIT 10000
    `);

    seedProgress = { phase: 'matching_imdb', processed: 0, total: unmatched.rows.length, files: totalFiles, matched: 0 };
    logger.info(`Seed Match: ${unmatched.rows.length} unmatched files`);

    const titleCache = new Map<string, string | null>();
    let batchIdx = 0;
    await client.query('BEGIN');

    for (let i = 0; i < unmatched.rows.length; i++) {
      const row = unmatched.rows[i];
      const parsed = parseTitle(row.filename);
      if (!parsed.cleanTitle || parsed.cleanTitle.length < 2) continue;

      const searchKey = `${parsed.cleanTitle}|${parsed.year || ''}`;

      let imdbId: string | null;
      if (titleCache.has(searchKey)) {
        imdbId = titleCache.get(searchKey) || null;
      } else {
        const type = parsed.isEpisode ? 'tv' : 'movie';
        imdbId = await searchTMDB(parsed.cleanTitle, parsed.year, type, tmdbKey);
        titleCache.set(searchKey, imdbId);
        await sleep(250);
      }

      if (imdbId) {
        const cr = await client.query(`
          INSERT INTO content (imdb_id, type, title, year)
          VALUES ($1, $2, $3, $4)
          ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW()
          RETURNING id
        `, [imdbId, parsed.isEpisode ? 'series' : 'movie', parsed.cleanTitle, parsed.year]);

        if (cr.rows[0]) {
          await client.query(`
            INSERT INTO content_files (content_id, file_id, match_method, confidence)
            VALUES ($1, $2, 'tmdb_title_search', 0.6)
            ON CONFLICT DO NOTHING
          `, [cr.rows[0].id, row.id]);
        }
        totalMatched++;
        batchIdx++;
      }

      if (batchIdx >= 200) {
        await client.query('COMMIT');
        await client.query('BEGIN');
        batchIdx = 0;
      }

      seedProgress = { phase: 'matching_imdb', processed: i + 1, total: unmatched.rows.length, files: totalFiles, matched: totalMatched };

      if ((i + 1) % 200 === 0) {
        logger.info(`Seed Match: ${totalMatched}/${i + 1} matched (${titleCache.size} unique titles)`);
      }
    }

    await client.query('COMMIT');
    seedProgress = { ...seedProgress, phase: 'complete' };
    logger.info(`Seed Match complete: ${totalMatched} files matched to IMDB IDs`);
  } catch (err: any) {
    try { await client.query('ROLLBACK'); } catch {}
    logger.error('Seed Match error', { error: err.message });
    seedProgress = { ...seedProgress, phase: 'error' };
  } finally {
    client.release();
  }
}

async function runSeedPipeline(tmdbKey: string) {
  const client = await pool.connect();
  let totalFiles = 0;
  let totalMatched = 0;

  try {
    // ═══ PHASE 1: Import DMM hashes into files table ═══
    logger.info('Seed Phase 1: Importing DMM hashes from GitHub');
    seedProgress = { phase: 'listing', processed: 0, total: 0, files: 0, matched: 0 };

    const listText = await fetchText(DMM_GITHUB_API);
    if (!listText) {
      logger.error('Seed: Failed to fetch DMM file list');
      return;
    }

    let dmmFiles: { name: string; download_url: string }[];
    try {
      const listing = JSON.parse(listText);
      dmmFiles = listing.filter((f: any) => f.name.endsWith('.html') && f.type === 'file');
    } catch {
      logger.error('Seed: Failed to parse DMM listing');
      return;
    }

    logger.info(`Seed: Found ${dmmFiles.length} DMM hashlist files`);
    seedProgress = { phase: 'importing_hashes', processed: 0, total: dmmFiles.length, files: 0, matched: 0 };

    let batchCount = 0;
    await client.query('BEGIN');

    for (let i = 0; i < dmmFiles.length; i++) {
      const file = dmmFiles[i];
      const rawUrl = file.download_url || `${DMM_RAW_BASE}/${file.name}`;

      const html = await fetchText(rawUrl);
      if (!html) {
        await sleep(2000);
        continue;
      }

      const entries = decompressDMMHtml(html);
      if (!entries.length) {
        await sleep(500);
        continue;
      }

      for (const entry of entries) {
        const parsed = parseTitle(entry.filename);
        await client.query(`
          INSERT INTO files (infohash, file_idx, filename, file_size, torrent_name, resolution, video_codec, hdr, metadata_src, confidence)
          VALUES ($1, 0, $2, $3, $4, $5, $6, $7, 'dmm_hashlist', 0.3)
          ON CONFLICT (infohash, file_idx) DO NOTHING
        `, [entry.hash, entry.filename, entry.bytes || null, entry.filename, parsed.resolution, parsed.codec, parsed.hdr]);

        await client.query(`
          INSERT INTO probe_jobs (infohash, priority, source)
          VALUES ($1, 0, 'dmm_hashlist')
          ON CONFLICT (infohash) DO NOTHING
        `, [entry.hash]);

        totalFiles++;
        batchCount++;
      }

      if (batchCount >= 1000) {
        await client.query('COMMIT');
        await client.query('BEGIN');
        batchCount = 0;
      }

      seedProgress = { phase: 'importing_hashes', processed: i + 1, total: dmmFiles.length, files: totalFiles, matched: 0 };

      if ((i + 1) % 50 === 0 || i === 0) {
        logger.info(`Seed: [${i + 1}/${dmmFiles.length}] ${totalFiles.toLocaleString()} hashes imported`);
      }

      // Rate limit: ~1.5s between GitHub raw file requests
      await sleep(1500);
    }

    await client.query('COMMIT');
    logger.info(`Seed Phase 1 complete: ${totalFiles.toLocaleString()} hashes imported from ${dmmFiles.length} files`);

    // ═══ PHASE 2: Match filenames to IMDB IDs via TMDB ═══
    if (tmdbKey) {
      logger.info('Seed Phase 2: Matching filenames to IMDB IDs via TMDB');
      seedProgress = { phase: 'matching_imdb', processed: 0, total: 0, files: totalFiles, matched: 0 };

      // Get unmatched files (those without content_files edges)
      const unmatched = await client.query(`
        SELECT f.id, f.filename, f.infohash
        FROM files f
        LEFT JOIN content_files cf ON cf.file_id = f.id
        WHERE cf.file_id IS NULL AND f.filename != ''
        ORDER BY f.file_size DESC NULLS LAST
        LIMIT 5000
      `);

      seedProgress.total = unmatched.rows.length;
      logger.info(`Seed: ${unmatched.rows.length} unmatched files to process`);

      // Group by clean title to avoid duplicate TMDB lookups
      const titleCache = new Map<string, string | null>(); // cleanTitle -> imdbId
      let batchIdx = 0;
      await client.query('BEGIN');

      for (let i = 0; i < unmatched.rows.length; i++) {
        const row = unmatched.rows[i];
        const parsed = parseTitle(row.filename);
        const searchKey = `${parsed.cleanTitle}|${parsed.year || ''}`;

        let imdbId: string | null;
        if (titleCache.has(searchKey)) {
          imdbId = titleCache.get(searchKey) || null;
        } else {
          const type = parsed.isEpisode ? 'tv' : 'movie';
          imdbId = await searchTMDB(parsed.cleanTitle, parsed.year, type, tmdbKey);
          titleCache.set(searchKey, imdbId);
          await sleep(250); // TMDB rate limit: ~40 req/s
        }

        if (imdbId) {
          const cr = await client.query(`
            INSERT INTO content (imdb_id, type, title, year)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW()
            RETURNING id
          `, [imdbId, parsed.isEpisode ? 'series' : 'movie', parsed.cleanTitle, parsed.year]);

          if (cr.rows[0]) {
            await client.query(`
              INSERT INTO content_files (content_id, file_id, match_method, confidence)
              VALUES ($1, $2, 'tmdb_title_search', 0.6)
              ON CONFLICT DO NOTHING
            `, [cr.rows[0].id, row.id]);
          }

          totalMatched++;
          batchIdx++;
        }

        if (batchIdx >= 200) {
          await client.query('COMMIT');
          await client.query('BEGIN');
          batchIdx = 0;
        }

        seedProgress = { phase: 'matching_imdb', processed: i + 1, total: unmatched.rows.length, files: totalFiles, matched: totalMatched };

        if ((i + 1) % 100 === 0) {
          logger.info(`Seed: Matched ${totalMatched}/${i + 1} files to IMDB IDs (${titleCache.size} unique titles)`);
        }
      }

      await client.query('COMMIT');
      logger.info(`Seed Phase 2 complete: ${totalMatched} files matched to IMDB IDs`);
    } else {
      logger.info('Seed: Skipping Phase 2 (no TMDB_API_KEY configured)');
    }

    seedProgress = { phase: 'complete', processed: seedProgress.total, total: seedProgress.total, files: totalFiles, matched: totalMatched };
    logger.info(`Seed complete! Files: ${totalFiles.toLocaleString()}, Matched: ${totalMatched.toLocaleString()}`);

  } catch (err: any) {
    try { await client.query('ROLLBACK'); } catch {}
    logger.error('Seed error', { error: err.message });
    seedProgress = { ...seedProgress, phase: 'error' };
  } finally {
    client.release();
  }
}

export default router;
