import { Router } from 'express';
import { pool } from '../db/pool.js';
import { logger } from '../utils/logger.js';
import { decompressFromBase64 } from '../utils/lz-string.js';

const router = Router();

let seedingInProgress = false;
let seedProgress = { phase: '', processed: 0, total: 0 };

router.post('/api/seed', async (req, res) => {
  if (seedingInProgress) {
    return res.json({ status: 'already_running', progress: seedProgress });
  }

  seedingInProgress = true;
  seedProgress = { phase: 'starting', processed: 0, total: 0 };
  res.json({ status: 'started' });

  seedFromDMM().finally(() => { seedingInProgress = false; });
});

// Accept bulk hash data via POST (for external scripts to push data in)
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

      await client.query(`
        INSERT INTO files (infohash, file_idx, filename, file_size, torrent_name, resolution, video_codec, hdr, metadata_src, confidence)
        VALUES ($1, 0, $2, $3, $4, $5, $6, $7, 'bulk_seed', 0.4)
        ON CONFLICT (infohash, file_idx) DO NOTHING
      `, [hash, entry.title || '', entry.size || null, entry.title || '', parsed.resolution, parsed.codec, parsed.hdr]);

      const cr = await client.query(`
        INSERT INTO content (imdb_id, type, title)
        VALUES ($1, $2, $3)
        ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW()
        RETURNING id
      `, [imdb, parsed.isEpisode ? 'episode' : 'movie', entry.title || imdb]);

      const fr = await client.query('SELECT id FROM files WHERE infohash = $1 AND file_idx = 0', [hash]);
      if (fr.rows[0] && cr.rows[0]) {
        await client.query(`
          INSERT INTO content_files (content_id, file_id, match_method, confidence)
          VALUES ($1, $2, 'bulk_seed', 0.7)
          ON CONFLICT DO NOTHING
        `, [cr.rows[0].id, fr.rows[0].id]);
      }

      await client.query(`
        INSERT INTO probe_jobs (infohash, priority, source)
        VALUES ($1, 1, 'bulk_seed')
        ON CONFLICT (infohash) DO NOTHING
      `, [hash]);

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

// ── Title parser ──────────────────────────────────────────────

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

  return { resolution, codec, hdr, isEpisode };
}

// ── DMM Hashlist Seeder ───────────────────────────────────────

const DMM_GITHUB_API = 'https://api.github.com/repos/debridmediamanager/hashlists/contents';
const DMM_RAW_BASE = 'https://raw.githubusercontent.com/debridmediamanager/hashlists/main';

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

function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }

interface DMMHashEntry {
  filename: string;
  hash: string;
  bytes: number;
}

function parseDMMHtml(html: string): { imdbId: string | null; entries: DMMHashEntry[] } {
  // Extract the LZ-string data from iframe src after #
  const match = html.match(/src="[^"]*#([^"]+)"/);
  if (!match) return { imdbId: null, entries: [] };

  const compressed = match[1];
  const decompressed = decompressFromBase64(compressed);
  if (!decompressed) return { imdbId: null, entries: [] };

  try {
    const data = JSON.parse(decompressed);
    // DMM format: { imdbId?: string, files?: [...], hashes?: [...] } or array of entries
    if (Array.isArray(data)) {
      return {
        imdbId: null,
        entries: data.filter((e: any) => e.hash && e.filename).map((e: any) => ({
          filename: e.filename || e.title || '',
          hash: (e.hash || '').toLowerCase(),
          bytes: e.bytes || e.filesize || e.size || 0,
        })),
      };
    }

    // Object format
    const imdbId = data.imdbId || data.imdb_id || null;
    const files = data.files || data.hashes || data.torrents || [];
    return {
      imdbId,
      entries: Array.isArray(files) ? files.filter((e: any) => e.hash && e.filename).map((e: any) => ({
        filename: e.filename || e.title || '',
        hash: (e.hash || '').toLowerCase(),
        bytes: e.bytes || e.filesize || e.size || 0,
      })) : [],
    };
  } catch {
    return { imdbId: null, entries: [] };
  }
}

async function importDMMEntries(
  client: any,
  imdbId: string,
  entries: DMMHashEntry[],
): Promise<number> {
  let imported = 0;

  for (const entry of entries) {
    if (entry.hash.length !== 40) continue;
    const parsed = parseTitle(entry.filename);

    await client.query(`
      INSERT INTO files (infohash, file_idx, filename, file_size, torrent_name, resolution, video_codec, hdr, metadata_src, confidence)
      VALUES ($1, 0, $2, $3, $4, $5, $6, $7, 'dmm_hashlist', 0.3)
      ON CONFLICT (infohash, file_idx) DO NOTHING
    `, [entry.hash, entry.filename, entry.bytes || null, entry.filename, parsed.resolution, parsed.codec, parsed.hdr]);

    const cr = await client.query(`
      INSERT INTO content (imdb_id, type, title)
      VALUES ($1, 'movie', $2)
      ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW()
      RETURNING id
    `, [imdbId, entry.filename.split(/[\.\s](?:(?:19|20)\d{2})/)[0]?.replace(/\./g, ' ').trim() || imdbId]);

    const fr = await client.query('SELECT id FROM files WHERE infohash = $1 AND file_idx = 0', [entry.hash]);
    if (fr.rows[0] && cr.rows[0]) {
      await client.query(`
        INSERT INTO content_files (content_id, file_id, match_method, confidence)
        VALUES ($1, $2, 'dmm_hashlist', 0.7)
        ON CONFLICT DO NOTHING
      `, [cr.rows[0].id, fr.rows[0].id]);
    }

    await client.query(`
      INSERT INTO probe_jobs (infohash, priority, source)
      VALUES ($1, 0, 'dmm_hashlist')
      ON CONFLICT (infohash) DO NOTHING
    `, [entry.hash]);

    imported++;
  }

  return imported;
}

async function seedFromDMM() {
  const client = await pool.connect();
  let total = 0;

  try {
    logger.info('Seed: Starting DMM hashlist import from GitHub');
    seedProgress = { phase: 'listing', processed: 0, total: 0 };

    // Get list of HTML files from DMM repo
    const listText = await fetchText(DMM_GITHUB_API);
    if (!listText) {
      logger.error('Seed: Failed to fetch DMM file list from GitHub');
      return;
    }

    let files: { name: string; download_url: string }[];
    try {
      const listing = JSON.parse(listText);
      files = listing.filter((f: any) => f.name.endsWith('.html') && f.type === 'file');
    } catch {
      logger.error('Seed: Failed to parse DMM file list');
      return;
    }

    logger.info(`Seed: Found ${files.length} DMM hashlist files`);
    seedProgress = { phase: 'importing', processed: 0, total: files.length };

    let batchCount = 0;
    await client.query('BEGIN');

    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      const rawUrl = file.download_url || `${DMM_RAW_BASE}/${file.name}`;

      const html = await fetchText(rawUrl);
      if (!html) {
        logger.debug(`Seed: Failed to download ${file.name}`);
        await sleep(2000);
        continue;
      }

      const { imdbId, entries } = parseDMMHtml(html);
      if (!entries.length) {
        await sleep(500);
        continue;
      }

      // Use filename-derived IMDB ID or embedded one
      const id = imdbId || `dmm_${file.name.replace('.html', '')}`;

      const count = await importDMMEntries(client, id, entries);
      total += count;
      batchCount += count;

      if (batchCount >= 500) {
        await client.query('COMMIT');
        await client.query('BEGIN');
        batchCount = 0;
      }

      seedProgress = { phase: 'importing', processed: i + 1, total: files.length };
      logger.debug(`Seed: [${i + 1}/${files.length}] ${file.name}: ${count} hashes (total: ${total})`);

      // Rate limit GitHub
      await sleep(1500);
    }

    await client.query('COMMIT');
    seedProgress = { phase: 'complete', processed: files.length, total: files.length };
    logger.info(`Seed: DMM import complete! Total: ${total} streams`);
  } catch (err: any) {
    try { await client.query('ROLLBACK'); } catch {}
    logger.error('Seed error', { error: err.message });
    seedProgress = { phase: 'error', processed: seedProgress.processed, total: seedProgress.total };
  } finally {
    client.release();
  }
}

export default router;
