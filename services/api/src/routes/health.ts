import { Router } from 'express';
import { pool } from '../db/pool.js';
import { logger } from '../utils/logger.js';

const router = Router();

router.get('/health', async (_req, res) => {
  try {
    await pool.query('SELECT 1');

    // Include probe stats if available
    let probe: Record<string, number> | undefined;
    try {
      const result = await pool.query(`SELECT status, COUNT(*)::int as count FROM probe_jobs GROUP BY status`);
      probe = { pending: 0, processing: 0, completed: 0, failed: 0 };
      for (const row of result.rows) probe[row.status] = row.count;
    } catch { /* probe_jobs table may not exist yet */ }

    res.json({ status: 'ok', timestamp: new Date().toISOString(), probe });
  } catch {
    res.status(503).json({ status: 'error', timestamp: new Date().toISOString() });
  }
});

// Stats endpoint (used by configure.html)
router.get('/api/stats', async (_req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        (SELECT COUNT(*)::int FROM content) as content,
        (SELECT COUNT(*)::int FROM files) as files,
        (SELECT COUNT(*)::int FROM content_files) as mappings,
        (SELECT COUNT(*)::int FROM debrid_cache WHERE available = true) as "debridCached"
    `);
    res.json(result.rows[0]);
  } catch {
    res.json({ content: 0, files: 0, mappings: 0, debridCached: 0 });
  }
});

// Seed endpoint — triggers Torrentio-based seeding in-process
let seedingInProgress = false;

router.post('/api/seed', async (req, res) => {
  if (seedingInProgress) {
    return res.json({ status: 'already_running' });
  }

  seedingInProgress = true;
  const mode = req.body?.mode || 'popular'; // 'popular' uses hardcoded list
  res.json({ status: 'started', mode });

  // Run seeding in background
  seedFromTorrentio().finally(() => { seedingInProgress = false; });
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
    res.json({ seeding: seedingInProgress, ...result.rows[0] });
  } catch {
    res.json({ seeding: seedingInProgress, error: 'db_error' });
  }
});

// ── Inline Torrentio seeder (runs in-process) ────────────────

const TORRENTIO_BASE = 'https://torrentio.strem.fun';

function sleepMs(ms: number) { return new Promise(r => setTimeout(r, ms)); }

async function fetchJsonSafe(url: string): Promise<any> {
  for (let i = 0; i < 3; i++) {
    try {
      const res = await fetch(url, {
        headers: { 'User-Agent': 'StreamDB/1.0' },
        signal: AbortSignal.timeout(15000),
      });
      if (res.status === 429) { await sleepMs(5000); continue; }
      if (!res.ok) return null;
      return await res.json();
    } catch {
      await sleepMs(2000 * (i + 1));
    }
  }
  return null;
}

function parseTorrentioTitle(title: string) {
  let resolution: string | null = null;
  if (/2160p|4k|uhd/i.test(title)) resolution = '2160p';
  else if (/1080p/i.test(title)) resolution = '1080p';
  else if (/720p/i.test(title)) resolution = '720p';
  else if (/480p/i.test(title)) resolution = '480p';

  let codec: string | null = null;
  if (/x265|hevc|h\.?265/i.test(title)) codec = 'x265';
  else if (/x264|h\.?264|avc/i.test(title)) codec = 'x264';

  let hdr: string | null = 'SDR';
  if (/dolby[\.\s-]?vision|[\.\s]dv[\.\s]|dovi/i.test(title)) hdr = 'DV';
  else if (/hdr10\+|hdr10plus/i.test(title)) hdr = 'HDR10+';
  else if (/hdr10|hdr/i.test(title)) hdr = 'HDR10';

  let size: number | null = null;
  const sizeMatch = title.match(/([\d.]+)\s*GB/i);
  if (sizeMatch) size = Math.round(parseFloat(sizeMatch[1]) * 1024 * 1024 * 1024);

  return { resolution, codec, hdr, size };
}

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
  'tt1517268','tt9362722','tt6263850','tt10872600','tt13238346','tt14539740','tt15239678',
  'tt11866324','tt14230458','tt21692408','tt5433140','tt9603212','tt14998742',
];

const POPULAR_SERIES = [
  { imdb: 'tt0903747', seasons: 5, title: 'Breaking Bad' },
  { imdb: 'tt0944947', seasons: 8, title: 'Game of Thrones' },
  { imdb: 'tt0306414', seasons: 5, title: 'The Wire' },
  { imdb: 'tt4574334', seasons: 4, title: 'Stranger Things' },
  { imdb: 'tt7366338', seasons: 1, title: 'Chernobyl' },
  { imdb: 'tt5180504', seasons: 3, title: 'The Witcher' },
  { imdb: 'tt3032476', seasons: 6, title: 'Better Call Saul' },
  { imdb: 'tt1190634', seasons: 4, title: 'The Boys' },
  { imdb: 'tt11198330', seasons: 2, title: 'House of the Dragon' },
  { imdb: 'tt7660850', seasons: 4, title: 'Succession' },
  { imdb: 'tt14688458', seasons: 1, title: 'Shogun' },
  { imdb: 'tt15384528', seasons: 1, title: 'Fallout' },
  { imdb: 'tt13443470', seasons: 2, title: 'Wednesday' },
  { imdb: 'tt2356777', seasons: 4, title: 'True Detective' },
  { imdb: 'tt0141842', seasons: 6, title: 'The Sopranos' },
];

async function seedFromTorrentio() {
  const client = await pool.connect();
  let total = 0;

  try {
    logger.info('Seed: Starting Torrentio import');

    // Movies
    for (const imdbId of POPULAR_MOVIES) {
      try {
        const data = await fetchJsonSafe(`${TORRENTIO_BASE}/stream/movie/${imdbId}.json`);
        if (!data?.streams?.length) continue;

        await client.query('BEGIN');
        for (const s of data.streams) {
          if (!s.infoHash || s.infoHash.length !== 40) continue;
          const hash = s.infoHash.toLowerCase();
          const title = (s.title || '').split('\n')[0].replace(/[^\x20-\x7E]/g, '').trim();
          const parsed = parseTorrentioTitle(s.title || '');

          await client.query(`INSERT INTO files (infohash, file_idx, filename, file_size, torrent_name, resolution, video_codec, hdr, metadata_src, confidence) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'torrentio_seed', 0.4) ON CONFLICT (infohash, file_idx) DO NOTHING`,
            [hash, s.fileIdx || 0, title, parsed.size, title, parsed.resolution, parsed.codec, parsed.hdr]);

          const cr = await client.query(`INSERT INTO content (imdb_id, type, title) VALUES ($1, 'movie', $2) ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW() RETURNING id`, [imdbId, title]);
          const fr = await client.query('SELECT id FROM files WHERE infohash = $1 AND file_idx = $2', [hash, s.fileIdx || 0]);
          if (fr.rows[0] && cr.rows[0]) {
            await client.query(`INSERT INTO content_files (content_id, file_id, match_method, confidence) VALUES ($1, $2, 'torrentio_seed', 0.8) ON CONFLICT DO NOTHING`, [cr.rows[0].id, fr.rows[0].id]);
          }
          await client.query(`INSERT INTO probe_jobs (infohash, priority, source) VALUES ($1, 1, 'torrentio_seed') ON CONFLICT (infohash) DO NOTHING`, [hash]);
          total++;
        }
        await client.query('COMMIT');
        await sleepMs(800);
      } catch (err: any) {
        try { await client.query('ROLLBACK'); } catch {}
        logger.debug('Seed movie error', { imdbId, error: err.message });
      }
    }

    logger.info(`Seed: Movies done, ${total} streams`);

    // Series
    for (const series of POPULAR_SERIES) {
      for (let s = 1; s <= series.seasons; s++) {
        for (let e = 1; e <= 15; e++) {
          try {
            const id = `${series.imdb}:${s}:${e}`;
            const data = await fetchJsonSafe(`${TORRENTIO_BASE}/stream/series/${id}.json`);
            if (!data?.streams?.length && e > 2) break;
            if (!data?.streams?.length) continue;

            await client.query('BEGIN');
            for (const st of data.streams) {
              if (!st.infoHash || st.infoHash.length !== 40) continue;
              const hash = st.infoHash.toLowerCase();
              const title = (st.title || '').split('\n')[0].replace(/[^\x20-\x7E]/g, '').trim();
              const parsed = parseTorrentioTitle(st.title || '');

              await client.query(`INSERT INTO files (infohash, file_idx, filename, file_size, torrent_name, resolution, video_codec, hdr, metadata_src, confidence) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'torrentio_seed', 0.4) ON CONFLICT (infohash, file_idx) DO NOTHING`,
                [hash, st.fileIdx || 0, title, parsed.size, title, parsed.resolution, parsed.codec, parsed.hdr]);

              const contentKey = `${series.imdb}:${s}:${e}`;
              const cr = await client.query(`INSERT INTO content (imdb_id, type, title, season, episode, parent_imdb) VALUES ($1, 'episode', $2, $3, $4, $5) ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW() RETURNING id`,
                [contentKey, series.title, s, e, series.imdb]);
              const fr = await client.query('SELECT id FROM files WHERE infohash = $1 AND file_idx = $2', [hash, st.fileIdx || 0]);
              if (fr.rows[0] && cr.rows[0]) {
                await client.query(`INSERT INTO content_files (content_id, file_id, match_method, confidence) VALUES ($1, $2, 'torrentio_seed', 0.8) ON CONFLICT DO NOTHING`, [cr.rows[0].id, fr.rows[0].id]);
              }
              await client.query(`INSERT INTO probe_jobs (infohash, priority, source) VALUES ($1, 1, 'torrentio_seed') ON CONFLICT (infohash) DO NOTHING`, [hash]);
              total++;
            }
            await client.query('COMMIT');
            await sleepMs(800);
          } catch (err: any) {
            try { await client.query('ROLLBACK'); } catch {}
          }
        }
      }
    }

    logger.info(`Seed: Complete! Total ${total} streams imported`);
  } finally {
    client.release();
  }
}

export default router;
