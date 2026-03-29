import { Router } from 'express';
import { pool } from '../db/pool.js';

const router = Router();

router.get('/health', async (_req, res) => {
  try {
    await pool.query('SELECT 1');

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

// Debug: check what data exists for an IMDB ID
router.get('/api/debug/:imdbId', async (req, res) => {
  try {
    const imdbId = req.params.imdbId;
    const content = await pool.query('SELECT * FROM content WHERE imdb_id = $1', [imdbId]);
    const files = await pool.query(`
      SELECT f.infohash, f.file_idx, f.filename, f.file_size, f.resolution, f.video_codec, f.hdr, f.metadata_src, f.confidence, cf.match_method
      FROM files f JOIN content_files cf ON cf.file_id = f.id JOIN content c ON c.id = cf.content_id
      WHERE c.imdb_id = $1 ORDER BY f.confidence DESC LIMIT 20
    `, [imdbId]);
    const debrid = await pool.query(`
      SELECT dc.infohash, dc.service, dc.available, dc.expires_at
      FROM debrid_cache dc WHERE dc.infohash = ANY(SELECT f.infohash FROM files f JOIN content_files cf ON cf.file_id = f.id JOIN content c ON c.id = cf.content_id WHERE c.imdb_id = $1)
    `, [imdbId]);
    res.json({ content: content.rows, files: files.rows, debridCache: debrid.rows });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Debug: recently probed jobs
router.get('/api/probes', async (req, res) => {
  try {
    const status = (req.query.status as string) || 'completed';
    const limit = Math.min(parseInt(req.query.limit as string) || 20, 100);
    const result = await pool.query(`
      SELECT pj.id, pj.infohash, pj.status, pj.error, pj.attempts,
             pj.completed_at, pj.created_at,
             f.filename, f.torrent_name, f.file_size, f.resolution,
             f.video_codec, f.os_hash, f.metadata_src
      FROM probe_jobs pj
      LEFT JOIN files f ON f.infohash = pj.infohash AND f.file_idx = 0
      WHERE pj.status = $1
      ORDER BY pj.completed_at DESC NULLS LAST, pj.id DESC
      LIMIT $2
    `, [status, limit]);
    res.json(result.rows);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Seed progress — track toward 1M target
router.get('/api/seed-progress', async (_req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        (SELECT COUNT(*)::int FROM content) as content_count,
        (SELECT COUNT(*)::int FROM files) as file_count,
        (SELECT COUNT(*)::int FROM content_files) as edge_count,
        (SELECT COUNT(*)::int FROM content_files WHERE match_method IN ('zilean_api', 'zilean')) as zilean_edges,
        (SELECT COUNT(*)::int FROM content_files WHERE match_method = 'tmdb_title_search') as tmdb_edges,
        (SELECT COUNT(*)::int FROM content_files WHERE match_method = 'title_match') as title_match_edges,
        (SELECT COUNT(*)::int FROM probe_jobs WHERE status = 'pending') as probes_pending,
        (SELECT COUNT(*)::int FROM probe_jobs WHERE status = 'completed') as probes_completed,
        (SELECT COUNT(*)::int FROM probe_jobs WHERE status = 'failed') as probes_failed,
        (SELECT COUNT(*)::int FROM probe_jobs WHERE status = 'processing') as probes_processing,
        (SELECT COUNT(*)::int FROM files WHERE resolution IS NOT NULL) as files_with_resolution,
        (SELECT COUNT(*)::int FROM files WHERE os_hash IS NOT NULL) as files_with_hash
    `);
    const r = result.rows[0];
    const target = 1_000_000;
    res.json({
      ...r,
      target,
      progress_pct: parseFloat((r.edge_count / target * 100).toFixed(2)),
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Admin: clear debrid cache (stale entries from bad API keys)
router.delete('/api/debrid-cache', async (req, res) => {
  try {
    const imdbId = req.query.imdb as string | undefined;
    if (imdbId) {
      const result = await pool.query(`
        DELETE FROM debrid_cache WHERE infohash = ANY(
          SELECT f.infohash FROM files f JOIN content_files cf ON cf.file_id = f.id
          JOIN content c ON c.id = cf.content_id WHERE c.imdb_id = $1
        )`, [imdbId]);
      res.json({ deleted: result.rowCount, scope: imdbId });
    } else {
      const result = await pool.query('DELETE FROM debrid_cache');
      res.json({ deleted: result.rowCount, scope: 'all' });
    }
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
