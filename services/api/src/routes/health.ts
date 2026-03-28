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

export default router;
