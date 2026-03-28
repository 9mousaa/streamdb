import { Router } from 'express';
import { pool } from '../db/pool.js';

const router = Router();

// Stats endpoint for dashboard
router.get('/api/stats', async (_req, res) => {
  try {
    const [content, files, edges, debrid] = await Promise.all([
      pool.query('SELECT COUNT(*) as count FROM content'),
      pool.query('SELECT COUNT(*) as count FROM files'),
      pool.query('SELECT COUNT(*) as count FROM content_files'),
      pool.query('SELECT COUNT(*) as count FROM debrid_cache WHERE available = TRUE'),
    ]);

    res.json({
      content: parseInt(content.rows[0].count),
      files: parseInt(files.rows[0].count),
      mappings: parseInt(edges.rows[0].count),
      debridCached: parseInt(debrid.rows[0].count),
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
