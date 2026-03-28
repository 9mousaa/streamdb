import { Router } from 'express';
import { pool } from '../db/pool.js';

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

export default router;
