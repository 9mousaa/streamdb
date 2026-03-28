import pg from 'pg';
import { config } from '../config.js';
import { logger } from '../utils/logger.js';

const { Pool } = pg;

export const pool = new Pool({
  connectionString: config.databaseUrl,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

pool.on('error', (err) => {
  logger.error('Unexpected PostgreSQL pool error', { error: err.message });
});

export async function initDatabase(): Promise<void> {
  const client = await pool.connect();
  try {
    logger.info('Connected to PostgreSQL');

    // Create migration tracking table
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        filename TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    // Scan and apply migrations in order
    const { readdirSync, readFileSync } = await import('fs');
    const { dirname, join } = await import('path');
    const { fileURLToPath } = await import('url');
    const __dirname = dirname(fileURLToPath(import.meta.url));
    const migrationsDir = join(__dirname, '../../../../shared/db/migrations');

    const files = readdirSync(migrationsDir)
      .filter(f => f.endsWith('.sql') && !f.startsWith('init-'))
      .sort();

    const applied = await client.query('SELECT filename FROM schema_migrations');
    const appliedSet = new Set(applied.rows.map((r: any) => r.filename));

    for (const file of files) {
      if (appliedSet.has(file)) continue;
      const sql = readFileSync(join(migrationsDir, file), 'utf-8');
      await client.query('BEGIN');
      try {
        await client.query(sql);
        await client.query('INSERT INTO schema_migrations (filename) VALUES ($1)', [file]);
        await client.query('COMMIT');
        logger.info(`Applied migration: ${file}`);
      } catch (err: any) {
        await client.query('ROLLBACK');
        throw new Error(`Migration ${file} failed: ${err.message}`);
      }
    }

    logger.info('Database migrations up to date');
  } finally {
    client.release();
  }
}
