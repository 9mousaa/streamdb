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
    // Run migration
    const { readFileSync } = await import('fs');
    const { dirname, join } = await import('path');
    const { fileURLToPath } = await import('url');
    const __dirname = dirname(fileURLToPath(import.meta.url));
    const schema = readFileSync(join(__dirname, '../../../../shared/db/migrations/001_initial.sql'), 'utf-8');
    await client.query(schema);
    logger.info('Database migrations applied');
  } finally {
    client.release();
  }
}
