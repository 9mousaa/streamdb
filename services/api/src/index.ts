import express from 'express';
import { config } from './config.js';
import { initDatabase } from './db/pool.js';
import { logger } from './utils/logger.js';
import stremioRoutes from './routes/stremio.js';
import healthRoutes from './routes/health.js';
import configureRoutes from './routes/configure.js';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

const app = express();

// CORS for Stremio
app.use((_req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', '*');
  next();
});

app.use(express.json());
app.use(express.static(join(__dirname, '../../../public')));

// Routes
app.use(healthRoutes);
app.use(configureRoutes);
app.use(stremioRoutes);

async function start() {
  try {
    await initDatabase();
    app.listen(config.port, () => {
      logger.info(`StreamDB API listening on port ${config.port}`);
    });
  } catch (err: any) {
    logger.error('Failed to start', { error: err.message });
    process.exit(1);
  }
}

start();
