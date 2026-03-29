import express from 'express';
import { config } from './config.js';
import { initDatabase } from './db/pool.js';
import { logger } from './utils/logger.js';
import stremioRoutes from './routes/stremio.js';
import healthRoutes from './routes/health.js';
import configureRoutes from './routes/configure.js';
import seedRoutes from './routes/seed.js';
import hlsRoutes from './routes/hls.js';
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

app.use(express.json({ limit: '50mb' }));
app.use(express.static(join(__dirname, '../../../public')));

// Serve configure.html at /configure (Stremio expects this path)
app.get('/configure', (_req, res) => {
  res.sendFile(join(__dirname, '../../../public/configure.html'));
});

// Routes
app.use(healthRoutes);
app.use(seedRoutes);
app.use(configureRoutes);
app.use(hlsRoutes);
app.use(stremioRoutes);

async function start() {
  try {
    await initDatabase();

    // Start probe worker (torrent-based, uses DHT for peer discovery)
    if (config.dhtEnabled) {
      const { startProbeWorker } = await import('./probe/worker.js');
      startProbeWorker(config.probeIntervalMs, config.probeConcurrency);
    }

    // Start DHT passive listener if enabled
    if (config.dhtEnabled) {
      const { startDHTCrawler } = await import('./crawler/processor.js');
      startDHTCrawler();
    }

    // Start reference frame builder (for phash content matching)
    {
      const { startReferenceBuilder } = await import('./recognition/build-references.js');
      startReferenceBuilder();
    }

    // Start TMDB enrichment worker (runtime data for duration matching + identity graph)
    if (config.tmdbApiKey) {
      const { startTmdbEnrichmentWorker } = await import('./metadata/tmdb.js');
      startTmdbEnrichmentWorker(10_000); // One enrichment every 10s
    }

    app.listen(config.port, () => {
      logger.info(`StreamDB API listening on port ${config.port}`);
    });
  } catch (err: any) {
    logger.error('Failed to start', { error: err.message });
    process.exit(1);
  }
}

start();
