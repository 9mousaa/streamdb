export const config = {
  port: parseInt(process.env.PORT || '3000'),
  nodeEnv: process.env.NODE_ENV || 'development',
  logLevel: process.env.LOG_LEVEL || 'info',
  databaseUrl: process.env.DATABASE_URL || 'postgres://streamdb:streamdb@localhost:5432/streamdb',
  baseUrl: process.env.BASE_URL || 'http://localhost:3000',
  tmdbApiKey: process.env.TMDB_API_KEY || '',
  probeIntervalMs: parseInt(process.env.PROBE_INTERVAL_MS || '30000'),
  dhtEnabled: process.env.DHT_ENABLED === 'true',
  dhtPort: parseInt(process.env.DHT_PORT || '6881'),
  trailerioBaseUrl: process.env.TRAILERIO_BASE_URL || 'https://trailerio.plaio.cc/api',
} as const;
