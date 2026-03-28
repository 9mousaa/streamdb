export const config = {
  port: parseInt(process.env.PORT || '3000'),
  nodeEnv: process.env.NODE_ENV || 'development',
  logLevel: process.env.LOG_LEVEL || 'info',
  databaseUrl: process.env.DATABASE_URL || 'postgres://streamdb:streamdb@localhost:5432/streamdb',
  baseUrl: process.env.BASE_URL || 'http://localhost:3000',
} as const;
