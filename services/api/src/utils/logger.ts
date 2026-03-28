const LEVELS = { error: 0, warn: 1, info: 2, debug: 3 } as const;
type Level = keyof typeof LEVELS;

const currentLevel = LEVELS[(process.env.LOG_LEVEL as Level) || 'info'] ?? LEVELS.info;

function log(level: Level, msg: string, data?: Record<string, unknown>) {
  if (LEVELS[level] > currentLevel) return;
  const ts = new Date().toISOString();
  const line = data ? `${ts} [${level.toUpperCase()}] ${msg} ${JSON.stringify(data)}` : `${ts} [${level.toUpperCase()}] ${msg}`;
  if (level === 'error') console.error(line);
  else console.log(line);
}

export const logger = {
  error: (msg: string, data?: Record<string, unknown>) => log('error', msg, data),
  warn: (msg: string, data?: Record<string, unknown>) => log('warn', msg, data),
  info: (msg: string, data?: Record<string, unknown>) => log('info', msg, data),
  debug: (msg: string, data?: Record<string, unknown>) => log('debug', msg, data),
};
