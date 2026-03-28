import { pool } from '../db/pool.js';
import { config } from '../config.js';
import { logger } from '../utils/logger.js';

const TMDB_BASE = 'https://api.themoviedb.org/3';

interface TMDBFindResult {
  movie_results: Array<{ id: number }>;
  tv_results: Array<{ id: number }>;
}

interface TMDBExternalIds {
  imdb_id?: string;
  tvdb_id?: number;
  wikidata_id?: string;
}

interface TMDBDetails {
  runtime?: number; // minutes (movies)
  episode_run_time?: number[]; // minutes (TV)
}

export async function enrichContentNode(imdbId: string): Promise<number | null> {
  if (!config.tmdbApiKey) return null;

  // Check if already enriched
  const existing = await pool.query(
    'SELECT tmdb_id FROM content WHERE imdb_id = $1 AND tmdb_id IS NOT NULL',
    [imdbId]
  );
  if (existing.rows.length > 0) return null;

  // Find TMDB ID from IMDB ID
  const findUrl = `${TMDB_BASE}/find/${imdbId}?external_source=imdb_id&api_key=${config.tmdbApiKey}`;
  const findRes = await fetch(findUrl);
  if (!findRes.ok) {
    logger.warn('TMDB find failed', { imdbId, status: findRes.status });
    return null;
  }

  const findData = await findRes.json() as TMDBFindResult;
  const movie = findData.movie_results[0];
  const tv = findData.tv_results[0];
  if (!movie && !tv) return null;

  const tmdbId = movie?.id ?? tv?.id;
  const type = movie ? 'movie' : 'tv';

  // Get all external IDs
  const extUrl = `${TMDB_BASE}/${type}/${tmdbId}/external_ids?api_key=${config.tmdbApiKey}`;
  const extRes = await fetch(extUrl);
  if (!extRes.ok) {
    // Still update tmdb_id even if external_ids fails
    await pool.query('UPDATE content SET tmdb_id = $1 WHERE imdb_id = $2', [tmdbId, imdbId]);
    return null;
  }

  const extData = await extRes.json() as TMDBExternalIds;

  // Get runtime/duration details
  let durationSec: number | null = null;
  try {
    const detailUrl = `${TMDB_BASE}/${type}/${tmdbId}?api_key=${config.tmdbApiKey}`;
    const detailRes = await fetch(detailUrl);
    if (detailRes.ok) {
      const details = await detailRes.json() as TMDBDetails;
      if (details.runtime) {
        durationSec = details.runtime * 60; // Convert minutes to seconds
      } else if (details.episode_run_time?.length) {
        durationSec = details.episode_run_time[0] * 60;
      }
    }
  } catch { /* non-fatal */ }

  const runtimeMinutes = durationSec ? Math.round(durationSec / 60) : null;

  await pool.query(
    `UPDATE content SET tmdb_id = $1, tvdb_id = COALESCE($2, tvdb_id),
     wikidata_id = COALESCE($3, wikidata_id),
     runtime_minutes = COALESCE($5, runtime_minutes)
     WHERE imdb_id = $4`,
    [tmdbId, extData.tvdb_id ?? null, extData.wikidata_id ?? null, imdbId, runtimeMinutes]
  );

  logger.debug('Enriched content', { imdbId, tmdbId, tvdbId: extData.tvdb_id, runtimeMinutes });
  return durationSec;
}

/**
 * Background worker that enriches content nodes with TMDB data.
 * Processes one content item per tick.
 */
let enrichRunning = false;

export function startTmdbEnrichmentWorker(intervalMs = 10_000): void {
  logger.info('TMDB enrichment worker started', { intervalMs });
  setInterval(() => enrichNext().catch(err =>
    logger.error('TMDB enrichment tick failed', { error: err.message })
  ), intervalMs);
}

async function enrichNext(): Promise<void> {
  if (enrichRunning || !config.tmdbApiKey) return;
  enrichRunning = true;
  try {
    // Find a content node with imdb_id but no tmdb_id
    const result = await pool.query(`
      SELECT imdb_id FROM content
      WHERE imdb_id IS NOT NULL AND imdb_id LIKE 'tt%'
        AND tmdb_id IS NULL
      ORDER BY RANDOM()
      LIMIT 1
    `);
    if (!result.rows.length) return;

    await enrichContentNode(result.rows[0].imdb_id);
  } finally {
    enrichRunning = false;
  }
}
