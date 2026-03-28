/**
 * Trailerio client — fetches trailer URLs for IMDB content.
 * Service: https://trailerio.plaio.cc/api/meta/{type}/{imdbId}.json
 *
 * Returns direct video URLs (mp4, m3u8) that can be piped through ffmpeg
 * for frame extraction and phash computation.
 */

import { config } from '../config.js';
import { logger } from '../utils/logger.js';

interface TrailerioResponse {
  meta: {
    id: string;
    type: string;
    name: string;
    links: Array<{
      trailers: string;
      provider: string;
    }>;
  };
}

let lastRequestTime = 0;
const MIN_INTERVAL = 1000; // 1 req/s rate limit

/**
 * Fetch the best trailer URL for a given IMDB ID.
 * Returns null if no trailer found.
 */
export async function fetchTrailerUrl(imdbId: string, type: 'movie' | 'series' = 'movie'): Promise<string | null> {
  // Rate limiting
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < MIN_INTERVAL) {
    await new Promise(r => setTimeout(r, MIN_INTERVAL - elapsed));
  }
  lastRequestTime = Date.now();

  const url = `${config.trailerioBaseUrl}/meta/${type}/${imdbId}.json`;

  try {
    const res = await fetch(url, {
      headers: { 'User-Agent': 'StreamDB/1.0' },
      signal: AbortSignal.timeout(15000),
    });

    if (!res.ok) {
      if (res.status !== 404) {
        logger.debug('Trailerio request failed', { imdbId, status: res.status });
      }
      return null;
    }

    const data = await res.json() as TrailerioResponse;
    if (!data.meta?.links?.length) return null;

    // Prefer mp4 over m3u8 (easier for ffmpeg frame extraction)
    const mp4Link = data.meta.links.find(l => l.trailers.endsWith('.mp4') || l.trailers.includes('.mp4?'));
    if (mp4Link) return mp4Link.trailers;

    // Fall back to first available
    return data.meta.links[0].trailers;
  } catch (err: any) {
    logger.debug('Trailerio fetch failed', { imdbId, error: err.message });
    return null;
  }
}
