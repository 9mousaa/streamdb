import { getFilesForContent, getFilesForEpisode, queueProbeJob } from '../db/queries/graph.js';
import { checkDebridAvailability, unrestrictHash, type DebridConfig } from '../debrid/manager.js';
import { scoreFiles, formatStreamTitle, type UserPreferences } from './scoring.js';
import { logger } from '../utils/logger.js';

export interface StreamResult {
  streams: StremioStream[];
}

interface StremioStream {
  name: string;
  title: string;
  url?: string;
  infoHash?: string;
  fileIdx?: number;
  behaviorHints?: Record<string, unknown>;
}

export async function getStreams(
  type: string,
  id: string,
  prefs: UserPreferences,
  debridConfigs: DebridConfig[],
  singleStream: boolean = false
): Promise<StreamResult> {
  // Parse ID
  let imdbId: string;
  let season: number | undefined;
  let episode: number | undefined;

  if (type === 'series') {
    const parts = id.split(':');
    imdbId = parts[0];
    season = parseInt(parts[1]);
    episode = parseInt(parts[2]);
  } else {
    imdbId = id.replace('.json', '');
  }

  logger.debug('Stream request', { type, imdbId, season, episode });

  // Fire-and-forget TMDB enrichment if not already done
  import('../metadata/tmdb.js').then(({ enrichContentNode }) =>
    enrichContentNode(imdbId).catch((err: any) => logger.debug('TMDB enrich skipped', { error: err.message }))
  );

  // Get all files for this content
  const files = type === 'series' && season !== undefined && episode !== undefined
    ? await getFilesForEpisode(imdbId, season, episode)
    : await getFilesForContent(imdbId);

  if (!files.length) {
    logger.debug('No files found', { imdbId });
    return { streams: [] };
  }

  // Queue probe jobs for unprobed low-confidence files
  for (const f of files) {
    if (f.confidence < 0.5) {
      queueProbeJob(f.infohash, 5, 'stream_request').catch(() => {});
    }
  }

  // Get unique infohashes
  const infohashes = [...new Set(files.map(f => f.infohash))];

  // Check debrid availability
  const availability = await checkDebridAvailability(infohashes, debridConfigs);

  // Filter to debrid-cached only
  const cachedFiles = files.filter(f => {
    const avail = availability.get(f.infohash);
    return avail && avail.some(a => a.available);
  });

  if (!cachedFiles.length) {
    logger.debug('No cached files', { imdbId, totalFiles: files.length });
    return { streams: [] };
  }

  // Score and rank
  const scored = scoreFiles(cachedFiles, prefs);

  // Build stream objects — only direct URLs, no raw infohash fallback
  const maxStreams = singleStream ? 1 : 15;
  const streams: StremioStream[] = [];
  // Sort debrid configs by priority for unrestrict attempts
  const sortedDebrid = [...debridConfigs].sort((a, b) => a.priority - b.priority);

  for (const { file, score } of scored.slice(0, maxStreams)) {
    const avail = availability.get(file.infohash);
    if (!avail?.some(a => a.available)) continue;

    const title = formatStreamTitle(file, score);

    // Get direct URL from debrid service
    const result = await unrestrictHash(file.infohash, file.file_idx, sortedDebrid);
    if (result) {
      streams.push({
        name: 'StreamDB',
        title,
        url: result.url,
        behaviorHints: { notWebReady: true },
      });
    }
  }

  logger.info('Streams served', { imdbId, total: files.length, cached: cachedFiles.length, returned: streams.length });
  return { streams };
}
