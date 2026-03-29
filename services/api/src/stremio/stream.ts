import { getFilesForContent, getFilesForEpisode, queueProbeJob, type FileRecord } from '../db/queries/graph.js';
import { checkDebridAvailability, unrestrictHash, type DebridConfig } from '../debrid/manager.js';
import { scoreFiles, formatStreamTitle, type UserPreferences } from './scoring.js';
import { config } from '../config.js';
import { logger } from '../utils/logger.js';
import { randomUUID } from 'crypto';

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

function buildBingeGroup(file: FileRecord, service: string): string {
  const parts = ['streamdb', service];
  if (file.resolution) parts.push(file.resolution);
  if (file.video_codec) parts.push(file.video_codec);
  if (file.hdr && file.hdr.toLowerCase() !== 'sdr') parts.push(file.hdr);
  return parts.join('|');
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

  // Build stream objects
  const maxStreams = singleStream ? 1 : 15;
  const streams: StremioStream[] = [];
  const sortedDebrid = [...debridConfigs].sort((a, b) => a.priority - b.priority);

  let hlsCreated = false;

  for (const { file, score } of scored.slice(0, maxStreams)) {
    const avail = availability.get(file.infohash);
    if (!avail?.some(a => a.available)) continue;

    // Get direct URL from debrid service
    const result = await unrestrictHash(file.infohash, file.file_idx, sortedDebrid);
    if (!result) continue;

    const serviceName = result.service === 'realdebrid' ? 'RD' : result.service === 'torbox' ? 'TB' : result.service;

    // For the best stream: try to create an HLS session for streaming-service experience
    if (!hlsCreated) {
      try {
        // Create HLS session — probeUrl runs on-demand to discover tracks
        const sessionId = randomUUID().replace(/-/g, '').substring(0, 16);
        const { createHlsSession } = await import('../routes/hls.js');
        const sessionUrl = await createHlsSession(sessionId, result.url);

        if (sessionUrl) {
          // Build title from file metadata (track info added by probe if available)
          const titleParts: string[] = [];
          if (file.resolution) titleParts.push(file.resolution);
          if (file.hdr && file.hdr.toLowerCase() !== 'sdr') titleParts.push(file.hdr);
          if (file.video_codec) titleParts.push(file.video_codec.toUpperCase());
          if (file.audio_tracks.length > 1) titleParts.push(`${file.audio_tracks.length} Audio`);
          else if (file.audio_tracks.length === 1 && file.audio_tracks[0]?.codec) titleParts.push(file.audio_tracks[0].codec);
          if (file.subtitle_tracks.length > 0) titleParts.push(`${file.subtitle_tracks.length} Subs`);
          if (file.file_size) titleParts.push(`${(file.file_size / (1024 ** 3)).toFixed(1)}GB`);

          streams.push({
            name: 'StreamDB HLS',
            title: titleParts.join(' \u00b7 ') || 'HLS Stream',
            url: `${config.baseUrl}/hls/${sessionId}/master.m3u8`,
            behaviorHints: {
              bingeGroup: buildBingeGroup(file, result.service),
            },
          });
          hlsCreated = true;
          continue;
        }
      } catch (err: any) {
        logger.debug('HLS session creation failed, falling back to direct', { error: err.message });
      }
    }

    // Fallback: direct debrid URL
    const title = formatStreamTitle(file, score);
    const hints: Record<string, unknown> = {
      notWebReady: true,
      bingeGroup: buildBingeGroup(file, result.service),
    };
    if (file.filename) hints.filename = file.filename;
    else if (file.torrent_name) hints.filename = file.torrent_name;

    streams.push({
      name: `StreamDB ${serviceName}`,
      title,
      url: result.url,
      behaviorHints: hints,
    });
  }

  logger.info('Streams served', { imdbId, total: files.length, cached: cachedFiles.length, returned: streams.length, hasHls: hlsCreated });
  return { streams };
}
