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
  const streams: StremioStream[] = [];
  const sortedDebrid = [...debridConfigs].sort((a, b) => a.priority - b.priority);

  // ── Build unified HLS master stream ───────────────────────
  // Goal: one master playlist with multiple resolutions + all audio dubs + all subs
  // Strategy: pick the richest file per resolution (most languages, largest size)
  try {
    const { createHlsSession } = await import('../routes/hls.js');
    type VariantInput = { tag: string; sourceUrl: string };

    // Score files for language richness (from filename when DB tracks empty)
    const langKeywords = ['eng', 'fre', 'fra', 'ger', 'deu', 'ita', 'spa', 'por', 'pol', 'cze', 'ces', 'rus', 'jpn', 'kor', 'chi', 'zho', 'hin', 'ara', 'tur', 'dut', 'nld', 'swe', 'nor', 'dan', 'fin', 'hun', 'rom', 'gre', 'ell', 'heb', 'tha', 'vie', 'ind', 'ukr', 'bul', 'hrv', 'slv', 'lit', 'lat'];

    function estimateLanguages(file: FileRecord): number {
      // If we have probed tracks, use those
      if (file.audio_tracks.length > 0) return file.audio_tracks.length;
      // Otherwise estimate from filename
      const name = (file.torrent_name || file.filename || '').toLowerCase();
      if (name.includes('multi') || name.includes('remux')) {
        // Count language codes in filename
        let count = 0;
        for (const lang of langKeywords) {
          if (name.includes(lang)) count++;
        }
        return Math.max(count, 3); // MULTI/REMUX have at least 3
      }
      if (name.includes('dual')) return 2;
      return 1;
    }

    // Group cached files by resolution, pick richest per resolution
    const resolutionOrder = ['2160p', '1080p', '720p', '480p'];
    const bestPerRes = new Map<string, FileRecord>();

    for (const { file } of scored) {
      const res = file.resolution || 'unknown';
      if (!resolutionOrder.includes(res)) continue;
      const avail = availability.get(file.infohash);
      if (!avail?.some(a => a.available)) continue;

      const existing = bestPerRes.get(res);
      if (!existing) {
        bestPerRes.set(res, file);
      } else {
        // Prefer file with more languages, then larger size (remux)
        const existingLangs = estimateLanguages(existing);
        const newLangs = estimateLanguages(file);
        if (newLangs > existingLangs || (newLangs === existingLangs && (file.file_size || 0) > (existing.file_size || 0))) {
          bestPerRes.set(res, file);
        }
      }
    }

    // Unrestrict best file per resolution (up to 4 variants)
    const variantInputs: VariantInput[] = [];
    const resolutions: string[] = [];
    let hlsBingeGroup = '';

    for (const res of resolutionOrder) {
      if (variantInputs.length >= 4) break;
      const file = bestPerRes.get(res);
      if (!file) continue;

      const result = await unrestrictHash(file.infohash, file.file_idx, sortedDebrid);
      if (!result) continue;

      variantInputs.push({ tag: res, sourceUrl: result.url });
      resolutions.push(res);
      if (!hlsBingeGroup) hlsBingeGroup = buildBingeGroup(file, result.service);

      logger.debug('HLS variant selected', {
        res, langs: estimateLanguages(file),
        size: file.file_size ? `${(file.file_size / (1024 ** 3)).toFixed(1)}GB` : '?',
        name: (file.torrent_name || '').substring(0, 60),
      });
    }

    if (variantInputs.length > 0) {
      const sessionId = randomUUID().replace(/-/g, '').substring(0, 16);
      createHlsSession(sessionId, variantInputs);

      const title = resolutions.length > 1
        ? `${resolutions.join(' / ')} · Adaptive HLS`
        : `${resolutions[0]} · HLS`;

      streams.push({
        name: 'StreamDB',
        title,
        url: `${config.baseUrl}/hls/${sessionId}/master.m3u8`,
        behaviorHints: { bingeGroup: hlsBingeGroup },
      });
    }
  } catch (err: any) {
    logger.debug('HLS creation failed', { error: err.message });
  }

  // ── Also add direct debrid streams as fallbacks ───────────
  const maxDirect = singleStream ? 0 : Math.max(0, 15 - streams.length);
  let directCount = 0;

  for (const { file, score } of scored) {
    if (directCount >= maxDirect) break;

    const avail = availability.get(file.infohash);
    if (!avail?.some(a => a.available)) continue;

    const result = await unrestrictHash(file.infohash, file.file_idx, sortedDebrid);
    if (!result) continue;

    const serviceName = result.service === 'realdebrid' ? 'RD' : result.service === 'torbox' ? 'TB' : result.service;
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
    directCount++;
  }

  logger.info('Streams served', {
    imdbId, total: files.length, cached: cachedFiles.length,
    returned: streams.length, hasHls: streams.length > 0 && streams[0].name === 'StreamDB',
  });
  return { streams };
}
