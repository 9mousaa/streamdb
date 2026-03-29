import { Router } from 'express';
import { spawn, type ChildProcess } from 'child_process';
import { mkdirSync, existsSync, readFileSync } from 'fs';
import { rmSync } from 'fs';
import { randomUUID } from 'crypto';
import { probeUrl, type ProbeResult, type ProbeTrack } from '../probe/ffprobe.js';
import { logger } from '../utils/logger.js';

const router = Router();

// ── Session Management ──────────────────────────────────────

interface Variant {
  tag: string;
  sourceUrl: string;
  probe: ProbeResult | null;
  probePromise: Promise<ProbeResult> | null;
  ffmpeg: ChildProcess | null;
  started: boolean;
}

interface HlsSession {
  id: string;
  segmentDir: string;
  variants: Variant[];
  createdAt: number;
}

const sessions = new Map<string, HlsSession>();
const SESSION_TTL_MS = 4 * 60 * 60 * 1000;
const HLS_BASE = '/tmp/hls';

setInterval(() => {
  const now = Date.now();
  for (const [id, session] of sessions) {
    if (now - session.createdAt > SESSION_TTL_MS) destroySession(id);
  }
}, 10 * 60 * 1000);

function destroySession(id: string) {
  const session = sessions.get(id);
  if (!session) return;
  for (const v of session.variants) {
    if (v.ffmpeg) v.ffmpeg.kill('SIGTERM');
  }
  try { rmSync(session.segmentDir, { recursive: true, force: true }); } catch {}
  sessions.delete(id);
  logger.debug('HLS session destroyed', { id });
}

// ── Create Session (REST) ───────────────────────────────────

router.post('/hls/create', (req, res) => {
  const { url } = req.body;
  if (!url || typeof url !== 'string') {
    return res.status(400).json({ error: 'url required' });
  }
  const id = randomUUID().replace(/-/g, '').substring(0, 16);
  const masterUrl = createHlsSession(id, [{ tag: 'default', sourceUrl: url }]);
  res.json({ sessionId: id, masterUrl });
});

// ── Master Playlist ─────────────────────────────────────────

router.get('/hls/:sessionId/master.m3u8', async (req, res) => {
  const session = sessions.get(req.params.sessionId);
  if (!session) return res.status(404).send('Session not found');

  // Wait for all probes
  for (const v of session.variants) {
    if (!v.probe && v.probePromise) {
      try { await v.probePromise; } catch {}
    }
  }

  const probed = session.variants.filter(v => v.probe);
  if (probed.length === 0) return res.status(503).send('Failed to probe sources');

  const lines: string[] = ['#EXTM3U'];

  // Audio from variant with most tracks
  const richestAudio = probed.reduce((best, v) =>
    (v.probe!.audioTracks.length > best.probe!.audioTracks.length) ? v : best, probed[0]);
  const audioTracks = richestAudio.probe!.audioTracks;
  const audioVi = session.variants.indexOf(richestAudio);

  for (let i = 0; i < audioTracks.length; i++) {
    const t = audioTracks[i];
    const lang = t.language || 'und';
    const name = formatTrackName(t, 'audio', i);
    const isDefault = t.is_default || i === 0 ? 'YES' : 'NO';
    lines.push(
      `#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="${name}",LANGUAGE="${lang}",DEFAULT=${isDefault},AUTOSELECT=${isDefault},URI="/hls/${session.id}/v/${audioVi}/audio/${i}/index.m3u8"`
    );
  }

  // Subs from variant with most text subs
  const bitmapFormats = ['hdmv_pgs_subtitle', 'pgssub', 'dvd_subtitle'];
  const richestSubs = probed.reduce((best, v) =>
    (v.probe!.subtitleTracks.length > best.probe!.subtitleTracks.length) ? v : best, probed[0]);
  const subTracks = richestSubs.probe!.subtitleTracks;
  const subVi = session.variants.indexOf(richestSubs);
  let hasTextSubs = false;

  for (let i = 0; i < subTracks.length; i++) {
    const t = subTracks[i];
    if (bitmapFormats.includes(t.sub_format?.toLowerCase() || '')) continue;
    hasTextSubs = true;
    const lang = t.language || 'und';
    const name = formatTrackName(t, 'subtitle', i);
    lines.push(
      `#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="${name}",LANGUAGE="${lang}",DEFAULT=${t.is_default ? 'YES' : 'NO'},AUTOSELECT=NO,FORCED=${t.forced ? 'YES' : 'NO'},URI="/hls/${session.id}/v/${subVi}/subs/${i}/index.m3u8"`
    );
  }

  // Video variants
  for (const v of probed) {
    const p = v.probe!;
    const vi = session.variants.indexOf(v);
    const bandwidth = p.bitrate || 20_000_000;
    const resolution = p.resolution === '2160p' ? '3840x2160'
      : p.resolution === '1080p' ? '1920x1080'
      : p.resolution === '720p' ? '1280x720'
      : p.resolution === '480p' ? '854x480' : '1920x1080';

    let inf = `#EXT-X-STREAM-INF:BANDWIDTH=${bandwidth},RESOLUTION=${resolution}`;
    if (audioTracks.length > 0) inf += ',AUDIO="audio"';
    if (hasTextSubs) inf += ',SUBTITLES="subs"';
    lines.push(inf);
    lines.push(`/hls/${session.id}/v/${vi}/video/index.m3u8`);
  }

  res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
  res.setHeader('Cache-Control', 'no-cache');
  res.send(lines.join('\n') + '\n');
});

// ── Video Playlist (starts video-only ffmpeg — zero CPU) ────

router.get('/hls/:sessionId/v/:vi/video/index.m3u8', async (req, res) => {
  const session = sessions.get(req.params.sessionId);
  if (!session) return res.status(404).send('Session not found');

  const vi = parseInt(req.params.vi);
  const variant = session.variants[vi];
  if (!variant?.probe) return res.status(503).send('Not probed yet');

  if (!variant.started) {
    startSegmentation(session, variant, vi);
  }

  const playlistPath = `${session.segmentDir}/v${vi}-video.m3u8`;
  const deadline = Date.now() + 30_000;
  while (!existsSync(playlistPath) && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 500));
  }
  if (!existsSync(playlistPath)) return res.status(503).send('Not ready');

  res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
  res.setHeader('Cache-Control', 'no-cache');
  res.send(readFileSync(playlistPath, 'utf-8'));
});

// ── Audio Playlist (starts per-track ffmpeg on demand) ──────

router.get('/hls/:sessionId/v/:vi/audio/:trackIdx/index.m3u8', async (req, res) => {
  const session = sessions.get(req.params.sessionId);
  if (!session) return res.status(404).send('Session not found');

  const vi = parseInt(req.params.vi);
  const idx = parseInt(req.params.trackIdx);
  const variant = session.variants[vi];
  if (!variant?.probe) return res.status(503).send('Not probed yet');

  if (!variant.started) {
    startSegmentation(session, variant, vi);
  }

  const playlistPath = `${session.segmentDir}/v${vi}-audio-${idx}.m3u8`;
  const deadline = Date.now() + 30_000;
  while (!existsSync(playlistPath) && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 500));
  }
  if (!existsSync(playlistPath)) return res.status(503).send('Not ready');

  res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
  res.setHeader('Cache-Control', 'no-cache');
  res.send(readFileSync(playlistPath, 'utf-8'));
});

// ── Subtitle Playlist ───────────────────────────────────────

router.get('/hls/:sessionId/v/:vi/subs/:trackIdx/index.m3u8', async (req, res) => {
  const session = sessions.get(req.params.sessionId);
  if (!session) return res.status(404).send('Session not found');

  const vi = parseInt(req.params.vi);
  const variant = session.variants[vi];
  if (!variant) return res.status(404).send('Variant not found');

  const idx = parseInt(req.params.trackIdx);
  const vttPath = `${session.segmentDir}/v${vi}-sub-${idx}.vtt`;

  if (!existsSync(vttPath)) await extractSubtitle(session, variant, vi, idx);
  if (!existsSync(vttPath)) return res.status(404).send('Subtitle not available');

  const lines = [
    '#EXTM3U', '#EXT-X-TARGETDURATION:99999', '#EXT-X-PLAYLIST-TYPE:VOD',
    `#EXTINF:${variant.probe?.duration || 7200},`,
    `/hls/${session.id}/v/${vi}/subs/${idx}/sub.vtt`,
    '#EXT-X-ENDLIST',
  ];
  res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
  res.send(lines.join('\n') + '\n');
});

router.get('/hls/:sessionId/v/:vi/subs/:trackIdx/sub.vtt', (req, res) => {
  const session = sessions.get(req.params.sessionId);
  if (!session) return res.status(404).send('Session not found');
  const vi = parseInt(req.params.vi);
  const idx = parseInt(req.params.trackIdx);
  const vttPath = `${session.segmentDir}/v${vi}-sub-${idx}.vtt`;
  if (!existsSync(vttPath)) return res.status(404).send('Not found');
  res.setHeader('Content-Type', 'text/vtt');
  res.sendFile(vttPath);
});

// ── Serve Segments ──────────────────────────────────────────

router.get('/hls/:sessionId/:filename', (req, res) => {
  const session = sessions.get(req.params.sessionId);
  if (!session) return res.status(404).send('Session not found');

  const filePath = `${session.segmentDir}/${req.params.filename}`;
  if (!existsSync(filePath)) return res.status(404).send('Segment not found');

  const ext = req.params.filename.split('.').pop();
  if (ext === 'ts') res.setHeader('Content-Type', 'video/mp2t');
  else if (ext === 'm4s') res.setHeader('Content-Type', 'video/iso.segment');
  else if (ext === 'mp4') res.setHeader('Content-Type', 'video/mp4');
  else if (ext === 'm3u8') res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
  else if (ext === 'vtt') res.setHeader('Content-Type', 'text/vtt');

  res.sendFile(filePath);
});

// ── Destroy ─────────────────────────────────────────────────

router.delete('/hls/:sessionId', (req, res) => {
  destroySession(req.params.sessionId);
  res.json({ ok: true });
});

// ── Segmentation (all copy — zero CPU) ──────────────────────

function startSegmentation(session: HlsSession, variant: Variant, vi: number) {
  if (variant.started || !variant.probe) return;
  variant.started = true;

  const probe = variant.probe;
  const baseUrl = `/hls/${session.id}/`;
  const prefix = `v${vi}`;

  // Everything is copy — no transcoding, no CPU usage
  // Just remux from MKV/MP4 into fMP4 HLS segments
  const args: string[] = [
    '-i', variant.sourceUrl, '-y',
    '-map', '0:v:0', '-c:v', 'copy',
    '-f', 'hls', '-hls_time', '10', '-hls_playlist_type', 'event',
    '-hls_segment_type', 'fmp4',
    '-hls_base_url', baseUrl,
    '-hls_fmp4_init_filename', `${prefix}-vinit.mp4`,
    '-hls_segment_filename', `${session.segmentDir}/${prefix}-vseg-%d.m4s`,
    `${session.segmentDir}/${prefix}-video.m3u8`,
  ];

  // Audio: all copy, no transcoding
  for (let i = 0; i < probe.audioTracks.length; i++) {
    args.push(
      '-map', `0:a:${i}`, '-c:a', 'copy',
      '-f', 'hls', '-hls_time', '10', '-hls_playlist_type', 'event',
      '-hls_segment_type', 'fmp4',
      '-hls_base_url', baseUrl,
      '-hls_fmp4_init_filename', `${prefix}-ainit-${i}.mp4`,
      '-hls_segment_filename', `${session.segmentDir}/${prefix}-aseg-${i}-%d.m4s`,
      `${session.segmentDir}/${prefix}-audio-${i}.m3u8`,
    );
  }

  logger.info('Starting segmentation (all copy)', {
    sessionId: session.id, variant: vi, tag: variant.tag,
    audioTracks: probe.audioTracks.length,
  });

  const proc = spawn('/usr/bin/ffmpeg', args, { stdio: ['ignore', 'pipe', 'pipe'] });
  proc.stderr?.on('data', (d: Buffer) => {
    const l = d.toString().trim();
    if (l && !l.startsWith('frame=') && !l.startsWith('size=')) {
      logger.debug('ffmpeg', { session: session.id, vi, line: l.substring(0, 200) });
    }
  });
  proc.on('exit', (code) => logger.info('ffmpeg exited', { session: session.id, vi, code }));
  variant.ffmpeg = proc;
}

// ── Subtitle Extraction ─────────────────────────────────────

async function extractSubtitle(session: HlsSession, variant: Variant, vi: number, trackIdx: number): Promise<void> {
  const track = variant.probe?.subtitleTracks[trackIdx];
  const format = track?.sub_format?.toLowerCase() || '';
  if (['hdmv_pgs_subtitle', 'pgssub', 'dvd_subtitle'].includes(format)) return;

  const vttPath = `${session.segmentDir}/v${vi}-sub-${trackIdx}.vtt`;
  return new Promise((resolve) => {
    const proc = spawn('/usr/bin/ffmpeg', [
      '-i', variant.sourceUrl, '-map', `0:s:${trackIdx}`, '-c:s', 'webvtt', '-y', vttPath,
    ], { stdio: ['ignore', 'ignore', 'pipe'], timeout: 120_000 });
    proc.on('exit', () => resolve());
    proc.on('error', () => resolve());
  });
}

// ── Helpers ─────────────────────────────────────────────────

function formatTrackName(track: ProbeTrack, type: string, index: number): string {
  const parts: string[] = [];
  if (track.language) parts.push(track.language.toUpperCase());

  if (type === 'audio') {
    if (track.codec) {
      const c = track.codec.toLowerCase();
      if (c.includes('truehd')) parts.push('TrueHD');
      else if (c.includes('eac3') || c === 'eac3') parts.push('EAC3');
      else if (c.includes('ac3') || c === 'ac3') parts.push('AC3');
      else if (c.includes('aac')) parts.push('AAC');
      else if (c.includes('dts')) parts.push('DTS');
      else if (c.includes('flac')) parts.push('FLAC');
      else if (c.includes('opus')) parts.push('Opus');
      else parts.push(track.codec);
    }
    if (track.channels) {
      if (track.channels >= 7) parts.push('7.1');
      else if (track.channels >= 5) parts.push('5.1');
      else if (track.channels >= 2) parts.push('Stereo');
      else parts.push('Mono');
    }
  } else {
    if (track.forced) parts.push('Forced');
    if (track.sub_format) parts.push(track.sub_format);
  }

  return parts.join(' ') || `Track ${index + 1}`;
}

// ── Programmatic session creation ───────────────────────────

export interface VariantInput {
  tag: string;
  sourceUrl: string;
}

export function createHlsSession(id: string, variantInputs: VariantInput[]): string {
  const segmentDir = `${HLS_BASE}/${id}`;
  mkdirSync(segmentDir, { recursive: true });

  const variants: Variant[] = variantInputs.map(input => {
    const variant: Variant = {
      tag: input.tag,
      sourceUrl: input.sourceUrl,
      probe: null,
      probePromise: null,
      ffmpeg: null,
      started: false,
    };

    variant.probePromise = probeUrl(input.sourceUrl).then(probe => {
      variant.probe = probe;
      logger.info('HLS variant probed', {
        id, tag: input.tag, resolution: probe.resolution,
        audioTracks: probe.audioTracks.length, subtitleTracks: probe.subtitleTracks.length,
      });
      return probe;
    }).catch(err => {
      logger.error('HLS probe failed', { id, tag: input.tag, error: err.message });
      throw err;
    });

    return variant;
  });

  sessions.set(id, { id, segmentDir, variants, createdAt: Date.now() });
  logger.info('HLS session created', { id, variants: variantInputs.length });
  return `/hls/${id}/master.m3u8`;
}

export default router;
