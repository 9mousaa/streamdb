import { Router } from 'express';
import { spawn, type ChildProcess } from 'child_process';
import { mkdirSync, existsSync, readFileSync, readdirSync, statSync } from 'fs';
import { rmSync } from 'fs';
import { randomUUID } from 'crypto';
import { probeUrl, type ProbeResult, type ProbeTrack } from '../probe/ffprobe.js';
import { logger } from '../utils/logger.js';

const router = Router();

// ── Session Management ──────────────────────────────────────

interface HlsSession {
  id: string;
  sourceUrl: string;
  probe: ProbeResult;
  segmentDir: string;
  ffmpeg: ChildProcess | null;
  started: boolean;
  createdAt: number;
}

const sessions = new Map<string, HlsSession>();
const SESSION_TTL_MS = 4 * 60 * 60 * 1000; // 4 hours
const HLS_BASE = '/tmp/hls';

// Cleanup expired sessions every 10 minutes
setInterval(() => {
  const now = Date.now();
  for (const [id, session] of sessions) {
    if (now - session.createdAt > SESSION_TTL_MS) {
      destroySession(id);
    }
  }
}, 10 * 60 * 1000);

function destroySession(id: string) {
  const session = sessions.get(id);
  if (!session) return;
  if (session.ffmpeg) {
    session.ffmpeg.kill('SIGTERM');
  }
  try { rmSync(session.segmentDir, { recursive: true, force: true }); } catch {}
  sessions.delete(id);
  logger.debug('HLS session destroyed', { id });
}

// ── Create Session ──────────────────────────────────────────

router.post('/hls/create', async (req, res) => {
  const { url } = req.body;
  if (!url || typeof url !== 'string') {
    return res.status(400).json({ error: 'url required' });
  }

  try {
    // Probe the source to discover tracks
    const probe = await probeUrl(url);

    const id = randomUUID().replace(/-/g, '').substring(0, 16);
    const segmentDir = `${HLS_BASE}/${id}`;
    mkdirSync(segmentDir, { recursive: true });
    mkdirSync(`${segmentDir}/subs`, { recursive: true });

    const session: HlsSession = {
      id,
      sourceUrl: url,
      probe,
      segmentDir,
      ffmpeg: null,
      started: false,
      createdAt: Date.now(),
    };
    sessions.set(id, session);

    logger.info('HLS session created', {
      id,
      resolution: probe.resolution,
      audioTracks: probe.audioTracks.length,
      subtitleTracks: probe.subtitleTracks.length,
      duration: probe.duration,
    });

    res.json({
      sessionId: id,
      masterUrl: `/hls/${id}/master.m3u8`,
      probe: {
        resolution: probe.resolution,
        video_codec: probe.video_codec,
        hdr: probe.hdr,
        duration: probe.duration,
        audioTracks: probe.audioTracks.map((t, i) => ({
          index: i, codec: t.codec, channels: t.channels,
          language: t.language, isDefault: t.is_default,
        })),
        subtitleTracks: probe.subtitleTracks.map((t, i) => ({
          index: i, language: t.language, format: t.sub_format,
          forced: t.forced, isDefault: t.is_default,
        })),
      },
    });
  } catch (err: any) {
    logger.error('HLS session creation failed', { error: err.message });
    res.status(500).json({ error: err.message });
  }
});

// ── Master Playlist ─────────────────────────────────────────

router.get('/hls/:sessionId/master.m3u8', (req, res) => {
  const session = sessions.get(req.params.sessionId);
  if (!session) return res.status(404).send('Session not found');

  const { probe } = session;
  const lines: string[] = ['#EXTM3U'];

  // Audio tracks
  const audioTracks = probe.audioTracks;
  if (audioTracks.length > 0) {
    for (let i = 0; i < audioTracks.length; i++) {
      const t = audioTracks[i];
      const lang = t.language || 'und';
      const name = formatTrackName(t, 'audio', i);
      const isDefault = t.is_default || i === 0 ? 'YES' : 'NO';
      lines.push(
        `#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="${name}",LANGUAGE="${lang}",DEFAULT=${isDefault},AUTOSELECT=${isDefault},URI="/hls/${session.id}/audio/${i}/index.m3u8"`
      );
    }
  }

  // Subtitle tracks (skip bitmap formats like PGS that can't be WebVTT)
  const subTracks = probe.subtitleTracks;
  const bitmapFormats = ['hdmv_pgs_subtitle', 'pgssub', 'dvd_subtitle'];
  let hasTextSubs = false;
  for (let i = 0; i < subTracks.length; i++) {
    const t = subTracks[i];
    const fmt = t.sub_format?.toLowerCase() || '';
    if (bitmapFormats.includes(fmt)) continue; // Skip PGS/DVD bitmap subs
    hasTextSubs = true;
    const lang = t.language || 'und';
    const name = formatTrackName(t, 'subtitle', i);
    const isDefault = t.is_default ? 'YES' : 'NO';
    const forced = t.forced ? 'YES' : 'NO';
    lines.push(
      `#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="${name}",LANGUAGE="${lang}",DEFAULT=${isDefault},AUTOSELECT=NO,FORCED=${forced},URI="/hls/${session.id}/subs/${i}/index.m3u8"`
    );
  }

  // Video stream — passthrough (no transcode)
  const bandwidth = probe.bitrate || 20_000_000;
  const resolution = probe.resolution === '2160p' ? '3840x2160'
    : probe.resolution === '1080p' ? '1920x1080'
    : probe.resolution === '720p' ? '1280x720'
    : '1920x1080';

  let streamInf = `#EXT-X-STREAM-INF:BANDWIDTH=${bandwidth},RESOLUTION=${resolution}`;
  if (audioTracks.length > 0) streamInf += ',AUDIO="audio"';
  if (hasTextSubs) streamInf += ',SUBTITLES="subs"';
  lines.push(streamInf);
  lines.push(`/hls/${session.id}/video/index.m3u8`);

  res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
  res.setHeader('Cache-Control', 'no-cache');
  res.send(lines.join('\n') + '\n');
});

// ── Video Stream Playlist ───────────────────────────────────

router.get('/hls/:sessionId/video/index.m3u8', async (req, res) => {
  const session = sessions.get(req.params.sessionId);
  if (!session) return res.status(404).send('Session not found');

  // Start ffmpeg if not already running
  if (!session.started) {
    startSegmentation(session);
  }

  // Wait for the playlist file to appear (up to 30s)
  const playlistPath = `${session.segmentDir}/video.m3u8`;
  const deadline = Date.now() + 30_000;
  while (!existsSync(playlistPath) && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 500));
  }

  if (!existsSync(playlistPath)) {
    return res.status(503).send('Stream not ready yet');
  }

  const content = readFileSync(playlistPath, 'utf-8');
  res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
  res.setHeader('Cache-Control', 'no-cache');
  res.send(content);
});

// ── Audio Stream Playlist ───────────────────────────────────

router.get('/hls/:sessionId/audio/:trackIdx/index.m3u8', async (req, res) => {
  const session = sessions.get(req.params.sessionId);
  if (!session) return res.status(404).send('Session not found');

  if (!session.started) {
    startSegmentation(session);
  }

  const idx = parseInt(req.params.trackIdx);
  const playlistPath = `${session.segmentDir}/audio-${idx}.m3u8`;
  const deadline = Date.now() + 30_000;
  while (!existsSync(playlistPath) && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 500));
  }

  if (!existsSync(playlistPath)) {
    return res.status(503).send('Stream not ready yet');
  }

  const content = readFileSync(playlistPath, 'utf-8');
  res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
  res.setHeader('Cache-Control', 'no-cache');
  res.send(content);
});

// ── Subtitle Playlist ───────────────────────────────────────

router.get('/hls/:sessionId/subs/:trackIdx/index.m3u8', async (req, res) => {
  const session = sessions.get(req.params.sessionId);
  if (!session) return res.status(404).send('Session not found');

  const idx = parseInt(req.params.trackIdx);
  const vttPath = `${session.segmentDir}/subs/sub-${idx}.vtt`;

  // Extract subtitle if not yet done
  if (!existsSync(vttPath)) {
    await extractSubtitle(session, idx);
  }

  if (!existsSync(vttPath)) {
    return res.status(404).send('Subtitle not available');
  }

  // Return a simple HLS playlist pointing to the VTT file
  const lines = [
    '#EXTM3U',
    '#EXT-X-TARGETDURATION:99999',
    '#EXT-X-PLAYLIST-TYPE:VOD',
    `#EXTINF:${session.probe.duration || 7200},`,
    `/hls/${session.id}/subs/${idx}/sub.vtt`,
    '#EXT-X-ENDLIST',
  ];

  res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
  res.send(lines.join('\n') + '\n');
});

// ── Serve VTT file ──────────────────────────────────────────

router.get('/hls/:sessionId/subs/:trackIdx/sub.vtt', (req, res) => {
  const session = sessions.get(req.params.sessionId);
  if (!session) return res.status(404).send('Session not found');

  const idx = parseInt(req.params.trackIdx);
  const vttPath = `${session.segmentDir}/subs/sub-${idx}.vtt`;
  if (!existsSync(vttPath)) return res.status(404).send('Not found');

  res.setHeader('Content-Type', 'text/vtt');
  res.sendFile(vttPath);
});

// ── Serve Segments ──────────────────────────────────────────

router.get('/hls/:sessionId/:type/:filename', (req, res) => {
  const session = sessions.get(req.params.sessionId);
  if (!session) return res.status(404).send('Session not found');

  const filePath = `${session.segmentDir}/${req.params.filename}`;
  if (!existsSync(filePath)) return res.status(404).send('Segment not found');

  const ext = req.params.filename.split('.').pop();
  if (ext === 'ts') res.setHeader('Content-Type', 'video/mp2t');
  else if (ext === 'm4s') res.setHeader('Content-Type', 'video/iso.segment');
  else if (ext === 'mp4') res.setHeader('Content-Type', 'video/mp4');
  else if (ext === 'vtt') res.setHeader('Content-Type', 'text/vtt');

  res.sendFile(filePath);
});

// Also serve segments at the root level (ffmpeg outputs them here with hls_base_url)
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

  res.sendFile(filePath);
});

// ── Destroy Session ─────────────────────────────────────────

router.delete('/hls/:sessionId', (req, res) => {
  destroySession(req.params.sessionId);
  res.json({ ok: true });
});

// ── ffmpeg Segmentation ─────────────────────────────────────

function startSegmentation(session: HlsSession) {
  if (session.started) return;
  session.started = true;

  const baseUrl = `/hls/${session.id}/`;

  // Use fMP4 segments — works with all codecs (HEVC, H.264, AAC, etc.)
  // MPEG-TS has issues with HEVC which is common in 4K MKV torrents
  const args: string[] = [
    '-i', session.sourceUrl,
    '-y',
    // Video: copy codec, output to video.m3u8 with fMP4 segments
    '-map', '0:v:0',
    '-c:v', 'copy',
    '-f', 'hls',
    '-hls_time', '10',
    '-hls_playlist_type', 'event',
    '-hls_segment_type', 'fmp4',
    '-hls_base_url', baseUrl,
    '-hls_fmp4_init_filename', 'vinit.mp4',
    '-hls_segment_filename', `${session.segmentDir}/vseg-%d.m4s`,
    `${session.segmentDir}/video.m3u8`,
  ];

  // Audio tracks: each gets its own HLS output with fMP4 segments
  const audioTracks = session.probe.audioTracks;
  for (let i = 0; i < audioTracks.length; i++) {
    args.push(
      '-map', `0:a:${i}`,
      '-c:a', 'aac', '-b:a', '192k',
      '-f', 'hls',
      '-hls_time', '10',
      '-hls_playlist_type', 'event',
      '-hls_segment_type', 'fmp4',
      '-hls_base_url', baseUrl,
      '-hls_fmp4_init_filename', `ainit-${i}.mp4`,
      '-hls_segment_filename', `${session.segmentDir}/aseg-${i}-%d.m4s`,
      `${session.segmentDir}/audio-${i}.m3u8`,
    );
  }

  logger.info('Starting ffmpeg segmentation', {
    sessionId: session.id,
    audioTracks: audioTracks.length,
  });

  const proc = spawn('/usr/bin/ffmpeg', args, {
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  proc.stderr?.on('data', (data: Buffer) => {
    const line = data.toString().trim();
    if (line && !line.startsWith('frame=')) {
      logger.debug('ffmpeg', { sessionId: session.id, line: line.substring(0, 200) });
    }
  });

  proc.on('exit', (code) => {
    logger.info('ffmpeg exited', { sessionId: session.id, code });
  });

  session.ffmpeg = proc;
}

async function extractSubtitle(session: HlsSession, trackIdx: number): Promise<void> {
  const track = session.probe.subtitleTracks[trackIdx];
  const format = track?.sub_format?.toLowerCase() || '';

  // PGS/HDMV bitmap subtitles cannot be converted to WebVTT — skip
  if (format === 'hdmv_pgs_subtitle' || format === 'pgssub' || format === 'dvd_subtitle') {
    logger.debug('Skipping bitmap subtitle', { sessionId: session.id, trackIdx, format });
    return;
  }

  const vttPath = `${session.segmentDir}/subs/sub-${trackIdx}.vtt`;

  return new Promise((resolve) => {
    const proc = spawn('/usr/bin/ffmpeg', [
      '-i', session.sourceUrl,
      '-map', `0:s:${trackIdx}`,
      '-c:s', 'webvtt',
      '-y', vttPath,
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
      const codec = track.codec.toLowerCase();
      if (codec.includes('truehd')) parts.push('TrueHD');
      else if (codec.includes('eac3') || codec === 'eac3') parts.push('EAC3');
      else if (codec.includes('ac3') || codec === 'ac3') parts.push('AC3');
      else if (codec.includes('aac')) parts.push('AAC');
      else if (codec.includes('dts')) parts.push('DTS');
      else if (codec.includes('flac')) parts.push('FLAC');
      else if (codec.includes('opus')) parts.push('Opus');
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

// ── Programmatic session creation (used by stream.ts) ───────

export async function createHlsSession(id: string, sourceUrl: string): Promise<string | null> {
  try {
    const probe = await probeUrl(sourceUrl);
    const segmentDir = `${HLS_BASE}/${id}`;
    mkdirSync(segmentDir, { recursive: true });
    mkdirSync(`${segmentDir}/subs`, { recursive: true });

    const session: HlsSession = {
      id,
      sourceUrl,
      probe,
      segmentDir,
      ffmpeg: null,
      started: false,
      createdAt: Date.now(),
    };
    sessions.set(id, session);

    logger.info('HLS session created programmatically', {
      id,
      audioTracks: probe.audioTracks.length,
      subtitleTracks: probe.subtitleTracks.length,
    });

    return `/hls/${id}/master.m3u8`;
  } catch (err: any) {
    logger.error('Failed to create HLS session', { error: err.message });
    return null;
  }
}

export default router;
