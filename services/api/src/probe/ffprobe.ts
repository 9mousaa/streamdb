import { execFile } from 'child_process';
import { logger } from '../utils/logger.js';

export interface ProbeResult {
  resolution: string | null;
  video_codec: string | null;
  hdr: string | null;
  bit_depth: number | null;
  bitrate: number | null;
  duration: number | null;
  container: string | null;
  file_size: number | null;
  audioTracks: ProbeTrack[];
  subtitleTracks: ProbeTrack[];
}

export interface ProbeTrack {
  codec: string | null;
  channels: number | null;
  language: string | null;
  sub_format: string | null;
  forced: boolean;
  is_default: boolean;
}

interface FFprobeOutput {
  format?: {
    format_name?: string;
    duration?: string;
    bit_rate?: string;
    size?: string;
  };
  streams?: FFprobeStream[];
}

interface FFprobeStream {
  codec_type: string;
  codec_name?: string;
  width?: number;
  height?: number;
  bits_per_raw_sample?: string;
  color_transfer?: string;
  color_primaries?: string;
  channels?: number;
  channel_layout?: string;
  tags?: Record<string, string>;
  disposition?: Record<string, number>;
  side_data_list?: Array<{ side_data_type?: string }>;
}

function detectResolution(width?: number, height?: number): string | null {
  if (!height) return null;
  if (height >= 2160 || (width && width >= 3840)) return '2160p';
  if (height >= 1080 || (width && width >= 1920)) return '1080p';
  if (height >= 720 || (width && width >= 1280)) return '720p';
  if (height >= 480) return '480p';
  return `${height}p`;
}

function detectHDR(stream: FFprobeStream): string | null {
  const sideData = stream.side_data_list || [];
  for (const sd of sideData) {
    const t = sd.side_data_type?.toLowerCase() || '';
    if (t.includes('dolby vision')) return 'DV';
    if (t.includes('hdr10+') || t.includes('hdr10plus')) return 'HDR10+';
  }
  if (stream.color_transfer === 'smpte2084') return 'HDR10';
  if (stream.color_transfer === 'arib-std-b67') return 'HLG';
  return null;
}

export function probeUrl(url: string): Promise<ProbeResult> {
  return new Promise((resolve, reject) => {
    const args = [
      '-v', 'quiet',
      '-print_format', 'json',
      '-show_format',
      '-show_streams',
      '-timeout', '30000000', // 30s in microseconds
      url,
    ];

    execFile('/usr/bin/ffprobe', args, { timeout: 60_000, maxBuffer: 5 * 1024 * 1024 }, (err, stdout) => {
      if (err) return reject(err);
      try {
        const data = JSON.parse(stdout) as FFprobeOutput;
        resolve(parseProbeResult(data));
      } catch (e) {
        reject(e);
      }
    });
  });
}

function parseProbeResult(data: FFprobeOutput): ProbeResult {
  const streams = data.streams || [];
  const videoStream = streams.find(s => s.codec_type === 'video');
  const audioStreams = streams.filter(s => s.codec_type === 'audio');
  const subtitleStreams = streams.filter(s => s.codec_type === 'subtitle');

  return {
    resolution: videoStream ? detectResolution(videoStream.width, videoStream.height) : null,
    video_codec: videoStream?.codec_name ?? null,
    hdr: videoStream ? detectHDR(videoStream) : null,
    bit_depth: videoStream?.bits_per_raw_sample ? parseInt(videoStream.bits_per_raw_sample) : null,
    bitrate: data.format?.bit_rate ? parseInt(data.format.bit_rate) : null,
    duration: data.format?.duration ? Math.round(parseFloat(data.format.duration)) : null,
    container: data.format?.format_name?.split(',')[0] ?? null,
    file_size: data.format?.size ? parseInt(data.format.size) : null,
    audioTracks: audioStreams.map(s => ({
      codec: s.codec_name ?? null,
      channels: s.channels ?? null,
      language: s.tags?.language ?? null,
      sub_format: null,
      forced: !!(s.disposition?.forced),
      is_default: !!(s.disposition?.default),
    })),
    subtitleTracks: subtitleStreams.map(s => ({
      codec: null,
      channels: null,
      language: s.tags?.language ?? null,
      sub_format: s.codec_name ?? null,
      forced: !!(s.disposition?.forced),
      is_default: !!(s.disposition?.default),
    })),
  };
}
