import type { FileRecord } from '../db/queries/graph.js';

export interface UserPreferences {
  resolutions: string[];       // ['2160p', '1080p'] in priority order
  hdrFormats: string[];        // ['DV', 'HDR10+', 'HDR10', 'SDR']
  audioLangs: string[];        // ['en']
  audioFormats: string[];      // ['TrueHD', 'DTS-HD MA', 'EAC3', 'AAC']
  dubLang?: string;            // 'ar'
  subLang?: string;            // 'ar'
  maxFileSizeGb: number;       // 80
}

export interface ScoredFile {
  file: FileRecord;
  score: number;
  breakdown: Record<string, number>;
}

const RESOLUTION_SCORES: Record<string, number> = {
  '2160p': 100, '1080p': 80, '720p': 60, '480p': 40,
};

const HDR_RANK: Record<string, number> = {
  'DV': 5, 'dolby_vision': 5,
  'HDR10+': 4, 'hdr10plus': 4,
  'HDR10': 3, 'hdr10': 3,
  'HLG': 2, 'hlg': 2,
  'SDR': 1, 'sdr': 1,
};

const AUDIO_FORMAT_RANK: Record<string, number> = {
  'atmos': 6, 'truehd': 5, 'TrueHD': 5,
  'dts-hd ma': 4, 'DTS-HD MA': 4,
  'eac3': 3, 'EAC3': 3, 'ddplus': 3,
  'dts': 2, 'DTS': 2,
  'aac': 1, 'AAC': 1, 'ac3': 1, 'AC3': 1,
};

export function scoreFiles(files: FileRecord[], prefs: UserPreferences): ScoredFile[] {
  return files
    .map((file) => scoreFile(file, prefs))
    .sort((a, b) => b.score - a.score);
}

function scoreFile(file: FileRecord, prefs: UserPreferences): ScoredFile {
  const breakdown: Record<string, number> = {};
  let score = 0;

  // Resolution match
  if (file.resolution) {
    const resIdx = prefs.resolutions.indexOf(file.resolution);
    if (resIdx === 0) {
      breakdown.resolution = 80;
    } else if (resIdx > 0) {
      breakdown.resolution = 60 - (resIdx * 10);
    } else {
      breakdown.resolution = (RESOLUTION_SCORES[file.resolution] || 0) / 4;
    }
    score += breakdown.resolution;
  }

  // HDR match
  if (file.hdr) {
    const userHdr = prefs.hdrFormats.map(h => h.toLowerCase());
    const fileHdr = file.hdr.toLowerCase();
    const hdrIdx = userHdr.indexOf(fileHdr);
    if (hdrIdx === 0) {
      breakdown.hdr = 60;
    } else if (hdrIdx > 0) {
      breakdown.hdr = 50 - (hdrIdx * 10);
    } else if (HDR_RANK[fileHdr]) {
      breakdown.hdr = HDR_RANK[fileHdr] * 5;
    }
    score += breakdown.hdr || 0;
  }

  // Audio language match
  const hasPreferredAudioLang = file.audio_tracks.some(
    t => t.language && prefs.audioLangs.includes(t.language)
  );
  if (hasPreferredAudioLang) {
    breakdown.audioLang = 40;
    score += 40;
  }

  // Audio format match
  const bestAudioScore = Math.max(0, ...file.audio_tracks.map(t => {
    if (!t.codec) return 0;
    const codec = t.codec.toLowerCase();
    const fmtIdx = prefs.audioFormats.findIndex(f => f.toLowerCase() === codec);
    if (fmtIdx === 0) return 50;
    if (fmtIdx > 0) return 40 - (fmtIdx * 5);
    return (AUDIO_FORMAT_RANK[codec] || 0) * 5;
  }));
  breakdown.audioFormat = bestAudioScore;
  score += bestAudioScore;

  // Dub language
  if (prefs.dubLang) {
    const hasDub = file.audio_tracks.some(
      t => t.language === prefs.dubLang
    );
    if (hasDub) {
      breakdown.dub = 40;
      score += 40;
    }
  }

  // Subtitle language
  if (prefs.subLang) {
    const hasSub = file.subtitle_tracks.some(
      t => t.language === prefs.subLang
    );
    if (hasSub) {
      breakdown.subtitle = 30;
      score += 30;
    }
  }

  // All tracks in one file bonus
  const hasAll = hasPreferredAudioLang
    && (!prefs.subLang || file.subtitle_tracks.some(t => t.language === prefs.subLang));
  if (hasAll) {
    breakdown.allInOne = 100;
    score += 100;
  }

  // File size
  if (file.file_size) {
    const sizeGb = file.file_size / (1024 ** 3);
    if (sizeGb <= prefs.maxFileSizeGb) {
      breakdown.fileSize = 20;
      score += 20;
    } else {
      breakdown.fileSize = -50;
      score -= 50;
    }
  }

  // Metadata confidence
  if (file.confidence >= 0.8) {
    breakdown.confidence = 10;
    score += 10;
  }

  return { file, score, breakdown };
}

export function formatStreamTitle(file: FileRecord, score: number): string {
  const parts: string[] = [];

  if (file.resolution) parts.push(file.resolution);
  if (file.hdr && file.hdr.toLowerCase() !== 'sdr') parts.push(file.hdr);
  if (file.video_codec) parts.push(file.video_codec.toUpperCase());

  // Best audio track
  const bestAudio = file.audio_tracks.find(t => t.is_default) || file.audio_tracks[0];
  if (bestAudio) {
    const audioParts: string[] = [];
    if (bestAudio.codec) audioParts.push(bestAudio.codec);
    if (bestAudio.channels) audioParts.push(`${bestAudio.channels}ch`);
    if (bestAudio.language) audioParts.push(bestAudio.language.toUpperCase());
    parts.push(audioParts.join(' '));
  }

  // Subtitle languages
  const subLangs = [...new Set(file.subtitle_tracks.map(t => t.language).filter(Boolean))];
  if (subLangs.length) {
    parts.push(`Sub: ${subLangs.map(l => l!.toUpperCase()).join(',')}`);
  }

  // File size
  if (file.file_size) {
    const gb = (file.file_size / (1024 ** 3)).toFixed(1);
    parts.push(`${gb}GB`);
  }

  return parts.join(' \u00b7 ');
}
