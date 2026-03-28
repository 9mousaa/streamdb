import { Router } from 'express';
import { getManifest } from '../stremio/manifest.js';
import { getStreams } from '../stremio/stream.js';
import type { UserPreferences } from '../stremio/scoring.js';
import type { DebridConfig } from '../debrid/manager.js';
import { config } from '../config.js';
import { logger } from '../utils/logger.js';

const router = Router();

interface UserConfig {
  rdApiKey?: string;
  tbApiKey?: string;
  resolutions?: string[];
  hdrFormats?: string[];
  audioLangs?: string[];
  audioFormats?: string[];
  dubLang?: string;
  subLang?: string;
  maxSizeGb?: number;
}

function decodeConfig(configStr: string): UserConfig {
  try {
    return JSON.parse(Buffer.from(configStr, 'base64url').toString('utf-8'));
  } catch {
    return {};
  }
}

function buildPreferences(cfg: UserConfig): UserPreferences {
  return {
    resolutions: cfg.resolutions || ['2160p', '1080p', '720p'],
    hdrFormats: cfg.hdrFormats || ['DV', 'HDR10+', 'HDR10', 'SDR'],
    audioLangs: cfg.audioLangs || ['en'],
    audioFormats: cfg.audioFormats || ['TrueHD', 'EAC3', 'AAC'],
    dubLang: cfg.dubLang,
    subLang: cfg.subLang,
    maxFileSizeGb: cfg.maxSizeGb || 80,
  };
}

function buildDebridConfigs(cfg: UserConfig): DebridConfig[] {
  const configs: DebridConfig[] = [];
  if (cfg.rdApiKey) configs.push({ service: 'realdebrid', apiKey: cfg.rdApiKey, priority: 1 });
  if (cfg.tbApiKey) configs.push({ service: 'torbox', apiKey: cfg.tbApiKey, priority: 2 });
  return configs;
}

// Default manifest (no config)
router.get('/manifest.json', (_req, res) => {
  res.json(getManifest(config.baseUrl));
});

// Configured manifest
router.get('/:config/manifest.json', (_req, res) => {
  res.json(getManifest(config.baseUrl));
});

// Stream endpoint
router.get('/:config/stream/:type/:id.json', async (req, res) => {
  try {
    const cfg = decodeConfig(req.params.config);
    const prefs = buildPreferences(cfg);
    const debridConfigs = buildDebridConfigs(cfg);

    if (!debridConfigs.length) {
      return res.json({ streams: [] });
    }

    const result = await getStreams(req.params.type, req.params.id, prefs, debridConfigs);
    res.json(result);
  } catch (err: any) {
    logger.error('Stream handler error', { error: err.message, id: req.params.id });
    res.json({ streams: [] });
  }
});

// Stream without config (returns empty)
router.get('/stream/:type/:id.json', (_req, res) => {
  res.json({ streams: [] });
});

export default router;
