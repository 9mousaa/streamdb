import { logger } from '../utils/logger.js';
import { pool } from '../db/pool.js';

export interface DebridConfig {
  service: string;
  apiKey: string;
  priority: number;
}

export interface DebridAvailability {
  infohash: string;
  service: string;
  available: boolean;
  files?: Record<string, { filename: string; filesize: number }>;
}

export async function checkDebridAvailability(
  infohashes: string[],
  debridConfigs: DebridConfig[]
): Promise<Map<string, DebridAvailability[]>> {
  const results = new Map<string, DebridAvailability[]>();
  if (!infohashes.length || !debridConfigs.length) return results;

  // Check cache first
  const cached = await getCachedAvailability(infohashes, debridConfigs.map(d => d.service));
  const uncachedByService = new Map<string, string[]>();

  for (const config of debridConfigs) {
    const uncached: string[] = [];
    for (const hash of infohashes) {
      const key = `${hash}:${config.service}`;
      const hit = cached.get(key);
      if (hit !== undefined) {
        if (!results.has(hash)) results.set(hash, []);
        if (hit) results.get(hash)!.push({ infohash: hash, service: config.service, available: true });
      } else {
        uncached.push(hash);
      }
    }
    if (uncached.length) uncachedByService.set(config.service, uncached);
  }

  // Fetch uncached from debrid APIs
  for (const config of debridConfigs) {
    const hashes = uncachedByService.get(config.service);
    if (!hashes?.length) continue;

    try {
      const avail = config.service === 'realdebrid'
        ? await checkRealDebrid(hashes, config.apiKey)
        : await checkTorBox(hashes, config.apiKey);

      // Store results and cache
      for (const hash of hashes) {
        const isAvailable = avail.has(hash);
        if (!results.has(hash)) results.set(hash, []);
        if (isAvailable) {
          results.get(hash)!.push({ infohash: hash, service: config.service, available: true });
        }
        await cacheAvailability(hash, config.service, isAvailable);
      }
    } catch (err: any) {
      logger.error(`Debrid check failed for ${config.service}`, { error: err.message });
    }
  }

  return results;
}

async function checkRealDebrid(hashes: string[], apiKey: string): Promise<Set<string>> {
  const available = new Set<string>();
  // Batch in groups of 100
  for (let i = 0; i < hashes.length; i += 100) {
    const batch = hashes.slice(i, i + 100);
    const url = `https://api.real-debrid.com/rest/1.0/torrents/instantAvailability/${batch.join('/')}`;
    const res = await fetch(url, {
      headers: { 'Authorization': `Bearer ${apiKey}` },
    });

    if (!res.ok) {
      logger.warn('RealDebrid API error', { status: res.status });
      continue;
    }

    const data = await res.json() as Record<string, any>;
    for (const hash of batch) {
      const entry = data[hash] || data[hash.toLowerCase()];
      if (entry && entry.rd && entry.rd.length > 0) {
        available.add(hash);
      }
    }
  }
  return available;
}

async function checkTorBox(hashes: string[], apiKey: string): Promise<Set<string>> {
  const available = new Set<string>();
  // Batch in groups of 100
  for (let i = 0; i < hashes.length; i += 100) {
    const batch = hashes.slice(i, i + 100);
    const url = `https://api.torbox.app/v1/api/torrents/checkcached?hash=${batch.join(',')}&format=list`;
    const res = await fetch(url, {
      headers: { 'Authorization': `Bearer ${apiKey}` },
    });

    if (!res.ok) {
      logger.warn('TorBox API error', { status: res.status });
      continue;
    }

    const data = await res.json() as { data?: { hash: string }[] };
    if (data.data) {
      for (const item of data.data) {
        available.add(item.hash.toLowerCase());
      }
    }
  }
  return available;
}

async function getCachedAvailability(
  hashes: string[],
  services: string[]
): Promise<Map<string, boolean>> {
  const cache = new Map<string, boolean>();
  if (!hashes.length) return cache;

  const result = await pool.query(`
    SELECT infohash, service, available
    FROM debrid_cache
    WHERE infohash = ANY($1) AND service = ANY($2) AND expires_at > NOW()
  `, [hashes, services]);

  for (const row of result.rows) {
    cache.set(`${row.infohash}:${row.service}`, row.available);
  }
  return cache;
}

async function cacheAvailability(infohash: string, service: string, available: boolean): Promise<void> {
  await pool.query(`
    INSERT INTO debrid_cache (infohash, service, available, expires_at)
    VALUES ($1, $2, $3, NOW() + INTERVAL '1 hour')
    ON CONFLICT (infohash, service) DO UPDATE
    SET available = $3, checked_at = NOW(), expires_at = NOW() + INTERVAL '1 hour'
  `, [infohash, service, available]);
}

export async function unrestrictRealDebrid(infohash: string, apiKey: string): Promise<string | null> {
  try {
    // Add magnet
    const addRes = await fetch('https://api.real-debrid.com/rest/1.0/torrents/addMagnet', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: `magnet=magnet:?xt=urn:btih:${infohash}`,
    });
    if (!addRes.ok) return null;
    const { id } = await addRes.json() as { id: string };

    // Select all files
    await fetch(`https://api.real-debrid.com/rest/1.0/torrents/selectFiles/${id}`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: 'files=all',
    });

    // Get info
    const infoRes = await fetch(`https://api.real-debrid.com/rest/1.0/torrents/info/${id}`, {
      headers: { 'Authorization': `Bearer ${apiKey}` },
    });
    if (!infoRes.ok) return null;
    const info = await infoRes.json() as { links?: string[] };
    if (!info.links?.length) return null;

    // Unrestrict first link
    const unRes = await fetch('https://api.real-debrid.com/rest/1.0/unrestrict/link', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: `link=${encodeURIComponent(info.links[0])}`,
    });
    if (!unRes.ok) return null;
    const { download } = await unRes.json() as { download: string };
    return download;
  } catch (err: any) {
    logger.error('RealDebrid unrestrict failed', { error: err.message });
    return null;
  }
}
