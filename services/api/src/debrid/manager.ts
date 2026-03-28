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
  // RD deprecated instantAvailability — use addMagnet + check status instead
  // Process up to 5 at a time to avoid rate limits
  for (let i = 0; i < hashes.length; i += 5) {
    const batch = hashes.slice(i, i + 5);
    const checks = batch.map(async (hash) => {
      try {
        const addRes = await fetch('https://api.real-debrid.com/rest/1.0/torrents/addMagnet', {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${apiKey}`,
            'Content-Type': 'application/x-www-form-urlencoded',
          },
          body: `magnet=magnet:?xt=urn:btih:${hash}`,
        });
        if (!addRes.ok) return;
        const { id } = await addRes.json() as { id: string };

        // Check info — if status is 'waiting_files_selection', it's cached
        const infoRes = await fetch(`https://api.real-debrid.com/rest/1.0/torrents/info/${id}`, {
          headers: { 'Authorization': `Bearer ${apiKey}` },
        });
        if (!infoRes.ok) return;
        const info = await infoRes.json() as { status?: string; files?: any[] };

        if (info.status === 'waiting_files_selection' || info.status === 'downloaded') {
          available.add(hash);
        }

        // Clean up — delete the torrent entry to not pollute user's list
        await fetch(`https://api.real-debrid.com/rest/1.0/torrents/delete/${id}`, {
          method: 'DELETE',
          headers: { 'Authorization': `Bearer ${apiKey}` },
        });
      } catch { /* skip */ }
    });
    await Promise.all(checks);
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

/**
 * Get a direct download URL from a debrid service for a given infohash.
 * Tries each configured service in priority order until one succeeds.
 */
export async function unrestrictHash(
  infohash: string,
  fileIdx: number,
  debridConfigs: DebridConfig[]
): Promise<{ url: string; service: string } | null> {
  for (const cfg of debridConfigs) {
    try {
      const url = cfg.service === 'realdebrid'
        ? await unrestrictRealDebrid(infohash, fileIdx, cfg.apiKey)
        : await unrestrictTorBox(infohash, fileIdx, cfg.apiKey);
      if (url) return { url, service: cfg.service };
    } catch (err: any) {
      logger.warn(`Unrestrict failed for ${cfg.service}`, { infohash, error: err.message });
    }
  }
  return null;
}

export async function unrestrictRealDebrid(infohash: string, fileIdx: number, apiKey: string): Promise<string | null> {
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

    // Get torrent info to find the right file
    const infoRes = await fetch(`https://api.real-debrid.com/rest/1.0/torrents/info/${id}`, {
      headers: { 'Authorization': `Bearer ${apiKey}` },
    });
    if (!infoRes.ok) return null;
    const info = await infoRes.json() as { files?: { id: number; path: string; bytes: number; selected: number }[]; links?: string[]; status?: string };

    // If files are available but not yet selected, select the target file
    if (info.files && info.files.length > 0 && (!info.links || info.links.length === 0)) {
      // RD file IDs are 1-based; fileIdx is 0-based
      const targetFileId = fileIdx + 1;
      const fileExists = info.files.some(f => f.id === targetFileId);
      const selectId = fileExists ? String(targetFileId) : 'all';

      await fetch(`https://api.real-debrid.com/rest/1.0/torrents/selectFiles/${id}`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: `files=${selectId}`,
      });

      // Re-fetch info after selecting
      const infoRes2 = await fetch(`https://api.real-debrid.com/rest/1.0/torrents/info/${id}`, {
        headers: { 'Authorization': `Bearer ${apiKey}` },
      });
      if (!infoRes2.ok) return null;
      const info2 = await infoRes2.json() as { links?: string[] };
      if (!info2.links?.length) return null;

      // Unrestrict the first available link
      return await unrestrictRdLink(info2.links[0], apiKey);
    }

    if (!info.links?.length) return null;
    return await unrestrictRdLink(info.links[0], apiKey);
  } catch (err: any) {
    logger.error('RealDebrid unrestrict failed', { error: err.message });
    return null;
  }
}

async function unrestrictRdLink(link: string, apiKey: string): Promise<string | null> {
  const unRes = await fetch('https://api.real-debrid.com/rest/1.0/unrestrict/link', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${apiKey}`,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: `link=${encodeURIComponent(link)}`,
  });
  if (!unRes.ok) return null;
  const { download } = await unRes.json() as { download: string };
  return download;
}

async function unrestrictTorBox(infohash: string, fileIdx: number, apiKey: string): Promise<string | null> {
  try {
    // Create torrent request — TorBox requires multipart form data
    const formBody = new URLSearchParams();
    formBody.set('magnet', `magnet:?xt=urn:btih:${infohash}`);
    formBody.set('seed', '1');

    const createRes = await fetch('https://api.torbox.app/v1/api/torrents/createtorrent', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: formBody.toString(),
    });
    if (!createRes.ok) {
      logger.warn('TorBox createtorrent failed', { status: createRes.status });
      return null;
    }
    const createData = await createRes.json() as { success?: boolean; data?: { torrent_id: number } };
    if (!createData.success || !createData.data?.torrent_id) return null;
    const torrentId = createData.data.torrent_id;

    // Request download link — TorBox uses token as query param for this endpoint
    const dlRes = await fetch(
      `https://api.torbox.app/v1/api/torrents/requestdl?token=${apiKey}&torrent_id=${torrentId}&file_id=${fileIdx}&zip_link=false`
    );
    if (!dlRes.ok) {
      logger.warn('TorBox requestdl failed', { status: dlRes.status, torrentId });
      return null;
    }
    const dlData = await dlRes.json() as { success?: boolean; data?: string };
    return dlData.success && dlData.data ? dlData.data : null;
  } catch (err: any) {
    logger.error('TorBox unrestrict failed', { error: err.message });
    return null;
  }
}
