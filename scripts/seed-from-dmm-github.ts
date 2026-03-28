#!/usr/bin/env npx tsx
/**
 * StreamDB Seeder — Fetches DebridMediaManager hashlists from GitHub
 * DMM publishes movie/TV hashlists as JSON files in their GitHub repo.
 *
 * Usage:
 *   npx tsx scripts/seed-from-dmm-github.ts
 *
 * This fetches the hashlist index from DMM's GitHub, downloads each file,
 * and imports all IMDB→infohash mappings into the database.
 *
 * Env:
 *   DATABASE_URL — Postgres connection string
 */

import pg from 'pg';

const BATCH_SIZE = 500;
const DMM_API = 'https://api.github.com/repos/debridmediamanager/hashlists/contents';
const DMM_RAW = 'https://raw.githubusercontent.com/debridmediamanager/hashlists/main';

function sleep(ms: number) {
  return new Promise(r => setTimeout(r, ms));
}

async function fetchJson(url: string, retries = 3): Promise<any> {
  for (let i = 0; i < retries; i++) {
    try {
      const res = await fetch(url, {
        headers: {
          'User-Agent': 'StreamDB/1.0',
          'Accept': 'application/json',
        },
        signal: AbortSignal.timeout(30000),
      });
      if (res.status === 403 || res.status === 429) {
        console.log(`  Rate limited (${res.status}), waiting ${10 * (i + 1)}s...`);
        await sleep(10000 * (i + 1));
        continue;
      }
      if (!res.ok) return null;
      const text = await res.text();
      try { return JSON.parse(text); } catch { return null; }
    } catch (err: any) {
      if (i < retries - 1) {
        await sleep(3000 * (i + 1));
        continue;
      }
      return null;
    }
  }
  return null;
}

function parseFromFilename(name: string): {
  resolution: string | null;
  codec: string | null;
  hdr: string | null;
  season: number | null;
  episode: number | null;
} {
  let resolution: string | null = null;
  if (/2160p|4k|uhd/i.test(name)) resolution = '2160p';
  else if (/1080p/i.test(name)) resolution = '1080p';
  else if (/720p/i.test(name)) resolution = '720p';
  else if (/480p/i.test(name)) resolution = '480p';

  let codec: string | null = null;
  if (/x265|hevc|h\.?265/i.test(name)) codec = 'x265';
  else if (/x264|h\.?264|avc/i.test(name)) codec = 'x264';
  else if (/av1/i.test(name)) codec = 'AV1';

  let hdr: string | null = 'SDR';
  if (/dolby[\.\s-]?vision|[\.\s]dv[\.\s]|dovi/i.test(name)) hdr = 'DV';
  else if (/hdr10\+|hdr10plus/i.test(name)) hdr = 'HDR10+';
  else if (/hdr10|hdr/i.test(name)) hdr = 'HDR10';

  const seMatch = name.match(/S(\d{1,2})E(\d{1,3})/i);
  const season = seMatch ? parseInt(seMatch[1]) : null;
  const episode = seMatch ? parseInt(seMatch[2]) : null;

  return { resolution, codec, hdr, season, episode };
}

async function importHashlist(
  client: pg.PoolClient,
  data: Record<string, any[]>,
): Promise<number> {
  let imported = 0;
  let batchCount = 0;

  const imdbIds = Object.keys(data).filter(k => k.startsWith('tt'));

  for (const imdbId of imdbIds) {
    const entries = data[imdbId];
    if (!Array.isArray(entries)) continue;

    for (const entry of entries) {
      const hash = (entry.hash || entry.infohash || entry.infoHash || '').toLowerCase();
      if (!hash || hash.length !== 40) continue;

      const filename = entry.filename || entry.title || entry.name || '';
      const filesize = entry.filesize || entry.size || null;
      const parsed = parseFromFilename(filename);

      const isEpisode = parsed.season !== null && parsed.episode !== null;
      const contentType = isEpisode ? 'episode' : 'movie';
      const contentKey = isEpisode ? `${imdbId}:${parsed.season}:${parsed.episode}` : imdbId;

      // Upsert file
      await client.query(`
        INSERT INTO files (infohash, file_idx, filename, file_size, torrent_name, resolution, video_codec, hdr, metadata_src, confidence)
        VALUES ($1, 0, $2, $3, $4, $5, $6, $7, 'dmm_github', 0.3)
        ON CONFLICT (infohash, file_idx) DO NOTHING
      `, [hash, filename, filesize, filename, parsed.resolution, parsed.codec, parsed.hdr]);

      // Upsert content
      const contentResult = await client.query(`
        INSERT INTO content (imdb_id, type, title, year)
        VALUES ($1, $2, $3, NULL)
        ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW()
        RETURNING id
      `, [contentKey, contentType, filename.split(/[\.\s](?:(?:19|20)\d{2})/)[0]?.replace(/\./g, ' ').trim() || imdbId]);

      // Create edge
      const fileResult = await client.query('SELECT id FROM files WHERE infohash = $1 AND file_idx = 0', [hash]);
      if (fileResult.rows[0] && contentResult.rows[0]) {
        await client.query(`
          INSERT INTO content_files (content_id, file_id, match_method, confidence)
          VALUES ($1, $2, 'dmm_github', 0.7)
          ON CONFLICT DO NOTHING
        `, [contentResult.rows[0].id, fileResult.rows[0].id]);
      }

      // Queue probe job
      await client.query(`
        INSERT INTO probe_jobs (infohash, priority, source)
        VALUES ($1, 10, 'dmm_github')
        ON CONFLICT (infohash) DO NOTHING
      `, [hash]);

      imported++;
      batchCount++;

      if (batchCount >= BATCH_SIZE) {
        await client.query('COMMIT');
        await client.query('BEGIN');
        batchCount = 0;
      }
    }
  }

  return imported;
}

async function main() {
  const dbUrl = process.env.DATABASE_URL || 'postgres://streamdb:streamdb_secret@localhost:5432/streamdb';
  const pool = new pg.Pool({ connectionString: dbUrl });
  const client = await pool.connect();

  let totalImported = 0;
  let totalFiles = 0;

  try {
    // List files in the DMM hashlists repo
    console.log('Fetching DMM hashlist index from GitHub...');

    // Try to get directory listing
    const listing = await fetchJson(DMM_API);

    if (Array.isArray(listing)) {
      // GitHub API returned directory contents
      const jsonFiles = listing.filter((f: any) => f.name.endsWith('.json') && f.type === 'file');
      console.log(`Found ${jsonFiles.length} hashlist files\n`);

      for (let i = 0; i < jsonFiles.length; i++) {
        const file = jsonFiles[i];
        console.log(`\n[${i + 1}/${jsonFiles.length}] Downloading: ${file.name}`);

        const rawUrl = file.download_url || `${DMM_RAW}/${file.name}`;
        const data = await fetchJson(rawUrl);

        if (!data || typeof data !== 'object') {
          console.log('  Skipping (invalid data)');
          continue;
        }

        await client.query('BEGIN');
        const count = await importHashlist(client, data);
        await client.query('COMMIT');

        totalImported += count;
        totalFiles++;
        console.log(`  Imported: ${count.toLocaleString()} hashes (total: ${totalImported.toLocaleString()})`);

        await sleep(1000); // Rate limit GitHub
      }
    } else {
      // Fallback: try known file paths
      console.log('Could not list directory, trying known paths...');
      const knownFiles = ['movies.json', 'tv.json', 'movie.json', 'series.json', 'hashes.json'];

      for (const filename of knownFiles) {
        console.log(`Trying: ${filename}`);
        const data = await fetchJson(`${DMM_RAW}/${filename}`);
        if (data && typeof data === 'object') {
          await client.query('BEGIN');
          const count = await importHashlist(client, data);
          await client.query('COMMIT');
          totalImported += count;
          totalFiles++;
          console.log(`  Imported: ${count.toLocaleString()} hashes`);
        }
      }
    }
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch {}
    throw err;
  } finally {
    client.release();
  }

  // Print stats
  const stats = await pool.query(`
    SELECT
      (SELECT COUNT(*) FROM content) as content,
      (SELECT COUNT(*) FROM files) as files,
      (SELECT COUNT(*) FROM content_files) as mappings,
      (SELECT COUNT(*) FROM probe_jobs) as probe_jobs
  `);
  console.log('\n═══════════════════════════════════════');
  console.log(`Imported: ${totalImported.toLocaleString()} hashes from ${totalFiles} files`);
  console.log('DB Stats:', stats.rows[0]);
  console.log('═══════════════════════════════════════');

  await pool.end();
}

main().catch(err => {
  console.error('Seed failed:', err);
  process.exit(1);
});
