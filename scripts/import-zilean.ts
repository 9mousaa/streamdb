#!/usr/bin/env npx tsx
/**
 * Zilean Database Importer
 * Reads Zilean's PostgreSQL (Torrents + ImdbFiles tables) and imports
 * IMDB→infohash mappings into StreamDB.
 *
 * Usage:
 *   ZILEAN_DB=postgres://user:pass@host:5432/zilean npx tsx scripts/import-zilean.ts
 *
 * Zilean schema:
 *   "Torrents": "InfoHash" (PK), "ImdbId" (FK), "RawTitle", "ParsedTitle",
 *               "Resolution", "Seasons" (int[]), "Episodes" (int[]),
 *               "Languages" (text[]), "Hdr" (text[]), "Codec", "Audio" (text[]),
 *               "Size" (bigint), "Year" (int), ...
 *   "ImdbFiles": "ImdbId" (PK), "Category", "Title", "Year" (int), "Adult" (bool)
 */

import pg from 'pg';

const BATCH_SIZE = 1000;
const COMMIT_EVERY = 10000;

function parseTitle(raw: string): { resolution: string | null; codec: string | null; hdr: string | null } {
  const t = raw.toLowerCase();

  let resolution: string | null = null;
  if (/2160p|4k|uhd/i.test(t)) resolution = '2160p';
  else if (/1080p/i.test(t)) resolution = '1080p';
  else if (/720p/i.test(t)) resolution = '720p';
  else if (/480p/i.test(t)) resolution = '480p';

  let codec: string | null = null;
  if (/x265|hevc|h\.?265/i.test(t)) codec = 'x265';
  else if (/x264|h\.?264|avc/i.test(t)) codec = 'x264';
  else if (/av1/i.test(t)) codec = 'AV1';

  let hdr: string | null = 'SDR';
  if (/dolby[\.\s-]?vision|[\.\s]dv[\.\s]|dovi/i.test(t)) hdr = 'DV';
  else if (/hdr10\+|hdr10plus/i.test(t)) hdr = 'HDR10+';
  else if (/hdr10|hdr/i.test(t)) hdr = 'HDR10';

  return { resolution, codec, hdr };
}

async function main() {
  const zileanUrl = process.env.ZILEAN_DB;
  if (!zileanUrl) {
    console.error('Usage: ZILEAN_DB=postgres://... npx tsx scripts/import-zilean.ts');
    process.exit(1);
  }

  const streamdbUrl = process.env.DATABASE_URL || 'postgres://streamdb:streamdb_secret@localhost:5432/streamdb';

  const zilean = new pg.Pool({ connectionString: zileanUrl, max: 5 });
  const streamdb = new pg.Pool({ connectionString: streamdbUrl, max: 5 });

  // Load IMDB files into memory for fast lookup
  console.log('Loading Zilean ImdbFiles...');
  const imdbResult = await zilean.query('SELECT "ImdbId", "Category", "Title", "Year" FROM "ImdbFiles"');
  const imdbMap = new Map<string, { category: string; title: string; year: number | null }>();
  for (const row of imdbResult.rows) {
    imdbMap.set(row.ImdbId, {
      category: row.Category || 'movie',
      title: row.Title || '',
      year: row.Year || null,
    });
  }
  console.log(`Loaded ${imdbMap.size.toLocaleString()} IMDB entries`);

  // Count torrents
  const countResult = await zilean.query('SELECT COUNT(*)::int as total FROM "Torrents"');
  const total = countResult.rows[0].total;
  console.log(`Total Zilean torrents: ${total.toLocaleString()}`);

  const client = await streamdb.connect();
  let imported = 0;
  let skipped = 0;
  let edges = 0;
  let offset = 0;

  try {
    await client.query('BEGIN');

    while (offset < total) {
      const batch = await zilean.query(
        `SELECT "InfoHash", "ImdbId", "RawTitle", "ParsedTitle", "Resolution",
                "Seasons", "Episodes", "Languages", "Hdr", "Codec", "Audio",
                "Size", "Year"
         FROM "Torrents"
         ORDER BY "InfoHash"
         LIMIT $1 OFFSET $2`,
        [BATCH_SIZE, offset]
      );

      if (batch.rows.length === 0) break;

      for (const t of batch.rows) {
        const hash = (t.InfoHash || '').toLowerCase().trim();
        if (!hash || hash.length !== 40) { skipped++; continue; }

        const imdbId = t.ImdbId || null;
        const rawTitle = t.RawTitle || t.ParsedTitle || '';
        const parsed = parseTitle(rawTitle);

        // Use Zilean's metadata if available, fall back to title parsing
        const resolution = t.Resolution || parsed.resolution;
        const codec = t.Codec || parsed.codec;
        const hdrArr = Array.isArray(t.Hdr) && t.Hdr.length > 0 ? t.Hdr : null;
        const hdr = hdrArr ? hdrArr[0] : parsed.hdr;
        const fileSize = t.Size ? BigInt(t.Size) : null;

        // Insert file
        await client.query(`
          INSERT INTO files (infohash, file_idx, filename, file_size, torrent_name, resolution, video_codec, hdr, metadata_src, confidence)
          VALUES ($1, 0, $2, $3, $4, $5, $6, $7, 'zilean', 0.5)
          ON CONFLICT (infohash, file_idx) DO NOTHING
        `, [hash, rawTitle, fileSize, rawTitle, resolution, codec, hdr]);

        // Create content + edge if we have IMDB
        if (imdbId && imdbId.startsWith('tt')) {
          const imdbInfo = imdbMap.get(imdbId);
          const title = imdbInfo?.title || rawTitle.split(/[\.\s](?:(?:19|20)\d{2})/)[0]?.replace(/\./g, ' ').trim() || imdbId;
          const year = t.Year || imdbInfo?.year || null;
          const category = imdbInfo?.category || 'movie';

          const seasons = Array.isArray(t.Seasons) ? t.Seasons : [];
          const episodes = Array.isArray(t.Episodes) ? t.Episodes : [];

          if (seasons.length > 0 && episodes.length > 0) {
            // Series with specific episodes — create episode nodes
            for (let si = 0; si < seasons.length; si++) {
              const season = seasons[si];
              const episode = episodes[si] || episodes[0];
              const epImdb = `${imdbId}:${season}:${episode}`;

              const contentResult = await client.query(`
                INSERT INTO content (imdb_id, type, title, year, season, episode, parent_imdb)
                VALUES ($1, 'episode', $2, $3, $4, $5, $6)
                ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW()
                RETURNING id
              `, [epImdb, title, year, season, episode, imdbId]);

              const fileResult = await client.query('SELECT id FROM files WHERE infohash = $1 AND file_idx = 0', [hash]);
              if (fileResult.rows[0] && contentResult.rows[0]) {
                await client.query(`
                  INSERT INTO content_files (content_id, file_id, match_method, confidence)
                  VALUES ($1, $2, 'zilean', 0.8)
                  ON CONFLICT DO NOTHING
                `, [contentResult.rows[0].id, fileResult.rows[0].id]);
                edges++;
              }
            }

            // Also create the parent series node
            await client.query(`
              INSERT INTO content (imdb_id, type, title, year)
              VALUES ($1, 'series', $2, $3)
              ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW()
            `, [imdbId, title, year]);
          } else {
            // Movie or series without episode info
            const type = category === 'tv' || category === 'tvSeries' ? 'series' : 'movie';
            const contentResult = await client.query(`
              INSERT INTO content (imdb_id, type, title, year)
              VALUES ($1, $2, $3, $4)
              ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW()
              RETURNING id
            `, [imdbId, type, title, year]);

            const fileResult = await client.query('SELECT id FROM files WHERE infohash = $1 AND file_idx = 0', [hash]);
            if (fileResult.rows[0] && contentResult.rows[0]) {
              await client.query(`
                INSERT INTO content_files (content_id, file_id, match_method, confidence)
                VALUES ($1, $2, 'zilean', 0.8)
                ON CONFLICT DO NOTHING
              `, [contentResult.rows[0].id, fileResult.rows[0].id]);
              edges++;
            }
          }
        }

        imported++;
      }

      // Commit periodically
      if (imported % COMMIT_EVERY < BATCH_SIZE) {
        await client.query('COMMIT');
        await client.query('BEGIN');
        const pct = ((offset / total) * 100).toFixed(1);
        console.log(`Progress: ${imported.toLocaleString()} files, ${edges.toLocaleString()} edges (${pct}%)`);
      }

      offset += BATCH_SIZE;
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }

  // Print stats
  const stats = await streamdb.query(`
    SELECT
      (SELECT COUNT(*) FROM content) as content_count,
      (SELECT COUNT(*) FROM files) as file_count,
      (SELECT COUNT(*) FROM content_files) as edge_count
  `);

  console.log('\n═══════════════════════════════════════');
  console.log(`Imported: ${imported.toLocaleString()} files, ${edges.toLocaleString()} edges`);
  console.log(`Skipped: ${skipped.toLocaleString()}`);
  console.log('DB Stats:', stats.rows[0]);
  console.log('═══════════════════════════════════════');

  await zilean.end();
  await streamdb.end();
}

main().catch(err => {
  console.error('Zilean import failed:', err);
  process.exit(1);
});
