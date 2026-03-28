#!/usr/bin/env npx tsx
/**
 * Knightcrawler Database Importer
 * Reads Knightcrawler's PostgreSQL (ingested_torrents table) and imports
 * IMDB→infohash mappings into StreamDB.
 *
 * Usage:
 *   KC_DB=postgres://user:pass@host:5432/knightcrawler npx tsx scripts/import-knightcrawler.ts
 *
 * Knightcrawler schema:
 *   ingested_torrents: info_hash, name, source, category, imdb, size, seeders, leechers
 */

import pg from 'pg';

const BATCH_SIZE = 1000;
const COMMIT_EVERY = 10000;

function parseTitle(raw: string): {
  resolution: string | null;
  codec: string | null;
  hdr: string | null;
  season: number | null;
  episode: number | null;
  cleanTitle: string;
} {
  const t = raw;

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

  const seMatch = t.match(/S(\d{1,2})E(\d{1,3})/i);
  const season = seMatch ? parseInt(seMatch[1]) : null;
  const episode = seMatch ? parseInt(seMatch[2]) : null;

  const cleanTitle = t.split(/[\.\s](?:(?:19|20)\d{2})/)[0]?.replace(/\./g, ' ').trim() || t;

  return { resolution, codec, hdr, season, episode, cleanTitle };
}

async function main() {
  const kcUrl = process.env.KC_DB;
  if (!kcUrl) {
    console.error('Usage: KC_DB=postgres://... npx tsx scripts/import-knightcrawler.ts');
    process.exit(1);
  }

  const streamdbUrl = process.env.DATABASE_URL || 'postgres://streamdb:streamdb_secret@localhost:5432/streamdb';

  const kc = new pg.Pool({ connectionString: kcUrl, max: 5 });
  const streamdb = new pg.Pool({ connectionString: streamdbUrl, max: 5 });

  // Count torrents
  const countResult = await kc.query('SELECT COUNT(*)::int as total FROM ingested_torrents');
  const total = countResult.rows[0].total;
  console.log(`Total Knightcrawler torrents: ${total.toLocaleString()}`);

  const client = await streamdb.connect();
  let imported = 0;
  let skipped = 0;
  let edges = 0;
  let offset = 0;

  try {
    await client.query('BEGIN');

    while (offset < total) {
      const batch = await kc.query(
        `SELECT info_hash, name, imdb, size, category, source, seeders
         FROM ingested_torrents
         ORDER BY info_hash
         LIMIT $1 OFFSET $2`,
        [BATCH_SIZE, offset]
      );

      if (batch.rows.length === 0) break;

      for (const row of batch.rows) {
        const hash = (row.info_hash || '').toLowerCase().trim();
        if (!hash || hash.length !== 40) { skipped++; continue; }

        const name = row.name || '';
        const parsed = parseTitle(name);

        // Insert file
        await client.query(`
          INSERT INTO files (infohash, file_idx, filename, file_size, torrent_name, resolution, video_codec, hdr, metadata_src, confidence)
          VALUES ($1, 0, $2, $3, $4, $5, $6, $7, 'knightcrawler', 0.5)
          ON CONFLICT (infohash, file_idx) DO NOTHING
        `, [hash, name, row.size || null, name, parsed.resolution, parsed.codec, parsed.hdr]);

        // Create content + edge if we have IMDB
        const imdbId = row.imdb;
        if (imdbId && imdbId.startsWith('tt')) {
          const isEpisode = parsed.season !== null && parsed.episode !== null;
          const category = row.category || '';
          const isTv = isEpisode || /tv|series/i.test(category);

          if (isEpisode) {
            const epImdb = `${imdbId}:${parsed.season}:${parsed.episode}`;
            const contentResult = await client.query(`
              INSERT INTO content (imdb_id, type, title, year, season, episode, parent_imdb)
              VALUES ($1, 'episode', $2, NULL, $3, $4, $5)
              ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW()
              RETURNING id
            `, [epImdb, parsed.cleanTitle, parsed.season, parsed.episode, imdbId]);

            const fileResult = await client.query('SELECT id FROM files WHERE infohash = $1 AND file_idx = 0', [hash]);
            if (fileResult.rows[0] && contentResult.rows[0]) {
              await client.query(`
                INSERT INTO content_files (content_id, file_id, match_method, confidence)
                VALUES ($1, $2, 'knightcrawler', 0.8)
                ON CONFLICT DO NOTHING
              `, [contentResult.rows[0].id, fileResult.rows[0].id]);
              edges++;
            }

            // Parent series node
            await client.query(`
              INSERT INTO content (imdb_id, type, title, year)
              VALUES ($1, 'series', $2, NULL)
              ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW()
            `, [imdbId, parsed.cleanTitle]);
          } else {
            const type = isTv ? 'series' : 'movie';
            const contentResult = await client.query(`
              INSERT INTO content (imdb_id, type, title, year)
              VALUES ($1, $2, $3, NULL)
              ON CONFLICT (imdb_id) DO UPDATE SET updated_at = NOW()
              RETURNING id
            `, [imdbId, type, parsed.cleanTitle]);

            const fileResult = await client.query('SELECT id FROM files WHERE infohash = $1 AND file_idx = 0', [hash]);
            if (fileResult.rows[0] && contentResult.rows[0]) {
              await client.query(`
                INSERT INTO content_files (content_id, file_id, match_method, confidence)
                VALUES ($1, $2, 'knightcrawler', 0.8)
                ON CONFLICT DO NOTHING
              `, [contentResult.rows[0].id, fileResult.rows[0].id]);
              edges++;
            }
          }
        }

        imported++;
      }

      if (imported > 0 && imported % COMMIT_EVERY === 0) {
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

  await kc.end();
  await streamdb.end();
}

main().catch(err => {
  console.error('Knightcrawler import failed:', err);
  process.exit(1);
});
