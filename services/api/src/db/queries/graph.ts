import { pool } from '../pool.js';

export interface FileRecord {
  id: number;
  infohash: string;
  file_idx: number;
  filename: string | null;
  file_size: number | null;
  torrent_name: string | null;
  resolution: string | null;
  video_codec: string | null;
  hdr: string | null;
  bit_depth: number | null;
  bitrate: number | null;
  duration: number | null;
  container: string | null;
  metadata_src: string | null;
  confidence: number;
  audio_tracks: TrackRecord[];
  subtitle_tracks: TrackRecord[];
}

export interface TrackRecord {
  codec: string | null;
  channels: number | null;
  language: string | null;
  sub_format: string | null;
  forced: boolean;
  is_default: boolean;
}

export async function getFilesForContent(imdbId: string): Promise<FileRecord[]> {
  const result = await pool.query(`
    SELECT
      f.id, f.infohash, f.file_idx, f.filename, f.file_size, f.torrent_name,
      f.resolution, f.video_codec, f.hdr, f.bit_depth, f.bitrate,
      f.duration, f.container, f.metadata_src, f.confidence
    FROM files f
    JOIN content_files cf ON cf.file_id = f.id
    JOIN content c ON c.id = cf.content_id
    WHERE c.imdb_id = $1
    ORDER BY f.confidence DESC, f.file_size DESC
  `, [imdbId]);

  const files: FileRecord[] = [];

  for (const row of result.rows) {
    const tracks = await pool.query(`
      SELECT track_type, codec, channels, language, sub_format, forced, is_default
      FROM file_tracks WHERE file_id = $1
    `, [row.id]);

    files.push({
      ...row,
      audio_tracks: tracks.rows.filter((t: any) => t.track_type === 'audio'),
      subtitle_tracks: tracks.rows.filter((t: any) => t.track_type === 'subtitle'),
    });
  }

  return files;
}

export async function queueProbeJob(infohash: string, priority: number = 0, source: string = 'auto'): Promise<void> {
  await pool.query(`
    INSERT INTO probe_jobs (infohash, priority, source)
    VALUES ($1, $2, $3)
    ON CONFLICT (infohash) DO NOTHING
  `, [infohash, priority, source]);
}

export async function getFilesForEpisode(imdbId: string, season: number, episode: number): Promise<FileRecord[]> {
  // First try exact episode match
  const result = await pool.query(`
    SELECT
      f.id, f.infohash, f.file_idx, f.filename, f.file_size, f.torrent_name,
      f.resolution, f.video_codec, f.hdr, f.bit_depth, f.bitrate,
      f.duration, f.container, f.metadata_src, f.confidence
    FROM files f
    JOIN content_files cf ON cf.file_id = f.id
    JOIN content c ON c.id = cf.content_id
    WHERE c.parent_imdb = $1 AND c.season = $2 AND c.episode = $3
    ORDER BY f.confidence DESC, f.file_size DESC
  `, [imdbId, season, episode]);

  const files: FileRecord[] = [];

  for (const row of result.rows) {
    const tracks = await pool.query(`
      SELECT track_type, codec, channels, language, sub_format, forced, is_default
      FROM file_tracks WHERE file_id = $1
    `, [row.id]);

    files.push({
      ...row,
      audio_tracks: tracks.rows.filter((t: any) => t.track_type === 'audio'),
      subtitle_tracks: tracks.rows.filter((t: any) => t.track_type === 'subtitle'),
    });
  }

  return files;
}
