-- StreamDB Core Schema
-- Identity Graph: content nodes, file nodes, and edges between them

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Content nodes (movies, series, episodes)
CREATE TABLE IF NOT EXISTS content (
    id            SERIAL PRIMARY KEY,
    imdb_id       VARCHAR(12) UNIQUE,
    tmdb_id       INTEGER,
    tvdb_id       INTEGER,
    mal_id        INTEGER,
    anidb_id      INTEGER,
    type          VARCHAR(10) NOT NULL CHECK (type IN ('movie','series','episode')),
    title         TEXT NOT NULL,
    year          SMALLINT,
    season        SMALLINT,
    episode       SMALLINT,
    parent_imdb   VARCHAR(12),
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_content_imdb ON content(imdb_id);
CREATE INDEX IF NOT EXISTS idx_content_tmdb ON content(tmdb_id);
CREATE INDEX IF NOT EXISTS idx_content_parent ON content(parent_imdb, season, episode);
CREATE INDEX IF NOT EXISTS idx_content_title_trgm ON content USING gin(title gin_trgm_ops);

-- File nodes (unique torrent files)
CREATE TABLE IF NOT EXISTS files (
    id            SERIAL PRIMARY KEY,
    infohash      CHAR(40) NOT NULL,
    file_idx      SMALLINT DEFAULT 0,
    filename      TEXT,
    file_size     BIGINT,
    torrent_name  TEXT,
    -- Video
    resolution    VARCHAR(10),
    video_codec   VARCHAR(20),
    hdr           VARCHAR(20),
    bit_depth     SMALLINT,
    bitrate       INTEGER,
    duration      INTEGER,
    container     VARCHAR(10),
    -- Identity
    os_hash       VARCHAR(16),
    chromaprint   TEXT,
    phash_seq     TEXT,
    -- Provenance
    verified_by   VARCHAR(20)[],
    metadata_src  VARCHAR(20),
    confidence    REAL DEFAULT 0,
    discovered_at TIMESTAMPTZ DEFAULT NOW(),
    probed_at     TIMESTAMPTZ,
    UNIQUE(infohash, file_idx)
);
CREATE INDEX IF NOT EXISTS idx_files_infohash ON files(infohash);
CREATE INDEX IF NOT EXISTS idx_files_os_hash ON files(os_hash) WHERE os_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_files_resolution ON files(resolution);
CREATE INDEX IF NOT EXISTS idx_files_probed ON files(probed_at) WHERE probed_at IS NULL;

-- Audio/subtitle tracks per file
CREATE TABLE IF NOT EXISTS file_tracks (
    id            SERIAL PRIMARY KEY,
    file_id       INTEGER REFERENCES files(id) ON DELETE CASCADE,
    track_type    VARCHAR(10) NOT NULL CHECK (track_type IN ('audio','subtitle')),
    codec         VARCHAR(20),
    channels      REAL,
    language      VARCHAR(5),
    sub_format    VARCHAR(20),
    forced        BOOLEAN DEFAULT FALSE,
    is_default    BOOLEAN DEFAULT FALSE,
    track_index   SMALLINT
);
CREATE INDEX IF NOT EXISTS idx_file_tracks_file ON file_tracks(file_id);
CREATE INDEX IF NOT EXISTS idx_file_tracks_lang ON file_tracks(language, track_type);

-- Content <-> File mapping (graph edges)
CREATE TABLE IF NOT EXISTS content_files (
    content_id    INTEGER REFERENCES content(id) ON DELETE CASCADE,
    file_id       INTEGER REFERENCES files(id) ON DELETE CASCADE,
    match_method  VARCHAR(20),
    confidence    REAL DEFAULT 0,
    PRIMARY KEY (content_id, file_id)
);
CREATE INDEX IF NOT EXISTS idx_cf_content ON content_files(content_id);
CREATE INDEX IF NOT EXISTS idx_cf_file ON content_files(file_id);

-- Debrid availability cache
CREATE TABLE IF NOT EXISTS debrid_cache (
    infohash      CHAR(40) NOT NULL,
    service       VARCHAR(20) NOT NULL,
    available     BOOLEAN NOT NULL,
    file_ids      JSONB,
    checked_at    TIMESTAMPTZ DEFAULT NOW(),
    expires_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (infohash, service)
);
CREATE INDEX IF NOT EXISTS idx_debrid_expires ON debrid_cache(expires_at);
CREATE INDEX IF NOT EXISTS idx_debrid_avail ON debrid_cache(available, service) WHERE available = TRUE;

-- Probe job queue
CREATE TABLE IF NOT EXISTS probe_jobs (
    id            SERIAL PRIMARY KEY,
    infohash      CHAR(40) UNIQUE NOT NULL,
    status        VARCHAR(20) DEFAULT 'pending',
    priority      SMALLINT DEFAULT 0,
    source        VARCHAR(20),
    attempts      SMALLINT DEFAULT 0,
    max_attempts  SMALLINT DEFAULT 3,
    claimed_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    error         TEXT,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_probe_pending ON probe_jobs(priority DESC, created_at)
    WHERE status = 'pending';

-- Audio fingerprints (Phase 5)
CREATE TABLE IF NOT EXISTS audio_fingerprints (
    id            SERIAL PRIMARY KEY,
    content_id    INTEGER NOT NULL REFERENCES content(id) ON DELETE CASCADE,
    file_id       INTEGER REFERENCES files(id) ON DELETE SET NULL,
    fingerprint   BYTEA NOT NULL,
    duration_ms   INTEGER,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Video frame fingerprints (Phase 5)
CREATE TABLE IF NOT EXISTS video_fingerprints (
    id            SERIAL PRIMARY KEY,
    content_id    INTEGER NOT NULL REFERENCES content(id) ON DELETE CASCADE,
    phash         BYTEA NOT NULL,
    frame_ts_ms   INTEGER,
    source        VARCHAR(20),
    created_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_video_fp_content ON video_fingerprints(content_id);
