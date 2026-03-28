-- Phase 2: TMDB enrichment + forward-looking tables

-- Add Wikidata ID to content
ALTER TABLE content ADD COLUMN IF NOT EXISTS wikidata_id VARCHAR(20);
CREATE INDEX IF NOT EXISTS idx_content_wikidata ON content(wikidata_id) WHERE wikidata_id IS NOT NULL;

-- Segments table (intro/credits/recap detection)
CREATE TABLE IF NOT EXISTS segments (
    id            SERIAL PRIMARY KEY,
    content_id    INTEGER NOT NULL REFERENCES content(id) ON DELETE CASCADE,
    segment_type  VARCHAR(10) NOT NULL CHECK (segment_type IN ('intro','credits','recap')),
    start_ms      INTEGER NOT NULL,
    end_ms        INTEGER NOT NULL,
    source        VARCHAR(20),
    created_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_segments_content ON segments(content_id);

-- Trailer frame fingerprints (for visual matching)
CREATE TABLE IF NOT EXISTS trailer_frames (
    id            SERIAL PRIMARY KEY,
    content_id    INTEGER NOT NULL REFERENCES content(id) ON DELETE CASCADE,
    phash         BYTEA NOT NULL,
    frame_ts_ms   INTEGER,
    source        VARCHAR(20),
    created_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_trailer_frames_content ON trailer_frames(content_id);
