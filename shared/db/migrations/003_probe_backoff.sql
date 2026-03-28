-- Phase 3: Probe backoff + runtime enrichment

-- Add next_attempt_at for exponential backoff on failed probe jobs
ALTER TABLE probe_jobs ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ DEFAULT NOW();

-- Add runtime_minutes for duration-based content matching
ALTER TABLE content ADD COLUMN IF NOT EXISTS runtime_minutes SMALLINT;

-- Fix priority inversion: boost seeded sources (popular, known IMDB-mapped),
-- lower DHT (random hashes with mostly dead peers)
UPDATE probe_jobs SET priority = 10 WHERE source IN ('dmm_github', 'torrentio_seed', 'dmm_hashlist') AND status = 'pending';
UPDATE probe_jobs SET priority = 1 WHERE source = 'dht' AND status = 'pending';

-- Update the partial index to include next_attempt_at
DROP INDEX IF EXISTS idx_probe_pending;
CREATE INDEX IF NOT EXISTS idx_probe_pending ON probe_jobs(priority DESC, created_at)
    WHERE status = 'pending';
