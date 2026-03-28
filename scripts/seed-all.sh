#!/bin/bash
# Run all seed scripts inside the API container.
# Usage: docker exec streamdb-api bash /app/scripts/seed-all.sh
set -e

export DATABASE_URL="${DATABASE_URL:-postgres://streamdb:streamdb_secret@postgres:5432/streamdb}"
export TMDB_API_KEY="${TMDB_API_KEY:-}"

cd /app

echo "==========================================="
echo "StreamDB Full Seed Pipeline"
echo "==========================================="

# Step 1: Seed content from TMDB export (popular movies + shows)
echo ""
echo "[1/3] Seeding IMDB content from TMDB daily export..."
npx tsx scripts/seed-imdb-from-tmdb-export.ts --limit 100000 2>&1 || echo "TMDB export seed completed (or skipped)"

# Step 2: Seed hashes from DMM GitHub
echo ""
echo "[2/3] Seeding hashes from DebridMediaManager GitHub..."
npx tsx scripts/seed-from-dmm-github.ts 2>&1 || echo "DMM seed completed (or skipped)"

# Step 3: Seed hashes from Torrentio for popular titles
echo ""
echo "[3/3] Seeding hashes from Torrentio API..."
npx tsx scripts/seed-from-torrentio.ts --pages 20 2>&1 || echo "Torrentio seed completed (or skipped)"

echo ""
echo "==========================================="
echo "Seed pipeline complete. Checking stats..."
echo "==========================================="

# Print final stats
node -e "
const pg = require('pg');
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
pool.query(\`
  SELECT
    (SELECT COUNT(*) FROM content) as content,
    (SELECT COUNT(*) FROM files) as files,
    (SELECT COUNT(*) FROM content_files) as mappings,
    (SELECT COUNT(*) FROM probe_jobs WHERE status = 'pending') as pending_probes
\`).then(r => {
  console.log('DB Stats:', r.rows[0]);
  pool.end();
}).catch(e => { console.error(e.message); pool.end(); });
"
