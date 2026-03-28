# StreamDB

Universal media intelligence layer for Stremio. Crawls the torrent ecosystem, probes files, maps them to content via an identity graph, and serves the perfect personalized stream per user.

## Architecture

- **Identity Graph** — PostgreSQL with content nodes (IMDB/TMDB/TVDB) linked to file nodes (infohash + full metadata)
- **Probe Pipeline** — FFprobe/Chromaprint/pHash extraction from partial torrent downloads
- **Smart Serving** — Stremio addon with per-user scoring based on resolution, HDR, audio, subtitle preferences
- **Debrid Integration** — RealDebrid + TorBox cache checking and CDN link generation

## Quick Start

```bash
cp .env.example .env
docker compose up -d
```

The addon will be available at `http://localhost:3000` (or your configured `BASE_URL`).

## Configure

Visit the root URL to configure your preferences and generate a Stremio addon URL.

## Data Import

### RARBG Dump

Download `rarbg_db.sqlite` from Internet Archive, then:

```bash
cd scripts && npm install
DATABASE_URL=postgres://streamdb:streamdb_secret@localhost:5432/streamdb npx tsx import-rarbg.ts ./rarbg_db.sqlite
```

## Stack

| Component | Technology |
|-----------|-----------|
| API | Node.js + TypeScript + Express |
| Database | PostgreSQL 16 |
| Deployment | Docker Compose + Traefik |
| Probe Pipeline | FFprobe + Chromaprint (Phase 3) |
| DHT Crawler | Bitmagnet (Phase 4) |
