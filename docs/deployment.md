# Deployment Guide

## Architecture Overview

```
StatsCan API → Pipeline (Python) → PostgreSQL → Hugo → Cloudflare Pages
                    ↓                                       ↓
              Claude API                              Newsletter (Resend)
```

## Option 1: Local Development

```bash
# Start the database
docker compose up -d db

# Configure environment
cp .env.example .env
# Set DATABASE_URL=postgresql://postgres:postgres@localhost:5433/stats_bluenoser

# Run the pipeline
python -m pipeline.run_daily --force --no-analyze

# Preview the site
cd site && hugo server
```

## Option 2: Docker Compose (Recommended for Production)

```bash
# Configure environment
cp .env.example .env
# Edit .env with production values:
#   DATABASE_URL (auto-configured in docker-compose)
#   ANTHROPIC_API_KEY=sk-ant-...
#   RESEND_API_KEY=re_...
#   NEWSLETTER_RECIPIENTS=you@example.com
#   ALERT_EMAIL=you@example.com

# Build and run
docker compose up --build

# Or run in background
docker compose up -d
docker compose logs -f pipeline
```

## Option 3: VPS Deployment

### 1. Server Setup

Any Linux VPS with Docker support ($5-10/month):
- Railway, Fly.io, DigitalOcean, Linode, etc.

```bash
# Install Docker
curl -fsSL https://get.docker.com | sh

# Clone the repo
git clone git@github.com:MaddDennison/stats.bluenoser.ai.git
cd stats.bluenoser.ai

# Configure
cp .env.example .env
# Edit .env with production values
```

### 2. Cron Scheduling

```bash
crontab -e
# Add:
CRON_TZ=America/Toronto
31 8 * * 1-5 /path/to/stats.bluenoser.ai/scripts/cron_daily.sh >> /path/to/stats.bluenoser.ai/logs/pipeline.log 2>&1
```

StatsCan publishes data at 08:30 ET on business days. The pipeline runs at 08:31 ET.

### 3. Database

**Supabase (managed, recommended):**
- Create a project at supabase.com
- Copy the connection string to `.env`
- Run: `psql "$DATABASE_URL" -f db/schema.sql -f db/seed_sources.sql -f db/seed_topics.sql`

**Local Docker (development):**
- `docker compose up -d db` — auto-seeds on first run

**Migration:** `pg_dump` from Supabase → `pg_restore` to any PostgreSQL host → change `DATABASE_URL`. No vendor lock-in.

## Website Deployment (Cloudflare Pages)

1. Create a Cloudflare Pages project at dash.cloudflare.com
2. Connect to the GitHub repo
3. Build settings:
   - Build command: `hugo --source site`
   - Build output directory: `site/public`
   - Environment variable: `HUGO_VERSION` = `0.159.1`
4. Deploy on push to `main`

Or use the pipeline's auto-deploy: `publisher.deploy_site()` commits and pushes site changes.

## Newsletter (Resend)

1. Create a Resend account at resend.com (free tier: 100 emails/day)
2. Add your sending domain
3. Set `RESEND_API_KEY` in `.env`
4. Set `NEWSLETTER_RECIPIENTS` to a comma-separated list of email addresses
5. Set `NEWSLETTER_FROM` to your verified sender address

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Yes | PostgreSQL connection string |
| `ANTHROPIC_API_KEY` | For releases | Claude API key for AI generation |
| `RESEND_API_KEY` | For newsletter | Resend API key for email sending |
| `NEWSLETTER_RECIPIENTS` | For newsletter | Comma-separated recipient emails |
| `NEWSLETTER_FROM` | For newsletter | Sender address (must be verified in Resend) |
| `ALERT_EMAIL` | For alerts | Email for pipeline failure alerts |
| `LOG_LEVEL` | No | Logging level (default: INFO) |
| `LOG_FORMAT` | No | Set to "json" for structured logging |
| `STATCAN_RATE_LIMIT` | No | API requests per second (default: 20) |

## Monitoring

- **Logs**: `logs/pipeline.log` (JSON format, rotated at 10 MB, 5 backups)
- **Health summary**: Emitted as JSON at the end of every pipeline run
- **Failure alerts**: Email via Resend on total pipeline failure (exit code 2)
- **CI**: GitHub Actions runs tests and Hugo build on every push

## Backfill

After initial deployment, backfill historical data:

```bash
# All tables with configured vectors (20 years)
python -m scripts.backfill

# Specific table
python -m scripts.backfill 18100004 20

# Verify data quality
python -m scripts.verify_data
```
