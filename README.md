# sportsapp-sync

Scheduled jobs for The 300 Level that need no local machine.

- `fetch_youtube_videos.py` — pulls new videos from the curated YouTube
  channels in Supabase `youtube_accounts`, matches them to players, and
  upserts into `player_videos`. Runs hourly; ~312 YouTube quota units a
  pass, against a 10,000/day budget.

## Secrets

| Name | What it is |
| --- | --- |
| `YOUTUBE_API_KEY` | YouTube Data API v3 key, restricted to that API |
| `SUPABASE_DB_URL` | `postgresql://…` pooler connection string |

## Running by hand

Actions tab → YouTube feed sync → Run workflow. The `bulk` option widens
the lookback from 2 days to 12 months for a backfill.
