"""
fetch_youtube_videos.py  (v3)
==============================
Fetches videos from YouTube channels using the uploads playlist API.

Two modes:
  --bulk    Fetches up to 200 videos per channel going back 6 months
             Run once to backfill historical data
  --nightly Fetches only videos from the last 2 days (default)
             Run each morning via cron

Quota usage (nightly):
  ~2 units per channel x 156 channels = ~312 units per run
  Hourly = ~7,500/day, inside the 10,000 free daily limit.
Quota usage (bulk):
  ~10 units per channel x 156 channels = ~1,560 units per run

The uploads-playlist lookup that used to cost a unit per channel is gone:
a channel's uploads playlist id is its channel id with UC swapped for UU.
If that ever fails for a channel we fall back to asking the API, so an
unusual channel still works, it just costs the extra unit.

Both the API key and the database URL come from the environment, so this
script runs unchanged on this Mac and on GitHub Actions:
    YOUTUBE_API_KEY   — YouTube Data API v3 key
    SUPABASE_DB_URL   — Postgres connection string (db_connect falls back
                        to the local one when it is not set)

Usage:
    cd ~/Desktop/SportsApp
    python3 fetch_youtube_videos.py           # nightly mode (last 2 days)
    python3 fetch_youtube_videos.py --bulk    # bulk mode (last 12 months)
"""

import os
import requests
import time
import re
import sys
import logging
from datetime import datetime, timezone, timedelta
from db_connect import get_conn

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
# Never hardcode this. Locally: export YOUTUBE_API_KEY="..." in your shell
# profile. On GitHub: a repository secret of the same name.
YOUTUBE_API_KEY  = os.environ.get("YOUTUBE_API_KEY", "")
MAX_DURATION_SEC = 600   # 10 minutes max video length
BASE_URL         = "https://www.googleapis.com/youtube/v3"

# Mode settings
BULK_MODE        = "--bulk" in sys.argv
MAX_VIDEOS       = 600 if BULK_MODE else 50
LOOKBACK_DAYS    = 365 if BULK_MODE else 2  # nightly window; safe to overlap
CUTOFF_DATE      = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def api_get(endpoint, params):
    params["key"] = YOUTUBE_API_KEY
    try:
        r = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=15)
        if r.status_code == 403:
            data = r.json()
            reason = data.get("error", {}).get("errors", [{}])[0].get("reason", "")
            if "quotaExceeded" in reason:
                log.error("  QUOTA EXCEEDED — stopping")
                return "QUOTA_EXCEEDED"
            log.warning(f"  403: {reason}")
            return None
        if r.status_code != 200:
            log.warning(f"  HTTP {r.status_code} for {endpoint}")
            return None
        return r.json()
    except Exception as e:
        log.warning(f"  Request error: {e}")
        return None


def iso_to_seconds(duration):
    if not duration:
        return None
    m = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration)
    if not m:
        return None
    return int(m.group(1) or 0)*3600 + int(m.group(2) or 0)*60 + int(m.group(3) or 0)


def parse_dt(dt_str):
    """Parse ISO datetime string to timezone-aware datetime."""
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except Exception:
        return None


def uploads_playlist_id(channel_id):
    """
    Free: every channel's uploads playlist is its channel id with the UC
    prefix replaced by UU. Saves one quota unit per channel per run.
    """
    if channel_id and channel_id.startswith("UC"):
        return "UU" + channel_id[2:]
    return None


def get_uploads_playlist_id(channel_id):
    """The paid fallback, for anything the rule above doesn't cover."""
    data = api_get("channels", {"id": channel_id, "part": "contentDetails"})
    if data == "QUOTA_EXCEEDED":
        return "QUOTA_EXCEEDED"
    if not data or not data.get("items"):
        return None
    return data["items"][0].get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")


def get_videos_from_playlist(playlist_id, max_videos, cutoff_date):
    """
    Fetches videos from a playlist with pagination.
    Stops when hitting videos older than cutoff_date or max_videos reached.
    Returns list of video dicts.
    """
    videos = []
    next_page_token = None
    hit_cutoff = False

    while len(videos) < max_videos and not hit_cutoff:
        params = {
            "playlistId": playlist_id,
            "part":       "snippet",
            "maxResults": min(50, max_videos - len(videos)),
        }
        if next_page_token:
            params["pageToken"] = next_page_token

        data = api_get("playlistItems", params)
        if data == "QUOTA_EXCEEDED":
            return "QUOTA_EXCEEDED"
        if not data:
            break

        for item in data.get("items", []):
            snippet     = item.get("snippet", {})
            vid_id      = snippet.get("resourceId", {}).get("videoId")
            published   = snippet.get("publishedAt")
            pub_dt      = parse_dt(published)

            if not vid_id:
                continue

            # Stop if video is older than cutoff
            if pub_dt and pub_dt < cutoff_date:
                hit_cutoff = True
                break

            thumbs = snippet.get("thumbnails", {})
            thumb  = (thumbs.get("maxres") or thumbs.get("high") or
                      thumbs.get("medium") or thumbs.get("default") or {})

            videos.append({
                "video_id":     vid_id,
                "title":        snippet.get("title", ""),
                "published_at": published,
                "thumbnail_url":thumb.get("url"),
            })

        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break

        time.sleep(0.1)

    return videos


def get_video_details(video_ids):
    """Fetch duration and view count for a list of video IDs (50 at a time)."""
    if not video_ids:
        return {}
    results = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        data = api_get("videos", {"id": ",".join(batch), "part": "contentDetails,statistics"})
        if not data or data == "QUOTA_EXCEEDED":
            continue
        for item in data.get("items", []):
            results[item["id"]] = {
                "duration_seconds": iso_to_seconds(item.get("contentDetails", {}).get("duration")),
                "view_count": int(item.get("statistics", {}).get("viewCount", 0) or 0),
            }
    return results


def build_player_index(conn):
    c = conn.cursor()
    c.execute("""
        SELECT id, full_name, search_name, league, team_abbreviation
        FROM players
        WHERE status IN ('Active', 'Prospect')
        AND search_name IS NOT NULL
        AND LENGTH(search_name) >= 5
    """)
    index = {}
    for row in c.fetchall():
        player_id, full_name, search_name, league, team_abbr = row
        index[search_name.lower().strip()] = {
            "player_id": player_id,
            "full_name": full_name,
            "league":    league,
            "team_abbr": team_abbr,
        }
    log.info(f"  Players indexed: {len(index)}")
    return index


def match_players(title, player_index, sport):
    title_lower = title.lower()
    sport_leagues = {
        "MLB": ["MLB"], "NHL": ["NHL"],
        "NBA": ["NBA"], "NFL": ["NFL"],
        "ALL": ["MLB", "NHL", "NBA", "NFL"],
    }
    valid = sport_leagues.get(sport, [])
    matches = []
    for name, player in player_index.items():
        if valid and player["league"] not in valid:
            continue
        if name in title_lower:
            matches.append((player["player_id"], player["full_name"]))
    return matches


def upsert_video(c, player_id, video, channel, confidence):
    try:
        c.execute("""
            INSERT INTO player_videos
            (player_id, video_id, title, thumbnail_url, channel_id,
             channel_name, account_type, sport, published_at,
             duration_seconds, view_count, confidence, last_updated)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (player_id, video_id) DO UPDATE SET
                title=EXCLUDED.title, thumbnail_url=EXCLUDED.thumbnail_url,
                view_count=EXCLUDED.view_count, confidence=EXCLUDED.confidence,
                last_updated=NOW()
        """, (
            player_id, video["video_id"], video["title"],
            video.get("thumbnail_url"), channel["channel_id"], channel["name"],
            channel["account_type"], channel["sport"], video.get("published_at"),
            video.get("duration_seconds"), video.get("view_count"), confidence,
        ))
        return c.rowcount
    except Exception as e:
        log.warning(f"  Upsert error: {e}")
        return 0


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    if not YOUTUBE_API_KEY:
        print("Set the YOUTUBE_API_KEY environment variable before running.")
        sys.exit(1)

    start = datetime.now()
    mode  = "BULK (6 months)" if BULK_MODE else "NIGHTLY (last 2 days)"

    log.info("=" * 55)
    log.info(f"  YouTube Video Fetch v3 — {start.strftime('%Y-%m-%d %H:%M')}")
    log.info(f"  Mode: {mode}")
    log.info(f"  Cutoff: {CUTOFF_DATE.strftime('%Y-%m-%d')}")
    log.info("=" * 55)

    # Everything comes from Supabase now — no file on any particular Mac,
    # which is what lets this run on GitHub Actions.
    conn = get_conn()
    c    = conn.cursor()

    c.execute("""
        SELECT name, channel_id, sport, account_type
        FROM public.youtube_accounts
        WHERE active = true AND channel_id IS NOT NULL
        ORDER BY sport, account_type
    """)
    channels = c.fetchall()
    log.info(f"  Channels to process: {len(channels)}")

    player_index = build_player_index(conn)

    total_videos = total_matched = total_saved = quota_used = 0
    quota_exceeded = False
    processed = 0

    for i, ch in enumerate(channels):
        if quota_exceeded:
            break

        name, channel_id, sport, account_type = ch
        confidence = "high" if account_type in ("team", "league") else "medium"

        # Step 1: Uploads playlist ID — derived for free, asked for only
        # if the derivation doesn't hold or the playlist comes back empty.
        playlist_id = uploads_playlist_id(channel_id)

        # Step 2: Fetch videos with pagination up to MAX_VIDEOS
        videos = (get_videos_from_playlist(playlist_id, MAX_VIDEOS, CUTOFF_DATE)
                  if playlist_id else None)
        if not videos:
            playlist_id = get_uploads_playlist_id(channel_id)
            quota_used += 1
            if playlist_id == "QUOTA_EXCEEDED":
                quota_exceeded = True
                break
            if playlist_id:
                videos = get_videos_from_playlist(
                    playlist_id, MAX_VIDEOS, CUTOFF_DATE)
        quota_used += max(1, len(videos) // 50) if videos and videos != "QUOTA_EXCEEDED" else 1
        if videos == "QUOTA_EXCEEDED":
            quota_exceeded = True
            break
        if not videos:
            continue

        # Step 3: Get durations and view counts
        details = get_video_details([v["video_id"] for v in videos])
        quota_used += max(1, len(videos) // 50)

        # Filter by duration
        filtered = []
        for v in videos:
            d = details.get(v["video_id"], {})
            v["duration_seconds"] = d.get("duration_seconds")
            v["view_count"]       = d.get("view_count", 0)
            dur = v["duration_seconds"]
            if dur is None or dur <= MAX_DURATION_SEC:
                filtered.append(v)

        total_videos += len(filtered)
        channel_matches = 0

        # Step 4: Match players and save
        for video in filtered:
            for player_id, player_name in match_players(video["title"], player_index, sport):
                saved = upsert_video(c, player_id, video, {
                    "channel_id":   channel_id,
                    "name":         name,
                    "account_type": account_type,
                    "sport":        sport,
                }, confidence)
                total_saved   += saved
                channel_matches += 1
                total_matched += 1

        log.info(f"  [{i+1}/{len(channels)}] {name}: {len(filtered)} videos, {channel_matches} matches")

        conn.commit()
        processed = i + 1
        time.sleep(0.2)

    conn.close()
    elapsed = (datetime.now() - start).seconds
    log.info("")
    log.info("=" * 55)
    log.info("  COMPLETE — %dm %ds" % (elapsed // 60, elapsed % 60))
    log.info("=" * 55)
    log.info(f"  Channels processed: {processed}/{len(channels)}")
    log.info(f"  Videos fetched:     {total_videos}")
    log.info(f"  Player matches:     {total_matched}")
    log.info(f"  DB rows saved:      {total_saved}")
    log.info(f"  Est. quota used:    ~{quota_used} units")
    if quota_exceeded:
        log.warning("  ⚠ Quota exceeded mid-run")


if __name__ == "__main__":
    main()
