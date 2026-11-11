#!/usr/bin/env python3
"""
live_scores.py — live scores in the app, about a minute behind the game.
Runs on GitHub Actions (.github/workflows/live_scores.yml), so it keeps going
when the Mac is asleep, off or out of battery.

Each run lasts up to ~45 minutes (a new run takes over every 30). Every
minute it asks Supabase's own schedule whether any game is on right now —
started in the last 6 hours and not over. Only then does it spend a Tank01
call: one scoreboard call covers all of a league's games that day. NBA and
NHL are checked every minute, the NFL every 2. When nothing is on and
nothing starts before the run would end, it stops early.

Each game's score, status and clock go into public.live_scores, keyed to the
same game_id as `schedules`; the app hears every change within seconds
through Supabase Realtime. Rows older than 2 days are cleared.

Needs nothing from the Mac: the team list and the call counter live in
Supabase too.

Budget. Tank01 Pro gives each league 1,000 calls a day. This script keeps its
own count in public.tank01_calls_cloud and stops at CLOUD_BUDGET (350) per
league per UTC day; the Mac's scripts stop at 600 (fetch_tank01.py), so
together they stay under 1,000. A heavy night uses ~300.

Environment (GitHub secrets):
  TANK01_KEY        the RapidAPI key
  SUPABASE_DB_URL   Postgres connection string

Usage:
  python live_scores.py                  # what the workflow runs
  python live_scores.py --minutes 2      # a short test run
  python live_scores.py --probe NBA      # one live game exactly as Tank01
                                         #   sends it, and how it was read
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import Json

HOSTS = {
    "NHL": "tank01-nhl-live-in-game-real-time-statistics-nhl.p.rapidapi.com",
    "NBA": "tank01-fantasy-stats.p.rapidapi.com",
    "NFL": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com",
    "MLB": "tank01-mlb-live-in-game-real-time-statistics.p.rapidapi.com",
}
LEAGUES = ["NBA", "NHL", "NFL"]          # add MLB on opening day 2027
EVERY_MINUTES = {"NBA": 1, "NHL": 1, "NFL": 2, "MLB": 2}
CLOUD_BUDGET = 350                        # calls per league per UTC day
ET = ZoneInfo("America/New_York")         # Tank01 dates games by the Eastern day
GAME_ID = re.compile(r"^\d{8}_[A-Z0-9]+@[A-Z0-9]+$")

# Tank01 codes that differ from ours (same table as build_tank01_teams.py).
ABBR = {
    "NHL": {"UTA": {"UTAH", "UHC"}, "LA": {"LAK"}, "NJ": {"NJD"}, "SJ": {"SJS"},
            "TB": {"TBL"}, "VGK": {"VEG"}, "WSH": {"WAS"}, "MTL": {"MON"}},
    "NBA": {"NY": {"NYK"}, "GS": {"GSW"}, "SA": {"SAS"}, "NO": {"NOP", "NOR"},
            "PHO": {"PHX"}, "WAS": {"WSH"}, "BKN": {"BRK"}, "CHA": {"CHO"},
            "UTA": {"UTAH"}},
    "NFL": {"WSH": {"WAS"}, "JAX": {"JAC"}, "LAR": {"LA"}, "LV": {"LVR", "OAK"},
            "ARI": {"ARZ"}},
    "MLB": {"CHW": {"CWS"}, "CWS": {"CHW"}, "WAS": {"WSH", "WSN"}, "WSH": {"WAS"},
            "KC": {"KCR"}, "SD": {"SDP"}, "SF": {"SFG"}, "TB": {"TBR"},
            "ARI": {"AZ"}, "AZ": {"ARI"}, "OAK": {"ATH", "LV"}, "ATH": {"OAK"}},
}


def log(msg):
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}Z] {msg}", flush=True)


# ── reading Tank01's scoreboard, loosely ──────────────────────────────

def first(d, *keys):
    for k in keys:
        v = d.get(k)
        if v not in (None, ""):
            return v
    return None


def to_int(v):
    try:
        return int(float(str(v).strip()))
    except (TypeError, ValueError):
        return None


def status_of(g):
    code = str(g.get("gameStatusCode", "")).strip()
    text = str(g.get("gameStatus", "")).lower()
    if code == "2" or "complete" in text or "final" in text:
        return "Final"
    if code == "1" or "progress" in text or "live" in text or "half" in text:
        return "InProgress"
    if "postpon" in text or "suspend" in text or "cancel" in text:
        return "Postponed"
    return "Scheduled"


ORD_TO_Q = {"1st": "Q1", "2nd": "Q2", "3rd": "Q3", "4th": "Q4"}
CLOCK = re.compile(r"^\s*(\d{1,2}:\d{2}(?:\.\d)?)\s*-?\s*(.*)$")


def _period(league, p):
    p = str(p).strip()
    if league in ("NBA", "NFL"):
        if p.isdigit():
            return "Q" + p if int(p) <= 4 else "OT"
        return ORD_TO_Q.get(p.lower(), p)
    if league == "NHL" and p.isdigit():
        return {1: "1st", 2: "2nd", 3: "3rd"}.get(int(p), "OT")
    return p


def detail_of(league, g, status):
    """'Q3 5:32', '2nd 12:10', 'Halftime', 'End of 2nd', 'Final/OT'…"""
    if status == "Final":
        txt = str(g.get("gameStatus", "")).upper().replace(" ", "")
        for tag in ("3OT", "2OT", "SO", "OT"):
            if tag in txt:
                return "Final/" + tag
        return "Final"
    if status != "InProgress":
        return ""
    clock = str(first(g, "gameClock", "currentGameClock", "clock") or "").strip()
    period = first(g, "currentPeriod", "period", "quarter", "gamePeriod")
    if not clock:
        text = str(g.get("gameStatus", ""))
        if "half" in text.lower():
            return "Halftime"
        return _period(league, period) if period else "Live"
    m = CLOCK.match(clock)
    if m:                                   # "10:30 - 4th" / "10:30 4th" → "Q4 10:30"
        t, rest = m.group(1), m.group(2).strip()
        p = _period(league, rest or period or "")
        return f"{p} {t}".strip()
    return clock                            # "Halftime", "End of 2nd"…


def scores_of(g):
    """(home, away). Tank01's field names differ by league, so: the usual
    names first, then any home*/away* field that looks like a score, then
    the line score's totals."""
    home = to_int(first(g, "homePts", "homeScore", "homeGoals", "homeTotal",
                        "homeRuns", "homeTeamScore"))
    away = to_int(first(g, "awayPts", "awayScore", "awayGoals", "awayTotal",
                        "awayRuns", "awayTeamScore"))
    if home is None or away is None:
        for k, v in g.items():
            kl = k.lower()
            if not any(w in kl for w in ("score", "pts", "goal", "total", "runs")):
                continue
            if kl.startswith("home") and home is None and to_int(v) is not None:
                home = to_int(v)
            elif kl.startswith("away") and away is None and to_int(v) is not None:
                away = to_int(v)
    ls = g.get("lineScore")
    if (home is None or away is None) and isinstance(ls, dict):
        def total(side):
            d = ls.get(side) or {}
            return to_int(first(d, "R", "totalPts", "totalGoals", "total", "score",
                                "goals", "T"))
        home = home if home is not None else total("home")
        away = away if away is not None else total("away")
    return home, away


def shootout_winner(league, g, status, home, away):
    """A hockey game can't end tied: a level final score means the shootout
    decided it, and the shootout goal isn't in either team's goals. Add it
    back for whoever won. Returns (home, away, decided_by_shootout)."""
    if league != "NHL" or status != "Final" or home is None or home != away:
        return home, away, False
    hr = str(g.get("homeResult", "")).upper()
    ar = str(g.get("awayResult", "")).upper()
    if hr == "W" or ar in ("L", "OTL", "SOL"):
        return home + 1, away, True
    if ar == "W" or hr in ("L", "OTL", "SOL"):
        return home, away + 1, True
    return home, away, False


def games_in(body):
    items = body.values() if isinstance(body, dict) else (body or [])
    return [g for g in items if isinstance(g, dict)
            and GAME_ID.match(str(g.get("gameID", "")))]


# ── Supabase ──────────────────────────────────────────────────────────

SETUP = """
CREATE TABLE IF NOT EXISTS public.live_scores (
    league        text NOT NULL,
    game_id       text NOT NULL,
    tank01_id     text,
    home_team_id  integer,
    away_team_id  integer,
    status        text,
    home_score    integer,
    away_score    integer,
    detail        text,
    raw           jsonb,
    final_at      timestamptz,
    updated_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (league, game_id)
);
CREATE TABLE IF NOT EXISTS public.tank01_calls_cloud (
    league  text NOT NULL,
    day     date NOT NULL,
    calls   integer NOT NULL DEFAULT 0,
    PRIMARY KEY (league, day)
);
ALTER TABLE public.live_scores ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.tank01_calls_cloud ENABLE ROW LEVEL SECURITY;   -- no app access
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'live_scores'
                 AND policyname = 'read live scores') THEN
    CREATE POLICY "read live scores" ON public.live_scores FOR SELECT USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_publication_tables WHERE pubname = 'supabase_realtime'
                 AND schemaname = 'public' AND tablename = 'live_scores') THEN
    ALTER PUBLICATION supabase_realtime ADD TABLE public.live_scores;
  END IF;
END $$;
REVOKE ALL ON public.live_scores, public.tank01_calls_cloud FROM anon, authenticated;
GRANT SELECT ON public.live_scores TO anon, authenticated;
"""


class Budget(Exception):
    pass


class Tank01:
    def __init__(self, cur, key):
        self.cur, self.key = cur, key

    def get(self, league, path, **params):
        self.cur.execute("""
            INSERT INTO public.tank01_calls_cloud (league, day, calls)
            VALUES (%s, (now() AT TIME ZONE 'utc')::date, 1)
            ON CONFLICT (league, day) DO UPDATE SET calls = tank01_calls_cloud.calls + 1
            RETURNING calls""", (league,))
        used = self.cur.fetchone()[0]
        self.cur.connection.commit()
        if used > CLOUD_BUDGET:
            raise Budget(f"{league} has used its {CLOUD_BUDGET} cloud calls today (UTC)")
        host = HOSTS[league]
        url = f"https://{host}{path}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers={
            "X-RapidAPI-Key": self.key, "X-RapidAPI-Host": host})
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            log(f"  {league}: HTTP {e.code}")
            if e.code == 429:
                raise Budget("RapidAPI returned 429")
        except Exception as e:
            log(f"  {league}: {e}")
        return None


def team_lookup(cur, league):
    cur.execute("SELECT id, abbreviation FROM public.teams WHERE league = %s", (league,))
    ours = {ab.upper(): tid for tid, ab in cur.fetchall() if ab}

    def lookup(t01):
        t01 = (t01 or "").upper()
        for cand in [t01, *sorted(ABBR.get(league, {}).get(t01, set()))]:
            if cand in ours:
                return ours[cand]
        return None
    return lookup


def games_on_now(cur, league):
    """schedules rows that could be live: started in the last 6 hours (or
    starting in the next 10 minutes) and not already final."""
    cur.execute("""
        SELECT s.game_id::text, s.home_team_id, s.away_team_id, s.game_time_utc::text
        FROM public.schedules s
        LEFT JOIN public.live_scores l
               ON l.league = s.league AND l.game_id = s.game_id::text
        WHERE s.league = %s
          AND nullif(s.game_time_utc::text, '') IS NOT NULL
          AND s.game_time_utc::text::timestamptz BETWEEN now() - interval '6 hours'
                                                     AND now() + interval '10 minutes'
          AND coalesce(s.game_status, '') <> 'Final'
          AND coalesce(l.status, '') <> 'Final'
    """, (league,))
    return cur.fetchall()


def anything_soon(cur, until):
    """Is any game on now, or starting before [until]?"""
    cur.execute("""
        SELECT count(*) FROM public.schedules s
        LEFT JOIN public.live_scores l
               ON l.league = s.league AND l.game_id = s.game_id::text
        WHERE s.league = ANY(%s)
          AND nullif(s.game_time_utc::text, '') IS NOT NULL
          AND s.game_time_utc::text::timestamptz BETWEEN now() - interval '6 hours'
                                                     AND %s
          AND coalesce(s.game_status, '') <> 'Final'
          AND coalesce(l.status, '') <> 'Final'
    """, (LEAGUES, until))
    return cur.fetchone()[0] > 0


def poll(cur, api, league, lookups):
    on = games_on_now(cur, league)
    if not on:
        return 0
    by_teams = {(h, a): gid for gid, h, a, _ in on}
    days = sorted({datetime.fromisoformat(t.replace("Z", "+00:00"))
                   .astimezone(ET).strftime("%Y%m%d") for _, _, _, t in on})
    lookup = lookups.setdefault(league, team_lookup(cur, league))
    n = 0
    for day in days:
        body = (api.get(league, f"/get{league}ScoresOnly", gameDate=day) or {}).get("body")
        for g in games_in(body):
            away_ab, home_ab = g["gameID"].split("_", 1)[1].split("@")
            hid, aid = lookup(home_ab), lookup(away_ab)
            gid = by_teams.get((hid, aid))
            if gid is None:
                continue
            st = status_of(g)
            hs, as_ = scores_of(g)
            hs, as_, so = shootout_winner(league, g, st, hs, as_)
            detail = "Final/SO" if so else detail_of(league, g, st)
            cur.execute("""
                INSERT INTO public.live_scores (league, game_id, tank01_id,
                    home_team_id, away_team_id, status, home_score, away_score,
                    detail, raw, final_at, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        CASE WHEN %s = 'Final' THEN now() END, now())
                ON CONFLICT (league, game_id) DO UPDATE SET
                    status = excluded.status,
                    home_score = coalesce(excluded.home_score, live_scores.home_score),
                    away_score = coalesce(excluded.away_score, live_scores.away_score),
                    detail = excluded.detail, raw = excluded.raw,
                    final_at = coalesce(live_scores.final_at, excluded.final_at),
                    updated_at = now()
            """, (league, gid, g["gameID"], hid, aid, st, hs, as_,
                  detail, Json(g), st))
            n += 1
    cur.connection.commit()
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=45,
                    help="how long this run keeps polling (default 45)")
    ap.add_argument("--probe", choices=list(HOSTS))
    a = ap.parse_args()

    key = os.environ.get("TANK01_KEY", "").strip()
    db = os.environ.get("SUPABASE_DB_URL", "").strip()
    if not key or not db:
        sys.exit("Set TANK01_KEY and SUPABASE_DB_URL.")
    pg = psycopg2.connect(db)
    cur = pg.cursor()
    cur.execute(SETUP)
    cur.execute("DELETE FROM public.live_scores WHERE updated_at < now() - interval '2 days'")
    pg.commit()
    api = Tank01(cur, key)

    if a.probe:
        day = datetime.now(ET).strftime("%Y%m%d")
        body = (api.get(a.probe, f"/get{a.probe}ScoresOnly", gameDate=day) or {}).get("body")
        gs = games_in(body)
        live = [g for g in gs if status_of(g) == "InProgress"] or gs
        if not live:
            print("No games on that scoreboard today.")
            return
        g = live[0]
        print(json.dumps(g, indent=2)[:4000])
        st = status_of(g)
        print("\nread as:", st, "home/away", scores_of(g), repr(detail_of(a.probe, g, st)))
        return

    end = datetime.now(timezone.utc) + timedelta(minutes=a.minutes)
    lookups = {}
    while datetime.now(timezone.utc) < end:
        if not anything_soon(cur, end):
            log("No games on, and none starting before this run ends — stopping.")
            break
        minute = datetime.now(timezone.utc).minute
        for lg in LEAGUES:
            if minute % EVERY_MINUTES[lg]:
                continue
            try:
                n = poll(cur, api, lg, lookups)
                if n:
                    log(f"  {lg}: {n} game(s) updated")
            except Budget as e:
                log(f"  {lg}: stopped — {e}")
            except Exception as e:                # one bad poll mustn't end the run
                pg.rollback()
                log(f"  {lg}: error — {e}")
        # Wake at the top of the next minute.
        time.sleep(max(5, 60 - datetime.now(timezone.utc).second))
    pg.close()


if __name__ == "__main__":
    main()
