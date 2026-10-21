"""
refresh_schedules.py - Game Schedule Sync
NFL uses ESPN site scoreboard date-range (all inline).
NHL uses NHL API per-team, deduplicated.
NBA uses ESPN scoreboard by date.
MLB uses MLB Stats API monthly chunks.

Writes to Supabase, not to sports.db. The SQL is unchanged: it goes
through pg_compat, which translates ? and :named placeholders. That is
also what lets this run on GitHub Actions, where no sports.db exists.

    --mode daily   a narrow window around today, for scores
    --mode full    whole configured seasons, for the slate
"""
import requests, json, time, logging, argparse
from datetime import datetime, date, timedelta
from pg_compat import connect as pg_connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH     = None   # kept for the --db flag's signature; unused
HEADERS     = {"User-Agent": "Mozilla/5.0 (compatible; SportsApp/1.0)"}
NHL_SEASONS = ["20252026", "20262027"]
NBA_SEASONS = [2027]   # nba_api source; 2025-26 stays as historical ESPN rows
NFL_SEASONS = [2025, 2026]
MLB_SEASONS = [2026, 2027]
NBA_SEASON_DATES = {
    2026: ("2025-10-22", "2026-06-30"),
    2027: ("2026-10-01", "2027-06-30"),
}
MLB_SEASON_DATES = {
    2026: ("2026-03-26", "2026-10-01"),
}
NFL_SEASON_WINDOWS = {
    2025: ("20250904", "20260301"),
    2026: ("20260903", "20270301"),
}
ESPN_NBA_ABBR = {"UTA": "UTAH", "NOP": "NO"}

# nba_api ScheduleLeagueV2 tricodes -> our teams.abbreviation (only the 6 that differ)
NBA_TRICODE_TO_DB = {
    "NOP": "NO", "GSW": "GS", "NYK": "NY",
    "SAS": "SA", "UTA": "UTAH", "WAS": "WSH",
}

def _nba_date(v):
    """Normalize an nba_api date field to YYYY-MM-DD (handles ISO and MM/DD/YYYY)."""
    v = str(v or "")
    if "T" in v:
        return v[:10]
    if "/" in v:
        p = v.split()[0].split("/")
        if len(p) == 3:
            try:
                return "%04d-%02d-%02d" % (int(p[2]), int(p[0]), int(p[1]))
            except Exception:
                pass
    return v[:10]

def get_json(url, pause=0.15, retries=2):
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 200:
                time.sleep(pause); return r.json()
            elif r.status_code == 404: return None
            elif r.status_code == 429: log.warning("Rate limited"); time.sleep(10)
            else: log.debug("HTTP %d: %s" % (r.status_code, url[:60]))
        except Exception as e:
            if attempt == retries: log.warning("Fetch error: %s" % e)
            time.sleep(1)
    return None

def ensure_schedules_table(conn):
    """
    The table already exists in Supabase, with the unique constraint the
    upsert needs. Rather than create it, check it is really there — a
    missing constraint would make every upsert fail one row at a time,
    which is a confusing way to find out.
    """
    row = conn.execute("""
        SELECT COUNT(*) FROM pg_constraint
        WHERE conrelid = 'public.schedules'::regclass
          AND contype = 'u'
    """).fetchone()
    if not row or not row[0]:
        raise RuntimeError(
            "schedules has no unique constraint on (league, game_id); "
            "the ON CONFLICT upsert cannot work without it")
    log.info("  schedules table ready")

def upsert_game(conn, row):
    conn.execute("""INSERT INTO schedules (
        league,season,game_id,game_date,game_time_utc,game_type,week,
        home_team_id,home_team_abbr,away_team_id,away_team_abbr,
        home_score,away_score,game_status,status_detail,
        venue_name,venue_city,venue_state,neutral_site,
        broadcasts,data_source,last_updated
    ) VALUES (
        :league,:season,:game_id,:game_date,:game_time_utc,:game_type,:week,
        :home_team_id,:home_team_abbr,:away_team_id,:away_team_abbr,
        :home_score,:away_score,:game_status,:status_detail,
        :venue_name,:venue_city,:venue_state,:neutral_site,
        :broadcasts,:data_source,:last_updated
    ) ON CONFLICT(league,game_id) DO UPDATE SET
        game_date=excluded.game_date, game_time_utc=excluded.game_time_utc,
        home_team_id=excluded.home_team_id, home_team_abbr=excluded.home_team_abbr,
        away_team_id=excluded.away_team_id, away_team_abbr=excluded.away_team_abbr,
        home_score=excluded.home_score, away_score=excluded.away_score,
        game_status=excluded.game_status, status_detail=excluded.status_detail,
        venue_name=COALESCE(excluded.venue_name,schedules.venue_name),
        venue_city=COALESCE(excluded.venue_city,schedules.venue_city),
        venue_state=COALESCE(excluded.venue_state,schedules.venue_state),
        neutral_site=excluded.neutral_site,
        broadcasts=COALESCE(excluded.broadcasts,schedules.broadcasts),
        last_updated=excluded.last_updated""", row)

def resolve_team(conn, league, abbr=None, native_id=None):
    if native_id:
        row = conn.execute(
            "SELECT id,abbreviation FROM teams WHERE league=? AND espn_id=?",
            (league, str(native_id))).fetchone()
        if row: return row
    if abbr:
        row = conn.execute(
            "SELECT id,abbreviation FROM teams WHERE league=? AND abbreviation=?",
            (league, abbr)).fetchone()
        if row: return row
    return (None, abbr)

def sync_nhl_schedule(conn, seasons=None, mode="full"):
    # gameState: OFF/FINAL=done, LIVE/CRIT=live, PRE/FUT=upcoming
    if seasons is None: seasons = NHL_SEASONS
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S"); saved = 0
    teams = [r[0] for r in conn.execute(
        "SELECT abbreviation FROM teams WHERE league='NHL' ORDER BY abbreviation")]
    if not teams: log.warning("  [NHL] No teams"); return 0
    gtype_map = {1:"PR", 2:"R", 3:"P"}
    for season_str in seasons:
        sl = "%s-%s" % (season_str[:4], season_str[6:])
        log.info("  [NHL] Loading %s (%d teams)..." % (sl, len(teams)))
        test = get_json("https://api-web.nhle.com/v1/club-schedule-season/%s/%s" % (teams[0], season_str))
        if not test or not test.get("games"):
            log.info("  [NHL] %s not available" % sl); continue
        seen, n_saved = set(), 0
        for abbr in teams:
            data = get_json("https://api-web.nhle.com/v1/club-schedule-season/%s/%s" % (abbr, season_str), pause=0.1)
            if not data: continue
            for g in data.get("games", []):
                gid = str(g.get("id", ""))
                if not gid or gid in seen: continue
                seen.add(gid)
                gt = gtype_map.get(g.get("gameType"), "R")
                if gt == "PR": continue
                away = g.get("awayTeam", {}); home = g.get("homeTeam", {})
                aid, aa = resolve_team(conn, "NHL", abbr=away.get("abbrev"))
                hid, ha = resolve_team(conn, "NHL", abbr=home.get("abbrev"))
                state = g.get("gameState", ""); outcome = g.get("gameOutcome") or {}
                pt = outcome.get("lastPeriodType", "")
                if state in ("FINAL", "OFF"):
                    status = "Final"; detail = ("Final/%s" % pt) if pt not in ("REG", "") else "Final"
                elif state in ("LIVE", "CRIT"): status, detail = "InProgress", "In Progress"
                elif state == "PRE": status, detail = "Scheduled", "Pregame"
                elif home.get("score") is not None and away.get("score") is not None:
                    status, detail = "Final", "Final"
                else: status, detail = "Scheduled", "Scheduled"
                venue = g.get("venue", {}); nets = []
                for b in sorted(g.get("tvBroadcasts", []), key=lambda x: x.get("sequenceNumber", 999)):
                    n = b.get("network", "")
                    if n and n not in nets: nets.append(n)
                upsert_game(conn, {
                    "league":"NHL", "season":sl, "game_id":gid,
                    "game_date":g.get("gameDate","")[:10], "game_time_utc":g.get("startTimeUTC"),
                    "game_type":gt, "week":None,
                    "home_team_id":hid, "home_team_abbr":ha,
                    "away_team_id":aid, "away_team_abbr":aa,
                    "home_score":home.get("score"), "away_score":away.get("score"),
                    "game_status":status, "status_detail":detail,
                    "venue_name":venue.get("default") if isinstance(venue, dict) else None,
                    "venue_city":None, "venue_state":None,
                    "neutral_site":1 if g.get("neutralSite") else 0,
                    "broadcasts":json.dumps(nets) if nets else None,
                    "data_source":"NHL_API", "last_updated":now}); n_saved += 1
        conn.commit(); log.info("  [NHL] %s: %d games" % (sl, n_saved)); saved += n_saved
    return saved

def sync_nba_schedule(conn, seasons=None, mode="full"):
    # ESPN's NBA scoreboard is IP-blocked (403), so pull the full-season schedule
    # from nba_api's ScheduleLeagueV2 (stats.nba.com), which this machine can reach.
    # One call returns the whole season (past + upcoming); we upsert so nightly
    # runs refresh scores. gameId[2]: 1=pre 2=reg 3=allstar 4=playoffs 5=play-in.
    if seasons is None:
        seasons = NBA_SEASONS
    try:
        from nba_api.stats.endpoints import scheduleleaguev2
    except ImportError:
        log.warning("  [NBA] nba_api not installed - skipping"); return 0

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S"); saved = 0
    for season_year in seasons:
        sl = "%d-%s" % (season_year - 1, str(season_year)[2:])   # 2027 -> "2026-27"
        log.info("  [NBA] Loading %s via nba_api..." % sl)
        try:
            df = scheduleleaguev2.ScheduleLeagueV2(
                league_id="00", season=sl, timeout=45).get_data_frames()[0]
        except Exception as e:
            log.warning("  [NBA] %s fetch failed: %s" % (sl, e)); continue

        n_saved = 0
        for _, r in df.iterrows():
            gid_raw = str(r.get("gameId") or "").strip()
            if len(gid_raw) < 3:
                continue
            kind = gid_raw[2]
            if kind == "2":
                gtype = "R"
            elif kind in ("4", "5"):
                gtype = "P"
            else:
                continue   # skip preseason / all-star / specials

            def tri(side):
                t = str(r.get("%s_teamTricode" % side) or "").strip()
                return NBA_TRICODE_TO_DB.get(t, t)
            hid, ha = resolve_team(conn, "NBA", abbr=tri("homeTeam"))
            aid, aa = resolve_team(conn, "NBA", abbr=tri("awayTeam"))

            code = r.get("gameStatus")
            try: code = int(code)
            except Exception: code = 1
            if code == 3:
                status, detail = "Final", str(r.get("gameStatusText") or "Final").strip()
            elif code == 2:
                status, detail = "InProgress", str(r.get("gameStatusText") or "").strip()
            else:
                status, detail = "Scheduled", str(r.get("gameStatusText") or "Scheduled").strip()

            def score(side):
                v = r.get("%s_score" % side)
                if code in (2, 3) and v is not None:
                    try: return int(v)
                    except Exception: return None
                return None

            wk = None
            wkv = str(r.get("weekNumber", "")).strip()
            if wkv and wkv.lower() not in ("nan", "none", ""):
                try: wk = int(float(wkv))
                except Exception: wk = None

            nets = []
            for i in range(3):
                nb = r.get("nationalBroadcasters_%d_broadcasterDisplay" % i)
                nb = str(nb).strip() if nb is not None else ""
                if nb and nb.lower() != "nan" and nb not in nets:
                    nets.append(nb)

            upsert_game(conn, {
                "league":"NBA", "season":sl, "game_id":"NBA_%s" % gid_raw,
                "game_date":_nba_date(r.get("gameDateEst") or r.get("gameDate")),
                "game_time_utc": r.get("gameDateTimeUTC") or r.get("gameDateUTC"),
                "game_type":gtype, "week":wk,
                "home_team_id":hid, "home_team_abbr":ha,
                "away_team_id":aid, "away_team_abbr":aa,
                "home_score":score("homeTeam"), "away_score":score("awayTeam"),
                "game_status":status, "status_detail":detail,
                "venue_name": r.get("arenaName"),
                "venue_city": r.get("arenaCity"), "venue_state": r.get("arenaState"),
                "neutral_site": 1 if r.get("isNeutral") else 0,
                "broadcasts": json.dumps(nets) if nets else None,
                "data_source":"NBA_API", "last_updated":now}); n_saved += 1
        conn.commit(); log.info("  [NBA] %s: %d games" % (sl, n_saved)); saved += n_saved
    return saved

def sync_nfl_schedule(conn, seasons=None, mode="full"):
    """
    ESPN site scoreboard, one day at a time.

    It used to take dates=YYYYMMDD-YYYYMMDD, and no longer does — a range
    now returns HTTP 400, which the old code logged as "No data". Hence
    day-by-day: a handful of requests in daily mode, a season's worth on
    the nightly full pass. Days with no games return an empty events list,
    which costs one cheap request.
    """
    if seasons is None: seasons = NFL_SEASONS
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S"); saved = 0
    abbr_map = {r[1]: (r[0], r[1]) for r in conn.execute(
        "SELECT id,abbreviation FROM teams WHERE league='NFL'")}

    for season_year in seasons:
        if season_year not in NFL_SEASON_WINDOWS: continue
        sl = str(season_year); ss, es = NFL_SEASON_WINDOWS[season_year]
        if mode == "daily":
            # Just enough to catch yesterday's finals and the next few days.
            today = date.today()
            d0 = today - timedelta(days=2)
            d1 = today + timedelta(days=8)
        else:
            d0 = datetime.strptime(ss, "%Y%m%d").date()
            d1 = datetime.strptime(es, "%Y%m%d").date()
            # The window runs into next spring; nothing past today's slate
            # exists for a season that hasn't started yet, but asking is
            # harmless and keeps the logic simple.
        log.info("  [NFL] Loading season %s (%s to %s)..." % (sl, d0, d1))

        n_saved = 0
        cur_day = d0
        while cur_day <= d1:
            data = get_json(
                "https://site.api.espn.com/apis/site/v2/sports/football/nfl"
                "/scoreboard?dates=%s" % cur_day.strftime("%Y%m%d"), pause=0.1)
            cur_day += timedelta(days=1)
            if not data: continue
            for ev in data.get("events", []):
                # A season's window overlaps the next one's start; trust the
                # event's own season rather than the loop we are in.
                ev_year = str((ev.get("season", {}) or {}).get("year", sl))
                if ev_year != sl: continue
                gid = "NFL_%s" % ev.get("id", ""); comp = ev.get("competitions", [{}])[0]
                wk = ev.get("week", {}).get("number")
                hid = aid = ha = aa = hs = as_ = None
                for c in comp.get("competitors", []):
                    t = c.get("team", {}); abbr = t.get("abbreviation", "")
                    sc = c.get("score")
                    try: sc = int(sc) if sc is not None else None
                    except: sc = None
                    did, da = abbr_map.get(abbr, (None, abbr))
                    if c.get("homeAway") == "home": hid, ha, hs = did, da, sc
                    else: aid, aa, as_ = did, da, sc
                venue = comp.get("venue", {}); nets = []
                for b in comp.get("broadcasts", []):
                    for n in b.get("names", []):
                        if n not in nets: nets.append(n)
                st = ev.get("status", {}).get("type", {})
                if st.get("completed"): status, detail = "Final", st.get("shortDetail", "Final")
                elif st.get("state") == "in": status, detail = "InProgress", st.get("shortDetail", "")
                else: status, detail = "Scheduled", st.get("shortDetail", "Scheduled")
                stype = ev.get("season", {}).get("type", 2)
                upsert_game(conn, {
                    "league":"NFL", "season":sl, "game_id":gid,
                    "game_date":ev.get("date", "")[:10], "game_time_utc":ev.get("date"),
                    "game_type":"P" if stype == 3 else "R", "week":wk,
                    "home_team_id":hid, "home_team_abbr":ha,
                    "away_team_id":aid, "away_team_abbr":aa,
                    "home_score":hs, "away_score":as_,
                    "game_status":status, "status_detail":detail,
                    "venue_name":venue.get("fullName") if isinstance(venue, dict) else None,
                    "venue_city":venue.get("address", {}).get("city") if isinstance(venue, dict) else None,
                    "venue_state":venue.get("address", {}).get("state") if isinstance(venue, dict) else None,
                    "neutral_site":1 if comp.get("neutralSite") else 0,
                    "broadcasts":json.dumps(nets) if nets else None,
                    "data_source":"ESPN", "last_updated":now}); n_saved += 1
            conn.commit()
        log.info("  [NFL] %s: %d games" % (sl, n_saved)); saved += n_saved
    return saved


def sync_mlb_schedule(conn, seasons=None, mode="full"):
    if seasons is None: seasons = MLB_SEASONS
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S"); saved = 0
    for season_year in seasons:
        if season_year not in MLB_SEASON_DATES: continue
        sl = str(season_year); ss, es = MLB_SEASON_DATES[season_year]
        log.info("  [MLB] Loading season %s..." % sl)
        if mode == "daily":
            today = datetime.today(); sdt = today - timedelta(days=3); edt = today + timedelta(days=14)
        else:
            sdt = datetime.strptime(ss, "%Y-%m-%d"); edt = datetime.strptime(es, "%Y-%m-%d")
        n_saved = 0; cur = sdt
        while cur <= edt:
            ce = min(cur + timedelta(days=29), edt)
            data = get_json(
                "https://statsapi.mlb.com/api/v1/schedule"
                "?sportId=1&season=%d&gameType=R,F,D,L,W,C,S"
                "&startDate=%s&endDate=%s&hydrate=team,venue,broadcasts(all)"
                % (season_year, cur.strftime("%Y-%m-%d"), ce.strftime("%Y-%m-%d")), pause=0.2)
            if data:
                for de in data.get("dates", []):
                    for g in de.get("games", []):
                        gid = "MLB_%s" % g.get("gamePk", ""); teams = g.get("teams", {})
                        at = teams.get("away", {}).get("team", {}); ht = teams.get("home", {}).get("team", {})
                        aid, aa = resolve_team(conn, "MLB", abbr=at.get("abbreviation", ""))
                        hid, ha = resolve_team(conn, "MLB", abbr=ht.get("abbreviation", ""))
                        nets = [b.get("name","") for b in g.get("broadcasts",[]) if b.get("name")]
                        so = g.get("status", {}); ab = so.get("abstractGameState",""); det = so.get("detailedState","")
                        if ab == "Final": status, detail = "Final", det
                        elif ab == "Live": status, detail = "InProgress", det
                        elif "Postponed" in det or "Suspended" in det: status, detail = "Postponed", det
                        else: status, detail = "Scheduled", det
                        upsert_game(conn, {
                            "league":"MLB", "season":sl, "game_id":gid,
                            "game_date":g.get("officialDate", g.get("gameDate","")[:10]),
                            "game_time_utc":g.get("gameDate"), "game_type":g.get("gameType","R"), "week":None,
                            "home_team_id":hid, "home_team_abbr":ha,
                            "away_team_id":aid, "away_team_abbr":aa,
                            "home_score":teams.get("home",{}).get("score"),
                            "away_score":teams.get("away",{}).get("score"),
                            "game_status":status, "status_detail":detail,
                            "venue_name":g.get("venue",{}).get("name"),
                            "venue_city":None, "venue_state":None, "neutral_site":0,
                            "broadcasts":json.dumps(nets) if nets else None,
                            "data_source":"MLB_API", "last_updated":now}); n_saved += 1
            cur = ce + timedelta(days=1)
        conn.commit(); log.info("  [MLB] %s: %d games" % (sl, n_saved)); saved += n_saved
    return saved

def _season_label_from_nhl(v):
    """20262027 -> 2026-27."""
    v = str(v or "")
    return "%s-%s" % (v[:4], v[6:]) if len(v) == 8 else ""


def sync_nhl_daily(conn, days_back=1, days_fwd=1):
    """
    Scores path: NHL's per-date scoreboard, a handful of calls instead of
    one per team per season. Used for --mode daily, which runs often; the
    full per-team pass stays for --mode full, which fills in the slate,
    venues and broadcasts.
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    gtype_map = {1: "PR", 2: "R", 3: "P"}
    saved = 0
    for offset in range(-days_back, days_fwd + 1):
        d = (date.today() + timedelta(days=offset)).strftime("%Y-%m-%d")
        data = get_json("https://api-web.nhle.com/v1/score/%s" % d, pause=0.1)
        if not data:
            continue
        for g in data.get("games", []):
            gid = str(g.get("id", ""))
            if not gid:
                continue
            gt = gtype_map.get(g.get("gameType"), "R")
            if gt == "PR":
                continue
            home = g.get("homeTeam", {}) or {}
            away = g.get("awayTeam", {}) or {}
            hid, ha = resolve_team(conn, "NHL", abbr=home.get("abbrev"))
            aid, aa = resolve_team(conn, "NHL", abbr=away.get("abbrev"))
            state = g.get("gameState", "")
            outcome = g.get("gameOutcome") or {}
            pt = outcome.get("lastPeriodType", "")
            if state in ("FINAL", "OFF"):
                status = "Final"
                detail = ("Final/%s" % pt) if pt not in ("REG", "") else "Final"
            elif state in ("LIVE", "CRIT"):
                status, detail = "InProgress", "In Progress"
            elif state == "PRE":
                status, detail = "Scheduled", "Pregame"
            else:
                status, detail = "Scheduled", "Scheduled"
            # venue / broadcasts are left None on purpose — the upsert
            # COALESCEs them, so whatever the full pass stored survives.
            upsert_game(conn, {
                "league": "NHL",
                "season": _season_label_from_nhl(g.get("season")),
                "game_id": gid,
                "game_date": g.get("gameDate", d)[:10],
                "game_time_utc": g.get("startTimeUTC"),
                "game_type": gt, "week": None,
                "home_team_id": hid, "home_team_abbr": ha,
                "away_team_id": aid, "away_team_abbr": aa,
                "home_score": home.get("score"), "away_score": away.get("score"),
                "game_status": status, "status_detail": detail,
                "venue_name": None, "venue_city": None, "venue_state": None,
                "neutral_site": 1 if g.get("neutralSite") else 0,
                "broadcasts": None,
                "data_source": "NHL_API", "last_updated": now})
            saved += 1
    conn.commit()
    log.info("  [NHL] daily: %d games" % saved)
    return saved


def sync_nba_daily(conn, days_back=1):
    """
    Scores path for NBA. stats.nba.com and cdn.nba.com both refuse this
    machine (403), so scores come from ESPN's scoreboard, the same source
    the NFL path uses.

    ESPN numbers its games differently from nba_api, which owns the rows
    the full pass writes. Inserting by ESPN id would duplicate every game,
    so this UPDATES the existing row, matched on the date and the two
    teams. A game ESPN knows about and we don't is left for the nightly
    full pass to create.
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    updated = missing = 0
    for offset in range(-days_back, 1):
        d = date.today() + timedelta(days=offset)
        ds = d.strftime("%Y-%m-%d")
        data = get_json(
            "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
            "/scoreboard?dates=%s" % d.strftime("%Y%m%d"), pause=0.2)
        if not data:
            continue
        for ev in data.get("events", []):
            comp = (ev.get("competitions") or [{}])[0]
            ha = aa = hs = as_ = None
            for c in comp.get("competitors", []):
                abbr = (c.get("team", {}) or {}).get("abbreviation", "")
                abbr = ESPN_NBA_ABBR.get(abbr, abbr)
                sc = c.get("score")
                try:
                    sc = int(sc) if sc is not None and str(sc) != "" else None
                except Exception:
                    sc = None
                if c.get("homeAway") == "home":
                    ha, hs = abbr, sc
                else:
                    aa, as_ = abbr, sc
            if not ha or not aa:
                continue
            st = (ev.get("status", {}) or {}).get("type", {}) or {}
            if st.get("completed"):
                status, detail = "Final", st.get("shortDetail", "Final")
            elif st.get("state") == "in":
                status, detail = "InProgress", st.get("shortDetail", "")
            else:
                status, detail = "Scheduled", st.get("shortDetail", "Scheduled")
                hs = as_ = None   # don't write scores onto an unplayed game

            cur = conn.execute("""
                UPDATE schedules SET
                    home_score=?, away_score=?,
                    game_status=?, status_detail=?, last_updated=?
                WHERE league='NBA' AND game_date=?
                  AND home_team_abbr=? AND away_team_abbr=?
            """, (hs, as_, status, detail, now, ds, ha, aa))
            if cur.rowcount:
                updated += cur.rowcount
            else:
                missing += 1
                log.debug("  [NBA] no row for %s %s@%s" % (ds, aa, ha))
    conn.commit()
    log.info("  [NBA] daily: %d updated%s"
             % (updated, (", %d not in schedule yet" % missing) if missing else ""))
    return updated


def run(db_path=DB_PATH, league=None, mode="full"):
    conn = pg_connect(); ensure_schedules_table(conn); results = {}
    leagues = [league.upper()] if league else ["NHL","NBA","NFL","MLB"]
    # daily = the cheap scoreboard endpoints; full = the whole slate.
    if "NHL" in leagues:
        log.info("-- NHL --")
        results["nhl"] = (sync_nhl_daily(conn) if mode == "daily"
                          else sync_nhl_schedule(conn, mode=mode))
    if "NBA" in leagues:
        log.info("-- NBA --")
        results["nba"] = (sync_nba_daily(conn) if mode == "daily"
                          else sync_nba_schedule(conn, mode=mode))
    if "NFL" in leagues: log.info("-- NFL --"); results["nfl"] = sync_nfl_schedule(conn, mode=mode)
    if "MLB" in leagues: log.info("-- MLB --"); results["mlb"] = sync_mlb_schedule(conn, mode=mode)
    conn.close(); log.info("Schedules complete: %s" % results); return results

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=DB_PATH, help="ignored; Supabase comes from SUPABASE_DB_URL")
    p.add_argument("--league", default=None)
    p.add_argument("--mode", default="full", choices=["full","daily"])
    a = p.parse_args()
    run(db_path=a.db, league=a.league, mode=a.mode)

