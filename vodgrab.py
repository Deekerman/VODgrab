#!/usr/bin/env python3
"""
VODgrab: download Xtream IPTV VOD as real video files for Sonarr and Radarr.

Single file, Python 3 standard library only. ffprobe (from the ffmpeg package)
is optional but recommended for verifying downloads.

  python3 vodgrab.py serve      run the web UI, Newznab indexer and SABnzbd API
  python3 vodgrab.py install    install and start a systemd service (root)
  python3 vodgrab.py sync       run one catalog sync and exit
"""
import argparse
import base64
import collections
import datetime as dt
import difflib
import email.utils
import gzip
import json
import os
import random
import re
import secrets
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import threading
import time
import traceback
import unicodedata
import zlib
from concurrent.futures import ThreadPoolExecutor
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from xml.sax.saxutils import escape as xesc, quoteattr

VERSION = "1.13.1"
SCALE = 10 ** 10  # catalog ids are provider_id * SCALE + provider stream id
HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("VODGRAB_DATA", os.path.join(HERE, "data"))
DB_PATH = os.path.join(DATA_DIR, "vodgrab.db")
META_PATH = os.path.join(DATA_DIR, "meta.db")  # details from TMDB, OMDb and TVDB; never cleared by a sync
TMDB_BASE = os.environ.get("VODGRAB_TMDB_BASE", "https://api.themoviedb.org/3").rstrip("/")
TMDB_IMG = os.environ.get("VODGRAB_TMDB_IMG", "https://image.tmdb.org/t/p").rstrip("/")
OMDB_BASE = os.environ.get("VODGRAB_OMDB_BASE", "https://www.omdbapi.com/")
TVDB_BASE = os.environ.get("VODGRAB_TVDB_BASE", "https://api4.thetvdb.com/v4").rstrip("/")
MB = 1024 * 1024
GB = 1024 * MB

ALLOWED_INTERVALS = [6, 8, 12, 24, 48, 72, 96, 120, 144, 168]  # no shorter: a sync re-reads whole catalogs
RECHECK_HOURS = [0, 1, 2, 4, 6]  # 0 waits for the next scheduled sync
DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
REF_MONDAY = dt.date(2024, 1, 1)

DEFAULTS = {
    "port": 8765,
    "api_key": "",
    "web_username": "",
    "web_password": "",
    "external_url": "",
    "base_path": "/downloads/iptv",
    "incomplete_path": "",
    "sonarr_complete_path": "",
    "radarr_complete_path": "",
    "manual_tv_path": "",
    "manual_movies_path": "",
    "chown": "",
    "sonarr_url": "",
    "sonarr_api_key": "",
    "sonarr_root": "",
    "sonarr_profile_id": 0,
    "radarr_url": "",
    "radarr_api_key": "",
    "radarr_root": "",
    "radarr_profile_id": 0,
    "sonarr_indexer_priority": 25,
    "radarr_indexer_priority": 25,
    "radarr_add_search": True,
    "sonarr_add_search": True,
    "sonarr_add_monitor": "all",
    "concurrency": 0,
    "speed_limit_kbps": 0,
    "gap_min": 5,
    "gap_max": 30,
    "retries": 3,
    "retry_backoff": [1, 5, 15],
    "max_wait_hours": 2,
    "connect_timeout": 30,
    "paused": False,
    "schedule_enabled": False,
    "on_window_end": "finish",
    "windows": [],
    "sync_interval_hours": 24,
    "grace_cycles": 1,
    "missing_recheck_hours": 2,
    "min_score": 90,
    "cleanup_patterns": [],
    "auto_import_manual": True,
    "default_quality": "1080p",
    "probe_on_search": True,
    "probe_on_open": True,
    "probe_series_open": "season",
    "show_adult": False,
    "wanted_auto_search": False,
    "category_map": [],
    "probe_budget": 30,
    "probe_max_age_days": 30,
    "tmdb_key": "",
    "meta_lang": "en-US",
    "meta_region": "CA",
    "meta_auto": True,
    "meta_rate": 8,
    "meta_match": True,
    "omdb_key": "",
    "omdb_on": True,
    "tvdb_key": "",
    "tvdb_pin": "",
    "backups_on": True,
    "backups_keep": 3,
    "backup_interval_hours": 24,
    "backup_config": True,
    "backup_meta": True,
    "backup_catalog": True,
    "backup_path": "",
    "update_check": True,
    "dispatcharr_url": "",
    "dispatcharr_api_key": "",
    "dispatcharr_proxy": False,
    "dispatcharr_import": False,
}
SECRET_KEYS = ("sonarr_api_key", "radarr_api_key", "web_password", "tmdb_key", "omdb_key", "tvdb_key", "tvdb_pin",
               "dispatcharr_api_key")
DEFAULT_UA = "VLC/3.0.20 LibVLC/3.0.20"

# name: (settings key, default folder under base_path)
FOLDERS = collections.OrderedDict([
    ("incomplete", ("incomplete_path", "incomplete")),
    ("sonarr", ("sonarr_complete_path", "complete/sonarr")),
    ("radarr", ("radarr_complete_path", "complete/radarr")),
    ("manual_tv", ("manual_tv_path", "manual/tv")),
    ("manual_movies", ("manual_movies_path", "manual/movies")),
])

ACTIVE = ("queued", "retry_wait", "downloading", "verifying", "importing")

# ------------------------------------------------------------------ logging

LOG = collections.deque(maxlen=600)
SETTINGS = {}


PROVIDERS = collections.OrderedDict()  # id -> dict, ordered by priority


def mask(s):
    s = str(s)
    vals = [SETTINGS.get(k) for k in ("sonarr_api_key", "radarr_api_key", "tmdb_key", "omdb_key", "tvdb_key",
                                      "dispatcharr_api_key",
                                      "tvdb_pin")]
    for p in list(PROVIDERS.values()):
        vals += [p.get("password"), p.get("username")]
    for v in vals:
        if v and len(str(v)) >= 4:
            v = str(v)
            s = s.replace(v, "***").replace(urllib.parse.quote(v, safe=""), "***")
    return s


LOG_DIR = os.path.join(DATA_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, "vodgrab.log")
LOG_MAX = 5 * MB
LOG_KEEP = 4  # rotated files, vodgrab.log.1 (newest) to vodgrab.log.4
_log_lock = threading.Lock()


def log(msg, level="info"):
    line = "%s [%s] %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), level, mask(msg))
    LOG.append(line)
    print(line, flush=True)
    with _log_lock:
        try:
            os.makedirs(LOG_DIR, exist_ok=True)
            if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) >= LOG_MAX:
                for i in range(LOG_KEEP, 0, -1):
                    src = LOG_FILE + (".%d" % (i - 1) if i > 1 else "")
                    if os.path.exists(src):
                        os.replace(src, "%s.%d" % (LOG_FILE, i))
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except OSError:
            pass


def log_files():
    """All log files, oldest first."""
    names = ["%s.%d" % (LOG_FILE, i) for i in range(LOG_KEEP, 0, -1)] + [LOG_FILE]
    return [n for n in names if os.path.exists(n)]


# ------------------------------------------------------------------ database

_local = threading.local()
FTS = False


def db():
    c = getattr(_local, "c", None)
    if c is None:
        c = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA busy_timeout=30000")
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("ATTACH DATABASE ? AS md", (META_PATH,))
        c.execute("PRAGMA md.journal_mode=WAL")
        c.execute("PRAGMA md.synchronous=NORMAL")
        _local.c = c
    return c


def q(sql, *a):
    return db().execute(sql, a).fetchall()


def q1(sql, *a):
    return db().execute(sql, a).fetchone()


def ex(sql, *a):
    return db().execute(sql, a)


SCHEMA_VERSION = 2
CAT_COLS = [("lang", "TEXT"), ("genres", "TEXT"), ("igenres", "TEXT"), ("services", "TEXT"), ("lists", "TEXT"),
            ("adult", "INTEGER DEFAULT 0"), ("rating_n", "REAL"), ("first_seen", "INTEGER"), ("work", "TEXT"),
            ("srch", "TEXT")]
MEDIA_COLS = [("width", "INTEGER"), ("height", "INTEGER"), ("vcodec", "TEXT"), ("duration", "INTEGER"),
              ("bitrate", "INTEGER")]
CATALOG = """
CREATE TABLE IF NOT EXISTS categories(kind TEXT, id TEXT, name TEXT, prov INTEGER, PRIMARY KEY(kind, id));
CREATE TABLE IF NOT EXISTS movies(id INTEGER PRIMARY KEY, prov INTEGER, name TEXT, clean TEXT, norm TEXT,
  year INTEGER, tmdb INTEGER, ext TEXT, cat TEXT, icon TEXT, rating TEXT, added INTEGER, gen INTEGER,
  missing INTEGER DEFAULT 0);
CREATE INDEX IF NOT EXISTS movies_norm ON movies(norm);
CREATE INDEX IF NOT EXISTS movies_tmdb ON movies(tmdb);
CREATE TABLE IF NOT EXISTS series(id INTEGER PRIMARY KEY, prov INTEGER, name TEXT, clean TEXT, norm TEXT,
  year INTEGER, tmdb INTEGER, cat TEXT, icon TEXT, plot TEXT, rating TEXT, modified INTEGER, gen INTEGER,
  missing INTEGER DEFAULT 0, eps_fetched REAL DEFAULT 0);
CREATE INDEX IF NOT EXISTS series_norm ON series(norm);
CREATE TABLE IF NOT EXISTS episodes(id INTEGER PRIMARY KEY, prov INTEGER, series_id INTEGER, season INTEGER,
  episode INTEGER, title TEXT, ext TEXT, added INTEGER);
CREATE INDEX IF NOT EXISTS ep_series ON episodes(series_id, season, episode);
CREATE TABLE IF NOT EXISTS vodinfo(id INTEGER PRIMARY KEY, data TEXT, fetched REAL);
"""
SCHEMA = """
CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS providers(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, url TEXT, username TEXT,
  password TEXT, user_agent TEXT, max_conn INTEGER DEFAULT 1, priority INTEGER DEFAULT 0,
  enabled INTEGER DEFAULT 1);
CREATE TABLE IF NOT EXISTS matches(arr TEXT, arr_id TEXT, xids TEXT, method TEXT, score REAL, title TEXT,
  year INTEGER, checked REAL, PRIMARY KEY(arr, arr_id));
CREATE TABLE IF NOT EXISTS overrides(arr TEXT, arr_id TEXT, xid INTEGER, PRIMARY KEY(arr, arr_id));
CREATE TABLE IF NOT EXISTS unlinks(arr TEXT, arr_id TEXT, work TEXT, at REAL, PRIMARY KEY(arr, arr_id, work));
CREATE TABLE IF NOT EXISTS unmatched(arr TEXT, arr_id TEXT, title TEXT, year INTEGER, seen REAL,
  PRIMARY KEY(arr, arr_id));
CREATE TABLE IF NOT EXISTS jobs(id INTEGER PRIMARY KEY AUTOINCREMENT, nzo TEXT UNIQUE, source TEXT, arr TEXT,
  kind TEXT, xid INTEGER, ext TEXT, release TEXT, label TEXT, status TEXT, done INTEGER DEFAULT 0,
  total INTEGER DEFAULT 0, attempts INTEGER DEFAULT 0, waited REAL DEFAULT 0, next_at REAL DEFAULT 0,
  error TEXT DEFAULT '', path TEXT DEFAULT '', pos REAL DEFAULT 0, force INTEGER DEFAULT 0,
  hidden INTEGER DEFAULT 0, meta TEXT DEFAULT '{}', created REAL, updated REAL, started REAL, finished REAL);
CREATE INDEX IF NOT EXISTS jobs_status ON jobs(status);
CREATE TABLE IF NOT EXISTS sync_runs(id INTEGER PRIMARY KEY AUTOINCREMENT, started REAL, finished REAL,
  movies INTEGER, series INTEGER, removed INTEGER, ok INTEGER, error TEXT, trigger TEXT, added_movies INTEGER,
  added_series INTEGER, removed_movies INTEGER, removed_series INTEGER, detail TEXT);
"""


# Metadata lives in its own file (meta.db) so a sync, a provider removal or an upgrade never touches it.
META_SCHEMA = """
CREATE TABLE IF NOT EXISTS md.meta(kind TEXT, tmdb INTEGER, fetched REAL, lang TEXT, data BLOB, title TEXT,
  year INTEGER, olang TEXT, runtime INTEGER, cert TEXT, rating REAL, votes INTEGER, pop REAL, status TEXT, imdb TEXT,
  tvdb INTEGER, coll INTEGER, genres TEXT, keywords TEXT, people TEXT, watch TEXT, poster TEXT, backdrop TEXT,
  err TEXT, PRIMARY KEY(kind, tmdb));
CREATE INDEX IF NOT EXISTS md.meta_fetched ON meta(fetched);
CREATE TABLE IF NOT EXISTS md.meta_map(kind TEXT, work TEXT, tmdb INTEGER, score REAL, at REAL, PRIMARY KEY(kind, work));
CREATE TABLE IF NOT EXISTS md.meta_season(tmdb INTEGER, season INTEGER, data BLOB, fetched REAL,
  PRIMARY KEY(tmdb, season));
CREATE TABLE IF NOT EXISTS md.meta_coll(id INTEGER PRIMARY KEY, data BLOB, fetched REAL);
CREATE TABLE IF NOT EXISTS md.omdb(imdb TEXT PRIMARY KEY, data TEXT, fetched REAL);
CREATE TABLE IF NOT EXISTS md.meta_kv(k TEXT PRIMARY KEY, v TEXT);
"""
WORK_META_COLS = [("mt", "INTEGER"), ("pgenres", "TEXT"), ("prating", "REAL"), ("cert", "TEXT"), ("runtime", "INTEGER"),
                  ("pop", "REAL"), ("votes", "INTEGER"), ("mstatus", "TEXT"), ("watch", "TEXT"), ("olang", "TEXT"),
                  ("poster", "TEXT")]


def init_db():
    global FTS
    os.makedirs(DATA_DIR, exist_ok=True)
    apply_staged_restore()
    db().executescript(SCHEMA)
    cols = {r["name"] for r in q("PRAGMA table_info(jobs)")}
    if "sources" not in cols:
        ex("ALTER TABLE jobs ADD COLUMN sources TEXT DEFAULT '[]'")
    pcols = {r["name"] for r in q("PRAGMA table_info(providers)")}
    if "source" not in pcols:
        ex("ALTER TABLE providers ADD COLUMN source TEXT DEFAULT ''")
    if "prov" not in cols:
        ex("ALTER TABLE jobs ADD COLUMN prov INTEGER DEFAULT 0")
    have = {r["name"] for r in q("PRAGMA table_info(sync_runs)")}
    for col, typ in (("added_movies", "INTEGER"), ("added_series", "INTEGER"), ("removed_movies", "INTEGER"),
                     ("removed_series", "INTEGER"), ("detail", "TEXT")):
        if col not in have:
            ex("ALTER TABLE sync_runs ADD COLUMN %s %s" % (col, typ))
    migrate()
    db().executescript(CATALOG)
    for t in ("movies", "episodes"):
        have = {r["name"] for r in q("PRAGMA table_info(%s)" % t)}
        for col, typ in MEDIA_COLS:
            if col not in have:
                ex("ALTER TABLE %s ADD COLUMN %s %s" % (t, col, typ))
                ex("DELETE FROM vodinfo")
                ex("UPDATE series SET eps_fetched=0")
    ex("CREATE TABLE IF NOT EXISTS probes(id INTEGER PRIMARY KEY, width INTEGER, height INTEGER, vcodec TEXT, "
       "duration INTEGER, bitrate INTEGER, size INTEGER, at REAL)")
    if "error" not in {r["name"] for r in q("PRAGMA table_info(probes)")}:
        ex("ALTER TABLE probes ADD COLUMN error TEXT")
    added = False
    for t, cols in (("movies", CAT_COLS + [("qual", "TEXT")]), ("series", CAT_COLS), ("categories", [("grp", "TEXT"),
                                                                                             ("canon", "TEXT")])):
        have = {r["name"] for r in q("PRAGMA table_info(%s)" % t)}
        for col, typ in cols:
            if col not in have:
                ex("ALTER TABLE %s ADD COLUMN %s %s" % (t, col, typ))
                added = True
    if added and q1("SELECT 1 FROM movies LIMIT 1"):
        ex("INSERT OR REPLACE INTO settings(k, v) VALUES('needs_resync', 'true')")
    for idx in ("movies_work ON movies(work)", "series_work ON series(work)", "movies_year ON movies(year)",
                "series_year ON series(year)", "movies_seen ON movies(first_seen)", "series_seen ON series(first_seen)",
                "movies_provwork ON movies(prov, missing)", "series_provwork ON series(prov, missing)"):
        ex("CREATE INDEX IF NOT EXISTS " + idx)
    ex("DROP INDEX IF EXISTS movies_prov")
    ex("DROP INDEX IF EXISTS series_prov")
    ex("DROP TABLE IF EXISTS fts")
    ex("CREATE TABLE IF NOT EXISTS works(wid INTEGER PRIMARY KEY, kind TEXT, work TEXT, rep INTEGER, clean TEXT, "
       "norm TEXT, year INTEGER, tmdb INTEGER, icon TEXT, rating_n REAL, first_seen INTEGER, genres TEXT, "
       "services TEXT, lists TEXT, langs TEXT, provs TEXT, adult INTEGER, np INTEGER, srch TEXT, qrank INTEGER)")
    have = {r["name"] for r in q("PRAGMA table_info(works)")}
    for col, typ in WORK_META_COLS:
        if col not in have:
            ex("ALTER TABLE works ADD COLUMN %s %s" % (col, typ))
    db().executescript(META_SCHEMA)
    for idx in ("works_new ON works(kind, first_seen)", "works_title ON works(kind, clean COLLATE NOCASE)",
                "works_year ON works(kind, year)", "works_rating ON works(kind, rating_n)",
                "works_q ON works(kind, qrank)", "works_work ON works(kind, work)", "works_norm ON works(norm)",
                "works_mt ON works(kind, mt)", "works_pop ON works(kind, pop)", "works_sk ON works(kind, norm, year)"):
        ex("CREATE INDEX IF NOT EXISTS " + idx)
    ex("CREATE TABLE IF NOT EXISTS arr_lib(kind TEXT, arr_id INTEGER, tmdb INTEGER, tvdb INTEGER, norm TEXT, "
       "year INTEGER, has_file INTEGER)")
    have = {r["name"] for r in q("PRAGMA table_info(arr_lib)")}
    for col, typ in (("monitored", "INTEGER DEFAULT 0"), ("available", "INTEGER DEFAULT 1"), ("title", "TEXT"),
                     ("imdb", "TEXT")):
        if col not in have:
            ex("ALTER TABLE arr_lib ADD COLUMN %s %s" % (col, typ))
    ex("CREATE TABLE IF NOT EXISTS wanted(kind TEXT, arr_id INTEGER, arr_ep INTEGER, work TEXT, season INTEGER, "
       "episode INTEGER, title TEXT, air TEXT, available INTEGER, found REAL, searched REAL)")
    ex("CREATE INDEX IF NOT EXISTS wanted_work ON wanted(kind, work)")
    ex("CREATE INDEX IF NOT EXISTS arr_lib_tmdb ON arr_lib(kind, tmdb)")
    ex("CREATE INDEX IF NOT EXISTS arr_lib_norm ON arr_lib(kind, norm)")
    try:
        for t in ("tri_movies", "tri_series", "tri_works"):
            ex("CREATE VIRTUAL TABLE IF NOT EXISTS %s USING fts5(srch, tokenize='trigram')" % t)
        FTS = True
    except sqlite3.OperationalError:
        FTS = False
    load_settings()
    load_providers()
    if not q1("SELECT 1 FROM works LIMIT 1") and q1("SELECT 1 FROM movies UNION ALL SELECT 1 FROM series LIMIT 1"):
        rebuild_works()
    elif q1("SELECT 1 FROM works WHERE mt IS NULL AND tmdb IS NOT NULL LIMIT 1"):
        ex("UPDATE works SET pgenres=genres, prating=rating_n WHERE pgenres IS NULL")
        apply_meta()


def migrate():
    """Version 1 had a single provider stored in settings and raw stream ids in the catalog."""
    r = q1("SELECT v FROM settings WHERE k='schema'")
    if r and int(json.loads(r["v"])) >= SCHEMA_VERSION:
        return
    old = {x["k"]: json.loads(x["v"]) for x in q("SELECT k, v FROM settings WHERE k LIKE 'xtream_%' OR k='user_agent'")}
    if old.get("xtream_url") and not q1("SELECT 1 FROM providers"):
        pid = ex("INSERT INTO providers(name, url, username, password, user_agent, max_conn, priority, enabled) "
                 "VALUES(?,?,?,?,?,1,1,1)", "Provider 1", old.get("xtream_url"), old.get("xtream_username", ""),
                 old.get("xtream_password", ""), old.get("user_agent") or DEFAULT_UA).lastrowid
        for j in q("SELECT id, xid, ext FROM jobs"):
            cid = pid * SCALE + (j["xid"] or 0)
            ex("UPDATE jobs SET xid=?, prov=?, sources=? WHERE id=?", cid, pid, json.dumps([[cid, j["ext"]]]), j["id"])
    for t in ("movies", "series", "episodes", "categories", "vodinfo", "matches", "overrides", "fts", "tri_movies",
              "tri_series"):
        ex("DROP TABLE IF EXISTS %s" % t)
    db().executescript(SCHEMA)
    ex("DELETE FROM settings WHERE k LIKE 'xtream_%' OR k='user_agent'")
    ex("INSERT OR REPLACE INTO settings(k, v) VALUES('schema', ?)", json.dumps(SCHEMA_VERSION))


# ------------------------------------------------------------------ providers


def enc(pid, raw):
    return int(pid) * SCALE + int(raw)


def dec(cid):
    return divmod(int(cid), SCALE)


def load_providers():
    rows = q("SELECT * FROM providers ORDER BY priority, id")
    PROVIDERS.clear()
    for r in rows:
        PROVIDERS[r["id"]] = dict(r)


def prov(pid):
    p = PROVIDERS.get(int(pid))
    if not p:
        raise RuntimeError("Provider %s no longer exists" % pid)
    return p


def active_providers():
    return [p for p in PROVIDERS.values() if p["enabled"] and p["url"]]


def pname(pid):
    p = PROVIDERS.get(int(pid or 0))
    return p["name"] if p else "Removed provider"


def save_provider(data):
    pid = to_int(data.get("id"))
    name = (data.get("name") or "").strip()
    url = (data.get("url") or "").strip().rstrip("/")
    if not url:
        raise ValueError("Server URL is required")
    if not re.match(r"^https?://", url):
        url = "http://" + url
    vals = {"name": name or urllib.parse.urlparse(url).hostname or "Provider",
            "url": url, "username": (data.get("username") or "").strip(),
            "user_agent": (data.get("user_agent") or "").strip() or DEFAULT_UA,
            "max_conn": max(1, min(20, to_int(data.get("max_conn")) or 1)),
            "priority": to_int(data.get("priority")) or 0,
            "enabled": 1 if data.get("enabled", True) not in (False, 0, "0", "false") else 0}
    if "source" in data:
        vals["source"] = data.get("source") or ""
    if data.get("password"):
        vals["password"] = data["password"]
    if pid:
        if pid not in PROVIDERS:
            raise ValueError("Provider not found")
        ex("UPDATE providers SET %s WHERE id=?" % ", ".join("%s=?" % k for k in vals), *(list(vals.values()) + [pid]))
    else:
        if not vals["priority"]:
            r = q1("SELECT MAX(priority) m FROM providers")
            vals["priority"] = (r["m"] or 0) + 1
        vals.setdefault("password", "")
        pid = ex("INSERT INTO providers(%s) VALUES(%s)" % (", ".join(vals), ", ".join("?" * len(vals))),
                 *vals.values()).lastrowid
    load_providers()
    ENGINE.prov_errors.pop(pid, None)
    threading.Thread(target=catalog_changed, args=(True,), daemon=True).start()
    return pid


def delete_provider(pid):
    pid = int(pid)
    lo, hi = pid * SCALE, (pid + 1) * SCALE
    for t in ("movies", "series", "episodes", "vodinfo"):
        ex("DELETE FROM %s WHERE id>=? AND id<?" % t, lo, hi)
    ex("DELETE FROM categories WHERE prov=?", pid)
    if FTS:
        for t in ("tri_movies", "tri_series"):
            ex("DELETE FROM %s WHERE rowid>=? AND rowid<?" % t, lo, hi)
    ex("DELETE FROM providers WHERE id=?", pid)
    ex("DELETE FROM matches")
    load_providers()
    threading.Thread(target=catalog_changed, args=(True,), daemon=True).start()


def providers_view():
    out = []
    for p in PROVIDERS.values():
        d = {k: p[k] for k in ("id", "name", "url", "username", "user_agent", "max_conn", "priority", "enabled")}
        d["source"] = p.get("source") or ""
        d["password_set"] = bool(p.get("password"))
        d["active"] = ENGINE.active_count(p["id"])
        d["error"] = ENGINE.prov_errors.get(p["id"], "")
        d["movies"] = q1("SELECT COUNT(*) c FROM movies WHERE prov=?", p["id"])["c"]
        d["series"] = q1("SELECT COUNT(*) c FROM series WHERE prov=?", p["id"])["c"]
        out.append(d)
    return out


# ------------------------------------------------------------------ settings

_slock = threading.Lock()
SCHED = {"reset": False}


def load_settings():
    s = dict(DEFAULTS)
    for r in q("SELECT k, v FROM settings"):
        try:
            s[r["k"]] = json.loads(r["v"])
        except ValueError:
            pass
    SETTINGS.clear()
    SETTINGS.update(s)
    if not q1("SELECT 1 FROM settings WHERE k='migrated_1_12'"):
        # 1.12.0: missing titles are kept for 1 sync (was 3) and checked again sooner; TV quality checks on open
        # get their own setting, starting from what movies use
        up = {"probe_series_open": "season" if SETTINGS.get("probe_on_open") else "off"}
        if SETTINGS.get("grace_cycles") == 3:
            up["grace_cycles"] = 1
        save_settings(up)
        ex("INSERT OR REPLACE INTO settings(k, v) VALUES('migrated_1_12', 'true')")
    if not q1("SELECT 1 FROM settings WHERE k='migrated_1_13_1'"):
        # 1.13.1: syncs no more often than every 6 hours; rechecks from a short list (the old default 6 becomes 2)
        up = {}
        for k in ("sync_interval_hours", "backup_interval_hours"):
            if to_int(SETTINGS.get(k)) not in ALLOWED_INTERVALS:
                up[k] = min(ALLOWED_INTERVALS, key=lambda h: abs(h - (to_int(SETTINGS.get(k)) or 24)))
        rc = to_int(SETTINGS.get("missing_recheck_hours"))
        if rc == 6 or rc not in RECHECK_HOURS:
            up["missing_recheck_hours"] = 2 if rc == 6 else min(RECHECK_HOURS, key=lambda h: abs(h - (rc or 0)))
        if up:
            save_settings(up)
        ex("INSERT OR REPLACE INTO settings(k, v) VALUES('migrated_1_13_1', 'true')")
    if not SETTINGS.get("api_key"):
        save_settings({"api_key": secrets.token_hex(16)})


def S():
    return SETTINGS


def coerce(k, v):
    d = DEFAULTS[k]
    if isinstance(d, bool):
        if isinstance(v, str):
            return v.lower() in ("1", "true", "yes", "on")
        return bool(v)
    if isinstance(d, int):
        return int(float(v or 0))
    if isinstance(d, list):
        if isinstance(v, str):
            v = [x.strip() for x in re.split(r"[,\n]", v) if x.strip()]
        return list(v or [])
    return str(v if v is not None else "").strip()


def save_settings(upd):
    with _slock:
        old_interval = SETTINGS.get("sync_interval_hours")
        for k, v in upd.items():
            if k not in DEFAULTS:
                continue
            v = coerce(k, v)
            if k == "backup_interval_hours" and v not in ALLOWED_INTERVALS:
                raise ValueError("Backup interval must be one of %s hours" % ALLOWED_INTERVALS)
            if k == "sync_interval_hours" and v not in ALLOWED_INTERVALS:
                raise ValueError("Sync interval must be one of %s hours" % ALLOWED_INTERVALS)
            if k.endswith("_indexer_priority"):
                v = max(1, min(50, v or 25))
            if k == "probe_series_open" and v not in ("season", "first", "off"):
                raise ValueError("TV quality check must be season, first or off")
            if k == "missing_recheck_hours" and v not in RECHECK_HOURS:
                raise ValueError("Recheck must be one of %s hours (0 is off)" % RECHECK_HOURS)
            if k == "grace_cycles":
                v = max(0, min(30, v))
            if k == "probe_budget":
                v = max(5, min(90, v))
            if k == "probe_max_age_days":
                v = max(1, min(365, v))
            if k == "concurrency":
                v = max(0, min(50, v))
            if k.endswith("_path") and v:
                if not v.startswith("/"):
                    raise ValueError("Folder paths must be absolute, like /downloads/iptv")
                v = v.rstrip("/") or "/"
            if k == "retries":
                v = max(0, min(50, v))
            if k == "meta_rate":
                v = max(1, min(40, v))
            if k == "backups_keep":
                v = max(1, min(60, v))
            if k == "meta_region":
                v = v.upper()
                if not re.match(r"^[A-Z]{2}$", v):
                    raise ValueError("Region must be a two letter country code, like CA or US")
            if k == "meta_lang":
                m_ = re.match(r"^([a-zA-Z]{2})(?:[-_]([a-zA-Z]{2}))?$", v)
                if not m_:
                    raise ValueError("Metadata language must look like en-US or fr-CA")
                v = m_.group(1).lower() + ("-" + m_.group(2).upper() if m_.group(2) else "")
            changed = SETTINGS.get(k) != v
            if k == "retry_backoff":
                v = [max(0, int(float(x))) for x in v] or [5]
            if k == "category_map":
                bad = [x for x in v if x.strip() and not re.match(r"^\s*(.+?)\s*=\s*(service|genre|list|ignore|adult)"
                                                                 r"\s*(?::\s*(.+))?$", x, re.I)]
                if bad:
                    raise ValueError("Category override not understood: %s. Use: Name = service, genre, list, ignore "
                                     "or adult" % bad[0])
            if k == "cleanup_patterns":
                for p in v:
                    re.compile(p)
            if k == "windows":
                v = [clean_window(w) for w in v]
            SETTINGS[k] = v
            ex("INSERT OR REPLACE INTO settings(k, v) VALUES(?, ?)", k, json.dumps(v))
            if changed:
                meta_setting_changed(k)
        if SETTINGS.get("sync_interval_hours") != old_interval:
            SCHED["reset"] = True
        if "show_adult" in upd:
            _facet_cache.clear()


def meta_setting_changed(k):
    if k in ("tmdb_key", "tvdb_key", "tvdb_pin", "meta_auto", "meta_match"):
        META.update(auth_bad=False, error="")
        TVDB_TOKEN["token"] = None
        META["wake"].set()
    elif k == "meta_region":
        threading.Thread(target=recompute_meta_cols, daemon=True).start()
    elif k == "meta_lang":
        ex("UPDATE md.meta SET fetched=0 WHERE data IS NOT NULL")
        ex("UPDATE md.meta_season SET fetched=0")
        ex("UPDATE md.meta_coll SET fetched=0")
        META["wake"].set()


def clean_window(w):
    days = [d for d in (w.get("days") or []) if d in DAYS] or list(DAYS)

    def hm(v, dflt):
        m = re.match(r"^(\d{1,2}):(\d{2})$", str(v or ""))
        if not m or int(m.group(1)) > 23 or int(m.group(2)) > 59:
            return dflt
        return "%02d:%02d" % (int(m.group(1)), int(m.group(2)))

    return {"days": days, "start": hm(w.get("start"), "00:00"), "end": hm(w.get("end"), "23:59"),
            "concurrency": max(0, min(50, int(float(w.get("concurrency") or 0))))}


def folder(name):
    key, sub = FOLDERS[name]
    v = (S().get(key) or "").strip()
    return v.rstrip("/") if v else os.path.join(S()["base_path"].rstrip("/"), sub)


def folders_view():
    return {name: folder(name) for name in FOLDERS}


def under(path, *names):
    path = os.path.abspath(path or "")
    return any(path.startswith(os.path.abspath(folder(n)) + os.sep) for n in names)


FOLDER_ERRORS = {}


def ensure_dirs():
    """Create the download folders and confirm VODgrab can write into each one."""
    current = {folder(n) for n in FOLDERS}
    for stale in [d for d in FOLDER_ERRORS if d not in current]:
        FOLDER_ERRORS.pop(stale, None)
    for name in FOLDERS:
        d = folder(name)
        try:
            parent = os.path.dirname(d)
            if parent.startswith(S()["base_path"].rstrip("/")) and not os.path.isdir(parent):
                os.makedirs(parent, exist_ok=True)
                fix_owner(parent)
            if not os.path.isdir(d):
                os.makedirs(d, exist_ok=True)
            fix_owner(d)
            test = os.path.join(d, ".vodgrab-write-test")
            with open(test, "w") as fh:
                fh.write("ok")
            os.remove(test)
            if FOLDER_ERRORS.pop(d, None):
                log("VODgrab can write to %s again" % d)
        except OSError as e:
            if d not in FOLDER_ERRORS:
                log("VODgrab cannot write to %s: %s" % (d, e.strerror or e), "error")
            FOLDER_ERRORS[d] = ("VODgrab cannot write to %s (%s). Give the VODgrab user write access to it, "
                                "or pick another folder in Settings." % (d, e.strerror or e))


def fix_owner(path):
    """Make what VODgrab creates readable and writable by everyone (777). Folders and files that belong to
    another user are left as they are: only their owner may change them, and they may already be fine."""
    try:
        st = os.stat(path)
    except OSError:
        return
    if st.st_uid == os.geteuid() and (st.st_mode & 0o777) != 0o777:
        try:
            os.chmod(path, 0o777)
        except OSError as e:
            log("chmod failed for %s: %s" % (path, e), "warn")
    spec = (S().get("chown") or "").strip()
    if not spec:
        return
    try:
        uid, _, gid = spec.partition(":")
        os.chown(path, int(uid), int(gid or uid))
    except (ValueError, OSError) as e:
        log("chown failed for %s: %s" % (path, e), "warn")


# ------------------------------------------------------------------ text helpers

TAG_RE = re.compile(r"\s*[\[\(](?:4K|8K|UHD|FHD|HD|SD|HEVC|MULTI|MULTI SUB|VOST\w*|SUB\w*|DUB\w*|NF|AMZN)[\]\)]",
                    re.I)
LANGS = {"EN": "English", "ENG": "English", "UK": "English", "US": "English", "USA": "English", "CA": "English",
         "AU": "English", "FR": "French", "FRE": "French", "VF": "French", "VOSTFR": "French", "DE": "German",
         "GER": "German", "ES": "Spanish", "SPA": "Spanish", "LAT": "Spanish", "IT": "Italian", "ITA": "Italian",
         "NL": "Dutch", "PT": "Portuguese", "BR": "Portuguese", "PL": "Polish", "TR": "Turkish", "AR": "Arabic",
         "RU": "Russian", "SE": "Swedish", "SW": "Swedish", "NO": "Norwegian", "DK": "Danish", "FI": "Finnish",
         "IN": "Hindi", "HI": "Hindi", "GR": "Greek", "RO": "Romanian", "HU": "Hungarian", "CZ": "Czech",
         "AL": "Albanian", "EX": "Yugoslav", "KR": "Korean", "JP": "Japanese", "CN": "Chinese"}
TAGWORDS = set(LANGS) | {"4K", "8K", "UHD", "FHD", "HD", "SD", "MULTI", "NF", "AMZN", "VIP", "HEVC", "3D"}
PREFIX_RE = re.compile(r"^\s*(?:\[([^\]]{1,12})\]|\|([^|]{1,12})\||\{([^}]{1,12})\}|([A-Za-z0-9+]{2,6})(?:\s+-\s+|\s*\|\s*|\s*:\s+))")
YEAR_RE = re.compile(r"(?:\s*[\(\[]((?:19|20)\d{2})[\)\]]|\s+-\s+((?:19|20)\d{2}))\s*$")
STOP = {"the", "and", "of", "a", "an", "in", "on", "to", "for", "de", "la", "le"}
ROMAN = {"ii": "2", "iii": "3", "iv": "4", "v": "5", "vi": "6", "vii": "7", "viii": "8", "ix": "9", "x": "10"}


def to_int(v):
    if v is None or v == "":
        return None
    try:
        return int(float(str(v).strip()))
    except (ValueError, TypeError):
        return None


def strip_prefixes(name):
    """Remove leading provider tags like 'EN |', '[4K]', '|FR|'. Returns (rest, language or None)."""
    n, lang = name or "", None
    for _ in range(3):
        m = PREFIX_RE.match(n)
        if not m:
            break
        tag = next(g for g in m.groups() if g is not None).strip().upper()
        if tag not in TAGWORDS:
            break
        lang = lang or LANGS.get(tag)
        n = n[m.end():]
    return n, lang


def clean_title(name, with_lang=False):
    n, lang = strip_prefixes(name)
    n = TAG_RE.sub("", n)
    for p in S().get("cleanup_patterns") or []:
        try:
            n = re.sub(p, "", n, flags=re.I)
        except re.error:
            pass
    n = re.sub(r"\s+", " ", n).strip().strip("|").strip()
    year = None
    m = YEAR_RE.search(n)
    if m:
        year = int(m.group(1) or m.group(2))
        n = n[:m.start()].strip()
    n = re.sub(r"\s+[-:|]+$", "", n).strip() or (name or "").strip()
    return (n, year, lang) if with_lang else (n, year)


def year_of(*vals):
    for v in vals:
        m = re.search(r"(?:^|\D)((?:19|20)\d{2})(?:\D|$)", str(v or ""))
        if m:
            return int(m.group(1))
    return None


def episode_name(raw, show, season, episode):
    """'S.W.A.T. Exiles (2026) - S01E01 - Clean Entry' -> 'Clean Entry'; "'Allo 'Allo! S01E01" -> ''."""
    t = (raw or "").strip()
    m = re.search(r"\bS\d{1,3}\s*E\d{1,4}\b\s*[-:|.]*\s*", t, re.I)
    if m:
        t = t[m.end():]
    elif show and norm(t).startswith(norm(show)):
        t = t[len(show):]
    t = t.strip(" -:|.")
    if not t or norm(t) in (norm(show), "episode %s" % episode, "episode %02d" % (episode or 0)):
        return ""
    return t


def norm(s):
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    s = re.sub(r"^(the|a|an) ", "", s)
    return re.sub(r"\s+", " ", s).strip()


def search_text(*names):
    """Everything a title can be searched by: normalized words, joined form, roman numerals as digits."""
    parts = []
    for n in names:
        w = norm(n)
        if not w:
            continue
        parts.append(w)
        parts.append(w.replace(" ", ""))
        r = " ".join(ROMAN.get(x, x) for x in w.split())
        if r != w:
            parts.append(r)
    return " | ".join(dict.fromkeys(parts))


# Category sorting: service, genre, list, ignore or adult. Keys are norm() forms.
SERVICES = {
    "netflix": "Netflix", "amazon prime video": "Prime Video", "prime video": "Prime Video", "amazon": "Prime Video",
    "amazon prime": "Prime Video", "disney": "Disney+", "disney plus": "Disney+", "hbo max": "HBO Max", "max": "HBO Max",
    "hbo": "HBO Max", "apple tv": "Apple TV+", "apple tv plus": "Apple TV+", "hulu": "Hulu", "paramount": "Paramount+",
    "paramount plus": "Paramount+", "peacock": "Peacock", "crunchyroll": "Crunchyroll", "starz": "Starz",
    "amc": "AMC+", "amc plus": "AMC+", "mgm": "MGM+", "mgm plus": "MGM+", "discovery": "Discovery+",
    "discovery plus": "Discovery+", "now sky": "NOW", "now": "NOW", "now tv": "NOW", "sky go": "Sky Go",
    "britbox": "BritBox", "crave": "Crave", "stan": "Stan", "binge": "BINGE", "foxtel now": "Foxtel Now",
    "viaplay": "Viaplay", "acorn tv": "Acorn TV", "shudder": "Shudder", "curiosity stream": "Curiosity Stream",
    "youtube premium": "YouTube Premium", "fubotv": "fuboTV", "fubo": "fuboTV", "tubi": "Tubi", "philo": "Philo",
    "plex": "Plex", "pluto tv": "Pluto TV", "roku channel": "The Roku Channel", "xumo play": "Xumo Play",
    "hayu": "Hayu", "showtime": "Showtime", "mubi": "MUBI", "criterion channel": "Criterion Channel",
    "bbc iplayer": "BBC iPlayer", "itvx": "ITVX", "channel 4": "Channel 4", "sky": "Sky", "canal": "Canal+",
    "canal plus": "Canal+", "hotstar": "Hotstar", "zee5": "ZEE5", "sonyliv": "SonyLIV", "funimation": "Funimation",
    "hidive": "HIDIVE", "vix": "ViX", "rtl": "RTL+", "joyn": "Joyn", "videoland": "Videoland", "skyshowtime": "SkyShowtime",
}
GENRES = {
    "action": ["Action"], "adventure": ["Adventure"], "action and adventure": ["Action", "Adventure"],
    "animation": ["Animation"], "animated": ["Animation"], "anime": ["Anime"], "comedy": ["Comedy"],
    "comedies": ["Comedy"], "crime": ["Crime"], "documentary": ["Documentary"], "documentaries": ["Documentary"],
    "docs": ["Documentary"], "drama": ["Drama"], "dramas": ["Drama"], "family": ["Family"], "fantasy": ["Fantasy"],
    "history": ["History"], "historical": ["History"], "horror": ["Horror"], "music": ["Music"],
    "musical": ["Music"], "mystery": ["Mystery"], "romance": ["Romance"], "romantic": ["Romance"],
    "science fiction": ["Science Fiction"], "sci fi": ["Science Fiction"], "scifi": ["Science Fiction"],
    "sci fi and fantasy": ["Science Fiction", "Fantasy"], "thriller": ["Thriller"], "thrillers": ["Thriller"],
    "tv movie": ["TV Movie"], "tv movies": ["TV Movie"], "war": ["War"], "war and politics": ["War", "Politics"],
    "politics": ["Politics"], "western": ["Western"], "westerns": ["Western"], "kids": ["Kids"],
    "children": ["Kids"], "reality": ["Reality"], "reality tv": ["Reality"], "talk show": ["Talk Show"],
    "talk": ["Talk Show"], "game show": ["Game Show"], "news": ["News"], "food": ["Food"], "cooking": ["Food"],
    "sport": ["Sport"], "sports": ["Sport"], "biography": ["Biography"], "biopic": ["Biography"],
    "soap": ["Soap"], "stand up": ["Stand Up"], "standup": ["Stand Up"], "stand up comedy": ["Stand Up"],
    "christmas": ["Christmas"], "holiday": ["Christmas"], "martial arts": ["Action"], "superhero": ["Action"],
    "suspense": ["Thriller"], "sitcom": ["Comedy"], "teen": ["Family"], "lgbt": ["LGBTQ"], "lgbtq": ["LGBTQ"],
    "concert": ["Music"], "concerts": ["Music"], "musicals": ["Music"],
}
IGNORE_RE = re.compile(r"^(unlabeled|unlabelled|unsorted|other|others|misc|miscellaneous|uncategori[sz]ed|"
                       r"all|all movies|all series|vod|movies|series|tv shows|tv series)( movies| series)?$")
ADULT_RE = re.compile(r"(^|[^a-z0-9])(xxx|adult|adults|18\+|\+18|porn|erotic|for adults)([^a-z0-9]|$)", re.I)


def category_map():
    """User overrides from Settings, lines like 'Top Picks = list' or 'Docu = genre: Documentary'."""
    out = {}
    for line in S().get("category_map") or []:
        m = re.match(r"^\s*(.+?)\s*=\s*(service|genre|list|ignore|adult)\s*(?::\s*(.+))?$", line, re.I)
        if m:
            out[norm(m.group(1))] = (m.group(2).lower(), [x.strip() for x in (m.group(3) or m.group(1)).split(",")])
    return out


def classify(name):
    """Sort a provider category into (group, [canonical names]). group is service, genre, list, ignore or adult."""
    raw = name or ""
    rest, _ = strip_prefixes(raw)
    key = norm(rest.replace("+", " plus "))
    key2 = norm(rest)
    over = category_map()
    for k in (norm(raw), key2, key):
        if k in over:
            return over[k]
    if ADULT_RE.search(raw):
        return "adult", ["Adult"]
    if not key2 or IGNORE_RE.match(key2):
        return "ignore", []
    for k in (key, key2, re.sub(r"\s+(movies|series|tv|shows|originals|original)$", "", key2)):
        if k in SERVICES:
            return "service", [SERVICES[k]]
        if k in GENRES:
            return "genre", GENRES[k]
    # 'Sci-Fi & Fantasy Movies', 'Comedy Series' and similar
    stripped = re.sub(r"\b(movies?|films?|series|tv|shows?)\b", "", key2).strip()
    if stripped in GENRES:
        return "genre", GENRES[stripped]
    if stripped in SERVICES:
        return "service", [SERVICES[stripped]]
    return "list", [re.sub(r"\s+", " ", rest).strip() or raw]


def split_genres(text):
    out = []
    for g in re.split(r"\s*[,/|;]\s*|\s+&\s+(?=[A-Z])", text or ""):
        k = norm(g)
        if not k:
            continue
        out += GENRES.get(k, [g.strip().title()])
    return list(dict.fromkeys(out))


def tagcol(values):
    """Store a set of names so SQL can filter with LIKE '%|name|%'."""
    vals = [v for v in dict.fromkeys(values) if v]
    return "|" + "|".join(vals) + "|" if vals else ""


def untag(col):
    return [x for x in (col or "").split("|") if x]


def rating10(item):
    r5 = to_float(item.get("rating_5based"))
    if r5:
        return round(min(10.0, r5 * 2), 1)
    r = to_float(item.get("rating"))
    return round(min(10.0, r), 1) if r else None


def to_float(v):
    try:
        f = float(str(v).strip())
        return f if f > 0 else None
    except (ValueError, TypeError):
        return None


def score(a, b):
    a = " ".join(sorted(norm(a).split()))
    b = " ".join(sorted(norm(b).split()))
    if not a or not b:
        return 0.0
    if a == b:
        return 100.0
    return difflib.SequenceMatcher(None, a, b).ratio() * 100


def quality_of(name):
    n = (name or "").lower()
    if re.search(r"(^|[^a-z0-9])(4k|uhd|2160p?)([^a-z0-9]|$)", n):
        return "2160p"
    if re.search(r"(^|[^a-z0-9])720p?([^a-z0-9]|$)", n):
        return "720p"
    if re.search(r"(^|[^a-z0-9])(sd|480p?)([^a-z0-9]|$)", n):
        return "480p"
    return S().get("default_quality") or "1080p"


CODECS = {"hevc": "HEVC", "h265": "HEVC", "h264": "H.264", "avc": "H.264", "av1": "AV1", "mpeg4": "MPEG4",
          "mpeg2video": "MPEG2", "vp9": "VP9"}


def res_label(w, h):
    w, h = to_int(w) or 0, to_int(h) or 0
    if not w and not h:
        return None
    if w >= 3200 or h >= 1800:
        return "2160p"
    if w >= 1700 or h >= 1000:
        return "1080p"
    if w >= 1100 or h >= 700:
        return "720p"
    return "480p"


def media_of(info):
    """Video details an Xtream panel reports in get_vod_info or get_series_info."""
    if not isinstance(info, dict):
        return {}
    v = info.get("video")
    v = v if isinstance(v, dict) else {}
    dur = to_int(info.get("duration_secs"))
    if not dur and info.get("duration"):
        m = re.match(r"^(\d+):(\d{2}):(\d{2})$", str(info["duration"]))
        dur = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3)) if m else None
    return {"width": to_int(v.get("width")), "height": to_int(v.get("height")),
            "vcodec": v.get("codec_name") or None, "duration": dur or None,
            "bitrate": to_int(info.get("bitrate")) or None}


def media_view(row):
    """Quality summary for a catalog row, preferring a direct check over what the provider reports."""
    d = dict(row) if row else {}
    p = q1("SELECT * FROM probes WHERE id=? AND error IS NULL", d.get("id")) if d.get("id") else None
    src = "provider"
    if p:
        d.update({k: p[k] for k in ("width", "height", "vcodec", "duration", "bitrate") if p[k]})
        src = "checked"
    label = res_label(d.get("width"), d.get("height"))
    size = (p["size"] if p and p["size"] else None) or (
        int(d["bitrate"] * 1000 / 8 * d["duration"]) if d.get("bitrate") and d.get("duration") else None)
    return {"quality": label, "known": bool(label), "source": src if label else None,
            "width": d.get("width"), "height": d.get("height"),
            "codec": CODECS.get((d.get("vcodec") or "").lower(), (d.get("vcodec") or "").upper() or None),
            "duration": d.get("duration"), "bitrate": d.get("bitrate"), "size": size,
            "checked_at": p["at"] if p else None}


def quality_for(row, name):
    mv = media_view(row) if row is not None and "width" in row.keys() else {}
    return mv.get("quality") or quality_of(name)


def rel_name(*parts):
    t = ".".join(str(p) for p in parts if p not in (None, ""))
    t = unicodedata.normalize("NFKD", t).encode("ascii", "ignore").decode()
    t = t.replace("&", "and")
    t = re.sub(r"[^A-Za-z0-9.\-\s]", "", t)
    t = re.sub(r"[\s.]+", ".", t).strip(".")
    return t or "untitled"


def b64e(obj):
    return base64.urlsafe_b64encode(json.dumps(obj, separators=(",", ":")).encode()).decode().rstrip("=")


def b64d(s):
    s = s.strip()
    return json.loads(base64.urlsafe_b64decode(s + "=" * (-len(s) % 4)).decode())


# ------------------------------------------------------------------ http + xtream


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None


_OPENER = urllib.request.build_opener(_NoRedirect)


def http(url, method="GET", body=None, headers=None, timeout=60, final=None):
    """HTTP request that follows redirects for every method, keeping the method and body on 307 and 308.
    If final is a list, the URL that finally answered is appended to it."""
    h = {"User-Agent": DEFAULT_UA, "Accept": "*/*"}
    if headers:
        h.update(headers)
    if body is not None and not isinstance(body, bytes):
        body = json.dumps(body).encode()
        h.setdefault("Content-Type", "application/json")
    for _ in range(6):
        req = urllib.request.Request(url, data=body, headers=h, method=method)
        try:
            with _OPENER.open(req, timeout=timeout) as r:
                if final is not None:
                    final.append(url)
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            loc = e.headers.get("Location") if e.headers else None
            if e.code not in (301, 302, 303, 307, 308) or not loc:
                raise
            url = urllib.parse.urljoin(url, loc)
            if e.code == 303:
                method, body = "GET", None
    raise RuntimeError("Too many redirects")


def xt_api(pid, action=None, timeout=120, **params):
    p = prov(pid)
    if not p["url"]:
        raise RuntimeError("%s has no server URL" % p["name"])
    args = {"username": p["username"], "password": p["password"]}
    if action:
        args["action"] = action
    args.update(params)
    url = "%s/player_api.php?%s" % (p["url"].rstrip("/"), urllib.parse.urlencode(args))
    try:
        _, raw = http(url, headers={"User-Agent": p["user_agent"] or DEFAULT_UA}, timeout=timeout)
    except urllib.error.HTTPError as e:
        raise RuntimeError("%s returned HTTP %s for %s" % (p["name"], e.code, action or "account info"))
    except (urllib.error.URLError, OSError) as e:
        raise RuntimeError("%s unreachable: %s" % (p["name"], getattr(e, "reason", e)))
    try:
        return json.loads(raw.decode("utf-8", "replace") or "null")
    except ValueError:
        raise RuntimeError("%s returned something that is not JSON for %s" % (p["name"], action or "account info"))


def stream_url(kind, cid, ext):
    pid, raw = dec(cid)
    p = prov(pid)
    seg = "movie" if kind == "movie" else "series"
    return "%s/%s/%s/%s/%s.%s" % (p["url"].rstrip("/"), seg, urllib.parse.quote(p["username"], safe=""),
                                  urllib.parse.quote(p["password"], safe=""), raw, ext)


# ------------------------------------------------------------------ arr clients


class ArrError(Exception):
    pass


class Arr:
    def __init__(self, name):
        self.name = name

    @property
    def label(self):
        return self.name.title()

    def cfg(self):
        s = S()
        return (s.get(self.name + "_url") or "").rstrip("/"), s.get(self.name + "_api_key") or ""

    def ok(self):
        u, k = self.cfg()
        return bool(u and k)

    def call(self, method, path, params=None, body=None, timeout=30):
        u, k = self.cfg()
        if not (u and k):
            raise ArrError("%s is not configured" % self.label)
        url = u + path + ("?" + urllib.parse.urlencode(params, doseq=True) if params else "")
        final = []
        try:
            _, raw = http(url, method, body, {"X-Api-Key": k, "Accept": "application/json"}, timeout, final)
            if final and final[0] != url:
                self.learn(url, final[0], path)
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", "replace")[:300]
            raise ArrError("%s %s %s returned %s: %s" % (self.label, method, path, e.code, detail))
        except (urllib.error.URLError, OSError) as e:
            raise ArrError("%s unreachable: %s" % (self.label, getattr(e, "reason", e)))
        if not raw:
            return None
        try:
            return json.loads(raw)
        except ValueError:
            return None

    def learn(self, asked, answered, path):
        """The arr redirected us: save the address it actually answers on so later calls go straight there."""
        p = urllib.parse.urlparse(answered)
        i = p.path.find(path)
        if i < 0:
            return
        base = "%s://%s%s" % (p.scheme, p.netloc, p.path[:i].rstrip("/"))
        if base and base != self.cfg()[0]:
            save_settings({self.name + "_url": base})
            log("%s redirected %s to %s, saved %s as its URL" % (self.label, asked.split("?")[0],
                                                                answered.split("?")[0], base))

    def get(self, path, **params):
        return self.call("GET", path, params or None)

    def post(self, path, body, params=None):
        return self.call("POST", path, params, body)

    def put(self, path, body, params=None):
        return self.call("PUT", path, params, body)

    def delete(self, path, params=None):
        return self.call("DELETE", path, params)


SONARR = Arr("sonarr")
RADARR = Arr("radarr")


def ARR(name):
    return SONARR if name == "sonarr" else RADARR


_info_cache = {}


def cached(key, fn, ttl=3600):
    v = _info_cache.get(key)
    if v and time.time() - v[0] < ttl:
        return v[1]
    r = fn()
    _info_cache[key] = (time.time(), r)
    return r


def sonarr_series_by_tvdb(tvdb, fresh=False):
    def f():
        r = SONARR.get("/api/v3/series", tvdbId=tvdb) or []
        r = [x for x in r if str(x.get("tvdbId")) == str(tvdb)]
        if r:
            return r[0]
        r = SONARR.get("/api/v3/series/lookup", term="tvdb:%s" % tvdb) or []
        return r[0] if r else None
    if fresh:
        _info_cache.pop(("s", str(tvdb)), None)
    return cached(("s", str(tvdb)), f)


def imdb_tt(imdb):
    if not imdb:
        return None
    imdb = str(imdb)
    if imdb.startswith("tt"):
        return imdb
    return "tt%07d" % int(imdb) if imdb.isdigit() else imdb


def radarr_movie(tmdb=None, imdb=None, fresh=False):
    def f():
        if tmdb:
            r = RADARR.get("/api/v3/movie", tmdbId=tmdb) or []
            r = [m for m in r if str(m.get("tmdbId")) == str(tmdb)]
            if r:
                return r[0]
            try:
                return RADARR.get("/api/v3/movie/lookup/tmdb", tmdbId=tmdb)
            except ArrError:
                return None
        if imdb:
            try:
                return RADARR.get("/api/v3/movie/lookup/imdb", imdbId=imdb_tt(imdb))
            except ArrError:
                return None
        return None
    key = ("m", str(tmdb), str(imdb))
    if fresh:
        _info_cache.pop(key, None)
    return cached(key, f)


def titles_of(info):
    out = []
    for t in [info.get("title"), info.get("originalTitle")] + [a.get("title") for a in info.get("alternateTitles") or []]:
        if t and t not in out:
            out.append(t)
    return out


def arr_profile(name):
    s = S()
    pid = s.get(name + "_profile_id")
    if pid:
        return int(pid)
    profs = ARR(name).get("/api/v3/qualityprofile") or []
    if not profs:
        raise ArrError("%s has no quality profiles" % name.title())
    return profs[0]["id"]


def arr_root(name):
    s = S()
    if s.get(name + "_root"):
        return s[name + "_root"]
    roots = ARR(name).get("/api/v3/rootfolder") or []
    if not roots:
        raise ArrError("%s has no root folders" % name.title())
    return roots[0]["path"]


def best_by_title(results, title, year):
    best, best_sc = None, 0
    for r in results or []:
        ry = r.get("year")
        if year and ry and abs(int(year) - int(ry)) > 1:
            continue
        sc = max([score(title, t) for t in titles_of(r)] or [0])
        if sc > best_sc:
            best, best_sc = r, sc
    return best if best_sc >= S()["min_score"] else None


# ------------------------------------------------------------------ matching


def live(adult=None):
    """SQL filter for catalog rows that belong to enabled providers, hiding adult titles unless allowed."""
    ids = [str(p["id"]) for p in active_providers()]
    sql = "prov IN (%s)" % (",".join(ids) or "0")
    if adult is False or (adult is None and not S().get("show_adult")):
        sql += " AND COALESCE(adult, 0)=0"
    return sql


def prio(cid):
    pid = dec(cid)[0]
    keys = list(PROVIDERS)
    return keys.index(pid) if pid in keys else 999


def find_catalog(kind, titles, year, tmdb):
    table = "movies" if kind == "movie" else "series"
    if tmdb:
        rows = q("SELECT id FROM %s WHERE tmdb=? AND %s" % (table, live()), int(tmdb))
        if not rows:
            rows = q("SELECT id FROM %s WHERE work IN (SELECT work FROM works WHERE kind=? AND mt=?) AND %s"
                     % (table, live()), kind, int(tmdb))
        if rows:
            return sorted([r["id"] for r in rows], key=prio), "tmdb", 100.0
    cands = {}
    for t in titles:
        toks = [w for w in norm(t).split() if len(w) >= 3 and w not in STOP] or norm(t).split()
        if not toks:
            continue
        tok = max(toks, key=len)
        for r in q("SELECT id, clean, year FROM %s WHERE norm LIKE ? AND %s" % (table, live()), "%" + tok + "%"):
            cands[r["id"]] = r
    scored = []
    for r in cands.values():
        if year and r["year"] and abs(int(year) - int(r["year"])) > 1:
            continue
        sc = max(score(t, r["clean"]) for t in titles)
        if sc >= S()["min_score"]:
            scored.append((sc, r["id"]))
    if not scored:
        return [], "none", 0.0
    best = max(sc for sc, _ in scored)
    return sorted([i for sc, i in scored if sc >= best - 3], key=prio), "fuzzy", best


def equivalents(kind, cid):
    """The same movie or series on every enabled provider, best provider first."""
    table = "movies" if kind == "movie" else "series"
    r = q1("SELECT id, norm, year, tmdb FROM %s WHERE id=?" % table, cid)
    if not r:
        return []
    rows = q("SELECT id FROM %s WHERE %s AND ((tmdb IS NOT NULL AND tmdb=?) OR (norm=? AND (year IS NULL OR ? IS NULL "
             "OR ABS(year-?)<=1)))" % (table, live()), r["tmdb"], r["norm"], r["year"], r["year"])
    ids = {x["id"] for x in rows} | {cid}
    return sorted(ids, key=prio)


def drop_unlinked(table, arr, arr_id, ids):
    """Leave out catalog titles you marked as not being this Radarr movie or Sonarr series."""
    bad = {r["work"] for r in q("SELECT work FROM unlinks WHERE arr=? AND arr_id=?", arr, str(arr_id))}
    if not bad or not ids:
        return ids
    keep = []
    for i in ids:
        r = q1("SELECT work FROM %s WHERE id=?" % table, i)
        if not (r and r["work"] in bad):
            keep.append(i)
    return keep


def link_key(lib_row):
    """How Radarr and Sonarr searches name a title (TMDB ID for movies, TVDB ID for series); overrides, unlinks and
    matches are stored under this key."""
    if not lib_row:
        return None
    if lib_row["kind"] == "movie":
        return str(lib_row["tmdb"]) if lib_row["tmdb"] else (lib_row["imdb"] or None)
    return str(lib_row["tvdb"]) if lib_row["tvdb"] else None


def arr_links(kind, xid):
    """The Radarr movies (or Sonarr series) a catalog title is linked to, and how."""
    arr, table = ("radarr", "movies") if kind == "movie" else ("sonarr", "series")
    row = q1("SELECT work, tmdb, norm, year FROM %s WHERE id=?" % table, xid)
    if not row or not row["work"]:
        return []
    w = q1("SELECT * FROM works WHERE kind=? AND work=?", kind, row["work"])
    mt = (w["mt"] or w["tmdb"]) if w else row["tmdb"]
    nrm, yr = (w["norm"], w["year"]) if w else (row["norm"], row["year"])
    out, seen = [], set()
    key = "CAST(l.%s AS TEXT)" % ("tmdb" if kind == "movie" else "tvdb")
    for r in q("SELECT l.* FROM overrides o JOIN %s x ON x.id=o.xid JOIN arr_lib l ON l.kind=? AND %s=o.arr_id "
               "WHERE o.arr=? AND x.work=?" % (table, key), kind, arr, row["work"]):
        seen.add(r["arr_id"])
        out.append((r, "manual"))
    for r in q("SELECT * FROM arr_lib l WHERE l.kind=? AND ((? IS NOT NULL AND l.tmdb=?) OR (l.norm=? AND (? IS NULL OR "
               "l.year IS NULL OR ABS(l.year-?)<=1))) AND NOT EXISTS (SELECT 1 FROM unlinks u WHERE u.arr=? AND "
               "u.arr_id=%s AND u.work=?)" % key, kind, mt, mt, nrm, yr, yr, arr, row["work"]):
        if r["arr_id"] not in seen:
            seen.add(r["arr_id"])
            out.append((r, "tmdb" if mt and r["tmdb"] == mt else "title"))
    return [{"arr": arr, "arr_id": r["arr_id"], "title": r["title"], "year": r["year"], "tmdb": r["tmdb"],
             "imdb": r["imdb"], "tvdb": r["tvdb"], "has_file": bool(r["has_file"]), "monitored": bool(r["monitored"]),
             "how": how} for r, how in out]


def link_change(data):
    """'Not this movie' (unlink) or 'link to a different movie' (relink) for a catalog title."""
    kind = "movie" if data.get("kind") == "movie" else "series"
    arr, table = ("radarr", "movies") if kind == "movie" else ("sonarr", "series")
    xid = to_int(data.get("xid"))
    row = q1("SELECT id, work, clean, year FROM %s WHERE id=?" % table, xid)
    if not row or not row["work"]:
        raise ValueError("Title not found")
    olds = data.get("unlink")
    olds = [to_int(x) for x in (olds if isinstance(olds, list) else [olds]) if to_int(x)]
    new = to_int(data.get("link"))
    name = "%s (%s)" % (row["clean"], row["year"] or "?")
    for old in olds:
        if old == new:
            continue
        was = q1("SELECT * FROM arr_lib WHERE kind=? AND arr_id=?", kind, old)
        key = link_key(was)
        if not key:
            continue
        ex("INSERT OR REPLACE INTO unlinks(arr, arr_id, work, at) VALUES(?,?,?,?)", arr, key, row["work"], time.time())
        ex("DELETE FROM overrides WHERE arr=? AND arr_id=? AND xid IN (SELECT id FROM %s WHERE work=?)" % table,
           arr, key, row["work"])
        ex("DELETE FROM matches WHERE arr=? AND arr_id=?", arr, key)
        log("%s is not %s's %s (%s)" % (name, arr.title(), was["title"], was["year"]))
    if new:
        lib = q1("SELECT * FROM arr_lib WHERE kind=? AND arr_id=?", kind, new)
        key = link_key(lib)
        if not key:
            raise ValueError("That %s is not in %s's library, or has no %s ID" % (
                kind, arr.title(), "TMDB" if kind == "movie" else "TVDB"))
        ex("DELETE FROM unlinks WHERE arr=? AND arr_id=? AND work=?", arr, key, row["work"])
        ex("INSERT OR REPLACE INTO overrides(arr, arr_id, xid) VALUES(?,?,?)", arr, key, row["id"])
        ex("DELETE FROM matches WHERE arr=? AND arr_id=?", arr, key)
        ex("DELETE FROM unmatched WHERE arr=? AND arr_id=?", arr, key)
    if new:
        log("%s is now linked to %s's %s (%s)" % (name, arr.title(), lib["title"], lib["year"]))
    _facet_cache.clear()
    threading.Thread(target=refresh_wanted, daemon=True).start()
    return {"links": arr_links(kind, row["id"])}


def arr_lib_search(kind, text):
    """Radarr movies or Sonarr series by title, for 'link to a different movie'."""
    toks = [t for t in norm(text or "").split() if t]
    if not toks:
        return []
    sql = " AND ".join("norm LIKE ?" for _ in toks)
    rows = q("SELECT * FROM arr_lib WHERE kind=? AND %s ORDER BY title LIMIT 25" % sql, kind,
             *["%" + t + "%" for t in toks])
    return [{"arr_id": r["arr_id"], "title": r["title"], "year": r["year"], "tmdb": r["tmdb"], "imdb": r["imdb"],
             "tvdb": r["tvdb"], "has_file": bool(r["has_file"])} for r in rows]


def match(arr, arr_id, info_fn, tmdb_hint=None):
    """Return (catalog ids, arr info) for an arr item."""
    kind = "movie" if arr == "radarr" else "series"
    table = "movies" if kind == "movie" else "series"
    arr_id = str(arr_id)
    info = None
    try:
        info = info_fn() if ARR(arr).ok() else None
    except ArrError as e:
        log(str(e), "warn")
    o = q1("SELECT xid FROM overrides WHERE arr=? AND arr_id=?", arr, arr_id)
    if o and q1("SELECT 1 FROM %s WHERE id=? AND %s" % (table, live()), o["xid"]):
        return [o["xid"]] + [i for i in equivalents(kind, o["xid"]) if i != o["xid"]], info
    m = q1("SELECT xids, checked FROM matches WHERE arr=? AND arr_id=?", arr, arr_id)
    if m and time.time() - (m["checked"] or 0) < 6 * 3600:
        ids = [i for i in json.loads(m["xids"] or "[]") if q1("SELECT 1 FROM %s WHERE id=? AND %s" % (table, live()), i)]
        ids = drop_unlinked(table, arr, arr_id, ids)
        if ids:
            return ids, info
    titles = titles_of(info) if info else []
    year = info.get("year") if info else None
    tmdb = (info.get("tmdbId") if info else None) or tmdb_hint
    if not titles and not tmdb:
        return [], info
    ids, method, sc = find_catalog(kind, titles, year, tmdb)
    ids = drop_unlinked(table, arr, arr_id, ids)
    if not ids and method == "tmdb" and titles:  # every TMDB match was unlinked: try by title instead
        ids, method, sc = find_catalog(kind, titles, year, None)
        ids = drop_unlinked(table, arr, arr_id, ids)
    if ids:
        ids = drop_unlinked(table, arr, arr_id, sorted(set(ids) | set(equivalents(kind, ids[0])), key=prio))
    title = titles[0] if titles else ""
    ex("INSERT OR REPLACE INTO matches(arr, arr_id, xids, method, score, title, year, checked) VALUES(?,?,?,?,?,?,?,?)",
       arr, arr_id, json.dumps(ids), method, sc, title, year, time.time())
    if ids:
        ex("DELETE FROM unmatched WHERE arr=? AND arr_id=?", arr, arr_id)
    elif title:
        ex("INSERT OR REPLACE INTO unmatched(arr, arr_id, title, year, seen) VALUES(?,?,?,?,?)",
           arr, arr_id, title, year, time.time())
    return ids, info


# ------------------------------------------------------------------ catalog sync

SYNC = {"running": False, "phase": "", "error": ""}
_sync_lock = threading.Lock()
_ep_locks = collections.defaultdict(threading.Lock)


def run_sync(trigger="schedule", only=None):
    """Sync every enabled provider's catalog, or only the providers in only (a recheck of missing titles)."""
    if not _sync_lock.acquire(blocking=False):
        return False
    run_id = None
    try:
        SYNC.update(running=True, phase="Starting", error="")
        started = time.time()
        run_id = ex("INSERT INTO sync_runs(started, trigger) VALUES(?, ?)", started, trigger).lastrowid
        dispatcharr_refresh()
        provs = active_providers()
        if only:
            provs = [p for p in provs if p["id"] in only] or provs
        if not provs:
            raise RuntimeError("No enabled providers")
        log("Catalog sync started (%s)" % trigger if not only else "Catalog sync started (%s: %s)" % (
            trigger, ", ".join(p["name"] for p in provs)))
        keys = ("movies", "series", "added_movies", "added_series", "removed_movies", "removed_series")
        tot = dict.fromkeys(keys, 0)
        detail = []
        errors = []
        for p in provs:
            try:
                res = sync_provider(p, started)
                detail.append(res)
                for k in keys:
                    tot[k] += res[k]
            except Exception as e:
                errors.append("%s: %s" % (p["name"], mask(str(e))))
                log("Catalog sync failed for %s: %s" % (p["name"], e), "error")
        ex("DELETE FROM matches")
        ex("DELETE FROM settings WHERE k='needs_resync'")
        try:
            ex("PRAGMA optimize=0x10002")
        except sqlite3.Error:
            pass
        catalog_changed(warm=True)
        threading.Thread(target=refresh_library, daemon=True).start()
        err = "; ".join(errors)
        ok = len(errors) < len(provs)
        SYNC["error"] = err
        ex("UPDATE sync_runs SET finished=?, movies=?, series=?, removed=?, ok=?, error=?, added_movies=?, "
           "added_series=?, removed_movies=?, removed_series=?, detail=? WHERE id=?",
           time.time(), tot["movies"], tot["series"], tot["removed_movies"] + tot["removed_series"], 1 if ok else 0,
           err or None, tot["added_movies"], tot["added_series"], tot["removed_movies"], tot["removed_series"],
           json.dumps([{k: v for k, v in d.items() if k != "titles"} for d in detail]), run_id)
        log("Catalog sync done: %s%s" % (sync_summary(tot), ("; errors: " + err) if err else ""))
        # Providers whose lists left titles missing get a recheck of just their lists later
        missing = [d for d in detail if d.get("kept_movies", 0) + d.get("kept_series", 0)]
        kept = sum(d["kept_movies"] + d["kept_series"] for d in missing)
        hours = int(S().get("missing_recheck_hours") or 0)
        at = time.time() + hours * 3600 if kept and hours and int(S()["grace_cycles"]) > 0 else 0
        if at and at >= next_slot(time.time(), S()["sync_interval_hours"]):
            at = 0  # the next scheduled sync comes first and checks them anyway
        ex("INSERT OR REPLACE INTO settings(k, v) VALUES('recheck_at', ?)", json.dumps(at))
        ex("INSERT OR REPLACE INTO settings(k, v) VALUES('recheck_provs', ?)",
           json.dumps([d["pid"] for d in missing] if at else []))
        if at:
            log("%d title%s missing from %s; checking %s again at %s" % (
                kept, "" if kept == 1 else "s", ", ".join(d["provider"] for d in missing),
                "it" if len(missing) == 1 else "them", time.strftime("%Y-%m-%d %H:%M", time.localtime(at))))
        try:
            write_sync_changes(run_id, started, trigger, detail, tot, err)
        except OSError as e:
            log("Could not save the sync's change list: %s" % e, "warn")
        return ok
    except Exception as e:
        SYNC["error"] = mask(str(e))
        log("Catalog sync failed: %s" % e, "error")
        if run_id:
            ex("UPDATE sync_runs SET finished=?, ok=0, error=? WHERE id=?", time.time(), mask(str(e)), run_id)
        return False
    finally:
        SYNC.update(running=False, phase="")
        _sync_lock.release()


def added_trustworthy(items):
    """Some panels stamp their whole catalog with the same 'added' date; then it says nothing about what is new."""
    days = [to_int(it.get("added")) // 86400 for it in items if isinstance(it, dict) and to_int(it.get("added"))]
    if len(days) < 50:
        return bool(days)
    top = collections.Counter(days).most_common(1)[0][1]
    return top < len(days) * 0.5


def item_cats(it):
    ids = it.get("category_ids")
    ids = [str(x) for x in ids] if isinstance(ids, list) and ids else [str(it.get("category_id") or "")]
    return [x for x in ids if x and x != "None"]


def sync_provider(p, started):
    pid, name = p["id"], p["name"]
    SYNC["phase"] = "%s categories" % name
    cats, catinfo = [], {}
    for kind, action in (("movie", "get_vod_categories"), ("series", "get_series_categories")):
        for c in xt_api(pid, action) or []:
            cid = str(c.get("category_id"))
            cname = c.get("category_name") or ""
            grp, canon = classify(cname)
            lang = strip_prefixes(cname)[1]
            catinfo[(kind, cid)] = (grp, canon, lang)
            cats.append((kind, "%d:%s" % (pid, cid), cname, pid, grp, tagcol(canon)))
    SYNC["phase"] = "%s movies" % name
    movies = xt_api(pid, "get_vod_streams", timeout=300)
    SYNC["phase"] = "%s series" % name
    series = xt_api(pid, "get_series", timeout=300)
    if not isinstance(movies, list) or not isinstance(series, list):
        raise RuntimeError("unexpected catalog format")
    if not movies and not series:
        raise RuntimeError("empty catalog, keeping the current one")
    SYNC["phase"] = "%s saving" % name
    gen = int(started)
    grace = int(S()["grace_cycles"])
    for label, items, table in (("movies", movies, "movies"), ("series", series, "series")):
        had = q1("SELECT COUNT(*) c FROM %s WHERE prov=? AND missing=0" % table, pid)["c"]
        if not items and had > 100:
            log("%s returned 0 %s (had %d); keeping them for up to %d more syncs" % (name, label, had, grace), "warn")
    initial = bool(q1("SELECT 1 FROM settings WHERE k='needs_resync'")) or not q1(
        "SELECT 1 FROM movies WHERE prov=? UNION ALL SELECT 1 FROM series WHERE prov=? LIMIT 1", pid, pid)
    now = int(time.time())

    def common(kind, it, raw_name):
        clean, y, lang = clean_title(raw_name, True)
        svc, gen_, lst, adult = [], [], [], str(it.get("is_adult")) == "1"
        for cid in item_cats(it):
            grp, canon, clang = catinfo.get((kind, cid), ("list", [], None))
            lang = lang or clang
            if grp == "service":
                svc += canon
            elif grp == "genre":
                gen_ += canon
            elif grp == "list":
                lst += canon
            elif grp == "adult":
                adult = True
        gen_ += split_genres(it.get("genre"))
        if ADULT_RE.search(raw_name or ""):
            adult = True
        return clean, y, lang, svc, gen_, lst, adult

    trust_m, trust_s = added_trustworthy(movies), added_trustworthy(series)
    mrows, srows = [], []
    for m in movies:
        if not isinstance(m, dict):
            continue
        raw = to_int(m.get("stream_id"))
        if not raw:
            continue
        clean, y, lang, svc, gen_, lst, adult = common("movie", m, m.get("name"))
        year = to_int(m.get("year")) or y
        tmdb = to_int(m.get("tmdb") or m.get("tmdb_id")) or None
        added = to_int(m.get("added")) or 0
        first = (added if trust_m else 0) if initial else now
        mrows.append((enc(pid, raw), pid, m.get("name") or "", clean, norm(clean), year, tmdb,
                      (m.get("container_extension") or "mp4").strip(".") or "mp4",
                      "%d:%s" % (pid, (item_cats(m) or [""])[0]), m.get("stream_icon") or "",
                      str(m.get("rating") or ""), added, gen, lang, tagcol(gen_), tagcol(svc), tagcol(lst),
                      1 if adult else 0, rating10(m), first, ("t%d" % tmdb) if tmdb else "n:%s:%s" % (norm(clean), year or ""),
                      search_text(clean, strip_prefixes(m.get("name"))[0])))
    for sr in series:
        if not isinstance(sr, dict):
            continue
        raw = to_int(sr.get("series_id"))
        if not raw:
            continue
        clean, y, lang, svc, gen_, lst, adult = common("series", sr, sr.get("name"))
        year = to_int(sr.get("year")) or y or year_of(sr.get("releaseDate"), sr.get("release_date"))
        tmdb = to_int(sr.get("tmdb") or sr.get("tmdb_id")) or None
        added = to_int(sr.get("last_modified")) or 0
        first = (added if trust_s else 0) if initial else now
        srows.append((enc(pid, raw), pid, sr.get("name") or "", clean, norm(clean), year, tmdb,
                      "%d:%s" % (pid, (item_cats(sr) or [""])[0]), sr.get("cover") or "", sr.get("plot") or "",
                      str(sr.get("rating") or ""), added, gen, lang, tagcol(gen_), tagcol(svc), tagcol(lst),
                      1 if adult else 0, rating10(sr), first, ("t%d" % tmdb) if tmdb else "n:%s:%s" % (norm(clean), year or ""),
                      search_text(clean, strip_prefixes(sr.get("name"))[0])))
    c = db()
    c.execute("BEGIN")
    try:
        had_m = {r[0] for r in c.execute("SELECT id FROM movies WHERE prov=?", (pid,))}
        had_s = {r[0] for r in c.execute("SELECT id FROM series WHERE prov=?", (pid,))}
        c.execute("DELETE FROM categories WHERE prov=?", (pid,))
        c.executemany("INSERT OR REPLACE INTO categories(kind, id, name, prov, grp, canon) VALUES(?,?,?,?,?,?)", cats)
        c.executemany("""INSERT INTO movies(id, prov, name, clean, norm, year, tmdb, ext, cat, icon, rating, added, gen,
            lang, genres, services, lists, adult, rating_n, first_seen, work, srch, missing)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0) ON CONFLICT(id) DO UPDATE SET name=excluded.name,
            clean=excluded.clean, norm=excluded.norm, year=COALESCE(excluded.year, movies.year),
            tmdb=COALESCE(excluded.tmdb, movies.tmdb), ext=excluded.ext, cat=excluded.cat, icon=excluded.icon,
            rating=excluded.rating, added=excluded.added, gen=excluded.gen, lang=excluded.lang,
            genres=excluded.genres, services=excluded.services, lists=excluded.lists, adult=excluded.adult,
            rating_n=COALESCE(excluded.rating_n, movies.rating_n), first_seen=COALESCE(movies.first_seen,
            excluded.first_seen), work=CASE WHEN excluded.work LIKE 't%' OR movies.work IS NULL THEN excluded.work
            ELSE movies.work END, srch=excluded.srch, missing=0""", mrows)
        c.executemany("""INSERT INTO series(id, prov, name, clean, norm, year, tmdb, cat, icon, plot, rating, modified,
            gen, lang, genres, services, lists, adult, rating_n, first_seen, work, srch, missing)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0) ON CONFLICT(id) DO UPDATE SET name=excluded.name,
            clean=excluded.clean, norm=excluded.norm, year=COALESCE(excluded.year, series.year),
            tmdb=COALESCE(excluded.tmdb, series.tmdb), cat=excluded.cat, icon=excluded.icon, plot=excluded.plot,
            rating=excluded.rating,
            eps_fetched=CASE WHEN series.modified<>excluded.modified THEN 0 ELSE series.eps_fetched END,
            modified=excluded.modified, gen=excluded.gen, lang=excluded.lang, genres=excluded.genres,
            services=excluded.services, lists=excluded.lists, adult=excluded.adult,
            rating_n=COALESCE(excluded.rating_n, series.rating_n),
            first_seen=COALESCE(series.first_seen, excluded.first_seen), work=excluded.work, srch=excluded.srch,
            missing=0""", srows)
        # (title, year) of every title this sync added, removed, or found missing but kept for now
        added = {"movie": {r[0]: (r[3], r[5]) for r in mrows if r[0] not in had_m},
                 "series": {r[0]: (r[3], r[5]) for r in srows if r[0] not in had_s}}
        c.execute("UPDATE movies SET missing=missing+1 WHERE prov=? AND gen<>?", (pid, gen))
        c.execute("UPDATE series SET missing=missing+1 WHERE prov=? AND gen<>?", (pid, gen))
        removed, kept = {}, {}
        for kind, table in (("movie", "movies"), ("series", "series")):
            removed[kind] = [(r[0], r[1]) for r in c.execute(
                "SELECT clean, year FROM %s WHERE prov=? AND missing>?" % table, (pid, grace))]
            kept[kind] = c.execute("SELECT COUNT(*) FROM %s WHERE prov=? AND missing BETWEEN 1 AND ?" % table,
                                   (pid, grace)).fetchone()[0]
            c.execute("DELETE FROM %s WHERE prov=? AND missing>?" % table, (pid, grace))
        c.execute("DELETE FROM episodes WHERE prov=? AND series_id NOT IN (SELECT id FROM series)", (pid,))
        if FTS:
            lo, hi = pid * SCALE, (pid + 1) * SCALE
            for t, tab in (("tri_movies", "movies"), ("tri_series", "series")):
                c.execute("DELETE FROM %s WHERE rowid>=? AND rowid<?" % t, (lo, hi))
                c.execute("INSERT INTO %s(rowid, srch) SELECT id, srch FROM %s WHERE prov=?" % (t, tab), (pid,))
        c.execute("COMMIT")
    except Exception:
        c.execute("ROLLBACK")
        raise
    for r in q("SELECT id FROM movies WHERE prov=? AND qual IS NULL AND (width IS NOT NULL OR id IN "
               "(SELECT id FROM probes WHERE error IS NULL))", pid):
        set_qual("movies", r["id"])
    res = {"provider": name, "pid": pid, "movies": len(mrows), "series": len(srows),
           "added_movies": len(added["movie"]), "added_series": len(added["series"]),
           "removed_movies": len(removed["movie"]), "removed_series": len(removed["series"]),
           "kept_movies": kept["movie"], "kept_series": kept["series"],
           "titles": {"added": {k: list(v.values()) for k, v in added.items()}, "removed": removed}}
    log("%s: %s" % (name, sync_summary(res)))
    return res


SYNC_CHANGES_DIR = os.path.join(DATA_DIR, "sync-changes")
SYNC_CHANGES_KEEP = 30


def sync_changes_path(run_id):
    return os.path.join(SYNC_CHANGES_DIR, "sync-%d.txt" % int(run_id))


def write_sync_changes(run_id, started, trigger, detail, tot, err):
    """A readable list of every title a sync added or removed, one file per sync (the newest 30 are kept)."""
    out = ["VODgrab catalog sync %s (%s)" % (time.strftime("%Y-%m-%d %H:%M", time.localtime(started)), trigger), ""]
    for d in detail:
        out.append("%s: %s" % (d["provider"], sync_summary(d)))
    out.append("Total: %s" % sync_summary(tot))
    if err:
        out.append("Errors: %s" % err)
    for section, label in (("removed", "Removed"), ("added", "Added")):
        lines = []
        for d in detail:
            for kind in ("movie", "series"):
                for title, year in d["titles"][section][kind]:
                    lines.append((d["provider"], kind, (title or "").lower(), "[%s] %-6s  %s%s" % (
                        d["provider"], kind, title, " (%s)" % year if year else "")))
        out += ["", "== %s (%d) ==" % (label, len(lines))] + [x[3] for x in sorted(lines)]
    kept = [(d["provider"], d["kept_movies"], d["kept_series"]) for d in detail if d["kept_movies"] or d["kept_series"]]
    out += ["", "== No longer listed by the provider, kept for now (%d) ==" % sum(m + s for _, m, s in kept)]
    out += ["%s: %d movies, %d series" % k for k in kept]
    if kept:
        g = int(S()["grace_cycles"])
        out.append("These are removed if they are still missing after %d more sync%s, and then listed above."
                   % (g, "" if g == 1 else "s"))
    os.makedirs(SYNC_CHANGES_DIR, exist_ok=True)
    tmp = sync_changes_path(run_id) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")
    os.replace(tmp, sync_changes_path(run_id))
    files = sorted((f for f in os.listdir(SYNC_CHANGES_DIR) if re.match(r"^sync-\d+\.txt$", f)),
                   key=lambda f: int(f[5:-4]))
    for f in files[:-SYNC_CHANGES_KEEP]:
        os.remove(os.path.join(SYNC_CHANGES_DIR, f))


def sync_summary(r):
    return "%d movies (+%d new, %d removed), %d series (+%d new, %d removed)" % (
        r["movies"], r["added_movies"], r["removed_movies"], r["series"], r["added_series"], r["removed_series"])


def ensure_episodes(sid, max_age=12 * 3600):
    with _ep_locks[sid]:
        r = q1("SELECT eps_fetched FROM series WHERE id=?", sid)
        if r is None or time.time() - (r["eps_fetched"] or 0) < max_age:
            return
        pid, raw = dec(sid)
        data = xt_api(pid, "get_series_info", series_id=raw) or {}
        if not isinstance(data, dict):
            data = {}
        eps = data.get("episodes") or {}
        items = []
        if isinstance(eps, dict):
            for key, lst in eps.items():
                for e in lst or []:
                    if isinstance(e, dict):
                        items.append((to_int(key), e))
        elif isinstance(eps, list):
            for lst in eps:
                for e in (lst if isinstance(lst, list) else [lst]):
                    if isinstance(e, dict):
                        items.append((None, e))
        rows = []
        show = (q1("SELECT clean FROM series WHERE id=?", sid) or {"clean": ""})["clean"]
        for key_season, e in items:
            eid = to_int(e.get("id"))
            season = to_int(e.get("season"))
            if season is None:
                season = key_season
            num = to_int(e.get("episode_num"))
            if not eid or season is None or num is None:
                continue
            md = media_of(e.get("info"))
            info_ = e.get("info") if isinstance(e.get("info"), dict) else {}
            title = episode_name(e.get("title") or info_.get("name") or "", show, season, num)
            rows.append((enc(pid, eid), pid, sid, season, num, title,
                         (e.get("container_extension") or "mkv").strip(".") or "mkv", to_int(e.get("added")) or 0,
                         md.get("width"), md.get("height"), md.get("vcodec"), md.get("duration"), md.get("bitrate")))
        info = data.get("info") or {}
        if not isinstance(info, dict):
            info = {}
        tmdb = to_int(info.get("tmdb") or info.get("tmdb_id")) or None
        c = db()
        c.execute("BEGIN")
        try:
            c.execute("DELETE FROM episodes WHERE series_id=?", (sid,))
            c.executemany("INSERT OR REPLACE INTO episodes(id, prov, series_id, season, episode, title, ext, added, "
                          "width, height, vcodec, duration, bitrate) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
            c.execute("UPDATE series SET eps_fetched=?, tmdb=COALESCE(tmdb, ?), plot=CASE WHEN plot='' "
                      "THEN ? ELSE plot END WHERE id=?", (time.time(), tmdb, info.get("plot") or "", sid))
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise


def episode_equivalents(eid, fetch=True):
    """The same episode on every enabled provider, best provider first, as (id, ext) pairs."""
    e = q1("SELECT * FROM episodes WHERE id=?", eid)
    if not e:
        return []
    out = [(e["id"], e["ext"])]
    for sid in equivalents("series", e["series_id"]):
        if sid == e["series_id"]:
            continue
        if fetch:
            try:
                ensure_episodes(sid)
            except Exception as ex_:
                log("Episode list for %s failed: %s" % (pname(dec(sid)[0]), ex_), "warn")
                continue
        o = q1("SELECT id, ext FROM episodes WHERE series_id=? AND season=? AND episode=?", sid, e["season"],
               e["episode"])
        if o:
            out.append((o["id"], o["ext"]))
    return sorted(out, key=lambda x: prio(x[0]))


def movie_sources(mid):
    rows = [q1("SELECT id, ext FROM movies WHERE id=?", i) for i in equivalents("movie", mid)]
    return [(r["id"], r["ext"]) for r in rows if r]


def vod_info(xid):
    r = q1("SELECT data, fetched FROM vodinfo WHERE id=?", xid)
    if r and time.time() - r["fetched"] < 7 * 86400:
        return json.loads(r["data"])
    pid, raw = dec(xid)
    data = xt_api(pid, "get_vod_info", vod_id=raw) or {}
    info = data.get("info") if isinstance(data, dict) else None
    info = info if isinstance(info, dict) else {}
    ex("INSERT OR REPLACE INTO vodinfo(id, data, fetched) VALUES(?,?,?)", xid, json.dumps(info), time.time())
    tmdb = to_int(info.get("tmdb_id") or info.get("tmdb")) or None
    if tmdb:
        ex("UPDATE movies SET tmdb=COALESCE(tmdb, ?) WHERE id=?", tmdb, xid)
    md = media_of(info)
    if any(md.values()):
        ex("UPDATE movies SET width=?, height=?, vcodec=?, duration=?, bitrate=? WHERE id=?", md["width"],
           md["height"], md["vcodec"], md["duration"], md["bitrate"], xid)
    g = split_genres(info.get("genre"))
    if g:
        ex("UPDATE movies SET igenres=? WHERE id=?", tagcol(g), xid)
    set_qual("movies", xid)
    return info


def set_qual(table, cid):
    """Keep the quality column used by the browse filter in step with what is known."""
    if table != "movies":
        return
    r = q1("SELECT * FROM movies WHERE id=?", cid)
    if r:
        qual = media_view(r)["quality"]
        if qual != r["qual"]:
            ex("UPDATE movies SET qual=? WHERE id=?", qual, cid)
            update_work_quality(cid)


LIB = {"at": 0, "running": False}


def refresh_library(wanted=True):
    """Cache what Sonarr and Radarr already have (library status, TVDB IDs), then what they want."""
    if LIB["running"]:
        return
    LIB["running"] = True
    try:
        rows = []
        if RADARR.ok():
            for m in RADARR.call("GET", "/api/v3/movie", timeout=120) or []:
                rows.append(("movie", m.get("id"), to_int(m.get("tmdbId")), None, norm(m.get("title")),
                             to_int(m.get("year")), 1 if m.get("hasFile") else 0, 1 if m.get("monitored") else 0,
                             1 if m.get("isAvailable", True) else 0, m.get("title") or "", m.get("imdbId") or None))
        if SONARR.ok():
            for sr in SONARR.call("GET", "/api/v3/series", timeout=120) or []:
                st = sr.get("statistics") or {}
                rows.append(("series", sr.get("id"), to_int(sr.get("tmdbId")), to_int(sr.get("tvdbId")),
                             norm(sr.get("title")), to_int(sr.get("year")),
                             1 if (st.get("episodeFileCount") or 0) > 0 else 0, 1 if sr.get("monitored") else 0, 1,
                             sr.get("title") or "", sr.get("imdbId") or None))
        c = db()
        c.execute("BEGIN")
        try:
            c.execute("DELETE FROM arr_lib")
            c.executemany("INSERT INTO arr_lib(kind, arr_id, tmdb, tvdb, norm, year, has_file, monitored, available, "
                          "title, imdb) VALUES(?,?,?,?,?,?,?,?,?,?,?)", rows)
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        LIB["at"] = time.time()
    except Exception as e:
        log("Library refresh failed: %s" % e, "warn")
        LIB["at"] = time.time() - 3000
        wanted = False
    finally:
        LIB["running"] = False
    if wanted:
        refresh_wanted()


# ------------------------------------------------------------------ wanted

WANTED = {"running": False, "at": 0, "error": ""}


def arr_work(kind, lib_row):
    """The catalog title (merged across providers) matching an arr library entry, if any provider has it."""
    arr, table = ("radarr", "movies") if kind == "movie" else ("sonarr", "series")
    key = link_key(lib_row)
    o = q1("SELECT xid FROM overrides WHERE arr=? AND arr_id=?", arr, key) if key else None
    r = q1("SELECT work FROM %s WHERE id=? AND %s" % (table, live()), o["xid"]) if o else None
    if r and r["work"]:
        return r["work"]
    adult = "" if S().get("show_adult") else " AND adult=0"
    bad = [x["work"] for x in q("SELECT work FROM unlinks WHERE arr=? AND arr_id=?", arr, key)] if key else []
    adult += "".join(" AND work<>'%s'" % w.replace("'", "''") for w in bad)
    r = q1("SELECT work FROM works WHERE kind=? AND mt IS NOT NULL AND mt=?%s LIMIT 1" % adult, kind,
           lib_row["tmdb"]) if lib_row["tmdb"] else None
    if not r and lib_row["norm"]:
        r = q1("SELECT work FROM works WHERE kind=? AND norm=? AND (year IS NULL OR ? IS NULL OR ABS(year-?)<=1)%s "
               "LIMIT 1" % adult, kind, lib_row["norm"], lib_row["year"], lib_row["year"])
    return r["work"] if r else None


def refresh_wanted():
    """Everything Sonarr and Radarr want (monitored, released or aired, no file) that a provider has."""
    if WANTED["running"]:
        return
    WANTED["running"] = True
    try:
        now = time.time()
        rows = []
        if RADARR.ok():
            for m in q("SELECT * FROM arr_lib WHERE kind='movie' AND monitored=1 AND has_file=0 AND available=1"):
                work = arr_work("movie", m)
                if work:
                    rows.append(("movie", m["arr_id"], None, work, None, None, m["title"], None, 1))
        if SONARR.ok():
            missing, page = [], 1
            while page < 60:
                res = SONARR.call("GET", "/api/v3/wanted/missing", {"page": page, "pageSize": 500,
                                                                   "monitored": "true", "sortKey": "airDateUtc",
                                                                   "sortDirection": "descending"}, timeout=120) or {}
                recs = res.get("records") or []
                missing += recs
                if len(recs) < 500 or len(missing) >= (res.get("totalRecords") or 0):
                    break
                page += 1
            by_series = collections.defaultdict(list)
            for e in missing:
                if e.get("monitored", True):
                    by_series[e.get("seriesId")].append(e)
            for sid, eps in by_series.items():
                lib = q1("SELECT * FROM arr_lib WHERE kind='series' AND arr_id=?", sid)
                if not lib:
                    continue
                work = arr_work("series", lib)
                if not work:
                    continue
                have = set()
                for cp in q("SELECT id FROM series INDEXED BY series_work WHERE work=? AND %s" % live(), work):
                    try:
                        ensure_episodes(cp["id"])
                    except Exception as e:
                        log("Episode list for %s failed: %s" % (lib["title"], e), "warn")
                    have.update((r["season"], r["episode"]) for r in
                                q("SELECT season, episode FROM episodes WHERE series_id=?", cp["id"]))
                for e in eps:
                    se, ep = to_int(e.get("seasonNumber")), to_int(e.get("episodeNumber"))
                    rows.append(("series", sid, e.get("id"), work, se, ep, e.get("title") or "",
                                 e.get("airDateUtc") or "", 1 if (se, ep) in have else 0))
        old = {(r["kind"], r["arr_id"], r["arr_ep"]): r for r in q("SELECT * FROM wanted")}
        c = db()
        c.execute("BEGIN")
        try:
            c.execute("DELETE FROM wanted")
            for kind, aid, ep_id, work, se, ep, title, air, avail in rows:
                prev = old.get((kind, aid, ep_id))
                c.execute("INSERT INTO wanted(kind, arr_id, arr_ep, work, season, episode, title, air, available, "
                          "found, searched) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                          (kind, aid, ep_id, work, se, ep, title, air, avail,
                           prev["found"] if prev and prev["available"] == avail else now,
                           prev["searched"] if prev else None))
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        WANTED.update(at=now, error="")
        ex("INSERT OR REPLACE INTO settings(k, v) VALUES('wanted_at', ?)", json.dumps(now))
        n_m = sum(1 for r in rows if r[0] == "movie")
        n_e = sum(1 for r in rows if r[0] == "series" and r[8])
        log("Wanted: %d movies and %d episodes that Radarr and Sonarr want are available" % (n_m, n_e))
        if S().get("wanted_auto_search"):
            fresh_m = [r["arr_id"] for r in q("SELECT arr_id FROM wanted WHERE kind='movie' AND searched IS NULL")]
            fresh_e = [r["arr_ep"] for r in q("SELECT arr_ep FROM wanted WHERE kind='series' AND available=1 AND "
                                              "searched IS NULL")]
            if fresh_m or fresh_e:
                wanted_search(fresh_m, fresh_e, auto=True)
    except Exception as e:
        WANTED["error"] = mask(str(e))
        log("Wanted refresh failed: %s" % e, "warn")
    finally:
        WANTED["running"] = False
        _facet_cache.clear()


def wanted_search(movie_ids, episode_ids, auto=False):
    """Ask Radarr and Sonarr to search; they find VODgrab through the indexer and apply your quality profiles."""
    done = {"movies": 0, "episodes": 0}
    movie_ids = [int(x) for x in movie_ids or []]
    episode_ids = [int(x) for x in episode_ids or []]
    for i in range(0, len(movie_ids), 50):
        chunk = movie_ids[i:i + 50]
        RADARR.post("/api/v3/command", {"name": "MoviesSearch", "movieIds": chunk})
        ex("UPDATE wanted SET searched=? WHERE kind='movie' AND arr_id IN (%s)" % ",".join("?" * len(chunk)),
           time.time(), *chunk)
        done["movies"] += len(chunk)
    for i in range(0, len(episode_ids), 50):
        chunk = episode_ids[i:i + 50]
        SONARR.post("/api/v3/command", {"name": "EpisodeSearch", "episodeIds": chunk})
        ex("UPDATE wanted SET searched=? WHERE kind='series' AND arr_ep IN (%s)" % ",".join("?" * len(chunk)),
           time.time(), *chunk)
        done["episodes"] += len(chunk)
    log("%sAsked Radarr to search %d movies and Sonarr to search %d episodes" % (
        "Automatic: " if auto else "", done["movies"], done["episodes"]))
    return done


def wanted_view():
    shows_ep = collections.OrderedDict()
    movies = []
    rep_of = {}

    def rep(kind, work):
        key = (kind, work)
        if key not in rep_of:
            r = q1("SELECT %s FROM works w WHERE kind=? AND work=?" % WORK_CARD_COLS, kind, work)
            rep_of[key] = card(r) if r else None
        return rep_of[key]

    for r in q("SELECT * FROM wanted ORDER BY kind, title"):
        c = rep(r["kind"], r["work"])
        if not c:
            continue
        if r["kind"] == "movie":
            movies.append({"arr_id": r["arr_id"], "title": r["title"], "card": c, "searched": r["searched"],
                           "found": r["found"]})
        else:
            sh = shows_ep.setdefault(r["arr_id"], {"arr_id": r["arr_id"], "card": c, "episodes": []})
            sh["episodes"].append({"arr_ep": r["arr_ep"], "season": r["season"], "episode": r["episode"],
                                   "title": r["title"], "air": r["air"], "available": bool(r["available"]),
                                   "searched": r["searched"], "found": r["found"]})
    shows = []
    for sh in shows_ep.values():
        sh["episodes"].sort(key=lambda e: (e["season"] or 0, e["episode"] or 0))
        sh["missing"] = len(sh["episodes"])
        sh["available"] = sum(1 for e in sh["episodes"] if e["available"])
        if sh["available"]:
            shows.append(sh)
    shows.sort(key=lambda x: x["card"]["clean"].lower())
    movies.sort(key=lambda x: x["card"]["clean"].lower())
    if not WANTED["at"]:
        r = q1("SELECT v FROM settings WHERE k='wanted_at'")
        WANTED["at"] = json.loads(r["v"]) if r else 0
    return {"movies": movies, "shows": shows, "at": WANTED["at"], "running": WANTED["running"] or LIB["running"],
            "error": WANTED["error"], "auto": bool(S().get("wanted_auto_search")),
            "sonarr": SONARR.ok(), "radarr": RADARR.ok()}


def wanted_count():
    r = q1("SELECT (SELECT COUNT(*) FROM wanted WHERE kind='movie') + (SELECT COUNT(DISTINCT arr_id) FROM wanted "
           "WHERE kind='series' AND available=1) c")
    return r["c"] if r else 0


def movie_row(xid, fetch=True):
    """Movie row with provider video details loaded when available."""
    if fetch:
        try:
            vod_info(xid)
        except Exception as e:
            log("Movie info for %s failed: %s" % (pname(dec(xid)[0]), e), "warn")
    return q1("SELECT * FROM movies WHERE id=?", xid)


PROBE_BG = collections.OrderedDict()


def acquire_slot(pid, deadline):
    """Take one of a provider's connections for a quality check, waiting until the deadline."""
    while True:
        with ENGINE.lock:
            p = PROVIDERS.get(pid)
            if not p or not ENGINE.usable(pid):
                return False
            if ENGINE.active_count(pid) < int(p["max_conn"] or 1):
                ENGINE.probing[pid] += 1
                return True
        if time.time() >= deadline:
            return False
        time.sleep(0.3)


def release_slot(pid):
    with ENGINE.lock:
        ENGINE.probing[pid] -= 1


def probe_fresh(cid):
    r = q1("SELECT at, error FROM probes WHERE id=?", cid)
    if not r:
        return False
    age = time.time() - (r["at"] or 0)
    return age < (86400 if r["error"] else S()["probe_max_age_days"] * 86400)


def probe_missing(cid):
    """True when a recent check found the file missing or unreadable on the provider."""
    r = q1("SELECT at, error FROM probes WHERE id=?", cid)
    return bool(r and r["error"] and time.time() - (r["at"] or 0) < 86400)


def _probe_run(kind, cid):
    table = "movies" if kind == "movie" else "episodes"
    r = q1("SELECT * FROM %s WHERE id=?" % table, cid)
    if not r:
        raise ValueError("Title not found")
    p = prov(dec(cid)[0])
    try:
        out = subprocess.run([FFPROBE, "-v", "error", "-user_agent", p["user_agent"] or DEFAULT_UA,
                              "-rw_timeout", "20000000", "-show_entries",
                              "format=duration,bit_rate,size:stream=codec_type,codec_name,width,height",
                              "-of", "json", stream_url(kind, cid, r["ext"])], capture_output=True, timeout=60)
        data = json.loads(out.stdout or b"{}")
    except subprocess.TimeoutExpired:
        raise ValueError("%s did not answer in time" % p["name"])
    except ValueError:
        raise ValueError("Could not read the file header")
    vids = [x for x in data.get("streams") or [] if x.get("codec_type") == "video"]
    if not vids:
        err = (out.stderr or b"").decode("utf-8", "replace").strip()
        last = mask(err.splitlines()[-1] if err else "no video found")
        if re.search(r"\b(40[0-9]|410)\b|not found|forbidden|invalid data found", err, re.I) or (
                not err and out.returncode == 0):
            ex("INSERT OR REPLACE INTO probes(id, at, error) VALUES(?,?,?)", cid, time.time(), last[:300])
        raise ValueError("Could not read the video: %s" % last)
    f = data.get("format") or {}
    v = vids[0]
    br = to_int(f.get("bit_rate"))
    ex("INSERT OR REPLACE INTO probes(id, width, height, vcodec, duration, bitrate, size, at, error) "
       "VALUES(?,?,?,?,?,?,?,?,NULL)", cid, to_int(v.get("width")), to_int(v.get("height")), v.get("codec_name"),
       to_int(f.get("duration")), br // 1000 if br else None, to_int(f.get("size")), time.time())
    PROBE_BG.pop((kind, cid), None)
    set_qual(table, cid)
    return media_view(q1("SELECT * FROM %s WHERE id=?" % table, cid))


def probe(kind, cid):
    """Read the real video details from the stream header with ffprobe (Check quality button)."""
    if not FFPROBE:
        raise ValueError("Install ffmpeg on the VODgrab host to check quality")
    pid = dec(cid)[0]
    p = prov(pid)
    if not acquire_slot(pid, time.time() + 5):
        raise ValueError("%s is using all its connections right now. Try again when a download finishes."
                         % p["name"])
    try:
        return _probe_run(kind, cid)
    finally:
        release_slot(pid)


# ---- watching a title in the browser

PLAY = {}  # session id -> {"pid", "kind", "cid", "ext", "stop": Event, "busy": bool, "last": time}
_play_lock = threading.Lock()
PLAY_TYPES = {"mp4": "video/mp4", "m4v": "video/mp4", "mov": "video/mp4", "mkv": "video/webm", "webm": "video/webm",
              "ts": "video/mp2t", "avi": "video/x-msvideo"}


def play_start(kind, cid):
    """Reserve one of the provider's connections for the player. Every request of this player shares it, so
    skipping around never needs a second connection."""
    table = "movies" if kind == "movie" else "episodes"
    r = q1("SELECT id, ext FROM %s WHERE id=?" % table, cid)
    if not r:
        raise ValueError("Title not found")
    pid = dec(cid)[0]
    p = PROVIDERS.get(pid)
    if not p:
        raise ValueError("That provider is disabled")
    if not acquire_slot(pid, time.time() + 4):
        raise ValueError("%s has no free connection right now (it allows %s, and downloads or another player are "
                         "using them). Try again when one finishes." % (p["name"], p["max_conn"]))
    sid = secrets.token_hex(8)
    with _play_lock:
        PLAY[sid] = {"pid": pid, "kind": kind, "cid": cid, "ext": r["ext"], "stop": threading.Event(), "busy": False,
                     "last": time.time()}
    log("Playing %s from %s in the browser" % (cid % SCALE, p["name"]))
    return {"session": sid, "type": PLAY_TYPES.get((r["ext"] or "").lower(), "video/mp4")}


def play_stop(sid):
    with _play_lock:
        sess = PLAY.pop(sid, None)
    if sess:
        sess["stop"].set()
        release_slot(sess["pid"])


def play_reaper():
    """Give the connection back when a player has been closed without saying so."""
    while True:
        time.sleep(10)
        for sid, sess in list(PLAY.items()):
            if not sess["busy"] and time.time() - sess["last"] > 30:
                play_stop(sid)


def ensure_probed(items, budget=None, force=False):
    """Check every listed copy that has no recent check, in parallel, within the time budget.
    Anything not finished in time is checked in the background so the next search is accurate."""
    if not FFPROBE or not (force or S()["probe_on_search"]):
        return
    todo = collections.deque(x for x in dict.fromkeys(items) if not probe_fresh(x[1]))
    if not todo:
        return
    deadline = time.time() + (budget or S()["probe_budget"])
    pids = {dec(c)[0] for _, c in todo}
    width = max(1, min(len(todo), 8, sum(int(PROVIDERS[p]["max_conn"] or 1) for p in pids if p in PROVIDERS)))
    lock = threading.Lock()

    def worker():
        while True:
            with lock:
                if not todo:
                    return
                kind, cid = todo.popleft()
            pid = dec(cid)[0]
            if time.time() >= deadline or not acquire_slot(pid, deadline):
                PROBE_BG[(kind, cid)] = time.time()
                continue
            try:
                _probe_run(kind, cid)
            except Exception as e:
                log("Quality check failed for %s on %s: %s" % (cid % SCALE, pname(pid), e), "warn")
            finally:
                release_slot(pid)

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(width)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(max(0.1, deadline - time.time()))
    with lock:
        while todo:
            PROBE_BG[todo.popleft()] = time.time()
    done = sum(1 for k, c in items if probe_fresh(c))
    if not force:
        log("Quality checks for search: %d of %d copies checked" % (done, len(set(items))))


def probe_open(items):
    """Quality checks when a title is opened: every listed copy without a recent check, using free connections
    only, for up to 40 seconds. Returns what is known now for each copy."""
    if not FFPROBE:
        return {}
    ok = {"movie": bool(S().get("probe_on_open")), "episode": S().get("probe_series_open", "season") != "off"}
    pairs = [(k, int(c)) for k, c in items or [] if ok.get(k)][:60]
    ensure_probed(pairs, budget=40, force=True)
    out = {}
    for kind, cid in pairs:
        r = q1("SELECT * FROM %s WHERE id=?" % ("movies" if kind == "movie" else "episodes"), cid)
        if r:
            mv = media_view(r)
            if probe_missing(cid):
                mv["missing"] = (q1("SELECT error FROM probes WHERE id=?", cid) or {"error": ""})["error"]
            mv["pending"] = not probe_fresh(cid)
            out[str(cid)] = mv
    return out


def probe_background():
    """Finish quality checks that did not fit in a search's time budget, using free connections only."""
    while True:
        time.sleep(5)
        try:
            if not FFPROBE:
                continue
            for key in list(PROBE_BG)[:20]:
                kind, cid = key
                if probe_fresh(cid):
                    PROBE_BG.pop(key, None)
                    continue
                pid = dec(cid)[0]
                if pid not in PROVIDERS:
                    PROBE_BG.pop(key, None)
                    continue
                waiting = q1("SELECT 1 FROM jobs WHERE status IN ('queued','retry_wait') AND prov=? AND next_at<=?",
                             pid, time.time())
                if waiting or not acquire_slot(pid, time.time()):
                    continue
                try:
                    _probe_run(kind, cid)
                except Exception as e:
                    log("Background quality check failed: %s" % e, "warn")
                    PROBE_BG.pop(key, None)
                finally:
                    release_slot(pid)
        except Exception:
            log(traceback.format_exc(), "error")


# ------------------------------------------------------------------ schedules


def prev_slot(t, hours):
    d = dt.datetime.fromtimestamp(t)
    midnight = d.replace(hour=0, minute=0, second=0, microsecond=0)
    if hours < 24:
        n = int((d - midnight).total_seconds() // (hours * 3600))
        return (midnight + dt.timedelta(hours=n * hours)).timestamp()
    days = hours // 24
    day = midnight.date()
    while (day - REF_MONDAY).days % days:
        day -= dt.timedelta(days=1)
    return dt.datetime.combine(day, dt.time()).timestamp()


def next_slot(t, hours):
    p = dt.datetime.fromtimestamp(prev_slot(t, hours))
    if hours < 24:
        nxt = p + dt.timedelta(hours=hours)
        if nxt.date() != p.date():
            nxt = nxt.replace(hour=0)
        return nxt.timestamp()
    return dt.datetime.combine(p.date() + dt.timedelta(days=hours // 24), dt.time()).timestamp()


def _mins(v):
    h, m = str(v).split(":")
    return int(h) * 60 + int(m)


def window_state(t=None):
    s = S()
    base = int(s["concurrency"]) or 999
    if not s["schedule_enabled"] or not s["windows"]:
        return True, base
    d = dt.datetime.fromtimestamp(t or time.time())
    mins = d.hour * 60 + d.minute
    today, yday = DAYS[d.weekday()], DAYS[(d.weekday() - 1) % 7]
    for w in s["windows"]:
        st, en = _mins(w["start"]), _mins(w["end"])
        if en == 23 * 60 + 59:
            en = 1440
        conc = int(w.get("concurrency") or 0) or base
        days = w.get("days") or DAYS
        if st < en:
            if today in days and st <= mins < en:
                return True, conc
        elif st > en:
            if (today in days and mins >= st) or (yday in days and mins < en):
                return True, conc
    return False, 0


def scheduler():
    last = prev_slot(time.time(), S()["sync_interval_hours"])
    threading.Thread(target=refresh_library, daemon=True).start()
    if q1("SELECT 1 FROM settings WHERE k='needs_resync'") or any(
            not q1("SELECT 1 FROM movies WHERE prov=? UNION SELECT 1 FROM series WHERE prov=?", p["id"], p["id"])
            for p in active_providers()):
        threading.Thread(target=run_sync, args=("startup",), daemon=True).start()
    while True:
        time.sleep(15)
        try:
            h = S()["sync_interval_hours"]
            if SCHED["reset"]:
                SCHED["reset"] = False
                last = prev_slot(time.time(), h)
                continue
            if FOLDER_ERRORS and int(time.time()) % 300 < 15:
                ensure_dirs()
            if int(time.time()) % 600 < 15:
                import_checks()
            slot = prev_slot(time.time(), h)
            if slot > last:
                last = slot
                if active_providers():
                    threading.Thread(target=run_sync, args=("schedule",), daemon=True).start()
            else:
                r = q1("SELECT v FROM settings WHERE k='recheck_at'")
                at = json.loads(r["v"]) if r else 0
                if at and time.time() >= at and not SYNC["running"] and active_providers():
                    ex("INSERT OR REPLACE INTO settings(k, v) VALUES('recheck_at', '0')")
                    r = q1("SELECT v FROM settings WHERE k='recheck_provs'")
                    only = [int(x) for x in json.loads(r["v"])] if r else []
                    threading.Thread(target=run_sync, args=("recheck", only), daemon=True).start()
        except Exception:
            log(traceback.format_exc(), "error")


# ------------------------------------------------------------------ jobs


def job(jid):
    return q1("SELECT * FROM jobs WHERE id=?", jid)


def upd(jid, **kw):
    kw["updated"] = time.time()
    cols = ", ".join("%s=?" % k for k in kw)
    ex("UPDATE jobs SET %s WHERE id=?" % cols, *(list(kw.values()) + [jid]))


def jmeta(j):
    try:
        return json.loads(j["meta"] or "{}")
    except ValueError:
        return {}


def new_job(source, arr, kind, xid, ext, release, label, meta, force=False, sources=None, reuse=None):
    """Queue a download. With reuse (an earlier completed job), the new job points at that job's file instead."""
    sources = [list(x) for x in (sources or [(xid, ext)])]
    xid, ext = sources[0]
    same = q1("SELECT * FROM jobs WHERE source=? AND kind=? AND release=? AND status IN (%s)"
              % ",".join("?" * len(ACTIVE)), source, kind, release, *ACTIVE)
    if same:
        if force:
            upd(same["id"], force=1)
        return same
    r = q1("SELECT MAX(pos) AS p FROM jobs")
    pos = ((r["p"] or 0) + 1)
    nzo = "SABnzbd_nzo_" + secrets.token_hex(6)
    t = time.time()
    status, path, total, finished = "queued", "", 0, None
    if reuse:
        status, path, total, finished = "completed", reuse["path"], reuse["total"] or 0, t
        meta = dict(meta or {}, probe=jmeta(reuse).get("probe"), reused=reuse["id"])
    jid = ex("INSERT INTO jobs(nzo, source, arr, kind, xid, ext, release, label, status, pos, force, meta, created, "
             "updated, sources, prov, path, total, done, started, finished) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
             nzo, source, arr, kind, xid, ext, release, label, status, pos, 1 if force else 0, json.dumps(meta or {}),
             t, t, json.dumps(sources), dec(xid)[0], path, total, total, finished, finished).lastrowid
    if not reuse:
        log("Queued %s (%s, %d source%s)" % (release, source, len(sources), "" if len(sources) == 1 else "s"))
    return job(jid)


def first_then(cid, pairs):
    pairs = [tuple(p) for p in pairs]
    head = [p for p in pairs if p[0] == cid]
    return head + [p for p in pairs if p[0] != cid]


def ep_label(show, e):
    """'S.W.A.T. Exiles S01E01 · Clean Entry' for queue, history and anywhere an episode stands alone."""
    base = "%s S%02dE%02d" % (show, e["season"], e["episode"])
    return base + (" · " + e["title"] if e["title"] else "")


def movie_release(title, year, name, row=None):
    return rel_name(title, year, quality_for(row, name), "WEB-DL", "IPTV")


def episode_release(title, season, episode, name, row=None):
    return rel_name(title, "S%02dE%02d" % (season, episode), quality_for(row, name), "WEB-DL", "IPTV")


def found_tmdb(kind, work):
    """TMDB ID for a title the provider did not tag, when VODgrab found it by name."""
    r = q1("SELECT mt FROM works WHERE kind=? AND work=?", kind, work) if work else None
    return r["mt"] if r else None


def resolve_radarr(m):
    tmdb = m["tmdb"] or found_tmdb("movie", m["work"])
    if not tmdb:
        try:
            info = vod_info(m["id"])
            tmdb = to_int(info.get("tmdb_id") or info.get("tmdb")) or None
        except Exception as e:
            log("Could not load movie info: %s" % e, "warn")
    look = None
    if tmdb:
        found = [x for x in (RADARR.get("/api/v3/movie", tmdbId=tmdb) or []) if str(x.get("tmdbId")) == str(tmdb)]
        if found:
            return found[0]
        try:
            look = RADARR.get("/api/v3/movie/lookup/tmdb", tmdbId=tmdb)
        except ArrError:
            look = None
    if not look:
        term = ("%s %s" % (m["clean"], m["year"] or "")).strip()
        look = best_by_title(RADARR.get("/api/v3/movie/lookup", term=term) or [], m["clean"], m["year"])
    if not look:
        return None
    if look.get("id"):
        return look
    body = dict(look)
    body.update(qualityProfileId=arr_profile("radarr"), rootFolderPath=arr_root("radarr"), monitored=True,
                minimumAvailability="released", addOptions={"searchForMovie": False})
    added = RADARR.post("/api/v3/movie", body)
    log("Added %s to Radarr" % look.get("title"))
    return added


def resolve_sonarr(srow):
    res = SONARR.get("/api/v3/series/lookup", term=srow["clean"]) or []
    look = None
    if srow["tmdb"]:
        look = next((r for r in res if str(r.get("tmdbId")) == str(srow["tmdb"])), None)
    look = look or best_by_title(res, srow["clean"], srow["year"])
    if not look:
        return None
    if look.get("id"):
        return look
    body = dict(look)
    body.update(qualityProfileId=arr_profile("sonarr"), rootFolderPath=arr_root("sonarr"), monitored=True,
                seasonFolder=True, addOptions={"monitor": "none", "searchForMissingEpisodes": False,
                                               "searchForCutoffUnmetEpisodes": False})
    try:
        lp = SONARR.get("/api/v3/languageprofile") or []
        if lp:
            body["languageProfileId"] = lp[0]["id"]
    except ArrError:
        pass
    added = SONARR.post("/api/v3/series", body)
    log("Added %s to Sonarr" % look.get("title"))
    return added


MONITOR_OPTIONS = ("all", "future", "missing", "existing", "firstSeason", "lastSeason", "pilot", "none")


def lookup_movie(m):
    """Radarr's record for a catalog movie: the library entry if present, else its lookup result."""
    tmdb = m["tmdb"] or found_tmdb("movie", m["work"])
    if not tmdb:
        try:
            tmdb = to_int(vod_info(m["id"]).get("tmdb_id")) or None
        except Exception:
            tmdb = None
    if tmdb:
        found = [x for x in (RADARR.get("/api/v3/movie", tmdbId=tmdb) or []) if str(x.get("tmdbId")) == str(tmdb)]
        if found:
            return found[0]
        try:
            look = RADARR.get("/api/v3/movie/lookup/tmdb", tmdbId=tmdb)
            if look:
                return look
        except ArrError:
            pass
    term = ("%s %s" % (m["clean"], m["year"] or "")).strip()
    return best_by_title(RADARR.get("/api/v3/movie/lookup", term=term) or [], m["clean"], m["year"])


def lookup_series(srow):
    """Sonarr's record for a catalog series: the library entry if present, else its lookup result."""
    lib = q1("SELECT arr_id FROM arr_lib WHERE kind='series' AND ((? IS NOT NULL AND tmdb=?) OR (norm=? AND "
             "(year IS NULL OR ? IS NULL OR ABS(year-?)<=1))) LIMIT 1", srow["tmdb"], srow["tmdb"], srow["norm"],
             srow["year"], srow["year"])
    if lib:
        try:
            return SONARR.get("/api/v3/series/%s" % lib["arr_id"])
        except ArrError:
            pass
    mt = srow["tmdb"] or found_tmdb("series", srow["work"])
    mr = meta_row("series", mt)
    if mr and mr["tvdb"]:
        res = SONARR.get("/api/v3/series/lookup", term="tvdb:%s" % mr["tvdb"]) or []
        if res:
            return res[0]
    res = SONARR.get("/api/v3/series/lookup", term=srow["clean"]) or []
    look = next((r for r in res if mt and str(r.get("tmdbId")) == str(mt)), None)
    return look or best_by_title(res, srow["clean"], srow["year"])


def add_to_arr(kind, cid):
    """Add a catalog title to Radarr or Sonarr with the profile, folder, monitoring and search options from
    Settings. Returns (ok, message)."""
    s = S()
    if kind == "movie":
        if not RADARR.ok():
            return False, "Radarr is not connected"
        m = q1("SELECT * FROM movies WHERE id=?", cid)
        if not m:
            return False, "Movie not found"
        look = lookup_movie(m)
        if not look:
            return False, "Radarr could not identify %s" % m["clean"]
        if look.get("id"):
            return True, "%s is already in Radarr" % look.get("title")
        body = dict(look)
        body.update(qualityProfileId=arr_profile("radarr"), rootFolderPath=arr_root("radarr"), monitored=True,
                    minimumAvailability="released", addOptions={"searchForMovie": bool(s["radarr_add_search"])})
        added = RADARR.post("/api/v3/movie", body) or {}
        ex("INSERT INTO arr_lib(kind, arr_id, tmdb, tvdb, norm, year, has_file, monitored, available, title) "
           "VALUES('movie',?,?,NULL,?,?,0,1,1,?)", added.get("id"), to_int(look.get("tmdbId")), norm(look.get("title")),
           to_int(look.get("year")), look.get("title"))
        log("Added %s to Radarr%s" % (look.get("title"), " and started a search" if s["radarr_add_search"] else ""))
        return True, "Added %s to Radarr" % look.get("title")
    if not SONARR.ok():
        return False, "Sonarr is not connected"
    srow = q1("SELECT * FROM series WHERE id=?", cid)
    if not srow:
        return False, "Series not found"
    look = lookup_series(srow)
    if not look:
        return False, "Sonarr could not identify %s" % srow["clean"]
    if look.get("id"):
        return True, "%s is already in Sonarr" % look.get("title")
    mon = s["sonarr_add_monitor"] if s["sonarr_add_monitor"] in MONITOR_OPTIONS else "all"
    body = dict(look)
    body.update(qualityProfileId=arr_profile("sonarr"), rootFolderPath=arr_root("sonarr"), monitored=mon != "none",
                seasonFolder=True, addOptions={"monitor": mon, "searchForMissingEpisodes": bool(s["sonarr_add_search"]),
                                               "searchForCutoffUnmetEpisodes": False})
    try:
        lp = SONARR.get("/api/v3/languageprofile") or []
        if lp:
            body["languageProfileId"] = lp[0]["id"]
    except ArrError:
        pass
    added = SONARR.post("/api/v3/series", body) or {}
    ex("INSERT INTO arr_lib(kind, arr_id, tmdb, tvdb, norm, year, has_file, monitored, available, title) "
       "VALUES('series',?,?,?,?,?,0,?,1,?)", added.get("id"), to_int(look.get("tmdbId")) or srow["tmdb"],
       to_int(look.get("tvdbId")), norm(look.get("title")), to_int(look.get("year")), 1 if mon != "none" else 0,
       look.get("title"))
    log("Added %s to Sonarr%s" % (look.get("title"), " and started a search" if s["sonarr_add_search"] else ""))
    return True, "Added %s to Sonarr" % look.get("title")


def add_many(items):
    out = []
    for it in items or []:
        try:
            ok, msg = add_to_arr("movie" if it.get("kind") == "movie" else "series", int(it.get("id")))
        except (ArrError, ValueError) as e:
            ok, msg = False, str(e)
        out.append({"id": it.get("id"), "ok": ok, "message": mask(msg)})
    if any(r["ok"] for r in out):
        def later():
            time.sleep(20)
            refresh_library()
        threading.Thread(target=later, daemon=True).start()
    return {"results": out, "added": sum(1 for r in out if r["ok"] and r["message"].startswith("Added")),
            "failed": sum(1 for r in out if not r["ok"])}


def enqueue_manual(kind, xid, season=None, force=False, dp=None):
    notes = []
    if kind == "movie":
        m = q1("SELECT * FROM movies WHERE id=?", xid)
        if not m:
            raise ValueError("Movie not found in catalog")
        title, year, meta = m["clean"], m["year"], {}
        if RADARR.ok() and S()["auto_import_manual"]:
            try:
                info = resolve_radarr(m)
                if info:
                    title, year = info.get("title") or title, info.get("year") or year
                    meta = {"tmdb": info.get("tmdbId"), "arr_title": title}
                    ex("INSERT OR REPLACE INTO matches(arr, arr_id, xids, method, score, title, year, checked) "
                       "VALUES('radarr', ?, ?, 'manual', 100, ?, ?, ?)", str(info.get("tmdbId")),
                       json.dumps([xid]), title, year, time.time())
                else:
                    notes.append("Radarr could not identify this movie; it will still download")
            except ArrError as e:
                notes.append(str(e))
        if dp and dp.get("account_id") and dp.get("stream_id"):  # a provider picked inside Dispatcharr
            meta["dp_pref"] = {"cid": xid, "m3u_account_id": to_int(dp["account_id"]), "stream_id": str(dp["stream_id"]),
                               "account": dp.get("account") or ""}
        j = new_job("manual", "radarr", "movie", xid, m["ext"], movie_release(title, year, m["name"], movie_row(xid)),
                    "%s (%s)" % (title, year) if year else title, meta, force,
                    first_then(xid, movie_sources(xid)))
        return [j], notes
    if kind in ("episode", "season", "series"):
        if kind == "episode":
            e = q1("SELECT * FROM episodes WHERE id=?", xid)
            if not e:
                raise ValueError("Episode not found")
            sid, eps = e["series_id"], [e]
        else:
            sid = xid
            ensure_episodes(sid)
            if kind == "season":
                eps = q("SELECT * FROM episodes WHERE series_id=? AND season=? ORDER BY episode", sid, int(season))
            else:
                eps = q("SELECT * FROM episodes WHERE series_id=? ORDER BY season, episode", sid)
        srow = q1("SELECT * FROM series WHERE id=?", sid)
        title, meta = srow["clean"], {}
        if SONARR.ok() and S()["auto_import_manual"]:
            try:
                info = resolve_sonarr(srow)
                if info:
                    title = info.get("title") or title
                    meta = {"tvdb": info.get("tvdbId"), "arr_title": title}
                    ex("INSERT OR REPLACE INTO matches(arr, arr_id, xids, method, score, title, year, checked) "
                       "VALUES('sonarr', ?, ?, 'manual', 100, ?, ?, ?)", str(info.get("tvdbId")),
                       json.dumps([sid]), title, info.get("year"), time.time())
                else:
                    notes.append("Sonarr could not identify this series; files will still download")
            except ArrError as e:
                notes.append(str(e))
        jobs = []
        for e in eps:
            md = dict(meta, season=e["season"], ep=e["episode"])
            jobs.append(new_job("manual", "sonarr", "episode", e["id"], e["ext"],
                                episode_release(title, e["season"], e["episode"], srow["name"], e),
                                ep_label(title, e), md, force,
                                first_then(e["id"], episode_equivalents(e["id"]))))
        return jobs, notes
    raise ValueError("Unknown download kind")


# ------------------------------------------------------------------ download engine


class Stop(Exception):
    pass


class Fatal(Exception):
    pass


class Transient(Exception):
    pass


class Limited(Exception):
    pass


class Flag(threading.Event):
    reason = ""

    def stop(self, reason):
        self.reason = reason
        self.set()


FFPROBE = shutil.which("ffprobe")


def verify(path, total):
    size = os.path.getsize(path)
    if total and size != total:
        return False, "size is %d of %d bytes" % (size, total)
    if size < MB:
        return False, "file is only %d bytes" % size
    if not FFPROBE:
        return True, "size check only (ffprobe not installed)"
    try:
        out = subprocess.run([FFPROBE, "-v", "error", "-show_entries",
                              "format=duration:stream=codec_type,codec_name,height", "-of", "json", path],
                             capture_output=True, timeout=180)
        data = json.loads(out.stdout or b"{}")
    except (subprocess.TimeoutExpired, ValueError, OSError) as e:
        return False, "ffprobe failed: %s" % e
    dur = float((data.get("format") or {}).get("duration") or 0)
    vids = [s for s in data.get("streams") or [] if s.get("codec_type") == "video"]
    if not vids:
        return False, "no video stream found"
    if dur <= 0:
        return False, "no duration found"
    v = vids[0]
    return True, "%sp %s, %d min" % (v.get("height") or "?", v.get("codec_name") or "?", int(dur // 60))


class AuthFail(Exception):
    pass


def check_account(pid):
    try:
        info = (xt_api(pid, timeout=20) or {}).get("user_info") or {}
    except Exception:
        return "unknown"
    if str(info.get("auth")) == "0" or str(info.get("status", "Active")).lower() not in ("active", ""):
        return "bad"
    if to_int(info.get("max_connections")) and (to_int(info.get("active_cons")) or 0) >= to_int(info.get("max_connections")):
        return "full"
    return "ok"


def job_sources(j):
    try:
        src = json.loads(j["sources"] or "[]")
    except (ValueError, TypeError):
        src = []
    return src or [[j["xid"], j["ext"]]]


class Engine:
    def __init__(self):
        self.lock = threading.Lock()
        self.workers = {}
        self.progress = {}
        self.probing = collections.Counter()
        self.gap_until = 0
        self.prov_errors = {}
        self.cool = {}

    @property
    def banner(self):
        return " ".join(list(self.prov_errors.values()) + list(FOLDER_ERRORS.values()))

    def active_count(self, pid):
        return sum(1 for w in list(self.workers.values()) if w[2] == pid) + self.probing[pid]

    def start(self):
        ex("UPDATE jobs SET status='queued' WHERE status IN ('downloading','verifying')")
        for r in q("SELECT id FROM jobs WHERE status='importing'"):
            threading.Thread(target=self._import, args=(r["id"],), daemon=True).start()
        threading.Thread(target=self.loop, daemon=True).start()

    def loop(self):
        while True:
            try:
                self.tick()
            except Exception:
                log(traceback.format_exc(), "error")
            time.sleep(2)

    def usable(self, pid, t=None):
        p = PROVIDERS.get(pid)
        return bool(p and p["enabled"] and p["url"] and pid not in self.prov_errors)

    def viable(self, j):
        """True while some source is untried on a provider that still exists (disabled ones just wait)."""
        bad = set(jmeta(j).get("bad") or [])
        return any(cid not in bad and dec(cid)[0] in PROVIDERS for cid, _ in job_sources(j))

    def pick(self, j, per, t):
        bad = set(jmeta(j).get("bad") or [])
        for cid, ext in job_sources(j):
            pid = dec(cid)[0]
            if cid in bad or not self.usable(pid) or self.cool.get(pid, 0) > t:
                continue
            if per.get(pid, 0) >= int(PROVIDERS[pid]["max_conn"] or 1):
                continue
            return cid, ext, pid
        return None

    def tick(self):
        s = S()
        t = time.time()
        in_win, conc = window_state(t)
        paused = bool(s["paused"])
        with self.lock:
            for jid, (th, flag, pid) in list(self.workers.items()):
                j = job(jid)
                if j and not j["force"] and (paused or (not in_win and s["on_window_end"] == "pause")):
                    flag.stop("pause")
            if t < self.gap_until:
                return
            normal_limit = conc if (in_win and not paused) else 0
            forced_limit = int(s["concurrency"]) or 999
            running = len(self.workers)
            per = collections.Counter(w[2] for w in self.workers.values()) + self.probing
            rows = q("SELECT * FROM jobs WHERE status IN ('queued','retry_wait') AND next_at<=? "
                     "ORDER BY force DESC, pos ASC, id ASC LIMIT 200", t)
            for r in rows:
                if r["id"] in self.workers:
                    continue
                if not self.viable(r):
                    self.fail(r["id"], r["error"] or "No enabled provider has this title")
                    continue
                limit = forced_limit if r["force"] else normal_limit
                if running >= limit:
                    continue
                got = self.pick(r, per, t)
                if not got:
                    continue
                cid, ext, pid = got
                flag = Flag()
                th = threading.Thread(target=self.run, args=(r["id"], flag, cid, ext, pid), daemon=True)
                self.workers[r["id"]] = (th, flag, pid)
                per[pid] += 1
                upd(r["id"], status="downloading", started=time.time(), prov=pid)
                th.start()
                running += 1

    def stop_job(self, jid, reason):
        with self.lock:
            w = self.workers.get(jid)
        if w:
            w[1].stop(reason)
            return True
        return False

    def run(self, jid, flag, cid, ext, pid):
        try:
            self._run(jid, flag, cid, ext, pid)
        except Exception as e:
            log(traceback.format_exc(), "error")
            self.fail(jid, "Internal error: %s" % e)
        finally:
            with self.lock:
                self.workers.pop(jid, None)
                self.progress.pop(jid, None)
            s = S()
            lo, hi = sorted([max(0, s["gap_min"]), max(0, s["gap_max"])])
            self.gap_until = time.time() + (random.uniform(lo, hi) if hi else 0)

    def fail(self, jid, msg):
        upd(jid, status="failed", error=mask(msg), finished=time.time())
        j = job(jid)
        log("Failed %s: %s" % (j["release"] if j else jid, msg), "warn")

    def next_source(self, jid, cid, msg):
        j = job(jid)
        meta = jmeta(j)
        meta["bad"] = sorted(set(meta.get("bad") or []) | {cid})
        upd(jid, meta=json.dumps(meta), attempts=0, waited=0, done=0)
        if self.viable(job(jid)):
            upd(jid, status="queued", next_at=0, error="%s, trying the next provider" % msg)
            log("%s: %s, trying the next provider" % (j["release"], msg), "warn")
        else:
            self.fail(jid, msg)

    def _run(self, jid, flag, cid, ext, pid):
        s = S()
        name = pname(pid)
        j = job(jid)
        url = stream_url(j["kind"], cid, ext)
        pref = jmeta(j).get("dp_pref") or {}
        if pref.get("cid") == cid:  # ask Dispatcharr for the provider copy you picked (it still enforces limits)
            url += "?" + urllib.parse.urlencode({"m3u_account_id": pref["m3u_account_id"], "stream_id": pref["stream_id"]})
        os.makedirs(folder("incomplete"), exist_ok=True)
        part = os.path.join(folder("incomplete"), "job%d.part" % jid)
        meta = jmeta(j)
        if meta.get("part_src") != cid:
            if os.path.exists(part):
                os.remove(part)
            meta["part_src"] = cid
        upd(jid, meta=json.dumps(meta), ext=ext, prov=pid)
        before = os.path.getsize(part) if os.path.exists(part) else 0
        try:
            total = self.fetch(jid, url, part, flag, pid)
        except Stop:
            if flag.reason == "cancel":
                try:
                    os.remove(part)
                except OSError:
                    pass
                j = job(jid)
                if j["source"] == "arr":
                    upd(jid, status="failed", error="Cancelled in VODgrab", finished=time.time())
                else:
                    upd(jid, status="cancelled", error="", finished=time.time())
            else:
                upd(jid, status="queued", error="Paused, will resume")
            return
        except AuthFail as e:
            self.prov_errors[pid] = "%s rejected the account. Check it in Settings, then press Resume." % name
            upd(jid, status="queued", next_at=0, error="%s: %s" % (name, e))
            return
        except Fatal as e:
            return self.next_source(jid, cid, "%s: %s" % (name, e))
        except Limited as e:
            self.cool[pid] = time.time() + 120
            j = job(jid)
            waited = (j["waited"] or 0) + 120
            if waited > s["max_wait_hours"] * 3600:
                return self.next_source(jid, cid, "%s stayed at its connection limit for over %s h"
                                        % (name, s["max_wait_hours"]))
            upd(jid, status="retry_wait", next_at=time.time() + 5, waited=waited,
                error="%s at its connection limit (%s)" % (name, e))
            return
        except Transient as e:
            after = os.path.getsize(part) if os.path.exists(part) else 0
            j = job(jid)
            att = j["attempts"]
            if after - before < 5 * MB:
                att += 1
            if att > s["retries"]:
                return self.next_source(jid, cid, "%s: %s (gave up after %d retries)" % (name, e, s["retries"]))
            backoff = s["retry_backoff"] or [5]
            wait = 10 if att == j["attempts"] else backoff[min(att - 1, len(backoff) - 1)] * 60
            upd(jid, status="retry_wait", attempts=att, next_at=time.time() + wait,
                error="Retry %d of %d on %s: %s" % (max(att, 1), s["retries"], name, e))
            return
        upd(jid, status="verifying", done=total, total=total)
        ok, info = verify(part, total)
        if not ok:
            try:
                os.remove(part)
            except OSError:
                pass
            meta = jmeta(job(jid))
            if meta.get("reverified") != cid:
                meta["reverified"] = cid
                upd(jid, status="queued", meta=json.dumps(meta), done=0,
                    error="Verification failed (%s), downloading again" % info)
                return
            return self.next_source(jid, cid, "%s: verification failed (%s)" % (name, info))
        j = job(jid)
        meta = jmeta(j)
        meta["probe"] = info
        dest = self.place(j, part)
        upd(jid, path=dest, meta=json.dumps(meta), finished=time.time(), error="",
            status="completed" if j["source"] == "arr" else "importing")
        log("Downloaded %s from %s (%s)" % (j["release"], name, info))
        if j["source"] != "arr":
            self.import_manual(jid)

    def place(self, j, part):
        rel, ext = j["release"], j["ext"]
        if j["source"] == "arr":
            dest_dir = os.path.join(folder(j["arr"]), rel)
            if os.path.exists(dest_dir) and os.listdir(dest_dir):
                dest_dir = dest_dir + ".%d" % j["id"]
        else:
            dest_dir = folder("manual_movies" if j["kind"] == "movie" else "manual_tv")
        os.makedirs(dest_dir, exist_ok=True)
        fix_owner(dest_dir)
        dest = os.path.join(dest_dir, "%s.%s" % (rel, ext))
        if os.path.exists(dest):
            dest = os.path.join(dest_dir, "%s.%d.%s" % (rel, j["id"], ext))
        shutil.move(part, dest)
        fix_owner(dest)
        return dest

    def fetch(self, jid, url, part, flag, pid):
        s = S()
        have = os.path.getsize(part) if os.path.exists(part) else 0
        headers = {"User-Agent": PROVIDERS[pid]["user_agent"] or DEFAULT_UA, "Accept": "*/*"}
        if have:
            headers["Range"] = "bytes=%d-" % have
        req = urllib.request.Request(url, headers=headers)
        try:
            resp = urllib.request.urlopen(req, timeout=max(5, int(s["connect_timeout"])))
        except urllib.error.HTTPError as e:
            code = e.code
            if code == 416 and have:
                return have
            if code in (401, 403):
                acct = check_account(pid)
                if acct == "bad":
                    raise AuthFail("account rejected (HTTP %d)" % code)
                if acct == "full":
                    raise Limited("HTTP %d while all connections are in use" % code)
                raise Fatal("title refused (HTTP %d)" % code)
            if code in (404, 410):
                raise Fatal("title not found (HTTP %d)" % code)
            if code in (429, 458):
                raise Limited("HTTP %d" % code)
            if code in (503, 509) and check_account(pid) == "full":
                raise Limited("HTTP %d" % code)
            raise Transient("HTTP %d" % code)
        except (urllib.error.URLError, OSError) as e:
            raise Transient("connection error: %s" % getattr(e, "reason", e))
        with resp:
            status = resp.status
            ctype = resp.headers.get("Content-Type", "")
            if have and status == 200:
                have = 0
            if status == 206:
                m = re.search(r"/(\d+)", resp.headers.get("Content-Range", ""))
                total = int(m.group(1)) if m else have + int(resp.headers.get("Content-Length") or 0)
            else:
                total = int(resp.headers.get("Content-Length") or 0)
            if ("text/html" in ctype or "json" in ctype) and total < MB:
                raise Transient("provider returned a web page instead of video")
            done = have
            upd(jid, total=total, done=done)
            pr = {"done": done, "total": total, "speed": 0}
            self.progress[jid] = pr
            t_last, b_last, t_db = time.time(), done, time.time()
            t_start, b_start = time.time(), done
            with open(part, "ab" if have and status == 206 else "wb") as f:
                fix_owner(part)
                while True:
                    if flag.is_set():
                        upd(jid, done=done)
                        raise Stop()
                    try:
                        chunk = resp.read(256 * 1024)
                    except Exception as e:
                        upd(jid, done=done)
                        raise Transient("connection dropped: %s" % e)
                    if not chunk:
                        break
                    f.write(chunk)
                    done += len(chunk)
                    now = time.time()
                    pr["done"] = done
                    if now - t_last >= 1:
                        pr["speed"] = (done - b_last) / (now - t_last)
                        t_last, b_last = now, done
                    if now - t_db >= 3:
                        upd(jid, done=done)
                        t_db = now
                    lim = S()["speed_limit_kbps"] * 1024
                    if lim > 0:
                        share = lim / max(1, len(self.workers))
                        ahead = (done - b_start) / share - (now - t_start)
                        if ahead > 0:
                            time.sleep(min(ahead, 2))
        upd(jid, done=done)
        if total and done < total:
            raise Transient("connection closed at %d%%" % (done * 100 // total))
        return total or done

    def import_manual(self, jid):
        j = job(jid)
        arr = RADARR if j["kind"] == "movie" else SONARR
        if not S()["auto_import_manual"] or not arr.ok():
            upd(jid, status="completed", error="")
            return
        upd(jid, status="importing", error="")
        threading.Thread(target=self._import, args=(jid,), daemon=True).start()

    def _import(self, jid):
        j = job(jid)
        arr = RADARR if j["kind"] == "movie" else SONARR
        path = j["path"]
        if not path or not os.path.exists(path):
            upd(jid, status="imported" if path else "import_failed", error="" if path else "File missing")
            return
        name = "DownloadedMoviesScan" if j["kind"] == "movie" else "DownloadedEpisodesScan"
        try:
            cmd = arr.post("/api/v3/command", {"name": name, "path": path, "importMode": "Move"}) or {}
            cid, c = cmd.get("id"), cmd
            deadline = time.time() + 900
            while cid and time.time() < deadline:
                time.sleep(3)
                c = arr.get("/api/v3/command/%s" % cid) or {}
                if c.get("status") in ("completed", "failed", "aborted", "cancelled", "orphaned"):
                    break
            if not os.path.exists(path):
                upd(jid, status="imported", error="")
                log("Imported %s into %s" % (j["release"], arr.label))
                return
            reason = self.rejections(arr, path) or c.get("message") or "%s did not import the file" % arr.label
            upd(jid, status="import_failed", error=reason)
            log("Import failed for %s: %s" % (j["release"], reason), "warn")
        except ArrError as e:
            upd(jid, status="import_failed", error=str(e))

    def rejections(self, arr, path):
        try:
            items = arr.get("/api/v3/manualimport", folder=os.path.dirname(path), filterExistingFiles="false") or []
        except ArrError:
            return ""
        for it in items:
            if os.path.basename(it.get("path") or "") == os.path.basename(path):
                return "; ".join(r.get("reason") or "" for r in it.get("rejections") or [])
        return ""


ENGINE = Engine()


def arr_retry(j):
    arr = ARR(j["arr"])
    meta = jmeta(j)
    removed = 0
    bl = arr.get("/api/v3/blocklist", page=1, pageSize=1000) or {}
    for r in (bl.get("records") if isinstance(bl, dict) else bl) or []:
        if r.get("sourceTitle") == j["release"]:
            arr.delete("/api/v3/blocklist/%s" % r["id"])
            removed += 1
    parsed = {}
    try:
        parsed = arr.get("/api/v3/parse", title=j["release"]) or {}
    except ArrError:
        parsed = {}
    if j["arr"] == "radarr":
        mv = (parsed.get("movie") or {}) if parsed.get("movie") else None
        if not (mv and mv.get("id")) and meta.get("tmdb"):
            mv = radarr_movie(tmdb=meta.get("tmdb"), fresh=True)
        if not mv or not mv.get("id"):
            raise ArrError("Movie not found in Radarr")
        arr.post("/api/v3/command", {"name": "MoviesSearch", "movieIds": [mv["id"]]})
    else:
        ids = [e["id"] for e in parsed.get("episodes") or [] if e.get("id")]
        if not ids:
            ser = sonarr_series_by_tvdb(meta.get("tvdb"), fresh=True) if meta.get("tvdb") else None
            if not ser or not ser.get("id"):
                raise ArrError("Series not found in Sonarr")
            eps = arr.get("/api/v3/episode", seriesId=ser["id"]) or []
            ids = [e["id"] for e in eps if e.get("seasonNumber") == meta.get("season")
                   and e.get("episodeNumber") == meta.get("ep")]
        if not ids:
            raise ArrError("Episode not found in Sonarr")
        arr.post("/api/v3/command", {"name": "EpisodeSearch", "episodeIds": ids})
    upd(j["id"], status="retried", hidden=1, error="Handed back to %s for a new search" % arr.label)
    return removed


def job_action(jid, action):
    j = job(jid)
    if not j:
        raise ValueError("Job not found")
    if action == "cancel":
        if not ENGINE.stop_job(jid, "cancel"):
            if j["status"] in ("queued", "retry_wait"):
                if j["source"] == "arr":
                    upd(jid, status="failed", error="Cancelled in VODgrab", finished=time.time())
                else:
                    upd(jid, status="cancelled", finished=time.time())
        return {"ok": True}
    if action == "now":
        r = q1("SELECT MIN(pos) AS p FROM jobs")
        upd(jid, force=1, pos=(r["p"] or 0) - 1, next_at=0)
        return {"ok": True}
    if action in ("up", "down"):
        op, order = ("<", "DESC") if action == "up" else (">", "ASC")
        other = q1("SELECT id, pos FROM jobs WHERE status IN ('queued','retry_wait') AND pos %s ? ORDER BY pos %s LIMIT 1"
                   % (op, order), j["pos"])
        if other:
            upd(jid, pos=other["pos"])
            upd(other["id"], pos=j["pos"])
        return {"ok": True}
    if action == "retry":
        if j["status"] == "import_failed":
            upd(jid, status="importing", error="")
            threading.Thread(target=ENGINE._import, args=(jid,), daemon=True).start()
            return {"ok": True, "message": "Import retried"}
        if j["source"] == "arr" and j["status"] == "failed":
            n = arr_retry(j)
            return {"ok": True, "message": "Cleared %d blocklist entries and started a new search" % n}
        meta = jmeta(j)
        meta.pop("reverified", None)
        meta.pop("bad", None)
        upd(jid, status="queued", attempts=0, waited=0, next_at=0, error="", done=0, meta=json.dumps(meta),
            hidden=0, finished=None)
        return {"ok": True, "message": "Queued again"}
    if action == "delete":
        if j["status"] in ACTIVE and j["status"] != "importing":
            raise ValueError("Cancel the job first")
        if j["source"] == "manual" and j["status"] in ("import_failed", "completed") and j["path"] and \
                os.path.exists(j["path"]) and under(j["path"], "manual_tv", "manual_movies"):
            os.remove(j["path"])
        ex("DELETE FROM jobs WHERE id=?", jid)
        return {"ok": True}
    raise ValueError("Unknown action")


# ------------------------------------------------------------------ newznab

CAPS_XML = """<?xml version="1.0" encoding="UTF-8"?>
<caps><server version="1.0" title="VODgrab" strapline="Xtream VOD for Sonarr and Radarr"/>
<limits max="500" default="100"/>
<registration available="no" open="no"/>
<searching>
<search available="yes" supportedParams="q"/>
<tv-search available="yes" supportedParams="q,season,ep,tvdbid"/>
<movie-search available="yes" supportedParams="q,imdbid,tmdbid"/>
</searching>
<categories>
<category id="2000" name="Movies"><subcat id="2040" name="Movies/HD"/><subcat id="2045" name="Movies/UHD"/><subcat id="2030" name="Movies/SD"/></category>
<category id="5000" name="TV"><subcat id="5040" name="TV/HD"/><subcat id="5045" name="TV/UHD"/><subcat id="5030" name="TV/SD"/></category>
</categories></caps>"""


def nz_error(code, desc):
    return '<?xml version="1.0" encoding="UTF-8"?><error code="%d" description=%s/>' % (code, quoteattr(desc))


def cat_for(kind, quality):
    base = 2000 if kind == "movie" else 5000
    return base + {"2160p": 45, "480p": 30}.get(quality, 40)


def est_size(kind, quality):
    gb = {"2160p": 14, "1080p": 4, "720p": 2, "480p": 1}.get(quality, 4)
    return int(gb * GB if kind == "movie" else gb * GB / 3)


def ep_item(srow, e, title, tvdb=None, src=None):
    qual = quality_for(e, srow["name"])
    size = media_view(e)["size"]
    return {"kind": "episode", "xid": e["id"], "ext": e["ext"], "arr": "sonarr",
            "title": episode_release(title, e["season"], e["episode"], srow["name"], e),
            "label": ep_label(title, e),
            "pub": e["added"] or srow["modified"] or time.time(), "cat": cat_for("episode", qual),
            "size": size or est_size("episode", qual), "tvdb": to_int(tvdb), "season": e["season"],
            "ep": e["episode"], "src": src or [[e["id"], e["ext"]]]}


def movie_item(m, title, year, tmdb=None, imdb=None, src=None, fetch=False):
    if fetch:
        m = movie_row(m["id"]) or m
    qual = quality_for(m, m["name"])
    size = media_view(m)["size"]
    return {"kind": "movie", "xid": m["id"], "ext": m["ext"], "arr": "radarr",
            "title": movie_release(title, year, m["name"], m), "label": "%s (%s)" % (title, year) if year else title,
            "pub": m["added"] or time.time(), "cat": cat_for("movie", qual), "size": size or est_size("movie", qual),
            "tmdb": to_int(tmdb or m["tmdb"]), "imdb": imdb, "src": src or [[m["id"], m["ext"]]]}


def episodes_for(sid, season, ep):
    try:
        ensure_episodes(sid)
    except Exception as e:
        log("Episode list for series %s failed: %s" % (sid, e), "warn")
    sql, args = "SELECT * FROM episodes WHERE series_id=?", [sid]
    if season is not None:
        sql += " AND season=?"
        args.append(season)
    if ep is not None:
        sql += " AND episode=?"
        args.append(ep)
    return q(sql + " ORDER BY season, episode", *args)


def split_by_quality(pairs, name_of):
    """Group copies of one title by their real quality, dropping copies whose file is missing.
    pairs: (row, source_name) best provider first. Returns [(quality, [rows])] best quality first."""
    order = {"2160p": 0, "1080p": 1, "720p": 2, "480p": 3}
    out = collections.OrderedDict()
    for row, name in pairs:
        if probe_missing(row["id"]):
            continue
        out.setdefault(quality_for(row, name), []).append(row)
    return sorted(out.items(), key=lambda kv: order.get(kv[0], 9))


def grouped_episodes(sids, season, ep, title_fn, tvdb=None, check=True):
    """One release per episode and real quality, with every provider's copy of that quality as a source."""
    groups = collections.OrderedDict()
    for sid in sorted(sids, key=prio):
        srow = q1("SELECT * FROM series WHERE id=?", sid)
        if not srow:
            continue
        for e in episodes_for(sid, season, ep):
            groups.setdefault((e["season"], e["episode"]), []).append((srow, e))
    if check:
        ensure_probed([("episode", e["id"]) for g in groups.values() for _, e in g])
    items = []
    for key in sorted(groups):
        srows = {e["id"]: srow for srow, e in groups[key]}
        for qual, rows in split_by_quality([(e, srow["name"]) for srow, e in groups[key]], None):
            srow = srows[rows[0]["id"]]
            items.append(ep_item(srow, rows[0], title_fn(srow), tvdb, [[r["id"], r["ext"]] for r in rows]))
    return items


def grouped_movies(ids, title_fn, check=True):
    """One release per distinct movie and real quality, with every provider's copy of that quality as a source."""
    groups, seen = [], set()
    for mid in sorted(ids, key=prio):
        if mid in seen:
            continue
        same = [i for i in equivalents("movie", mid) if i not in seen]
        seen.update(same)
        groups.append(same)
    if check:
        ensure_probed([("movie", i) for g in groups for i in g])
    items = []
    for same in groups:
        rows = [r for r in (movie_row(i) for i in same) if r]
        if not rows:
            continue
        title, year, tmdb, imdb = title_fn(rows[0])
        for qual, qrows in split_by_quality([(r, r["name"]) for r in rows], None):
            items.append(movie_item(qrows[0], title, year, tmdb, imdb, [[r["id"], r["ext"]] for r in qrows]))
    return items


def tv_search(qs):
    season, ep = to_int(qs.get("season")), to_int(qs.get("ep"))
    tvdb = to_int(qs.get("tvdbid"))
    if tvdb:
        ids, info = match("sonarr", tvdb, lambda: sonarr_series_by_tvdb(tvdb))
        return grouped_episodes(ids, season, ep, lambda srow: (info or {}).get("title") or srow["clean"], tvdb)
    term = (qs.get("q") or "").strip()
    if term:
        ids, _, _ = find_catalog("series", [term], None, None)
        if ids:
            ids = sorted(set(ids) | set(equivalents("series", ids[0])), key=prio)
        return grouped_episodes(ids, season, ep, lambda srow: srow["clean"])
    items = []
    for srow in q("SELECT * FROM series WHERE %s ORDER BY modified DESC LIMIT 10" % live()):
        eps = episodes_for(srow["id"], None, None)
        items += [ep_item(srow, e, srow["clean"]) for e in list(eps)[-3:]]
    return items


def movie_search(qs):
    tmdb, imdb = to_int(qs.get("tmdbid")), qs.get("imdbid")
    if tmdb or imdb:
        key = str(tmdb) if tmdb else imdb_tt(imdb)
        ids, info = match("radarr", key, lambda: radarr_movie(tmdb, imdb), tmdb_hint=tmdb)
        info = info or {}
        return grouped_movies(ids, lambda m: (info.get("title") or m["clean"], info.get("year") or m["year"],
                                              info.get("tmdbId") or tmdb, imdb))
    term = (qs.get("q") or "").strip()
    if term:
        y = None
        mm = re.search(r"\b((?:19|20)\d{2})\s*$", term)
        if mm:
            y, term = int(mm.group(1)), term[:mm.start()].strip()
        ids, _, _ = find_catalog("movie", [term], y, None)
        return grouped_movies(ids, lambda m: (m["clean"], m["year"], None, None))
    return [movie_item(m, m["clean"], m["year"])
            for m in q("SELECT * FROM movies WHERE %s ORDER BY added DESC LIMIT 100" % live())]


def newznab_search(t, qs):
    cats = {c for c in (qs.get("cat") or "").split(",") if c}
    tv_ok = not cats or any(c.startswith("5") for c in cats)
    mv_ok = not cats or any(c.startswith("2") for c in cats)
    if t in ("tvsearch", "tv"):
        return tv_search(qs)
    if t == "movie":
        return movie_search(qs)
    if t == "search":
        return (tv_search(qs) if tv_ok else []) + (movie_search(qs) if mv_ok else [])
    return []


def rss_xml(items, host, total, off):
    key = S()["api_key"]
    out = ['<?xml version="1.0" encoding="UTF-8"?>',
           '<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom" '
           'xmlns:newznab="http://www.newznab.com/DTD/2010/feeds/attributes/">',
           "<channel><title>VODgrab</title><description>VODgrab</description><link>%s</link>" % xesc(host),
           '<newznab:response offset="%d" total="%d"/>' % (off, total)]
    for it in items:
        payload = b64e({k: it[k] for k in ("kind", "xid", "ext", "title", "arr", "tvdb", "season", "ep", "tmdb", "label",
                                           "src") if it.get(k) is not None})
        link = "%s/nzb/%s.nzb?apikey=%s" % (host, payload, key)
        attrs = [("category", it["cat"] // 1000 * 1000), ("category", it["cat"]), ("size", it["size"])]
        if it.get("tvdb"):
            attrs.append(("tvdbid", it["tvdb"]))
        if it.get("season") is not None:
            attrs += [("season", "S%02d" % it["season"]), ("episode", "E%02d" % it["ep"])]
        if it.get("tmdb"):
            attrs.append(("tmdbid", it["tmdb"]))
        if it.get("imdb"):
            attrs.append(("imdb", str(it["imdb"]).replace("tt", "")))
        out.append("<item><title>%s</title><guid isPermaLink=\"false\">vodgrab-%s-%s</guid><link>%s</link>"
                   "<comments>%s/</comments><pubDate>%s</pubDate><category>%d</category><size>%d</size>"
                   "<description>%s</description><enclosure url=%s length=\"%d\" type=\"application/x-nzb\"/>%s</item>"
                   % (xesc(it["title"]), it["kind"], it["xid"], xesc(link), xesc(host),
                      email.utils.formatdate(float(it["pub"] or time.time()), usegmt=True), it["cat"], it["size"],
                      xesc(it.get("label") or ""), quoteattr(link), it["size"],
                      "".join('<newznab:attr name="%s" value=%s/>' % (n, quoteattr(str(v))) for n, v in attrs)))
    out.append("</channel></rss>")
    return "\n".join(out)


def nzb_doc(payload):
    p = b64d(payload)
    title = xesc(p.get("title") or "vodgrab")
    return """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
<head><meta type="name">%s</meta><meta type="vodgrab">VODGRAB:%s</meta></head>
<file poster="vodgrab" date="%d" subject="%s"><groups><group>alt.binaries.vodgrab</group></groups>
<segments><segment bytes="1" number="1">vodgrab-%s-%s@vodgrab</segment></segments></file>
</nzb>""" % (title, payload, int(time.time()), title, p.get("kind"), p.get("xid"))


# ------------------------------------------------------------------ sabnzbd emulation

SAB_STATUS = {"queued": "Queued", "retry_wait": "Queued", "downloading": "Downloading", "verifying": "Verifying"}


def sab_add(payload, cat):
    p = b64d(payload)
    arr = cat if cat in ("sonarr", "radarr") else p.get("arr") or ("radarr" if p["kind"] == "movie" else "sonarr")
    meta = {k: p.get(k) for k in ("tvdb", "season", "ep", "tmdb") if p.get(k) is not None}
    src = [[int(c), e] for c, e in (p.get("src") or [[p["xid"], p.get("ext") or "mkv"]])]
    src = [x for x in src if dec(x[0])[0] in PROVIDERS] or src
    prev = earlier_copy(arr, p["kind"], [x[0] for x in src], p["title"])
    j = new_job("arr", arr, p["kind"], src[0][0], src[0][1], p["title"], p.get("label") or p["title"], meta,
                sources=src, reuse=prev)
    if prev and j["status"] == "completed":
        upd(prev["id"], hidden=1)
        log("%s grabbed %s again; reusing the copy downloaded %s instead of downloading it again" % (
            arr.title(), p["title"], time.strftime("%Y-%m-%d %H:%M", time.localtime(prev["finished"] or prev["updated"]))),
            "warn")
        threading.Thread(target=import_check, args=(j["id"], True), daemon=True).start()
    return j["nzo"]


def earlier_copy(arr, kind, xids, release):
    """A finished download of the same stream the arr has not imported yet (its file is still in place)."""
    rows = q("SELECT * FROM jobs WHERE source='arr' AND arr=? AND kind=? AND status='completed' AND path<>'' AND "
             "(release=? OR xid IN (%s)) ORDER BY finished DESC" % ",".join("?" * len(xids)), arr, kind, release, *xids)
    for r in rows:
        if os.path.isfile(r["path"]):
            return r
    return None


def import_check(jid, now=False):
    """Log once why Radarr or Sonarr has not imported a finished download, using the arr's own rejection reasons."""
    j = job(jid)
    if not j or j["status"] != "completed" or not j["path"] or not os.path.isfile(j["path"]):
        return
    meta = jmeta(j)
    if meta.get("import_checked"):
        return
    arr = ARR(j["arr"])
    reason = ENGINE.rejections(arr, j["path"]) if arr.ok() else ""
    meta["import_checked"] = time.time()
    if reason:
        meta["import_reason"] = reason
    ex("UPDATE jobs SET meta=?, error=? WHERE id=?", json.dumps(meta), "%s: %s" % (arr.label, reason) if reason else "", jid)
    log("%s has not imported %s%s: %s" % (
        arr.label, j["release"], "" if now else " after 30 minutes",
        reason or "no reason given; check %s's Activity page" % arr.label), "warn")


def import_checks():
    cutoff = time.time() - 30 * 60
    for r in q("SELECT id FROM jobs WHERE source='arr' AND hidden=0 AND status='completed' AND finished<? AND "
               "finished>? AND meta NOT LIKE '%import_checked%'", cutoff, time.time() - 14 * 86400):
        import_check(r["id"])


def fmt_left(secs):
    secs = int(max(0, secs))
    d, r = divmod(secs, 86400)
    h, r = divmod(r, 3600)
    m, s = divmod(r, 60)
    return ("%d:%02d:%02d:%02d" % (d, h, m, s)) if d else ("%d:%02d:%02d" % (h, m, s))


def sab_queue(cat):
    rows = q("SELECT * FROM jobs WHERE source='arr' AND hidden=0 AND status IN ('queued','retry_wait','downloading',"
             "'verifying') ORDER BY CASE status WHEN 'downloading' THEN 0 WHEN 'verifying' THEN 0 ELSE 1 END, pos")
    slots, kbps = [], 0.0
    for i, j in enumerate(rows):
        if cat and cat != "*" and j["arr"] != cat:
            continue
        pr = ENGINE.progress.get(j["id"]) or {}
        done = pr.get("done", j["done"]) or 0
        total = j["total"] or pr.get("total") or 0
        speed = pr.get("speed") or 0
        kbps += speed / 1024
        left = (total - done) / speed if speed and total else 0
        slots.append({"status": SAB_STATUS.get(j["status"], "Queued"), "index": i, "password": "", "avg_age": "0d",
                      "script": "None", "has_rating": False, "mb": "%.2f" % (total / MB),
                      "mbleft": "%.2f" % (max(0, total - done) / MB), "mbmissing": "0.0",
                      "size": "%.1f MB" % (total / MB), "sizeleft": "%.1f MB" % (max(0, total - done) / MB),
                      "filename": j["release"], "labels": [], "priority": "Normal", "cat": j["arr"], "eta": "unknown",
                      "timeleft": fmt_left(left), "percentage": str(int(done * 100 / total) if total else 0),
                      "nzo_id": j["nzo"], "unpackopts": "3"})
    return {"queue": {"status": "Downloading" if kbps else "Idle", "paused": False, "speedlimit": "0",
                      "speedlimit_abs": "", "noofslots": len(slots), "noofslots_total": len(slots),
                      "kbpersec": "%.2f" % kbps, "speed": "%.1f K" % kbps, "slots": slots,
                      "timeleft": "0:00:00", "mb": "0", "mbleft": "0", "diskspace1": "100", "diskspace2": "100"}}


def sab_history(cat, limit, start=0):
    # Filter by category before the limit: Radarr asks for 60 items, and a busy Sonarr would otherwise push
    # Radarr's downloads out of its view before they are imported, so Radarr forgets them and grabs again.
    where, args = "source='arr' AND hidden=0", []
    if cat and cat != "*":
        where += " AND arr=?"
        args.append(cat)
    rows = q("SELECT * FROM jobs WHERE %s AND status IN ('completed','imported','failed') "
             "ORDER BY updated DESC LIMIT ? OFFSET ?" % where, *(args + [limit, start]))
    # Downloads still waiting for import always stay visible, so the arr keeps tracking them.
    seen = {j["id"] for j in rows}
    rows += [j for j in q("SELECT * FROM jobs WHERE %s AND status='completed' AND finished>? ORDER BY updated DESC"
                          % where, *(args + [time.time() - 14 * 86400])) if j["id"] not in seen]
    slots = []
    for j in rows:
        ok = j["status"] in ("completed", "imported")
        slots.append({"fail_message": "" if ok else (j["error"] or "Download failed"), "bytes": j["total"] or 0,
                      "category": j["arr"], "nzb_name": j["release"] + ".nzb",
                      "download_time": int((j["finished"] or j["updated"]) - (j["started"] or j["created"])),
                      "storage": os.path.dirname(j["path"]) if ok and j["path"] else "",
                      "status": "Completed" if ok else "Failed", "nzo_id": j["nzo"], "name": j["release"],
                      "completed": int(j["finished"] or j["updated"]), "downloaded": j["total"] or 0,
                      "path": os.path.dirname(j["path"]) if ok and j["path"] else ""})
    return {"history": {"noofslots": len(slots), "slots": slots}}


def sab_delete(value, del_files):
    for nzo in (value or "").split(","):
        j = q1("SELECT * FROM jobs WHERE nzo=?", nzo.strip())
        if not j:
            continue
        if j["status"] in ("queued", "retry_wait", "downloading", "verifying"):
            if not ENGINE.stop_job(j["id"], "cancel"):
                upd(j["id"], status="cancelled", finished=time.time())
            upd(j["id"], error="Removed by %s" % j["arr"].title())
        upd(j["id"], hidden=1)
        if del_files and j["path"]:
            d = os.path.dirname(j["path"])
            if under(d, "sonarr", "radarr"):
                shutil.rmtree(d, ignore_errors=True)


def sab_config():
    cats = [{"name": "*", "order": 0, "pp": "3", "script": "None", "dir": "", "priority": 0}]
    for i, c in enumerate(("sonarr", "radarr")):
        cats.append({"name": c, "order": i + 1, "pp": "", "script": "Default", "dir": folder(c),
                     "priority": -100})
    return {"config": {"misc": {"complete_dir": os.path.dirname(folder("sonarr")),
                                "download_dir": folder("incomplete"),
                                "enable_tv_sorting": False, "tv_categories": [], "enable_movie_sorting": False,
                                "movie_categories": [], "enable_date_sorting": False, "date_categories": [],
                                "pre_check": False, "history_retention": "", "history_retention_option": "all",
                                "history_retention_number": 0},
                       "categories": cats, "sorters": []}}


# ------------------------------------------------------------------ arr setup + tests


def set_fields(body, vals):
    for f in body.get("fields") or []:
        if f.get("name") in vals:
            f["value"] = vals[f["name"]]
    return body


def upsert(arr, kind, body):
    existing = [d for d in arr.get("/api/v3/%s" % kind) or [] if d.get("name") == "VODgrab"]
    if existing:
        body["id"] = existing[0]["id"]
        return arr.put("/api/v3/%s/%s" % (kind, body["id"]), body, {"forceSave": "true"})
    return arr.post("/api/v3/%s" % kind, body, {"forceSave": "true"})


def setup_arr(name, ext_url):
    arr = ARR(name)
    s = S()
    ext_url = (ext_url or s["external_url"] or "").rstrip("/")
    if not ext_url:
        raise ValueError("External URL is required")
    save_settings({"external_url": ext_url})
    u = urllib.parse.urlparse(ext_url)
    ssl = u.scheme == "https"
    prefix = u.path.strip("/")
    sch = next((x for x in arr.get("/api/v3/downloadclient/schema") or [] if x.get("implementation") == "Sabnzbd"), None)
    if not sch:
        raise ArrError("%s has no SABnzbd download client type" % arr.label)
    body = set_fields(dict(sch), {"host": u.hostname, "port": u.port or (443 if ssl else 80), "useSsl": ssl,
                                  "urlBase": (prefix + "/sabnzbd").strip("/"), "apiKey": s["api_key"],
                                  "username": "", "password": "", "tvCategory": "sonarr", "movieCategory": "radarr"})
    # Lowest client priority so regular NZBs always go to the real usenet client. The VODgrab indexer is pinned
    # to this client below, so VODgrab releases still land here.
    body.update(name="VODgrab", enable=True, priority=50, removeCompletedDownloads=True, removeFailedDownloads=True)
    dc = upsert(arr, "downloadclient", body)
    sch = next((x for x in arr.get("/api/v3/indexer/schema") or [] if x.get("implementation") == "Newznab"), None)
    if not sch:
        raise ArrError("%s has no Newznab indexer type" % arr.label)
    cats = [5000, 5030, 5040, 5045] if name == "sonarr" else [2000, 2030, 2040, 2045]
    body = set_fields(dict(sch), {"baseUrl": ext_url + "/newznab", "apiPath": "/api", "apiKey": s["api_key"],
                                  "categories": cats, "animeCategories": []})
    body.update(name="VODgrab", enableRss=True, enableAutomaticSearch=True, enableInteractiveSearch=True,
                priority=int(s.get(name + "_indexer_priority") or 25), downloadClientId=(dc or {}).get("id", 0))
    upsert(arr, "indexer", body)
    sch = next((x for x in arr.get("/api/v3/notification/schema") or [] if x.get("implementation") == "Webhook"), None)
    hook = False
    if sch:
        body = set_fields(dict(sch), {"url": "%s/api/webhook?apikey=%s" % (ext_url, s["api_key"]), "method": 1})
        body.update(name="VODgrab", onDownload=True, onUpgrade=True, onGrab=False)
        upsert(arr, "notification", body)
        hook = True
    return {"ok": True, "message": "%s now has the VODgrab indexer (priority %s) pinned to the VODgrab download "
                                   "client (client priority 50, so your other clients are preferred for regular "
                                   "NZBs)%s" % (arr.label, s.get(name + "_indexer_priority") or 25,
                                               " and an import webhook" if hook else "")}


PREFIX_SPLIT = re.compile(r"^\s*([\[\(|{]?[^\]\)|}:\-]{1,14}?[\]\)|}]?)\s*(\||:|\s-\s|\]|\))")


def _example(v):
    """A short, safe example value for the report: no links, no long text."""
    if isinstance(v, dict):
        return "(fields: %s)" % ", ".join("%s=%s" % (k, _example(x)) for k, x in list(v.items())[:12])
    if isinstance(v, list):
        return "(list of %d)" % len(v)
    v = mask(str(v))
    if re.match(r"^https?://", v):
        return "(link)"
    return v[:60] + ("..." if len(v) > 60 else "")


def _fields(items, n=400):
    seen = collections.OrderedDict()
    for it in items[:n]:
        if not isinstance(it, dict):
            continue
        for k, v in it.items():
            ent = seen.setdefault(k, {"present": 0, "example": None})
            if v not in (None, "", [], {}, "0", 0):
                ent["present"] += 1
                if ent["example"] is None:
                    ent["example"] = _example(v)
    total = min(len(items), n)
    return {k: {"filled_in": "%d of %d" % (v["present"], total), "example": v["example"]} for k, v in seen.items()}


def _prefix(name):
    m = PREFIX_SPLIT.match(name or "")
    return m.group(0).strip() if m else None


def catalog_report():
    """Everything needed to tune title cleaning, categories and filters, without credentials or addresses."""
    rep = {"vodgrab_version": VERSION, "created": time.strftime("%Y-%m-%d %H:%M"), "providers": []}
    for p in active_providers():
        pid = p["id"]
        out = {"provider": p["name"]}
        try:
            for kind, cat_action, list_action, cat_key in (
                    ("movies", "get_vod_categories", "get_vod_streams", "category_id"),
                    ("series", "get_series_categories", "get_series", "category_id")):
                cats = xt_api(pid, cat_action) or []
                items = xt_api(pid, list_action, timeout=300) or []
                by_cat = collections.defaultdict(list)
                for it in items:
                    if isinstance(it, dict):
                        by_cat[str(it.get(cat_key))].append(it)
                cat_rows = []
                for c in cats:
                    cid = str(c.get("category_id"))
                    rows = by_cat.get(cid, [])
                    samples = []
                    for it in rows[:15]:
                        clean, y = clean_title(it.get("name"))
                        samples.append({"raw": mask(it.get("name") or ""), "cleaned": clean, "year": y or it.get("year")})
                    cat_rows.append({"id": cid, "name": c.get("category_name"), "items": len(rows),
                                     "parent_id": c.get("parent_id"),
                                     "looks_adult": bool(ADULT_RE.search(c.get("category_name") or "")),
                                     "samples": samples})
                pref_titles = collections.Counter(filter(None, (_prefix(it.get("name")) for it in items
                                                                 if isinstance(it, dict))))
                pref_cats = collections.Counter(filter(None, (_prefix(c.get("category_name")) for c in cats)))
                adult_flag = sum(1 for it in items if isinstance(it, dict) and str(it.get("is_adult")) == "1")
                out[kind] = {"total_items": len(items), "total_categories": len(cats),
                             "items_without_category": len(items) - sum(len(v) for k, v in by_cat.items()
                                                                        if k in {str(c.get("category_id")) for c in cats}),
                             "items_flagged_adult_by_provider": adult_flag,
                             "category_fields": _fields(cats), "item_fields": _fields(items),
                             "top_title_prefixes": pref_titles.most_common(60),
                             "top_category_prefixes": pref_cats.most_common(40),
                             "categories": cat_rows}
                if kind == "movies" and items:
                    ex_ = next((it for it in items if isinstance(it, dict) and it.get("stream_id")), None)
                    if ex_:
                        info = xt_api(pid, "get_vod_info", vod_id=ex_["stream_id"]) or {}
                        out[kind]["movie_info_fields"] = _fields([info.get("info") or {}], 1)
                if kind == "series" and items:
                    eps_fields, done = [], 0
                    for it in items[:40]:
                        if done >= 3:
                            break
                        info = xt_api(pid, "get_series_info", series_id=it.get("series_id")) or {}
                        eps = info.get("episodes") or {}
                        lst = [e for v in (eps.values() if isinstance(eps, dict) else eps) for e in (v or [])
                               if isinstance(e, dict)]
                        if lst:
                            eps_fields += lst[:5]
                            done += 1
                    out[kind]["episode_fields"] = _fields(eps_fields)
                    out[kind]["episode_info_fields"] = _fields([e.get("info") for e in eps_fields
                                                                if isinstance(e.get("info"), dict)])
        except Exception as e:
            out["error"] = mask(str(e))
        rep["providers"].append(out)
    return rep


def test_provider(pid):
    p = prov(pid)
    info = xt_api(pid, timeout=20) or {}
    ui = info.get("user_info") or {}
    if str(ui.get("auth")) == "0":
        return {"ok": False, "message": "%s rejected the username or password" % p["name"]}
    ENGINE.prov_errors.pop(pid, None)
    exp = to_int(ui.get("exp_date"))
    out = {"ok": True, "status": ui.get("status"), "max_connections": ui.get("max_connections"),
           "active_connections": ui.get("active_cons"),
           "expires": dt.datetime.fromtimestamp(exp).strftime("%Y-%m-%d") if exp else "never"}
    m = q1("SELECT id, ext FROM movies WHERE prov=? ORDER BY RANDOM() LIMIT 1", pid)
    if not m:
        out["file_check"] = "Run a sync first to test a file download"
        return out
    try:
        req = urllib.request.Request(stream_url("movie", m["id"], m["ext"]),
                                     headers={"User-Agent": p["user_agent"] or DEFAULT_UA, "Range": "bytes=0-0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            mm = re.search(r"/(\d+)", r.headers.get("Content-Range", ""))
            out["file_check"] = {"http": r.status, "type": r.headers.get("Content-Type"),
                                 "size_gb": round(int(mm.group(1)) / GB, 2) if mm else None,
                                 "resume": r.status == 206 or "bytes" in (r.headers.get("Accept-Ranges") or "")}
    except Exception as e:
        out["file_check"] = {"error": mask(str(e))}
    return out


def test_target(name):
    arr = ARR(name)
    st = arr.get("/api/v3/system/status") or {}
    return {"ok": True, "version": st.get("version"), "message": "%s %s reachable" % (arr.label, st.get("version"))}


# ------------------------------------------------------------------ UI data


def status_view():
    s = S()
    in_win, conc = window_state()
    last = q1("SELECT * FROM sync_runs WHERE finished IS NOT NULL ORDER BY id DESC LIMIT 1")
    return {"version": VERSION, "update": UPDATE["latest"] if UPDATE["latest"] and vtuple(UPDATE["latest"]) >
            vtuple(VERSION) else "", "configured": bool(active_providers()), "paused": s["paused"], "banner": ENGINE.banner, "prov_error": bool(ENGINE.prov_errors), "arrs": {"radarr": RADARR.ok(), "sonarr": SONARR.ok()},
            "ffprobe": bool(FFPROBE), "fts": FTS, "probe_open": bool(FFPROBE and s.get("probe_on_open")),
            "probe_series": s.get("probe_series_open", "season") if FFPROBE else "off",
            "window": {"enabled": s["schedule_enabled"] and bool(s["windows"]), "open": in_win, "concurrency": conc},
            "sync": {"running": SYNC["running"], "phase": SYNC["phase"], "error": SYNC["error"],
                     "last": dict(last) if last else None,
                     "next": next_slot(time.time(), s["sync_interval_hours"]),
                     "recheck": json.loads((q1("SELECT v FROM settings WHERE k='recheck_at'") or {"v": "0"})["v"])},
            "counts": {"movies": q1("SELECT COUNT(*) c FROM movies")["c"],
                       "series": q1("SELECT COUNT(*) c FROM series")["c"],
                       "active": q1("SELECT COUNT(*) c FROM jobs WHERE status IN ('queued','retry_wait','downloading',"
                                    "'verifying','importing')")["c"],
                       "unmatched": q1("SELECT COUNT(*) c FROM unmatched")["c"], "wanted": wanted_count()}}


# ------------------------------------------------------------------ metadata: TMDB, OMDb, TVDB

ISO_LANGS = {"en": "English", "fr": "French", "de": "German", "es": "Spanish", "it": "Italian", "nl": "Dutch",
             "pt": "Portuguese", "pl": "Polish", "tr": "Turkish", "ar": "Arabic", "ru": "Russian", "sv": "Swedish",
             "no": "Norwegian", "nb": "Norwegian", "da": "Danish", "fi": "Finnish", "hi": "Hindi", "el": "Greek",
             "ro": "Romanian", "hu": "Hungarian", "cs": "Czech", "sq": "Albanian", "ko": "Korean", "ja": "Japanese",
             "zh": "Chinese", "cn": "Chinese", "th": "Thai", "id": "Indonesian", "ms": "Malay", "tl": "Tagalog",
             "vi": "Vietnamese", "he": "Hebrew", "fa": "Persian", "uk": "Ukrainian", "ta": "Tamil", "te": "Telugu",
             "ml": "Malayalam", "kn": "Kannada", "bn": "Bengali", "mr": "Marathi", "pa": "Punjabi", "ur": "Urdu",
             "is": "Icelandic", "et": "Estonian", "lv": "Latvian", "lt": "Lithuanian", "sr": "Serbian",
             "hr": "Croatian", "bs": "Bosnian", "sl": "Slovenian", "sk": "Slovak", "bg": "Bulgarian", "ca": "Catalan",
             "eu": "Basque", "gl": "Galician", "ga": "Irish", "cy": "Welsh", "af": "Afrikaans", "sw": "Swahili",
             "xx": "No language"}
CREW_JOBS = {"Director": "Director", "Screenplay": "Writer", "Writer": "Writer", "Story": "Story", "Novel": "Novel",
             "Author": "Novel", "Original Music Composer": "Music", "Director of Photography": "Cinematography",
             "Producer": "Producer", "Creator": "Creator"}
RUNTIME_BUCKETS = {"movie": [("short", 0, 90), ("medium", 90, 120), ("long", 120, 150), ("epic", 150, 9999)],
                   "series": [("short", 0, 30), ("medium", 30, 50), ("long", 50, 9999)]}


class MetaError(Exception):
    pass


class MetaAuth(MetaError):
    pass


class MetaNotFound(MetaError):
    pass


class Rate:
    """Spaces requests out so the background fill stays at the requests per second set in Settings."""

    def __init__(self):
        self.lock = threading.Lock()
        self.next = 0.0

    def wait(self, per_sec):
        with self.lock:
            now = time.time()
            t = max(now, self.next)
            self.next = t + 1.0 / max(0.5, float(per_sec))
        if t > now:
            time.sleep(t - now)


TMDB_RATE = Rate()
META = {"running": False, "phase": "", "done": 0, "todo": 0, "error": "", "auth_bad": False, "force": False,
        "wake": threading.Event(), "rate_seen": 0.0, "backup": "", "backup_running": False}


def pack(d):
    return zlib.compress(json.dumps(d, separators=(",", ":"), ensure_ascii=False).encode(), 6)


def unpack(b):
    if not b:
        return None
    try:
        return json.loads(zlib.decompress(b).decode())
    except (zlib.error, ValueError):
        return None


def kv_get(k, dflt=None):
    r = q1("SELECT v FROM md.meta_kv WHERE k=?", k)
    return json.loads(r["v"]) if r else dflt


def kv_set(k, v):
    ex("INSERT OR REPLACE INTO md.meta_kv(k, v) VALUES(?, ?)", k, json.dumps(v))


def img(path, size="w342"):
    return "%s/%s%s" % (TMDB_IMG, size, path) if path else None


def tmdb_get(path, rate=None, **params):
    key = (S().get("tmdb_key") or "").strip()
    if not key:
        raise MetaAuth("Add a TMDB key in Settings")
    headers = {"Accept": "application/json", "User-Agent": "VODgrab/" + VERSION}
    if len(key) > 40:
        headers["Authorization"] = "Bearer " + key  # the long Read Access Token
    else:
        params["api_key"] = key
    url = TMDB_BASE + path + "?" + urllib.parse.urlencode(params)
    for attempt in range(4):
        if rate:
            TMDB_RATE.wait(rate)
        try:
            _, raw = http(url, headers=headers, timeout=20)
            return json.loads(raw.decode("utf-8", "replace") or "null")
        except urllib.error.HTTPError as e:
            if e.code == 401:
                raise MetaAuth("TMDB did not accept the key")
            if e.code == 404:
                raise MetaNotFound(path)
            if e.code == 429 or e.code >= 500:
                wait = to_float(e.headers.get("Retry-After") if e.headers else None) or 2 * (attempt + 1)
                time.sleep(min(30, wait))
                continue
            raise MetaError("TMDB answered HTTP %s" % e.code)
        except (urllib.error.URLError, OSError, ValueError) as e:
            if attempt == 3:
                raise MetaError("TMDB unreachable: %s" % getattr(e, "reason", e))
            time.sleep(1 + attempt)
    raise MetaError("TMDB is busy, try again later")


def trim_tmdb(kind, j):
    """Keep what VODgrab shows or filters on, a few KB per title instead of the 50 to 150 KB TMDB sends."""
    tv = kind == "series"
    credits = j.get("aggregate_credits") or j.get("credits") or {}
    cast = []
    for c in (credits.get("cast") or [])[:20]:
        ch = c.get("character") or ", ".join(r.get("character") for r in c.get("roles") or [] if r.get("character"))
        cast.append({"n": c.get("name"), "c": (ch or "")[:80], "p": c.get("profile_path")})
    crew, seen = [], set()
    for c in j.get("created_by") or []:
        seen.add((c.get("name"), "Creator"))
        crew.append({"n": c.get("name"), "j": "Creator"})
    for c in credits.get("crew") or []:
        jobs = [c.get("job")] if c.get("job") else [x.get("job") for x in c.get("jobs") or []]
        for jb in jobs:
            role = CREW_JOBS.get(jb)
            if role and (c.get("name"), role) not in seen and len(crew) < 24:
                if role == "Producer" and sum(1 for x in crew if x["j"] == "Producer") >= 4:
                    continue
                seen.add((c.get("name"), role))
                crew.append({"n": c.get("name"), "j": role})
    certs = {}
    if tv:
        for r in (j.get("content_ratings") or {}).get("results") or []:
            if (r.get("rating") or "").strip():
                certs[r.get("iso_3166_1")] = r["rating"].strip()
    else:
        order = {3: 0, 4: 1, 5: 2, 6: 3, 2: 4, 1: 5}
        for r in (j.get("release_dates") or {}).get("results") or []:
            rs = sorted(r.get("release_dates") or [], key=lambda x: order.get(x.get("type"), 9))
            c = next(((x.get("certification") or "").strip() for x in rs if (x.get("certification") or "").strip()), None)
            if c:
                certs[r.get("iso_3166_1")] = c
    vids = [v for v in (j.get("videos") or {}).get("results") or []
            if v.get("site") == "YouTube" and v.get("type") in ("Trailer", "Teaser")]
    vids.sort(key=lambda v: (v.get("type") != "Trailer", not v.get("official"), v.get("iso_639_1") != "en"))
    watch, logos = {}, {}
    for reg, w in ((j.get("watch/providers") or {}).get("results") or {}).items():
        ent = {}
        for typ, short in (("flatrate", "f"), ("free", "a"), ("ads", "a")):
            for p in w.get(typ) or []:
                n = p.get("provider_name")
                if n and n not in ent.get(short, []):
                    ent.setdefault(short, []).append(n)
                    logos[n] = p.get("logo_path")
        if ent:
            watch[reg] = ent
    kw = (j.get("keywords") or {}).get("keywords") or (j.get("keywords") or {}).get("results") or []
    ext = j.get("external_ids") or {}
    recs = []
    for r in ((j.get("recommendations") or {}).get("results") or [])[:20]:
        recs.append({"id": r.get("id"), "t": r.get("title") or r.get("name"),
                     "y": year_of(r.get("release_date") or r.get("first_air_date")), "p": r.get("poster_path")})
    coll = j.get("belongs_to_collection")
    d = {"v": 1, "title": j.get("title") or j.get("name"), "otitle": j.get("original_title") or j.get("original_name"),
         "olang": j.get("original_language"), "tagline": j.get("tagline") or "", "overview": j.get("overview") or "",
         "date": j.get("release_date") or j.get("first_air_date") or "",
         "genres": [g.get("name") for g in j.get("genres") or []],
         "status": j.get("status"), "rating": j.get("vote_average"), "votes": j.get("vote_count"),
         "pop": j.get("popularity"), "poster": j.get("poster_path"), "backdrop": j.get("backdrop_path"),
         "imdb": j.get("imdb_id") or ext.get("imdb_id"), "tvdb": ext.get("tvdb_id"),
         "coll": {"id": coll.get("id"), "name": coll.get("name"), "poster": coll.get("poster_path")} if coll else None,
         "cast": cast, "crew": crew, "kw": [k.get("name") for k in kw][:40], "certs": certs,
         "videos": [{"k": v.get("key"), "n": v.get("name"), "t": v.get("type")} for v in vids[:4]],
         "watch": watch, "logos": logos, "recs": recs,
         "companies": [c.get("name") for c in j.get("production_companies") or []][:6],
         "countries": [c.get("iso_3166_1") for c in j.get("production_countries") or []],
         "homepage": j.get("homepage") or ""}
    if tv:
        last = j.get("last_episode_to_air") or {}
        nxt = j.get("next_episode_to_air") or {}
        d.update(runtime=(j.get("episode_run_time") or [None])[0] or last.get("runtime"),
                 networks=[n.get("name") for n in j.get("networks") or []], last=j.get("last_air_date") or "",
                 nseasons=j.get("number_of_seasons"), neps=j.get("number_of_episodes"), type=j.get("type"),
                 next={"date": nxt.get("air_date"), "s": nxt.get("season_number"), "e": nxt.get("episode_number"),
                       "name": nxt.get("name")} if nxt else None,
                 seasons=[{"n": x.get("season_number"), "name": x.get("name"), "eps": x.get("episode_count"),
                           "date": x.get("air_date"), "poster": x.get("poster_path")} for x in j.get("seasons") or []])
    else:
        d.update(runtime=j.get("runtime"), budget=j.get("budget"), revenue=j.get("revenue"))
    return d


def meta_cols(kind, d):
    """The searchable and filterable columns of a meta row, worked out for the region set in Settings."""
    region = (S().get("meta_region") or "US").upper()
    certs = d.get("certs") or {}
    genres = []
    for g in d.get("genres") or []:
        genres += split_genres(g)
    w = (d.get("watch") or {}).get(region) or {}
    people = [norm(x.get("n")) for x in (d.get("cast") or [])[:20] + (d.get("crew") or [])]
    return {"title": d.get("title"), "year": year_of(d.get("date")),
            "olang": ISO_LANGS.get(d.get("olang") or "", (d.get("olang") or "").upper() or None),
            "runtime": to_int(d.get("runtime")) or None, "cert": certs.get(region) or certs.get("US"),
            "rating": to_float(d.get("rating")), "votes": to_int(d.get("votes")) or 0, "pop": to_float(d.get("pop")),
            "status": d.get("status") if kind == "series" else None, "imdb": d.get("imdb") or None,
            "tvdb": to_int(d.get("tvdb")) or None, "coll": (d.get("coll") or {}).get("id"),
            "genres": tagcol(genres), "keywords": tagcol(norm(k) for k in d.get("kw") or []),
            "people": tagcol(p for p in people if p), "watch": tagcol((w.get("f") or []) + (w.get("a") or [])),
            "poster": img(d.get("poster")), "backdrop": d.get("backdrop")}


META_COLS = ("title", "year", "olang", "runtime", "cert", "rating", "votes", "pop", "status", "imdb", "tvdb", "coll",
             "genres", "keywords", "people", "watch", "poster", "backdrop")


def save_meta(kind, tmdb, d):
    cols = meta_cols(kind, d)
    ex("INSERT INTO md.meta(kind, tmdb, fetched, lang, data, err, %s) VALUES(?,?,?,?,?,NULL,%s) "
       "ON CONFLICT(kind, tmdb) DO UPDATE SET fetched=excluded.fetched, lang=excluded.lang, data=excluded.data, "
       "err=NULL, %s, tvdb=COALESCE(excluded.tvdb, meta.tvdb)"
       % (", ".join(META_COLS), ",".join("?" * len(META_COLS)),
          ", ".join("%s=excluded.%s" % (k, k) for k in META_COLS if k != "tvdb")),
       kind, tmdb, time.time(), S().get("meta_lang"), pack(d), *[cols[k] for k in META_COLS])


def fetch_meta(kind, tmdb, rate=None):
    """Fetch and store one title. A failed refresh keeps what was stored before."""
    tv = kind == "series"
    app = ("aggregate_credits,keywords,content_ratings,external_ids,videos,watch/providers,recommendations" if tv else
           "credits,keywords,release_dates,external_ids,videos,watch/providers,recommendations")
    lang = S().get("meta_lang") or "en-US"
    try:
        j = tmdb_get("/%s/%d" % ("tv" if tv else "movie", int(tmdb)), rate, append_to_response=app, language=lang,
                     include_video_language="%s,en,null" % lang[:2])
    except MetaNotFound:
        ex("INSERT INTO md.meta(kind, tmdb, fetched, err) VALUES(?,?,?,'not found') ON CONFLICT(kind, tmdb) DO UPDATE "
           "SET fetched=excluded.fetched, err=CASE WHEN meta.data IS NULL THEN 'not found' ELSE meta.err END",
           kind, int(tmdb), time.time())
        return None
    if not isinstance(j, dict) or not j.get("id"):
        raise MetaError("TMDB sent something unexpected")
    d = trim_tmdb(kind, j)
    save_meta(kind, int(tmdb), d)
    return d


def tmdb_find(kind, title, year, rate=None):
    """Best TMDB match for a title the provider did not tag with an ID. Returns (tmdb id or 0, score)."""
    tv = kind == "series"
    params = {"query": title, "include_adult": "false", "language": S().get("meta_lang") or "en-US"}
    tries = [dict(params, **({"first_air_date_year" if tv else "year": year} if year else {}))]
    if year:
        tries.append(params)
    best = (0, 0.0)
    for p in tries:
        res = (tmdb_get("/search/%s" % ("tv" if tv else "movie"), rate, **p) or {}).get("results") or []
        for r in res[:10]:
            ry = year_of(r.get("release_date") or r.get("first_air_date"))
            if year and ry and abs(ry - int(year)) > 1:
                continue
            sc = max(score(title, r.get("title") or r.get("name") or ""),
                     score(title, r.get("original_title") or r.get("original_name") or ""))
            if not year or not ry:
                sc -= 4
            if sc > best[1] + 0.01:
                best = (r.get("id"), sc)
        if best[1] >= 92:
            break
    return (best[0], best[1]) if best[1] >= 92 else (0, best[1])


def match_work(kind, work, clean, year, rate=None):
    tid, sc = tmdb_find(kind, clean, year, rate)
    ex("INSERT OR REPLACE INTO md.meta_map(kind, work, tmdb, score, at) VALUES(?,?,?,?,?)", kind, work, tid, sc,
       time.time())
    if tid:
        ex("UPDATE works SET mt=? WHERE kind=? AND work=? AND tmdb IS NULL", tid, kind, work)
    return tid


def meta_row(kind, tmdb):
    return q1("SELECT * FROM md.meta WHERE kind=? AND tmdb=?", kind, tmdb) if tmdb else None


def ensure_meta(kind, work, fetch=True):
    """The metadata for a merged title, fetched right away when the title is opened and nothing is stored yet or it
    is older than two weeks. Returns (tmdb id, data dict or None)."""
    w = q1("SELECT * FROM works WHERE kind=? AND work=?", kind, work) if work else None
    if not w:
        return None, None
    mt = w["tmdb"] or w["mt"]
    key_ok = bool(S().get("tmdb_key")) and not META["auth_bad"]
    if not mt and fetch and key_ok and S().get("meta_match") and not w["adult"]:
        if not q1("SELECT 1 FROM md.meta_map WHERE kind=? AND work=?", kind, work):
            try:
                mt = match_work(kind, work, w["clean"], w["year"]) or None
            except MetaAuth as e:
                META.update(auth_bad=True, error=str(e))
            except MetaError as e:
                log("TMDB lookup for %s failed: %s" % (w["clean"], e), "warn")
    if not mt:
        return None, None
    r = meta_row(kind, mt)
    if fetch and key_ok and (not r or r["data"] is None and time.time() - (r["fetched"] or 0) > 86400
                             or time.time() - (r["fetched"] or 0) > 14 * 86400):
        try:
            fetch_meta(kind, mt)
            apply_meta(None, kind, mt)
            r = meta_row(kind, mt)
        except MetaAuth as e:
            META.update(auth_bad=True, error=str(e))
        except MetaError as e:
            log("TMDB details for %s failed: %s" % (w["clean"], e), "warn")
    return mt, (unpack(r["data"]) if r else None)


def omdb_for(imdb):
    """IMDb, Rotten Tomatoes and Metacritic ratings, only fetched when a title is opened, kept for 30 days."""
    s = S()
    if not imdb or not s.get("omdb_on") or not s.get("omdb_key"):
        return None
    r = q1("SELECT data, fetched FROM md.omdb WHERE imdb=?", imdb)
    old = json.loads(r["data"]) if r else None
    if old and time.time() - r["fetched"] < (30 * 86400 if old.get("Response") == "True" else 3600):
        return omdb_view(old)
    try:
        _, raw = http(OMDB_BASE + "?" + urllib.parse.urlencode({"i": imdb, "apikey": s["omdb_key"]}), timeout=8)
        d = json.loads(raw.decode("utf-8", "replace"))
    except urllib.error.HTTPError as e:
        if e.code != 401:
            return omdb_view(old)
        try:
            d = json.loads(e.read().decode("utf-8", "replace"))
        except ValueError:
            d = {"Response": "False", "Error": "OMDb did not accept the key"}
    except (urllib.error.URLError, OSError, ValueError):
        return omdb_view(old)
    if d.get("Response") != "True" and old and old.get("Response") == "True":
        return omdb_view(old)  # keep good data when OMDb has a bad day or the daily limit is reached
    ex("INSERT OR REPLACE INTO md.omdb(imdb, data, fetched) VALUES(?,?,?)", imdb, json.dumps(d), time.time())
    return omdb_view(d)


def omdb_view(d):
    if not d:
        return None
    if d.get("Response") != "True":
        return {"error": d.get("Error") or "OMDb has nothing for this title"}
    rt = next((x.get("Value") for x in d.get("Ratings") or [] if x.get("Source") == "Rotten Tomatoes"), None)

    def na(v):
        return None if v in (None, "", "N/A") else v
    return {"imdb_rating": na(d.get("imdbRating")), "imdb_votes": na(d.get("imdbVotes")), "rt": rt,
            "metacritic": na(d.get("Metascore")), "awards": na(d.get("Awards")), "rated": na(d.get("Rated")),
            "box_office": na(d.get("BoxOffice"))}


TVDB_TOKEN = {"token": None, "at": 0}


def tvdb_call(path, **params):
    s = S()
    if not s.get("tvdb_key"):
        return None
    if not TVDB_TOKEN["token"] or time.time() - TVDB_TOKEN["at"] > 20 * 86400:
        body = {"apikey": s["tvdb_key"]}
        if s.get("tvdb_pin"):
            body["pin"] = s["tvdb_pin"]
        _, raw = http(TVDB_BASE + "/login", "POST", body, timeout=15)
        TVDB_TOKEN.update(token=(json.loads(raw).get("data") or {}).get("token"), at=time.time())
    url = TVDB_BASE + path + ("?" + urllib.parse.urlencode(params) if params else "")
    try:
        _, raw = http(url, headers={"Authorization": "Bearer %s" % TVDB_TOKEN["token"]}, timeout=15)
    except urllib.error.HTTPError as e:
        if e.code == 401:
            TVDB_TOKEN["token"] = None
        raise
    return json.loads(raw).get("data")


def tvdb_lookup(title, year, imdb):
    """TVDB ID for a show, only used when neither Sonarr nor TMDB know it."""
    try:
        if imdb:
            for x in tvdb_call("/search/remoteid/%s" % imdb) or []:
                sid = (x.get("series") or {}).get("id")
                if sid:
                    return int(sid)
        res = tvdb_call("/search", query=title, type="series", **({"year": year} if year else {})) or []
        for x in res[:5]:
            if score(title, x.get("name") or "") >= 92 or any(score(title, a) >= 95 for a in x.get("aliases") or []):
                return to_int(x.get("tvdb_id"))
    except (urllib.error.URLError, OSError, ValueError, AttributeError) as e:
        log("TVDB lookup for %s failed: %s" % (title, getattr(e, "reason", e)), "warn")
    return None


def season_meta(tmdb, season, status):
    """TMDB episode names, dates, overviews and stills for one season, refreshed every few days while a show airs."""
    r = q1("SELECT data, fetched FROM md.meta_season WHERE tmdb=? AND season=?", tmdb, season)
    ttl = 90 * 86400 if status in ("Ended", "Canceled") else 3 * 86400
    if r and time.time() - r["fetched"] < ttl:
        return unpack(r["data"])
    if not S().get("tmdb_key") or META["auth_bad"]:
        return unpack(r["data"]) if r else None
    try:
        j = tmdb_get("/tv/%d/season/%d" % (tmdb, season), language=S().get("meta_lang") or "en-US")
    except MetaNotFound:
        ex("INSERT OR REPLACE INTO md.meta_season(tmdb, season, data, fetched) VALUES(?,?,?,?)", tmdb, season,
           r["data"] if r else pack([]), time.time())
        return unpack(r["data"]) if r else []
    except MetaError as e:
        log("TMDB season %s of %s failed: %s" % (season, tmdb, e), "warn")
        return unpack(r["data"]) if r else None
    eps = [{"e": e.get("episode_number"), "n": e.get("name") or "", "d": e.get("air_date") or "",
            "o": (e.get("overview") or "")[:600], "s": e.get("still_path"), "r": e.get("runtime"),
            "v": e.get("vote_average")} for e in j.get("episodes") or []]
    ex("INSERT OR REPLACE INTO md.meta_season(tmdb, season, data, fetched) VALUES(?,?,?,?)", tmdb, season, pack(eps),
       time.time())
    return eps


def collection_meta(cid):
    r = q1("SELECT data, fetched FROM md.meta_coll WHERE id=?", cid)
    if r and time.time() - r["fetched"] < 30 * 86400:
        return unpack(r["data"])
    if not S().get("tmdb_key") or META["auth_bad"]:
        return unpack(r["data"]) if r else None
    try:
        j = tmdb_get("/collection/%d" % cid, language=S().get("meta_lang") or "en-US")
    except MetaError:
        return unpack(r["data"]) if r else None
    parts = sorted(({"id": p.get("id"), "t": p.get("title"), "y": year_of(p.get("release_date")),
                     "p": p.get("poster_path"), "d": p.get("release_date") or ""} for p in j.get("parts") or []),
                   key=lambda p: p["d"] or "9999")
    d = {"name": j.get("name"), "parts": parts}
    ex("INSERT OR REPLACE INTO md.meta_coll(id, data, fetched) VALUES(?,?,?)", cid, pack(d), time.time())
    return d


WORK_CARD_COLS = ("w.kind, w.rep, w.work, w.clean, w.norm, w.year, w.tmdb, w.icon, w.rating_n, w.first_seen, w.provs, "
                  "w.np, w.qrank, w.mt, w.poster, w.pop, w.votes")


def works_by_tmdb(kind, ids):
    """Merged titles on the providers for a list of TMDB IDs: {tmdb: works row}."""
    ids = [int(i) for i in ids if i]
    if not ids:
        return {}
    adult = "" if S().get("show_adult") else " AND adult=0"
    rows = q("SELECT %s FROM works w WHERE kind=? AND mt IN (%s)%s" % (WORK_CARD_COLS, ",".join("?" * len(ids)), adult),
             kind, *ids)
    return {r["mt"]: r for r in rows}


def people_view(d):
    by = collections.OrderedDict()
    for c in d.get("crew") or []:
        by.setdefault(c["j"], []).append(c["n"])
    return [{"job": k, "names": v} for k, v in by.items()]


def meta_view(kind, work, fetch=True):
    """Everything the detail page shows from TMDB, OMDb and TVDB, merged with what the providers have."""
    mt, d = ensure_meta(kind, work, fetch)
    if not d:
        return {"tmdb": mt, "has": False, "key": bool(S().get("tmdb_key"))}
    region = (S().get("meta_region") or "US").upper()
    certs = d.get("certs") or {}
    w = (d.get("watch") or {}).get(region) or {}
    logos = d.get("logos") or {}
    cert_region = region if certs.get(region) else "US" if certs.get("US") else None
    out = {"tmdb": mt, "has": True, "title": d.get("title"),
           "otitle": d.get("otitle") if d.get("otitle") != d.get("title") else None, "tagline": d.get("tagline"),
           "overview": d.get("overview"), "date": d.get("date"), "runtime": d.get("runtime"),
           "genres": [{"name": g, "tag": (split_genres(g) or [g])[0]} for g in d.get("genres") or []],
           "cert": certs.get(region) or certs.get("US"), "cert_region": cert_region,
           "status": d.get("status"), "rating": d.get("rating"), "votes": d.get("votes"),
           "poster": img(d.get("poster"), "w500"), "backdrop": img(d.get("backdrop"), "w1280"),
           "imdb": d.get("imdb"), "olang": ISO_LANGS.get(d.get("olang") or "", d.get("olang")),
           "cast": [{"name": c["n"], "character": c["c"], "photo": img(c.get("p"), "w185")} for c in d.get("cast") or []],
           "crew": people_view(d), "keywords": d.get("kw") or [], "videos": d.get("videos") or [],
           "watch": [{"name": n, "logo": img(logos.get(n), "w92")} for n in (w.get("f") or []) + (w.get("a") or [])],
           "watch_link": "https://www.themoviedb.org/%s/%s/watch?locale=%s" % ("tv" if kind == "series" else "movie",
                                                                               mt, region),
           "region": region, "companies": d.get("companies") or [], "networks": d.get("networks") or [],
           "homepage": d.get("homepage"), "next": d.get("next"), "nseasons": d.get("nseasons"),
           "neps": d.get("neps"), "last": d.get("last"), "seasons": d.get("seasons") or []}
    r = meta_row(kind, mt)
    out["tvdb"] = r["tvdb"] if r else None
    out["fetched"] = r["fetched"] if r else None
    recs = works_by_tmdb(kind, [x["id"] for x in d.get("recs") or []])
    seen = {mt}
    out["recs"] = []
    for x in d.get("recs") or []:
        if x["id"] in recs and x["id"] not in seen and len(out["recs"]) < 14:
            seen.add(x["id"])
            out["recs"].append(card(recs[x["id"]]))
    if kind == "movie" and d.get("coll") and fetch:
        col = collection_meta(d["coll"]["id"])
        if col and len(col.get("parts") or []) > 1:
            have = works_by_tmdb("movie", [p["id"] for p in col["parts"]])
            lib = {x["tmdb"]: x for x in q("SELECT tmdb, has_file FROM arr_lib WHERE kind='movie' AND tmdb IN (%s)" %
                                           (",".join(str(int(p["id"])) for p in col["parts"] if p["id"]) or "0"))}
            out["collection"] = {"name": col["name"], "parts": [
                {"title": p["t"], "year": p["y"], "poster": img(p["p"], "w185"), "tmdb": p["id"],
                 "current": p["id"] == mt, "id": have[p["id"]]["rep"] if p["id"] in have else None,
                 "lib": ("has_file" if lib[p["id"]]["has_file"] else "in_arr") if p["id"] in lib else None}
                for p in col["parts"]]}
    if fetch:
        out["omdb"] = omdb_for(d.get("imdb"))
    return out


# ---- background fill


def meta_counts():
    have = q1("SELECT COUNT(*) c FROM md.meta WHERE data IS NOT NULL")["c"]
    ids = q1("SELECT COUNT(*) c FROM (SELECT DISTINCT kind, mt FROM works WHERE mt IS NOT NULL)")["c"]
    missing = q1("SELECT COUNT(*) c FROM (SELECT DISTINCT kind, mt FROM works w WHERE mt IS NOT NULL AND NOT EXISTS "
                 "(SELECT 1 FROM md.meta m WHERE m.kind=w.kind AND m.tmdb=w.mt))")["c"]
    stale = q1("SELECT COUNT(*) c FROM md.meta WHERE fetched=0")["c"]
    unmatched = q1("SELECT COUNT(*) c FROM works w WHERE mt IS NULL AND tmdb IS NULL AND adult=0 AND NOT EXISTS (SELECT 1 FROM "
                   "md.meta_map mm WHERE mm.kind=w.kind AND mm.work=w.work)")["c"] if S().get("meta_match") else 0
    return {"have": have, "with_id": ids, "missing": missing, "stale": stale, "unmatched": unmatched,
            "no_id": q1("SELECT COUNT(*) c FROM works WHERE mt IS NULL")["c"],
            "titles": q1("SELECT COUNT(*) c FROM works")["c"],
            "matched": q1("SELECT COUNT(*) c FROM md.meta_map WHERE tmdb>0")["c"]}


def meta_status():
    c = meta_counts()
    left = c["missing"] + c["stale"] + c["unmatched"] * 2
    rate = max(1, min(40, int(S().get("meta_rate") or 8)))
    size = {n: sum(os.path.getsize(p + x) for x in ("", "-wal") if os.path.exists(p + x))
            for n, p in (("catalog", DB_PATH), ("meta", META_PATH))}
    return {"counts": c, "running": META["running"], "phase": META["phase"], "done": META["done"],
            "todo": META["todo"], "error": META["error"], "key": bool(S().get("tmdb_key")),
            "auto": bool(S().get("meta_auto")), "eta": left / rate if left else 0, "sizes": size,
            "changes_at": kv_get("changes_at", 0), "backups": backups_list(), "backup_at": kv_get("backup_at", 0),
            "backup_msg": META["backup"], "backup_dir": backup_dir(), "backup_running": META["backup_running"],
            "backup_next": next_slot(time.time(), S().get("backup_interval_hours") or 24)
            if S().get("backups_on") else None}


def tmdb_changes(rate):
    """Mark titles TMDB changed since the last check so the fill refreshes them. Runs once a day."""
    last = kv_get("changes_at", 0)
    now = time.time()
    if now - last < 86400 or not q1("SELECT 1 FROM md.meta LIMIT 1"):
        return
    if last and now - last > 13 * 86400:
        ex("UPDATE md.meta SET fetched=0 WHERE fetched<?", now - 13 * 86400)
        kv_set("changes_at", now)
        return
    start = time.strftime("%Y-%m-%d", time.gmtime(last or now - 86400))
    end = time.strftime("%Y-%m-%d", time.gmtime(now))
    META["phase"] = "Checking what TMDB changed"
    marked = 0
    for kind, path in (("movie", "/movie/changes"), ("series", "/tv/changes")):
        page, pages = 1, 1
        while page <= pages and page <= 200:
            j = tmdb_get(path, rate, start_date=start, end_date=end, page=page) or {}
            pages = to_int(j.get("total_pages")) or 1
            ids = [r.get("id") for r in j.get("results") or [] if r.get("id")]
            if ids:
                marked += ex("UPDATE md.meta SET fetched=0 WHERE kind=? AND fetched>0 AND tmdb IN (%s)" %
                             ",".join(str(int(i)) for i in ids), kind).rowcount
            page += 1
    ex("UPDATE md.meta SET fetched=0 WHERE err IS NOT NULL AND fetched>0 AND fetched<?", now - 30 * 86400)
    kv_set("changes_at", now)
    if marked:
        log("TMDB changed %d of your titles since %s; refreshing them" % (marked, start))


def meta_pass():
    """One round of background work: TMDB changes, titles with an ID and no details, stale titles, then titles
    without an ID (searched by name). Returns True when there was something to do."""
    s = S()
    rate = max(1, min(40, int(s.get("meta_rate") or 8)))
    tmdb_changes(rate)
    todo = [("fetch", r["kind"], r["mt"], None) for r in q(
        "SELECT kind, mt, MAX(first_seen) fs FROM works w WHERE mt IS NOT NULL AND NOT EXISTS (SELECT 1 FROM md.meta m "
        "WHERE m.kind=w.kind AND m.tmdb=w.mt) GROUP BY kind, mt ORDER BY fs DESC LIMIT 400")]
    if len(todo) < 400:
        todo += [("fetch", r["kind"], r["tmdb"], None) for r in q(
            "SELECT kind, tmdb FROM md.meta WHERE fetched=0 LIMIT ?", 400 - len(todo))]
    if len(todo) < 400 and s.get("meta_match"):
        todo += [("match", r["kind"], None, r) for r in q(
            "SELECT kind, work, clean, year FROM works w WHERE mt IS NULL AND tmdb IS NULL AND adult=0 AND NOT EXISTS (SELECT 1 FROM "
            "md.meta_map mm WHERE mm.kind=w.kind AND mm.work=w.work) ORDER BY first_seen DESC LIMIT ?",
            400 - len(todo))]
    if not todo:
        return False
    c = meta_counts()
    META.update(todo=c["missing"] + c["stale"] + c["unmatched"], done=0, phase="Filling in details")
    t0, n = time.time(), [0]

    def one(item):
        what, kind, tmdb, w = item
        if META["auth_bad"]:
            return
        try:
            if what == "match":
                tmdb = match_work(kind, w["work"], w["clean"], w["year"], rate)
                if tmdb and not meta_row(kind, tmdb):
                    fetch_meta(kind, tmdb, rate)
            else:
                fetch_meta(kind, tmdb, rate)
            if META["error"] and not META["auth_bad"]:
                META["error"] = ""
        except MetaAuth as e:
            META.update(auth_bad=True, error=str(e))
            log("Metadata: %s. Fix the key in Settings." % e, "warn")
        except MetaError as e:
            META["error"] = str(e)
        except Exception as e:
            log("Metadata for %s %s failed: %s" % (kind, tmdb or (w and w["clean"]), e), "warn")
        n[0] += 1
        META["done"] += 1

    last_apply = time.time()
    with ThreadPoolExecutor(max_workers=max(2, min(8, rate // 2))) as pool:
        for _ in pool.map(one, todo):
            if time.time() - last_apply > 30:
                apply_meta()
                _facet_cache.clear()
                last_apply = time.time()
    apply_meta()
    _facet_cache.clear()
    META["rate_seen"] = n[0] / max(0.1, time.time() - t0)
    return not META["auth_bad"]


def meta_worker():
    time.sleep(15)
    while True:
        busy = False
        try:
            s = S()
            if s.get("tmdb_key") and not META["auth_bad"] and (s.get("meta_auto") or META["force"]):
                META["running"] = True
                busy = meta_pass()
                if not busy:
                    META["force"] = False
        except MetaAuth as e:
            META.update(auth_bad=True, error=str(e))
        except MetaError as e:
            META["error"] = str(e)
        except Exception:
            log(traceback.format_exc(), "error")
        finally:
            META["running"] = False
            if not busy:
                META["phase"] = ""
        try:
            maybe_backup()
        except Exception as e:
            log("Backup failed: %s" % e, "warn")
        META["wake"].wait(1 if busy else 120)
        META["wake"].clear()


def recompute_meta_cols():
    """Region changed: redo age ratings and streaming services from what is stored, no refetch needed."""
    rows = q("SELECT kind, tmdb, data FROM md.meta WHERE data IS NOT NULL")
    c = db()
    c.execute("BEGIN")
    try:
        for r in rows:
            d = unpack(r["data"])
            if d:
                cols = meta_cols(r["kind"], d)
                c.execute("UPDATE md.meta SET cert=?, watch=? WHERE kind=? AND tmdb=?",
                          (cols["cert"], cols["watch"], r["kind"], r["tmdb"]))
        c.execute("COMMIT")
    except Exception:
        c.execute("ROLLBACK")
        raise
    apply_meta()
    _facet_cache.clear()


def test_meta():
    out = {}
    s = S()
    if s.get("tmdb_key"):
        try:
            j = tmdb_get("/configuration")
            META.update(auth_bad=False, error="")
            META["wake"].set()
            out["TMDB"] = "OK" if isinstance(j, dict) else "Unexpected answer"
        except MetaAuth as e:
            META.update(auth_bad=True, error=str(e))
            out["TMDB"] = str(e)
        except MetaError as e:
            out["TMDB"] = str(e)
    else:
        out["TMDB"] = "No key"
    if s.get("omdb_key"):
        try:
            _, raw = http(OMDB_BASE + "?" + urllib.parse.urlencode({"i": "tt0133093", "apikey": s["omdb_key"]}),
                          timeout=8)
            d = json.loads(raw)
            out["OMDb"] = "OK" if d.get("Response") == "True" else d.get("Error") or "No answer"
        except urllib.error.HTTPError as e:
            out["OMDb"] = "OMDb did not accept the key" if e.code == 401 else "HTTP %s" % e.code
        except (urllib.error.URLError, OSError, ValueError) as e:
            out["OMDb"] = "Unreachable: %s" % getattr(e, "reason", e)
    if s.get("tvdb_key"):
        try:
            TVDB_TOKEN["token"] = None
            tvdb_call("/series/81189")
            out["TVDB"] = "OK"
        except urllib.error.HTTPError as e:
            out["TVDB"] = "TVDB did not accept the key or PIN" if e.code in (401, 403) else "HTTP %s" % e.code
        except (urllib.error.URLError, OSError, ValueError) as e:
            out["TVDB"] = "Unreachable: %s" % getattr(e, "reason", e)
    return out


# ---- backups and restore
#
# A backup is one .tar.gz holding any of: config.json (settings, providers, overrides; includes passwords and keys),
# meta.db (metadata) and vodgrab.db (catalog and download history). Restoring config applies right away; restoring
# a database is staged next to the live one and swapped in when VODgrab restarts itself a moment later.

BACKUP_PARTS = {"config": "Settings and providers", "meta": "Metadata", "catalog": "Catalog and download history"}
RESTORE_DIR = os.path.join(DATA_DIR, "restore")
UPLOADS = {}  # token -> {"path", "parts", "name", "at"}


def backup_dir():
    return (S().get("backup_path") or "").rstrip("/") or os.path.join(DATA_DIR, "backups")


def config_export():
    return {"vodgrab_config": 1, "version": VERSION, "exported": time.time(),
            "settings": {k: v for k, v in S().items() if k in DEFAULTS},
            "providers": [dict(r) for r in q("SELECT * FROM providers ORDER BY priority, id")],
            "overrides": [dict(r) for r in q("SELECT * FROM overrides")],
            "unlinks": [dict(r) for r in q("SELECT * FROM unlinks")]}


def config_import(cfg):
    """Apply an exported config: settings (keys, passwords and folders included), providers and overrides."""
    if not isinstance(cfg, dict) or not cfg.get("vodgrab_config"):
        raise ValueError("That file is not a VODgrab config")
    sets = {k: v for k, v in (cfg.get("settings") or {}).items() if k in DEFAULTS}
    save_settings(sets)
    provs = cfg.get("providers") or []
    if provs:
        c = db()
        c.execute("BEGIN")
        try:
            keep = {int(p["id"]) for p in provs if p.get("id")}
            for r in q("SELECT id FROM providers"):
                if r["id"] not in keep:
                    c.execute("DELETE FROM providers WHERE id=?", (r["id"],))
            for p in provs:
                c.execute("INSERT OR REPLACE INTO providers(id, name, url, username, password, user_agent, max_conn, "
                          "priority, enabled, source) VALUES(?,?,?,?,?,?,?,?,?,?)",
                          (p.get("id"), p.get("name"), p.get("url"), p.get("username"), p.get("password"),
                           p.get("user_agent"), p.get("max_conn") or 1, p.get("priority") or 0,
                           1 if p.get("enabled", 1) else 0, p.get("source") or ""))
            c.execute("DELETE FROM overrides")
            for o in cfg.get("overrides") or []:
                c.execute("INSERT OR REPLACE INTO overrides(arr, arr_id, xid) VALUES(?,?,?)",
                          (o.get("arr"), str(o.get("arr_id")), o.get("xid")))
            if "unlinks" in cfg:
                c.execute("DELETE FROM unlinks")
                for u in cfg.get("unlinks") or []:
                    c.execute("INSERT OR REPLACE INTO unlinks(arr, arr_id, work, at) VALUES(?,?,?,?)",
                              (u.get("arr"), str(u.get("arr_id")), u.get("work"), u.get("at")))
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        load_providers()
    ensure_dirs()
    catalog_changed(warm=True)
    META["wake"].set()


def backup_parts_wanted():
    s = S()
    return [p for p, k in (("config", "backup_config"), ("meta", "backup_meta"), ("catalog", "backup_catalog"))
            if s.get(k)]


def db_copy(path, dest):
    src, dst = sqlite3.connect(path, timeout=60), sqlite3.connect(dest)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()


def backup_now(parts=None, keep_old=True):
    """Write one backup file with the chosen parts. Returns its path."""
    if META["backup_running"]:
        raise RuntimeError("A backup is already running")
    parts = [p for p in (parts or backup_parts_wanted()) if p in BACKUP_PARTS]
    if not parts:
        raise ValueError("Pick at least one thing to back up")
    META["backup_running"] = True
    folder_ = backup_dir()
    tmpdir = os.path.join(folder_, ".work")
    try:
        os.makedirs(tmpdir, exist_ok=True)
        fix_owner(folder_)
        stamp = time.strftime("%Y%m%d-%H%M%S")
        tag = "full" if len(parts) == 3 else "-".join(parts)
        out = os.path.join(folder_, "vodgrab-backup-%s-%s.tar.gz" % (stamp, tag))
        manifest = {"vodgrab_backup": 1, "version": VERSION, "created": time.time(), "parts": parts}
        files = []
        if "config" in parts:
            p = os.path.join(tmpdir, "config.json")
            with open(p, "w") as f:
                json.dump(config_export(), f, indent=1)
            files.append(("config.json", p))
        if "meta" in parts:
            p = os.path.join(tmpdir, "meta.db")
            db_copy(META_PATH, p)
            files.append(("meta.db", p))
        if "catalog" in parts:
            p = os.path.join(tmpdir, "vodgrab.db")
            db_copy(DB_PATH, p)
            files.append(("vodgrab.db", p))
        mp = os.path.join(tmpdir, "manifest.json")
        with open(mp, "w") as f:
            json.dump(manifest, f)
        with tarfile.open(out + ".part", "w:gz", compresslevel=6) as t:
            t.add(mp, "manifest.json")
            for name, p in files:
                t.add(p, name)
        os.replace(out + ".part", out)
        fix_owner(out)
        size = os.path.getsize(out)
        if keep_old:
            keep = max(1, int(S().get("backups_keep") or 1))
            mine = sorted(f for f in os.listdir(folder_) if f.startswith("vodgrab-backup-") and f.endswith(".tar.gz"))
            for old in mine[:-keep]:
                os.remove(os.path.join(folder_, old))
        kv_set("backup_at", time.time())
        META["backup"] = "Last backup %s, %s" % (time.strftime("%Y-%m-%d %H:%M"), human_size(size))
        log("Backup written: %s (%s)" % (out, human_size(size)))
        return out
    except Exception as e:
        META["backup"] = "Backup failed: %s" % e
        raise
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
        META["backup_running"] = False


def human_size(n):
    return "%.1f MB" % (n / MB) if n >= 100 * 1024 else "%d KB" % max(1, n // 1024)


def read_manifest(path):
    try:
        with tarfile.open(path, "r:gz") as t:
            m = t.extractfile("manifest.json")
            return json.load(m) if m else None
    except (tarfile.TarError, KeyError, OSError, ValueError):
        return None


def backups_list():
    folder_ = backup_dir()
    if not os.path.isdir(folder_):
        return []
    out = []
    for f in sorted(os.listdir(folder_), reverse=True):
        p = os.path.join(folder_, f)
        if f.startswith("vodgrab-backup-") and f.endswith(".tar.gz"):
            m = read_manifest(p) or {}
            out.append({"name": f, "size": os.path.getsize(p), "at": m.get("created") or os.path.getmtime(p),
                        "parts": m.get("parts") or []})
        elif f.endswith(".db.gz"):  # 1.7.0 style
            out.append({"name": f, "size": os.path.getsize(p), "at": os.path.getmtime(p),
                        "parts": ["meta" if f.startswith("meta-") else "catalog"]})
    return out


def backup_file(name):
    name = os.path.basename(name or "")
    p = os.path.join(backup_dir(), name)
    if not name or not os.path.isfile(p):
        raise ValueError("No such backup")
    return p


def maybe_backup():
    s = S()
    if not s.get("backups_on") or int(s.get("backups_keep") or 0) <= 0 or not backup_parts_wanted():
        return
    hours = s.get("backup_interval_hours") or 24
    if kv_get("backup_at", 0) < prev_slot(time.time(), hours):
        backup_now()


def sqlite_kind(path):
    """'meta' or 'catalog' for a VODgrab database file, None for anything else."""
    try:
        c = sqlite3.connect("file:%s?mode=ro" % path, uri=True)
        try:
            names = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        finally:
            c.close()
    except sqlite3.Error:
        return None
    if "meta" in names and "meta_map" in names:
        return "meta"
    if "providers" in names and "settings" in names:
        return "catalog"
    return None


def inspect_upload(path, fname):
    """Work out what an uploaded file holds. Unpacks into a folder of its own; returns {part: file path}."""
    work = path + ".d"
    os.makedirs(work, exist_ok=True)
    found = {}
    with open(path, "rb") as f:
        head = f.read(16)
    if head[:2] == b"\x1f\x8b":
        try:
            with tarfile.open(path, "r:gz") as t:
                for mem in t.getmembers():
                    if mem.isfile() and mem.name in ("config.json", "meta.db", "vodgrab.db"):
                        t.extract(mem, work, filter="data") if hasattr(tarfile, "data_filter") else t.extract(mem, work)
                        found[{"config.json": "config", "meta.db": "meta", "vodgrab.db": "catalog"}[mem.name]] = \
                            os.path.join(work, mem.name)
        except tarfile.ReadError:
            raw = os.path.join(work, "file.db")
            with gzip.open(path, "rb") as g, open(raw, "wb") as o:
                shutil.copyfileobj(g, o, MB)
            k = sqlite_kind(raw)
            if k:
                found[k] = raw
    elif head.startswith(b"SQLite format 3"):
        k = sqlite_kind(path)
        if k:
            found[k] = path
    else:
        try:
            with open(path) as f:
                cfg = json.load(f)
            if isinstance(cfg, dict) and cfg.get("vodgrab_config"):
                found["config"] = path
        except (ValueError, UnicodeDecodeError):
            pass
    for part, p in list(found.items()):
        if part != "config" and sqlite_kind(p) != part:
            found.pop(part)
    if not found:
        raise ValueError("%s is not a VODgrab backup, config or database" % (fname or "That file"))
    return found


def restore(found, parts):
    """Apply the chosen parts. Config applies now; databases are swapped in by a restart."""
    parts = [p for p in parts if p in found]
    if not parts:
        raise ValueError("Pick at least one part to restore")
    need_restart = any(p in parts for p in ("meta", "catalog"))
    if need_restart:
        shutil.rmtree(RESTORE_DIR, ignore_errors=True)
        os.makedirs(RESTORE_DIR, exist_ok=True)
        for p in ("meta", "catalog"):
            if p in parts:
                shutil.copyfile(found[p], os.path.join(RESTORE_DIR, "meta.db" if p == "meta" else "vodgrab.db"))
        # A restored catalog file carries its own settings; keep the current ones unless config was picked too
        cfg = None
        if "config" in parts:
            with open(found["config"]) as f:
                cfg = json.load(f)
        elif "catalog" in parts:
            cfg = config_export()
        if cfg:
            with open(os.path.join(RESTORE_DIR, "config.json"), "w") as f:
                json.dump(cfg, f)
        log("Restoring %s; restarting VODgrab to swap the files in" % ", ".join(BACKUP_PARTS[p] for p in parts))
        threading.Thread(target=restart_self, daemon=True).start()
        return {"ok": True, "restart": True, "parts": parts}
    with open(found["config"]) as f:
        config_import(json.load(f))
    log("Restored settings and providers from a backup")
    return {"ok": True, "restart": False, "parts": parts}


def restart_self():
    time.sleep(1.5)
    log("Restarting")
    os.execv(sys.executable, [sys.executable, os.path.abspath(__file__)] + sys.argv[1:])


def apply_staged_restore():
    """At startup, before the databases open: swap in files staged by a restore."""
    if not os.path.isdir(RESTORE_DIR):
        return
    for name, live in (("vodgrab.db", DB_PATH), ("meta.db", META_PATH)):
        p = os.path.join(RESTORE_DIR, name)
        if os.path.isfile(p):
            if os.path.exists(live):
                os.replace(live, live + ".before-restore")
            for x in ("-wal", "-shm"):
                if os.path.exists(live + x):
                    os.remove(live + x)
            os.replace(p, live)
            print("Restored %s from backup (the old file is kept as %s.before-restore)" % (name, name), flush=True)


def finish_staged_restore():
    """After the databases open: apply the config that goes with a restored catalog."""
    p = os.path.join(RESTORE_DIR, "config.json")
    if os.path.isfile(p):
        try:
            with open(p) as f:
                config_import(json.load(f))
            log("Applied settings and providers after the restore")
        except Exception as e:
            log("Applying settings after the restore failed: %s" % e, "error")
    shutil.rmtree(RESTORE_DIR, ignore_errors=True)


def clean_uploads():
    for tok, u in list(UPLOADS.items()):
        if time.time() - u["at"] > 3600:
            shutil.rmtree(u["dir"], ignore_errors=True)
            UPLOADS.pop(tok, None)


# ------------------------------------------------------------------ search and browse



def parse_query(text):
    """Split a search into words, quoted phrases, excluded words and shortcuts like year:1990..1999 or 4k."""
    out = {"terms": [], "phrases": [], "excludes": [], "year_from": None, "year_to": None, "soft_year": None,
           "quality": [], "text": "", "people": [], "kw": []}
    rest = []

    def shortcut(m):
        val = norm(m.group(2) or m.group(3))
        if val:
            (out["kw"] if m.group(1).lower() in ("kw", "tag") else out["people"]).append(val)
        return " "
    text = re.sub(r'(?i)(?<!\S)(by|with|cast|kw|tag):(?:"([^"]+)"|(\S+))', shortcut, text or "")
    for m in re.finditer(r'(-?)"([^"]+)"|(\S+)', text or ""):
        if m.group(2):
            (out["excludes"] if m.group(1) else out["phrases"]).append(norm(m.group(2)))
            continue
        tok = m.group(3)
        low = tok.lower()
        ym = re.match(r"^(?:year|y):((?:19|20)\d\d)?(?:\.\.((?:19|20)\d\d)?)?$", low)
        if ym:
            a_, b_ = ym.group(1), ym.group(2)
            out["year_from"] = int(a_) if a_ else out["year_from"]
            out["year_to"] = int(b_) if b_ else (int(a_) if a_ and ".." not in low else out["year_to"])
            continue
        if low in ("4k", "uhd", "2160p"):
            out["quality"].append("2160p")
            continue
        if low in ("1080p", "fhd", "720p", "480p", "sd"):
            out["quality"].append({"fhd": "1080p", "sd": "480p"}.get(low, low))
            continue
        if low.startswith("-") and len(low) > 1:
            out["excludes"].append(norm(tok[1:]))
            continue
        if re.match(r"^(19|20)\d\d$", low) and out["soft_year"] is None:
            out["soft_year"] = int(low)
            continue
        rest.append(tok)
    out["text"] = norm(" ".join(rest))
    words = out["text"].split()
    out["terms"] = [ROMAN.get(w, w) if len(w) < 3 and w in ROMAN else w for w in words]
    if not out["terms"] and not out["phrases"] and out["soft_year"] and out["year_from"] is None:
        # A bare number like 2012 on its own is a title search, not only a year
        out["terms"] = [str(out["soft_year"])]
        out["text"] = str(out["soft_year"])
    return out


def lib_sql(kind, a="t", tcol=None):
    """SQL for 'this title is in Radarr/Sonarr', 'has a file there' and 'VODgrab downloaded it'."""
    k = "movie" if kind == "movie" else "series"
    tc = tcol or "%s.tmdb" % a
    arr, table = ("radarr", "movies") if kind == "movie" else ("sonarr", "series")
    # Linked by TMDB ID or title and year, unless you said it is not this title; or linked by hand
    key = "CAST(l.%s AS TEXT)" % ("tmdb" if kind == "movie" else "tvdb")  # how Radarr/Sonarr searches name it
    m = ("SELECT 1 FROM arr_lib l WHERE l.kind='%s' AND ((((%s IS NOT NULL AND l.tmdb=%s) OR "
         "(l.norm=%s.norm AND (%s.year IS NULL OR l.year IS NULL OR ABS(l.year-%s.year)<=1))) AND NOT EXISTS "
         "(SELECT 1 FROM unlinks u WHERE u.arr='%s' AND u.arr_id=%s AND u.work=%s.work)) OR %s IN "
         "(SELECT o.arr_id FROM overrides o JOIN %s x ON x.id=o.xid WHERE o.arr='%s' AND x.work=%s.work))"
         % (k, tc, tc, a, a, a, arr, key, a, key, table, arr, a))
    if kind == "movie":
        vg = ("SELECT 1 FROM jobs j WHERE j.kind='movie' AND j.status IN ('imported','completed') AND j.xid IN "
              "(SELECT id FROM movies x INDEXED BY movies_work WHERE x.work=%s.work)" % a)
    else:
        vg = ("SELECT 1 FROM jobs j JOIN episodes e ON e.id=j.xid WHERE j.kind='episode' AND "
              "j.status IN ('imported','completed') AND e.series_id IN (SELECT id FROM series x INDEXED BY "
              "series_work WHERE x.work=%s.work)" % a)
    return m, m + " AND l.has_file=1", vg


QRANK = "CASE %s WHEN '2160p' THEN 0 WHEN '1080p' THEN 1 WHEN '720p' THEN 2 WHEN '480p' THEN 3 ELSE 9 END"


def rebuild_works():
    """One row per title (movie or series) across all enabled providers: the best provider's copy as the
    representative, with genres, services, lists, languages and providers of every copy merged in."""
    ids = ",".join(str(p["id"]) for p in active_providers()) or "0"
    c = db()
    c.execute("BEGIN")
    try:
        c.execute("DELETE FROM works")
        for kind, table in (("movie", "movies"), ("series", "series")):
            ig = "COALESCE(t.igenres,'')" if kind == "movie" else "''"
            qr = QRANK % "t.qual" if kind == "movie" else "9"
            c.execute("""INSERT INTO works(kind, work, rep, clean, norm, year, tmdb, icon, rating_n, first_seen, genres,
                services, lists, langs, provs, adult, np, srch, qrank)
                SELECT '%s', r.work, r.id, r.clean, r.norm, r.year, g.tmdb, r.icon, g.rating_n, g.first_seen, g.genres,
                       g.services, g.lists, g.langs, g.provs, g.adult, g.np, g.srch, g.qrank
                FROM (SELECT t.id, t.work, t.clean, t.norm, t.year, t.icon,
                             ROW_NUMBER() OVER (PARTITION BY t.work ORDER BY %s, t.id) rn
                      FROM %s t WHERE t.prov IN (%s)) r
                JOIN (SELECT t.work, MAX(t.tmdb) tmdb, MAX(t.rating_n) rating_n, MIN(COALESCE(t.first_seen, 0)) first_seen,
                             group_concat(COALESCE(t.genres,'') || %s, '') genres,
                             group_concat(COALESCE(t.services,''), '') services,
                             group_concat(COALESCE(t.lists,''), '') lists,
                             group_concat(CASE WHEN t.lang IS NULL THEN '' ELSE '|' || t.lang || '|' END, '') langs,
                             group_concat('|' || t.prov || '|', '') provs, MAX(COALESCE(t.adult,0)) adult,
                             COUNT(*) np, group_concat(DISTINCT t.srch) srch, MIN(%s) qrank
                      FROM %s t WHERE t.prov IN (%s) GROUP BY t.work) g ON g.work=r.work
                WHERE r.rn=1""" % (kind, prank_sql(), table, ids, ig, qr, table, ids))
        c.execute("UPDATE works SET pgenres=genres, prating=rating_n")
        apply_meta(c)
        if FTS:
            c.execute("DELETE FROM tri_works")
            c.execute("INSERT INTO tri_works(rowid, srch) SELECT wid, srch FROM works")
        c.execute("COMMIT")
    except Exception:
        c.execute("ROLLBACK")
        raise


def apply_meta(c=None, kind=None, tmdb=None):
    """Copy what TMDB knows onto the merged titles: the TMDB ID (from the provider or found by title), age rating,
    runtime, popularity, votes, show status, streaming services, original language and poster. Genres and rating
    are merged with the provider's own, which are kept in pgenres and prating."""
    c = c or db()
    one, args = "", []
    if kind and tmdb:
        one, args = " AND works.kind=? AND works.mt=?", [kind, tmdb]
    else:
        c.execute("UPDATE works SET mt=COALESCE(tmdb, (SELECT NULLIF(mm.tmdb, 0) FROM md.meta_map mm WHERE "
                  "mm.kind=works.kind AND mm.work=works.work))")
    c.execute("""UPDATE works SET cert=m.cert, runtime=m.runtime, pop=m.pop, votes=m.votes, mstatus=m.status,
        watch=m.watch, olang=m.olang, poster=m.poster, genres=COALESCE(works.pgenres, '') || COALESCE(m.genres, ''),
        rating_n=CASE WHEN m.votes>=25 AND m.rating>0 THEN m.rating ELSE works.prating END
        FROM md.meta m WHERE m.kind=works.kind AND m.tmdb=works.mt AND m.data IS NOT NULL%s""" % one, args)


def update_work_quality(cid):
    r = q1("SELECT work FROM movies WHERE id=?", cid)
    if r and r["work"]:
        ex("UPDATE works SET qrank=(SELECT MIN(%s) FROM movies t INDEXED BY movies_work WHERE t.work=?) "
           "WHERE kind='movie' AND work=?" % (QRANK % "t.qual"), r["work"], r["work"])


def prank_sql():
    keys = list(PROVIDERS)
    if not keys:
        return "0"
    return "CASE t.prov %s ELSE 99 END" % " ".join("WHEN %d THEN %d" % (p, i) for i, p in enumerate(keys))


def read_filters(qs):
    def many(k):
        return [x for x in (qs.get(k) or "").split(",") if x]
    kinds = {"movie": ["movie"], "series": ["series"], "all": ["movie", "series"]}.get(qs.get("type") or "movie",
                                                                                     ["movie"])
    f = {"service": many("service"), "genre": many("genre"), "list": many("list"), "lang": many("lang"),
         "prov": [int(x) for x in many("prov") if x.isdigit()], "quality": many("quality"), "lib": many("lib"),
         "year_from": to_int(qs.get("year_from")), "year_to": to_int(qs.get("year_to")),
         "rating": to_float(qs.get("rating")), "new": to_int(qs.get("new")), "cert": many("cert"),
         "runtime": many("runtime"), "olang": many("olang"), "watch": many("watch"), "status": many("status"),
         "merge": qs.get("merge") != "0", "kinds": kinds}
    return f


QUALS = ("2160p", "1080p", "720p", "480p")
TABLES = {"movie": "movies", "series": "series"}


def filter_sql(f, kind=None, merged=True):
    """WHERE clause for the merged titles table (alias w) or, when merged is False, one provider's rows (alias w)."""
    where, args = [], []
    if merged:
        where.append("w.kind IN (%s)" % ",".join("'%s'" % k for k in f["kinds"]))
        ids = [p["id"] for p in active_providers()]
        if not S().get("show_adult"):
            where.append("w.adult=0")
    else:
        where.append(live().replace("prov", "w.prov").replace("adult", "w.adult"))

    def tags(cols, vals):
        if not vals:
            return
        parts = []
        for v in vals:
            for c in cols:
                parts.append("%s LIKE ?" % c)
                args.append("%|" + v + "|%")
        where.append("(" + " OR ".join(parts) + ")")

    tags(["w.services"], f["service"])
    tags(["w.genres"] + (["w.igenres"] if not merged and kind == "movie" else []), f["genre"])
    tags(["w.lists"], f["list"])
    if merged:
        tags(["w.langs"], f["lang"])
        tags(["w.provs"], [str(p) for p in f["prov"]])
        if not f["prov"] and len(ids) > 0:
            pass
    else:
        if f["lang"]:
            where.append("w.lang IN (%s)" % ",".join("?" * len(f["lang"])))
            args += f["lang"]
        if f["prov"]:
            where.append("w.prov IN (%s)" % ",".join("?" * len(f["prov"])))
            args += f["prov"]
    if f["year_from"]:
        where.append("w.year>=?")
        args.append(f["year_from"])
    if f["year_to"]:
        where.append("w.year<=?")
        args.append(f["year_to"])
    if f["rating"]:
        where.append("w.rating_n>=?")
        args.append(f["rating"])
    if f["new"]:
        where.append("w.first_seen>=?")
        args.append(int(time.time()) - f["new"] * 86400)
    if f["quality"]:
        qexpr = "w.qrank" if merged else "(%s)" % (QRANK % "w.qual") if kind == "movie" else "9"
        ranks = [QUALS.index(x) for x in f["quality"] if x in QUALS] + ([9] if "unknown" in f["quality"] else [])
        where.append("%s IN (%s)" % (qexpr, ",".join(str(x) for x in ranks) or "-1"))
        if merged:
            where.append("w.kind='movie'")
        elif kind != "movie":
            where.append("0")
    meta_where(f, "w" if merged else "wm", f["kinds"] if merged else [kind], where, args)
    if f["lib"]:
        parts = []
        for k in f["kinds"] if merged else [kind]:
            inarr, hasfile, vg = lib_sql(k, "w", "w.mt" if merged else None)
            cond = {"missing": "NOT EXISTS(%s)" % inarr,
                    "in_arr": "EXISTS(%s) AND NOT EXISTS(%s)" % (inarr, hasfile),
                    "has_file": "EXISTS(%s)" % hasfile, "vodgrab": "EXISTS(%s)" % vg,
                    "wanted": "EXISTS(SELECT 1 FROM wanted x WHERE x.kind='%s' AND x.work=w.work AND x.available=1)"
                              % k}
            sub = [cond[x] for x in f["lib"] if x in cond]
            if sub:
                parts.append(("(w.kind='%s' AND (%s))" % (k, " OR ".join("(%s)" % p for p in sub))) if merged
                             else "(%s)" % " OR ".join("(%s)" % p for p in sub))
        if parts:
            where.append("(" + " OR ".join(parts) + ")")
    return where, args


def meta_where(f, a, kinds, where, args):
    """Filters that come from TMDB: age rating, runtime, original language, streaming services, show status."""
    for key, col in (("cert", "cert"), ("olang", "olang"), ("status", "mstatus")):
        if f[key]:
            where.append("%s.%s IN (%s)" % (a, col, ",".join("?" * len(f[key]))))
            args += f[key]
    if f["watch"]:
        where.append("(%s)" % " OR ".join("%s.watch LIKE ?" % a for _ in f["watch"]))
        args += ["%|" + v + "|%" for v in f["watch"]]
    if f["runtime"]:
        parts = []
        for k in kinds:
            for name, lo, hi in RUNTIME_BUCKETS[k]:
                if name in f["runtime"]:
                    parts.append("(%s.kind='%s' AND %s.runtime>=%d AND %s.runtime<%d)" % (a, k, a, lo, a, hi))
        where.append("(%s)" % (" OR ".join(parts) or "0"))


SORTS = {"new": "first_seen DESC, rep DESC", "title": "norm, year, rep", "title_desc": "norm DESC, year DESC, rep DESC",
         "year": "year DESC NULLS LAST, clean", "rating": "rating_n DESC NULLS LAST, clean",
         "quality": "qrank, rating_n DESC NULLS LAST", "popular": "pop DESC NULLS LAST, clean",
         "top": "topr DESC NULLS LAST, pop DESC NULLS LAST"}


def browse(qs):
    f = read_filters(qs)
    text = (qs.get("q") or "").strip()
    limit, offset = max(1, min(200, int(qs.get("limit") or 60))), max(0, int(qs.get("offset") or 0))
    if S().get("sonarr_url") or S().get("radarr_url"):
        if time.time() - LIB["at"] > 3600 and not LIB["running"]:
            threading.Thread(target=refresh_library, daemon=True).start()
    if text:
        return search(text, f, qs.get("sort") or "relevance", limit, offset, qs.get("marks") == "1")
    return listing(f, qs.get("sort") or "new", limit, offset, qs.get("marks") == "1")


def row_source(f):
    """FROM clause and column list: the merged titles table, or each provider's rows when not merging."""
    if f["merge"]:
        return [("works w", WORK_CARD_COLS + ", CASE WHEN w.votes>=100 THEN w.rating_n END AS topr", None)]
    out = []
    for kind in f["kinds"]:
        qr = (QRANK % "w.qual") if kind == "movie" else "9"
        out.append(("%s w LEFT JOIN works wm ON wm.kind='%s' AND wm.work=w.work" % (TABLES[kind], kind),
                    "'%s' AS kind, w.id AS rep, w.work, w.clean, w.norm, w.year, w.tmdb, "
                    "w.icon, w.rating_n, w.first_seen, '|' || w.prov || '|' AS provs, 1 AS np, %s AS qrank, wm.mt, "
                    "wm.poster, wm.pop, wm.votes, CASE WHEN wm.votes>=100 THEN wm.rating_n END AS topr" % (kind, qr),
                    kind))
    return out


LETTER_SQL = "CASE WHEN substr(w.norm, 1, 1) BETWEEN 'a' AND 'z' THEN upper(substr(w.norm, 1, 1)) ELSE '#' END"
DECADE_SQL = "CASE WHEN w.year IS NULL THEN -1 ELSE (w.year / 10) * 10 END"


def jump_marks(groups, sort):
    """Letters (title sorts) or decades (year sort) in list order, each with its count and the offset where it
    starts, for the jump bar."""
    if sort in ("title", "title_desc"):
        order = ["#"] + [chr(c) for c in range(65, 91)]
        if sort == "title_desc":
            order.reverse()
    else:
        order = sorted((g for g in groups if g != -1), reverse=True) + [-1]
    out, off = [], 0
    for g in order:
        n = groups.get(g, 0)
        label = g if isinstance(g, str) else ("Unknown" if g == -1 else "%ds" % g)
        if isinstance(g, str) or n:
            out.append([label, n, off])
        off += n
    return out


def letter_of(norm_):
    c = (norm_ or "")[:1]
    return c.upper() if "a" <= c <= "z" else "#"


def listing(f, sort, limit, offset, marks=False):
    parts, args, total = [], [], 0
    for frm, cols, kind in row_source(f):
        where, a = filter_sql(f, kind, f["merge"])
        parts.append("SELECT %s FROM %s WHERE %s" % (cols, frm, " AND ".join(where)))
        args += a
        total += q1("SELECT COUNT(*) c FROM %s WHERE %s" % (frm, " AND ".join(where)), *a)["c"]
    sql = "SELECT * FROM (%s) ORDER BY %s LIMIT ? OFFSET ?" % (" UNION ALL ".join(parts), SORTS.get(sort, SORTS["new"]))
    rows = q(sql, *(args + [limit, offset]))
    out = {"total": total, "items": [card(r) for r in rows], "suggest": [], "fuzzy": False}
    if marks and sort in ("title", "title_desc", "year"):
        expr = LETTER_SQL if sort != "year" else DECADE_SQL
        groups = {}
        for frm, cols, kind in row_source(f):
            where, a = filter_sql(f, kind, f["merge"])
            for r in q("SELECT %s g, COUNT(*) n FROM %s WHERE %s GROUP BY g" % (expr, frm, " AND ".join(where)), *a):
                groups[r["g"]] = groups.get(r["g"], 0) + r["n"]
        out["marks"] = jump_marks(groups, sort)
    return out


def search_where(pq, f, kind):
    where, args = filter_sql(f, kind, f["merge"])
    tri = "tri_works" if f["merge"] else "tri_%s" % TABLES[kind]
    idcol = "w.wid" if f["merge"] else "w.id"
    long_terms = [t for t in pq["terms"] if len(t) >= 3]
    short_terms = [t for t in pq["terms"] if len(t) < 3]
    if FTS and long_terms:
        where.append("%s IN (SELECT rowid FROM %s WHERE %s MATCH ?)" % (idcol, tri, tri))
        args.append(" AND ".join('"%s"' % t.replace('"', "") for t in long_terms))
    for t in short_terms + ([] if FTS else long_terms):
        where.append("(' ' || w.srch || ' ') LIKE ?")
        args.append("%" + (" " + t if len(t) < 3 else t) + "%")
    for ph in pq["phrases"]:
        where.append("w.srch LIKE ?")
        args.append("%" + ph + "%")
    for exq in pq["excludes"]:
        where.append("(' ' || w.srch) NOT LIKE ?")
        args.append("% " + exq + "%")
    mcol = "+w.mt" if f["merge"] else "+wm.mt"  # unary plus: let the title index or text search lead
    for col, vals in (("people", pq["people"]), ("keywords", pq["kw"])):
        for v in vals:
            parts = []
            for k in (f["kinds"] if f["merge"] else [kind]):
                parts.append("(%s%s IN (SELECT tmdb FROM md.meta WHERE kind='%s' AND %s LIKE ?))" % (
                    "w.kind='%s' AND " % k if f["merge"] else "", mcol, k, col))
                args.append("%" + v + "%" if col == "people" else "%|" + v + "|%")
            where.append("(%s)" % " OR ".join(parts))
    if pq["year_from"]:
        where.append("w.year>=?")
        args.append(pq["year_from"])
    if pq["year_to"]:
        where.append("w.year<=?")
        args.append(pq["year_to"])
    if pq["quality"]:
        ranks = ",".join(str(QUALS.index(x)) for x in pq["quality"] if x in QUALS)
        where.append("%s IN (%s)" % ("w.qrank" if f["merge"] else (QRANK % "w.qual") if kind == "movie" else "9",
                                     ranks or "-1"))
    return where, args, tri, idcol


def candidates(pq, f, cap=4000):
    """Exact and starts-with matches first (always included), then any other matches up to the cap."""
    out, capped = [], False
    for frm, cols, kind in row_source(f):
        where, args, _, _ = search_where(pq, f, kind)
        w = " AND ".join(where)
        if pq["text"]:
            hi = pq["text"][:-1] + chr(ord(pq["text"][-1]) + 1)
            out += q("SELECT %s FROM %s WHERE %s AND w.norm>=? AND w.norm<? LIMIT %d" % (cols, frm, w, cap), *(
                args + [pq["text"], hi]))
        rows = q("SELECT %s FROM %s WHERE %s LIMIT %d" % (cols, frm, w, cap + 1), *args)
        capped = capped or len(rows) > cap
        out += rows[:cap]
    return out, capped


def fuzzy_candidates(pq, f, cap=300):
    """Close spellings: any shared three letter piece, then rescored by similarity."""
    if not FTS or not pq["text"]:
        return []
    joined = pq["text"].replace(" ", "")
    grams = list(dict.fromkeys(joined[i:i + 3] for i in range(max(0, len(joined) - 2))))
    if not grams:
        return []
    out = []
    for frm, cols, kind in row_source(f):
        where, args = filter_sql(f, kind, f["merge"])
        tri = "tri_works" if f["merge"] else "tri_%s" % TABLES[kind]
        idcol = "w.wid" if f["merge"] else "w.id"
        rows = q("SELECT %s FROM (SELECT rowid AS rid FROM %s WHERE %s MATCH ? ORDER BY rank LIMIT %d) x, %s WHERE "
                 "%s=x.rid AND %s" % (cols, tri, tri, cap * 3, frm, idcol, " AND ".join(where)),
                 *([" OR ".join('"%s"' % g for g in grams)] + args))
        for r in rows:
            ratio = max(difflib.SequenceMatcher(None, pq["text"], r["norm"]).ratio(),
                        difflib.SequenceMatcher(None, joined, r["norm"].replace(" ", "")).ratio(),
                        difflib.SequenceMatcher(None, pq["text"], r["norm"][:len(pq["text"]) + 2]).ratio())
            if ratio >= 0.72:
                out.append((ratio, r))
    out.sort(key=lambda x: -x[0])
    return out[:cap]


def rank(r, pq, fuzzy_ratio=None):
    n, t = r["norm"], pq["text"]
    if not t and fuzzy_ratio is None:
        return 320 + min(300, r["pop"] or 0) + (r["rating_n"] or 0) * 3
    if fuzzy_ratio is not None:
        base = 200 + fuzzy_ratio * 300
    elif t and n == t:
        base = 1000
    elif t and n.startswith(t):
        base = 820
    elif t and all(re.search(r"(^| )" + re.escape(w), n) for w in t.split()):
        base = 640
    elif t and t in n:
        base = 480
    else:
        base = 320
    if pq["soft_year"] and r["year"] == pq["soft_year"]:
        base += 160
    return base + (r["rating_n"] or 0) * 3 - len(n) * 0.4


def search(text, f, sort, limit, offset, marks=False):
    pq = parse_query(text)
    scored = {}
    rows, capped = candidates(pq, f)
    for r in rows:
        scored[(r["kind"], r["rep"])] = (rank(r, pq), r)
    fuzzy, suggest = False, []
    if len(scored) < 3 and pq["text"]:
        for ratio, r in fuzzy_candidates(pq, f):
            key = (r["kind"], r["rep"])
            if key not in scored:
                fuzzy = True
                scored[key] = (rank(r, pq, ratio), r)
        if fuzzy:
            suggest = list(dict.fromkeys(v[1]["clean"] for v in sorted(scored.values(), key=lambda v: -v[0])[:3]))
    vals = list(scored.values())
    keyf = {"title": lambda v: (v[1]["norm"], v[1]["year"] or 0),
            "title_desc": lambda v: (v[1]["norm"], v[1]["year"] or 0),
            "year": lambda v: (v[1]["year"] is None, -(v[1]["year"] or 0)),
            "rating": lambda v: -(v[1]["rating_n"] or 0), "new": lambda v: -(v[1]["first_seen"] or 0),
            "quality": lambda v: (v[1]["qrank"], -(v[1]["rating_n"] or 0)),
            "popular": lambda v: -(v[1]["pop"] or 0),
            "top": lambda v: (v[1]["topr"] is None, -(v[1]["topr"] or 0), -(v[1]["pop"] or 0))}.get(sort, lambda v: -v[0])
    vals.sort(key=keyf, reverse=sort == "title_desc")
    out_marks = None
    if marks and sort in ("title", "title_desc", "year"):
        groups = collections.Counter(letter_of(r["norm"]) if sort != "year" else
                                     (-1 if r["year"] is None else r["year"] // 10 * 10) for _, r in vals)
        out_marks = jump_marks(groups, sort)
    return {"total": len(vals), "capped": capped, "items": [card(r) for _, r in vals[offset:offset + limit]],
            "marks": out_marks,
            "suggest": suggest, "fuzzy": fuzzy,
            "parsed": {k: pq[k] for k in ("year_from", "year_to", "soft_year", "quality", "excludes", "phrases", "people",
                                          "kw")}}


def card(r):
    """What a browse card needs: provider names, best quality, library status."""
    kind = r["kind"]
    provs = sorted({int(x) for x in untag(r["provs"])}, key=lambda p: list(PROVIDERS).index(p)
                   if p in PROVIDERS else 99)
    keys = r.keys()
    mt = (r["mt"] if "mt" in keys else None) or r["tmdb"]
    inarr, hasfile, vg = lib_sql(kind, "w")
    lib = q1("SELECT EXISTS(%s) a, EXISTS(%s) b, EXISTS(%s) c FROM (SELECT ? AS tmdb, ? AS norm, ? AS year, "
             "? AS work) w" % (inarr, hasfile, vg), mt, r["norm"], r["year"], r["work"])
    status = "vodgrab" if lib["c"] else "has_file" if lib["b"] else "in_arr" if lib["a"] else "missing"
    wanted = bool(r["work"] and q1("SELECT 1 FROM wanted WHERE kind=? AND work=? AND available=1 LIMIT 1", kind,
                                   r["work"]))
    qr = r["qrank"] if r["qrank"] is not None else 9
    return {"kind": kind, "id": r["rep"], "clean": r["clean"], "year": r["year"], "icon": r["icon"],
            "rating": r["rating_n"], "providers": [pname(p) for p in provs], "nprov": len(provs),
            "quality": QUALS[qr] if qr < len(QUALS) else None, "lib": status, "wanted": wanted,
            "poster": r["poster"] if "poster" in keys else None, "l": letter_of(r["norm"])}


_facet_cache = {}
CATVER = [0]


def catalog_changed(warm=False):
    """Rebuild the merged titles table; filter counts are cached until the catalog, providers or adult setting
    change."""
    CATVER[0] += 1
    _facet_cache.clear()
    try:
        rebuild_works()
    except Exception as e:
        log("Rebuilding the title index failed: %s" % e, "error")
    if warm:
        def go():
            for t in ("movie", "series", "all"):
                try:
                    facets({"type": t})
                except Exception as e:
                    log("Filter counts failed: %s" % e, "warn")
        threading.Thread(target=go, daemon=True).start()


def facets(qs):
    f = read_filters(qs)
    key = (tuple(f["kinds"]), bool(S().get("show_adult")), CATVER[0])
    if key in _facet_cache:
        return _facet_cache[key]
    svc, gen_, lst, lang, prov_n, cert, olang, watch, stat, runt = (collections.Counter() for _ in range(10))
    kinds = ",".join("'%s'" % k for k in f["kinds"])
    adult = "" if S().get("show_adult") else " AND adult=0"
    ymin = ymax = None
    buckets = RUNTIME_BUCKETS
    for r in q("SELECT kind, services, genres, lists, langs, provs, year, cert, olang, watch, mstatus, runtime FROM works "
               "WHERE kind IN (%s)%s" % (kinds, adult)):
        if r["cert"]:
            cert[r["cert"]] += 1
        if r["olang"]:
            olang[r["olang"]] += 1
        if r["mstatus"]:
            stat[r["mstatus"]] += 1
        if r["runtime"]:
            for name, lo, hi in buckets[r["kind"]]:
                if lo <= r["runtime"] < hi:
                    runt[name] += 1
        watch.update(set(untag(r["watch"])))
        svc.update(set(untag(r["services"])))
        gen_.update(set(untag(r["genres"])))
        lst.update(set(untag(r["lists"])))
        lang.update(set(untag(r["langs"])))
        prov_n.update(set(untag(r["provs"])))
        y = r["year"]
        if y:
            ymin = y if ymin is None or y < ymin else ymin
            ymax = y if ymax is None or y > ymax else ymax
    out = {"services": svc.most_common(), "genres": sorted(gen_.items()), "lists": sorted(lst.items()),
           "langs": lang.most_common(), "providers": [(p["id"], p["name"], prov_n[str(p["id"])])
                                                      for p in active_providers()],
           "year_min": ymin, "year_max": ymax, "certs": cert_order(cert), "olangs": olang.most_common(),
           "watch": watch.most_common(), "status": stat.most_common(), "runtime": [(b, runt[b]) for b in
                                                                                   ("short", "medium", "long", "epic")
                                                                                   if runt[b]],
           "region": (S().get("meta_region") or "US").upper(), "meta": bool(S().get("tmdb_key")),
           "library": bool(S().get("sonarr_url") or S().get("radarr_url")), "lib_at": LIB["at"]}
    _facet_cache[key] = out
    return out


CERT_ORDER = ["G", "TV-Y", "TV-Y7", "TV-G", "PG", "TV-PG", "14A", "PG-13", "TV-14", "18A", "R", "TV-MA", "NC-17", "A",
              "R18", "E", "NR"]


def cert_order(counter):
    """Age ratings from youngest to oldest audience, the ones VODgrab doesn't know sorted by how common they are."""
    known = [(c, counter[c]) for c in CERT_ORDER if counter.get(c)]
    rest = [(c, n) for c, n in counter.most_common() if c not in CERT_ORDER and n >= 3]
    return known + rest


def latest_job(kind, xid):
    r = q1("SELECT status, error FROM jobs WHERE kind=? AND xid=? ORDER BY id DESC LIMIT 1", kind, xid)
    return dict(r) if r else None


def movie_detail(xid):
    m = q1("SELECT * FROM movies WHERE id=?", xid)
    if not m:
        raise ValueError("Movie not found")
    out = dict(m)
    try:
        info = vod_info(xid)
        out.update(plot=info.get("plot") or info.get("description") or "", genre=info.get("genre") or "",
                   duration=info.get("duration") or "", cast=info.get("cast") or info.get("actors") or "",
                   director=info.get("director") or "", poster=info.get("movie_image") or m["icon"],
                   tmdb=m["tmdb"] or to_int(info.get("tmdb_id")))
    except Exception as e:
        out["info_error"] = mask(str(e))
    out["job"] = latest_job("movie", xid)
    out["provider"] = pname(m["prov"])
    out["also_on"] = [pname(dec(i)[0]) for i in equivalents("movie", xid) if i != xid]
    srcs = []
    for i in equivalents("movie", xid):
        row = movie_row(i) if i != xid else q1("SELECT * FROM movies WHERE id=?", i)
        if row:
            srcs.append(dict(media_view(row), id=i, provider=pname(dec(i)[0]), name=row["name"], ext=row["ext"],
                             clicked=i == xid))
    for src in srcs:
        try:
            copies = dispatcharr_copies(src["id"])
        except Exception as e:
            src["dp_error"] = mask(str(e))
            continue
        if copies is not None:
            src["dp_copies"] = copies
    out["sources"] = srcs
    try:
        out["meta"] = meta_view("movie", m["work"])
    except Exception as e:
        log("Details for %s failed: %s" % (m["clean"], e), "warn")
        out["meta"] = {"has": False, "error": mask(str(e))}
    out["tmdb"] = out.get("tmdb") or out["meta"].get("tmdb")
    out["links"] = arr_links("movie", xid)
    out["arr_ok"] = RADARR.ok()
    if RADARR.ok() and out.get("tmdb"):
        try:
            found = [x for x in RADARR.get("/api/v3/movie", tmdbId=out["tmdb"]) or []
                     if str(x.get("tmdbId")) == str(out["tmdb"])]
            out["library"] = {"in_radarr": bool(found), "has_file": bool(found and found[0].get("hasFile"))}
        except ArrError:
            pass
    return out


def series_detail(sid):
    srow = q1("SELECT * FROM series WHERE id=?", sid)
    if not srow:
        raise ValueError("Series not found")
    out = dict(srow)
    try:
        ensure_episodes(sid)
    except Exception as e:
        out["info_error"] = mask(str(e))
    seasons = collections.OrderedDict()
    jobs = {r["xid"]: r["status"] for r in q("SELECT xid, status FROM jobs WHERE kind='episode' AND xid IN "
                                              "(SELECT id FROM episodes WHERE series_id=?) ORDER BY id", sid)}
    for e in q("SELECT * FROM episodes WHERE series_id=? ORDER BY season, episode", sid):
        seasons.setdefault(e["season"], []).append(dict(e, job=jobs.get(e["id"]), media=media_view(e)))
    out["seasons"] = [{"season": k, "episodes": v} for k, v in seasons.items()]
    lib = q1("SELECT * FROM arr_lib WHERE kind='series' AND ((? IS NOT NULL AND tmdb=?) OR (norm=? AND (year IS NULL "
             "OR ? IS NULL OR ABS(year-?)<=1))) LIMIT 1", srow["tmdb"], srow["tmdb"], srow["norm"], srow["year"],
             srow["year"])
    out["tvdb"] = lib["tvdb"] if lib else None
    out["in_sonarr"] = bool(lib)
    wanted = {(w["season"], w["episode"]): w["arr_ep"] for w in
              q("SELECT season, episode, arr_ep FROM wanted WHERE kind='series' AND work=?", srow["work"])}
    for se in out["seasons"]:
        for e in se["episodes"]:
            e["wanted"] = (e["season"], e["episode"]) in wanted
    out["provider"] = pname(srow["prov"])
    out["also_on"] = [pname(dec(i)[0]) for i in equivalents("series", sid) if i != sid]
    try:
        out["meta"] = meta_view("series", srow["work"])
    except Exception as e:
        log("Details for %s failed: %s" % (srow["clean"], e), "warn")
        out["meta"] = {"has": False, "error": mask(str(e))}
    mv = out["meta"]
    out["tmdb"] = out.get("tmdb") or mv.get("tmdb")
    if mv.get("has"):
        if not out["tvdb"]:
            out["tvdb"] = mv.get("tvdb")
            if not out["tvdb"] and S().get("tvdb_key"):
                out["tvdb"] = tvdb_lookup(mv.get("title") or srow["clean"], srow["year"] or year_of(mv.get("date")),
                                          mv.get("imdb"))
                if out["tvdb"]:
                    ex("UPDATE md.meta SET tvdb=? WHERE kind='series' AND tmdb=?", out["tvdb"], mv["tmdb"])
        merge_seasons(out, mv)
    return out


def merge_seasons(out, mv):
    """Put TMDB episode names, air dates, overviews and stills next to the provider's episodes, and list aired
    episodes no provider has."""
    have = {se["season"] for se in out["seasons"]}
    wanted_seasons = sorted(have | {x["n"] for x in mv.get("seasons") or [] if x.get("n") and x.get("eps")})
    if not wanted_seasons:
        return
    with ThreadPoolExecutor(max_workers=4) as pool:
        got = dict(zip(wanted_seasons, pool.map(lambda n: season_meta(mv["tmdb"], n, mv.get("status")),
                                                 wanted_seasons)))
    today = time.strftime("%Y-%m-%d")
    by_num = {se["season"]: se for se in out["seasons"]}
    for n in wanted_seasons:
        teps = {e["e"]: e for e in got.get(n) or []}
        se = by_num.get(n)
        if not se:
            if not any(e["d"] and e["d"] <= today for e in teps.values()):
                continue
            se = {"season": n, "episodes": [], "none": True}
            out["seasons"].append(se)
        for e in se["episodes"]:
            t = teps.get(e["episode"])
            if t:
                e.update(tname=t["n"], overview=t["o"], air=t["d"], still=img(t["s"], "w300"), runtime=t["r"])
        pe = {e["episode"] for e in se["episodes"]}
        for num, t in teps.items():
            if num not in pe and t["d"] and t["d"] <= today:
                se["episodes"].append({"id": None, "season": n, "episode": num, "title": "", "tname": t["n"],
                                       "overview": t["o"], "air": t["d"], "still": img(t["s"], "w300"),
                                       "missing": True, "media": {}})
        se["episodes"].sort(key=lambda e: e["episode"])
        se["name"] = next((x.get("name") for x in mv.get("seasons") or [] if x.get("n") == n), None)
    out["seasons"].sort(key=lambda se: (se["season"] == 0, se["season"]))


def job_row(j):
    d = dict(j)
    d.pop("meta", None)
    meta = jmeta(j)
    d["probe"] = meta.get("probe")
    d["provider"] = pname(j["prov"]) if j["prov"] else ""
    d["sources"] = len(job_sources(j))
    pr = ENGINE.progress.get(j["id"])
    if pr:
        d["done"], d["speed"] = pr["done"], pr["speed"]
        d["total"] = d["total"] or pr["total"]
    return d


def settings_view():
    s = dict(S())
    secrets_set = {k: bool(s.get(k)) for k in SECRET_KEYS}
    for k in SECRET_KEYS:
        s[k] = ""
    return {"settings": s, "secrets_set": secrets_set, "intervals": ALLOWED_INTERVALS, "folders": folders_view(),
            "folder_defaults": {n: os.path.join(S()["base_path"].rstrip("/"), sub) for n, (k, sub) in FOLDERS.items()},
            "providers": providers_view(), "backup_default": os.path.join(DATA_DIR, "backups")}


def arr_meta(name):
    arr = ARR(name)
    out = {"profiles": [{"id": p["id"], "name": p["name"]} for p in arr.get("/api/v3/qualityprofile") or []],
           "roots": [r["path"] for r in arr.get("/api/v3/rootfolder") or []]}
    idx = [d for d in arr.get("/api/v3/indexer") or [] if d.get("name") == "VODgrab"]
    out["indexer"] = {"installed": bool(idx), "priority": idx[0].get("priority") if idx else None}
    out["url"] = arr.cfg()[0]
    return out


# ------------------------------------------------------------------ http server


class Handler(BaseHTTPRequestHandler):
    server_version = "VODgrab/" + VERSION
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def send(self, code, body, ctype="application/json", headers=None):
        if isinstance(body, (dict, list)):
            body = json.dumps(body, default=str).encode()
        elif isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def read_body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(n) if n else b""

    def do_GET(self):
        self.route("GET")

    def do_HEAD(self):
        self.route("GET")

    def do_POST(self):
        self.route("POST")

    def key_ok(self, qs):
        k = qs.get("apikey") or self.headers.get("X-Api-Key") or ""
        return secrets.compare_digest(str(k), str(S()["api_key"]))

    def ui_auth(self):
        s = S()
        if not s["web_username"]:
            return True
        want = base64.b64encode(("%s:%s" % (s["web_username"], s["web_password"])).encode()).decode()
        got = (self.headers.get("Authorization") or "").replace("Basic ", "", 1).strip()
        if secrets.compare_digest(got, want):
            return True
        self.send(401, {"error": "auth required"}, headers={"WWW-Authenticate": 'Basic realm="VODgrab"'})
        return False

    def route(self, method):
        u = urllib.parse.urlparse(self.path)
        path = u.path
        qs = {k: v[-1] for k, v in urllib.parse.parse_qs(u.query, keep_blank_values=True).items()}
        try:
            if path.startswith("/newznab"):
                return self.newznab(qs)
            if path.startswith("/sabnzbd"):
                return self.sab(qs, method)
            if path.startswith("/nzb/"):
                return self.nzb(path, qs)
            if path == "/api/webhook":
                return self.webhook(qs)
            if path in ICONS:  # before the login check, so the icon shows on the login prompt too
                return self.send(200, ICONS[path], "image/png")
            if not self.ui_auth():
                return
            if path in ("/", "/index.html"):
                return self.send(200, UI_HTML, "text/html; charset=utf-8")
            if path.startswith("/api/"):
                return self.api(method, path[5:], qs)
            self.send(404, {"error": "not found"})
        except ArrError as e:
            self.send(502, {"error": mask(str(e))})
        except (ValueError, RuntimeError) as e:
            self.send(400, {"error": mask(str(e))})
        except Exception as e:
            log(traceback.format_exc(), "error")
            self.send(500, {"error": mask(str(e))})

    def host_url(self):
        host = self.headers.get("Host")
        if host:
            return "%s://%s" % (self.headers.get("X-Forwarded-Proto") or "http", host)
        return (S().get("external_url") or "").rstrip("/") or "http://localhost:%s" % S()["port"]

    def newznab(self, qs):
        t = (qs.get("t") or "").lower()
        if t == "caps":
            return self.send(200, CAPS_XML, "application/xml")
        if not self.key_ok(qs):
            return self.send(200, nz_error(100, "Incorrect user credentials"), "application/xml")
        try:
            items = newznab_search(t, qs)
        except Exception as e:
            log("Search failed: %s" % e, "warn")
            return self.send(200, nz_error(900, "Search failed: %s" % mask(str(e))), "application/xml")
        off, lim = int(qs.get("offset") or 0), int(qs.get("limit") or 100)
        self.send(200, rss_xml(items[off:off + lim], self.host_url(), len(items), off), "application/rss+xml")

    def nzb(self, path, qs):
        if not self.key_ok(qs):
            return self.send(403, {"error": "bad api key"})
        payload = path[5:].rsplit(".nzb", 1)[0]
        doc = nzb_doc(payload)
        name = rel_name(b64d(payload).get("title") or "vodgrab")
        self.send(200, doc, "application/x-nzb", {"Content-Disposition": 'attachment; filename="%s.nzb"' % name})

    def sab(self, qs, method):
        raw = b""
        if method == "POST":
            raw = self.read_body()
            if "x-www-form-urlencoded" in (self.headers.get("Content-Type") or ""):
                for k, v in urllib.parse.parse_qs(raw.decode("utf-8", "replace")).items():
                    qs.setdefault(k, v[-1])
        mode = qs.get("mode") or ""
        if mode == "version":
            return self.send(200, {"version": "4.3.3"})
        if not self.key_ok(qs):
            return self.send(200, {"status": False, "error": "API Key Incorrect"})
        cat = qs.get("cat") or qs.get("category") or ""
        name = qs.get("name") or ""
        if mode == "auth":
            return self.send(200, {"auth": "apikey"})
        if mode == "get_config":
            return self.send(200, sab_config())
        if mode == "get_cats":
            return self.send(200, {"categories": ["*", "sonarr", "radarr"]})
        if mode in ("fullstatus", "status"):
            return self.send(200, {"status": {"completedir": os.path.dirname(folder("sonarr")), "downloaddir": folder("incomplete"),
                                              "paused": False, "version": "4.3.3", "diskspace1": 100}})
        if mode == "addfile":
            m = re.search(rb"VODGRAB:([A-Za-z0-9_\-]+)", raw)
            if not m:
                log("Refused an NZB from %s that did not come from VODgrab. In the arr, check that the VODgrab "
                    "download client has priority 50 and your real usenet client a lower number." % (cat or "an arr"),
                    "warn")
                return self.send(200, {"status": False, "error": "VODgrab only downloads VODgrab releases"})
            return self.send(200, {"status": True, "nzo_ids": [sab_add(m.group(1).decode(), cat)]})
        if mode == "addurl":
            m = re.search(r"/nzb/([A-Za-z0-9_\-]+)\.nzb", name)
            if not m:
                log("Refused a download URL from %s that did not come from VODgrab" % (cat or "an arr"), "warn")
                return self.send(200, {"status": False, "error": "VODgrab only downloads VODgrab releases"})
            return self.send(200, {"status": True, "nzo_ids": [sab_add(m.group(1), cat)]})
        if mode in ("queue", "history"):
            if name == "delete":
                sab_delete(qs.get("value"), qs.get("del_files") in ("1", "true"))
                return self.send(200, {"status": True})
            if name:
                return self.send(200, {"status": True})
            if mode == "queue":
                return self.send(200, sab_queue(cat))
            return self.send(200, sab_history(cat, int(qs.get("limit") or 100), int(qs.get("start") or 0)))
        if mode in ("pause", "resume", "change_cat", "switch", "retry", "change_opts", "set_config"):
            return self.send(200, {"status": True})
        self.send(200, {"status": False, "error": "Not supported: %s" % mode})

    def webhook(self, qs):
        if not self.key_ok(qs):
            return self.send(403, {"error": "bad api key"})
        try:
            body = json.loads(self.read_body() or b"{}")
        except ValueError:
            body = {}
        ev = body.get("eventType")
        if ev in ("Download", "MovieFileImported", "EpisodeFileImported"):
            did = body.get("downloadId")
            j = q1("SELECT * FROM jobs WHERE nzo=?", did) if did else None
            if j:
                upd(j["id"], status="imported", error="")
                d = os.path.dirname(j["path"] or "")
                if under(d, "sonarr", "radarr") and os.path.isdir(d) and not os.listdir(d):
                    os.rmdir(d)
                log("%s imported %s" % ((j["arr"] or "").title(), j["release"]))
        self.send(200, {"ok": True})

    def send_file(self, path, fname, ctype="application/gzip"):
        size = os.path.getsize(path)
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(size))
        self.send_header("Content-Disposition", 'attachment; filename="%s"' % fname)
        self.end_headers()
        with open(path, "rb") as f:
            shutil.copyfileobj(f, self.wfile, MB)

    def send_logs(self):
        """All log files back to back, oldest first, as one download."""
        with _log_lock:  # open and size them in one go; open files survive a rotation
            parts = []
            for path in log_files():
                f = open(path, "rb")
                parts.append((f, os.fstat(f.fileno()).st_size))
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(sum(n for _, n in parts)))
            self.send_header("Content-Disposition",
                             'attachment; filename="vodgrab-logs-%s.txt"' % time.strftime("%Y%m%d-%H%M"))
            self.end_headers()
            for f, n in parts:
                while n > 0:
                    chunk = f.read(min(MB, n))
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    n -= len(chunk)
        finally:
            for f, _ in parts:
                f.close()

    def play_stream(self, sid):
        """Pass the provider's stream to the browser, with byte ranges so the player can skip around."""
        sess = PLAY.get(sid)
        if not sess:
            return self.send(410, {"error": "The player was closed. Press Play again."})
        prev = sess.get("cur")
        if prev:
            prev.set()  # a new range request replaces the old one on the same connection
            for _ in range(30):
                if not sess["busy"]:
                    break
                time.sleep(0.1)
        me = threading.Event()
        sess["cur"] = me
        sess["busy"] = True
        p = prov(sess["pid"])
        headers = {"User-Agent": p["user_agent"] or DEFAULT_UA, "Accept": "*/*"}
        if self.headers.get("Range"):
            headers["Range"] = self.headers["Range"]
        try:
            try:
                up = urllib.request.urlopen(urllib.request.Request(
                    stream_url(sess["kind"], sess["cid"], sess["ext"]), headers=headers), timeout=30)
            except urllib.error.HTTPError as e:
                if e.code == 416:
                    return self.send(416, b"", "text/plain")
                return self.send(502, {"error": "%s answered HTTP %s" % (p["name"], e.code)})
            except (urllib.error.URLError, OSError) as e:
                return self.send(502, {"error": "%s unreachable: %s" % (p["name"], getattr(e, "reason", e))})
            with up:
                self.send_response(up.status)
                self.send_header("Content-Type", PLAY_TYPES.get((sess["ext"] or "").lower(),
                                                                up.headers.get("Content-Type") or "video/mp4"))
                self.send_header("Accept-Ranges", "bytes")
                self.send_header("Cache-Control", "no-store")
                for h in ("Content-Length", "Content-Range"):
                    if up.headers.get(h):
                        self.send_header(h, up.headers[h])
                if not up.headers.get("Content-Length"):
                    self.send_header("Connection", "close")
                    self.close_connection = True
                self.end_headers()
                if self.command == "HEAD":
                    return
                while not me.is_set() and not sess["stop"].is_set():
                    chunk = up.read(256 * 1024)
                    if not chunk:
                        break
                    try:
                        self.wfile.write(chunk)
                    except (BrokenPipeError, ConnectionResetError, OSError):
                        self.close_connection = True
                        break
                    sess["last"] = time.time()
                if me.is_set() or sess["stop"].is_set():
                    self.close_connection = True
        finally:
            if sess.get("cur") is me:
                sess["busy"] = False
            sess["last"] = time.time()

    def backup_api(self, method, sub, qs, data):
        if sub == "list":
            return self.send(200, backups_list())
        if sub == "download":
            if qs.get("name"):
                p = backup_file(qs["name"])
                return self.send_file(p, os.path.basename(p))
            if qs.get("what") == "config":
                body = json.dumps(config_export(), indent=1).encode()
                return self.send(200, body, "application/json", {
                    "Content-Disposition": 'attachment; filename="vodgrab-config-%s.json"' % time.strftime("%Y%m%d")})
            raise ValueError("Say which backup to download")
        if method != "POST":
            raise ValueError("Unknown backup action")
        if sub == "now":
            parts = data.get("parts") or None
            if data.get("wait"):
                p = backup_now(parts, keep_old=not data.get("extra"))
                return self.send(200, {"ok": True, "name": os.path.basename(p)})
            threading.Thread(target=lambda: backup_now(parts), daemon=True).start()
            return self.send(200, {"ok": True})
        if sub == "delete":
            os.remove(backup_file(data.get("name")))
            return self.send(200, {"ok": True})
        if sub == "upload":
            clean_uploads()
            tok = secrets.token_hex(8)
            d = os.path.join(DATA_DIR, "uploads", tok)
            os.makedirs(d, exist_ok=True)
            fname = os.path.basename(qs.get("filename") or "upload")
            p = os.path.join(d, "file")
            left = int(self.headers.get("Content-Length") or 0)
            if left <= 0:
                raise ValueError("Empty upload")
            with open(p, "wb") as f:
                while left > 0:
                    chunk = self.rfile.read(min(MB, left))
                    if not chunk:
                        break
                    f.write(chunk)
                    left -= len(chunk)
            try:
                found = inspect_upload(p, fname)
            except Exception:
                shutil.rmtree(d, ignore_errors=True)
                raise
            UPLOADS[tok] = {"dir": d, "found": found, "name": fname, "at": time.time()}
            return self.send(200, {"token": tok, "name": fname, "parts": list(found)})
        if sub == "restore":
            if data.get("token"):
                u = UPLOADS.get(data["token"])
                if not u:
                    raise ValueError("That upload expired, upload it again")
                return self.send(200, restore(u["found"], data.get("parts") or list(u["found"])))
            p = backup_file(data.get("name"))
            d = os.path.join(DATA_DIR, "uploads", "b" + secrets.token_hex(6))
            os.makedirs(d, exist_ok=True)
            shutil.copyfile(p, os.path.join(d, "file"))
            found = inspect_upload(os.path.join(d, "file"), data.get("name"))
            UPLOADS["b" + d[-12:]] = {"dir": d, "found": found, "name": data.get("name"), "at": time.time()}
            return self.send(200, restore(found, data.get("parts") or list(found)))
        raise ValueError("Unknown backup action")

    def api(self, method, p, qs):
        data = {}
        if method == "POST" and not p.startswith("backup/upload"):
            raw = self.read_body()
            data = json.loads(raw or b"{}") if raw else {}
        parts = p.strip("/").split("/")
        a = parts[0]
        if a == "status":
            return self.send(200, status_view())
        if a == "categories":
            kind = "series" if qs.get("kind") == "series" else "movie"
            many = len(active_providers()) > 1
            rows = q("SELECT id, name, prov FROM categories WHERE kind=? AND %s ORDER BY prov, name COLLATE NOCASE"
                     % live(True), kind)
            return self.send(200, [{"id": r["id"], "name": ("%s: %s" % (pname(r["prov"]), r["name"])) if many
                                    else r["name"]} for r in rows])
        if a == "browse":
            return self.send(200, browse(qs))
        if a == "wanted":
            if len(parts) > 1 and parts[1] == "refresh" and method == "POST":
                threading.Thread(target=refresh_library, daemon=True).start()
                return self.send(200, {"ok": True})
            if len(parts) > 1 and parts[1] == "search" and method == "POST":
                return self.send(200, wanted_search(data.get("movies"), data.get("episodes")))
            return self.send(200, wanted_view())
        if a == "facets":
            return self.send(200, facets(qs))
        if a == "library" and method == "POST":
            threading.Thread(target=refresh_library, daemon=True).start()
            return self.send(200, {"ok": True})
        if a == "movie" and len(parts) > 1:
            return self.send(200, movie_detail(int(parts[1])))
        if a == "series" and len(parts) > 1:
            return self.send(200, series_detail(int(parts[1])))
        if a == "add" and method == "POST":
            return self.send(200, add_many(data.get("items")))
        if a == "play":
            sub = parts[1] if len(parts) > 1 else ""
            if sub == "start" and method == "POST":
                return self.send(200, play_start("movie" if data.get("kind") == "movie" else "episode",
                                                 int(data.get("id"))))
            if sub == "stop" and method == "POST":
                play_stop(data.get("session"))
                return self.send(200, {"ok": True})
            return self.play_stream(sub)
        if a == "probe" and len(parts) > 1 and parts[1] == "open" and method == "POST":
            return self.send(200, probe_open(data.get("items")))
        if a == "probe" and method == "POST":
            return self.send(200, probe(data.get("kind"), int(data.get("id"))))
        if a == "download" and method == "POST":
            jobs, notes = enqueue_manual(data.get("kind"), int(data.get("id")), data.get("season"), bool(data.get("now")),
                                         data.get("dp"))
            return self.send(200, {"queued": len(jobs), "notes": notes})
        if a == "queue":
            rows = q("SELECT * FROM jobs WHERE status IN ('queued','retry_wait','downloading','verifying','importing') "
                     "ORDER BY CASE status WHEN 'downloading' THEN 0 WHEN 'verifying' THEN 0 WHEN 'importing' THEN 1 "
                     "ELSE 2 END, force DESC, pos")
            return self.send(200, [job_row(j) for j in rows])
        if a == "history":
            rows = q("SELECT * FROM jobs WHERE status NOT IN ('queued','retry_wait','downloading','verifying',"
                     "'importing') ORDER BY updated DESC LIMIT ?", int(qs.get("limit") or 300))
            return self.send(200, [job_row(j) for j in rows])
        if a == "job" and method == "POST" and len(parts) > 2:
            return self.send(200, job_action(int(parts[1]), parts[2]))
        if a == "pause" and method == "POST":
            save_settings({"paused": bool(data.get("paused"))})
            if not data.get("paused"):
                ENGINE.prov_errors.clear()
                ENGINE.cool.clear()
            return self.send(200, {"ok": True})
        if a == "settings":
            if method == "POST":
                upd_ = {k: v for k, v in data.items() if k in DEFAULTS and k != "api_key"}
                for k in SECRET_KEYS:
                    if k in upd_ and upd_[k] == "" and not data.get("clear_" + k):
                        upd_.pop(k)
                save_settings(upd_)
                ensure_dirs()
                _info_cache.clear()
            return self.send(200, settings_view())
        if a == "apikey" and method == "POST":
            save_settings({"api_key": secrets.token_hex(16)})
            return self.send(200, {"api_key": S()["api_key"]})
        if a == "sync" and method == "POST":
            if SYNC["running"]:
                return self.send(200, {"ok": False, "message": "A sync is already running"})
            threading.Thread(target=run_sync, args=("manual",), daemon=True).start()
            return self.send(200, {"ok": True})
        if a == "syncruns" and len(parts) > 2 and parts[2] == "changes":
            r = q1("SELECT * FROM sync_runs WHERE id=?", to_int(parts[1]))
            path = sync_changes_path(r["id"]) if r else ""
            if not path or not os.path.isfile(path):
                return self.send(404, {"error": "No change list for that sync (only the last %d are kept)"
                                                % SYNC_CHANGES_KEEP})
            return self.send_file(path, "vodgrab-sync-%s.txt" % time.strftime(
                "%Y%m%d-%H%M", time.localtime(r["started"])), "text/plain; charset=utf-8")
        if a == "syncruns":
            return self.send(200, [dict(r, changes=os.path.isfile(sync_changes_path(r["id"])))
                                   for r in q("SELECT * FROM sync_runs ORDER BY id DESC LIMIT 15")])
        if a == "unmatched":
            return self.send(200, [dict(r) for r in q("SELECT * FROM unmatched ORDER BY seen DESC")])
        if a == "override" and method == "POST":
            arr, arr_id, xid = data["arr"], str(data["arr_id"]), int(data["xid"])
            ex("INSERT OR REPLACE INTO overrides(arr, arr_id, xid) VALUES(?,?,?)", arr, arr_id, xid)
            ex("DELETE FROM unmatched WHERE arr=? AND arr_id=?", arr, arr_id)
            ex("DELETE FROM matches WHERE arr=? AND arr_id=?", arr, arr_id)
            return self.send(200, {"ok": True})
        if a == "link" and method == "POST":
            return self.send(200, link_change(data))
        if a == "arrsearch":
            return self.send(200, arr_lib_search("movie" if qs.get("kind") == "movie" else "series", qs.get("q")))
        if a == "dismiss" and method == "POST":
            ex("DELETE FROM unmatched WHERE arr=? AND arr_id=?", data["arr"], str(data["arr_id"]))
            return self.send(200, {"ok": True})
        if a == "providers":
            if method == "POST":
                if len(parts) > 2 and parts[2] == "delete":
                    delete_provider(int(parts[1]))
                else:
                    pid = save_provider(data)
                    return self.send(200, {"id": pid, "providers": providers_view()})
            return self.send(200, providers_view())
        if a == "test" and len(parts) > 2 and parts[1] == "provider":
            return self.send(200, test_provider(int(parts[2])))
        if a == "test" and len(parts) > 1:
            return self.send(200, test_target(parts[1]))
        if a == "setup" and method == "POST" and len(parts) > 1:
            return self.send(200, setup_arr(parts[1], data.get("external_url")))
        if a == "arrmeta" and len(parts) > 1:
            return self.send(200, arr_meta(parts[1]))
        if a == "meta":
            sub = parts[1] if len(parts) > 1 else ""
            if sub == "fill" and method == "POST":
                META.update(force=True, auth_bad=False, error="")
                META["wake"].set()
                return self.send(200, {"ok": True})
            if sub == "test" and method == "POST":
                return self.send(200, test_meta())
            return self.send(200, meta_status())
        if a == "backup":
            return self.backup_api(method, parts[1] if len(parts) > 1 else "", qs, data)
        if a == "report":
            body = json.dumps(catalog_report(), indent=1, ensure_ascii=False).encode()
            return self.send(200, body, "application/json", {
                "Content-Disposition": 'attachment; filename="vodgrab-catalog-report.json"'})
        if a == "dispatcharr":
            sub = parts[1] if len(parts) > 1 else ""
            if method == "POST" and sub == "mode":
                return self.send(200, dispatcharr_mode(data))
            return self.send(200, dispatcharr_connect(data if method == "POST" else {}))
        if a == "update":
            if method == "POST" and len(parts) > 1 and parts[1] == "apply":
                return self.send(200, apply_update())
            if method == "POST":
                return self.send(200, check_update())
            return self.send(200, update_view())
        if a == "logs" and len(parts) > 1 and parts[1] == "download":
            return self.send_logs()
        if a == "logs":
            return self.send(200, list(LOG)[-300:])
        self.send(404, {"error": "unknown endpoint"})


# ------------------------------------------------------------------ dispatcharr

def dispatcharr_call(path, url=None, key=None):
    """GET from Dispatcharr's API with an API key (Dispatcharr: Users, edit your user, API key)."""
    url = (url if url is not None else S().get("dispatcharr_url") or "").strip().rstrip("/")
    key = (key or S().get("dispatcharr_api_key") or "").strip()
    if not url or not key:
        raise ValueError("Enter Dispatcharr's address and an API key first")
    if not re.match(r"^https?://", url):
        url = "http://" + url
    try:
        _, raw = http(url + path, headers={"X-API-Key": key, "Accept": "application/json"}, timeout=30)
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            raise ValueError("Dispatcharr refused the API key (HTTP %s)" % e.code)
        raise ValueError("Dispatcharr returned HTTP %s for %s" % (e.code, path))
    except (urllib.error.URLError, OSError) as e:
        raise ValueError("Dispatcharr unreachable at %s: %s" % (url, getattr(e, "reason", e)))
    try:
        data = json.loads(raw.decode("utf-8", "replace"))
    except ValueError:
        raise ValueError("Dispatcharr's answer was not JSON; check the address (for example http://dispatcharr:9191)")
    return data.get("results", data) if isinstance(data, dict) and "results" in data else data


def dispatcharr_base():
    url = (S().get("dispatcharr_url") or "").strip().rstrip("/")
    return url if not url or re.match(r"^https?://", url) else "http://" + url


DP_SELF = "dispatcharr:self"


def dispatcharr_logins():
    """Every login Dispatcharr has for its Xtream accounts: one per active profile, each with its own stream limit.
    Dispatcharr keeps each profile's username and password from the provider's own account info once it has
    refreshed that profile; those stay on this server and are never sent to the browser."""
    out = []
    for a in dispatcharr_call("/api/m3u/accounts/") or []:
        if not isinstance(a, dict) or a.get("account_type") != "XC" or not a.get("server_url"):
            continue
        server = a["server_url"].strip().rstrip("/")
        profiles = [p for p in a.get("profiles") or [] if isinstance(p, dict)] or [
            {"id": 0, "name": "Default", "is_default": True, "is_active": True, "max_streams": a.get("max_streams")}]
        for p in profiles:
            if not p.get("is_active", True):
                continue
            ui = ((p.get("custom_properties") or {}).get("user_info") or {}) if isinstance(
                p.get("custom_properties"), dict) else {}
            user = ui.get("username") or (a.get("username") if p.get("is_default") else "") or ""
            limit = to_int(p.get("max_streams")) or to_int(ui.get("max_connections")) or 0
            out.append({"key": "dispatcharr:%s:%s" % (a.get("id"), p.get("id")),
                        "name": a.get("name") if p.get("is_default") and len(profiles) == 1 else
                        "%s · %s" % (a.get("name"), p.get("name") or "Profile %s" % p.get("id")),
                        "account": a.get("name"), "profile": p.get("name"), "server_url": server, "username": user,
                        "password": ui.get("password") or "", "max_streams": limit,
                        "active": bool(a.get("is_active", True))})
    return out


def dp_provider(source):
    return next((p for p in PROVIDERS.values() if (p.get("source") or "") == source), None)


def dispatcharr_connect(data):
    """Check the address and key, save them, and describe what Dispatcharr offers."""
    url = (data.get("url") or S().get("dispatcharr_url") or "").strip().rstrip("/")
    key = (data.get("api_key") or "").strip() or S().get("dispatcharr_api_key") or ""
    me = dispatcharr_call("/api/accounts/users/me/", url, key)
    save_settings({"dispatcharr_url": url, "dispatcharr_api_key": key})
    logins = dispatcharr_logins()
    have = {(p["url"].rstrip("/").lower(), (p["username"] or "").lower()) for p in PROVIDERS.values()}
    view = []
    for l in logins:
        pr = dp_provider(l["key"])
        v = {k: l[k] for k in ("key", "name", "account", "profile", "server_url", "username", "max_streams", "active")}
        v.update(has_password=bool(l["password"]), provider=pr["id"] if pr else None,
                 enabled=bool(pr and pr["enabled"]),
                 added=bool(pr) or (l["server_url"].lower(), l["username"].lower()) in have)
        view.append(v)
    props = (me.get("custom_properties") or {}) if isinstance(me, dict) else {}
    me_pr = dp_provider(DP_SELF)
    return {"user": me.get("username") if isinstance(me, dict) else "", "xc_ready": bool(props.get("xc_password")),
            "logins": view, "total_streams": sum(l["max_streams"] for l in logins if l["active"]),
            "proxy": bool(S().get("dispatcharr_proxy")), "import": bool(S().get("dispatcharr_import")),
            "self_provider": {"id": me_pr["id"], "max_conn": me_pr["max_conn"], "enabled": bool(me_pr["enabled"])}
            if me_pr else None}


def dispatcharr_self(enable=True):
    """Download through Dispatcharr: its Xtream output, logged in as the API key's user."""
    pr = dp_provider(DP_SELF) or next((p for p in PROVIDERS.values() if not p.get("source") and
                                       p["url"].rstrip("/").lower() == dispatcharr_base().lower()), None)
    if not enable:
        if pr and pr["enabled"]:
            save_provider(dict(pr, enabled=False, password=""))
            log("Stopped downloading through Dispatcharr (its provider is disabled, not removed)")
        return
    me = dispatcharr_call("/api/accounts/users/me/")
    props = me.get("custom_properties") or {}
    if not props.get("xc_password"):
        raise ValueError("Your Dispatcharr user %s has no XC password yet. Set one in Dispatcharr (Users, edit your "
                         "user, XC password), then try again." % me.get("username"))
    save_provider({"id": pr["id"] if pr else None, "name": pr["name"] if pr else "Dispatcharr",
                   "url": dispatcharr_base(), "username": me.get("username"), "password": props["xc_password"],
                   "max_conn": pr["max_conn"] if pr else 2, "priority": pr["priority"] if pr else 0,
                   "user_agent": pr["user_agent"] if pr else "", "enabled": True, "source": DP_SELF})
    if not pr or not pr["enabled"]:
        log("Downloading through Dispatcharr as user %s" % me.get("username"))


def dispatcharr_import(items):
    """Add or update the chosen Dispatcharr logins as providers. A password typed in the form wins; otherwise the
    one Dispatcharr has is used."""
    logins = {l["key"]: l for l in dispatcharr_logins()}
    todo = [(logins[it["key"]], (it.get("password") or "").strip()) for it in items or [] if it.get("key") in logins]
    missing = [l["name"] for l, pw in todo if not (pw or l["password"] or dp_provider(l["key"]))]
    if missing:  # check everything before changing anything
        raise ValueError("Dispatcharr has no password for %s yet: type it in, or refresh that account in Dispatcharr"
                         % ", ".join(missing))
    done = []
    for l, pw in todo:
        pw = pw or l["password"]
        pr = dp_provider(l["key"])
        save_provider({"id": pr["id"] if pr else None, "name": pr["name"] if pr else l["name"],
                       "url": l["server_url"], "username": l["username"] or (pr["username"] if pr else ""),
                       "password": pw, "max_conn": max(1, min(20, l["max_streams"] or 1)),
                       "priority": pr["priority"] if pr else 0, "user_agent": pr["user_agent"] if pr else "",
                       "enabled": l["active"], "source": l["key"]})
        done.append(l["name"])
    if not done:
        raise ValueError("Tick the logins you want to import")
    log("Imported from Dispatcharr: %s" % ", ".join(done))
    return done


def dispatcharr_mode(data):
    """The two ways to use Dispatcharr, switched on or off independently."""
    proxy, imp = bool(data.get("proxy")), bool(data.get("import"))
    if imp and data.get("logins") is not None:  # first, so a refused import changes nothing
        dispatcharr_import(data.get("logins"))
    dispatcharr_self(proxy)
    for p in list(PROVIDERS.values()):
        src = p.get("source") or ""
        if src.startswith("dispatcharr:") and src != DP_SELF:
            keep = imp and (data.get("logins") is None or any(i.get("key") == src for i in data["logins"]))
            if bool(p["enabled"]) != keep:
                save_provider(dict(p, enabled=keep, password=""))
    save_settings({"dispatcharr_proxy": proxy, "dispatcharr_import": imp})
    return dict(dispatcharr_connect({}), providers=providers_view())


DP_COPIES = {}  # catalog id -> (fetched at, copies)


def dispatcharr_copies(cid):
    """For a movie that comes through Dispatcharr: every provider copy Dispatcharr has, from its native VOD API
    (the Xtream output merges them into one). Each can be picked for a download."""
    pid, raw = dec(cid)
    p = PROVIDERS.get(pid)
    if not p or (p.get("source") or "") != DP_SELF:
        return None
    hit = DP_COPIES.get(cid)
    if hit and time.time() - hit[0] < 600:
        return hit[1]
    out = []
    for r in dispatcharr_call("/api/vod/movies/%s/providers/" % raw) or []:
        if not isinstance(r, dict):
            continue
        acc = r.get("m3u_account") if isinstance(r.get("m3u_account"), dict) else {"id": r.get("m3u_account")}
        qi = r.get("quality_info") or {}
        quality = qi.get("quality") or qi.get("resolution") or ""
        if not quality and qi.get("width") and qi.get("height"):
            quality = "%sx%s" % (qi["width"], qi["height"])
        out.append({"account_id": acc.get("id"), "account": acc.get("name") or "Account %s" % acc.get("id"),
                    "stream_id": str(r.get("stream_id") or ""), "ext": r.get("container_extension") or "",
                    "quality": str(quality)})
    DP_COPIES[cid] = (time.time(), out)
    return out


def dispatcharr_refresh():
    """Before a sync: pick up changed passwords and stream limits from Dispatcharr for the providers it made."""
    if not (S().get("dispatcharr_url") and S().get("dispatcharr_api_key")) or not any(
            (p.get("source") or "").startswith("dispatcharr:") and p["enabled"] for p in PROVIDERS.values()):
        return
    try:
        if S().get("dispatcharr_proxy"):
            dispatcharr_self(True)
        if S().get("dispatcharr_import"):
            for l in dispatcharr_logins():
                pr = dp_provider(l["key"])
                if not pr or not pr["enabled"] or not l["password"]:
                    continue
                limit = max(1, min(20, l["max_streams"] or pr["max_conn"]))
                if (l["password"], l["username"] or pr["username"], limit) != (pr["password"], pr["username"],
                                                                                pr["max_conn"]):
                    save_provider(dict(pr, username=l["username"] or pr["username"], password=l["password"],
                                       max_conn=limit))
                    log("Updated %s from Dispatcharr" % pr["name"])
    except Exception as e:
        log("Could not refresh logins from Dispatcharr: %s" % e, "warn")


# ------------------------------------------------------------------ updates

UPDATE_URL = os.environ.get("VODGRAB_UPDATE_URL",
                            "https://raw.githubusercontent.com/Deekerman/VODgrab/main/vodgrab.py")
CHANGELOG_URL = os.environ.get("VODGRAB_CHANGELOG_URL",
                               "https://raw.githubusercontent.com/Deekerman/VODgrab/main/CHANGELOG.md")
IN_DOCKER = os.environ.get("VODGRAB_DOCKER") == "1" or os.path.exists("/.dockerenv")
UPDATE = {"latest": "", "notes": "", "checked": 0, "error": "", "applying": False}
UPDATE_HEADERS = {"User-Agent": "VODgrab/%s" % VERSION, "Cache-Control": "no-cache"}


def vtuple(v):
    return tuple(int(x) for x in re.findall(r"\d+", v or "")[:3])


def remote_version(text):
    m = re.search(r'^VERSION = "([0-9][^"]*)"', text, re.M)
    return m.group(1) if m else ""


def release_notes(newer_than):
    """The CHANGELOG.md sections for versions newer than this one."""
    try:
        _, raw = http(CHANGELOG_URL, headers=UPDATE_HEADERS, timeout=20)
    except Exception:
        return ""
    keep, out = False, []
    for line in raw.decode("utf-8", "replace").splitlines():
        m = re.match(r"^##\s+v?(\d+\.\d+\.\d+)", line)
        if m:
            keep = vtuple(m.group(1)) > vtuple(newer_than)
        if keep:
            out.append(line)
    return "\n".join(out).strip()


def check_update():
    try:
        _, raw = http(UPDATE_URL, headers=dict(UPDATE_HEADERS, Range="bytes=0-8191"), timeout=20)
        latest = remote_version(raw.decode("utf-8", "replace"))
        if not latest:
            raise RuntimeError("could not read the version from %s" % UPDATE_URL)
        newer = vtuple(latest) > vtuple(VERSION)
        UPDATE.update(latest=latest, notes=release_notes(VERSION) if newer else "", error="")
        if newer and UPDATE.get("logged") != latest:
            UPDATE["logged"] = latest
            log("VODgrab %s is available (you have %s)" % (latest, VERSION))
    except Exception as e:
        UPDATE["error"] = str(e)
    UPDATE["checked"] = time.time()
    return update_view()


def update_view():
    me = os.path.abspath(__file__)
    writable = os.access(me, os.W_OK) and os.access(os.path.dirname(me), os.W_OK)
    return {"current": VERSION, "latest": UPDATE["latest"], "notes": UPDATE["notes"], "error": UPDATE["error"],
            "checked": UPDATE["checked"], "docker": IN_DOCKER, "applying": UPDATE["applying"],
            "available": bool(UPDATE["latest"]) and vtuple(UPDATE["latest"]) > vtuple(VERSION),
            "can_update": not IN_DOCKER and writable, "path": me,
            "manual": "sudo curl -fsSL -o %s %s && sudo systemctl restart vodgrab" % (me, UPDATE_URL)}


def apply_update():
    """Replace this file with the newest vodgrab.py from GitHub, then restart in place."""
    v = update_view()
    if IN_DOCKER:
        raise ValueError("This copy runs in Docker. Update with: docker compose pull && docker compose up -d")
    if UPDATE["applying"]:
        raise ValueError("An update is already being installed")
    if not v["can_update"]:
        raise ValueError("VODgrab cannot write to %s. Update by hand: %s" % (os.path.dirname(v["path"]), v["manual"]))
    try:
        _, raw = http(UPDATE_URL, headers=UPDATE_HEADERS, timeout=180)
    except Exception as e:
        raise ValueError("Download failed: %s" % e)
    text = raw.decode("utf-8")
    latest = remote_version(text)
    if not latest or "def main():" not in text:
        raise ValueError("The downloaded file does not look like VODgrab; nothing was changed")
    if vtuple(latest) <= vtuple(VERSION):
        raise ValueError("You already have the latest version (%s)" % VERSION)
    try:
        compile(text, v["path"], "exec")
    except SyntaxError as e:
        raise ValueError("The downloaded file is damaged (%s); nothing was changed" % e)
    me = v["path"]
    new = me + ".new"
    with open(new, "w", encoding="utf-8") as f:
        f.write(text)
    shutil.copymode(me, new)
    # Start the new file once on its own; it must load and report its version before it replaces this one
    try:
        out = subprocess.run([sys.executable, new, "version"], capture_output=True, text=True, timeout=60,
                             env=dict(os.environ, VODGRAB_DATA=os.path.join(DATA_DIR, "update-check")))
        ok = out.returncode == 0 and out.stdout.strip() == latest
        why = (out.stderr or out.stdout).strip().splitlines()[-1:] if not ok else []
    except (OSError, subprocess.TimeoutExpired) as e:
        ok, why = False, [str(e)]
    shutil.rmtree(os.path.join(DATA_DIR, "update-check"), ignore_errors=True)
    if not ok:
        os.remove(new)
        raise ValueError("The new version did not start (%s); nothing was changed" % (why[0] if why else "no output"))
    shutil.copy2(me, me + ".bak")
    os.replace(new, me)
    UPDATE["applying"] = True
    log("Updating VODgrab %s to %s and restarting (previous version saved as %s.bak)" % (VERSION, latest, me))
    threading.Thread(target=restart_self, daemon=True).start()
    return {"ok": True, "version": latest}


def restart_self():
    time.sleep(1.5)  # let the browser get its answer first
    os.execv(sys.executable, [sys.executable, os.path.abspath(__file__)] + sys.argv[1:])


def update_worker():
    time.sleep(60)
    while True:
        if S().get("update_check"):
            check_update()
        time.sleep(24 * 3600)


# ------------------------------------------------------------------ commands


def cmd_serve(args):
    init_db()
    finish_staged_restore()
    shutil.rmtree(os.path.join(DATA_DIR, "uploads"), ignore_errors=True)
    os.umask(0)
    try:
        ensure_dirs()
    except OSError as e:
        log("Could not create download folders under %s: %s (set the base path in Settings)" % (S()["base_path"], e),
            "warn")
    port = getattr(args, "port", None) or S()["port"]
    ENGINE.start()
    threading.Thread(target=scheduler, daemon=True).start()
    threading.Thread(target=probe_background, daemon=True).start()
    threading.Thread(target=meta_worker, daemon=True).start()
    threading.Thread(target=play_reaper, daemon=True).start()
    threading.Thread(target=update_worker, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", int(port)), Handler)
    srv.daemon_threads = True
    log("VODgrab %s listening on port %s (data in %s)" % (VERSION, port, DATA_DIR))
    if not FFPROBE:
        log("ffprobe not found: downloads are checked by size only. Install ffmpeg for full verification.", "warn")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


def cmd_sync(args):
    init_db()
    ok = run_sync("cli")
    sys.exit(0 if ok else 1)


def cmd_install(args):
    if os.geteuid() != 0:
        print("Run install as root.")
        sys.exit(1)
    unit = """[Unit]
Description=VODgrab (Xtream VOD for Sonarr and Radarr)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=%s %s serve
WorkingDirectory=%s
Environment=PYTHONUNBUFFERED=1
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
""" % (sys.executable, os.path.abspath(__file__), HERE)
    with open("/etc/systemd/system/vodgrab.service", "w") as f:
        f.write(unit)
    subprocess.run(["systemctl", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "enable", "--now", "vodgrab"], check=True)
    subprocess.run(["systemctl", "restart", "vodgrab"], check=True)
    init_db()
    print("VODgrab is running. Open http://<this-host>:%s" % S()["port"])


def main():
    ap = argparse.ArgumentParser(description="VODgrab")
    sub = ap.add_subparsers(dest="cmd")
    sp = sub.add_parser("serve")
    sp.add_argument("--port", type=int)
    sub.add_parser("install")
    sub.add_parser("sync")
    sub.add_parser("version")
    args = ap.parse_args()
    if args.cmd == "version":
        print(VERSION)
        return
    {"serve": cmd_serve, "install": cmd_install, "sync": cmd_sync}.get(args.cmd or "serve")(args)


# App icon: 64 px for the browser tab, 180 px for phone home-screen shortcuts
FAVICON_PNG = base64.b64decode("""
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
""")
TOUCH_ICON_PNG = base64.b64decode("""
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
""")
ICONS = {"/favicon.ico": FAVICON_PNG, "/favicon.png": FAVICON_PNG, "/apple-touch-icon.png": TOUCH_ICON_PNG}

UI_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" type="image/png" href="/favicon.png">
<link rel="apple-touch-icon" href="/apple-touch-icon.png">
<title>VODgrab</title>
<style>
:root{--bg:#0f1115;--panel:#171a21;--panel2:#1e222b;--line:#2a2f3a;--text:#e6e8ec;--dim:#8b93a3;--acc:#f0a53a;--ok:#4cc38a;--bad:#ef5b5b;--info:#5b9cef;--r:10px}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
a{color:var(--acc)}
header{position:sticky;top:0;z-index:5;background:rgba(15,17,21,.95);backdrop-filter:blur(6px);border-bottom:1px solid var(--line)}
.bar{display:flex;align-items:center;gap:14px;padding:10px 16px;flex-wrap:wrap}
.brand{font-weight:700;font-size:17px;letter-spacing:.3px}
.brand span{color:var(--acc)}
.brand img{display:block;height:36px;width:auto}
nav{display:flex;gap:4px;flex-wrap:wrap}
nav button{background:none;border:0;color:var(--dim);padding:7px 11px;border-radius:8px;cursor:pointer;font:inherit}
nav button.on{background:var(--panel2);color:var(--text)}
nav .n{background:var(--acc);color:#111;border-radius:9px;padding:0 6px;font-size:11px;margin-left:4px;font-weight:700}
.pills{margin-left:auto;display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.pill{font-size:12px;padding:3px 9px;border-radius:20px;background:var(--panel2);color:var(--dim);white-space:nowrap}
.pill.ok{color:var(--ok)}.pill.bad{color:var(--bad)}.pill.warn{color:var(--acc)}
.banner{background:#3a1d1d;color:#ffd6d6;padding:9px 16px;display:none}
main{padding:16px;max-width:1400px;margin:0 auto}
.btn{background:var(--panel2);border:1px solid var(--line);color:var(--text);padding:7px 12px;border-radius:8px;cursor:pointer;font:inherit;white-space:nowrap}
a.btn{text-decoration:none;display:inline-block}
.btn:hover{border-color:#3b4250}
.btn.p{background:var(--acc);border-color:var(--acc);color:#111;font-weight:600}
.btn.s{padding:4px 9px;font-size:12px}
.btn.d{color:var(--bad)}
.btn:disabled{opacity:.5;cursor:default}
input,select,textarea{background:var(--panel2);border:1px solid var(--line);color:var(--text);padding:7px 10px;border-radius:8px;font:inherit;width:100%}
textarea{min-height:70px;resize:vertical}
input[type=checkbox]{width:auto}
.tools{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:14px;align-items:center}
.tools input[type=search]{flex:1;min-width:180px}
.tools select{width:auto;max-width:260px}
.seg{display:inline-flex;background:var(--panel2);border:1px solid var(--line);border-radius:8px;overflow:hidden}
.seg button{background:none;border:0;color:var(--dim);padding:7px 12px;cursor:pointer;font:inherit}
.seg button.on{background:var(--acc);color:#111;font-weight:600}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:14px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:var(--r);overflow:hidden;cursor:pointer;transition:transform .12s}
.card:hover{transform:translateY(-2px);border-color:#3b4250}
.poster{aspect-ratio:2/3;background:var(--panel2) center/cover no-repeat;display:flex;align-items:center;justify-content:center;color:var(--dim);font-size:12px;text-align:center;padding:8px}
.card .t{padding:7px 9px;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.card .y{color:var(--dim);font-size:12px}
.more{text-align:center;margin:18px 0}
.muted{color:var(--dim)}
.list{display:flex;flex-direction:column;gap:8px}
.row{background:var(--panel);border:1px solid var(--line);border-radius:var(--r);padding:10px 12px;display:flex;gap:12px;align-items:center;flex-wrap:wrap}
.row .main{flex:1;min-width:220px}
.row .title{font-weight:600;word-break:break-word}
.row .sub{color:var(--dim);font-size:12px;word-break:break-word}
.row .acts{display:flex;gap:6px;flex-wrap:wrap}
.prog{height:6px;background:var(--panel2);border-radius:4px;overflow:hidden;margin-top:6px}
.prog i{display:block;height:100%;background:var(--acc)}
.tag{font-size:11px;padding:2px 7px;border-radius:5px;background:var(--panel2);color:var(--dim);margin-right:6px}
.st{font-size:11px;padding:2px 8px;border-radius:5px;font-weight:600}
.st.imported,.st.completed{background:#16352a;color:var(--ok)}
.st.failed,.st.import_failed{background:#3a1d1d;color:var(--bad)}
.st.downloading,.st.verifying,.st.importing{background:#1c2c45;color:var(--info)}
.st.queued,.st.retry_wait{background:#352a16;color:var(--acc)}
.st.cancelled,.st.retried{background:var(--panel2);color:var(--dim)}
.modal{position:fixed;inset:0;background:rgba(0,0,0,.6);display:none;align-items:flex-start;justify-content:center;z-index:20;overflow:auto;padding:30px 12px}
.modal.on{display:flex}
.sheet{background:var(--panel);border:1px solid var(--line);border-radius:14px;max-width:900px;width:100%;overflow:hidden}
.sheet .head{display:flex;gap:18px;padding:18px}
.sheet .head .poster{width:160px;flex:none;border-radius:8px}
.sheet h2{margin:0 0 4px;font-size:20px}
.sheet .body{padding:0 18px 18px}
.x{float:right;background:none;border:0;color:var(--dim);font-size:22px;cursor:pointer}
.eps{display:flex;flex-direction:column;gap:6px;max-height:50vh;overflow:auto;margin-top:10px}
.ep{display:flex;gap:10px;align-items:center;background:var(--panel2);border-radius:8px;padding:7px 10px}
.ep .n{color:var(--acc);font-weight:600;width:52px;flex:none}
.ep .et{flex:1;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.tabs{display:flex;gap:6px;flex-wrap:wrap}
section.set{background:var(--panel);border:1px solid var(--line);border-radius:var(--r);padding:14px 16px;margin-bottom:14px}
section.set h3{margin:0 0 10px;font-size:15px}
.fields{display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:12px}
.f label{display:block;font-size:12px;color:var(--dim);margin-bottom:4px}
.f .hint{font-size:11px;color:var(--dim);margin-top:3px}
.chk{display:flex;gap:8px;align-items:center;padding-top:20px}
.win{display:flex;gap:8px;align-items:center;flex-wrap:wrap;background:var(--panel2);padding:8px;border-radius:8px;margin-bottom:8px}
.win .days label{font-size:12px;margin-right:6px;white-space:nowrap}
.win input[type=time]{width:auto}.win input[type=number]{width:70px}
.out{white-space:pre-wrap;background:var(--panel2);border-radius:8px;padding:10px;font:12px/1.4 ui-monospace,Menlo,Consolas,monospace;margin-top:10px;max-height:340px;overflow:auto;display:none}
.toast{position:fixed;bottom:18px;left:50%;transform:translateX(-50%);background:var(--panel2);border:1px solid var(--line);padding:10px 16px;border-radius:10px;z-index:40;display:none;max-width:90vw}
.toast.bad{border-color:var(--bad);color:#ffd6d6}
.empty{padding:40px;text-align:center;color:var(--dim)}
.prov{background:var(--panel2);border:1px solid var(--line);border-radius:10px;padding:12px;margin-bottom:10px}
.prov .ph{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:10px}
.prov .ph b{font-size:14px}
.ptag{font-size:10px;padding:1px 6px;border-radius:4px;background:#23304a;color:#9fc0f5;margin-left:6px;vertical-align:middle}
.path{font:12px ui-monospace,Menlo,Consolas,monospace;color:var(--ok)}
.q{font-size:11px;padding:2px 7px;border-radius:5px;font-weight:700;background:#1c2c45;color:#9fc0f5}
.q.checking{animation:pulse 1s ease-in-out infinite}@keyframes pulse{50%{opacity:.35}}
.q.k2160p{background:#3a2a10;color:#f0c35a}.q.k1080p{background:#16352a;color:var(--ok)}.q.k720p{background:#1c2c45;color:#9fc0f5}.q.k480p,.q.unk{background:var(--panel2);color:var(--dim)}
.srcs{display:flex;flex-direction:column;gap:6px;margin:12px 0}
.src{display:flex;gap:10px;align-items:center;flex-wrap:wrap;background:var(--panel2);border-radius:8px;padding:8px 10px}
.src .pn{font-weight:600;min-width:90px}.src .mi{flex:1;min-width:160px;color:var(--dim);font-size:12px}
.fpanel{background:var(--panel);border:1px solid var(--line);border-radius:var(--r);padding:12px 14px;margin-bottom:12px}
.fg{margin-bottom:10px}.fgt{font-size:12px;color:var(--dim);margin-bottom:6px;font-weight:600}
.fgrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(230px,1fr));gap:4px 16px}
.fchips{display:flex;flex-wrap:wrap;gap:6px}
.fc{background:var(--panel2);border:1px solid var(--line);color:var(--text);border-radius:16px;padding:4px 10px;font:inherit;font-size:12px;cursor:pointer}
.fc span{color:var(--dim);margin-left:2px}.fc.on{background:var(--acc);border-color:var(--acc);color:#111}.fc.on span{color:#333}.fc.more{color:var(--acc);background:none}
.frow{display:flex;gap:8px;align-items:center}.frow input{width:110px}
.chips{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:10px;align-items:center}
.chip{display:inline-flex;align-items:center;gap:4px;background:#2b2415;color:var(--acc);border:1px solid #4a3b1c;border-radius:16px;padding:3px 4px 3px 10px;font-size:12px}
.chip button{background:none;border:0;color:var(--acc);cursor:pointer;font-size:14px;line-height:1;padding:0 4px}
.tools .n{background:var(--acc);color:#111;border-radius:9px;padding:0 6px;font-size:11px;margin-left:6px;font-weight:700}
.poster{position:relative}.bdg{position:absolute;left:6px;top:6px;display:flex;gap:4px;flex-wrap:wrap}
.lb{font-size:10px;padding:2px 6px;border-radius:5px;font-weight:700;background:rgba(0,0,0,.7)}.lb1{color:#9fc0f5}.lb2{color:var(--ok)}
.tick{display:none;position:absolute;right:6px;top:6px;width:24px;height:24px;border-radius:50%;background:rgba(0,0,0,.6);border:2px solid #fff;color:transparent;align-items:center;justify-content:center;font-size:14px}
.selmode .tick{display:flex}.sel .tick{background:var(--acc);border-color:var(--acc);color:#111}.card.sel{border-color:var(--acc)}
.bulk[hidden]{display:none}.card .y{white-space:normal}
.bulk{position:fixed;bottom:16px;left:50%;transform:translateX(-50%);background:var(--panel2);border:1px solid var(--acc);border-radius:12px;padding:8px 12px;display:flex;gap:10px;align-items:center;z-index:15;box-shadow:0 6px 24px rgba(0,0,0,.5)}
.ltable{width:100%;border-collapse:collapse;font-size:13px}.ltable th{text-align:left;color:var(--dim);font-weight:600;font-size:12px;padding:6px 8px;border-bottom:1px solid var(--line)}
.ltable td{padding:7px 8px;border-bottom:1px solid var(--line)}.lrow{cursor:pointer}.lrow:hover{background:var(--panel)}.lt{font-weight:600}
.tk{width:20px;color:transparent}.lrow.selmode .tk{color:var(--dim)}.lrow.sel .tk{color:var(--acc)}.lrow.sel{background:#2b2415}
@media(max-width:700px){.ltable th:nth-child(4),.ltable td:nth-child(4),.ltable th:nth-child(5),.ltable td:nth-child(5),.ltable th:nth-child(7),.ltable td:nth-child(7){display:none}}
.lb3{color:var(--acc)}.wh{font-size:15px;margin:18px 0 8px}.ep.na{opacity:.55}
.addb{position:absolute;right:6px;bottom:6px;background:rgba(0,0,0,.75);border:1px solid var(--acc);color:var(--acc);border-radius:6px;font:inherit;font-size:11px;font-weight:700;padding:3px 7px;cursor:pointer;opacity:0;transition:opacity .12s}
.card:hover .addb,.addb.done,.addb:focus{opacity:1}.addb.done{border-color:var(--ok);color:var(--ok)}
@media(hover:none){.addb{opacity:1}}
.savebar{position:sticky;bottom:0;background:rgba(15,17,21,.95);padding:12px 0;display:flex;gap:10px;justify-content:flex-end;border-top:1px solid var(--line)}
.poster img{position:absolute;inset:0;width:100%;height:100%;object-fit:cover}.poster .pt{padding:8px}
.sheet{max-width:1000px}
.hero{position:relative;background:var(--panel2) center 20%/cover no-repeat;min-height:250px}
.hero:before{content:"";position:absolute;inset:0;background:linear-gradient(90deg,rgba(23,26,33,.98) 0%,rgba(23,26,33,.9) 40%,rgba(23,26,33,.55) 100%),linear-gradient(0deg,var(--panel) 0%,rgba(23,26,33,0) 50%)}
.hero-in{position:relative;display:flex;gap:22px;padding:22px 22px 8px}
.hero .pst{width:190px;flex:none;aspect-ratio:2/3;border-radius:10px;background:var(--panel2) center/cover;box-shadow:0 12px 32px rgba(0,0,0,.55)}
.hero h2{font-size:25px;margin:0;line-height:1.2}.hero .ot{color:var(--dim);font-size:13px;margin-top:2px}
.tagl{font-style:italic;color:#c3c9d4;margin:8px 0 2px}
.facts{display:flex;flex-wrap:wrap;gap:6px 10px;align-items:center;color:#c9ced8;font-size:13px;margin:10px 0 8px}
.facts .dot{color:#59606e}
.cert{border:1px solid #9aa2b1;border-radius:4px;padding:0 6px;font-size:12px;font-weight:700;color:var(--text)}
.gch{background:rgba(255,255,255,.09);border:0;border-radius:12px;padding:3px 10px;color:var(--text);font:inherit;font-size:12px;cursor:pointer}
.gch:hover{background:rgba(240,165,58,.25)}
.ratings{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}
.rb{display:flex;align-items:center;gap:7px;background:rgba(0,0,0,.38);border:1px solid rgba(255,255,255,.08);border-radius:9px;padding:5px 10px;font-size:13px}
.rb b{font-size:15px}.rb small{color:var(--dim)}
.ic{font-size:10px;font-weight:800;padding:2px 5px;border-radius:4px;letter-spacing:.3px}
.ic.tm{background:#01b4e4;color:#062733}.ic.im{background:#f5c518;color:#000}.ic.rt{background:#fa320a;color:#fff}.ic.mc{background:#66cc33;color:#0b1a05}
.hacts{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-top:6px}
.sb{padding:0 22px 22px}
.sec{margin-top:20px}.sec>h4{margin:0 0 10px;font-size:12px;text-transform:uppercase;letter-spacing:.7px;color:var(--dim);font-weight:700}
.ovw{font-size:14.5px;line-height:1.6;color:#dde1e8;margin:4px 0 0;max-width:75ch}
.scroller{display:flex;gap:12px;overflow-x:auto;padding-bottom:8px;scrollbar-width:thin}
.person{width:94px;flex:none;text-align:center;font-size:12px;cursor:pointer}
.person .ph{width:78px;height:78px;border-radius:50%;margin:0 auto 6px;background:var(--panel2) center/cover;display:flex;align-items:center;justify-content:center;color:var(--dim);font-weight:700;font-size:20px;border:1px solid var(--line)}
.person:hover .nm{color:var(--acc)}.person .nm{font-weight:600;line-height:1.25}.person .ch{color:var(--dim);line-height:1.25;margin-top:2px}
.mini{width:118px;flex:none;cursor:pointer;font-size:12px}
.mini .mp{aspect-ratio:2/3;border-radius:8px;background:var(--panel2) center/cover;margin-bottom:6px;position:relative;border:1px solid var(--line)}
.mini .mt{font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.mini .my{color:var(--dim)}
.mini.na{opacity:.42;cursor:default}.mini.cur .mp{outline:2px solid var(--acc);outline-offset:1px}.mini:not(.na):hover .mt{color:var(--acc)}
.svc{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.svcb{display:flex;align-items:center;gap:7px;background:var(--panel2);border:1px solid var(--line);border-radius:9px;padding:4px 11px 4px 4px;font-size:12px}
.svcb img{width:28px;height:28px;border-radius:6px}
.crew{display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:10px 18px;font-size:13px}
.crew .j{color:var(--dim);font-size:11px;text-transform:uppercase;letter-spacing:.5px;margin-bottom:2px}
.lnk{cursor:pointer;border-bottom:1px dotted #59606e}.lnk:hover{color:var(--acc);border-color:var(--acc)}
.kws{display:flex;flex-wrap:wrap;gap:6px}
.player{background:#000;border-radius:10px;overflow:hidden;margin:16px 0 0}.player video{display:block;width:100%;max-height:70vh;background:#000}
.player>.muted{padding:40px;text-align:center}
.pbar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;padding:8px 12px;background:var(--panel2);font-size:13px}.pbar>span:first-child{flex:1;min-width:0}
.trailer{position:relative;aspect-ratio:16/9;background:#000;border-radius:10px;overflow:hidden;margin:16px 0 0}.trailer iframe{position:absolute;inset:0;width:100%;height:100%;border:0}
.eps2{display:flex;flex-direction:column;gap:8px;margin-top:10px}
.ep2{display:flex;gap:12px;align-items:flex-start;background:var(--panel2);border-radius:10px;padding:8px}
.ep2 .still{width:136px;aspect-ratio:16/9;border-radius:6px;background:#0b0d11 center/cover;flex:none;display:flex;align-items:center;justify-content:center;color:#3b4250;font-weight:800;font-size:18px}
.ep2 .eb{flex:1;min-width:0}.ep2 .eh{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.ep2 .en{color:var(--acc);font-weight:700;font-size:12px}.ep2 .et{font-weight:600}
.ep2 .eo{color:var(--dim);font-size:12px;margin-top:3px;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;cursor:pointer}
.ep2 .eo.open{-webkit-line-clamp:unset}.ep2 .em{color:var(--dim);font-size:12px}
.ep2 .ea{display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-top:6px}
.ep2.na{opacity:.5;padding:5px 8px}.ep2.na .still{display:none}.ep2.na .eo{display:none}.ep2.na .ea{margin-top:0}.ep2.na{align-items:center}.stab.none{opacity:.55}
.ids{display:flex;gap:14px;flex-wrap:wrap;font-size:12px;color:var(--dim);margin-top:22px;border-top:1px solid var(--line);padding-top:12px}
.ids a{color:var(--dim)}
.nometa{background:var(--panel2);border:1px dashed var(--line);border-radius:10px;padding:10px 12px;font-size:13px;color:var(--dim);margin-top:14px}
.mstat{background:var(--panel2);border-radius:10px;padding:12px;margin-bottom:12px;font-size:13px}
.mstat .big{font-size:18px;font-weight:700}.mstat .prog{height:8px;margin:8px 0}
.mstat .grid2{display:grid;grid-template-columns:repeat(auto-fill,minmax(210px,1fr));gap:4px 16px;color:var(--dim);font-size:12px}
h4.sub{margin:16px 0 8px;font-size:13px}
.tips{background:var(--panel);border:1px solid var(--line);border-radius:var(--r);padding:10px 14px;margin-bottom:12px;font-size:13px;display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:6px 18px}
.tips[hidden]{display:none}
.rail{position:fixed;right:8px;top:calc(50% + 26px);transform:translateY(-50%);height:min(600px,calc(100vh - 120px));display:flex;flex-direction:column;justify-content:space-between;align-items:stretch;z-index:6;background:rgba(23,26,33,.86);backdrop-filter:blur(6px);border:1px solid var(--line);border-radius:14px;padding:6px 3px;user-select:none;touch-action:none}
.rail[hidden]{display:none}
.rail button{flex:1 1 0;min-height:0;background:none;border:0;color:#c9ced8;font:700 11px/1 system-ui,sans-serif;padding:0 6px;cursor:pointer;border-radius:6px;display:flex;align-items:center;justify-content:center}
.rail.dec button{font-size:10px;padding:0 4px}
.rail button:hover:not(.off){color:var(--acc)}.rail button.off{color:#434956;cursor:default}.rail button.on,.rail button.on:hover{background:var(--acc);color:#111}
.jbub{position:fixed;right:64px;top:calc(50% + 26px);transform:translateY(-50%) scale(.9);min-width:72px;height:72px;padding:0 14px;border-radius:36px;background:rgba(240,165,58,.95);color:#111;font:800 34px/72px system-ui,sans-serif;text-align:center;z-index:7;opacity:0;pointer-events:none;transition:opacity .25s,transform .25s;box-shadow:0 8px 30px rgba(0,0,0,.5)}
.jbub.on{opacity:1;transform:translateY(-50%) scale(1)}.jbub.sm{font-size:22px}
body.hasrail main{padding-right:52px}
@media(max-width:640px){.rail{right:2px;padding:4px 1px;border-radius:10px}.rail button{padding:0 4px;font-size:10px}.jbub{right:40px}body.hasrail main{padding-right:34px}}.tips code{background:var(--panel2);padding:1px 6px;border-radius:4px;color:var(--acc);font-size:12px}
@media(max-width:640px){.hero-in{flex-direction:column;padding:16px}.hero .pst{width:130px}.sb{padding:0 16px 16px}.ep2 .still{width:96px}.hero h2{font-size:21px}}
@media(max-width:640px){.sheet .head{flex-direction:column}.sheet .head .poster{width:120px}.pills{margin-left:0}}
</style>
</head>
<body>
<header>
  <div class="bar">
    <div class="brand"><img src="data:image/png;base64,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" alt="VODgrab" width="147" height="36"></div>
    <nav id="nav">
      <button data-t="browse" class="on">Browse</button>
      <button data-t="queue">Queue<span class="n" id="nQ" hidden></span></button>
      <button data-t="wanted">Wanted<span class="n" id="nW" hidden></span></button>
      <button data-t="history">History</button>
      <button data-t="unmatched">Unmatched</button>
      <button data-t="settings">Settings</button>
    </nav>
    <div class="pills">
      <span class="pill" id="pSync">Sync</span>
      <span class="pill" id="pWin"></span>
      <span class="pill ok" id="pUpd" hidden style="cursor:pointer" title="Open Settings to update"></span>
      <button class="btn s" id="pauseBtn">Pause all</button>
    </div>
  </div>
  <div class="banner" id="banner"></div>
</header>
<main id="main"></main>
<div class="modal" id="modal"><div class="sheet" id="sheet"></div></div>
<div class="toast" id="toast"></div>
<nav class="rail" id="rail" hidden></nav><div class="jbub" id="jbub"></div>
<script>
const $=(s,e=document)=>e.querySelector(s), $$=(s,e=document)=>[...e.querySelectorAll(s)];
const esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
async function api(p,body){const o=body===undefined?{}:{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)};
  const r=await fetch("/api/"+p,o);let j={};try{j=await r.json()}catch(e){}if(!r.ok)throw new Error(j.error||r.statusText);return j}
let tt;function toast(m,bad){const t=$("#toast");t.textContent=m;t.className="toast"+(bad?" bad":"");t.style.display="block";clearTimeout(tt);tt=setTimeout(()=>t.style.display="none",bad?7000:3500)}
const fb=n=>{n=+n||0;if(n<1024)return n+" B";const u=["KB","MB","GB","TB"];let i=-1;do{n/=1024;i++}while(n>=1024&&i<3);return n.toFixed(n<10?1:0)+" "+u[i]};
const fd=s=>{s=Math.max(0,Math.round(s));const h=Math.floor(s/3600),m=Math.floor(s%3600/60);return h?`${h}h ${m}m`:m?`${m}m`:`${s}s`};
const ago=t=>t?fd(Date.now()/1000-t)+" ago":"never";
const when=t=>{const d=new Date(t*1000),n=new Date();const hm=d.toLocaleTimeString([], {hour:"2-digit",minute:"2-digit"});return d.toDateString()==n.toDateString()?hm:d.toLocaleDateString([], {weekday:"short"})+" "+hm};
const STL={queued:"Queued",retry_wait:"Retrying",downloading:"Downloading",verifying:"Verifying",importing:"Importing",completed:"Downloaded",imported:"Imported",failed:"Failed",import_failed:"Import failed",cancelled:"Cancelled",retried:"Sent to arr"};
const dur=s=>{if(!s)return"";const h=Math.floor(s/3600),m=Math.round(s%3600/60);return h?`${h}h ${m}m`:`${m}m`};
const qb=m=>m&&m.known?`<span class="q k${m.quality}">${m.quality}</span>`:`<span class="q unk" title="Provider did not report quality">?</span>`;
const qinfo=m=>m&&m.known?[m.width&&m.height?`${m.width}×${m.height}`:"",m.codec,dur(m.duration),m.size?"about "+fb(m.size):"",m.source=="checked"?"checked":"reported by provider"].filter(Boolean).join(" · "):"Quality not reported";
async function probeIt(kind,id,btn){btn.disabled=true;btn.textContent="Checking…";try{const r=await api("probe",{kind,id});return r}catch(e){toast(e.message,1);btn.disabled=false;btn.textContent="Check quality";return null}}
const st=s=>`<span class="st ${esc(s)}">${esc(STL[s]||s)}</span>`;

let tab="browse", timer=null, status={};
const FKEYS=["service","genre","list","lang","prov","quality","lib","cert","runtime","olang","watch","status"];
const DEF={type:"movie",q:"",sort:"",year_from:"",year_to:"",rating:"",new:"",merge:"1",view:"grid"};
let F=null, FAC=null, SEL=new Map(), selMode=false, loading=false, B={offset:0,start:0,total:0,seq:0};

function readHash(){const h=location.hash.replace(/^#/,"");const [t,qs]=h.split("?");
  const p=new URLSearchParams(qs||"");const f={...DEF};FKEYS.forEach(k=>f[k]=[]);
  for(const [k,v] of p){if(FKEYS.includes(k))f[k]=v.split(",").filter(Boolean);else if(k in DEF)f[k]=v}
  return {tab:["browse","queue","wanted","history","unmatched","settings"].includes(t)?t:"browse",f}}
function writeHash(replace){const p=new URLSearchParams();
  for(const k in DEF)if(F[k]!==DEF[k]&&F[k]!=="")p.set(k,F[k]);FKEYS.forEach(k=>{if(F[k].length)p.set(k,F[k].join(","))});
  const h="#browse"+(p.toString()?"?"+p:"");if(location.hash!==h){skipHash=true;replace?history.replaceState(null,"",h):history.pushState(null,"",h)}}
let skipHash=false;
window.addEventListener("hashchange",()=>{if(skipHash){skipHash=false;return}route()});
window.addEventListener("popstate",()=>route());
function route(){const r=readHash();if(r.tab=="browse"){F=r.f}show(r.tab)}

$$("#nav button").forEach(b=>b.onclick=()=>go(b.dataset.t));
$("#pUpd").onclick=()=>go("settings");
function go(t){if(t=="browse"&&F){writeHash(false)}else if(location.hash.split("?")[0]!=="#"+t){skipHash=true;history.pushState(null,"","#"+t+(t=="wanted"&&typeof WF!="undefined"?(()=>{const p=new URLSearchParams();for(const k in WDEF)if(WF[k]!==WDEF[k]&&WF[k]!=="")p.set(k,WF[k]);return p.toString()?"?"+p:""})():""))}show(t)}
function show(t){tab=t;$$("#nav button").forEach(b=>b.classList.toggle("on",b.dataset.t==t));clearInterval(timer);hideRail();
  ({browse:renderBrowse,queue:renderQueue,wanted:renderWanted,history:renderHistory,unmatched:renderUnmatched,settings:renderSettings})[t]()}
document.addEventListener("keydown",e=>{if(e.key=="/"&&tab=="browse"&&!/INPUT|TEXTAREA|SELECT/.test(document.activeElement.tagName)){e.preventDefault();const q=$("#q");if(q)q.focus()}});

async function refreshStatus(){try{status=await api("status")}catch(e){return}
  const s=status.sync;let txt;
  if(s.running)txt="Syncing "+(s.phase||"").toLowerCase();
  else if(s.error)txt="Sync failed";
  else{const rc=s.recheck&&s.recheck<s.next,nx=rc?s.recheck:s.next;
    txt=s.last?`Synced ${ago(s.last.finished)} · ${rc?"recheck":"next"} ${when(nx)}`:`Next sync ${when(nx)}`}
  const ps=$("#pSync");ps.textContent=txt;ps.className="pill"+(s.error&&!s.running?" bad":s.running?" warn":"");ps.title=s.error||"";
  const w=status.window,pw=$("#pWin");
  if(status.paused){pw.textContent="Paused";pw.className="pill warn"}
  else if(!w.enabled){pw.textContent="Downloads any time";pw.className="pill ok"}
  else if(w.open){pw.textContent=`Window open · ${w.concurrency} stream${w.concurrency>1?"s":""}`;pw.className="pill ok"}
  else{pw.textContent="Outside download window";pw.className="pill warn"}
  const pu=$("#pUpd");pu.hidden=!status.update;pu.textContent=status.update?`Update available: ${status.update}`:"";
  $("#pauseBtn").textContent=status.paused||status.prov_error?"Resume":"Pause all";
  const b=$("#banner");b.style.display=status.banner?"block":"none";b.textContent=status.banner||"";
  const nq=$("#nQ");nq.hidden=!status.counts.active;nq.textContent=status.counts.active;
  const nw=$("#nW");nw.hidden=!status.counts.wanted;nw.textContent=status.counts.wanted}
$("#pauseBtn").onclick=async()=>{await api("pause",{paused:!(status.paused||status.prov_error)});refreshStatus();if(tab=="queue")renderQueue()};
setInterval(refreshStatus,8000);

/* ---------------- browse */
const LIBL={missing:"Not in library",in_arr:"In library, no file",has_file:"In library with file",vodgrab:"Downloaded by VODgrab",wanted:"Wanted by Sonarr or Radarr"};
const LIBB={in_arr:["In "+"arr","lb1"],has_file:["Have it","lb2"],vodgrab:["Downloaded","lb2"]};
const SORTL={relevance:"Best match",new:"New arrivals",popular:"Popular",top:"Top rated",title:"Title A to Z",title_desc:"Title Z to A",year:"Year",rating:"Rating",quality:"Quality"};
const RUNL={movie:{short:"Under 1h 30m",medium:"1h 30m to 2h",long:"2h to 2h 30m",epic:"Over 2h 30m"},series:{short:"Episodes under 30m",medium:"Episodes 30m to 50m",long:"Episodes over 50m"},all:{short:"Short",medium:"Medium",long:"Long",epic:"Very long"}};
const runL=v=>(RUNL[F&&F.type]||RUNL.all)[v]||v;
function activeCount(){return FKEYS.reduce((n,k)=>n+F[k].length,0)+["year_from","year_to","rating","new"].filter(k=>F[k]).length}
function setF(k,v,push){F[k]=v;writeHash(!push);load(true);drawChips();drawPanel()}
function toggleF(k,v){const a=F[k];const i=a.indexOf(v);i<0?a.push(v):a.splice(i,1);writeHash(false);load(true);drawChips();drawPanel()}

async function renderBrowse(){
  const m=$("#main");
  if(!F)F=readHash().f;
  if(!status.configured){m.innerHTML=`<div class="empty">Add a provider in <a href="#settings" onclick="go('settings');return false">Settings</a> to load the catalog.</div>`;return}
  m.innerHTML=`<div class="tools">
    <div class="seg" id="types"><button data-k="movie">Movies</button><button data-k="series">Series</button><button data-k="all">Both</button></div>
    <input type="search" id="q" placeholder="Search titles   ( / )" value="${esc(F.q)}" autocomplete="off">
    <select id="sort"></select>
    <button class="btn" id="tipbtn" title="Search tips">?</button>
    <button class="btn" id="fbtn">Filters<span class="n" id="fcount" hidden></span></button>
    <div class="seg" id="views"><button data-v="grid" title="Grid">▦</button><button data-v="list" title="List">☰</button></div>
    <button class="btn" id="selbtn">Select</button></div>
    <div id="tips" class="tips" hidden>
      <div><code>by:"Tom Hanks"</code> cast or crew</div><div><code>kw:heist</code> TMDB keyword</div>
      <div><code>year:1990..1999</code> year range</div><div><code>4k</code> <code>1080p</code> quality</div>
      <div><code>"exact words"</code> phrase</div><div><code>-word</code> leave out</div>
      <div class="muted" style="grid-column:1/-1">by: and kw: need a TMDB key in Settings. Click a name or keyword on any title to search it.</div></div>
    <div id="panel" class="fpanel" hidden></div>
    <div id="chips" class="chips"></div>
    <div class="muted" id="count" style="margin-bottom:8px"></div>
    <div id="topsent" style="height:1px"></div><div id="results"></div><div id="sentinel" style="height:40px"></div>
    <div class="bulk" id="bulk" hidden><span id="bulkn"></span><button class="btn p s" id="bulkdl">Download</button><button class="btn s" id="bulkadd">Add to Sonarr/Radarr</button><button class="btn s" id="bulkclr">Clear</button></div>`;
  $$("#types button").forEach(b=>b.onclick=()=>{F.type=b.dataset.k;if(F.type=="series")F.quality=[];FAC=null;SEL.clear();writeHash(false);renderBrowse()});
  $$("#views button").forEach(b=>b.onclick=()=>{F.view=b.dataset.v;writeHash(true);$$("#views button").forEach(x=>x.classList.toggle("on",x.dataset.v==F.view));load(true)});
  let deb;$("#q").oninput=e=>{clearTimeout(deb);deb=setTimeout(()=>{F.q=e.target.value.trim();writeHash(true);drawSort();load(true)},280)};
  $("#q").onkeydown=e=>{if(e.key=="Escape"){e.target.value="";F.q="";writeHash(true);drawSort();load(true)}};
  $("#tipbtn").onclick=()=>{$("#tips").hidden=!$("#tips").hidden};
  $("#fbtn").onclick=()=>{const p=$("#panel");p.hidden=!p.hidden;if(!p.hidden)drawPanel()};
  $("#selbtn").onclick=()=>{selMode=!selMode;if(!selMode)SEL.clear();$("#selbtn").classList.toggle("p",selMode);drawBulk();$$(".card,.lrow").forEach(c=>c.classList.toggle("selmode",selMode))};
  $("#bulkclr").onclick=()=>{SEL.clear();$$(".sel").forEach(c=>c.classList.remove("sel"));drawBulk()};
  $("#bulkdl").onclick=bulkDownload;
  $("#bulkadd").onclick=async()=>{const items=[...SEL.values()];if(!items.length)return;
    if(!confirm(`Add ${items.length} title${items.length==1?"":"s"} to Sonarr/Radarr with your settings from the Settings page?`))return;
    const r=await addArr(items);if(r){SEL.clear();selMode=false;$("#selbtn").classList.remove("p");$$(".sel,.selmode").forEach(c=>c.classList.remove("sel","selmode"));drawBulk();load(true)}};
  $$("#types button").forEach(b=>b.classList.toggle("on",b.dataset.k==F.type));
  $$("#views button").forEach(b=>b.classList.toggle("on",b.dataset.v==F.view));
  $("#selbtn").classList.toggle("p",selMode);
  drawSort();
  new IntersectionObserver(es=>{if(es[0].isIntersecting&&!loading&&B.offset<B.total)load(false)},{rootMargin:"600px"}).observe($("#sentinel"));
  new IntersectionObserver(es=>{if(es[0].isIntersecting&&!loading&&B.start>0)loadEarlier()},{rootMargin:"500px"}).observe($("#topsent"));
  try{FAC=await api("facets?type="+F.type)}catch(e){FAC=null}
  drawChips();load(true)}

function drawSort(){const s=$("#sort");if(!s)return;const opts=F.q?["relevance","new","popular","top","title","title_desc","year","rating","quality"]:["new","popular","top","title","title_desc","year","rating","quality"];
  const cur=F.sort&&opts.includes(F.sort)?F.sort:opts[0];
  s.innerHTML=opts.filter(o=>o!="quality"||F.type!="series").map(o=>`<option value="${o}">${SORTL[o]}</option>`).join("");s.value=cur;
  s.onchange=e=>setF("sort",e.target.value)}

function chipGroup(k,title,items,label){if(!items||!items.length)return"";
  const many=items.length>14,open=$("#panel")&&$("#panel").dataset["open"+k];
  const shown=many&&!open?items.slice(0,14):items;
  return `<div class="fg"><div class="fgt">${title}</div><div class="fchips">${shown.map(([v,n])=>`<button class="fc${F[k].includes(String(v))?" on":""}" data-k="${k}" data-v="${esc(v)}">${esc(label?label(v):v)}${n!=null?` <span>${Number(n).toLocaleString()}</span>`:""}</button>`).join("")}
    ${many?`<button class="fc more" data-more="${k}">${open?"Show fewer":"Show all "+items.length}</button>`:""}</div></div>`}
function drawPanel(){const p=$("#panel");if(!p||p.hidden)return;const fa=FAC||{};
  const years=fa.year_min?`<div class="fg"><div class="fgt">Year</div><div class="frow"><input type="number" id="yf" placeholder="${fa.year_min}" value="${esc(F.year_from)}" min="1900" max="2100"> to <input type="number" id="yt" placeholder="${fa.year_max}" value="${esc(F.year_to)}" min="1900" max="2100"></div></div>`:"";
  const rating=`<div class="fg"><div class="fgt">Minimum rating</div><div class="fchips">${["","5","6","7","8"].map(v=>`<button class="fc${String(F.rating)==v?" on":""}" data-one="rating" data-v="${v}">${v?v+"+":"Any"}</button>`).join("")}</div></div>`;
  const nw=`<div class="fg"><div class="fgt">New arrivals</div><div class="fchips">${[["","Any time"],["7","Last 7 days"],["30","Last 30 days"]].map(([v,l])=>`<button class="fc${String(F.new)==v?" on":""}" data-one="new" data-v="${v}">${l}</button>`).join("")}</div></div>`;
  const qual=F.type!="series"?chipGroup("quality","Quality (movies)",[["2160p"],["1080p"],["720p"],["480p"],["unknown"]],v=>v=="unknown"?"Not checked yet":v=="2160p"?"4K":v):"";
  const lib=fa.library?chipGroup("lib","Library",Object.keys(LIBL).map(v=>[v]),v=>LIBL[v]):"";
  const provs=fa.providers&&fa.providers.length>1?chipGroup("prov","Provider",fa.providers.map(([id,n,c])=>[String(id),c]),v=>{const x=fa.providers.find(y=>String(y[0])==v);return x?x[1]:v}):"";
  const cert=chipGroup("cert","Age rating"+(fa.region?` (${esc(fa.region)}, US when missing)`:""),fa.certs);
  const runt=chipGroup("runtime","Runtime",fa.runtime,runL);
  const watch=chipGroup("watch","Officially streaming in "+esc(fa.region||""),fa.watch);
  const stat=F.type!="movie"?chipGroup("status","Show status",fa.status):"";
  const metaHint=fa.meta?"":`<div class="fg muted" style="font-size:12px">Add a free TMDB key in <a href="#settings" onclick="go('settings');return false">Settings</a> for age ratings, runtime, streaming services, cast search and more.</div>`;
  p.innerHTML=chipGroup("service","Service",fa.services)+chipGroup("genre","Genre",fa.genres)+chipGroup("list","Lists",fa.lists)+
    `<div class="fgrid">${years}${rating}${nw}</div>`+cert+runt+stat+watch+qual+lib+provs+chipGroup("lang","Language (provider copy)",fa.langs)+chipGroup("olang","Original language",fa.olangs)+metaHint+
    `<div class="fg"><label class="muted" style="display:inline-flex;gap:8px;align-items:center"><input type="checkbox" id="merge" ${F.merge!="0"?"checked":""}> Show each title once, even when several providers have it</label></div>
    <div class="tools" style="margin:6px 0 0"><button class="btn s" id="fclear">Clear filters</button><button class="btn s" id="fclose">Close</button></div>`;
  $$(".fc[data-k]",p).forEach(b=>b.onclick=()=>toggleF(b.dataset.k,b.dataset.v));
  $$(".fc[data-one]",p).forEach(b=>b.onclick=()=>setF(b.dataset.one,b.dataset.v,true));
  $$(".fc[data-more]",p).forEach(b=>b.onclick=()=>{const k="open"+b.dataset.more;p.dataset[k]?delete p.dataset[k]:p.dataset[k]="1";drawPanel()});
  const yr=()=>{F.year_from=$("#yf").value;F.year_to=$("#yt").value;writeHash(true);load(true);drawChips()};
  if($("#yf")){$("#yf").onchange=yr;$("#yt").onchange=yr}
  $("#merge").onchange=e=>setF("merge",e.target.checked?"1":"0",true);
  $("#fclear").onclick=clearFilters;$("#fclose").onclick=()=>{p.hidden=true}}
function clearFilters(){FKEYS.forEach(k=>F[k]=[]);F.year_from=F.year_to=F.rating=F.new="";writeHash(false);load(true);drawChips();drawPanel()}
function drawChips(){const c=$("#chips");if(!c)return;const fa=FAC||{};const out=[];
  const nm={service:"",genre:"",list:"",lang:"",prov:"",quality:"",lib:""};
  FKEYS.forEach(k=>F[k].forEach(v=>{let l=v;if(k=="lib")l=LIBL[v]||v;if(k=="runtime")l=runL(v);if(k=="cert")l="Rated "+v;if(k=="olang")l="Original "+v;if(k=="watch")l="On "+v;if(k=="quality")l=v=="unknown"?"Quality not checked":v=="2160p"?"4K":v;
    if(k=="prov"){const x=(fa.providers||[]).find(y=>String(y[0])==v);l=x?x[1]:v}out.push([k,v,l])}));
  if(F.year_from||F.year_to)out.push(["year","",`Year ${F.year_from||"…"} to ${F.year_to||"…"}`]);
  if(F.rating)out.push(["rating","",`Rating ${F.rating}+`]);
  if(F.new)out.push(["new","",`New in last ${F.new} days`]);
  c.innerHTML=out.map(([k,v,l])=>`<span class="chip">${esc(l)}<button data-k="${k}" data-v="${esc(v)}" title="Remove">×</button></span>`).join("")+(out.length>1?`<button class="btn s" id="chipclr">Clear all</button>`:"");
  $$("button[data-k]",c).forEach(b=>b.onclick=()=>{const k=b.dataset.k;if(k=="year"){F.year_from=F.year_to=""}else if(k=="rating"||k=="new"){F[k]=""}else{F[k]=F[k].filter(x=>x!=b.dataset.v)}writeHash(false);load(true);drawChips();drawPanel()});
  if($("#chipclr"))$("#chipclr").onclick=clearFilters;
  const n=activeCount(),fc=$("#fcount");if(fc){fc.hidden=!n;fc.textContent=n}}

function params(){const p=new URLSearchParams();p.set("type",F.type);if(F.q)p.set("q",F.q);
  const opts=F.q?"relevance":"new";p.set("sort",F.sort||opts);if(F.merge=="0")p.set("merge","0");
  FKEYS.forEach(k=>{if(F[k].length)p.set(k,F[k].join(","))});["year_from","year_to","rating","new"].forEach(k=>{if(F[k])p.set(k,F[k])});
  p.set("offset",B.offset);p.set("limit",60);return p}
const curSort=()=>F.sort||(F.q?"relevance":"new");
const JUMPS=["title","title_desc","year"];
async function load(reset,from){const box=$("#results");if(!box)return;
  const jump=from!=null;
  if(reset){B.offset=jump?from:0;B.start=B.offset;B.total=0}
  const seq=++B.seq;loading=true;
  const p=params();if(reset&&!jump&&JUMPS.includes(curSort()))p.set("marks","1");
  let r;try{r=await api("browse?"+p)}catch(e){loading=false;toast(e.message,1);return}
  if(seq!==B.seq||!$("#results"))return;loading=false;
  if(reset){box.innerHTML=F.view=="list"?`<table class="ltable"><thead><tr><th></th><th>Title</th><th>Year</th><th>Type</th><th>Providers</th><th>Quality</th><th>Rating</th><th>Library</th><th></th></tr></thead><tbody></tbody></table>`:`<div class="grid"></div>`;
    if(jump)window.scrollTo(0,$("#results").getBoundingClientRect().top+scrollY-70);else window.scrollTo(0,0);
    if(!jump){MARKS=JUMPS.includes(curSort())?r.marks||null:null;drawRail()}}
  B.total=r.total;B.offset+=r.items.length;
  const what=F.type=="movie"?"movies":F.type=="series"?"series":"titles";
  let cnt=r.total?`${r.total.toLocaleString()}${r.capped?"+":""} ${r.total==1?what.replace(/s$/,"")+(what=="series"?"s":""):what}${r.capped?" (showing the best matches, add a word to narrow it down)":""}`:"";
  if(r.fuzzy)cnt=`No exact matches. Did you mean ${r.suggest.map(x=>`<a href="#" class="sug">${esc(x)}</a>`).join(", ")}? Showing close matches.`;
  $("#count").innerHTML=cnt;$$(".sug").forEach(a=>a.onclick=e=>{e.preventDefault();F.q=a.textContent;$("#q").value=F.q;writeHash(false);drawSort();load(true)});
  if(reset&&!r.items.length){box.innerHTML=`<div class="empty">${status.counts.movies+status.counts.series?(activeCount()||F.q?"Nothing matches. Try removing a filter.":"Nothing here."):"Catalog is empty. Run a sync from Settings."}</div>`;return}
  const html=r.items.map(i=>F.view=="list"?rowHtml(i):cardHtml(i)).join("");
  const tgt=F.view=="list"?$("tbody",box):$(".grid",box);if(!tgt)return;
  tgt.insertAdjacentHTML("beforeend",html);
  wire(box,r.items);
  if(PENDING){const l=PENDING;PENDING=null;jumpTo(l);return}
  if(jump)railMark();
  if(r.items.length&&B.offset<B.total&&$("#sentinel").getBoundingClientRect().top<innerHeight+600)load(false)}
async function loadEarlier(){const box=$("#results");if(!box||B.start<=0)return;
  const n=Math.min(60,B.start),off=B.start-n,seq=B.seq;loading=true;
  const p=params();p.set("offset",off);p.set("limit",n);
  let r;try{r=await api("browse?"+p)}catch(e){loading=false;return}
  loading=false;if(seq!==B.seq||!$("#results"))return;
  const h0=document.documentElement.scrollHeight,tgt=F.view=="list"?$("tbody",box):$(".grid",box);if(!tgt)return;
  tgt.insertAdjacentHTML("afterbegin",r.items.map(i=>F.view=="list"?rowHtml(i):cardHtml(i)).join(""));
  window.scrollBy(0,document.documentElement.scrollHeight-h0);B.start=off;wire(box,r.items)}
function wire(box,items){
  $$("[data-cid]:not([data-wired])",box).forEach(el=>{el.dataset.wired=1;const it=items.find(x=>String(x.id)==el.dataset.cid)||{id:+el.dataset.cid,kind:el.dataset.kind,clean:el.dataset.t};
    const ad=$("[data-add]",el);if(ad)ad.onclick=async e=>{e.stopPropagation();const r=await addArr([it],ad);if(r&&r.added){it.lib="in_arr";const b=$(".bdg",el);if(b)b.innerHTML=badges(it)}};
    el.onclick=e=>{if(e.target.closest("[data-dl1],[data-add]"))return;if(selMode){toggleSel(el,it);return}it.kind=="movie"?openMovie(it.id):openSeries(it.id)};
    const d=$("[data-dl1]",el);if(d)d.onclick=async e=>{e.stopPropagation();d.disabled=true;if(it.kind=="series"&&!confirm("Queue every episode of "+it.clean+"?")){d.disabled=false;return}await dl({kind:it.kind=="movie"?"movie":"series",id:it.id})}})}

/* jump bar: letters for title sorts, decades for year */
let MARKS=null,PENDING=null,RAILJUMP=null,bubT=null;
const markOf=el=>curSort()=="year"?(el.dataset.y?Math.floor(+el.dataset.y/10)*10+"s":"Unknown"):el.dataset.l;
const railLabel=l=>l=="Unknown"?"?":/^\d{4}s$/.test(l)?"\u2019"+l.slice(2):l;
function hideRail(){const r=$("#rail");if(r)r.hidden=true;document.body.classList.remove("hasrail");RAILJUMP=null}
function drawRail(marks,onJump){const r=$("#rail");marks=marks||MARKS;onJump=onJump||jumpTo;
  const on=tab=="browse"||tab=="wanted"?marks&&marks.filter(m=>m[1]).length>1:false;
  r.hidden=!on;document.body.classList.toggle("hasrail",!!on);if(!on){RAILJUMP=null;return}RAILJUMP=onJump;
  r.classList.toggle("dec",marks.some(m=>/s$|Unknown/.test(m[0])));
  r.innerHTML=marks.map(([l,n])=>`<button data-l="${esc(l)}" class="${n?"":"off"}" title="${n?Number(n).toLocaleString()+" "+esc(l):"Nothing under "+esc(l)}">${esc(railLabel(l))}</button>`).join("");
  let drag=null;
  r.onpointerdown=e=>{const b=e.target.closest("button");if(!b)return;e.preventDefault();drag=b.dataset.l;r.setPointerCapture(e.pointerId);bubble(drag,true)};
  r.onpointermove=e=>{if(drag===null)return;const b=document.elementFromPoint(e.clientX,e.clientY);const bt=b&&b.closest&&b.closest("#rail button");if(bt&&!bt.classList.contains("off")&&bt.dataset.l!=drag){drag=bt.dataset.l;bubble(drag,true)}};
  r.onpointerup=r.onpointercancel=e=>{if(drag===null)return;const l=drag;drag=null;bubble(l);const m=marks.find(x=>x[0]==l);if(m&&m[1])RAILJUMP(l)};
  railMark()}
function bubble(l,hold){const b=$("#jbub");b.textContent=railLabel(l)=="?"?"?":l;b.classList.toggle("sm",l.length>2);b.classList.add("on");clearTimeout(bubT);if(!hold)bubT=setTimeout(()=>b.classList.remove("on"),650)}
function jumpTo(l){const m=(MARKS||[]).find(x=>x[0]==l);if(!m||!m[1])return;bubble(l);load(true,m[2])}
function railMark(){const r=$("#rail");if(!r||r.hidden)return;let cur=null;
  const els=tab=="wanted"?$$("#wlist [data-l]"):$$("#results [data-cid]");
  for(const el of els){if(el.getBoundingClientRect().bottom>90){cur=tab=="wanted"?el.dataset.l:markOf(el);break}}
  $$("button",r).forEach(b=>b.classList.toggle("on",b.dataset.l==cur));return cur}
let scT=null,lastMark=null;
window.addEventListener("scroll",()=>{if(scT)return;scT=requestAnimationFrame(()=>{scT=null;const c=railMark();if(c&&c!==lastMark&&lastMark!==null&&!$("#rail").hidden)bubble(c);lastMark=c})},{passive:true});
document.addEventListener("keydown",e=>{if(tab!="browse"||e.ctrlKey||e.metaKey||e.altKey||!/^[a-z]$/i.test(e.key)||/INPUT|TEXTAREA|SELECT/.test(document.activeElement.tagName)||$("#modal").classList.contains("on"))return;
  const l=e.key.toUpperCase();if(curSort()=="title"||curSort()=="title_desc"){jumpTo(l);return}
  F.sort="title";writeHash(true);drawSort();PENDING=l;load(true)});
const ARRN=k=>k=="movie"?"Radarr":"Sonarr";
const canAdd=i=>i.lib=="missing"&&status.arrs&&status.arrs[i.kind=="movie"?"radarr":"sonarr"];
async function addArr(items,btn){if(btn)btn.disabled=true;
  try{const r=await api("add",{items:items.map(x=>({kind:x.kind,id:x.id}))});
    const bad=r.results.filter(x=>!x.ok);
    toast(r.results.length==1?r.results[0].message:`Added ${r.added} of ${items.length}`+(bad.length?`. ${bad.length} failed: ${bad.slice(0,2).map(x=>x.message).join("; ")}`:""),bad.length>0);
    if(btn&&!bad.length){btn.textContent="Added";btn.classList.add("done")}else if(btn)btn.disabled=false;
    return r}catch(e){toast(e.message,1);if(btn)btn.disabled=false}}
function badges(i){return `${i.wanted?`<span class="lb lb3">Wanted</span>`:""}${i.quality?`<span class="q k${i.quality}">${i.quality=="2160p"?"4K":i.quality}</span>`:""}${LIBB[i.lib]?`<span class="lb ${LIBB[i.lib][1]}">${i.lib=="in_arr"?(i.kind=="movie"?"In Radarr":"In Sonarr"):LIBB[i.lib][0]}</span>`:""}`}
function imgFail(el){const a=el.dataset.alt;if(a){el.dataset.alt="";el.src=a}else el.remove()}
function posterImg(i){const a=i.poster||i.icon,b=i.poster&&i.icon&&i.icon!=i.poster?i.icon:"";return a?`<img loading="lazy" alt="" src="${esc(a)}" data-alt="${esc(b)}" onerror="imgFail(this)">`:""}
function cardHtml(i){return `<div class="card${selMode?" selmode":""}${SEL.has(i.id)?" sel":""}" data-cid="${i.id}" data-kind="${i.kind}" data-t="${esc(i.clean)}" data-l="${esc(i.l||"")}" data-y="${i.year||""}">
  <div class="poster"><span class="pt">${esc(i.clean)}</span>${posterImg(i)}<div class="bdg">${badges(i)}</div><div class="tick">✓</div>${canAdd(i)?`<button class="addb" data-add title="Add to ${ARRN(i.kind)}">+ ${ARRN(i.kind)}</button>`:""}</div>
  <div class="t" title="${esc(i.clean)}">${esc(i.clean)}<div class="y">${i.year||""}${F.type=="all"?` · ${i.kind=="movie"?"Movie":"Series"}`:""}${i.rating?` · ★ ${i.rating}`:""}${i.nprov>1?`<span class="ptag" title="${esc(i.providers.join(", "))}">${i.nprov} providers</span>`:(FAC&&FAC.providers&&FAC.providers.length>1?`<span class="ptag">${esc(i.providers[0])}</span>`:"")}</div></div></div>`}
function rowHtml(i){return `<tr class="lrow${selMode?" selmode":""}${SEL.has(i.id)?" sel":""}" data-cid="${i.id}" data-kind="${i.kind}" data-t="${esc(i.clean)}" data-l="${esc(i.l||"")}" data-y="${i.year||""}"><td class="tk">✓</td><td class="lt">${esc(i.clean)}</td><td>${i.year||""}</td><td>${i.kind=="movie"?"Movie":"Series"}</td>
  <td>${esc(i.providers.join(", "))}</td><td>${i.quality?`<span class="q k${i.quality}">${i.quality=="2160p"?"4K":i.quality}</span>`:`<span class="muted">?</span>`}</td><td>${i.rating?"★ "+i.rating:""}</td><td>${i.wanted?`<span class="lb lb3">Wanted</span> `:""}${LIBL[i.lib]&&i.lib!="missing"?LIBL[i.lib]:`<span class="muted">No</span>`}</td>
  <td style="white-space:nowrap">${canAdd(i)?`<button class="btn s" data-add>Add to ${ARRN(i.kind)}</button> `:""}<button class="btn s" data-dl1>Download</button></td></tr>`}
function toggleSel(el,it){SEL.has(it.id)?SEL.delete(it.id):SEL.set(it.id,it);el.classList.toggle("sel",SEL.has(it.id));drawBulk()}
function drawBulk(){const b=$("#bulk");if(!b)return;b.hidden=!selMode;$("#bulkn").textContent=SEL.size?`${SEL.size} selected`:"Click titles to select them"}
async function bulkDownload(){const items=[...SEL.values()];if(!items.length)return;
  const ns=items.filter(x=>x.kind=="series").length;
  if(ns&&!confirm(`Queue every episode of ${ns} series${ns==1?"":""} plus ${items.length-ns} movie${items.length-ns==1?"":"s"}?`))return;
  let ok=0,notes=[];for(const it of items){try{const r=await api("download",{kind:it.kind=="movie"?"movie":"series",id:it.id});ok+=r.queued;notes=notes.concat(r.notes||[])}catch(e){notes.push(it.clean+": "+e.message)}}
  toast(`Queued ${ok} download${ok==1?"":"s"}`+(notes.length?". "+[...new Set(notes)].slice(0,3).join(". "):""),notes.length>0);
  SEL.clear();selMode=false;$("#selbtn").classList.remove("p");$$(".sel,.selmode").forEach(c=>c.classList.remove("sel","selmode"));drawBulk();refreshStatus()}

/* ---------------- detail */
function modal(html){stopPlayer();$("#sheet").innerHTML=html;$("#modal").classList.add("on");$(".x",$("#sheet")).onclick=closeModal}
function closeModal(){stopPlayer();$("#modal").classList.remove("on")}
let PLAYER=null;
function stopPlayer(){if(!PLAYER)return;const p=PLAYER;PLAYER=null;const v=$("#pv");if(v){v.pause();v.removeAttribute("src");v.load()}
  const box=$("#playbox");if(box)box.innerHTML="";api("play/stop",{session:p}).catch(()=>{})}
async function play(kind,id,label,info){const box=$("#playbox");if(!box)return;stopPlayer();
  box.innerHTML=`<div class="player"><div class="muted">Connecting…</div></div>`;box.scrollIntoView({behavior:"smooth",block:"nearest"});
  let r;try{r=await api("play/start",{kind,id})}catch(e){box.innerHTML=`<div class="nometa" style="color:#ffd6d6">${esc(e.message)}</div>`;return}
  PLAYER=r.session;
  box.innerHTML=`<div class="player"><video id="pv" controls autoplay playsinline preload="auto"></video>
    <div class="pbar"><span><b>${esc(label)}</b>${info?` <span class="muted">· ${esc(info)}</span>`:""}</span><span class="muted" id="pmsg">Uses one provider connection while open</span><button class="btn s" id="pclose">Close player</button></div></div>`;
  const v=$("#pv");v.src="/api/play/"+r.session;
  $("#pclose").onclick=stopPlayer;
  v.onerror=()=>{if(!PLAYER)return;$("#pmsg").innerHTML=`<span style="color:var(--bad)">Your browser can't play this file${info?" ("+esc(info)+")":""}. It uses a format browsers don't support; downloading it is still fine.</span>`};
  v.onloadedmetadata=()=>{if(!v.videoWidth)$("#pmsg").innerHTML=`<span style="color:var(--acc)">Sound only: the video format isn't supported by this browser.</span>`;
    else setTimeout(()=>{if(PLAYER&&v.webkitAudioDecodedByteCount===0&&v.mozHasAudio!==true&&v.currentTime>2)$("#pmsg").innerHTML=`<span style="color:var(--acc)">No sound? The audio format (often AC3 or DTS) isn't supported by browsers. The download will have sound.</span>`},4000)}}
$("#modal").onclick=e=>{if(e.target.id=="modal")closeModal()};
document.addEventListener("keydown",e=>{if(e.key=="Escape")closeModal()});
const nowBox=`<label class="muted" style="display:inline-flex;gap:6px;align-items:center"><input type="checkbox" id="now"> Ignore download schedule</label>`;
async function dl(body){try{const r=await api("download",body);toast(`Queued ${r.queued} item${r.queued==1?"":"s"}`+(r.notes&&r.notes.length?". "+r.notes.join(". "):""),r.notes&&r.notes.length);refreshStatus();return r}catch(e){toast(e.message,1)}}
function browseWith(o,type){closeModal();const f={...DEF};FKEYS.forEach(k=>f[k]=[]);f.type=type||"all";Object.assign(f,o);if(o.q)f.sort="";F=f;FAC=null;writeHash(false);show("browse")}
const fmtDate=d=>{if(!d)return"";const x=new Date(d+"T12:00:00");return isNaN(x)?d:x.toLocaleDateString([], {year:"numeric",month:"short",day:"numeric"})};
const initials=n=>String(n||"?").split(/\s+/).map(w=>w[0]).slice(0,2).join("");
const fmtVotes=v=>{v=+v||0;return v>=1e6?(v/1e6).toFixed(1)+"M":v>=1e3?(v/1e3).toFixed(v>=1e4?0:1)+"k":String(v)};
function ratingsHtml(m,prov){const o=m.omdb&&!m.omdb.error?m.omdb:null,out=[];
  if(m.rating&&m.votes)out.push(`<span class="rb" title="TMDB user score"><span class="ic tm">TMDB</span><b>${(+m.rating).toFixed(1)}</b><small>${fmtVotes(m.votes)} votes</small></span>`);
  if(o&&o.imdb_rating)out.push(`<span class="rb" title="IMDb rating"><span class="ic im">IMDb</span><b>${esc(o.imdb_rating)}</b>${o.imdb_votes?`<small>${esc(o.imdb_votes)}</small>`:""}</span>`);
  if(o&&o.rt)out.push(`<span class="rb" title="Rotten Tomatoes critics"><span class="ic rt">RT</span><b>${esc(o.rt)}</b></span>`);
  if(o&&o.metacritic)out.push(`<span class="rb" title="Metacritic"><span class="ic mc">MC</span><b>${esc(o.metacritic)}</b></span>`);
  if(!out.length&&prov)out.push(`<span class="rb" title="Rating from the provider"><b>★ ${esc(prov)}</b></span>`);
  return out.length?`<div class="ratings">${out.join("")}</div>`:""}
function heroHtml(kind,t,m,base,acts){
  const pst=m.poster||base.poster,yr=base.year||(m.date||"").slice(0,4);
  const facts=[m.cert?`<span class="cert" title="Age rating${m.cert_region?" ("+esc(m.cert_region)+")":""}">${esc(m.cert)}</span>`:"",
    kind=="movie"&&m.date?esc(fmtDate(m.date)):"",m.runtime?esc(dur(m.runtime*60))+(kind=="series"?" episodes":""):"",
    kind=="series"&&m.nseasons?`${m.nseasons} season${m.nseasons==1?"":"s"}`:"",kind=="series"&&m.status?esc(m.status):"",
    kind=="series"&&m.networks&&m.networks.length?esc(m.networks.slice(0,2).join(", ")):"",m.olang&&m.olang!="English"?esc(m.olang):""].filter(Boolean);
  const gen=(m.genres||[]).map(g=>`<button class="gch" data-genre="${esc(g.tag)}" title="Browse ${esc(g.name)}">${esc(g.name)}</button>`).join("");
  return `<div class="hero" ${m.backdrop?`style="background-image:url('${esc(m.backdrop)}')"`:""}><div class="hero-in">
    <div class="pst" ${pst?`style="background-image:url('${esc(pst)}')"`:""}></div>
    <div style="flex:1;min-width:0"><button class="x">×</button>
      <h2>${esc(t)} ${yr?`<span class="muted" style="font-weight:400">(${esc(yr)})</span>`:""}</h2>
      ${m.otitle?`<div class="ot">${esc(m.otitle)}</div>`:""}${m.tagline?`<div class="tagl">${esc(m.tagline)}</div>`:""}
      ${facts.length?`<div class="facts">${facts.join('<span class="dot">•</span>')}</div>`:""}
      ${gen?`<div class="kws">${gen}</div>`:""}
      ${ratingsHtml(m,base.rating)}
      <div class="hacts">${acts}${m.videos&&m.videos.length?`<button class="btn s" id="trl">▶ ${esc(m.videos[0].t||"Trailer")}</button>`:""}</div>
      ${m.next&&m.next.date?`<div class="muted" style="font-size:12px;margin-top:8px">Next episode S${String(m.next.s).padStart(2,"0")}E${String(m.next.e).padStart(2,"0")} on ${esc(fmtDate(m.next.date))}</div>`:""}
    </div></div></div>`}
function peopleHtml(m){let h="";
  if(m.crew&&m.crew.length)h+=`<div class="sec"><div class="crew">${m.crew.slice(0,8).map(c=>`<div><div class="j">${esc(c.names.length>1?({Director:"Directors",Writer:"Writers",Producer:"Producers",Creator:"Creators",Novel:"Novels"}[c.job]||c.job):c.job)}</div>${c.names.slice(0,4).map(n=>`<span class="lnk" data-by="${esc(n)}">${esc(n)}</span>`).join(", ")}</div>`).join("")}</div></div>`;
  if(m.cast&&m.cast.length)h+=`<div class="sec"><h4>Cast</h4><div class="scroller">${m.cast.map(c=>`<div class="person" data-by="${esc(c.name)}" title="Titles with ${esc(c.name)}"><div class="ph" ${c.photo?`style="background-image:url('${esc(c.photo)}')"`:""}>${c.photo?"":esc(initials(c.name))}</div><div class="nm">${esc(c.name)}</div><div class="ch">${esc(c.character)}</div></div>`).join("")}</div></div>`;
  return h}
function watchHtml(m){return m.watch&&m.watch.length?`<div class="sec"><h4>Officially streaming in ${esc(m.region)}</h4><div class="svc">${m.watch.map(w=>`<span class="svcb">${w.logo?`<img src="${esc(w.logo)}" alt="" onerror="this.remove()">`:""}${esc(w.name)}</span>`).join("")}<a class="muted" style="font-size:12px" href="${esc(m.watch_link)}" target="_blank" rel="noopener">Details on TMDB</a></div></div>`:""}
function recsHtml(m){return m.recs&&m.recs.length?`<div class="sec"><h4>More like this on your providers</h4><div class="scroller">${m.recs.map(r=>`<div class="mini" data-open="${r.kind}:${r.id}"><div class="mp" style="${r.poster||r.icon?`background-image:url('${esc(r.poster||r.icon)}')`:""}"></div><div class="mt" title="${esc(r.clean)}">${esc(r.clean)}</div><div class="my">${r.year||""}${r.rating?" · ★ "+r.rating:""}</div></div>`).join("")}</div></div>`:""}
function kwHtml(m){return m.keywords&&m.keywords.length?`<div class="sec"><h4>Keywords</h4><div class="kws">${m.keywords.slice(0,24).map(k=>`<button class="gch" data-kw="${esc(k)}">${esc(k)}</button>`).join("")}</div></div>`:""}
function idsHtml(kind,m,x){const t=kind=="movie"?"movie":"tv",o=m.omdb;const ids=[];
  if(m.tmdb)ids.push(`<a href="https://www.themoviedb.org/${t}/${m.tmdb}" target="_blank" rel="noopener">TMDB ${m.tmdb}</a>`);
  if(m.imdb)ids.push(`<a href="https://www.imdb.com/title/${esc(m.imdb)}/" target="_blank" rel="noopener">IMDb ${esc(m.imdb)}</a>`);
  if(x.tvdb)ids.push(`<a href="https://thetvdb.com/dereferrer/series/${x.tvdb}" target="_blank" rel="noopener">TVDB ${x.tvdb}</a>`);
  if(o&&o.awards)ids.push(esc(o.awards));if(o&&o.box_office)ids.push("Box office "+esc(o.box_office));
  if(m.companies&&m.companies.length)ids.push(esc(m.companies.slice(0,3).join(", ")));
  if(m.fetched)ids.push("Details from TMDB, updated "+ago(m.fetched));
  if(o&&o.error)ids.push("OMDb: "+esc(o.error));
  return ids.length?`<div class="ids">${ids.join("<span>·</span>")}</div>`:""}
function noMeta(m){return m&&m.has?"":m&&m.key?`<div class="nometa">TMDB has no details for this title${m.tmdb?"":" (the provider did not tag it with an ID and a search by name found no confident match)"}.</div>`:`<div class="nometa">Add a free TMDB key in <a href="#settings" onclick="closeModal();go('settings');return false">Settings</a> for artwork, cast, age ratings, trailers and where it streams.</div>`}
function wireDetail(kind,m){const sh=$("#sheet");
  $$("[data-genre]",sh).forEach(b=>b.onclick=()=>browseWith({genre:[b.dataset.genre]},kind));
  $$("[data-by]",sh).forEach(b=>b.onclick=()=>browseWith({q:`by:"${b.dataset.by}"`},"all"));
  $$("[data-kw]",sh).forEach(b=>b.onclick=()=>browseWith({q:`kw:"${b.dataset.kw}"`},kind));
  $$("[data-open]",sh).forEach(b=>b.onclick=()=>{const [k,i]=b.dataset.open.split(":");k=="movie"?openMovie(+i):openSeries(+i)});
  const t=$("#trl");if(t)t.onclick=()=>{const v=m.videos[0];const box=$("#trbox");box.innerHTML=box.innerHTML?"":`<div class="trailer"><iframe src="https://www.youtube-nocookie.com/embed/${encodeURIComponent(v.k)}?autoplay=1&rel=0" allow="autoplay; encrypted-media; picture-in-picture" allowfullscreen></iframe></div>`;if(box.innerHTML)box.scrollIntoView({behavior:"smooth",block:"nearest"})}}
function drawLinks(kind,id,links){const box=$("#linkSec");if(!box)return;
  const A=kind=="movie"?"Radarr":"Sonarr",w=kind=="movie"?"movie":"series";
  const how={tmdb:"matched by TMDB ID",title:"matched by title and year",manual:"linked by you"};
  const ids=l=>[l.imdb,l.tmdb?"TMDB "+l.tmdb:"",l.tvdb?"TVDB "+l.tvdb:""].filter(Boolean).join(" · ");
  box.innerHTML=`<h4>Linked in ${A}</h4>${links.length?links.map(l=>`<div class="src"><span class="pn">${esc(l.title)}${l.year?` (${l.year})`:""}</span>
      <span class="mi">${esc([ids(l),how[l.how],l.has_file?"has a file":"no file yet"].filter(Boolean).join(" · "))}</span>
      <button class="btn s d" data-un="${l.arr_id}">Not this ${w}</button></div>`).join(""):`<div class="muted">Not linked to anything in ${A}. VODgrab will not offer this title to ${A}.</div>`}
    <div class="tools" style="margin:8px 0 0"><button class="btn s" id="lnkChg">Link to a different ${w}…</button></div><div id="lnkPick"></div>`;
  $$("[data-un]",box).forEach(b=>b.onclick=async()=>{const l=links.find(y=>y.arr_id==b.dataset.un);
    if(!confirm(`This title is not ${A}'s "${l.title}${l.year?" ("+l.year+")":""}"?\n\nVODgrab will stop offering it to ${A} for that ${w} and remove it from Wanted.`))return;
    try{const r=await api("link",{kind,xid:id,unlink:+b.dataset.un});drawLinks(kind,id,r.links);toast(`Unlinked from ${A}`)}catch(e){toast(e.message,1)}});
  $("#lnkChg").onclick=()=>{const pk=$("#lnkPick");pk.innerHTML=`<input id="lnkQ" placeholder="Search ${A}'s library" style="margin-top:8px;width:100%;max-width:420px"><div id="lnkRes" style="margin-top:6px"></div>`;
    const q=$("#lnkQ");q.focus();let tm;q.oninput=()=>{clearTimeout(tm);tm=setTimeout(async()=>{if(q.value.trim().length<2){$("#lnkRes").innerHTML="";return}
      const r=(await api(`arrsearch?kind=${kind}&q=${encodeURIComponent(q.value)}`)).filter(c=>!links.some(l=>l.arr_id==c.arr_id));
      $("#lnkRes").innerHTML=r.length?r.map(c=>`<div class="src"><span class="pn">${esc(c.title)}${c.year?` (${c.year})`:""}</span><span class="mi">${esc(ids(c))}</span>
        <button class="btn s p" data-ln="${c.arr_id}">Link</button></div>`).join(""):`<div class="muted">Nothing in ${A} matches.</div>`;
      $$("[data-ln]",$("#lnkRes")).forEach(b=>b.onclick=async()=>{
        try{const r=await api("link",{kind,xid:id,link:+b.dataset.ln,unlink:links.map(l=>l.arr_id)});drawLinks(kind,id,r.links);toast(`Linked in ${A}`)}catch(e){toast(e.message,1)}})},250)}}}
async function openMovie(id){
  modal(`<div class="head"><div>Loading…</div><button class="x">×</button></div>`);
  let x;try{x=await api("movie/"+id)}catch(e){toast(e.message,1);closeModal();return}
  const m=x.meta||{};
  const lib=x.library?(x.library.has_file?`<span class="st imported">In Radarr with file</span>`:x.library.in_radarr?`<span class="st queued">In Radarr, no file</span>`:""):"";
  const acts=`${lib}${x.job?st(x.job.status):""}${status.arrs&&status.arrs.radarr&&!(x.library&&x.library.in_radarr)?`<button class="btn s" id="addm">+ Add to Radarr</button>`:""}`;
  const col=m.collection?`<div class="sec"><h4>${esc(m.collection.name)}</h4><div class="scroller">${m.collection.parts.map(p=>`<div class="mini${p.id?"":" na"}${p.current?" cur":""}" ${p.id&&!p.current?`data-open="movie:${p.id}"`:""} title="${p.id?"":"Not on your providers"}"><div class="mp" style="${p.poster?`background-image:url('${esc(p.poster)}')`:""}">${p.lib?`<div class="bdg"><span class="lb ${p.lib=="has_file"?"lb2":"lb1"}">${p.lib=="has_file"?"Have it":"In Radarr"}</span></div>`:""}</div><div class="mt">${esc(p.title)}</div><div class="my">${p.year||""}${p.id?"":" · not on providers"}</div></div>`).join("")}</div></div>`:"";
  const plot=m.overview||x.plot||"";
  modal(heroHtml("movie",x.clean,m,{year:x.year,poster:x.poster,rating:x.rating},acts)+`<div class="sb"><div id="trbox"></div><div id="playbox"></div>
    ${plot?`<p class="ovw">${esc(plot)}</p>`:""}${!m.has&&x.director?`<div class="muted" style="margin-top:8px">Director: ${esc(x.director)}${x.cast?" · Cast: "+esc(x.cast):""}</div>`:""}
    ${noMeta(m)}${x.info_error?`<div class="muted">Provider details unavailable: ${esc(x.info_error)}</div>`:""}
    <div class="sec"><h4>Download</h4><div class="srcs" style="margin:0">${x.sources.map(s=>`<div class="src" data-sid="${s.id}"><span class="pn">${esc(s.provider)}</span>${qb(s)}
      <span class="mi" title="${esc(s.name)}">${esc(qinfo(s))} · ${esc(s.ext)}</span>
      ${s.dp_copies&&s.dp_copies.length?`<select data-dpc title="Which provider Dispatcharr downloads this from" style="max-width:260px"><option value="">Dispatcharr picks (${s.dp_copies.length} cop${s.dp_copies.length==1?"y":"ies"})</option>${s.dp_copies.map((c,i)=>`<option value="${i}">${esc(c.account)}${c.quality?" · "+esc(c.quality):""}${c.ext?" · "+esc(c.ext):""}</option>`).join("")}</select>`:s.dp_error?`<span class="muted" title="${esc(s.dp_error)}">Dispatcharr's copies unavailable</span>`:""}
      ${s.known?"":`<button class="btn s" data-chk>Check quality</button>`}<button class="btn s" data-play>▶ Play</button><button class="btn s p" data-dl>Download</button></div>`).join("")}</div>
      <div class="tools" style="margin:8px 0 0">${nowBox}<span class="muted" style="font-size:12px">${x.sources.length>1?"Starts with the provider you pick and falls back to the others. ":""}Check quality reads the file header for a few seconds.</span></div></div>
    ${x.arr_ok?`<div class="sec" id="linkSec"></div>`:""}
    ${peopleHtml(m)}${watchHtml(m)}${col}${recsHtml(m)}${kwHtml(m)}${idsHtml("movie",m,x)}</div>`);
  wireDetail("movie",m);if(x.arr_ok)drawLinks("movie",id,x.links);
  if($("#addm"))$("#addm").onclick=e=>addArr([{kind:"movie",id}],e.target);
  $$(".src[data-sid]").forEach(r=>{const sid=+r.dataset.sid;
    $("[data-dl]",r).onclick=async e=>{e.target.disabled=true;const src=x.sources.find(y=>y.id==sid)||{},sel=$("[data-dpc]",r);
      await dl({kind:"movie",id:sid,now:$("#now").checked,dp:sel&&sel.value!==""?src.dp_copies[+sel.value]:null});closeModal()};
    $("[data-play]",r).onclick=()=>{const s=x.sources.find(y=>y.id==sid)||{};play("movie",sid,x.clean+" from "+(s.provider||""),[s.quality,s.codec,s.ext].filter(Boolean).join(" · "))};
    const c=$("[data-chk]",r);if(c)c.onclick=async()=>{const q=await probeIt("movie",sid,c);if(q){c.remove();$(".q",r).outerHTML=qb(q);$(".mi",r).textContent=qinfo(q)}}});
  const todo=status.probe_open?x.sources.filter(s=>s.source!="checked"):[];
  if(todo.length){todo.forEach(s=>{const r=$(`.src[data-sid="${s.id}"]`);if(!r)return;$(".q",r).outerHTML=`<span class="q unk checking" title="Checking quality">…</span>`;const c=$("[data-chk]",r);if(c)c.hidden=true});
    let res={};try{res=await api("probe/open",{items:todo.map(s=>["movie",s.id])})}catch(e){}
    todo.forEach(s=>{const r=$(`.src[data-sid="${s.id}"]`);if(!r)return;const q=res[s.id]||s;const c=$("[data-chk]",r);
      $(".q",r).outerHTML=qb(q);$(".mi",r).textContent=q.missing?"Missing on the provider: "+q.missing:qinfo(q);if(q.missing)$(".mi",r).style.color="var(--bad)";
      if(c){if(q.known&&!q.pending)c.remove();else c.hidden=false}})}}
async function openSeries(id){
  modal(`<div class="head"><div>Loading episodes…</div><button class="x">×</button></div>`);
  let x;try{x=await api("series/"+id)}catch(e){toast(e.message,1);closeModal();return}
  const m=x.meta||{};
  let cur=(x.seasons.find(s=>!s.none&&s.season>0)||x.seasons[0]||{}).season;if(cur===undefined)cur=null;
  const epHtml=e=>{const n=`E${String(e.episode).padStart(2,"0")}`,t=e.tname||e.title||("Episode "+e.episode);
    const meta=[e.air?fmtDate(e.air):"",e.runtime?e.runtime+"m":""].filter(Boolean).join(" · ");
    return `<div class="ep2${e.missing?" na":""}"><div class="still" ${e.still?`style="background-image:url('${esc(e.still)}')"`:""}>${e.still?"":n}</div>
      <div class="eb"><div class="eh"><span class="en">${n}</span><span class="et">${esc(t)}</span>${e.wanted?`<span class="lb lb3">Wanted</span>`:""}${meta?`<span class="em">${esc(meta)}</span>`:""}</div>
      ${e.overview?`<div class="eo" title="Click to expand">${esc(e.overview)}</div>`:""}
      <div class="ea">${e.missing?`<span class="muted" style="font-size:12px">Not on your providers</span>`:`<span title="${esc(qinfo(e.media))}">${e.checking?`<span class="q unk checking" title="Checking quality">…</span>`:qb(e.media)}</span>${e.media.missing?`<span style="color:var(--bad);font-size:12px">Missing on the provider</span>`:""}${e.media.known?`<span class="em">${esc([e.media.codec,dur(e.media.duration),e.media.size?fb(e.media.size):""].filter(Boolean).join(" · "))}</span>`:""}${e.job?st(e.job):""}
        ${e.media.known||e.checking?"":`<button class="btn s" data-c="${e.id}">Check quality</button>`}<button class="btn s" data-pl="${e.id}">▶ Play</button><button class="btn s p" data-e="${e.id}">Download</button>`}</div></div></div>`};
  const probed=new Set();
  const checkSeason=async se=>{const how=status.probe_series||"off";if(how=="off"||probed.has(se.season))return;probed.add(se.season);
    const avail=se.episodes.filter(e=>e.id&&!e.missing);
    const todo=(how=="first"?avail.slice(0,1):avail).filter(e=>e.media.source!="checked");if(!todo.length)return;
    todo.forEach(e=>e.checking=true);
    let res={};try{res=await api("probe/open",{items:todo.map(e=>["episode",e.id])})}catch(err){}
    todo.forEach(e=>{e.checking=false;if(res[e.id])e.media=res[e.id]});if(cur==se.season&&$("#eps"))draw()};
  const draw=()=>{const se=x.seasons.find(s=>s.season==cur);
    $("#eps").innerHTML=se?se.episodes.map(epHtml).join(""):"";
    $$("#stabs button").forEach(b=>b.classList.toggle("p",+b.dataset.s==cur));
    const has=se&&se.episodes.some(e=>!e.missing);$("#dls").disabled=!has;
    $$("#eps .eo").forEach(o=>o.onclick=()=>o.classList.toggle("open"));
    $$("#eps [data-c]").forEach(b=>b.onclick=async()=>{const q=await probeIt("episode",+b.dataset.c,b);if(q){const ep=se.episodes.find(y=>y.id==+b.dataset.c);ep.media=q;draw()}});
    $$("#eps [data-e]").forEach(b=>b.onclick=async()=>{b.disabled=true;await dl({kind:"episode",id:+b.dataset.e,now:$("#now").checked})});
    $$("#eps [data-pl]").forEach(b=>b.onclick=()=>{const e=se.episodes.find(y=>y.id==+b.dataset.pl);play("episode",e.id,`${x.clean} S${String(e.season).padStart(2,"0")}E${String(e.episode).padStart(2,"0")}${e.tname||e.title?" · "+(e.tname||e.title):""}`,[e.media.quality,e.media.codec,e.ext].filter(Boolean).join(" · "))});
    if(se)checkSeason(se)};
  const acts=`${status.arrs&&status.arrs.sonarr&&!x.in_sonarr?`<button class="btn s" id="adds">+ Add to Sonarr</button>`:x.in_sonarr?`<span class="st imported">In Sonarr</span>`:""}`;
  const avail=x.seasons.reduce((n,s)=>n+s.episodes.filter(e=>!e.missing).length,0),miss=x.seasons.reduce((n,s)=>n+s.episodes.filter(e=>e.missing).length,0);
  modal(heroHtml("series",x.clean,m,{year:x.year,poster:x.icon,rating:x.rating},acts)+`<div class="sb"><div id="trbox"></div>
    ${m.overview||x.plot?`<p class="ovw">${esc(m.overview||x.plot)}</p>`:""}${noMeta(m)}
    <div class="sec"><h4>Episodes <span style="text-transform:none;letter-spacing:0;font-weight:400">· ${avail} on ${esc(x.provider)}${x.also_on.length?" and "+x.also_on.map(esc).join(", "):""}${miss?` · ${miss} aired episode${miss==1?"":"s"} not on your providers`:""}</span></h4>
    <div class="tools" style="margin-bottom:8px"><div class="tabs" id="stabs">${x.seasons.map(s=>`<button class="btn s stab${s.none?" none":""}" data-s="${s.season}" title="${s.none?"Not on your providers":""}">${s.season==0?"Specials":esc(s.name&&!/^Season \d+$/.test(s.name)?s.name:"Season "+s.season)}</button>`).join("")}</div></div>
    <div class="tools"><button class="btn" id="dls">Download season</button><button class="btn" id="dla">Download all seasons</button>${nowBox}</div>
    ${x.info_error?`<div class="muted">Episode list failed: ${esc(x.info_error)}</div>`:""}
    <div id="playbox"></div><div class="eps2" id="eps">${x.seasons.length?"":`<div class="muted">No episodes listed by the provider.</div>`}</div></div>
    ${peopleHtml(m)}${watchHtml(m)}${recsHtml(m)}${kwHtml(m)}${idsHtml("series",m,x)}</div>`);
  wireDetail("series",m);
  $$("#stabs button").forEach(b=>b.onclick=()=>{cur=+b.dataset.s;draw()});
  if($("#adds"))$("#adds").onclick=e=>addArr([{kind:"series",id}],e.target);
  $("#dls").onclick=()=>cur!=null&&dl({kind:"season",id,season:cur,now:$("#now").checked});
  $("#dla").onclick=()=>{if(confirm("Queue every episode of this series?"))dl({kind:"series",id,now:$("#now").checked})};
  if(cur!=null)draw()}

/* ---------------- queue */
async function renderQueue(){
  const m=$("#main");
  const draw=async()=>{let rows;try{rows=await api("queue")}catch(e){return}
    if(tab!="queue")return;
    m.innerHTML=`<div class="tools"><span class="muted">${rows.length} active</span></div>`+(rows.length?`<div class="list">${rows.map(j=>{
      const pct=j.total?Math.min(100,j.done*100/j.total):0;
      const eta=j.speed&&j.total?fd((j.total-j.done)/j.speed):"";
      const info=j.status=="downloading"?`${fb(j.done)} of ${j.total?fb(j.total):"?"}${j.speed?" · "+fb(j.speed)+"/s":""}${eta?" · "+eta+" left":""}`
        :j.status=="retry_wait"?`${esc(j.error)}${j.next_at?" · next try "+when(j.next_at):""}`:j.error?esc(j.error):"";
      return `<div class="row"><div class="main"><div class="title">${esc(j.label)}</div>
        <div class="sub"><span class="tag">${j.source=="arr"?esc(j.arr):"Manual"}</span>${j.provider&&j.status!="queued"?`<span class="tag">${esc(j.provider)}</span>`:""}${st(j.status)} ${j.force?`<span class="tag">Now</span>`:""} ${info}</div>
        ${j.status=="downloading"||pct?`<div class="prog"><i style="width:${pct}%"></i></div>`:""}</div>
        <div class="acts">${["queued","retry_wait"].includes(j.status)?`<button class="btn s" data-a="now" data-id="${j.id}">Download now</button><button class="btn s" data-a="up" data-id="${j.id}">↑</button><button class="btn s" data-a="down" data-id="${j.id}">↓</button>`:""}
        ${j.status!="importing"?`<button class="btn s d" data-a="cancel" data-id="${j.id}">Cancel</button>`:""}</div></div>`}).join("")}</div>`:`<div class="empty">Nothing queued.</div>`);
    $$("[data-a]",m).forEach(b=>b.onclick=async()=>{try{await api(`job/${b.dataset.id}/${b.dataset.a}`,{});draw()}catch(e){toast(e.message,1)}})};
  await draw();timer=setInterval(draw,2000)}

/* ---------------- wanted */
const sk=t=>String(t||"").normalize("NFKD").replace(/[\u0300-\u036f]/g,"").toLowerCase().replace(/&/g," and ").replace(/[^a-z0-9]+/g," ").trim().replace(/^(the|a|an) /,"");
const skl=t=>{const c=sk(t)[0]||"";return c>="a"&&c<="z"?c.toUpperCase():"#"};
let W=null;const WDEF={arr:"all",q:"",prov:"",quality:"",fresh:"",sort:"title",na:""};let WF={...WDEF};
function wReadHash(){const [t,qs]=location.hash.replace(/^#/,"").split("?");if(t!="wanted")return;const p=new URLSearchParams(qs||"");WF={...WDEF};for(const [k,v] of p)if(k in WDEF)WF[k]=v}
function wWriteHash(){const p=new URLSearchParams();for(const k in WDEF)if(WF[k]!==WDEF[k]&&WF[k]!=="")p.set(k,WF[k]);const h="#wanted"+(p.toString()?"?"+p:"");if(location.hash!==h){skipHash=true;history.replaceState(null,"",h)}}
async function renderWanted(){
  const m=$("#main");wReadHash();if(!W)m.innerHTML=`<div class="empty">Loading…</div>`;
  try{W=await api("wanted")}catch(e){m.innerHTML=`<div class="empty">${esc(e.message)}</div>`;return}
  if(tab!="wanted")return;
  const provs=[...new Set([...W.movies.map(x=>x.card.providers),...W.shows.map(s=>s.card.providers)].flat())].sort();
  m.innerHTML=`<p class="muted">Movies and episodes Radarr and Sonarr are waiting for (monitored, released or aired, no file) that a provider has. Search asks the arr to look, so it grabs through VODgrab using your quality profiles.</p>
    <div class="tools">
      <div class="seg" id="wArr"><button data-v="all">Both</button><button data-v="radarr" ${W.radarr?"":"disabled"}>Radarr</button><button data-v="sonarr" ${W.sonarr?"":"disabled"}>Sonarr</button></div>
      <input type="search" id="wq" placeholder="Filter by title" value="${esc(WF.q)}" autocomplete="off">
      ${provs.length>1?`<select id="wprov"><option value="">All providers</option>${provs.map(p=>`<option>${esc(p)}</option>`).join("")}</select>`:""}
      <select id="wqual"><option value="">Any quality</option><option value="2160p">4K</option><option value="1080p">1080p</option><option value="720p">720p</option><option value="480p">480p</option><option value="unknown">Not checked yet</option></select>
      <select id="wsort"><option value="title">Title</option><option value="found">Newest found</option><option value="date">Air or release date</option></select>
      <label class="muted" style="display:inline-flex;gap:6px;align-items:center"><input type="checkbox" id="wfresh" ${WF.fresh?"checked":""}> Not searched yet</label>
      <label class="muted" style="display:inline-flex;gap:6px;align-items:center"><input type="checkbox" id="wna" ${WF.na?"checked":""}> Show episodes no provider has</label></div>
    <div class="tools"><button class="btn p" id="wAll">Search</button><button class="btn" id="wRef">Refresh</button>
      <span class="muted">${W.running?"Refreshing…":W.at?"Checked "+ago(W.at):"Not checked yet"}${W.error?" · "+esc(W.error):""} · Automatic search ${W.auto?"on":"off"} <a href="#settings" onclick="go('settings');return false">change</a></span></div>
    ${!W.sonarr&&!W.radarr?`<div class="empty">Connect Sonarr or Radarr in Settings to see what they want.</div>`:""}
    <div id="wlist"></div>`;
  $$("#wArr button").forEach(b=>{b.classList.toggle("on",b.dataset.v==WF.arr);b.onclick=()=>{WF.arr=b.dataset.v;$$("#wArr button").forEach(x=>x.classList.toggle("on",x.dataset.v==WF.arr));wDraw()}});
  let deb;$("#wq").oninput=e=>{clearTimeout(deb);deb=setTimeout(()=>{WF.q=e.target.value.trim();wDraw()},200)};
  if($("#wprov")){$("#wprov").value=WF.prov;$("#wprov").onchange=e=>{WF.prov=e.target.value;wDraw()}}
  $("#wqual").value=WF.quality;$("#wqual").onchange=e=>{WF.quality=e.target.value;wDraw()};
  $("#wsort").value=WF.sort;$("#wsort").onchange=e=>{WF.sort=e.target.value;wDraw()};
  $("#wfresh").onchange=e=>{WF.fresh=e.target.checked?"1":"";wDraw()};
  $("#wna").onchange=e=>{WF.na=e.target.checked?"1":"";wDraw()};
  $("#wRef").onclick=async()=>{await api("wanted/refresh",{});toast("Checking Sonarr and Radarr…");setTimeout(()=>{if(tab=="wanted")renderWanted();refreshStatus()},4000)};
  wDraw();
  if(W.running)setTimeout(()=>{if(tab=="wanted")renderWanted()},3000)}

function wFiltered(){const qn=WF.q.toLowerCase();
  const okCard=c=>(!qn||c.clean.toLowerCase().includes(qn))&&(!WF.prov||c.providers.includes(WF.prov))&&
    (!WF.quality||(WF.quality=="unknown"?!c.quality:c.quality==WF.quality));
  const movies=WF.arr=="sonarr"?[]:W.movies.filter(x=>okCard(x.card)&&(!WF.fresh||!x.searched));
  const shows=WF.arr=="radarr"||WF.quality&&WF.quality!="unknown"?[]:W.shows.filter(s=>okCard(s.card)).map(s=>{
    const eps=s.episodes.filter(e=>(e.available||WF.na)&&(!WF.fresh||!e.searched));return {...s,shown:eps}}).filter(s=>s.shown.some(e=>e.available));
  const newest=x=>Math.max(0,...(x.episodes||[x]).map(e=>e.found||0));
  const date=x=>x.card.year||0;
  const cmp={title:(a,b)=>sk(a.card.clean).localeCompare(sk(b.card.clean)),found:(a,b)=>newest(b)-newest(a)||a.card.clean.localeCompare(b.card.clean),
    date:(a,b)=>(b.episodes?Math.max(0,...b.episodes.map(e=>Date.parse(e.air)||0)):date(b)*3e10)-(a.episodes?Math.max(0,...a.episodes.map(e=>Date.parse(e.air)||0)):date(a)*3e10)}[WF.sort];
  return {movies:movies.slice().sort(cmp),shows:shows.slice().sort(cmp)}}

function wDraw(){const box=$("#wlist");if(!box)return;wWriteHash();
  const {movies,shows}=wFiltered();
  const ids={m:movies.map(x=>x.arr_id),e:shows.flatMap(s=>s.shown.filter(e=>e.available).map(e=>e.arr_ep))};
  const btn=$("#wAll"),n=ids.m.length+ids.e.length;
  btn.disabled=!n;btn.textContent=n?`Search these ${n} in ${WF.arr=="radarr"?"Radarr":WF.arr=="sonarr"?"Sonarr":"Sonarr and Radarr"}`:"Nothing to search";
  btn.onclick=e=>{if(confirm(`Ask ${ids.m.length?`Radarr to search ${ids.m.length} movie${ids.m.length==1?"":"s"}`:""}${ids.m.length&&ids.e.length?" and ":""}${ids.e.length?`Sonarr to search ${ids.e.length} episode${ids.e.length==1?"":"s"}`:""}?`))wGo(ids.m,ids.e,e.target)};
  const srch=t=>t?`<span class="muted" style="font-size:12px">searched ${ago(t)}</span>`:"";
  const nE=ids.e.length;
  box.innerHTML=(movies.length?`<h3 class="wh">Movies <span class="muted">${movies.length}</span></h3><div class="list">${movies.map(x=>`<div class="row" data-l="${skl(x.card.clean)}"><div class="main"><div class="title">${esc(x.card.clean)} ${x.card.year?`<span class="muted">(${x.card.year})</span>`:""}</div>
      <div class="sub">${x.card.quality?`<span class="q k${x.card.quality}">${x.card.quality=="2160p"?"4K":x.card.quality}</span> `:""}${esc(x.card.providers.join(", "))} ${srch(x.searched)}</div></div>
      <div class="acts"><button class="btn s" data-open-m="${x.card.id}">Details</button><button class="btn s p" data-sm="${x.arr_id}">Search in Radarr</button></div></div>`).join("")}</div>`:"")+
    (shows.length?`<h3 class="wh">Episodes <span class="muted">${nE} in ${shows.length} show${shows.length==1?"":"s"}</span></h3><div class="list">${shows.map((s,i)=>{const av=s.shown.filter(e=>e.available).length;return `<div class="row" style="display:block" data-l="${skl(s.card.clean)}">
      <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap"><div class="main"><div class="title">${esc(s.card.clean)} ${s.card.year?`<span class="muted">(${s.card.year})</span>`:""}</div>
      <div class="sub">${s.available} of ${s.missing} missing episode${s.missing==1?"":"s"} available · ${esc(s.card.providers.join(", "))}</div></div>
      <div class="acts"><button class="btn s" data-exp="${i}">Episodes</button><button class="btn s" data-open-s="${s.card.id}">Details</button><button class="btn s p" data-ss="${i}">Search ${av} in Sonarr</button></div></div>
      <div class="eps" data-eps="${i}" ${WF.q||shows.length==1?"":"hidden"}>${s.shown.map(e=>`<div class="ep${e.available?"":" na"}"><span class="n">S${String(e.season).padStart(2,"0")}E${String(e.episode).padStart(2,"0")}</span><span class="et">${esc(e.title)}${e.air?` <span class="muted" style="font-size:12px">· ${new Date(e.air).toLocaleDateString()}</span>`:""}</span>
        ${e.available?`${srch(e.searched)}<button class="btn s" data-se="${e.arr_ep}">Search</button>`:`<span class="muted" style="font-size:12px">Not on any provider</span>`}</div>`).join("")}</div></div>`}).join("")}</div>`:"")+
    ((W.sonarr||W.radarr)&&!movies.length&&!shows.length?`<div class="empty">${!W.at?"Press Refresh to check.":W.movies.length+W.shows.length?"Nothing matches these filters.":"Nothing Sonarr or Radarr wants is on your providers right now."}</div>`:"");
  $$("[data-sm]",box).forEach(b=>b.onclick=()=>wGo([+b.dataset.sm],[],b));
  $$("[data-ss]",box).forEach(b=>b.onclick=()=>wGo([],shows[+b.dataset.ss].shown.filter(e=>e.available).map(e=>e.arr_ep),b));
  $$("[data-se]",box).forEach(b=>b.onclick=()=>wGo([],[+b.dataset.se],b));
  $$("[data-exp]",box).forEach(b=>b.onclick=()=>{const d=$(`[data-eps="${b.dataset.exp}"]`,box);d.hidden=!d.hidden});
  $$("[data-open-m]",box).forEach(b=>b.onclick=()=>openMovie(+b.dataset.openM));
  $$("[data-open-s]",box).forEach(b=>b.onclick=()=>openSeries(+b.dataset.openS));
  if(WF.sort=="title"){const cnt={};$$("#wlist [data-l]").forEach(r=>cnt[r.dataset.l]=(cnt[r.dataset.l]||0)+1);
    drawRail(["#",..."ABCDEFGHIJKLMNOPQRSTUVWXYZ"].map(l=>[l,cnt[l]||0,0]),l=>{const el=$(`#wlist [data-l="${l}"]`);if(el){bubble(l);window.scrollTo({top:el.getBoundingClientRect().top+scrollY-80,behavior:"smooth"})}})}
  else hideRail()}
async function wGo(movies,episodes,btn){if(btn)btn.disabled=true;
  try{const r=await api("wanted/search",{movies,episodes});toast(`Asked Radarr to search ${r.movies} movie${r.movies==1?"":"s"} and Sonarr to search ${r.episodes} episode${r.episodes==1?"":"s"}`);renderWanted()}
  catch(e){toast(e.message,1);if(btn)btn.disabled=false}}

/* ---------------- history */
async function renderHistory(){
  const rows=await api("history");const m=$("#main");
  m.innerHTML=rows.length?`<div class="list">${rows.map(j=>`<div class="row"><div class="main"><div class="title">${esc(j.label)}</div>
    <div class="sub"><span class="tag">${j.source=="arr"?esc(j.arr):"Manual"}</span>${j.provider?`<span class="tag">${esc(j.provider)}</span>`:""}${st(j.status)} ${j.total?fb(j.total)+" · ":""}${j.probe?esc(j.probe)+" · ":""}${ago(j.updated)}</div>
    ${j.error?`<div class="sub" style="color:${["failed","import_failed"].includes(j.status)?"var(--bad)":"var(--dim)"}">${esc(j.error)}</div>`:""}
    <div class="sub">${esc(j.release)}</div></div>
    <div class="acts">${["failed","import_failed","cancelled"].includes(j.status)?`<button class="btn s" data-a="retry" data-id="${j.id}">${j.status=="import_failed"?"Retry import":"Retry"}</button>`:""}
    <button class="btn s d" data-a="delete" data-id="${j.id}">${j.status=="import_failed"?"Delete file":"Remove"}</button></div></div>`).join("")}</div>`:`<div class="empty">No history yet.</div>`;
  $$("[data-a]",m).forEach(b=>b.onclick=async()=>{if(b.dataset.a=="delete"&&b.textContent.includes("file")&&!confirm("Delete the downloaded file?"))return;
    try{const r=await api(`job/${b.dataset.id}/${b.dataset.a}`,{});if(r.message)toast(r.message);renderHistory();refreshStatus()}catch(e){toast(e.message,1)}})}

/* ---------------- unmatched */
async function renderUnmatched(){
  const rows=await api("unmatched");const m=$("#main");
  m.innerHTML=`<p class="muted">Titles Sonarr or Radarr searched for that could not be matched to the provider catalog. Pick the right provider title to create a permanent override.</p>`+
   (rows.length?`<div class="list">${rows.map((u,i)=>`<div class="row" style="display:block"><div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap">
    <div class="main"><div class="title">${esc(u.title)} ${u.year?`<span class="muted">(${u.year})</span>`:""}</div><div class="sub"><span class="tag">${esc(u.arr)}</span>${u.arr=="sonarr"?"TVDB":"ID"} ${esc(u.arr_id)} · ${ago(u.seen)}</div></div>
    <div class="acts"><button class="btn s" data-pick="${i}">Pick match</button><button class="btn s d" data-dis="${i}">Dismiss</button></div></div>
    <div data-box="${i}" hidden style="margin-top:10px"><input type="search" placeholder="Search provider catalog" value="${esc(u.title)}"><div class="list" style="margin-top:8px"></div></div></div>`).join("")}</div>`:`<div class="empty">Everything requested so far has matched.</div>`);
  $$("[data-dis]",m).forEach(b=>b.onclick=async()=>{const u=rows[b.dataset.dis];await api("dismiss",{arr:u.arr,arr_id:u.arr_id});renderUnmatched();refreshStatus()});
  $$("[data-pick]",m).forEach(b=>b.onclick=()=>{const i=b.dataset.pick,u=rows[i],box=$(`[data-box="${i}"]`,m);box.hidden=false;const inp=$("input",box),res=$(".list",box);
    const search=async()=>{const r=await api("browse?"+new URLSearchParams({type:u.arr=="radarr"?"movie":"series",q:inp.value,limit:12,merge:"0"}));
      res.innerHTML=r.items.map(x=>`<div class="ep"><span class="et">${esc(x.clean)} ${x.year?`(${x.year})`:""}</span><button class="btn s p" data-x="${x.id}">Use this</button></div>`).join("")||`<div class="muted">No results</div>`;
      $$("[data-x]",res).forEach(y=>y.onclick=async()=>{await api("override",{arr:u.arr,arr_id:u.arr_id,xid:+y.dataset.x});toast("Override saved. Search again from "+(u.arr=="radarr"?"Radarr":"Sonarr"));renderUnmatched();refreshStatus()})};
    let d;inp.oninput=()=>{clearTimeout(d);d=setTimeout(search,250)};search()})}

/* ---------------- settings */
const DL=["mon","tue","wed","thu","fri","sat","sun"];
let SV={};
function fld(k,label,type="text",hint="",opts){const v=SV.settings[k];let inp;
  if(type=="bool")return `<div class="f chk"><input type="checkbox" id="s_${k}" ${v?"checked":""}><label for="s_${k}" style="margin:0;color:var(--text)">${label}</label></div>`;
  if(type=="select")inp=`<select id="s_${k}">${opts.map(o=>`<option value="${esc(o[0])}" ${String(o[0])==String(v)?"selected":""}>${esc(o[1])}</option>`).join("")}</select>`;
  else if(type=="area")inp=`<textarea id="s_${k}">${esc((v||[]).join("\n"))}</textarea>`;
  else if(type=="secret")inp=`<input type="password" id="s_${k}" autocomplete="new-password" placeholder="${SV.secrets_set[k]?"Saved, leave blank to keep":""}">`;
  else inp=`<input type="${type}" id="s_${k}" value="${esc(Array.isArray(v)?v.join(", "):v)}">`;
  return `<div class="f"><label for="s_${k}">${label}</label>${inp}${hint?`<div class="hint">${hint}</div>`:""}</div>`}
function pathFld(k,name,label){return `<div class="f"><label for="s_${k}">${label}</label><input id="s_${k}" value="${esc(SV.settings[k])}" placeholder="${esc(SV.folder_defaults[name])}"><div class="hint">Now: <span class="path">${esc(SV.folders[name])}</span></div></div>`}
function provCard(p){const n=!p.id;return `<div class="prov" data-pid="${p.id||""}">
  <div class="ph"><b>${esc(p.name||"New provider")}</b>${n?"":`<span class="muted" style="font-size:12px">${p.movies.toLocaleString()} movies · ${p.series.toLocaleString()} series · ${p.active} active download${p.active==1?"":"s"}</span>`}
  ${p.error?`<span class="st failed">${esc(p.error)}</span>`:""}</div>
  <div class="fields">
   <div class="f"><label>Name</label><input data-f="name" value="${esc(p.name||"")}" placeholder="For example Provider A"></div>
   <div class="f"><label>Server URL</label><input data-f="url" value="${esc(p.url||"")}" placeholder="https://host:port"></div>
   <div class="f"><label>Username</label><input data-f="username" value="${esc(p.username||"")}"></div>
   <div class="f"><label>Password</label><input data-f="password" type="password" autocomplete="new-password" placeholder="${p.password_set?"Saved, leave blank to keep":""}"></div>
   <div class="f"><label>Max connections</label><input data-f="max_conn" type="number" min="1" max="20" value="${p.max_conn||1}"><div class="hint">Streams VODgrab may use at once on this account</div></div>
   <div class="f"><label>Priority</label><input data-f="priority" type="number" min="1" value="${p.priority||""}" placeholder="Next"><div class="hint">1 is tried first</div></div>
   <div class="f"><label>User agent</label><input data-f="user_agent" value="${esc(p.user_agent||"")}" placeholder="VLC/3.0.20 LibVLC/3.0.20"></div>
   <div class="f chk"><input type="checkbox" data-f="enabled" ${p.enabled!==0?"checked":""}><label style="margin:0;color:var(--text)">Enabled</label></div></div>
  <div class="tools" style="margin-top:12px;margin-bottom:0"><button class="btn p s" data-ps>Save provider</button>${n?"":`<button class="btn s" data-pt>Test connection and file download</button><button class="btn s d" data-pd>Remove</button>`}</div>
  <div class="out"></div></div>`}
async function drawProvs(list){SV.providers=list.filter(p=>p.id);const box=$("#provs");box.innerHTML=list.map(provCard).join("")||`<div class="muted" style="margin-bottom:10px">No providers yet.</div>`;
  $$(".prov",box).forEach(c=>{const pid=c.dataset.pid,out=t=>{const o=$(".out",c);o.style.display="block";o.textContent=t};
    const body=()=>{const b={id:pid||null};$$("[data-f]",c).forEach(i=>b[i.dataset.f]=i.type=="checkbox"?i.checked:i.value);return b};
    $("[data-ps]",c).onclick=async()=>{try{const r=await api("providers",body());toast("Provider saved"+(pid?"":". Run a sync to load its catalog."));drawProvs(r.providers);refreshStatus()}catch(e){out(e.message)}};
    const t=$("[data-pt]",c);if(t)t.onclick=async()=>{out("Testing…");try{await api("providers",body());out(JSON.stringify(await api("test/provider/"+pid),null,2))}catch(e){out(e.message)}};
    const d=$("[data-pd]",c);if(d)d.onclick=async()=>{if(!confirm("Remove this provider and its catalog? Queued downloads move to other providers that have the same title."))return;await api(`providers/${pid}/delete`,{});drawProvs(await api("providers"));refreshStatus()}})}
const ivLabel=h=>h<24?`Every ${h} hour${h>1?"s":""}`:h==24?"Every 24 hours (daily)":`Every ${h/24} days`;
function winRow(w,i){return `<div class="win" data-w="${i}"><span class="days">${DL.map(d=>`<label><input type="checkbox" value="${d}" ${w.days.includes(d)?"checked":""}> ${d[0].toUpperCase()+d.slice(1)}</label>`).join("")}</span>
  from <input type="time" class="ws" value="${w.start}"> to <input type="time" class="we" value="${w.end}"> total streams <input type="number" class="wc" min="0" max="50" value="${w.concurrency}" title="0 uses the total from Downloads">
  <button class="btn s d" data-rm="${i}">Remove</button></div>`}
async function renderSettings(){
  SV=await api("settings");const s=SV.settings;const m=$("#main");
  const arrSec=(a,L)=>`<section class="set"><h3>${L}</h3><div class="fields">
    ${fld(a+"_url","URL","text","For example http://10.0.0.20:"+(a=="sonarr"?"8989":"7878"))}${fld(a+"_api_key","API key","secret")}
    <div class="f"><label>Library folder for new ${a=="sonarr"?"shows":"movies"}</label><input id="s_${a}_root" list="dl_${a}" value="${esc(s[a+"_root"])}" placeholder="${L}'s first root folder"><datalist id="dl_${a}"></datalist>
      <div class="hint">Used when VODgrab adds ${a=="sonarr"?"a show":"a movie"} to ${L}: the Add to ${L} button, and manual downloads of titles ${L} doesn't have yet. Pick one of ${L}'s root folders.</div></div>
    <div class="f"><label>Quality profile for new ${a=="sonarr"?"shows":"movies"}</label><select id="s_${a}_profile_id"><option value="0">First profile</option></select><div class="hint">The quality ${L} aims for on titles VODgrab adds. Press Load from ${L} to list its profiles.</div></div>
    ${fld(a+"_add_search","Search right away after adding a "+(a=="sonarr"?"show":"movie"),"bool")}
    ${a=="sonarr"?fld("sonarr_add_monitor","Episodes to monitor when adding a show","select","",[["all","All episodes"],["future","Future episodes"],["missing","Missing episodes"],["existing","Existing episodes"],["firstSeason","First season"],["lastSeason","Latest season"],["pilot","Pilot only"],["none","None"]]):""}
    ${fld(a+"_indexer_priority","Indexer priority in "+L,"number","1 to 50, lower wins when several indexers offer the same release. "+L+"'s default is 25. Applied when you press Set up.")}</div>
    <div class="tools" style="margin-top:12px"><button class="btn" data-load="${a}">Load from ${L}</button><button class="btn" data-test="${a}">Test</button><button class="btn p" data-setup="${a}">Set up ${L} automatically</button></div>
    <div class="hint muted">Setup adds VODgrab to ${L} as a Newznab indexer, a SABnzbd download client and an import webhook, using this page's address. Run it again after changing the priority.</div><div class="out" id="o_${a}"></div></section>`;
  m.innerHTML=`
  <section class="set"><h3>Providers</h3>
    <div class="hint muted" style="margin-bottom:10px">Each title downloads from the first provider in priority order that has it and a free connection. If it fails there, the next provider is tried.</div>
    <div id="provs"></div><button class="btn s" id="addProv">Add provider</button></section>
  <section class="set"><h3>Dispatcharr</h3>
    <div class="hint muted" style="margin-bottom:10px">Use Dispatcharr as a provider, so downloads go through it and count toward each account's connection limit together with live TV. Or copy Dispatcharr's Xtream accounts into VODgrab as providers.</div>
    <div class="fields">
    ${fld("dispatcharr_url","Dispatcharr address","text","For example http://dispatcharr:9191")}
    ${fld("dispatcharr_api_key","Dispatcharr API key","secret","In Dispatcharr: Users, edit your user, API key")}
    </div>
    <div class="tools" style="margin-top:10px"><button class="btn s" id="dpConnect">Connect</button></div>
    <div id="dpBox"></div></section>
  ${arrSec("sonarr","Sonarr")}${arrSec("radarr","Radarr")}
  <section class="set"><h3>Folders</h3>
    <div class="hint muted" style="margin-bottom:10px">Downloads land in these folders first. Sonarr and Radarr then rename and move each file into their own library. All of these paths must be visible to Sonarr and Radarr at the same path.</div>
    <div class="fields">
    ${fld("base_path","Base path","text","Folders left blank below go under here")}
    ${pathFld("incomplete_path","incomplete","In progress downloads")}
    ${pathFld("sonarr_complete_path","sonarr","Finished Sonarr downloads")}
    ${pathFld("radarr_complete_path","radarr","Finished Radarr downloads")}
    ${pathFld("manual_tv_path","manual_tv","Finished manual TV downloads")}
    ${pathFld("manual_movies_path","manual_movies","Finished manual movie downloads")}
    ${fld("chown","Owner for new files (uid:gid)","text","Optional, for example 1000:1000")}
    ${fld("external_url","Address the arrs use to reach VODgrab","text","Filled in by automatic setup")}
    ${fld("auto_import_manual","Hand manual downloads to Sonarr and Radarr for import","bool")}</div></section>
  <section class="set"><h3>Downloads</h3><div class="fields">
    ${fld("concurrency","Total streams across all providers","number","0 means no total limit; each provider still has its own max")}
    ${fld("retries","Retry count","number","Attempts after the first failure")}
    ${fld("retry_backoff","Wait between retries (minutes)","text","Comma list, the last value repeats")}
    ${fld("max_wait_hours","Max hours to wait at connection limit","number")}
    ${fld("speed_limit_kbps","Speed limit (KB/s, 0 is unlimited)","number")}
    ${fld("gap_min","Pause between downloads, min seconds","number")}${fld("gap_max","Pause between downloads, max seconds","number")}
    ${fld("connect_timeout","Connection timeout (seconds)","number")}
    ${fld("default_quality","Quality when the name has no tag","select","",[["2160p","2160p"],["1080p","1080p"],["720p","720p"],["480p","480p"]])}</div></section>
  <section class="set"><h3>Download schedule</h3><div class="fields">
    ${fld("schedule_enabled","Only download inside these windows","bool")}
    ${fld("on_window_end","When a window ends","select","",[["finish","Finish the current download"],["pause","Pause and resume next window"]])}</div>
    <div id="wins" style="margin-top:12px"></div><button class="btn s" id="addWin">Add window</button>
    <div class="hint muted" style="margin-top:6px">Windows can cross midnight. Download now on a queue item ignores the schedule.</div></section>
  <section class="set"><h3>Catalog sync</h3><div class="fields">
    ${fld("sync_interval_hours","Sync interval","select","Runs start at midnight",SV.intervals.map(h=>[h,ivLabel(h)]))}
    ${fld("grace_cycles","Keep missing titles for this many syncs","number","When a provider stops listing a title, it stays this many more syncs before it is removed")}
    ${fld("missing_recheck_hours","Check missing titles again","select","An extra sync this long after titles go missing, re-reading only the providers that lost them, so a title gone twice is removed sooner. Only runs if it comes before the next scheduled sync.",[[0,"Off (next scheduled sync)"],[1,"After 1 hour"],[2,"After 2 hours"],[4,"After 4 hours"],[6,"After 6 hours"]])}</div>
    <div class="tools" style="margin-top:12px"><button class="btn" id="syncBtn">Sync now</button><a class="btn" id="lastChg" download hidden>Download last sync changes</a></div><div id="runs"></div></section>
  <section class="set"><h3>Quality checks</h3>
    <div class="hint muted" style="margin-bottom:10px">When Sonarr or Radarr search, VODgrab reads the header of every copy it is about to offer so the quality it reports is the real one. Copies with different quality become separate releases, and missing files are left out. Each check uses one provider connection for a few seconds and is remembered.</div>
    <div class="fields">
    ${fld("probe_on_search","Check quality when Sonarr or Radarr search","bool")}
    ${fld("probe_series_open","TV: check quality when you open a series","select","Whole season checks every episode of the season you are looking at",[["season","Whole season"],["first","First episode only"],["off","Don't check"]])}
    ${fld("probe_on_open","Movies: check quality when you open a movie","bool","Checks every copy of the movie that hasn't been checked yet. Both checks use free connections only, so downloads are never interrupted.")}
    ${fld("probe_budget","Time allowed per search (seconds)","number","Checks that don't finish in time run in the background for the next search. Keep under 60 so the arrs don't time out.")}
    ${fld("probe_max_age_days","Recheck a file after this many days","number")}</div></section>
  <section class="set"><h3>Browsing</h3><div class="fields">
    ${fld("show_adult","Show adult content","bool")}
    ${fld("wanted_auto_search","Automatically ask Sonarr and Radarr to search newly found wanted items","bool")}
    ${fld("category_map","Category sorting overrides","area","One per line, like <b>Top Picks = list</b> or <b>Docu = genre: Documentary</b> or <b>Max = service: HBO Max</b>. Groups: service, genre, list, ignore, adult. Applied on the next sync.")}</div>
    <div class="hint muted" style="margin-top:8px">With adult content off, adult titles are hidden from browsing and search and never offered to Sonarr or Radarr. VODgrab spots them from the provider's adult flag and category names like XXX, Adult or 18+.</div>
    <div class="tools" style="margin-top:10px"><button class="btn s" id="libBtn">Refresh library status from Sonarr and Radarr</button></div></section>
  <section class="set"><h3>Metadata</h3>
    <div class="hint muted" style="margin-bottom:10px">Artwork, cast, age ratings, runtime, trailers, where a title officially streams, collections and similar titles. TMDB fills this in the background at the rate below, then checks once a day for what changed. Details are kept in their own file (meta.db) that syncs, provider changes and upgrades never clear, and they go into the daily backups.</div>
    <div class="mstat" id="mstat">Loading…</div>
    <div class="fields">
    ${fld("tmdb_key","TMDB API key or Read Access Token","secret","Free: sign up at themoviedb.org, then Settings, API")}
    ${fld("meta_lang","Language for titles and overviews","text","Like en-US or fr-CA")}
    ${fld("meta_region","Country for age ratings and streaming services","text","Two letters, like CA or US")}
    ${fld("meta_rate","Requests per second while filling","number","TMDB allows about 40. 8 is gentle; 20 fills faster.")}
    ${fld("meta_auto","Fill in details in the background","bool")}
    ${fld("meta_match","Look up titles the provider didn't tag with a TMDB ID, by name and year","bool")}</div>
    <h4 class="sub">OMDb (optional)</h4><div class="fields">
    ${fld("omdb_key","OMDb API key","secret","Free key at omdbapi.com allows 1,000 lookups a day")}
    ${fld("omdb_on","Show IMDb, Rotten Tomatoes and Metacritic ratings when a title is opened","bool")}</div>
    <div class="hint muted">Only asked when you open a title, then remembered for 30 days.</div>
    <h4 class="sub">TVDB (optional)</h4><div class="fields">
    ${fld("tvdb_key","TVDB API key","secret")}${fld("tvdb_pin","TVDB subscriber PIN","secret","Only if your key needs one")}</div>
    <div class="hint muted">Only used when you open a show and neither Sonarr nor TMDB know its TVDB ID.</div>
    <div class="tools" style="margin-top:12px"><button class="btn" id="mTest">Test keys</button><button class="btn" id="mFill">Fill now</button></div><div class="out" id="o_meta"></div></section>
  <section class="set"><h3>Backups</h3>
    <div class="fields">
    ${fld("backups_on","Make backups on a schedule","bool")}
    ${fld("backup_interval_hours","How often","select","Counted from midnight, like the catalog sync",SV.intervals.map(h=>[h,ivLabel(h)]))}
    ${fld("backups_keep","Backups to keep","number","Older scheduled backups are deleted")}
    <div class="f"><label for="s_backup_path">Backup folder</label><input id="s_backup_path" value="${esc(s.backup_path)}" placeholder="${esc(SV.backup_default||"data/backups")}"><div class="hint">Blank keeps them next to the data. A NAS mount works well.</div></div></div>
    <div class="fgt" style="margin-top:12px">What scheduled backups include</div>
    <div class="fields">${fld("backup_config","Settings and providers (small; includes passwords and API keys)","bool")}${fld("backup_meta","Metadata from TMDB, OMDb and TVDB","bool")}${fld("backup_catalog","Catalog and download history","bool")}</div>
    <div class="tools" style="margin-top:14px">
      <button class="btn" id="bkNow">Back up now</button>
      <button class="btn" id="bkCfg">Download settings and providers</button>
      <button class="btn" id="bkDlNow">Download a full backup</button>
      <label class="btn" style="display:inline-flex;align-items:center;gap:6px">Upload and restore…<input type="file" id="bkUp" accept=".gz,.json,.db" hidden></label></div>
    <div class="out" id="o_bk"></div>
    <div id="bkRestore"></div>
    <div id="bklist" style="margin-top:10px"></div>
    <div class="hint muted" style="margin-top:8px">Settings and providers restore right away. Metadata or catalog restores swap the database files and restart VODgrab by itself; the old files are kept with .before-restore on the end. Restoring only the catalog keeps your current settings.</div></section>
  <section class="set"><h3>Matching</h3><div class="fields">
    ${fld("min_score","Minimum title match score (0 to 100)","number")}
    ${fld("cleanup_patterns","Extra cleanup patterns (regex, one per line)","area","Removed from provider names before matching")}</div></section>
  <section class="set"><h3>Access</h3><div class="fields">
    ${fld("web_username","Web username","text","Leave blank for no login")}${fld("web_password","Web password","secret")}
    <div class="f"><label>API key (indexer, download client, webhook)</label><input readonly value="${esc(s.api_key)}" onclick="this.select()"></div></div>
    <div class="tools" style="margin-top:12px"><button class="btn s" id="newKey">New API key</button></div></section>
  <section class="set"><h3>Catalog report</h3><div class="hint muted" style="margin-bottom:10px">A file listing every category, sample titles, name prefixes and the fields each provider sends. Useful for tuning search and filters. It contains no usernames, passwords or server addresses. It pulls the full catalog live, so it can take a minute.</div>
    <button class="btn s" id="repBtn">Download catalog report</button></section>
  <section class="set"><h3>Updates</h3><div id="updBox" class="sub">Checking for updates…</div>
    ${fld("update_check","Check for new versions once a day","bool")}
    <div class="tools" style="margin-top:10px"><button class="btn s" id="updCheck">Check now</button>
    <button class="btn s p" id="updApply" hidden>Update now</button></div></section>
  <section class="set"><h3>Log</h3><button class="btn s" id="logBtn">Show log</button> <a class="btn s" href="/api/logs/download" download>Download all logs</a><div class="out" id="o_log"></div></section>
  <div class="savebar"><button class="btn p" id="save">Save settings</button></div>`;
  drawProvs(SV.providers);$("#addProv").onclick=()=>{if($(".prov[data-pid='']"))return;drawProvs([...SV.providers,{}])};
  let wins=JSON.parse(JSON.stringify(s.windows||[]));
  const drawWins=()=>{$("#wins").innerHTML=wins.map(winRow).join("")||`<div class="muted">No windows. Downloads run any time.</div>`;
    $$("[data-rm]").forEach(b=>b.onclick=()=>{readWins();wins.splice(+b.dataset.rm,1);drawWins()})};
  const readWins=()=>{wins=$$("#wins .win").map(r=>({days:$$(".days input:checked",r).map(c=>c.value),start:$(".ws",r).value||"00:00",end:$(".we",r).value||"23:59",concurrency:+$(".wc",r).value||1}))};
  drawWins();$("#addWin").onclick=()=>{readWins();wins.push({days:[...DL],start:"01:00",end:"07:00",concurrency:0});drawWins()};
  const fillArr=(a,r)=>{$(`#dl_${a}`).innerHTML=r.roots.map(p=>`<option value="${esc(p)}">`).join("");
    const root=$(`#s_${a}_root`);if(!root.value&&r.roots.length)root.placeholder=r.roots[0]+" (first root folder)";
    const sel=$(`#s_${a}_profile_id`),cur=sel.value!="0"?sel.value:String(SV.settings[a+"_profile_id"]||0);
    sel.innerHTML=`<option value="0">First profile${r.profiles.length?" ("+esc(r.profiles[0].name)+")":""}</option>`+r.profiles.map(p=>`<option value="${p.id}">${esc(p.name)}</option>`).join("");sel.value=cur;if(sel.value!==cur)sel.value="0"};
  for(const a of ["sonarr","radarr"])if(s[a+"_url"]&&SV.secrets_set[a+"_api_key"])api("arrmeta/"+a).then(r=>fillArr(a,r)).catch(()=>{});
  $$("[data-load]").forEach(b=>b.onclick=async()=>{const a=b.dataset.load,L=a=="sonarr"?"Sonarr":"Radarr";out(a,"Loading…");
    try{await save(true);const r=await api("arrmeta/"+a);fillArr(a,r);$("#s_"+a+"_url").value=r.url;
      out(a,`Loaded ${r.profiles.length} quality profile${r.profiles.length==1?"":"s"} and ${r.roots.length} root folder${r.roots.length==1?"":"s"} from ${L}. `+
        (r.indexer.installed?`VODgrab is set up in ${L} with indexer priority ${r.indexer.priority}.`:`VODgrab is not set up in ${L} yet.`))}catch(e){out(a,e.message)}});
  const out=(id,t)=>{const o=$("#o_"+id);o.style.display="block";o.textContent=t};
  $$("[data-test]").forEach(b=>b.onclick=async()=>{const k=b.dataset.test;out(k,"Testing…");try{await save(true);const r=await api("test/"+k,{});out(k,JSON.stringify(r,null,2))}catch(e){out(k,e.message)}});
  $$("[data-setup]").forEach(b=>b.onclick=async()=>{const k=b.dataset.setup;out(k,"Setting up…");try{await save(true);const r=await api("setup/"+k,{external_url:location.origin});out(k,r.message)}catch(e){out(k,e.message)}});
  $("#syncBtn").onclick=async()=>{const r=await api("sync",{});toast(r.ok?"Sync started":r.message);setTimeout(()=>{refreshStatus();runs()},1500)};
  $("#libBtn").onclick=async()=>{await api("library",{});toast("Library status is refreshing")};
  $("#newKey").onclick=async()=>{if(!confirm("Replace the API key? Sonarr and Radarr need setup again afterwards."))return;await api("apikey",{});renderSettings()};
  $("#repBtn").onclick=async e=>{const b=e.target;b.disabled=true;b.textContent="Building report…";
    try{const r=await fetch("/api/report");if(!r.ok)throw new Error((await r.json()).error||r.statusText);const blob=await r.blob();
      const a=document.createElement("a");a.href=URL.createObjectURL(blob);a.download="vodgrab-catalog-report.json";document.body.appendChild(a);a.click();a.remove();toast("Report downloaded")}
    catch(x){toast(x.message,1)}b.disabled=false;b.textContent="Download catalog report"};
  const dpDraw=r=>{const box=$("#dpBox");
    const sp=r.self_provider,n=r.total_streams;
    const rows=r.logins.map(l=>`<label class="f" style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:0 0 8px 26px">
        <input type="checkbox" data-dpk="${esc(l.key)}" ${l.provider?(l.enabled?"checked":""):(l.has_password&&!l.added?"checked":"")}>
        <span style="min-width:240px"><b>${esc(l.name)}</b><br><span class="muted">${esc(l.username||"?")} @ ${esc(l.server_url)}${l.max_streams?` · ${l.max_streams} stream${l.max_streams>1?"s":""}`:""}${l.active?"":" · disabled in Dispatcharr"}</span></span>
        ${l.provider?`<span class="pill ok">${l.enabled?"Imported":"Imported, turned off"}</span>`:l.added?`<span class="pill">Already a provider (added by hand)</span>`:""}
        ${l.has_password?`<span class="muted">Password from Dispatcharr</span>`:`<input type="password" data-dpp="${esc(l.key)}" placeholder="${l.provider?"Saved, leave blank to keep":"Password"}" autocomplete="new-password" style="max-width:200px">`}</label>`).join("");
    box.innerHTML=`<div class="sub" style="margin:6px 0 10px">Connected as <b>${esc(r.user)}</b>. Dispatcharr has ${r.logins.length} Xtream login${r.logins.length==1?"":"s"}${n?` with ${n} streams in total`:""}.</div>
      <div class="f chk"><input type="checkbox" id="dpProxy" ${r.proxy?"checked":""} ${r.xc_ready||r.proxy?"":"disabled"}><label for="dpProxy" style="margin:0;color:var(--text)">Download through Dispatcharr</label></div>
      <div class="hint" style="margin:0 0 12px 26px">${r.xc_ready?`VODgrab adds Dispatcharr as one provider and downloads through it, so Dispatcharr spreads downloads over your logins and counts them together with live TV.${sp?` VODgrab uses up to ${sp.max_conn} stream${sp.max_conn>1?"s":""} at once; change that on the Dispatcharr provider above.`:""}`:"Your Dispatcharr user needs an XC password first (in Dispatcharr: Users, edit your user). Then press Connect again."}</div>
      <div class="f chk"><input type="checkbox" id="dpImp" ${r.import?"checked":""}><label for="dpImp" style="margin:0;color:var(--text)">Import Dispatcharr's Xtream logins as providers</label></div>
      <div class="hint" style="margin:0 0 8px 26px">Each login (every profile counts as its own login) becomes a provider that VODgrab uses directly. Passwords come from Dispatcharr when it has them, and are kept up to date before each sync.</div>
      <div id="dpRows" ${r.import?"":"hidden"}>${rows||`<div class="muted" style="margin-left:26px">No Xtream logins in Dispatcharr.</div>`}</div>
      ${r.proxy&&r.import?`<div class="hint" style="margin:6px 0 0;color:var(--acc)">Both are on, so each title is offered twice. The provider order decides which is tried first.</div>`:""}
      <div class="tools" style="margin-top:10px"><button class="btn s p" id="dpApply">Apply</button></div>`;
    $("#dpImp").onchange=e=>{$("#dpRows").hidden=!e.target.checked};
    $("#dpApply").onclick=async()=>{const b=$("#dpApply");b.disabled=true;
      const logins=$$("[data-dpk]",box).filter(i=>i.checked).map(i=>{const pw=$(`[data-dpp="${CSS.escape(i.dataset.dpk)}"]`,box);return {key:i.dataset.dpk,password:pw?pw.value:""}});
      try{const x=await api("dispatcharr/mode",{proxy:$("#dpProxy").checked,import:$("#dpImp").checked,logins});drawProvs(x.providers);dpDraw(x);refreshStatus();toast("Dispatcharr settings applied. Run a sync to load new providers' catalogs.")}
      catch(e){toast(e.message,1)}b.disabled=false}};
  $("#dpConnect").onclick=async()=>{const b=$("#dpConnect");b.disabled=true;$("#dpBox").innerHTML=`<div class="muted">Connecting…</div>`;
    try{dpDraw(await api("dispatcharr",{url:$("#s_dispatcharr_url").value,api_key:$("#s_dispatcharr_api_key").value}))}catch(e){$("#dpBox").innerHTML=`<div class="muted" style="color:var(--bad)">${esc(e.message)}</div>`}b.disabled=false};
  if(SV.settings.dispatcharr_url&&SV.secrets_set.dispatcharr_api_key)api("dispatcharr").then(dpDraw).catch(e=>{$("#dpBox").innerHTML=`<div class="muted" style="color:var(--bad)">${esc(e.message)}</div>`});
  let updLatest="";
  const updShow=u=>{const box=$("#updBox");if(!box)return;updLatest=u.latest;
    let h=`You have VODgrab <b>${esc(u.current)}</b>`;
    if(u.error&&!u.latest)h+=` · could not check for updates: ${esc(u.error)}`;
    else if(u.available)h+=` · <b style="color:var(--acc)">${esc(u.latest)} is available</b>`;
    else if(u.latest)h+=" · this is the latest version";
    if(u.checked)h+=` <span style="opacity:.7">(checked ${ago(u.checked)})</span>`;
    if(u.available&&u.notes)h+=`<pre class="out" style="display:block;white-space:pre-wrap;max-height:260px;overflow:auto;margin-top:8px">${esc(u.notes)}</pre>`;
    if(u.available&&u.docker)h+=`<div style="margin-top:8px">VODgrab runs in Docker. To update, run this where your docker-compose.yml is:<pre class="out" style="display:block;margin-top:6px">docker compose pull && docker compose up -d</pre></div>`;
    else if(u.available&&!u.can_update)h+=`<div style="margin-top:8px">VODgrab cannot replace its own file at ${esc(u.path)}. Update by hand:<pre class="out" style="display:block;white-space:pre-wrap;margin-top:6px">${esc(u.manual)}</pre></div>`;
    box.innerHTML=h;$("#updApply").hidden=!(u.available&&u.can_update)};
  api("update").then(u=>{if(!u.checked)return api("update",{});return u}).then(updShow).catch(()=>{});
  $("#updCheck").onclick=async()=>{$("#updCheck").disabled=true;try{updShow(await api("update",{}))}catch(e){toast(e.message,1)}$("#updCheck").disabled=false};
  $("#updApply").onclick=async()=>{const n=status&&status.counts?status.counts.active:0;
    if(!confirm(`Update VODgrab to ${updLatest} and restart?`+(n?`\n\n${n} active download${n>1?"s":""} will pause for a moment and then continue where they left off.`:"")))return;
    $("#updApply").disabled=true;
    try{const r=await api("update/apply",{});toast(`Installing ${r.version}, restarting…`);
      const t0=Date.now();await new Promise(ok=>setTimeout(ok,3000));
      for(;;){try{const s=await api("status");if(s.version==r.version){location.reload();return}}catch(e){}
        if(Date.now()-t0>90000){toast("VODgrab has not come back yet. Check the service log.",1);break}
        await new Promise(ok=>setTimeout(ok,2000))}}
    catch(e){toast(e.message,1)}$("#updApply").disabled=false};
  $("#logBtn").onclick=async()=>{const l=await api("logs");out("log",l.slice().reverse().join("\n")||"Empty")};
function runSum(x){return x.added_movies==null?`${x.movies} movies, ${x.series} series, ${x.removed} removed`:`${x.movies} movies (+${x.added_movies} new, ${x.removed_movies} removed), ${x.series} series (+${x.added_series} new, ${x.removed_series} removed)`}
  const runs=async()=>{const r=await api("syncruns");const lc=r.find(x=>x.changes);$("#lastChg").hidden=!lc;if(lc)$("#lastChg").href=`/api/syncruns/${lc.id}/changes`;$("#runs").innerHTML=r.length?`<div class="list" style="margin-top:10px">${r.slice(0,6).map(x=>`<div class="sub">${new Date(x.started*1000).toLocaleString()} · ${esc(x.trigger)} · ${x.finished==null?"running":x.ok?runSum(x):"failed: "+esc(x.error)}${x.changes?` · <a href="/api/syncruns/${x.id}/changes" download>Changes</a>`:""}</div>${x.ok&&x.detail?JSON.parse(x.detail).map(d=>`<div class="sub" style="padding-left:14px">${esc(d.provider)}: ${runSum(d)}</div>`).join(""):""}`).join("")}</div>`:""};runs();
  async function save(quiet){readWins();const body={};
    for(const el of $$("#main [id^=s_]")){const k=el.id.slice(2);if(el.type=="checkbox")body[k]=el.checked;else if(el.tagName=="TEXTAREA")body[k]=el.value.split("\n").map(x=>x.trim()).filter(Boolean);else body[k]=el.value}
    body.retry_backoff=String(body.retry_backoff).split(",").map(x=>x.trim()).filter(Boolean);body.windows=wins;
    try{SV=await api("settings",body);if(!quiet){toast("Saved");renderSettings()}refreshStatus()}catch(e){toast(e.message,1);throw e}}
  $("#save").onclick=()=>save(false);
  $("#mTest").onclick=async()=>{out("meta","Testing…");try{await save(true);const r=await api("meta/test",{});out("meta",Object.entries(r).map(([k,v])=>k+": "+v).join("\n"));drawMeta()}catch(e){out("meta",e.message)}};
  $("#mFill").onclick=async()=>{try{await save(true);await api("meta/fill",{});toast("Filling in details");setTimeout(drawMeta,1500)}catch(e){toast(e.message,1)}};
  $("#bkNow").onclick=async()=>{try{await save(true);await api("backup/now",{});toast("Backing up");setTimeout(drawMeta,2500)}catch(e){toast(e.message,1)}};
  $("#bkCfg").onclick=()=>{location.href="/api/backup/download?what=config"};
  $("#bkDlNow").onclick=async e=>{const b=e.target;b.disabled=true;b.textContent="Building backup…";
    try{const r=await api("backup/now",{wait:true,extra:true,parts:["config","meta","catalog"]});location.href="/api/backup/download?name="+encodeURIComponent(r.name);drawMeta()}catch(x){toast(x.message,1)}
    b.disabled=false;b.textContent="Download a full backup"};
  $("#bkUp").onchange=async e=>{const f=e.target.files[0];if(!f)return;out("bk",`Uploading ${f.name} (${fb(f.size)})…`);
    try{const r=await fetch("/api/backup/upload?filename="+encodeURIComponent(f.name),{method:"POST",headers:{"Content-Type":"application/octet-stream"},body:f});
      const j=await r.json();if(!r.ok)throw new Error(j.error||r.statusText);$("#o_bk").style.display="none";restorePick(j.parts,{token:j.token},j.name)}
    catch(x){out("bk",x.message)}e.target.value=""};
  drawMeta();timer=setInterval(()=>{if(tab=="settings")drawMeta()},4000)}
async function drawMeta(){let r;try{r=await api("meta")}catch(e){return}const box=$("#mstat");if(!box)return;const c=r.counts;
  const pct=c.with_id?Math.round(c.have*100/c.with_id):0;
  let head;
  if(!r.key)head=`<div class="big">No TMDB key yet</div><div class="muted">Add one below to fill in details.</div>`;
  else head=`<div class="big">${c.have.toLocaleString()} of ${c.with_id.toLocaleString()} titles have details (${pct}%)</div><div class="prog"><i style="width:${pct}%"></i></div>`;
  const st=r.error?`<span style="color:var(--bad)">${esc(r.error)}</span>`:r.running?`${esc(r.phase||"Working")}${r.todo?` · ${r.done.toLocaleString()} of ${r.todo.toLocaleString()} this round`:""}`:r.key&&!r.auto?"Background fill is off. Fill now runs one full pass.":r.key?(c.missing+c.stale+c.unmatched?"Waiting for the next round":"Up to date"):"";
  box.innerHTML=head+`<div style="margin:6px 0 8px">${st}${r.eta>60&&r.key&&(r.running||r.auto)?` · about ${fd(r.eta)} left`:""}</div>
    <div class="grid2"><div>${c.titles.toLocaleString()} titles in the catalog</div><div>${c.no_id.toLocaleString()} without a TMDB ID</div>
    <div>${c.matched.toLocaleString()} found by name</div><div>${(c.missing+c.stale).toLocaleString()} waiting for details</div>
    <div>Last change check ${r.changes_at?ago(r.changes_at):"not yet"}</div><div>Stored: catalog ${fb(r.sizes.catalog)}, metadata ${fb(r.sizes.meta)}</div></div>`;
  const bl=$("#bklist");if(!bl)return;
  const info=`<div class="hint muted" style="margin-bottom:6px">In ${esc(r.backup_dir)}${r.backup_running?" · backing up now…":""}${r.backup_msg?" · "+esc(r.backup_msg):""}${r.backup_next?" · next "+when(r.backup_next):" · schedule off"}</div>`;
  bl.innerHTML=info+(r.backups.length?`<div class="list">${r.backups.map(b=>`<div class="row" style="padding:7px 10px"><div class="main"><div class="title" style="font-weight:500;font-size:13px">${new Date(b.at*1000).toLocaleString()} <span class="muted">· ${fb(b.size)}</span></div><div class="sub">${b.parts.map(p=>BKP[p]||p).join(", ")}</div></div>
    <div class="acts"><a class="btn s" href="/api/backup/download?name=${encodeURIComponent(b.name)}">Download</a><button class="btn s" data-rs="${esc(b.name)}">Restore…</button><button class="btn s d" data-bd="${esc(b.name)}">Delete</button></div></div>`).join("")}</div>`:`<div class="muted">No backups yet</div>`);
  const byName=Object.fromEntries(r.backups.map(b=>[b.name,b]));
  $$("[data-rs]",bl).forEach(b=>b.onclick=()=>restorePick(byName[b.dataset.rs].parts,{name:b.dataset.rs},b.dataset.rs));
  $$("[data-bd]",bl).forEach(b=>b.onclick=async()=>{if(!confirm("Delete this backup?"))return;try{await api("backup/delete",{name:b.dataset.bd});drawMeta()}catch(e){toast(e.message,1)}})}
const BKP={config:"Settings and providers",meta:"Metadata",catalog:"Catalog and download history"};
function restorePick(parts,src,name){const box=$("#bkRestore");if(!box)return;
  box.innerHTML=`<div class="prov" style="margin-top:10px"><div class="ph"><b>Restore from ${esc(name)}</b></div>
    ${parts.map(p=>`<label style="display:flex;gap:8px;align-items:center;margin:4px 0"><input type="checkbox" value="${p}" checked> ${BKP[p]||p}</label>`).join("")}
    <div class="tools" style="margin:10px 0 0"><button class="btn p s" id="rsGo">Restore selected</button><button class="btn s" id="rsNo">Cancel</button></div></div>`;
  box.scrollIntoView({behavior:"smooth",block:"nearest"});
  $("#rsNo").onclick=()=>box.innerHTML="";
  $("#rsGo").onclick=async()=>{const sel=$$("input:checked",box).map(i=>i.value);if(!sel.length)return;
    if(!confirm(`Replace your current ${sel.map(p=>BKP[p].toLowerCase()).join(", ")} with the backup?`))return;
    try{const r=await api("backup/restore",{...src,parts:sel});box.innerHTML="";
      if(r.restart){toast("Restoring. VODgrab is restarting…");const t0=Date.now();const wait=async()=>{try{await api("status");if(Date.now()-t0>4000){location.reload();return}}catch(e){}setTimeout(wait,1500)};setTimeout(wait,3000)}
      else{toast("Settings and providers restored");renderSettings()}}catch(e){toast(e.message,1)}}}

refreshStatus().then(()=>route());
</script>
</body>
</html>
"""

if __name__ == "__main__":
    main()
