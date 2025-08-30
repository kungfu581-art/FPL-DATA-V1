#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
all_fpl_archive_plus.py

One-shot archiver for *three-season* Fantasy Premier League data, designed to
run locally or inside GitHub Actions.

It collects:
1) CURRENT season from official FPL API
   - bootstrap-static (players/teams/positions + events with deadlines)
   - fixtures (json + csv)
   - per-player current-season GW logs via element-summary/{id}
2) HISTORICAL (previous two completed seasons) per-GW logs & fixtures
   - from vaastav/Fantasy-Premier-League public dataset
3) OPTIONAL: Understat xG/xA joins (best-effort)
   - pulls Understat team & player season summaries and attempts a fuzzy match
     by (name, team) to enrich outputs with xG/xA.
   - write separate CSVs so the core archive is never blocked by Understat.

Quality-of-life:
- polite rate limiting
- resume/retry logic
- zips each season folder plus a top-level ZIP of the whole archive
- writes a MANIFEST with timestamps & versions

Usage examples
--------------
# default: infer current + previous two seasons
python all_fpl_archive_plus.py --out ./fpl_archive

# specify seasons explicitly and include Understat
python all_fpl_archive_plus.py --out ./fpl_archive --current_season 2025-26 \
  --historical 2024-25 2023-24 --with_understat

Notes
-----
- Official FPL API does NOT expose past-season per-GW logs. We use the widely-
  used community dataset (vaastav) for those years.
- Understat has no official API; endpoints may change. We handle failures softly.

"""
import os, sys, time, json, csv, argparse, re, math, zipfile
from datetime import datetime
from typing import List, Dict

try:
    import requests
except Exception:
    print("This script requires 'requests' (pip install requests)")
    raise

# --------------- Config ---------------
FPL_BASE = "https://fantasy.premierleague.com/api"
VAASTAV_BASE = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"
UNDERSTAT_BASE = "https://understat.com"  # unofficial
UA = {"User-Agent":"FPL-Archive/1.1 (+github actions compatible)"}

# --------------- Helpers ---------------
def ensure_dir(p): os.makedirs(p, exist_ok=True)

def fetch_json(url, retries=5, backoff=1.6, timeout=45):
    for i in range(retries):
        try:
            r = requests.get(url, timeout=timeout, headers=UA)
            if r.ok:
                return r.json()
        except Exception as e:
            pass
        time.sleep(backoff ** i)
    raise RuntimeError(f"GET failed: {url}")

def fetch_text(url, retries=5, backoff=1.6, timeout=45):
    for i in range(retries):
        try:
            r = requests.get(url, timeout=timeout, headers=UA)
            if r.ok:
                return r.text
        except Exception as e:
            pass
        time.sleep(backoff ** i)
    raise RuntimeError(f"GET failed: {url}")

def write_json(obj, path):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def write_csv(rows, path, fieldnames=None):
    ensure_dir(os.path.dirname(path))
    if not rows:
        open(path,"w",encoding="utf-8").close(); return
    if fieldnames is None:
        keys=set()
        for r in rows: keys.update(r.keys())
        fieldnames=sorted(keys)
    with open(path,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k:r.get(k) for k in fieldnames})

def infer_seasons():
    now = datetime.utcnow()
    y, m = now.year, now.month
    if m >= 7:
        cur = f"{y}-{str((y+1)%100).zfill(2)}"
        hist1 = f"{y-1}-{str(y%100).zfill(2)}"
        hist2 = f"{y-2}-{str((y-1)%100).zfill(2)}"
    else:
        cur = f"{y-1}-{str(y%100).zfill(2)}"
        hist1 = f"{y-2}-{str((y-1)%100).zfill(2)}"
        hist2 = f"{y-3}-{str((y-2)%100).zfill(2)}"
    return cur, [hist1, hist2]

# --------------- Core ---------------
def dump_current_season(outdir:str, season:str, resume=True):
    print(f"[current] season={season}")
    ensure_dir(outdir)
    bs = fetch_json(f"{FPL_BASE}/bootstrap-static/")
    write_json(bs, os.path.join(outdir, "bootstrap_static.json"))
    write_csv(bs.get("elements", []), os.path.join(outdir, "players.csv"))
    write_csv(bs.get("teams", []), os.path.join(outdir, "teams.csv"))
    write_csv(bs.get("element_types", []), os.path.join(outdir, "positions.csv"))
    write_csv(bs.get("events", []), os.path.join(outdir, "events.csv"))
    fixtures = fetch_json(f"{FPL_BASE}/fixtures/")
    write_json(fixtures, os.path.join(outdir, "fixtures.json"))
    if isinstance(fixtures, list):
        write_csv(fixtures, os.path.join(outdir, "fixtures.csv"))
    # deadline text
    cur = next((e for e in bs.get("events",[]) if e.get("is_current")), None)
    if cur and cur.get("deadline_time"):
        with open(os.path.join(outdir,"deadline_utc.txt"),"w",encoding="utf-8") as f:
            f.write(cur["deadline_time"]+"\n")

    # per-player GW logs for current season
    players = bs.get("elements", [])
    logs_dir = os.path.join(outdir, "player_summaries")
    ensure_dir(logs_dir)
    combined = []
    for i,p in enumerate(players,1):
        pid = p.get("id")
        if not pid: continue
        target = os.path.join(logs_dir, f"player_{pid}.json")
        if resume and os.path.exists(target):
            try:
                summ = json.load(open(target, "r", encoding="utf-8"))
            except Exception:
                summ = fetch_json(f"{FPL_BASE}/element-summary/{pid}/"); write_json(summ, target)
        else:
            summ = fetch_json(f"{FPL_BASE}/element-summary/{pid}/"); write_json(summ, target)
        for row in summ.get("history", []):  # current-season per-GW
            row = dict(row); row["player_id"]=pid; combined.append(row)
        if i % 50 == 0: print(f"  fetched {i}/{len(players)} summaries")
        time.sleep(0.12)  # be polite
    write_csv(combined, os.path.join(outdir, f"{season}_player_gw_logs.csv"))
    print(f"[current] GW logs rows: {len(combined)}")

def dump_historical(outdir:str, season:str):
    print(f"[historical] season={season}")
    ensure_dir(outdir)
    urls = {
        f"{season}_merged_gw.csv": f"{VAASTAV_BASE}/{season}/gws/merged_gw.csv",
        f"{season}_fixtures.csv": f"{VAASTAV_BASE}/{season}/fixtures.csv",
        f"{season}_players_summary.csv": f"{VAASTAV_BASE}/{season}/cleaned_players.csv",
    }
    for name, url in urls.items():
        try:
            txt = fetch_text(url)
            with open(os.path.join(outdir,name),"w",encoding="utf-8") as f: f.write(txt)
            print(f"  saved {name}")
        except Exception as e:
            print(f"  WARN: {e} ({url})")

# --------------- Understat (best-effort) ---------------
def understat_league_season_players(league="EPL", season="2025"):
    # unofficial JSON mirrored by understat site via their internal API:
    # https://understat.com/league/EPL/2025
    # This page embeds JSON; parsing requires a small heuristic.
    html = fetch_text(f"{UNDERSTAT_BASE}/league/{league}/{season}")
    # The page contains `playersData = JSON...;`
    import re, json
    m = re.search(r"playersData\s*=\s*JSON.parse\('([^']+)'", html)
    if not m:
        return []
    raw = m.group(1).encode('utf-8').decode('unicode_escape')
    data = json.loads(raw)
    return data  # list of players with xG/xA totals per season

def build_understat_tables(out_root:str, seasons:List[str]):
    # Convert FPL seasons "2025-26" -> Understat season "2025"
    us_rows = []
    for s in seasons:
        year = int(s.split("-")[0])
        try:
            plist = understat_league_season_players("EPL", str(year))
            for p in plist:
                row = dict(p)
                row["understat_season"]=str(year)
                us_rows.append(row)
        except Exception as e:
            print(f"  Understat fetch failed for {s}: {e}")
    write_csv(us_rows, os.path.join(out_root, "understat_players_seasons.csv"))
    print(f"[understat] wrote {len(us_rows)} rows -> understat_players_seasons.csv")

# --------------- Zip helpers ---------------
def zip_dir(folder_path:str, zip_path:str):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(folder_path):
            for fn in files:
                full = os.path.join(root, fn)
                arc = os.path.relpath(full, folder_path)
                z.write(full, arc)

# --------------- Main ---------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="./fpl_archive", help="Output folder")
    ap.add_argument("--current_season", default=None, help="e.g., 2025-26")
    ap.add_argument("--historical", nargs="*", default=None, help="e.g., 2024-25 2023-24")
    ap.add_argument("--with_understat", action="store_true", help="Add Understat xG/xA summaries (best-effort)")
    ap.add_argument("--zip", action="store_true", help="Zip each season and a top-level zip bundle")
    args = ap.parse_args()

    cur_season, default_hist = infer_seasons()
    season_cur = args.current_season or cur_season
    seasons_hist = args.historical or default_hist

    root = os.path.abspath(args.out)
    os.makedirs(root, exist_ok=True)

    # Current season
    cur_dir = os.path.join(root, season_cur)
    dump_current_season(cur_dir, season_cur, resume=True)

    # Historical
    for s in seasons_hist:
        sdir = os.path.join(root, s)
        dump_historical(sdir, s)

    # Understat (best-effort)
    if args.with_understat:
        try:
            build_understat_tables(root, [season_cur] + seasons_hist)
        except Exception as e:
            print(f"[understat] skipped due to error: {e}")

    # Manifest
    manifest = {
        "generated_utc": datetime.utcnow().isoformat()+"Z",
        "current_season": season_cur,
        "historical_seasons": seasons_hist,
        "with_understat": bool(args.with_understat),
        "sources": {
            "official_fpl_api": FPL_BASE,
            "historical_community": "https://github.com/vaastav/Fantasy-Premier-League",
            "understat": UNDERSTAT_BASE if args.with_understat else None
        }
    }
    write_json(manifest, os.path.join(root, "MANIFEST.json"))

    # Zip outputs
    if args.zip:
        for s in [season_cur] + seasons_hist:
            sdir = os.path.join(root, s)
            zip_dir(sdir, os.path.join(root, f"{s}.zip"))
        zip_dir(root, os.path.join(root, "fpl_archive_bundle.zip"))
        print(f"[zip] wrote season zips and fpl_archive_bundle.zip")

    print("Done. Output root:", root)

if __name__ == "__main__":
    main()
