#!/usr/bin/env python3
"""
Flashscore.se scraper for detailed Olympic match/event results.
Fetches results from Flashscore feed API for team sports (scores, periods)
and individual sports (positions, times).
Writes to data/flashscore_matches.json. Only updates if data has changed.
"""

import json
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

OUTPUT_FILE = "data/flashscore_matches.json"

FLASHSCORE_BASE = "https://www.flashscore.se/x/feed/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.flashscore.se/",
    "X-Fsign": "SW9D1eZo",
}

DELAY_BETWEEN_FEEDS = 2.0  # seconds

# ── Feed catalog ─────────────────────────────────────────────
# From docs/flashscore_investigation.md section 6
# type: "team" for match-based sports, "individual" for ranked results

FEEDS = [
    # ── Team sports ──
    {"feed": "t_4_8_C06aJvIB_1_sv_1", "sport": "Ishockey", "event": "Herrar", "type": "team"},
    # Hockey women feed ID unknown — will be added when discovered
    # Curling feed IDs unknown — will be added when discovered

    # ── Individual sports ──
    {"feed": "t_39_8401_OfheouK0_1_sv_1", "sport": "Alpint", "event": "Störtlopp herrar", "type": "individual"},
    {"feed": "t_39_8402_IyKiEbl0_1_sv_1", "sport": "Alpint", "event": "Störtlopp damer", "type": "individual"},
    {"feed": "t_39_28273_I3qVGY4S_1_sv_1", "sport": "Alpint", "event": "Kombination herrar", "type": "individual"},
    {"feed": "t_39_28275_xGt40CSk_1_sv_1", "sport": "Alpint", "event": "Kombination damer", "type": "individual"},
    {"feed": "t_40_8462_tvqeUbWm_1_sv_1", "sport": "Längdskidor", "event": "Sprint klassisk herrar", "type": "individual"},
    {"feed": "t_40_8463_On67TIof_1_sv_1", "sport": "Längdskidor", "event": "Sprint klassisk damer", "type": "individual"},
    {"feed": "t_40_8466_v9kUjyhL_1_sv_1", "sport": "Längdskidor", "event": "Skiathlon herrar", "type": "individual"},
    {"feed": "t_40_8467_zuueie7R_1_sv_1", "sport": "Längdskidor", "event": "Skiathlon damer", "type": "individual"},
    {"feed": "t_41_8446_tUJuI4Ug_1_sv_1", "sport": "Skidskytte", "event": "Individuell herrar", "type": "individual"},
    {"feed": "t_41_8456_bRNPS2EC_1_sv_1", "sport": "Skidskytte", "event": "Mixedstafett", "type": "individual"},
    {"feed": "t_38_8416_nVMa9LU8_1_sv_1", "sport": "Backhoppning", "event": "Normalbacke herrar", "type": "individual"},
    {"feed": "t_38_8417_ljWf8gPM_1_sv_1", "sport": "Backhoppning", "event": "Normalbacke damer", "type": "individual"},
    {"feed": "t_38_13911_WELa7DvT_1_sv_1", "sport": "Backhoppning", "event": "Normalbacke mixed", "type": "individual"},
    {"feed": "t_38_8418_thM38upF_1_sv_1", "sport": "Backhoppning", "event": "Storbacke herrar", "type": "individual"},
    {"feed": "t_38_8544_KrL77aaL_1_sv_1", "sport": "Backhoppning", "event": "Storbacke lag", "type": "individual"},
]

# Swedish country names used by flashscore.se
SWE_COUNTRIES = {"Sverige", "sweden"}


def fetch_feed(feed_url: str) -> str:
    """Fetch a Flashscore feed. Returns raw text or empty string on error."""
    url = FLASHSCORE_BASE + feed_url
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read().decode("utf-8", errors="replace")
            print(f"  GET {feed_url} -> {resp.getcode()} ({len(data)} bytes)")
            return data
    except urllib.error.HTTPError as e:
        print(f"  FAIL {feed_url}: HTTP {e.code}")
        return ""
    except Exception as e:
        print(f"  FAIL {feed_url}: {e}")
        return ""


def parse_fields(record: str) -> tuple[dict, list, list]:
    """Parse a single feed record into dict + RAA/RAB lists."""
    d: dict[str, str] = {}
    raa: list[str] = []
    rab: list[str] = []
    for field in record.split("\u00ac"):
        if "\u00f7" not in field:
            continue
        key, val = field.split("\u00f7", 1)
        if key == "RAA":
            raa.append(val)
        elif key == "RAB":
            rab.append(val)
        else:
            d[key] = val
    return d, raa, rab


def parse_team_feed(data: str, entry: dict) -> list[dict]:
    """Parse a team sport feed (hockey, curling) into match dicts."""
    records = data.split("~")
    matches: list[dict] = []
    i = 2  # Skip header records

    while i < len(records):
        d, _, _ = parse_fields(records[i])
        if "AA" not in d:
            i += 1
            continue

        # This record is a match with home team info
        status_code = d.get("AB", "")
        status = "finished" if status_code == "3" else ("live" if status_code == "2" else "scheduled")

        match = {
            "type": "team",
            "sport": entry["sport"],
            "event": entry["event"],
            "event_id": d.get("AA", ""),
            "home": d.get("CX", d.get("AE", "")),
            "away": "",
            "home_score": _int(d.get("AG", "")),
            "away_score": _int(d.get("AH", "")),
            "status": status,
            "timestamp": _int(d.get("AD", "")),
        }

        # Build periods string from BA-BF fields
        periods = []
        for h_key, a_key in [("BA", "BB"), ("BC", "BD"), ("BE", "BF")]:
            h = d.get(h_key, "")
            a = d.get(a_key, "")
            if h or a:
                periods.append(f"{h or '0'}-{a or '0'}")
        match["periods"] = ", ".join(periods) if periods else ""

        # Next record should be away team
        i += 1
        if i < len(records):
            d2, _, _ = parse_fields(records[i])
            match["away"] = d2.get("CX", d2.get("AE", ""))

        if status == "finished":
            matches.append(match)

        i += 1

    return matches


def parse_individual_feed(data: str, entry: dict) -> dict | None:
    """Parse an individual sport feed. Returns entry with Swedish + podium results."""
    records = data.split("~")
    athletes: list[dict] = []
    has_finished = False

    for rec in records[2:]:
        d, raa, rab = parse_fields(rec)
        if "AE" not in d:
            continue
        if d.get("AB") == "3":
            has_finished = True

        ra = dict(zip(raa, rab))
        pos = ra.get("7", "")
        athletes.append({
            "pos": _int(pos) if pos else 999,
            "name": d.get("AE", ""),
            "country": d.get("FU", ""),
            "time": ra.get("5", ""),
            "diff": ra.get("6", ""),
        })

    if not has_finished or not athletes:
        return None

    athletes.sort(key=lambda x: x["pos"])

    # Keep Swedish athletes + top 3
    swe = [a for a in athletes if a["country"] in SWE_COUNTRIES]
    podium = [a for a in athletes if a["pos"] <= 3]
    seen = set()
    filtered = []
    for a in podium + swe:
        key = a["name"] + a["country"]
        if key not in seen:
            seen.add(key)
            filtered.append(a)
    filtered.sort(key=lambda x: x["pos"])

    if not filtered:
        return None

    return {
        "type": "individual",
        "sport": entry["sport"],
        "event": entry["event"],
        "status": "finished",
        "results": filtered,
    }


def _int(s: str) -> int | None:
    """Safe int parse, returns None for empty/invalid."""
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


def scrape_all() -> list[dict]:
    """Scrape all feeds with rate limiting."""
    all_matches: list[dict] = []

    for entry in FEEDS:
        data = fetch_feed(entry["feed"])
        if not data:
            # Retry once
            time.sleep(5)
            data = fetch_feed(entry["feed"])
        if not data:
            print(f"  SKIP {entry['sport']} {entry['event']}: no data")
            time.sleep(DELAY_BETWEEN_FEEDS)
            continue

        if entry["type"] == "team":
            matches = parse_team_feed(data, entry)
            all_matches.extend(matches)
            print(f"  {entry['sport']} {entry['event']}: {len(matches)} finished matches")
        else:
            result = parse_individual_feed(data, entry)
            if result:
                all_matches.append(result)
                n_swe = sum(1 for r in result["results"] if r["country"] in SWE_COUNTRIES)
                print(f"  {entry['sport']} {entry['event']}: {len(result['results'])} results ({n_swe} SWE)")
            else:
                print(f"  {entry['sport']} {entry['event']}: no finished results")

        time.sleep(DELAY_BETWEEN_FEEDS)

    return all_matches


def write_if_changed(matches: list[dict]) -> bool:
    """Write JSON only if data changed. Never overwrite with empty."""
    output = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "matches": matches,
    }

    try:
        with open(OUTPUT_FILE) as f:
            existing = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing = {}

    if matches == existing.get("matches", []):
        print("  No changes")
        return False

    if not matches and existing.get("matches"):
        print("  Scraper returned 0 matches, keeping existing data")
        return False

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"  Wrote {len(matches)} entries to {OUTPUT_FILE}")
    return True


def main():
    print("Scraping Flashscore Winter Olympics 2026...")
    matches = scrape_all()
    print(f"Total: {len(matches)} entries")
    changed = write_if_changed(matches)
    if changed:
        print("Data changed!")
    else:
        print("No changes.")


if __name__ == "__main__":
    main()
