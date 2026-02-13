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
FLASHSCORE_SITE = "https://www.flashscore.se"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.flashscore.se/",
    "X-Fsign": "SW9D1eZo",
}
# Minimum timestamp for OS 2026 matches (2026-02-01 00:00 UTC)
MIN_TIMESTAMP = 1769904000

DELAY_BETWEEN_FEEDS = 2.0  # seconds

# ── Feed catalog ─────────────────────────────────────────────
# From docs/flashscore_investigation.md section 6
# type: "team" for match-based sports, "individual" for ranked results

# Team sports: scraped from HTML results page (feed API only shows today's matches)
TEAM_FEEDS = [
    {"url": "/ishockey/varld/olympiska-spelen/resultat/", "sport": "Ishockey", "event": "Herrar"},
    {"url": "/ishockey/varld/olympiska-spelen-damer/resultat/", "sport": "Ishockey", "event": "Damer"},
]

FEEDS = [
    # ── Alpint ──
    {"feed": "t_39_8401_OfheouK0_1_sv_1", "sport": "Alpint", "event": "Störtlopp herrar", "type": "individual"},
    {"feed": "t_39_8402_IyKiEbl0_1_sv_1", "sport": "Alpint", "event": "Störtlopp damer", "type": "individual"},
    {"feed": "t_39_8403_pAt4qJlD_1_sv_1", "sport": "Alpint", "event": "Super-G herrar", "type": "individual"},
    {"feed": "t_39_8404_M5UQDvZg_1_sv_1", "sport": "Alpint", "event": "Super-G damer", "type": "individual"},
    {"feed": "t_39_8405_KjH282RD_1_sv_1", "sport": "Alpint", "event": "Storslalom herrar", "type": "individual"},
    {"feed": "t_39_8406_IRdw204s_1_sv_1", "sport": "Alpint", "event": "Storslalom damer", "type": "individual"},
    {"feed": "t_39_8407_bsG67MtK_1_sv_1", "sport": "Alpint", "event": "Slalom herrar", "type": "individual"},
    {"feed": "t_39_8408_vc7EEKJm_1_sv_1", "sport": "Alpint", "event": "Slalom damer", "type": "individual"},
    {"feed": "t_39_28273_I3qVGY4S_1_sv_1", "sport": "Alpint", "event": "Lagkombination herrar", "type": "individual"},
    {"feed": "t_39_28275_xGt40CSk_1_sv_1", "sport": "Alpint", "event": "Lagkombination damer", "type": "individual"},

    # ── Längdskidor ──
    {"feed": "t_40_8462_tvqeUbWm_1_sv_1", "sport": "Längdskidor", "event": "Sprint klassisk herrar", "type": "individual"},
    {"feed": "t_40_8463_On67TIof_1_sv_1", "sport": "Längdskidor", "event": "Sprint klassisk damer", "type": "individual"},
    {"feed": "t_40_8535_2HOS5urr_1_sv_1", "sport": "Längdskidor", "event": "Sprint fristil herrar", "type": "individual"},
    {"feed": "t_40_8536_WUyu9LZQ_1_sv_1", "sport": "Längdskidor", "event": "Sprint fristil damer", "type": "individual"},
    {"feed": "t_40_8466_v9kUjyhL_1_sv_1", "sport": "Längdskidor", "event": "Skiathlon herrar", "type": "individual"},
    {"feed": "t_40_8467_zuueie7R_1_sv_1", "sport": "Längdskidor", "event": "Skiathlon damer", "type": "individual"},
    {"feed": "t_40_8527_8E020FFr_1_sv_1", "sport": "Längdskidor", "event": "Individuell fristil herrar", "type": "individual"},
    {"feed": "t_40_8528_2y06aZUl_1_sv_1", "sport": "Längdskidor", "event": "Individuell fristil damer", "type": "individual"},
    {"feed": "t_40_8460_Slyp4JCf_1_sv_1", "sport": "Längdskidor", "event": "Individuell klassisk herrar", "type": "individual"},
    {"feed": "t_40_8461_pQou5acl_1_sv_1", "sport": "Längdskidor", "event": "Individuell klassisk damer", "type": "individual"},
    {"feed": "t_40_8468_Gv4Abgpe_1_sv_1", "sport": "Längdskidor", "event": "Masstart klassisk herrar", "type": "individual"},
    {"feed": "t_40_8469_b3gFcDa1_1_sv_1", "sport": "Längdskidor", "event": "Masstart klassisk damer", "type": "individual"},
    {"feed": "t_40_8472_0Sc2elMN_1_sv_1", "sport": "Längdskidor", "event": "Stafett herrar", "type": "individual"},
    {"feed": "t_40_8473_KY9Le8yU_1_sv_1", "sport": "Längdskidor", "event": "Stafett damer", "type": "individual"},

    # ── Skidskytte ──
    {"feed": "t_41_8456_bRNPS2EC_1_sv_1", "sport": "Skidskytte", "event": "Mixedstafett", "type": "individual"},
    {"feed": "t_41_8446_tUJuI4Ug_1_sv_1", "sport": "Skidskytte", "event": "Individuell herrar", "type": "individual"},
    {"feed": "t_41_8447_bLdTIpFm_1_sv_1", "sport": "Skidskytte", "event": "Individuell damer", "type": "individual"},
    {"feed": "t_41_8448_AVOlmZF8_1_sv_1", "sport": "Skidskytte", "event": "Sprint herrar", "type": "individual"},
    {"feed": "t_41_8449_dSKplF02_1_sv_1", "sport": "Skidskytte", "event": "Sprint damer", "type": "individual"},
    {"feed": "t_41_8450_tIoMUQas_1_sv_1", "sport": "Skidskytte", "event": "Jaktstart herrar", "type": "individual"},
    {"feed": "t_41_8451_4hOhngVE_1_sv_1", "sport": "Skidskytte", "event": "Jaktstart damer", "type": "individual"},
    {"feed": "t_41_8452_G2YKTra6_1_sv_1", "sport": "Skidskytte", "event": "Masstart herrar", "type": "individual"},
    {"feed": "t_41_8453_2aUGUOq0_1_sv_1", "sport": "Skidskytte", "event": "Masstart damer", "type": "individual"},
    {"feed": "t_41_8454_8pZuh66J_1_sv_1", "sport": "Skidskytte", "event": "Stafett herrar", "type": "individual"},
    {"feed": "t_41_8455_xIMTRMTI_1_sv_1", "sport": "Skidskytte", "event": "Stafett damer", "type": "individual"},

    # ── Backhoppning ──
    {"feed": "t_38_8416_nVMa9LU8_1_sv_1", "sport": "Backhoppning", "event": "Normalbacke herrar", "type": "individual"},
    {"feed": "t_38_8417_ljWf8gPM_1_sv_1", "sport": "Backhoppning", "event": "Normalbacke damer", "type": "individual"},
    {"feed": "t_38_13911_WELa7DvT_1_sv_1", "sport": "Backhoppning", "event": "Normalbacke mixed", "type": "individual"},
    {"feed": "t_38_8418_thM38upF_1_sv_1", "sport": "Backhoppning", "event": "Storbacke herrar", "type": "individual"},
    {"feed": "t_38_8419_0th0i9wQ_1_sv_1", "sport": "Backhoppning", "event": "Storbacke damer", "type": "individual"},
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


def fetch_team_results_html(entry: dict) -> list[dict]:
    """Fetch all finished team matches from Flashscore HTML results page.

    The feed API only shows today's matches, but the HTML results page
    contains the full tournament history embedded in cjs.initialFeeds['results'].
    """
    import re
    page_url = FLASHSCORE_SITE + entry["url"]
    req = urllib.request.Request(page_url, headers={
        "User-Agent": HEADERS["User-Agent"],
        "Accept": "text/html",
    })
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="replace")
            print(f"  GET {entry['url']} -> {resp.getcode()} ({len(html)} bytes)")
    except Exception as e:
        print(f"  FAIL {entry['url']}: {e}")
        return []

    # Extract embedded results data
    m = re.search(r"cjs\.initialFeeds\['results'\]\s*=\s*\{\s*data:\s*`(.*?)`", html, re.DOTALL)
    if not m:
        print(f"  No results data found in HTML for {entry['sport']} {entry['event']}")
        return []

    data = m.group(1)
    records = data.split("~")
    matches: list[dict] = []

    for rec in records:
        fields = {}
        for field in rec.split("\u00ac"):
            if "\u00f7" not in field:
                continue
            k, v = field.split("\u00f7", 1)
            fields[k] = v

        if "AA" not in fields or fields.get("AB") != "3":
            continue

        timestamp = _int(fields.get("AD", ""))
        if not timestamp or timestamp < MIN_TIMESTAMP:
            continue

        home = _clean_team(fields.get("CX", fields.get("AE", "")))
        away = _clean_team(fields.get("AF", fields.get("FK", "")))

        periods = []
        for h_key, a_key in [("BA", "BB"), ("BC", "BD"), ("BE", "BF")]:
            h = fields.get(h_key, "")
            a = fields.get(a_key, "")
            if h or a:
                periods.append(f"{h or '0'}-{a or '0'}")

        matches.append({
            "type": "team",
            "sport": entry["sport"],
            "event": entry["event"],
            "event_id": fields.get("AA", ""),
            "home": home,
            "away": away,
            "home_score": _int(fields.get("AG", "")),
            "away_score": _int(fields.get("AH", "")),
            "status": "finished",
            "timestamp": timestamp,
            "periods": ", ".join(periods) if periods else "",
        })

    return matches


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


def _clean_team(name: str) -> str:
    """Strip gender suffix (e.g. 'Finland D' -> 'Finland')."""
    if name.endswith(" D"):
        return name[:-2]
    return name


def _fix_diacritics(s: str) -> str:
    """Restore common Nordic/European diacritics from ASCII transliterations.

    Flashscore URL slugs strip diacritics: 'hagstroem' → 'hagström'.
    """
    for ascii_form, char in [("oe", "ö"), ("ae", "ä"), ("ue", "ü")]:
        s = s.replace(ascii_form, char)
        s = s.replace(ascii_form.capitalize(), char.upper())
    return s


def _full_name(ae: str, wu: str) -> str:
    """Convert abbreviated name + URL slug to full name.

    AE='Klaebo J. H.', WU='klaebo-johannes-hoesflot' → 'Johannes Hösflot Kläbo'
    """
    if not wu or not ae:
        return _fix_diacritics(ae) if ae else ae
    parts = wu.split("-")
    if len(parts) < 2:
        return _fix_diacritics(ae)
    # Count initials in AE (words like "J.", "H.", "E.")
    initials = sum(1 for w in ae.split() if len(w) <= 3 and w.endswith("."))
    if initials == 0:
        return _fix_diacritics(ae)
    n_given = min(initials, len(parts) - 1)
    surname_parts = parts[: len(parts) - n_given]
    given_parts = parts[len(parts) - n_given :]
    surname = " ".join(_fix_diacritics(p).title() for p in surname_parts)
    given = " ".join(_fix_diacritics(p).title() for p in given_parts)
    return f"{given} {surname}" if given else surname


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

        home = _clean_team(d.get("CX", d.get("AE", "")))
        # Away team may be in same record (AF/FK fields) or in next record
        away = _clean_team(d.get("AF", d.get("FK", "")))

        match = {
            "type": "team",
            "sport": entry["sport"],
            "event": entry["event"],
            "event_id": d.get("AA", ""),
            "home": home,
            "away": away,
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

        # If away team not found in same record, check next record
        if not match["away"]:
            i += 1
            if i < len(records):
                d2, _, _ = parse_fields(records[i])
                match["away"] = _clean_team(d2.get("CX", d2.get("AE", "")))

        if status == "finished":
            matches.append(match)

        i += 1

    return matches


def parse_individual_feed(data: str, entry: dict) -> dict | None:
    """Parse an individual sport feed. Returns entry with Swedish + podium results."""
    records = data.split("~")
    athletes: list[dict] = []
    has_finished = False
    event_timestamp = None
    current_round = None  # Track section via ZAE header field
    # For sprint events: collect best times from heat rounds to fill missing Totalt times
    heat_times: dict[str, str] = {}  # WU key -> time

    for rec in records[1:]:
        d, raa, rab = parse_fields(rec)

        # Header record with round name (e.g. "Totalt", "Kvartsfinal 1", "Final")
        if "ZAE" in d:
            current_round = d["ZAE"]
            continue

        if "AE" not in d:
            continue

        # Track event timestamp from AD field
        ad = _int(d.get("AD", ""))
        if ad and (event_timestamp is None or ad > event_timestamp):
            event_timestamp = ad

        ra = dict(zip(raa, rab))

        # Collect times from heat rounds (non-Totalt) for fallback
        if current_round is not None and current_round != "Totalt":
            wu = d.get("WU", d.get("AE", ""))
            if ra.get("5") and wu not in heat_times:
                heat_times[wu] = ra["5"]
            continue

        if d.get("AB") == "3":
            has_finished = True

        pos = ra.get("7", "")
        athlete = {
            "pos": _int(pos) if pos else 999,
            "name": _full_name(d.get("AE", ""), d.get("WU", "")),
            "country": d.get("FU", ""),
            "_wu": d.get("WU", d.get("AE", "")),
        }
        # Time-based sports (alpine, XC, biathlon)
        if ra.get("5"):
            athlete["time"] = ra["5"]
        if ra.get("6"):
            athlete["diff"] = ra["6"]
        # Distance-based sports (ski jumping)
        if ra.get("2"):
            athlete["dist"] = ra["2"]
        if ra.get("3"):
            athlete["dist_pts"] = ra["3"]
        if ra.get("4"):
            athlete["style_pts"] = ra["4"]
        if ra.get("12"):
            athlete["best_dist"] = ra["12"]
        if ra.get("13"):
            athlete["best_pts"] = ra["13"]
        # Biathlon penalties (field 9 for some events, field 10 for others)
        pen_str = ra.get("9") or ra.get("10")
        if pen_str:
            athlete["penalties"] = _int(pen_str)
        athletes.append(athlete)

    # Fill missing times from heat rounds (sprint events)
    if heat_times:
        for a in athletes:
            if "time" not in a and a.get("_wu") in heat_times:
                a["time"] = heat_times[a["_wu"]]

    if not has_finished or not athletes:
        return None

    # Reject old data (e.g. Beijing 2022 results for events not yet competed in 2026)
    if event_timestamp and event_timestamp < MIN_TIMESTAMP:
        print(f"  SKIP {entry['sport']} {entry['event']}: old data "
              f"(timestamp {event_timestamp} < {MIN_TIMESTAMP})")
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

    # Strip internal helper fields
    for a in filtered:
        a.pop("_wu", None)

    if not filtered:
        return None

    return {
        "type": "individual",
        "sport": entry["sport"],
        "event": entry["event"],
        "status": "finished",
        "timestamp": event_timestamp,
        "results": filtered,
    }


def parse_start_list(data: str, entry: dict) -> dict | None:
    """Parse a feed for a non-finished event to extract the start list."""
    records = data.split("~")
    athletes: list[dict] = []
    has_scheduled = False
    current_round = None

    for rec in records[1:]:
        d, raa, rab = parse_fields(rec)

        if "ZAE" in d:
            current_round = d["ZAE"]
            continue

        if "AE" not in d:
            continue

        # Skip sub-rounds for multi-round events
        if current_round is not None and current_round != "Totalt":
            continue

        if d.get("AB") == "1":
            has_scheduled = True

        ra = dict(zip(raa, rab))
        bib = ra.get("7", "")
        athletes.append({
            "name": _full_name(d.get("AE", ""), d.get("WU", "")),
            "country": d.get("FU", ""),
            "bib": _int(bib) if bib else None,
        })

    if not has_scheduled or not athletes:
        return None

    swe = [a for a in athletes if a["country"] in SWE_COUNTRIES]

    return {
        "type": "startlist",
        "sport": entry["sport"],
        "event": entry["event"],
        "total": len(athletes),
        "swe_athletes": [{"name": a["name"], "country": a["country"]} for a in swe],
        "athletes": [{"name": a["name"], "country": a["country"], "bib": a["bib"]} for a in athletes],
    }


def _int(s: str) -> int | None:
    """Safe int parse, returns None for empty/invalid."""
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


def scrape_all() -> tuple[list[dict], set[str]]:
    """Scrape all feeds with rate limiting. Returns (matches, attempted_keys)."""
    all_matches: list[dict] = []
    attempted_keys: set[str] = set()  # Track all individual event keys we tried

    # Team sports: scrape from HTML results page (full history)
    for entry in TEAM_FEEDS:
        matches = fetch_team_results_html(entry)
        all_matches.extend(matches)
        print(f"  {entry['sport']} {entry['event']}: {len(matches)} finished matches")
        time.sleep(DELAY_BETWEEN_FEEDS)

    # Individual sports: scrape from feed API
    for entry in FEEDS:
        key = f"{entry['sport']}:{entry['event']}"
        attempted_keys.add(key)

        data = fetch_feed(entry["feed"])
        if not data:
            # Retry once
            time.sleep(5)
            data = fetch_feed(entry["feed"])
        if not data:
            print(f"  SKIP {entry['sport']} {entry['event']}: no data")
            time.sleep(DELAY_BETWEEN_FEEDS)
            continue

        result = parse_individual_feed(data, entry)
        if result:
            all_matches.append(result)
            n_swe = sum(1 for r in result["results"] if r["country"] in SWE_COUNTRIES)
            print(f"  {entry['sport']} {entry['event']}: {len(result['results'])} results ({n_swe} SWE)")
        else:
            # Try start list for upcoming events
            sl = parse_start_list(data, entry)
            if sl:
                all_matches.append(sl)
                print(f"  {entry['sport']} {entry['event']}: startlist {sl['total']} athletes ({len(sl['swe_athletes'])} SWE)")
            else:
                print(f"  {entry['sport']} {entry['event']}: no data")

        time.sleep(DELAY_BETWEEN_FEEDS)

    return all_matches, attempted_keys


def _match_key(m: dict) -> str:
    """Generate a unique key for a match (same logic as backend)."""
    if m.get("type") == "team":
        return f"{m['sport']}:{m['event']}:{m.get('home','')}-{m.get('away','')}"
    return f"{m['sport']}:{m['event']}"


def write_if_changed(matches: list[dict], attempted_keys: set[str] | None = None) -> bool:
    """Merge new matches with existing data and write. Removes stale entries."""
    try:
        with open(OUTPUT_FILE) as f:
            existing = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing = {}

    if not matches and existing.get("matches"):
        print("  Scraper returned 0 matches, keeping existing data")
        return False

    # Build set of new keys for quick lookup
    new_keys = {_match_key(m) for m in matches}

    # Build merged dict: start with existing, update/add from new scrape
    merged = {}
    for m in existing.get("matches", []):
        key = _match_key(m)
        # Remove stale entries: if we attempted this key but got no new data,
        # drop the old entry (it was likely old/invalid data)
        if attempted_keys and key in attempted_keys and key not in new_keys:
            print(f"  REMOVE stale: {key}")
            continue
        merged[key] = m
    for m in matches:
        merged[_match_key(m)] = m  # New data overwrites old for same key

    merged_list = list(merged.values())

    if merged_list == existing.get("matches", []):
        print("  No changes")
        return False

    output = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "matches": merged_list,
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"  Wrote {len(merged_list)} entries to {OUTPUT_FILE} ({len(merged_list) - len(existing.get('matches', []))} new)")
    return True


def main():
    print("Scraping Flashscore Winter Olympics 2026...")
    matches, attempted_keys = scrape_all()
    print(f"Total: {len(matches)} entries")
    changed = write_if_changed(matches, attempted_keys)
    if changed:
        print("Data changed!")
    else:
        print("No changes.")


if __name__ == "__main__":
    main()
