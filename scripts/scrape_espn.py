#!/usr/bin/env python3
"""
Standalone ESPN scraper for GitHub Actions.
Fetches medal results and medal standings from ESPN.
Writes to data/espn_results.json and data/espn_medals.json.
Only updates files if data has changed.
"""

import json
import re
import sys
import urllib.request
import urllib.error
from datetime import date, timedelta
from html.parser import HTMLParser

ESPN_URL = "https://www.espn.com/olympics/winter/{year}/results/_/date/{date}"
ESPN_MEDALS_URL = "https://www.espn.com/olympics/winter/{year}/medals"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
WINDOW_RE = re.compile(r"window\['([^']+)'\]\s*=\s*")

# Game config
GAME_YEAR = 2026
GAME_START = date(2026, 2, 6)
GAME_END = date(2026, 2, 22)
OUTPUT_FILE = "data/espn_results.json"
MEDALS_FILE = "data/espn_medals.json"

# IOC code to country name mapping
IOC_TO_COUNTRY = {
    "NOR": "Norway", "GER": "Germany", "USA": "United States",
    "CAN": "Canada", "SWE": "Sweden", "NED": "Netherlands",
    "SUI": "Switzerland", "AUT": "Austria", "FRA": "France",
    "JPN": "Japan", "ITA": "Italy", "FIN": "Finland",
    "KOR": "South Korea", "CHN": "China", "CZE": "Czech Republic",
    "AUS": "Australia", "SLO": "Slovenia", "POL": "Poland",
    "GBR": "Great Britain", "NZL": "New Zealand",
    "BEL": "Belgium", "ESP": "Spain", "DEN": "Denmark",
    "EST": "Estonia", "LAT": "Latvia", "ROU": "Romania",
    "SVK": "Slovakia", "CRO": "Croatia", "KAZ": "Kazakhstan",
    "BLR": "Belarus", "UKR": "Ukraine", "RUS": "Russia",
    "HUN": "Hungary", "BUL": "Bulgaria", "GEO": "Georgia",
    "AND": "Andorra", "LIE": "Liechtenstein",
}


def fetch_page(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="replace")
            print(f"  GET {url} -> {resp.getcode()} ({len(html)} bytes)")
            return html
    except urllib.error.HTTPError as e:
        print(f"  FAIL {url}: HTTP {e.code}")
        return ""
    except Exception as e:
        print(f"  FAIL {url}: {e}")
        return ""


def extract_json(html: str) -> dict | None:
    for script in re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL):
        if "__espnfitt__" not in script:
            continue
        parts = WINDOW_RE.split(script.strip())
        for i, part in enumerate(parts):
            if part == "__espnfitt__" and i + 1 < len(parts):
                raw = parts[i + 1].rstrip().rstrip(";")
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    return None
    return None


def parse_medal_events(data: dict) -> list[dict]:
    results = []
    try:
        disciplines = data["page"]["content"]["results"]["competitions"]
    except (KeyError, TypeError):
        return results

    for disc in disciplines:
        for event in disc.get("events", []):
            for comp in event.get("competitions", []):
                oly = comp.get("olympicsStatus", {})
                if not (oly.get("completed") or oly.get("state") == "post"):
                    continue

                td = comp.get("tableData", [])
                is_medal = (
                    (td and any(r.get("medal") for r in td))
                    or comp.get("finalMedalComp")
                    or "Medal" in oly.get("description", "")
                    or "Medal" in comp.get("description", "")
                )
                if not is_medal:
                    continue

                gold = silver = bronze = ""
                gold_result = silver_result = bronze_result = ""

                if td:
                    for row in td:
                        medal = row.get("medal", "")
                        place = row.get("place")
                        name = row.get("athletes", "")
                        cc = row.get("abbreviation") or row.get("country", "")
                        entry = f"{name} ({cc})" if cc else name
                        res = row.get("result", "")
                        if place == 1 or medal == "G":
                            gold = entry
                            gold_result = res
                        elif place == 2 or medal == "S":
                            silver = entry
                            silver_result = res
                        elif place == 3 or medal == "B":
                            bronze = entry
                            bronze_result = res

                if not gold:
                    for r in comp.get("results", []):
                        ci = r.get("country", {})
                        entry = f"{ci.get('name', '')} ({ci.get('abbreviation', '')})"
                        p = r.get("place")
                        if p == 1:
                            gold = entry
                        elif p == 2:
                            silver = entry
                        elif p == 3:
                            bronze = entry

                sport = comp.get("sportName", disc.get("disciplineName", ""))
                ev = comp.get("eventName", event.get("eventName", ""))
                full_event = f"{sport} – {ev}" if sport else ev

                if full_event and (gold or silver or bronze):
                    results.append({
                        "event": full_event,
                        "gold_winner": gold,
                        "silver_winner": silver,
                        "bronze_winner": bronze,
                        "gold_result": gold_result,
                        "silver_result": silver_result,
                        "bronze_result": bronze_result,
                    })

    return results


def scrape_all_days() -> list[dict]:
    today = date.today()
    end = min(today, GAME_END)
    start = GAME_START

    if today < start:
        print(f"Games haven't started yet (start: {start})")
        return []

    all_results = []
    seen_events: set[str] = set()

    current = start
    while current <= end:
        date_str = current.strftime("%Y%m%d")
        url = ESPN_URL.format(year=GAME_YEAR, date=date_str)
        html = fetch_page(url)
        if html:
            data = extract_json(html)
            if data:
                day_results = parse_medal_events(data)
                for r in day_results:
                    if r["event"] not in seen_events:
                        seen_events.add(r["event"])
                        all_results.append(r)
                if day_results:
                    print(f"  {date_str}: {len(day_results)} medal events")
        current += timedelta(days=1)

    return all_results


# Reverse mapping: country name → IOC code
COUNTRY_TO_IOC = {v: k for k, v in IOC_TO_COUNTRY.items()}
COUNTRY_TO_IOC.update({
    "ROC": "ROC", "OAR": "OAR",
    "Czechia": "CZE", "Czech Republic": "CZE",
    "Chinese Taipei": "TPE", "Hong Kong": "HKG",
    "Korea": "KOR", "Republic of Korea": "KOR",
})


class MedalTableParser(HTMLParser):
    """Parse all HTML tables, collecting rows as lists of cell text."""

    def __init__(self):
        super().__init__()
        self.tables: list[list[list[str]]] = []
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.current_table: list[list[str]] = []
        self.current_row: list[str] = []
        self.current_cell = ""

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self.in_table = True
            self.current_table = []
        elif self.in_table:
            if tag == "tr":
                self.in_row = True
                self.current_row = []
            elif tag in ("td", "th") and self.in_row:
                self.in_cell = True
                self.current_cell = ""

    def handle_endtag(self, tag):
        if tag == "table" and self.in_table:
            self.in_table = False
            if self.current_table:
                self.tables.append(self.current_table)
        elif tag == "tr" and self.in_row:
            self.in_row = False
            if self.current_row:
                self.current_table.append(self.current_row)
        elif tag in ("td", "th") and self.in_cell:
            self.in_cell = False
            self.current_row.append(self.current_cell.strip())

    def handle_data(self, data):
        if self.in_cell:
            self.current_cell += data


def _find_medal_table(tables: list[list[list[str]]]) -> list[list[str]] | None:
    """Find the table that looks like a medal standings table.

    Looks for a table where multiple rows have a country name + 4 numbers.
    """
    best = None
    best_score = 0
    for table in tables:
        score = 0
        for row in table:
            nums = [c for c in row if c.strip().isdigit()]
            names = [c for c in row if c.strip() in COUNTRY_TO_IOC]
            if len(nums) >= 3 and names:
                score += 1
        if score > best_score:
            best_score = score
            best = table
    return best


def scrape_medal_standings() -> list[dict]:
    """Scrape medal standings from ESPN medals page."""
    url = ESPN_MEDALS_URL.format(year=GAME_YEAR)
    html = fetch_page(url)
    if not html:
        return []

    parser = MedalTableParser()
    parser.feed(html)

    table = _find_medal_table(parser.tables)
    if not table:
        print("  No medal table found in HTML")
        return []

    standings = []
    for row in table:
        nums = []
        country_name = ""
        for cell in row:
            cell_clean = cell.strip()
            if cell_clean.isdigit():
                nums.append(int(cell_clean))
            elif cell_clean in COUNTRY_TO_IOC:
                country_name = cell_clean
            elif len(cell_clean) == 3 and cell_clean.isupper() and cell_clean in IOC_TO_COUNTRY:
                # Direct IOC code
                country_name = IOC_TO_COUNTRY[cell_clean]

        if not country_name or len(nums) < 3:
            continue

        ioc = COUNTRY_TO_IOC[country_name]

        if len(nums) >= 4:
            g, s, b, t = nums[-4], nums[-3], nums[-2], nums[-1]
        else:
            g, s, b = nums[-3], nums[-2], nums[-1]
            t = g + s + b

        standings.append({
            "country_name": country_name,
            "country_code": ioc,
            "gold": g,
            "silver": s,
            "bronze": b,
            "total": t,
        })

    standings.sort(key=lambda x: (-x["gold"], -x["silver"], -x["bronze"]))
    return standings


def write_if_changed(data, filepath: str, label: str, allow_empty: bool = False) -> bool:
    """Write JSON to file only if data changed. Returns True if written.

    Won't overwrite existing data with empty list unless allow_empty=True.
    """
    try:
        with open(filepath) as f:
            existing = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing = []

    if data == existing:
        print(f"  {label}: no changes")
        return False

    if not data and existing and not allow_empty:
        print(f"  {label}: scraper returned 0 but file has {len(existing)} entries, keeping existing data")
        return False

    if not allow_empty and len(data) < len(existing):
        print(f"  {label}: scraper returned {len(data)} but file has {len(existing)} entries, merging to avoid data loss")
        existing_by_event = {r["event"]: r for r in existing} if isinstance(existing, list) and existing and isinstance(existing[0], dict) and "event" in existing[0] else {}
        if existing_by_event:
            for r in data:
                existing_by_event[r["event"]] = r
            data = list(existing_by_event.values())

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"  {label}: wrote {len(data)} entries to {filepath}")
    return True


def main():
    print(f"Scraping ESPN Winter Olympics {GAME_YEAR}...")
    changed = False

    # 1. Medal event results
    results = scrape_all_days()
    print(f"Results: {len(results)} medal events")
    if write_if_changed(results, OUTPUT_FILE, "Results"):
        changed = True

    # 2. Medal standings
    standings = scrape_medal_standings()
    print(f"Standings: {len(standings)} countries")
    if write_if_changed(standings, MEDALS_FILE, "Standings"):
        changed = True

    if changed:
        print("Data changed!")
    else:
        print("No changes.")


if __name__ == "__main__":
    main()
