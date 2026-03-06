#!/usr/bin/env python3
"""
Wikipedia scraper for 2026 Winter Paralympics medal table and results.
Fetches medal standings and event results from Wikipedia.
Writes to data/wiki_medals.json and data/wiki_results.json.
Only updates files if data has changed.
"""

import json
import re
import urllib.request
import urllib.error
from html.parser import HTMLParser

MEDAL_TABLE_URL = "https://en.wikipedia.org/wiki/2026_Winter_Paralympics_medal_table"
RESULTS_URL = "https://en.wikipedia.org/wiki/List_of_2026_Winter_Paralympics_medal_winners"
RESULTS_FALLBACK_URL = "https://en.wikipedia.org/wiki/2026_Winter_Paralympics"
MEDALS_FILE = "data/wiki_medals.json"
RESULTS_FILE = "data/wiki_results.json"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# Country name → IOC code mapping
COUNTRY_TO_IOC = {
    "Norway": "NOR", "Germany": "GER", "United States": "USA",
    "Canada": "CAN", "Sweden": "SWE", "Netherlands": "NED",
    "Switzerland": "SUI", "Austria": "AUT", "France": "FRA",
    "Japan": "JPN", "Italy": "ITA", "Finland": "FIN",
    "South Korea": "KOR", "China": "CHN", "Czech Republic": "CZE",
    "Czechia": "CZE",
    "Australia": "AUS", "Slovenia": "SLO", "Poland": "POL",
    "Great Britain": "GBR", "New Zealand": "NZL",
    "Belgium": "BEL", "Spain": "ESP", "Denmark": "DEN",
    "Estonia": "EST", "Latvia": "LAT", "Romania": "ROU",
    "Slovakia": "SVK", "Croatia": "CRO", "Kazakhstan": "KAZ",
    "Belarus": "BLR", "Ukraine": "UKR", "Russia": "RUS",
    "Hungary": "HUN", "Bulgaria": "BUL", "Georgia": "GEO",
    "Andorra": "AND", "Liechtenstein": "LIE",
    "United Kingdom": "GBR", "Republic of Korea": "KOR",
    "People's Republic of China": "CHN",
    "ROC": "ROC", "Russian Paralympic Committee": "RPC",
    "Neutral Paralympic Athletes": "NPA",
    "Iran": "IRI", "Brazil": "BRA", "Argentina": "ARG",
    "Mexico": "MEX", "Chile": "CHI", "Colombia": "COL",
    "Mongolia": "MGL", "Thailand": "THA", "India": "IND",
    "Israel": "ISR", "Turkey": "TUR", "Greece": "GRE",
    "Ireland": "IRL", "Portugal": "POR", "Serbia": "SRB",
    "Bosnia and Herzegovina": "BIH", "Montenegro": "MNE",
    "North Macedonia": "MKD", "Albania": "ALB",
    "Lithuania": "LTU", "Moldova": "MDA",
    "Chinese Taipei": "TPE", "Hong Kong": "HKG",
    "Azerbaijan": "AZE", "Armenia": "ARM",
    "Uzbekistan": "UZB", "Tajikistan": "TJK",
}


class TableParser(HTMLParser):
    """Parse HTML tables from Wikipedia pages."""

    def __init__(self):
        super().__init__()
        self.tables: list[list[list[str]]] = []
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.in_link = False
        self.skip_tag = False
        self.current_table: list[list[str]] = []
        self.current_row: list[str] = []
        self.current_cell = ""
        self.table_classes: list[str] = []

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "table":
            self.in_table = True
            self.current_table = []
            self.table_classes.append(attrs_dict.get("class", ""))
        elif self.in_table:
            if tag == "tr":
                self.in_row = True
                self.current_row = []
            elif tag in ("td", "th") and self.in_row:
                self.in_cell = True
                self.current_cell = ""
            elif tag == "a" and self.in_cell:
                self.in_link = True
            elif tag in ("sup", "span") and self.in_cell:
                # Skip superscripts (footnotes) and some spans
                cls = attrs_dict.get("class", "")
                if "reference" in cls or "sortkey" in cls:
                    self.skip_tag = True
            elif tag == "img":
                pass  # Skip images (flags)

    def handle_endtag(self, tag):
        if tag == "table" and self.in_table:
            self.in_table = False
            if self.current_table:
                self.tables.append(self.current_table)
            if self.table_classes:
                self.table_classes.pop()
        elif tag == "tr" and self.in_row:
            self.in_row = False
            if self.current_row:
                self.current_table.append(self.current_row)
        elif tag in ("td", "th") and self.in_cell:
            self.in_cell = False
            self.current_row.append(self.current_cell.strip())
        elif tag == "a":
            self.in_link = False
        elif tag in ("sup", "span"):
            self.skip_tag = False

    def handle_data(self, data):
        if self.in_cell and not self.skip_tag:
            self.current_cell += data


def fetch_page(url: str) -> str:
    """Fetch a web page. Returns HTML or empty string on error."""
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


def _extract_country(cell: str) -> str:
    """Extract country name from a Wikipedia cell, stripping footnotes and whitespace."""
    # Remove common Wikipedia artifacts
    cell = re.sub(r'\[.*?\]', '', cell)  # Remove [1], [a], etc.
    cell = re.sub(r'\*', '', cell)
    cell = cell.strip()
    # Try to match known country names
    for name in sorted(COUNTRY_TO_IOC.keys(), key=len, reverse=True):
        if name.lower() in cell.lower():
            return name
    return cell


def scrape_medal_table() -> list[dict]:
    """Scrape medal standings from Wikipedia."""
    html = fetch_page(MEDAL_TABLE_URL)
    if not html:
        return []

    parser = TableParser()
    parser.feed(html)

    # Find the medal table (wikitable with gold/silver/bronze columns)
    medal_table = None
    for table in parser.tables:
        if len(table) < 2:
            continue
        header = [c.lower().strip() for c in table[0]]
        # Look for gold/silver/bronze or G/S/B columns
        has_medals = (
            any("gold" in h for h in header)
            or any(h == "g" for h in header)
            or (len(header) >= 5 and all(c.isdigit() or c == '0' for c in table[1][-4:] if c.strip()))
        )
        if has_medals:
            medal_table = table
            break

    if not medal_table:
        # Fallback: find table with most numeric cells and country names
        best = None
        best_score = 0
        for table in parser.tables:
            score = 0
            for row in table:
                nums = [c for c in row if c.strip().isdigit()]
                has_country = any(_extract_country(c) in COUNTRY_TO_IOC for c in row)
                if len(nums) >= 3 and has_country:
                    score += 1
            if score > best_score:
                best_score = score
                best = table
        medal_table = best

    if not medal_table:
        print("  No medal table found")
        return []

    standings = []
    for row in medal_table[1:]:  # Skip header
        country_name = ""
        nums = []

        for cell in row:
            cell_clean = cell.strip()
            if cell_clean.isdigit():
                nums.append(int(cell_clean))
            elif not country_name:
                extracted = _extract_country(cell_clean)
                if extracted in COUNTRY_TO_IOC:
                    country_name = extracted

        if not country_name or len(nums) < 3:
            continue

        # Check for "Total" or "Totals" row
        if country_name.lower() in ("total", "totals", "totalt"):
            continue

        ioc = COUNTRY_TO_IOC[country_name]

        if len(nums) >= 5:
            # Skip rank number: [rank, gold, silver, bronze, total]
            g, s, b, t = nums[-4], nums[-3], nums[-2], nums[-1]
        elif len(nums) >= 4:
            g, s, b, t = nums[0], nums[1], nums[2], nums[3]
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
    print(f"  Medal table: {len(standings)} countries")
    return standings


# Sport name translations for event names
SPORT_EN_TO_SV = {
    "Alpine skiing": "Alpint",
    "Biathlon": "Skidskytte",
    "Cross-country skiing": "Längdskidor",
    "Para alpine skiing": "Alpint",
    "Para biathlon": "Skidskytte",
    "Para cross-country skiing": "Längdskidor",
    "Para ice hockey": "Paraishockey",
    "Para snowboard": "Snowboard",
    "Wheelchair curling": "Rullstolscurling",
    "Snowboard": "Snowboard",
}


def _clean_winner(text: str) -> str:
    """Clean a winner name from Wikipedia markup."""
    text = re.sub(r'\[.*?\]', '', text)  # Remove footnotes
    text = re.sub(r'\*', '', text)
    text = text.strip()
    return text


def scrape_results() -> list[dict]:
    """Scrape event results from Wikipedia."""
    # Try dedicated medal winners page first
    html = fetch_page(RESULTS_URL)
    if not html:
        html = fetch_page(RESULTS_FALLBACK_URL)
    if not html:
        return []

    parser = TableParser()
    parser.feed(html)

    results = []
    seen_events = set()

    for table in parser.tables:
        if len(table) < 2:
            continue

        header = [c.lower().strip() for c in table[0]]

        # Look for tables with event/gold/silver/bronze columns
        event_col = None
        gold_col = None
        silver_col = None
        bronze_col = None

        for i, h in enumerate(header):
            if h in ("event", "discipline", "gren", "tävling"):
                event_col = i
            elif h in ("gold", "guld", "1st"):
                gold_col = i
            elif h in ("silver", "2nd"):
                silver_col = i
            elif h in ("bronze", "brons", "3rd"):
                bronze_col = i

        if event_col is None or gold_col is None:
            continue

        for row in table[1:]:
            if len(row) <= max(event_col, gold_col):
                continue

            event = _clean_winner(row[event_col])
            if not event or event.lower() in ("total", "totals"):
                continue

            gold = _clean_winner(row[gold_col]) if gold_col < len(row) else ""
            silver = _clean_winner(row[silver_col]) if silver_col is not None and silver_col < len(row) else ""
            bronze = _clean_winner(row[bronze_col]) if bronze_col is not None and bronze_col < len(row) else ""

            if not gold and not silver and not bronze:
                continue

            if event in seen_events:
                continue
            seen_events.add(event)

            results.append({
                "event": event,
                "gold_winner": gold,
                "silver_winner": silver,
                "bronze_winner": bronze,
            })

    print(f"  Results: {len(results)} events")
    return results


def write_if_changed(data, filepath: str, label: str) -> bool:
    """Write JSON to file only if data changed. Returns True if written."""
    try:
        with open(filepath) as f:
            existing = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing = []

    if data == existing:
        print(f"  {label}: no changes")
        return False

    if not data and existing:
        print(f"  {label}: scraper returned 0 but file has {len(existing)} entries, keeping existing")
        return False

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"  {label}: wrote {len(data)} entries to {filepath}")
    return True


def main():
    print("Scraping Wikipedia Paralympics 2026...")
    changed = False

    # 1. Medal standings
    standings = scrape_medal_table()
    print(f"Standings: {len(standings)} countries")
    if write_if_changed(standings, MEDALS_FILE, "Medals"):
        changed = True

    # 2. Event results
    results = scrape_results()
    print(f"Results: {len(results)} events")
    if write_if_changed(results, RESULTS_FILE, "Results"):
        changed = True

    if changed:
        print("Data changed!")
    else:
        print("No changes.")


if __name__ == "__main__":
    main()
