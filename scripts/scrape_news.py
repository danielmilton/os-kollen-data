#!/usr/bin/env python3
"""
Standalone RSS news scraper for GitHub Actions.
Fetches Swedish sports RSS feeds, filters for OS 2026 relevance,
writes to data/news.json. Only updates file if data has changed.
"""

import json
import re
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from html.parser import HTMLParser

OUTPUT_FILE = "data/news.json"

RSS_FEEDS = [
    ("SVT", "https://www.svt.se/sport/rss.xml"),
    ("Aftonbladet", "https://rss.aftonbladet.se/rss2/small/pages/sections/sportbladet/"),
    ("Expressen", "https://feeds.expressen.se/sport/"),
    ("TV4", "https://www.tv4.se/rss"),
]

OS_KEYWORDS = [
    # OS-specifika termer (matchas mot lowercased text)
    "os 2026", "os i ", "vinter-os", "olympiska", "olympiska spelen",
    "olympics", "olympic", "milano cortina", "cortina", "milano",
    "medaljhopp", "os-", "vinter-ol", "olympia", "vinterolympisk",
    "cortina d'ampezzo",
    # Medaljer & tävling
    "medalj", "vintersport",
    # Sporter
    "skidskytte", "biathlon",
    "längdskidor", "längdåkning", "langrenn",
    "backhoppning", "backhopp",
    "alpint", "störtlopp", "slalom", "super-g",
    "curling",
    "ishockey",
    "konståkning", "skridsko", "hastighetsåkning",
    "snowboard", "freestyle",
    "rodel", "skeleton", "bobsled",
    "short track", "nordisk kombination",
    # Svenska landslag
    "tre kronor", "damkronorna",
]

# Regex for uppercase "OS" as standalone word (Swedish for Olympiska Spelen)
# Can't use lowercased text since "os" = "oss/us" in Swedish
_OS_UPPER_RE = re.compile(r'\bOS\b')

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


class _HTMLStripper(HTMLParser):
    """Strip HTML tags, collect text and img src URLs."""
    def __init__(self):
        super().__init__()
        self.text_parts: list[str] = []
        self.images: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "img":
            for k, v in attrs:
                if k == "src" and v:
                    self.images.append(v)

    def handle_data(self, data):
        self.text_parts.append(data)

    def get_text(self) -> str:
        return " ".join("".join(self.text_parts).split()).strip()


def strip_html(html: str) -> tuple[str, list[str]]:
    """Strip HTML tags from string. Returns (clean_text, [image_urls])."""
    stripper = _HTMLStripper()
    try:
        stripper.feed(unescape(html))
    except Exception:
        return re.sub(r"<[^>]+>", "", html).strip(), []
    return stripper.get_text(), stripper.images


def is_os_relevant(title: str, summary: str = "") -> bool:
    text = (title + " " + (summary or "")).lower()
    if any(kw in text for kw in OS_KEYWORDS):
        return True
    # Check for uppercase "OS" in original text (e.g. "i OS", "på OS", "OS-guld")
    original = title + " " + (summary or "")
    if _OS_UPPER_RE.search(original):
        return True
    return False


def scrape_feeds() -> list[dict]:
    """Fetch all RSS feeds and return OS-relevant articles."""
    articles = []
    seen_links: set[str] = set()

    for source_name, feed_url in RSS_FEEDS:
        try:
            req = urllib.request.Request(feed_url, headers={
                "User-Agent": USER_AGENT,
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw = resp.read()
                print(f"  {source_name}: {len(raw)} bytes")
        except (urllib.error.URLError, Exception) as e:
            print(f"  WARN {source_name}: {e}")
            continue

        try:
            root = ET.fromstring(raw)
        except ET.ParseError as e:
            print(f"  WARN {source_name} parse: {e}")
            continue

        source_count = 0
        for item in root.iter("item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            raw_desc = (item.findtext("description") or "").strip()
            pub_date_str = item.findtext("pubDate")

            summary, desc_images = strip_html(raw_desc)

            if not title or not link:
                continue
            if link in seen_links:
                continue
            if not is_os_relevant(title, summary):
                continue

            pub_dt = None
            if pub_date_str:
                try:
                    pub_dt = parsedate_to_datetime(pub_date_str).isoformat()
                except Exception:
                    pass

            # Extract image: enclosure > media:content > inline <img>
            image_url = None
            enc = item.find("enclosure")
            if enc is not None and enc.get("type", "").startswith("image"):
                image_url = enc.get("url")
            if not image_url:
                for ns in ["{http://search.yahoo.com/mrss/}"]:
                    mc = item.find(f"{ns}content")
                    if mc is not None and mc.get("medium") == "image":
                        image_url = mc.get("url")
                        break
            if not image_url and desc_images:
                image_url = desc_images[0]

            seen_links.add(link)
            articles.append({
                "title": title,
                "link": link,
                "source": source_name,
                "published_at": pub_dt,
                "summary": (summary[:2000] if summary else None),
                "image_url": image_url,
            })
            source_count += 1

        print(f"  {source_name}: {source_count} OS-relevanta artiklar")

    # Sort by published_at descending (newest first)
    articles.sort(key=lambda a: a.get("published_at") or "", reverse=True)
    return articles


def main():
    print("Scraping Swedish sports RSS feeds for OS 2026 news...")
    new_articles = scrape_feeds()
    print(f"Total: {len(new_articles)} artiklar")

    # Load existing to merge (keep articles not in current scrape but < 7 days old)
    try:
        with open(OUTPUT_FILE) as f:
            existing = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing = []

    # Merge: new articles take priority, keep old ones not seen this scrape
    new_links = {a["link"] for a in new_articles}
    merged = list(new_articles)
    for old in existing:
        if old["link"] not in new_links:
            merged.append(old)

    # Cap at 50 articles max
    merged = merged[:50]

    if merged == existing:
        print("No changes.")
        return

    with open(OUTPUT_FILE, "w") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    print(f"Wrote {len(merged)} articles to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
