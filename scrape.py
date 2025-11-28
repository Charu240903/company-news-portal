#!/usr/bin/env python3
# debug mode: small, fast run for GitHub Actions testing

import json, time, feedparser, requests
from readability import Document
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

OUT_JSON = "portal.json"
RSS_FEEDS = {
    "LiveMint_Companies": "https://www.livemint.com/rss/companies",
    "TechCrunch_Top": "https://techcrunch.com/feed/"
}
HEADERS = {"User-Agent":"Mozilla/5.0 (X11; Linux x86_64)"}
REQ_TIMEOUT = 6
CUTOFF_DT = datetime.utcnow() - timedelta(days=7)

def extract_text(html):
    try:
        doc = Document(html)
        return BeautifulSoup(doc.summary(), "html.parser").get_text(separator=" ", strip=True)
    except:
        return BeautifulSoup(html, "html.parser").get_text(separator=" ", strip=True)

print("DEBUG: Starting small test run", datetime.utcnow().isoformat())
discovered = {}
for name, url in RSS_FEEDS.items():
    print("DEBUG: parsing feed ->", name)
    try:
        f = feedparser.parse(url)
    except Exception as e:
        print("DEBUG: feed parse error", e)
        continue
    for entry in f.entries[:6]:           # limit entries per feed
        link = entry.get("link") or entry.get("id")
        if not link: continue
        discovered[link] = {"feed": name, "published_at": datetime.utcnow().isoformat()}

print("DEBUG: discovered URLs:", len(discovered))
results = []
count = 0
for url, meta in discovered.items():
    count += 1
    print(f"DEBUG: [{count}] fetching", url)
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
        html = r.text if r.status_code == 200 else ""
    except Exception as e:
        print("DEBUG: fetch error", e)
        html = ""
    text = extract_text(html)[:1000] if html else ""
    results.append({
        "title": meta.get("feed"),
        "url": url,
        "snippet": text,
        "scraped_at": datetime.utcnow().isoformat()
    })
    time.sleep(1)

with open(OUT_JSON, "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print("DEBUG: Done. Wrote", len(results), "items to", OUT_JSON)
