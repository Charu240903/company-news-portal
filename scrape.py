#!/usr/bin/env python3
"""
scrape.py

Discovery (RSS + optional NewsAPI) -> fetch article HTML -> extract full article text
(JSON-LD or readability) -> find exact sentences containing your keywords ->
write portal.json

Usage:
    python scrape.py

Environment:
    NEWSAPI_KEY (optional) - if set, NewsAPI discovery will run
"""

import os
import json
import re
import time
import feedparser
import requests
import pandas as pd
from readability import Document
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# ---------------- CONFIG ----------------
COMPANY_FILE = "Company_List.xlsx"     # must be present in repo
OUT_JSON = "portal.json"
# If you want NewsAPI discovery, set NEWSAPI_KEY as an environment variable (in Actions secrets)
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY", "").strip()

# RSS feeds map (name -> url)
RSS_FEEDS = {
    "LiveMint_Companies": "https://www.livemint.com/rss/companies",
    "LiveMint_Markets": "https://www.livemint.com/rss/markets",
    "CNBC_TopNews": "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "CNBC_Earnings": "https://www.cnbc.com/id/15839135/device/rss/rss.html",
    "CNBC_Markets": "https://www.cnbc.com/id/10001147/device/rss/rss.html",
    "MarketWatch_Top": "https://www.marketwatch.com/rss/topstories",
    "Fortune_Latest": "https://fortune.com/feed/",
    "Economist_Business": "https://www.economist.com/business/rss.xml",
    "Economist_Finance": "https://www.economist.com/finance-and-economics/rss.xml",
    "FinancialTimes_Home": "https://www.ft.com/?format=rss",
    "NPR_Business": "https://feeds.npr.org/1006/rss.xml",
    "McKinsey_Insights": "https://www.mckinsey.com/insights/rss",
    "WSJ_World": "https://feeds.a.dj.com/rss/RSSWorldNews.xml",
    "Wired_All": "https://www.wired.com/feed/rss",
    "TechCrunch_Top": "https://techcrunch.com/feed/",
    "SiliconANGLE_Top": "https://siliconangle.com/feed/",
    "Inc_Magazine": "https://www.inc.com/rss",
    "VentureBeat": "https://venturebeat.com/feed/",
    "MIT_TechReview": "https://www.technologyreview.com/feed/",
    "InnovationOrigins": "https://innovationorigins.com/en/feed/",
    "TheVerge": "https://www.theverge.com/rss/index.xml",
    "Engadget": "https://www.engadget.com/rss.xml",
    "ArsTechnica": "https://feeds.arstechnica.com/arstechnica/index/",
    "AndroidAuthority": "https://www.androidauthority.com/feed/",
    "ProductHunt_Today": "https://www.producthunt.com/feed",
    "Crunchbase_News": "https://news.crunchbase.com/feed/",
    "Sifted_FT": "https://sifted.eu/feed/",
    "Investing_Com_News": "https://www.investing.com/rss/news.rss",
    "MotleyFool_Latest": "https://www.fool.com/feeds/index.aspx",
    "TheStreet_Investing": "https://www.thestreet.com/.rss/full",
    "SeekingAlpha_Latest": "https://seekingalpha.com/feed.xml",
    "YahooFinance_Top": "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^GSPC",
}

# Keyword categories (your packs) - edit/extend as needed
KEYWORD_CATEGORIES = {
    "Investment & Expansion": [
        "new investment","investment plan","capex","capital expenditure","expansion",
        "expansion plan","growth strategy","capacity expansion","greenfield","brownfield",
        "new facility","new plant","manufacturing plant","production facility","factory expansion",
        "new site","site selection","location scouting","relocation","footprint expansion",
        "supply chain expansion"
    ],
    "Geographic Markets": [
        "entering india","india investment","india expansion","asia expansion","apac expansion",
        "emerging markets expansion","gcc expansion","middle east expansion",
        "southeast asia expansion","africa expansion","latin america expansion"
    ],
    "Employment / Headcount": [
        "headcount expansion","hiring plans","jobs creation","talent expansion",
        "recruitment drive","workforce expansion"
    ],
    "Deals & Partnerships": [
        "mou","partnership","strategic partnership","jv","joint venture","collaboration",
        "investment agreement"
    ],
    "Manufacturing & Industrial": [
        "manufacturing capacity","assembly plant","production ramp-up","industrial park",
        "automation upgrade","supply chain diversification","reshoring","nearshoring"
    ],
    "Automotive & EV": [
        "ev plant","battery factory","gigafactory","ev supply chain","auto components expansion",
        "charging infrastructure investment"
    ],
    "Electronics & Semiconductors": [
        "semiconductor fab","atmp plant","chip manufacturing","foundry expansion",
        "pcb manufacturing","electronics assembly plant","r&d center expansion"
    ],
    "Lifesciences, Pharma & Biotech": [
        "api facility","formulations plant","clinical trial expansion","biomanufacturing",
        "vaccine production","fda approval","gmp manufacturing expansion"
    ],
    "IT, Digital Services & GCCs": [
        "global capability center","it hub","delivery center","technology center",
        "r&d center","digital innovation hub","ai/ml lab","cloud center","engineering center"
    ],
    "Energy, Renewables, Oil & Gas": [
        "solar plant","wind farm","renewable energy project","battery storage plant",
        "green hydrogen","refinery expansion","lng project","energy transition investment"
    ],
    "Retail, E-commerce & Consumer Goods": [
        "warehouse expansion","fulfillment center","distribution center","store expansion",
        "retail rollout","supply chain hub"
    ],
    "Chemicals & Materials": [
        "chemical plant expansion","materials facility","polymer plant",
        "specialty chemicals investment","capacity addition"
    ],
    "Aerospace & Defense": [
        "aerospace manufacturing","defense offset investment","mro facility","assembly line expansion"
    ],
    "Add-on Signals": [
        "procurement tender","land acquisition","large-scale hiring","orders placed for equipment",
        "fte increase","expansion capex cycle","supply agreement","long-term leasing",
        "infrastructure upgrade","capacity utilization approaching peak","record backlog",
        "new contract win","hiring"
    ]
}

# --------------- PREP (flatten keywords mapping) ---------------
ALL_KEYWORDS = []
KEYWORD_TO_CATEGORY = {}
for cat, kwlist in KEYWORD_CATEGORIES.items():
    for kw in kwlist:
        k = kw.lower()
        ALL_KEYWORDS.append(k)
        KEYWORD_TO_CATEGORY[k] = cat
KEYWORDS_SET = set(ALL_KEYWORDS)

# --------------- SETTINGS ---------------
CUTOFF_DT = datetime.utcnow() - timedelta(days=7)   # last 7 days
HEADERS = {"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/100 Safari/537.36"}
REQ_TIMEOUT = 15

# ------------------ HELPERS ------------------
def safe_open_excel(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Company file not found: {path}")
    return pd.read_excel(path)

def detect_company_column(df):
    candidates = [c for c in df.columns]
    company_col = None
    for c in candidates:
        if str(c).strip().lower() in ["company", "companies", "company name", "company_name", "name"]:
            company_col = c
            break
    if company_col is None:
        company_col = df.columns[0]
        print("⚠️ 'Company' column not found. Using first column:", company_col)
    else:
        print("✅ Detected company column:", company_col)
    return company_col

def extract_from_html(html):
    """Return best-effort article text: JSON-LD articleBody -> readability -> full text"""
    soup = BeautifulSoup(html, "html.parser")
    # JSON-LD articleBody
    ld = soup.select_one("script[type='application/ld+json']")
    if ld:
        try:
            j = json.loads(ld.string)
            if isinstance(j, dict) and j.get("articleBody"):
                return j.get("articleBody")
        except Exception:
            pass
    # readability fallback
    try:
        doc = Document(html)
        summary_html = doc.summary()
        text = BeautifulSoup(summary_html, "html.parser").get_text(separator=" ", strip=True)
        if len(text) > 50:
            return text
    except Exception:
        pass
    # last resort: plain text
    try:
        return soup.get_text(separator=" ", strip=True)
    except Exception:
        return ""

def match_keywords(text):
    t = text.lower()
    return [k for k in KEYWORDS_SET if k in t]

def match_keyword_categories(matched_keywords):
    return list({KEYWORD_TO_CATEGORY[k] for k in matched_keywords})

def match_companies(text, companies):
    t = text.lower()
    return [c for c in companies if c and c in t]

def extract_matched_sentences(full_text, matched_keywords):
    if not full_text or not matched_keywords:
        return []
    sents = re.split(r'(?<=[.!?])\s+', full_text)
    mk = [k.lower() for k in matched_keywords]
    matched = []
    for s in sents:
        sl = s.lower()
        if any(k in sl for k in mk):
            matched.append(s.strip())
    return matched

# ------------------ DISCOVERY ------------------
def discover_from_rss(rss_map):
    discovered = {}
    for feed_name, feed_url in rss_map.items():
        try:
            f = feedparser.parse(feed_url)
        except Exception as e:
            print(f"[RSS] Failed to parse {feed_name}: {e}")
            continue
        feed_title = f.feed.get("title") if hasattr(f, "feed") and f.feed else feed_name
        for entry in f.entries:
            pub_dt = None
            try:
                if getattr(entry, "published_parsed", None):
                    pub_dt = datetime.utcfromtimestamp(time.mktime(entry.published_parsed))
                elif getattr(entry, "updated_parsed", None):
                    pub_dt = datetime.utcfromtimestamp(time.mktime(entry.updated_parsed))
            except Exception:
                pub_dt = None
            if not pub_dt:
                pub_dt = datetime.utcnow()
            if pub_dt < CUTOFF_DT:
                continue
            link = entry.get("link") or entry.get("id")
            if not link:
                continue
            discovered[link] = {
                "source_type": "rss",
                "source_name": feed_title,
                "feed_name": feed_name,
                "feed_url": feed_url,
                "published_at": pub_dt.isoformat()
            }
    return discovered

def discover_from_newsapi(key):
    discovered = {}
    if not key:
        return discovered
    base_url = "https://newsapi.org/v2/everything"
    # chunk keywords so query isn't too long
    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]
    from_param = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    for chunk in chunks(ALL_KEYWORDS, 8):
        q = " OR ".join([f'"{kw}"' for kw in chunk])
        params = {
            "q": q,
            "language": "en",
            "pageSize": 100,
            "from": from_param,
            "sortBy": "publishedAt",
            "apiKey": key
        }
        try:
            r = requests.get(base_url, params=params, timeout=20, headers=HEADERS)
            if r.status_code != 200:
                print(f"[NewsAPI] non-200 {r.status_code}: {r.text[:200]}")
                continue
            data = r.json()
            for art in data.get("articles", []):
                url = art.get("url")
                if not url:
                    continue
                discovered[url] = {
                    "source_type": "newsapi",
                    "source_name": art.get("source", {}).get("name"),
                    "feed_name": "NewsAPI",
                    "feed_url": None,
                    "published_at": art.get("publishedAt") or datetime.utcnow().isoformat()
                }
        except Exception as e:
            print("[NewsAPI] chunk error:", e)
            continue
    return discovered

# ------------------ MAIN ------------------
def main():
    print("Starting scrape:", datetime.utcnow().isoformat())

    # load companies
    df_comp = safe_open_excel(COMPANY_FILE)
    comp_col = detect_company_column(df_comp)
    companies = df_comp[comp_col].astype(str).str.lower().str.strip().dropna().unique().tolist()
    print(f"Loaded {len(companies)} companies from {COMPANY_FILE}")

    # discovery
    print("Discovering from RSS...")
    urls = discover_from_rss(RSS_FEEDS)

    if NEWSAPI_KEY:
        print("Discovering from NewsAPI...")
        newsapi_urls = discover_from_newsapi(NEWSAPI_KEY)
        urls.update(newsapi_urls)

    print("Total discovered URLs:", len(urls))

    # fetch + extract + match
    results = []
    seen = set()
    for url, meta in urls.items():
        if url in seen:
            continue
        seen.add(url)

        try:
            r = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
            html = r.text if r.status_code == 200 else ""
        except Exception as e:
            print("Fetch error:", url, e)
            html = ""

        if not html:
            continue

        full_text = extract_from_html(html) or ""
        # get title
        title = ""
        try:
            title = Document(html).short_title()
        except Exception:
            soup = BeautifulSoup(html, "html.parser")
            title = (soup.title.string if soup.title else "") or ""

        combined = (title + " " + full_text).strip().lower()

        matched_keywords = match_keywords(combined)
        if not matched_keywords:
            # user requested only exact-article content that contains keywords
            continue

        matched_keyword_categories = match_keyword_categories(matched_keywords)
        matched_companies = match_companies(combined, companies)
        matched_sentences = extract_matched_sentences(full_text, matched_keywords)

        rec = {
            "title": title,
            "url": url,
            "source_type": meta.get("source_type"),
            "source_name": meta.get("source_name"),
            "feed_name": meta.get("feed_name"),
            "feed_url": meta.get("feed_url"),
            "published_at": meta.get("published_at"),
            "scraped_at": datetime.utcnow().isoformat(),
            "matched_companies": matched_companies,
            "matched_keywords": matched_keywords,
            "matched_keyword_categories": matched_keyword_categories,
            "matched_sentences": matched_sentences,
            "full_text": full_text,
            "snippet": (full_text[:400] + "...") if full_text else ""
        }

        results.append(rec)
        print("Matched:", rec["title"], "| kw:", matched_keywords, "| comp:", matched_companies)

    # write output
    with open(OUT_JSON, "w", encoding="utf-8") as fw:
        json.dump(results, fw, ensure_ascii=False, indent=2)

    print(f"Saved {len(results)} matched items to {OUT_JSON}")
    print("Finished:", datetime.utcnow().isoformat())


if __name__ == "__main__":
    main()
