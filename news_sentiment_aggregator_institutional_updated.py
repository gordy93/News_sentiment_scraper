
# -*- coding: utf-8 -*-
"""
Institutional-grade continuous news + Reddit sentiment scraper.

Major upgrades:
- Supports rich institutional source configuration.
- Uses Amsterdam time consistently for logs, scheduling, and report naming.
- Stores hourly reports and produces the 24H report only at Amsterdam day-end.
- Prediction blocks include chosen industries and stocks with end-of-cycle prices.
- Dashboards prioritize prediction output first, then news sections by geography.
- Reddit is handled in a separate dashboard section grouped by inferred country.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import html
import json
import logging
import math
import re
import sys
import time
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import feedparser
import numpy as np
import pandas as pd
import requests
import trafilatura
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

AMSTERDAM_TZ = ZoneInfo("Europe/Amsterdam")
UTC = timezone.utc
REQUEST_TIMEOUT = 20
POLL_INTERVAL_SECONDS = 180
MAX_ARTICLES_PER_FEED = 80
MAX_ITEMS_PER_WINDOW = 1200
BASE_PATH = Path("/content/drive/My Drive/Global News")

# -----------------------------
# Institutional-grade source blocks
# -----------------------------
SOURCE_FEEDS = [
    {"publisher": "Reuters", "country": "US", "region": "North America", "feed_name": "Reuters World", "feed_url": "https://news.google.com/rss/search?q=site:reuters.com%20when:1h&hl=en-US&gl=US&ceid=US:en", "source_type": "news", "priority": "tier1", "publisher_class": "wire", "coverage_tags": ["global", "macro", "markets", "companies", "geopolitics"]},
    {"publisher": "Associated Press", "country": "US", "region": "North America", "feed_name": "AP Top News", "feed_url": "https://apnews.com/hub/apf-topnews?output=rss", "source_type": "news", "priority": "tier1", "publisher_class": "wire", "coverage_tags": ["breaking", "politics", "global", "general"]},
    {"publisher": "Bloomberg", "country": "US", "region": "North America", "feed_name": "Bloomberg Markets", "feed_url": "https://news.google.com/rss/search?q=site:bloomberg.com%20(markets%20OR%20economics%20OR%20companies)%20when:1h&hl=en-US&gl=US&ceid=US:en", "source_type": "news", "priority": "tier1", "publisher_class": "financial_media", "coverage_tags": ["markets", "macro", "rates", "fx", "commodities", "companies"]},
    {"publisher": "CNBC", "country": "US", "region": "North America", "feed_name": "CNBC Latest", "feed_url": "https://www.cnbc.com/id/100003114/device/rss/rss.html", "source_type": "news", "priority": "tier1", "publisher_class": "financial_media", "coverage_tags": ["markets", "macro", "companies", "technology", "consumer"]},
    {"publisher": "MarketWatch", "country": "US", "region": "North America", "feed_name": "MarketWatch Top Stories", "feed_url": "https://feeds.content.dowjones.io/public/rss/mw_topstories", "source_type": "news", "priority": "tier2", "publisher_class": "financial_media", "coverage_tags": ["markets", "macro", "companies", "rates"]},
    {"publisher": "Yahoo Finance", "country": "US", "region": "North America", "feed_name": "Yahoo Finance News", "feed_url": "https://news.google.com/rss/search?q=site:finance.yahoo.com%20when:1h&hl=en-US&gl=US&ceid=US:en", "source_type": "news", "priority": "tier2", "publisher_class": "financial_media", "coverage_tags": ["markets", "companies", "technology", "crypto"]},
    {"publisher": "BBC", "country": "UK", "region": "Europe", "feed_name": "BBC Front", "feed_url": "http://feeds.bbci.co.uk/news/rss.xml", "source_type": "news", "priority": "tier1", "publisher_class": "broadcaster", "coverage_tags": ["global", "politics", "general", "business"]},
    {"publisher": "BBC", "country": "UK", "region": "Europe", "feed_name": "BBC Business", "feed_url": "http://feeds.bbci.co.uk/news/business/rss.xml", "source_type": "news", "priority": "tier1", "publisher_class": "broadcaster", "coverage_tags": ["business", "macro", "markets", "companies"]},
    {"publisher": "The Guardian", "country": "UK", "region": "Europe", "feed_name": "Guardian World", "feed_url": "https://www.theguardian.com/world/rss", "source_type": "news", "priority": "tier1", "publisher_class": "newspaper", "coverage_tags": ["global", "politics", "geopolitics", "general"]},
    {"publisher": "The Guardian", "country": "UK", "region": "Europe", "feed_name": "Guardian Business", "feed_url": "https://www.theguardian.com/uk/business/rss", "source_type": "news", "priority": "tier1", "publisher_class": "newspaper", "coverage_tags": ["business", "macro", "companies", "markets"]},
    {"publisher": "Financial Times", "country": "UK", "region": "Europe", "feed_name": "FT Global", "feed_url": "https://news.google.com/rss/search?q=site:ft.com%20when:1h&hl=en-GB&gl=GB&ceid=GB:en", "source_type": "news", "priority": "tier1", "publisher_class": "financial_media", "coverage_tags": ["macro", "markets", "companies", "policy", "geopolitics"]},
    {"publisher": "Sky News", "country": "UK", "region": "Europe", "feed_name": "Sky News Home", "feed_url": "https://feeds.skynews.com/feeds/rss/home.xml", "source_type": "news", "priority": "tier2", "publisher_class": "broadcaster", "coverage_tags": ["breaking", "general", "politics", "world"]},
    {"publisher": "Politico Europe", "country": "EU", "region": "Europe", "feed_name": "Politico Europe", "feed_url": "https://www.politico.eu/rss/politics-news/", "source_type": "news", "priority": "tier2", "publisher_class": "policy_media", "coverage_tags": ["policy", "europe", "regulation", "politics"]},
    {"publisher": "Euronews", "country": "EU", "region": "Europe", "feed_name": "Euronews Business", "feed_url": "https://www.euronews.com/rss?level=theme&name=business", "source_type": "news", "priority": "tier2", "publisher_class": "broadcaster", "coverage_tags": ["europe", "business", "macro", "policy"]},
    {"publisher": "Al Jazeera", "country": "Qatar", "region": "Middle East", "feed_name": "Al Jazeera News", "feed_url": "https://www.aljazeera.com/xml/rss/all.xml", "source_type": "news", "priority": "tier2", "publisher_class": "broadcaster", "coverage_tags": ["global", "geopolitics", "middle_east", "breaking"]},
    {"publisher": "Federal Reserve", "country": "US", "region": "North America", "feed_name": "Fed News", "feed_url": "https://news.google.com/rss/search?q=Federal%20Reserve%20when:1h&hl=en-US&gl=US&ceid=US:en", "source_type": "news", "priority": "tier1", "publisher_class": "official", "coverage_tags": ["macro", "rates", "policy", "central_banks"]},
    {"publisher": "ECB", "country": "EU", "region": "Europe", "feed_name": "ECB News", "feed_url": "https://news.google.com/rss/search?q=European%20Central%20Bank%20when:1h&hl=en-GB&gl=GB&ceid=GB:en", "source_type": "news", "priority": "tier1", "publisher_class": "official", "coverage_tags": ["macro", "rates", "policy", "central_banks"]},
    {"publisher": "Bank of England", "country": "UK", "region": "Europe", "feed_name": "BoE News", "feed_url": "https://news.google.com/rss/search?q=Bank%20of%20England%20when:1h&hl=en-GB&gl=GB&ceid=GB:en", "source_type": "news", "priority": "tier1", "publisher_class": "official", "coverage_tags": ["macro", "rates", "policy", "central_banks"]},
]

REDDIT_SOURCES = [
    {"publisher": "Reddit", "country": "Global", "region": "Global", "feed_name": "r/news", "subreddit": "news", "feed_url": "https://www.reddit.com/r/news/new/.rss", "source_type": "reddit", "priority": "tier1", "community_type": "general_news", "coverage_tags": ["breaking", "general", "us_news"]},
    {"publisher": "Reddit", "country": "Global", "region": "Global", "feed_name": "r/worldnews", "subreddit": "worldnews", "feed_url": "https://www.reddit.com/r/worldnews/new/.rss", "source_type": "reddit", "priority": "tier1", "community_type": "global_news", "coverage_tags": ["world", "geopolitics", "global"]},
    {"publisher": "Reddit", "country": "Global", "region": "Global", "feed_name": "r/politics", "subreddit": "politics", "feed_url": "https://www.reddit.com/r/politics/new/.rss", "source_type": "reddit", "priority": "tier1", "community_type": "politics", "coverage_tags": ["us_politics", "policy", "elections"]},
    {"publisher": "Reddit", "country": "Global", "region": "Global", "feed_name": "r/unitedkingdom", "subreddit": "unitedkingdom", "feed_url": "https://www.reddit.com/r/unitedkingdom/new/.rss", "source_type": "reddit", "priority": "tier2", "community_type": "regional_news", "coverage_tags": ["uk_news", "uk_politics", "regional"]},
    {"publisher": "Reddit", "country": "Global", "region": "Global", "feed_name": "r/ukpolitics", "subreddit": "ukpolitics", "feed_url": "https://www.reddit.com/r/ukpolitics/new/.rss", "source_type": "reddit", "priority": "tier2", "community_type": "politics", "coverage_tags": ["uk_politics", "policy", "government"]},
    {"publisher": "Reddit", "country": "Global", "region": "Global", "feed_name": "r/stocks", "subreddit": "stocks", "feed_url": "https://www.reddit.com/r/stocks/new/.rss", "source_type": "reddit", "priority": "tier1", "community_type": "equities", "coverage_tags": ["equities", "markets", "single_stocks"]},
    {"publisher": "Reddit", "country": "Global", "region": "Global", "feed_name": "r/investing", "subreddit": "investing", "feed_url": "https://www.reddit.com/r/investing/new/.rss", "source_type": "reddit", "priority": "tier1", "community_type": "multi_asset_investing", "coverage_tags": ["markets", "macro", "portfolio", "equities"]},
    {"publisher": "Reddit", "country": "Global", "region": "Global", "feed_name": "r/economics", "subreddit": "economics", "feed_url": "https://www.reddit.com/r/economics/new/.rss", "source_type": "reddit", "priority": "tier1", "community_type": "macro", "coverage_tags": ["macro", "economics", "policy", "rates"]},
    {"publisher": "Reddit", "country": "Global", "region": "Global", "feed_name": "r/business", "subreddit": "business", "feed_url": "https://www.reddit.com/r/business/new/.rss", "source_type": "reddit", "priority": "tier1", "community_type": "business_news", "coverage_tags": ["companies", "earnings", "corporate", "industry"]},
    {"publisher": "Reddit", "country": "Global", "region": "Global", "feed_name": "r/finance", "subreddit": "finance", "feed_url": "https://www.reddit.com/r/finance/new/.rss", "source_type": "reddit", "priority": "tier2", "community_type": "finance", "coverage_tags": ["finance", "macro", "markets"]},
    {"publisher": "Reddit", "country": "Global", "region": "Global", "feed_name": "r/technology", "subreddit": "technology", "feed_url": "https://www.reddit.com/r/technology/new/.rss", "source_type": "reddit", "priority": "tier2", "community_type": "technology_news", "coverage_tags": ["technology", "ai", "internet", "platforms"]},
    {"publisher": "Reddit", "country": "Global", "region": "Global", "feed_name": "r/energy", "subreddit": "energy", "feed_url": "https://www.reddit.com/r/energy/new/.rss", "source_type": "reddit", "priority": "tier2", "community_type": "sector_news", "coverage_tags": ["energy", "oil", "gas", "utilities", "transition"]},
    {"publisher": "Reddit", "country": "Global", "region": "Global", "feed_name": "r/geopolitics", "subreddit": "geopolitics", "feed_url": "https://www.reddit.com/r/geopolitics/new/.rss", "source_type": "reddit", "priority": "tier2", "community_type": "geopolitics", "coverage_tags": ["geopolitics", "security", "war", "diplomacy"]},
]

TOPIC_RULES = {
    "markets": ["stock", "stocks", "equity", "equities", "index", "indices", "futures", "options", "nasdaq", "s&p", "dow", "ftse", "stoxx", "nikkei", "hang seng", "volatility", "vix", "drawdown", "rally", "selloff", "correction", "valuation", "multiple expansion", "risk-on", "risk off", "market breadth", "trading volume", "liquidity"],
    "macro": ["inflation", "cpi", "ppi", "gdp", "pmi", "ism", "unemployment", "payrolls", "labor market", "consumer spending", "retail sales", "housing starts", "industrial production", "recession", "soft landing", "stagflation", "fiscal policy", "economic growth", "productivity", "deflation", "disinflation"],
    "central_banks": ["fed", "federal reserve", "ecb", "european central bank", "bank of england", "boe", "boj", "bank of japan", "pboc", "rba", "snb", "rate decision", "interest rate", "policy rate", "hawkish", "dovish", "quantitative tightening", "quantitative easing", "balance sheet", "forward guidance"],
    "rates_fx": ["treasury yield", "bond yield", "yield curve", "2-year yield", "10-year yield", "real yield", "dollar", "dxy", "eurusd", "usdjpy", "fx", "foreign exchange", "currency intervention", "devaluation", "sterling", "yen", "euro"],
    "credit": ["credit spread", "high yield", "investment grade", "default", "distressed debt", "leveraged loan", "private credit", "bank lending", "delinquency", "refinancing risk"],
    "commodities": ["oil", "crude", "wti", "brent", "natural gas", "lng", "gold", "silver", "copper", "iron ore", "aluminum", "nickel", "wheat", "corn", "soybeans", "commodity prices", "opec", "mine output", "refinery", "inventory draw"],
    "energy": ["energy security", "power grid", "electricity prices", "utilities", "renewables", "solar", "wind", "nuclear", "battery storage", "pipeline", "oilfield", "offshore drilling", "energy transition", "capacity", "outage"],
    "geopolitics": ["war", "conflict", "missile", "drone strike", "troop", "ceasefire", "sanctions", "export controls", "tariff", "trade war", "embargo", "border", "diplomacy", "summit", "peace talks", "military", "nato", "china tensions", "taiwan", "middle east"],
    "politics_policy": ["election", "parliament", "congress", "senate", "white house", "downing street", "prime minister", "president", "cabinet", "regulation", "antitrust", "legislation", "executive order", "budget", "tax reform", "industrial policy", "subsidy", "stimulus", "government shutdown"],
    "companies": ["earnings", "revenue", "ebitda", "guidance", "margin", "free cash flow", "buyback", "dividend", "merger", "acquisition", "takeover", "layoffs", "restructuring", "spinoff", "ipo", "secondary offering", "profit warning", "capital expenditure", "backlog", "order book", "inventory overhang", "demand slowdown"],
    "banks": ["bank", "lender", "deposit flight", "capital ratio", "tier 1 capital", "net interest margin", "loan loss provision", "liquidity coverage ratio", "commercial real estate exposure"],
    "technology": ["ai", "artificial intelligence", "semiconductor", "chip", "gpu", "data center", "cloud", "software", "cybersecurity", "openai", "microsoft", "nvidia", "amd", "broadcom", "foundry", "fab", "hyperscaler", "enterprise software", "saas"],
    "healthcare": ["drug trial", "fda", "ema approval", "biotech", "pharma", "medical device", "hospital", "health insurer", "patent challenge", "clinical results"],
    "consumer": ["consumer confidence", "retail", "e-commerce", "luxury", "travel demand", "airline", "hotel", "restaurant", "foot traffic", "pricing power", "inventory build"],
    "real_estate": ["commercial real estate", "office vacancy", "home prices", "mortgage rates", "housing affordability", "construction", "property developer", "reit"],
    "crypto": ["bitcoin", "ethereum", "crypto", "stablecoin", "etf inflows", "mining difficulty", "on-chain", "exchange outflows", "token unlock", "defi"],
    "supply_chain": ["shipping rates", "container rates", "logistics", "port congestion", "inventory cycle", "supplier disruption", "freight", "red sea", "semiconductor supply chain"],
}

SECTOR_TOPIC_MAP = {
    "technology": "Technology",
    "markets": "Multi-Asset / Market Structure",
    "macro": "Macro / Rates",
    "central_banks": "Macro / Rates",
    "rates_fx": "Macro / Rates",
    "credit": "Financials / Credit",
    "banks": "Financials / Credit",
    "companies": "Corporate / Cross-Sector",
    "consumer": "Consumer",
    "healthcare": "Healthcare",
    "real_estate": "Real Estate",
    "commodities": "Commodities / Materials",
    "energy": "Energy",
    "geopolitics": "Geopolitics / Sovereign Risk",
    "politics_policy": "Government / Regulation",
    "crypto": "Digital Assets",
    "supply_chain": "Industrials / Logistics",
}

SECTOR_TICKERS = {
    "Technology": [("AAPL", "Apple"), ("MSFT", "Microsoft"), ("NVDA", "NVIDIA"), ("AMD", "AMD"), ("AVGO", "Broadcom"), ("TSM", "TSMC")],
    "Multi-Asset / Market Structure": [("SPY", "SPDR S&P 500 ETF"), ("QQQ", "Invesco QQQ"), ("IWM", "iShares Russell 2000 ETF"), ("VXX", "iPath VIX ETN"), ("CME", "CME Group")],
    "Macro / Rates": [("TLT", "iShares 20+ Year Treasury Bond ETF"), ("IEF", "iShares 7-10 Year Treasury Bond ETF"), ("UUP", "Invesco DB US Dollar Index Bullish Fund"), ("FXE", "Invesco CurrencyShares Euro Trust"), ("GLD", "SPDR Gold Shares")],
    "Financials / Credit": [("JPM", "JPMorgan"), ("GS", "Goldman Sachs"), ("MS", "Morgan Stanley"), ("BAC", "Bank of America"), ("KRE", "SPDR S&P Regional Banking ETF"), ("HYG", "iShares iBoxx High Yield Corporate Bond ETF")],
    "Corporate / Cross-Sector": [("GE", "GE Aerospace"), ("CAT", "Caterpillar"), ("HON", "Honeywell"), ("UNP", "Union Pacific"), ("MMM", "3M")],
    "Consumer": [("AMZN", "Amazon"), ("WMT", "Walmart"), ("COST", "Costco"), ("MCD", "McDonald's"), ("BKNG", "Booking Holdings")],
    "Healthcare": [("LLY", "Eli Lilly"), ("JNJ", "Johnson & Johnson"), ("MRK", "Merck"), ("UNH", "UnitedHealth"), ("ISRG", "Intuitive Surgical")],
    "Real Estate": [("VNQ", "Vanguard Real Estate ETF"), ("AMT", "American Tower"), ("PLD", "Prologis"), ("SPG", "Simon Property Group"), ("CBRE", "CBRE Group")],
    "Commodities / Materials": [("XLB", "Materials Select Sector SPDR Fund"), ("FCX", "Freeport-McMoRan"), ("NEM", "Newmont"), ("AA", "Alcoa"), ("SCCO", "Southern Copper")],
    "Energy": [("XOM", "Exxon Mobil"), ("CVX", "Chevron"), ("COP", "ConocoPhillips"), ("SLB", "Schlumberger"), ("XLE", "Energy Select Sector SPDR Fund")],
    "Geopolitics / Sovereign Risk": [("GLD", "SPDR Gold Shares"), ("UUP", "Invesco DB US Dollar Index Bullish Fund"), ("ITA", "iShares U.S. Aerospace & Defense ETF"), ("XLE", "Energy Select Sector SPDR Fund")],
    "Government / Regulation": [("XLI", "Industrial Select Sector SPDR Fund"), ("XLV", "Health Care Select Sector SPDR Fund"), ("XLF", "Financial Select Sector SPDR Fund"), ("XLC", "Communication Services Select Sector SPDR Fund")],
    "Digital Assets": [("IBIT", "iShares Bitcoin Trust"), ("BITO", "ProShares Bitcoin Strategy ETF"), ("COIN", "Coinbase"), ("MSTR", "MicroStrategy")],
    "Industrials / Logistics": [("FDX", "FedEx"), ("UPS", "UPS"), ("UNP", "Union Pacific"), ("CSX", "CSX"), ("EXPD", "Expeditors")],
}

POSITIVE_TERMS = {
    "beat", "beats", "beating expectations", "above expectations", "strong demand", "robust demand", "sales growth", "revenue growth", "profit growth", "margin expansion", "record profit", "record revenue", "upside surprise", "guidance raised", "raises guidance", "accelerating growth", "strong outlook", "order growth", "backlog growth", "cash flow strength",
    "soft landing", "disinflation", "inflation cools", "inflation eases", "lower inflation", "rate cuts", "policy easing", "dovish shift", "stimulus", "fiscal support", "productivity gains", "resilient economy", "labor market resilience",
    "rally", "rebound", "recovery", "multiple expansion", "risk-on", "spread tightening", "yield declines", "lower yields", "improving liquidity", "credit improvement",
    "ceasefire", "de-escalation", "deal reached", "peace talks progress", "sanctions relief", "supply recovery", "inventory normalization", "shipping disruption eases",
    "upgrade", "buyback", "dividend increase", "strategic partnership", "cost discipline", "efficiency gains", "turnaround", "successful restructuring", "asset sale"
}
NEGATIVE_TERMS = {
    "miss", "misses", "below expectations", "weak demand", "demand slowdown", "profit warning", "guidance cut", "cuts guidance", "margin compression", "inventory glut", "inventory overhang", "sales slump", "revenue decline", "earnings decline", "cash burn", "write-down", "impairment", "restructuring charges", "layoffs", "default", "bankruptcy", "fraud",
    "inflation accelerates", "sticky inflation", "hot inflation", "rate hike", "higher for longer", "hawkish shift", "recession risk", "hard landing", "stagflation", "weak payrolls", "rising unemployment", "fiscal drag", "policy uncertainty",
    "selloff", "slump", "drawdown", "correction", "liquidity stress", "spread widening", "higher yields", "yield spike", "volatility surge", "risk-off", "capital outflows",
    "war", "escalation", "missile strike", "drone attack", "sanctions", "tariff increase", "trade war", "embargo", "supply disruption", "port congestion", "shipping disruption", "export controls", "cyberattack",
    "deposit flight", "credit losses", "liquidity crisis", "bank stress", "downgrade", "rating cut", "delinquency rise", "commercial real estate stress"
}
NEGATIONS = {"no", "not", "never", "without", "hardly", "rarely", "neither", "nor", "isn't", "wasn't", "aren't", "don't", "doesn't", "didn't", "can't", "couldn't", "won't", "wouldn't"}
UNCERTAINTY_TERMS = {"may", "might", "could", "unclear", "uncertainty", "rumor", "rumour", "reportedly", "possible", "possibly", "suggests", "appears", "indicates", "potential", "tentative", "unconfirmed", "if", "contingent", "preliminary", "sources say", "according to sources", "discussion", "considering", "weighing", "exploring", "expected to", "likely", "unlikely"}

COUNTRY_PATTERNS = {
    "US": ["united states", "u.s.", "us ", "america", "american", "white house", "congress", "fed", "federal reserve"],
    "UK": ["united kingdom", "u.k.", "uk ", "britain", "british", "downing street", "bank of england", "boe", "london"],
    "EU": ["european union", "eu ", "eurozone", "ecb", "brussels"],
    "China": ["china", "chinese", "beijing", "pboc"],
    "Japan": ["japan", "japanese", "tokyo", "boj"],
    "Middle East": ["israel", "gaza", "iran", "saudi", "qatar", "uae", "middle east"],
}

SOURCE_WEIGHTS = {"tier1": 1.0, "tier2": 0.8, "tier3": 0.6}


def default_output_root() -> Path:
    return BASE_PATH


def in_colab() -> bool:
    return "google.colab" in sys.modules


def maybe_mount_drive() -> None:
    if not in_colab():
        return
    try:
        from google.colab import drive
        drive.mount("/content/drive", force_remount=False)
    except Exception:
        pass


def amsterdam_now() -> datetime:
    return datetime.now(AMSTERDAM_TZ)


def floor_to_hour(dt: datetime) -> datetime:
    return dt.astimezone(AMSTERDAM_TZ).replace(minute=0, second=0, microsecond=0)


def parse_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = value if isinstance(value, datetime) else dateparser.parse(str(value))
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(AMSTERDAM_TZ)
    except Exception:
        return None


def clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    if isinstance(text, str) and "<" in text and ">" in text:
        txt = BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
    else:
        txt = str(text)
    return re.sub(r"\s+", " ", txt).strip()


def normalize_url(url: str) -> str:
    raw = (url or "").strip().split("#")[0]
    if not raw:
        return ""
    raw = re.sub(r"([?&](utm_[^=&]+|fbclid|gclid)=[^&#]*)", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"[?&]+$", "", raw)
    return raw


def domain_from_url(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def hash_text(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8", errors="ignore")).hexdigest()


class AmsterdamFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=AMSTERDAM_TZ)
        return dt.strftime(datefmt) if datefmt else dt.isoformat()


def build_logger(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("news_sentiment_scraper")
    logger.setLevel(logging.INFO)
    logger.handlers = []
    fmt = AmsterdamFormatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S %Z")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


@dataclass
class ArticleRecord:
    article_id: str
    canonical_id: str
    source_type: str
    publisher: str
    country: str
    region: str
    origin_country: str
    feed_name: str
    subreddit: str
    source_feed_url: str
    priority: str
    publisher_class: str
    coverage_tags: List[str]
    url: str
    domain: str
    title: str
    summary: str
    body: str
    authors: str
    published_at: Optional[str]
    first_seen_at: str
    final_sentiment_label: str
    final_sentiment_score: float
    market_sentiment_label: str
    market_sentiment_score: float
    sensitivity_analysis_score: float
    sentiment_model_used: str
    sentiment_model_note: str
    confidence_score: float
    topics: List[str]


@dataclass
class FeedSource:
    publisher: str
    country: str
    region: str
    feed_name: str
    feed_url: str
    source_type: str
    priority: str = "tier2"
    publisher_class: str = ""
    coverage_tags: Optional[List[str]] = None
    subreddit: str = ""
    community_type: str = ""


class StateManager:
    def __init__(self, state_dir: Path):
        self.seen_file = state_dir / "seen_urls.json"
        self.history_file = state_dir / "run_history.json"
        self.completed_file = state_dir / "completed_windows.json"
        self.hourly_prediction_file = state_dir / "hourly_sector_predictions.json"
        self.daily_prediction_file = state_dir / "daily_sector_predictions.json"
        self.hourly_manifest_file = state_dir / "hourly_manifest.json"
        self.seen_urls: Dict[str, str] = self._load(self.seen_file, {})
        self.run_history: List[Dict[str, Any]] = self._load(self.history_file, [])
        self.completed_windows = set(self._load(self.completed_file, []))
        self.hourly_predictions: Dict[str, Any] = self._load(self.hourly_prediction_file, {})
        self.daily_predictions: Dict[str, Any] = self._load(self.daily_prediction_file, {})
        self.hourly_manifest: Dict[str, List[str]] = self._load(self.hourly_manifest_file, {})

    @staticmethod
    def _load(path: Path, default: Any) -> Any:
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                return default
        return default

    @staticmethod
    def _save(path: Path, payload: Any) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)

    def persist(self) -> None:
        self._save(self.seen_file, self.seen_urls)
        self._save(self.history_file, self.run_history[-2000:])
        self._save(self.completed_file, sorted(self.completed_windows))
        self._save(self.hourly_prediction_file, self.hourly_predictions)
        self._save(self.daily_prediction_file, self.daily_predictions)
        self._save(self.hourly_manifest_file, self.hourly_manifest)


class SentimentEngine:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.finbert_model = None
        self.social_model = None
        try:
            from transformers import pipeline
            self.finbert_model = pipeline(task="text-classification", model="ProsusAI/finbert", tokenizer="ProsusAI/finbert", truncation=True, max_length=512, device=-1)
            self.logger.info("FinBERT loaded (news/finance path).")
        except Exception as exc:
            self.logger.warning("FinBERT unavailable, using rule-based sentiment for finance/news: %s", exc)
        try:
            from transformers import pipeline
            model_name = "cardiffnlp/twitter-roberta-base-sentiment-latest"
            self.social_model = pipeline(task="text-classification", model=model_name, tokenizer=model_name, truncation=True, max_length=512, device=-1)
            self.logger.info("Twitter RoBERTa loaded (reddit/social path).")
        except Exception as exc:
            self.logger.warning("Twitter RoBERTa unavailable, using rule-based sentiment for reddit/social: %s", exc)

        self.logger.info(
            "Sentiment analysis configuration | news_finance=%s | reddit_social=%s | fallback=rule_based | market_direction_adjustment=enabled",
            "FinBERT" if self.finbert_model is not None else "rule_based",
            "Twitter RoBERTa" if self.social_model is not None else "rule_based",
        )

    def _rule_score(self, text: str) -> Tuple[str, float, float]:
        lowered = text.lower()
        tokens = re.findall(r"[a-z']+", lowered)
        if not tokens:
            return "neutral", 0.5, 0.0
        raw = 0.0
        for term in POSITIVE_TERMS:
            if re.search(rf"\b{re.escape(term)}\b", lowered):
                raw += 1.8
        for term in NEGATIVE_TERMS:
            if re.search(rf"\b{re.escape(term)}\b", lowered):
                raw -= 1.8
        for i, t in enumerate(tokens):
            if t in NEGATIONS and i + 1 < len(tokens):
                nxt = tokens[i + 1]
                if nxt in {"good", "positive", "growth", "beat", "gain"}:
                    raw -= 1.2
                if nxt in {"bad", "negative", "loss", "drop", "slump"}:
                    raw += 1.2
        uncertainty_hits = sum(1 for w in tokens if w in UNCERTAINTY_TERMS)
        raw /= max(2.0, math.sqrt(len(tokens)))
        raw *= max(0.65, 1 - uncertainty_hits * 0.06)
        if raw > 0.12:
            label = "positive"
        elif raw < -0.12:
            label = "negative"
        else:
            label = "neutral"
        confidence = float(np.clip(0.52 + abs(raw), 0.0, 0.98))
        return label, confidence, float(raw)

    def score(self, text: str, topics: List[str], source_type: str = "news") -> Dict[str, Any]:
        text = clean_text(text)
        if not text:
            return {"label": "neutral", "confidence": 0.5, "raw": 0.0, "market_raw": 0.0, "market_label": "neutral", "model_used": "none", "model_note": "empty_text"}
        use_social_model = source_type == "reddit" and self.social_model is not None
        use_finbert_model = source_type != "reddit" and self.finbert_model is not None
        if use_social_model or use_finbert_model:
            try:
                pipeline_model = self.social_model if use_social_model else self.finbert_model
                out = pipeline_model(text[:3000])[0]
                lbl = out["label"].lower()
                if "positive" in lbl or "label_2" in lbl or lbl.endswith("2"):
                    label, raw = "positive", float(out["score"])
                elif "negative" in lbl or "label_0" in lbl or lbl.endswith("0"):
                    label, raw = "negative", -float(out["score"])
                else:
                    label, raw = "neutral", 0.0
                confidence = float(out["score"])
                model_used = "twitter_roberta" if use_social_model else "finbert"
                model_note = f"transformer_label={out['label']}"
            except Exception:
                label, confidence, raw = self._rule_score(text)
                model_used = "rule_based"
                model_note = "transformer_error_rule_fallback"
        else:
            label, confidence, raw = self._rule_score(text)
            model_used = "rule_based"
            model_note = "transformer_unavailable_rule_fallback"

        market_adj = 0.0
        ltxt = text.lower()
        if any(t in topics for t in ["macro", "central_banks", "rates_fx"]):
            if any(p in ltxt for p in ["inflation cools", "inflation eases", "lower inflation", "rate cuts", "dovish", "yield declines", "lower yields"]):
                market_adj += 0.55
            if any(p in ltxt for p in ["hot inflation", "sticky inflation", "rate hike", "higher for longer", "hawkish", "yield spike", "higher yields"]):
                market_adj -= 0.55
        if "geopolitics" in topics and any(p in ltxt for p in ["war", "missile", "escalation", "drone attack", "sanctions", "trade war"]):
            market_adj -= 0.65
        if "companies" in topics and any(p in ltxt for p in ["beats", "guidance raised", "raises guidance", "record revenue"]):
            market_adj += 0.35
        if "companies" in topics and any(p in ltxt for p in ["guidance cut", "profit warning", "demand slowdown", "layoffs"]):
            market_adj -= 0.35

        market_raw = float(np.clip(0.65 * raw + 0.35 * market_adj, -1.5, 1.5))
        if market_raw > 0.12:
            market_label = "positive"
        elif market_raw < -0.12:
            market_label = "negative"
        else:
            market_label = "neutral"
        return {"label": label, "confidence": confidence, "raw": float(raw), "market_raw": market_raw, "market_label": market_label, "model_used": model_used, "model_note": model_note}


class Scraper:
    def _build_news_sections_html(self, df: pd.DataFrame) -> str:
        grouped = self._group_news_sections(df)
        blocks = []
    
        section_meta = [
            ("US", "us-news", "U.S. News"),
            ("UK", "uk-news", "U.K. News"),
            ("Global", "global-news", "Global News"),
        ]
    
        for key, section_id, label in section_meta:
            part = grouped.get(key, pd.DataFrame())
            avg_sens = float(part["sensitivity_analysis_score"].mean()) if not part.empty else 0.0
            avg_mkt = float(part["market_sentiment_score"].mean()) if not part.empty else 0.0
            blocks.append(
                f"""
                <section class="card" id="{section_id}">
                  <div class="section-header">
                    <h2>{html.escape(label)}</h2>
                    <span class="section-subtitle">items={len(part)} | avg_market_sentiment={avg_mkt:.3f} | avg_sensitivity={avg_sens:.2f}</span>
                  </div>
                  <div class="subsection">
                    <h3>Subject summary</h3>
                    {self._render_country_subject_summary(part)}
                  </div>
                  <div class="subsection">
                    <h3>Top items</h3>
                    {self._render_item_list(part)}
                  </div>
                </section>
                """
            )
        return "".join(blocks)


    def _build_reddit_sections_html(self, df: pd.DataFrame) -> str:
        return f"""
        <section class="card" id="reddit-country">
          <div class="section-header">
            <h2>Reddit by Country</h2>
            <span class="section-subtitle">Country-level Reddit news and sentiment</span>
          </div>
          {self._render_reddit_by_country(df)}
        </section>
        """
    
    def __init__(self, output_root: Path, sources: Optional[List[FeedSource]] = None):
        self.output_root = output_root
        for p in ["logs", "state", "Report", "Data"]:
            (output_root / p).mkdir(parents=True, exist_ok=True)
        self.logger = build_logger(output_root / "logs" / "aggregator.log")
        self.state = StateManager(output_root / "state")
        self.sentiment = SentimentEngine(self.logger)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; NewsSentimentScraper/3.0)"})
        self.active_start = floor_to_hour(amsterdam_now())
        self.active_end = self.active_start + timedelta(hours=1)
        self.buffer: List[Dict[str, Any]] = []
        self.buffer_urls: set[str] = set()
        self.source_stats: Dict[str, Dict[str, int]] = {}
        self.sources = sources or default_sources()

    def _day_dirs(self, reference_dt: datetime) -> Tuple[Path, Path]:
        day_str = reference_dt.astimezone(AMSTERDAM_TZ).strftime("%d-%m-%Y")
        dashboard_dir = self.output_root / "Report" / day_str
        data_dir = self.output_root / "Data" / day_str
        dashboard_dir.mkdir(parents=True, exist_ok=True)
        data_dir.mkdir(parents=True, exist_ok=True)
        return dashboard_dir, data_dir

    def _in_window(self, dt: Optional[datetime]) -> bool:
        return bool(dt and self.active_start <= dt < self.active_end)

    def _discover(self) -> List[Dict[str, Any]]:
        candidates: List[Dict[str, Any]] = []
        for source in self.sources:
            source_cfg = asdict(source)
            source_name = source_cfg["feed_name"]
            self.source_stats.setdefault(source_name, {"fetched": 0, "accepted": 0, "errors": 0})
            try:
                parsed = feedparser.parse(source_cfg["feed_url"])
                for entry in parsed.entries[:MAX_ARTICLES_PER_FEED]:
                    self.source_stats[source_name]["fetched"] += 1
                    url = normalize_url(getattr(entry, "link", "") or getattr(entry, "id", ""))
                    if not url or url in self.state.seen_urls:
                        continue
                    published_at = parse_datetime(getattr(entry, "published", None) or getattr(entry, "updated", None) or getattr(entry, "pubDate", None))
                    if not self._in_window(published_at):
                        continue
                    candidates.append({
                        "source_type": source_cfg["source_type"],
                        "publisher": source_cfg["publisher"],
                        "country": source_cfg["country"],
                        "region": source_cfg.get("region", ""),
                        "priority": source_cfg.get("priority", "tier2"),
                        "publisher_class": source_cfg.get("publisher_class", ""),
                        "coverage_tags": source_cfg.get("coverage_tags") or [],
                        "subreddit": source_cfg.get("subreddit", ""),
                        "community_type": source_cfg.get("community_type", ""),
                        "feed_name": source_cfg["feed_name"],
                        "source_feed_url": source_cfg["feed_url"],
                        "url": url,
                        "title": clean_text(getattr(entry, "title", "")),
                        "summary": clean_text(getattr(entry, "summary", "") or getattr(entry, "description", "")),
                        "authors": clean_text(getattr(entry, "author", "")),
                        "published_at": published_at,
                    })
                    self.source_stats[source_name]["accepted"] += 1
            except Exception as exc:
                self.source_stats[source_name]["errors"] += 1
                self.logger.error("Feed error (%s): %s", source_cfg["feed_name"], exc)
        candidates.sort(key=lambda x: x.get("published_at") or self.active_start)
        return candidates[:MAX_ITEMS_PER_WINDOW]

    def _extract(self, item: Dict[str, Any]) -> Dict[str, Any]:
        if item["source_type"] == "reddit":
            item["body"] = item["summary"]
            item["domain"] = domain_from_url(item["url"]) or "reddit.com"
            return item
        body = ""
        try:
            resp = self.session.get(item["url"], timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200 and resp.text:
                raw = trafilatura.extract(resp.text, output_format="json", with_metadata=True, url=item["url"])
                if raw:
                    parsed = json.loads(raw)
                    body = clean_text(parsed.get("text", ""))
                    item["title"] = clean_text(parsed.get("title", "")) or item["title"]
                    item["authors"] = clean_text(parsed.get("author", "")) or item["authors"]
                    item["summary"] = item["summary"] or clean_text(parsed.get("description", ""))
                if not body:
                    soup = BeautifulSoup(resp.text, "html.parser")
                    body = " ".join(clean_text(p.get_text(" ", strip=True)) for p in soup.find_all("p")[:40])
        except Exception:
            pass
        item["body"] = clean_text(body)
        item["domain"] = domain_from_url(item["url"])
        return item

    def _topics(self, text: str, subreddit: str) -> List[str]:
        lowered = text.lower()
        topics = [t for t, kws in TOPIC_RULES.items() if any(k in lowered for k in kws)]
        if subreddit in {"stocks", "investing"} and "markets" not in topics:
            topics.insert(0, "markets")
        return topics or ["general"]

    def _canonical_id(self, item: Dict[str, Any]) -> str:
        base = re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", item["title"].lower())).strip()
        return hash_text(base or item["url"])

    def _infer_reddit_country(self, item: Dict[str, Any], text: str) -> str:
        subreddit = item.get("subreddit", "").lower()
        if subreddit in {"unitedkingdom", "ukpolitics"}:
            return "UK"
        if subreddit in {"politics", "news"}:
            return "US"
        lowered = text.lower()
        for country, patterns in COUNTRY_PATTERNS.items():
            if any(p in lowered for p in patterns):
                return country
        return "Global/Unknown"

    def _compute_sensitivity_score(self, market_raw: float, confidence: float, priority: str) -> float:
        return float(np.clip(abs(market_raw) * confidence * SOURCE_WEIGHTS.get(priority, 0.75) * 100.0, 0.0, 100.0))

    def _process_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if item["url"] in self.buffer_urls:
            return None
        item = self._extract(item)
        text = " ".join([item["title"], item["summary"], item.get("body", "")])
        topics = self._topics(text, item.get("subreddit", ""))
        sent = self.sentiment.score(text, topics, source_type=item.get("source_type", "news"))
        origin_country = item["country"] if item["source_type"] == "news" else self._infer_reddit_country(item, text)
        sensitivity = self._compute_sensitivity_score(sent["market_raw"], sent["confidence"], item.get("priority", "tier2"))
        out = {
            **item,
            "published_at": item["published_at"].isoformat() if isinstance(item.get("published_at"), datetime) else None,
            "first_seen_at": amsterdam_now().isoformat(),
            "origin_country": origin_country,
            "topics": topics,
            "canonical_id": self._canonical_id(item),
            "final_sentiment_label": sent["label"],
            "final_sentiment_score": sent["raw"],
            "market_sentiment_label": sent["market_label"],
            "market_sentiment_score": sent["market_raw"],
            "sensitivity_analysis_score": sensitivity,
            "sentiment_model_used": sent["model_used"],
            "sentiment_model_note": sent["model_note"],
            "confidence_score": sent["confidence"],
        }
        self.buffer_urls.add(item["url"])
        self.state.seen_urls[item["url"]] = amsterdam_now().isoformat()

        self.logger.info(
            "Added item | source=%s | published_at=%s | first_seen_at=%s | title=%s | url=%s",
            out["feed_name"], out["published_at"], out["first_seen_at"], out["title"][:160], out["url"]
        )
        self.logger.info(
            "Sentiment analysis | source=%s | model=%s | note=%s | text_sentiment=%s (%.4f) | market_sentiment=%s (%.4f) | sensitivity=%.2f",
            out["feed_name"], out["sentiment_model_used"], out["sentiment_model_note"], out["final_sentiment_label"],
            out["final_sentiment_score"], out["market_sentiment_label"], out["market_sentiment_score"], out["sensitivity_analysis_score"]
        )
        return out

    def _build_df(self) -> pd.DataFrame:
        rows = [asdict(ArticleRecord(
            article_id=hash_text(it["url"]),
            canonical_id=it["canonical_id"],
            source_type=it["source_type"],
            publisher=it["publisher"],
            country=it["country"],
            region=it.get("region", ""),
            origin_country=it["origin_country"],
            feed_name=it["feed_name"],
            subreddit=it.get("subreddit", ""),
            source_feed_url=it["source_feed_url"],
            priority=it.get("priority", "tier2"),
            publisher_class=it.get("publisher_class", ""),
            coverage_tags=it.get("coverage_tags", []),
            url=it["url"],
            domain=it.get("domain", ""),
            title=it.get("title", ""),
            summary=it.get("summary", ""),
            body=it.get("body", ""),
            authors=it.get("authors", ""),
            published_at=it.get("published_at"),
            first_seen_at=it["first_seen_at"],
            final_sentiment_label=it["final_sentiment_label"],
            final_sentiment_score=it["final_sentiment_score"],
            market_sentiment_label=it["market_sentiment_label"],
            market_sentiment_score=it["market_sentiment_score"],
            sensitivity_analysis_score=it["sensitivity_analysis_score"],
            sentiment_model_used=it.get("sentiment_model_used", "unknown"),
            sentiment_model_note=it.get("sentiment_model_note", ""),
            confidence_score=it["confidence_score"],
            topics=it["topics"],
        )) for it in self.buffer]
        return pd.DataFrame(rows)

    def _fetch_end_cycle_prices(self, tickers: List[str], cycle_end: datetime) -> Dict[str, Optional[float]]:
        out = {t: None for t in tickers}
        if not tickers:
            return out
        cycle_end_utc = int(cycle_end.astimezone(UTC).timestamp())
        cycle_start_utc = int((cycle_end - timedelta(days=5)).astimezone(UTC).timestamp())
        for ticker in tickers:
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
                params = {
                    "period1": cycle_start_utc,
                    "period2": cycle_end_utc + 300,
                    "interval": "1m",
                    "includePrePost": "true",
                    "events": "div,splits",
                }
                resp = self.session.get(url, params=params, timeout=10)
                if resp.status_code != 200:
                    continue
                data = resp.json().get("chart", {}).get("result", [])
                if not data:
                    continue
                timestamps = data[0].get("timestamp", [])
                indicators = data[0].get("indicators", {}).get("quote", [{}])[0]
                closes = indicators.get("close", [])
                valid = [(ts, px) for ts, px in zip(timestamps, closes) if ts <= cycle_end_utc and px is not None]
                if valid:
                    out[ticker] = float(valid[-1][1])
                    continue
                meta_px = data[0].get("meta", {}).get("regularMarketPrice")
                if isinstance(meta_px, (int, float)):
                    out[ticker] = float(meta_px)
            except Exception:
                continue
        return out

    def _build_prediction_block(self, df: pd.DataFrame, cycle: str, window_end: datetime) -> Dict[str, Any]:
        state_slot = self.state.hourly_predictions if cycle == "hourly" else self.state.daily_predictions
        if df.empty:
            return {"cycle": cycle, "generated_at": window_end.isoformat(), "sectors": [], "evaluation": []}
        sector_scores: Dict[str, List[float]] = {}
        sector_sensitivity: Dict[str, List[float]] = {}
        for _, row in df.iterrows():
            raw_topics = row.get("topics", [])
            if isinstance(raw_topics, str):
                raw_topics = [t.strip() for t in raw_topics.split(",") if t.strip()]
            for topic in raw_topics:
                sector = SECTOR_TOPIC_MAP.get(topic)
                if sector:
                    sector_scores.setdefault(sector, []).append(float(row.get("market_sentiment_score", 0.0)))
                    sector_sensitivity.setdefault(sector, []).append(float(row.get("sensitivity_analysis_score", 0.0)))
        if not sector_scores:
            return {"cycle": cycle, "generated_at": window_end.isoformat(), "sectors": [], "evaluation": []}

        ranked = sorted(
            ((s, float(np.mean(vals)), float(np.mean(sector_sensitivity.get(s, [0.0])))) for s, vals in sector_scores.items() if vals),
            key=lambda x: x[1],
            reverse=True,
        )
        chosen: List[Tuple[str, str, float, float]] = []
        chosen.append((ranked[0][0], "outperform", ranked[0][1], ranked[0][2]))
        if len(ranked) > 1:
            chosen.append((ranked[-1][0], "underperform", ranked[-1][1], ranked[-1][2]))

        sector_rows = []
        current_tickers: List[str] = []
        for sector, direction, score, sens in chosen:
            names = SECTOR_TICKERS.get(sector, [])[:3]
            firms = [{"ticker": t, "firm": n, "direction": direction} for t, n in names]
            current_tickers.extend([f["ticker"] for f in firms])
            sector_rows.append({"sector": sector, "sector_score": score, "direction": direction, "sensitivity_analysis_score": sens, "firms": firms})

        end_prices = self._fetch_end_cycle_prices(current_tickers, window_end)
        for row in sector_rows:
            for firm in row["firms"]:
                firm["price_at_cycle_end"] = end_prices.get(firm["ticker"])

        evaluation = []
        prev = state_slot.get("last_prediction")
        if prev and prev.get("firms"):
            latest_prices = self._fetch_end_cycle_prices([f["ticker"] for f in prev["firms"]], window_end)
            for firm in prev["firms"]:
                old_price = firm.get("price_at_prediction")
                new_price = latest_prices.get(firm["ticker"])
                if old_price is None or new_price is None:
                    continue
                move = "up" if new_price > old_price else ("down" if new_price < old_price else "flat")
                correct = (firm.get("direction") == "outperform" and move == "up") or (firm.get("direction") == "underperform" and move == "down")
                evaluation.append({
                    "ticker": firm["ticker"],
                    "firm": firm.get("firm"),
                    "direction_predicted": firm.get("direction"),
                    "price_previous": old_price,
                    "price_now": new_price,
                    "move": move,
                    "correct": bool(correct),
                    "sector": firm.get("sector"),
                })

        flat_current = []
        for row in sector_rows:
            for firm in row["firms"]:
                flat_current.append({
                    "ticker": firm["ticker"],
                    "firm": firm["firm"],
                    "direction": firm["direction"],
                    "price_at_prediction": firm.get("price_at_cycle_end"),
                    "sector": row["sector"],
                })
        state_slot["last_prediction"] = {"generated_at": window_end.isoformat(), "firms": flat_current}
        return {"cycle": cycle, "generated_at": window_end.isoformat(), "sectors": sector_rows, "evaluation": evaluation}

    def _group_news_sections(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        if df.empty:
            return {"US": pd.DataFrame(), "UK": pd.DataFrame(), "Global": pd.DataFrame()}
        news_df = df[df["source_type"] == "news"].copy()
        global_mask = ~news_df["country"].isin(["US", "UK"])
        return {
            "US": news_df[news_df["country"] == "US"].copy(),
            "UK": news_df[news_df["country"] == "UK"].copy(),
            "Global": news_df[global_mask].copy(),
        }

    def _render_item_list(self, section_df: pd.DataFrame, max_items: int = 12) -> str:
        if section_df.empty:
            return "<p>No items in this section.</p>"
        section_df = section_df.sort_values(["sensitivity_analysis_score", "market_sentiment_score"], ascending=[False, False]).head(max_items)
        lines = ["<ul>"]
        for _, row in section_df.iterrows():
            topics = row["topics"] if isinstance(row["topics"], list) else [t.strip() for t in str(row["topics"]).split(",") if t.strip()]
            lines.append(
                "<li>"
                f"<b>{html.escape(str(row.get('title', '')))}</b> "
                f"| source={html.escape(str(row.get('feed_name', '')))} "
                f"| topics={html.escape(', '.join(topics[:3]))} "
                f"| market={html.escape(str(row.get('market_sentiment_label', '')))} ({float(row.get('market_sentiment_score', 0.0)):.3f}) "
                f"| sensitivity={float(row.get('sensitivity_analysis_score', 0.0)):.2f} "
                f"| <a href=\"{html.escape(str(row.get('url', '')))}\">link</a>"
                "</li>"
            )
        lines.append("</ul>")
        return "".join(lines)

    def _render_country_subject_summary(self, section_df: pd.DataFrame) -> str:
        if section_df.empty:
            return "<p>No items in this section.</p>"
        rows = []
        exploded = section_df.copy()
        exploded["topic_primary"] = exploded["topics"].apply(lambda x: (x[0] if isinstance(x, list) and x else str(x).split(",")[0].strip()) if x is not None else "general")
        grouped = exploded.groupby("topic_primary").agg(
            item_count=("article_id", "count"),
            avg_market_sentiment=("market_sentiment_score", "mean"),
            avg_sensitivity=("sensitivity_analysis_score", "mean"),
        ).reset_index().sort_values(["avg_sensitivity", "item_count"], ascending=[False, False])
        rows.append("<table border='1' cellpadding='4' cellspacing='0'><tr><th>Subject</th><th>Items</th><th>Avg Market Sentiment</th><th>Avg Sensitivity</th></tr>")
        for _, r in grouped.iterrows():
            rows.append(
                f"<tr><td>{html.escape(str(r['topic_primary']))}</td><td>{int(r['item_count'])}</td><td>{float(r['avg_market_sentiment']):.3f}</td><td>{float(r['avg_sensitivity']):.2f}</td></tr>"
            )
        rows.append("</table>")
        return "".join(rows)

    def _render_reddit_by_country(self, df: pd.DataFrame) -> str:
        reddit_df = df[df["source_type"] == "reddit"].copy()
        if reddit_df.empty:
            return "<p>No Reddit items in this cycle.</p>"
    
        countries = sorted(reddit_df["origin_country"].fillna("Global/Unknown").unique().tolist())
        blocks = []
        for country in countries:
            part = reddit_df[reddit_df["origin_country"] == country].copy()
            avg_sens = float(part["sensitivity_analysis_score"].mean()) if not part.empty else 0.0
            avg_mkt = float(part["market_sentiment_score"].mean()) if not part.empty else 0.0
            blocks.append(
                f"""
                <div class="subsection">
                  <h3>{html.escape(country)} | items={len(part)} | avg_market_sentiment={avg_mkt:.3f} | avg_sensitivity={avg_sens:.2f}</h3>
                  {self._render_country_subject_summary(part)}
                  {self._render_item_list(part)}
                </div>
                """
            )
        return "".join(blocks)

    def _build_prediction_html(self, block: Dict[str, Any]) -> str:
        if not block.get("sectors"):
            return """
            <section class="card" id="predictions">
              <div class="section-header">
                <h2>Predictions</h2>
                <span class="section-subtitle">Next-cycle positioning</span>
              </div>
              <div class="empty-state">No sector signal available for this cycle.</div>
            </section>
            """
    
        rows = []
        for sector in block.get("sectors", []):
            sector_name = html.escape(str(sector.get("sector", "")))
            sector_score = sector.get("sector_score")
            sector_score_str = f"{sector_score:.3f}" if isinstance(sector_score, (int, float)) else "N/A"
            direction = html.escape(str(sector.get("direction", "")))
            direction_class = "badge-positive" if direction == "outperform" else "badge-negative" if direction == "underperform" else "badge-neutral"
            sector_sensitivity = sector.get("sensitivity_analysis_score")
            sensitivity_str = f"{sector_sensitivity:.3f}" if isinstance(sector_sensitivity, (int, float)) else "N/A"
    
            for firm in sector.get("firms", []):
                ticker = html.escape(str(firm.get("ticker", "")))
                firm_name = html.escape(str(firm.get("firm", "")))
                price_at_cycle_end = firm.get("price_at_cycle_end")
                price_str = f"{price_at_cycle_end:.2f}" if isinstance(price_at_cycle_end, (int, float)) else "N/A"
    
                rows.append(
                    f"""
                    <tr>
                      <td>{sector_name}</td>
                      <td>{firm_name}</td>
                      <td><span class="ticker">{ticker}</span></td>
                      <td><span class="badge {direction_class}">{direction}</span></td>
                      <td class="num">{sector_score_str}</td>
                      <td class="num">{sensitivity_str}</td>
                      <td class="num">{price_str}</td>
                    </tr>
                    """
                )
    
        eval_rows = []
        for row in block.get("evaluation", []):
            ticker = html.escape(str(row.get("ticker", "")))
            firm = html.escape(str(row.get("firm", "")))
            predicted = html.escape(str(row.get("direction_predicted", "")))
            prev_px = row.get("price_previous")
            now_px = row.get("price_now")
            move = html.escape(str(row.get("move", "")))
            correct = bool(row.get("correct"))
            correct_badge = '<span class="badge badge-positive">correct</span>' if correct else '<span class="badge badge-negative">incorrect</span>'
    
            prev_px_str = f"{prev_px:.2f}" if isinstance(prev_px, (int, float)) else "N/A"
            now_px_str = f"{now_px:.2f}" if isinstance(now_px, (int, float)) else "N/A"
    
            eval_rows.append(
                f"""
                <tr>
                  <td>{firm}</td>
                  <td><span class="ticker">{ticker}</span></td>
                  <td>{predicted}</td>
                  <td class="num">{prev_px_str}</td>
                  <td class="num">{now_px_str}</td>
                  <td>{move}</td>
                  <td>{correct_badge}</td>
                </tr>
                """
            )
    
        evaluation_html = f"""
        <div class="subsection">
          <h3>Previous-cycle evaluation</h3>
          <div class="table-wrap">
            <table class="styled-table">
              <thead>
                <tr>
                  <th>Firm</th>
                  <th>Ticker</th>
                  <th>Predicted</th>
                  <th>Previous price</th>
                  <th>Current price</th>
                  <th>Move</th>
                  <th>Result</th>
                </tr>
              </thead>
              <tbody>
                {''.join(eval_rows)}
              </tbody>
            </table>
          </div>
        </div>
        """ if eval_rows else """
        <div class="subsection">
          <h3>Previous-cycle evaluation</h3>
          <div class="empty-state">No previous price memory available yet for evaluation.</div>
        </div>
        """
    
        return f"""
        <section class="card" id="predictions">
          <div class="section-header">
            <h2>Predictions</h2>
            <span class="section-subtitle">Expected trajectory for the next cycle</span>
          </div>
    
          <div class="subsection">
            <h3>Current picks</h3>
            <div class="table-wrap">
              <table class="styled-table">
                <thead>
                  <tr>
                    <th>Sector / Industry</th>
                    <th>Stock</th>
                    <th>Ticker</th>
                    <th>Expected trajectory</th>
                    <th>Sector score</th>
                    <th>Sensitivity</th>
                    <th>End-of-cycle price</th>
                  </tr>
                </thead>
                <tbody>
                  {''.join(rows)}
                </tbody>
              </table>
            </div>
          </div>
    
          {evaluation_html}
        </section>
        """

    def _build_transparency_html(self, rows: List[Dict[str, Any]]) -> str:
        if not rows:
            return """
            <section class="card" id="transparency">
              <div class="section-header">
                <h2>Model Transparency</h2>
                <span class="section-subtitle">Per-item sentiment diagnostics</span>
              </div>
              <div class="empty-state">No items in this cycle.</div>
            </section>
            """
    
        table_rows = []
        for r in rows:
            title = html.escape(str(r.get("title", "")))
            source = html.escape(str(r.get("source_type", "")))
            text_sent = html.escape(str(r.get("final_sentiment_label", "")))
            market_sent = html.escape(str(r.get("market_sentiment_label", "")))
            model = html.escape(str(r.get("sentiment_model_used", "")))
            note = html.escape(str(r.get("sentiment_model_note", "")))
    
            def badge_class(val: str) -> str:
                if val == "positive":
                    return "badge-positive"
                if val == "negative":
                    return "badge-negative"
                return "badge-neutral"
    
            table_rows.append(
                f"""
                <tr>
                  <td class="title-cell">{title}</td>
                  <td>{source}</td>
                  <td><span class="badge {badge_class(text_sent)}">{text_sent}</span></td>
                  <td><span class="badge {badge_class(market_sent)}">{market_sent}</span></td>
                  <td>{model}</td>
                  <td>{note}</td>
                </tr>
                """
            )
    
        return f"""
        <section class="card" id="transparency">
          <div class="section-header">
            <h2>Model Transparency</h2>
            <span class="section-subtitle">Per-item sentiment diagnostics</span>
          </div>
          <details class="details-block">
            <summary>Show sentiment modeling details</summary>
            <div class="table-wrap top-gap">
              <table class="styled-table">
                <thead>
                  <tr>
                    <th>Title</th>
                    <th>Source</th>
                    <th>Text sentiment</th>
                    <th>Market sentiment</th>
                    <th>Model</th>
                    <th>Note</th>
                  </tr>
                </thead>
                <tbody>
                  {''.join(table_rows)}
                </tbody>
              </table>
            </div>
          </details>
        </section>
        """

    def _build_hourly_dashboard_html(self, stub: str, summary: Dict[str, Any], prediction_block: Dict[str, Any], transparency_rows: List[Dict[str, Any]], df: pd.DataFrame) -> str:
        pred = self._build_prediction_html(prediction_block)
        transparency = self._build_transparency_html(transparency_rows)
    
        def fmt_metric(v, pct=False):
            if v is None:
                return "N/A"
            if pct:
                return f"{float(v):.2%}"
            if isinstance(v, (int, float)):
                return f"{float(v):.3f}"
            return html.escape(str(v))
    
        news_sections = summary.get("news_sections_html", "")
        reddit_sections = summary.get("reddit_sections_html", "")
    
        return f"""
        <html>
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>{html.escape(stub)}</title>
          <style>
            :root {{
              --bg: #f4f7fb;
              --card: #ffffff;
              --text: #172033;
              --muted: #6b7280;
              --border: #e5e7eb;
              --shadow: 0 10px 30px rgba(15, 23, 42, 0.08);
              --pos-bg: #e8f7ee;
              --pos-text: #166534;
              --neg-bg: #fdecec;
              --neg-text: #b42318;
              --neu-bg: #eef2f7;
              --neu-text: #475467;
              --accent: #1f6feb;
              --accent-soft: #eaf2ff;
            }}
    
            * {{ box-sizing: border-box; }}
            html {{ scroll-behavior: smooth; }}
            body {{
              margin: 0;
              font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
              background: var(--bg);
              color: var(--text);
              line-height: 1.45;
            }}
    
            .container {{
              max-width: 1400px;
              margin: 0 auto;
              padding: 24px;
            }}
    
            .hero {{
              background: linear-gradient(135deg, #0f172a 0%, #1d4ed8 100%);
              color: white;
              border-radius: 22px;
              padding: 28px 30px;
              box-shadow: var(--shadow);
              margin-bottom: 20px;
            }}
    
            .hero h1 {{
              margin: 0 0 8px 0;
              font-size: 32px;
              font-weight: 800;
            }}
    
            .hero-meta {{
              display: flex;
              flex-wrap: wrap;
              gap: 18px;
              color: rgba(255,255,255,0.88);
              font-size: 14px;
            }}
    
            .topnav {{
              position: sticky;
              top: 0;
              z-index: 10;
              display: flex;
              flex-wrap: wrap;
              gap: 10px;
              padding: 14px 0 18px 0;
              background: linear-gradient(to bottom, var(--bg) 78%, rgba(244,247,251,0));
              backdrop-filter: blur(6px);
            }}
    
            .topnav a {{
              text-decoration: none;
              color: var(--accent);
              background: var(--accent-soft);
              border: 1px solid #d7e5ff;
              padding: 9px 14px;
              border-radius: 999px;
              font-size: 14px;
              font-weight: 600;
            }}
    
            .metrics {{
              display: grid;
              grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
              gap: 14px;
              margin-bottom: 20px;
            }}
    
            .metric {{
              background: var(--card);
              border: 1px solid var(--border);
              border-radius: 18px;
              padding: 18px;
              box-shadow: var(--shadow);
            }}
    
            .metric-label {{
              color: var(--muted);
              font-size: 13px;
              margin-bottom: 10px;
            }}
    
            .metric-value {{
              font-size: 28px;
              font-weight: 800;
              letter-spacing: -0.02em;
            }}
    
            .layout {{
              display: grid;
              gap: 18px;
            }}
    
            .card {{
              background: var(--card);
              border: 1px solid var(--border);
              border-radius: 22px;
              padding: 22px;
              box-shadow: var(--shadow);
            }}
    
            .section-header {{
              display: flex;
              justify-content: space-between;
              align-items: baseline;
              gap: 12px;
              flex-wrap: wrap;
              margin-bottom: 16px;
            }}
    
            .section-header h2 {{
              margin: 0;
              font-size: 24px;
              font-weight: 800;
            }}
    
            .section-subtitle {{
              color: var(--muted);
              font-size: 13px;
            }}
    
            .subsection {{
              margin-top: 18px;
            }}
    
            .subsection h3 {{
              margin: 0 0 10px 0;
              font-size: 18px;
            }}
    
            .table-wrap {{
              width: 100%;
              overflow-x: auto;
              border: 1px solid var(--border);
              border-radius: 16px;
            }}
    
            .styled-table {{
              width: 100%;
              border-collapse: collapse;
              min-width: 900px;
              background: white;
            }}
    
            .styled-table th,
            .styled-table td {{
              padding: 12px 14px;
              text-align: left;
              border-bottom: 1px solid var(--border);
              vertical-align: top;
              font-size: 14px;
            }}
    
            .styled-table th {{
              background: #f8fafc;
              color: #334155;
              font-size: 12px;
              text-transform: uppercase;
              letter-spacing: 0.04em;
            }}
    
            .styled-table tr:hover td {{
              background: #fafcff;
            }}
    
            .num {{
              text-align: right;
              font-variant-numeric: tabular-nums;
            }}
    
            .badge {{
              display: inline-block;
              padding: 4px 10px;
              border-radius: 999px;
              font-size: 12px;
              font-weight: 700;
              white-space: nowrap;
            }}
    
            .badge-positive {{
              background: var(--pos-bg);
              color: var(--pos-text);
            }}
    
            .badge-negative {{
              background: var(--neg-bg);
              color: var(--neg-text);
            }}
    
            .badge-neutral {{
              background: var(--neu-bg);
              color: var(--neu-text);
            }}
    
            .ticker {{
              font-weight: 800;
              letter-spacing: 0.03em;
            }}
    
            .empty-state {{
              color: var(--muted);
              background: #f8fafc;
              border: 1px dashed var(--border);
              border-radius: 16px;
              padding: 18px;
            }}
    
            .details-block {{
              margin-top: 10px;
            }}
    
            .details-block summary {{
              cursor: pointer;
              font-weight: 700;
              color: var(--accent);
            }}
    
            .top-gap {{
              margin-top: 14px;
            }}
    
            .title-cell {{
              max-width: 520px;
              min-width: 320px;
            }}
    
            @media (max-width: 900px) {{
              .container {{ padding: 14px; }}
              .hero {{ padding: 20px; }}
              .hero h1 {{ font-size: 26px; }}
              .metric-value {{ font-size: 24px; }}
              .section-header h2 {{ font-size: 22px; }}
            }}
          </style>
        </head>
        <body>
          <div class="container">
            <section class="hero">
              <h1>{html.escape(stub)}</h1>
              <div class="hero-meta">
                <span><strong>Type:</strong> Hourly report</span>
                <span><strong>Window start:</strong> {html.escape(str(summary.get("window_start", "N/A")))}</span>
                <span><strong>Window end:</strong> {html.escape(str(summary.get("window_end", "N/A")))}</span>
              </div>
            </section>
    
            <nav class="topnav">
              <a href="#predictions">Predictions</a>
              <a href="#us-news">U.S. News</a>
              <a href="#uk-news">U.K. News</a>
              <a href="#global-news">Global News</a>
              <a href="#reddit-country">Reddit by Country</a>
              <a href="#transparency">Transparency</a>
            </nav>
    
            <section class="metrics">
              <div class="metric">
                <div class="metric-label">Items</div>
                <div class="metric-value">{html.escape(str(summary.get("article_count", 0)))}</div>
              </div>
              <div class="metric">
                <div class="metric-label">Canonical items</div>
                <div class="metric-value">{html.escape(str(summary.get("canonical_article_count", 0)))}</div>
              </div>
              <div class="metric">
                <div class="metric-label">Avg market sentiment</div>
                <div class="metric-value">{fmt_metric(summary.get("avg_market_sentiment"))}</div>
              </div>
              <div class="metric">
                <div class="metric-label">Positive share</div>
                <div class="metric-value">{fmt_metric(summary.get("positive_share"), pct=True)}</div>
              </div>
              <div class="metric">
                <div class="metric-label">Negative share</div>
                <div class="metric-value">{fmt_metric(summary.get("negative_share"), pct=True)}</div>
              </div>
              <div class="metric">
                <div class="metric-label">Neutral share</div>
                <div class="metric-value">{fmt_metric(summary.get("neutral_share"), pct=True)}</div>
              </div>
            </section>
    
            <div class="layout">
              {pred}
              {news_sections}
              {reddit_sections}
              {transparency}
            </div>
          </div>
        </body>
        </html>
        """

    def _build_24h_dashboard_html(self, stub: str, summary: Dict[str, Any], prediction_block: Dict[str, Any], transparency_rows: List[Dict[str, Any]], df: pd.DataFrame) -> str:
        pred = self._build_prediction_html(prediction_block)
        transparency = self._build_transparency_html(transparency_rows)
    
        def fmt_metric(v, pct=False):
            if v is None:
                return "N/A"
            if pct:
                return f"{float(v):.2%}"
            if isinstance(v, (int, float)):
                return f"{float(v):.3f}"
            return html.escape(str(v))
    
        news_sections = summary.get("news_sections_html", "")
        reddit_sections = summary.get("reddit_sections_html", "")
    
        return f"""
        <html>
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>{html.escape(stub)}</title>
          <style>
            :root {{
              --bg: #f4f7fb;
              --card: #ffffff;
              --text: #172033;
              --muted: #6b7280;
              --border: #e5e7eb;
              --shadow: 0 10px 30px rgba(15, 23, 42, 0.08);
              --pos-bg: #e8f7ee;
              --pos-text: #166534;
              --neg-bg: #fdecec;
              --neg-text: #b42318;
              --neu-bg: #eef2f7;
              --neu-text: #475467;
              --accent: #1f6feb;
              --accent-soft: #eaf2ff;
            }}
    
            * {{ box-sizing: border-box; }}
            html {{ scroll-behavior: smooth; }}
            body {{
              margin: 0;
              font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
              background: var(--bg);
              color: var(--text);
              line-height: 1.45;
            }}
    
            .container {{
              max-width: 1400px;
              margin: 0 auto;
              padding: 24px;
            }}
    
            .hero {{
              background: linear-gradient(135deg, #0f172a 0%, #0f766e 100%);
              color: white;
              border-radius: 22px;
              padding: 28px 30px;
              box-shadow: var(--shadow);
              margin-bottom: 20px;
            }}
    
            .hero h1 {{
              margin: 0 0 8px 0;
              font-size: 32px;
              font-weight: 800;
            }}
    
            .hero-meta {{
              display: flex;
              flex-wrap: wrap;
              gap: 18px;
              color: rgba(255,255,255,0.88);
              font-size: 14px;
            }}
    
            .topnav {{
              position: sticky;
              top: 0;
              z-index: 10;
              display: flex;
              flex-wrap: wrap;
              gap: 10px;
              padding: 14px 0 18px 0;
              background: linear-gradient(to bottom, var(--bg) 78%, rgba(244,247,251,0));
              backdrop-filter: blur(6px);
            }}
    
            .topnav a {{
              text-decoration: none;
              color: var(--accent);
              background: var(--accent-soft);
              border: 1px solid #d7e5ff;
              padding: 9px 14px;
              border-radius: 999px;
              font-size: 14px;
              font-weight: 600;
            }}
    
            .metrics {{
              display: grid;
              grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
              gap: 14px;
              margin-bottom: 20px;
            }}
    
            .metric {{
              background: var(--card);
              border: 1px solid var(--border);
              border-radius: 18px;
              padding: 18px;
              box-shadow: var(--shadow);
            }}
    
            .metric-label {{
              color: var(--muted);
              font-size: 13px;
              margin-bottom: 10px;
            }}
    
            .metric-value {{
              font-size: 28px;
              font-weight: 800;
              letter-spacing: -0.02em;
            }}
    
            .layout {{
              display: grid;
              gap: 18px;
            }}
    
            .card {{
              background: var(--card);
              border: 1px solid var(--border);
              border-radius: 22px;
              padding: 22px;
              box-shadow: var(--shadow);
            }}
    
            .section-header {{
              display: flex;
              justify-content: space-between;
              align-items: baseline;
              gap: 12px;
              flex-wrap: wrap;
              margin-bottom: 16px;
            }}
    
            .section-header h2 {{
              margin: 0;
              font-size: 24px;
              font-weight: 800;
            }}
    
            .section-subtitle {{
              color: var(--muted);
              font-size: 13px;
            }}
    
            .subsection {{
              margin-top: 18px;
            }}
    
            .subsection h3 {{
              margin: 0 0 10px 0;
              font-size: 18px;
            }}
    
            .table-wrap {{
              width: 100%;
              overflow-x: auto;
              border: 1px solid var(--border);
              border-radius: 16px;
            }}
    
            .styled-table {{
              width: 100%;
              border-collapse: collapse;
              min-width: 900px;
              background: white;
            }}
    
            .styled-table th,
            .styled-table td {{
              padding: 12px 14px;
              text-align: left;
              border-bottom: 1px solid var(--border);
              vertical-align: top;
              font-size: 14px;
            }}
    
            .styled-table th {{
              background: #f8fafc;
              color: #334155;
              font-size: 12px;
              text-transform: uppercase;
              letter-spacing: 0.04em;
            }}
    
            .styled-table tr:hover td {{
              background: #fafcff;
            }}
    
            .num {{
              text-align: right;
              font-variant-numeric: tabular-nums;
            }}
    
            .badge {{
              display: inline-block;
              padding: 4px 10px;
              border-radius: 999px;
              font-size: 12px;
              font-weight: 700;
              white-space: nowrap;
            }}
    
            .badge-positive {{
              background: var(--pos-bg);
              color: var(--pos-text);
            }}
    
            .badge-negative {{
              background: var(--neg-bg);
              color: var(--neg-text);
            }}
    
            .badge-neutral {{
              background: var(--neu-bg);
              color: var(--neu-text);
            }}
    
            .ticker {{
              font-weight: 800;
              letter-spacing: 0.03em;
            }}
    
            .empty-state {{
              color: var(--muted);
              background: #f8fafc;
              border: 1px dashed var(--border);
              border-radius: 16px;
              padding: 18px;
            }}
    
            .details-block {{
              margin-top: 10px;
            }}
    
            .details-block summary {{
              cursor: pointer;
              font-weight: 700;
              color: var(--accent);
            }}
    
            .top-gap {{
              margin-top: 14px;
            }}
    
            .title-cell {{
              max-width: 520px;
              min-width: 320px;
            }}
    
            @media (max-width: 900px) {{
              .container {{ padding: 14px; }}
              .hero {{ padding: 20px; }}
              .hero h1 {{ font-size: 26px; }}
              .metric-value {{ font-size: 24px; }}
              .section-header h2 {{ font-size: 22px; }}
            }}
          </style>
        </head>
        <body>
          <div class="container">
            <section class="hero">
              <h1>{html.escape(stub)}</h1>
              <div class="hero-meta">
                <span><strong>Type:</strong> 24H report</span>
                <span><strong>Window start:</strong> {html.escape(str(summary.get("window_start", "N/A")))}</span>
                <span><strong>Window end:</strong> {html.escape(str(summary.get("window_end", "N/A")))}</span>
              </div>
            </section>
    
            <nav class="topnav">
              <a href="#predictions">Predictions</a>
              <a href="#us-news">U.S. News</a>
              <a href="#uk-news">U.K. News</a>
              <a href="#global-news">Global News</a>
              <a href="#reddit-country">Reddit by Country</a>
              <a href="#transparency">Transparency</a>
            </nav>
    
            <section class="metrics">
              <div class="metric">
                <div class="metric-label">Items</div>
                <div class="metric-value">{html.escape(str(summary.get("article_count", 0)))}</div>
              </div>
              <div class="metric">
                <div class="metric-label">Canonical items</div>
                <div class="metric-value">{html.escape(str(summary.get("canonical_article_count", 0)))}</div>
              </div>
              <div class="metric">
                <div class="metric-label">Avg market sentiment</div>
                <div class="metric-value">{fmt_metric(summary.get("avg_market_sentiment"))}</div>
              </div>
              <div class="metric">
                <div class="metric-label">Positive share</div>
                <div class="metric-value">{fmt_metric(summary.get("positive_share"), pct=True)}</div>
              </div>
              <div class="metric">
                <div class="metric-label">Negative share</div>
                <div class="metric-value">{fmt_metric(summary.get("negative_share"), pct=True)}</div>
              </div>
              <div class="metric">
                <div class="metric-label">Neutral share</div>
                <div class="metric-value">{fmt_metric(summary.get("neutral_share"), pct=True)}</div>
              </div>
            </section>
    
            <div class="layout">
              {pred}
              {news_sections}
              {reddit_sections}
              {transparency}
            </div>
          </div>
        </body>
        </html>
        """

    def _store_hourly_manifest(self, report_day_key: str, stub: str) -> None:
        manifest = self.state.hourly_manifest.setdefault(report_day_key, [])
        if stub not in manifest:
            manifest.append(stub)
            manifest.sort()

    def _save_hourly_report(self, df: pd.DataFrame, summary: Dict[str, Any], prediction_block: Dict[str, Any], transparency_rows: List[Dict[str, Any]]) -> None:
        stub = f"Report_{self.active_end.strftime('%Y-%m-%d')}_{self.active_end.strftime('%H')}"
        reports, articles = self._day_dirs(self.active_end)
    
        write_df = df.copy()
        if not write_df.empty:
            write_df["topics"] = write_df["topics"].apply(lambda x: ", ".join(x) if isinstance(x, list) else str(x))
            write_df["coverage_tags"] = write_df["coverage_tags"].apply(lambda x: ", ".join(x) if isinstance(x, list) else str(x))
        write_df.to_csv(articles / f"{stub}.csv", index=False)
    
        summary["news_sections_html"] = self._build_news_sections_html(df)
        summary["reddit_sections_html"] = self._build_reddit_sections_html(df)
    
        (reports / f"{stub}.html").write_text(
            self._build_hourly_dashboard_html(stub, summary, prediction_block, transparency_rows, df),
            encoding="utf-8",
        )
    
        report_day_key = (self.active_end - timedelta(hours=1)).astimezone(AMSTERDAM_TZ).strftime("%Y-%m-%d")
        self._store_hourly_manifest(report_day_key, stub)

    def _parse_window_end_from_stub(self, stub: str) -> Optional[datetime]:
        m = re.match(r"^Report_(\d{4}-\d{2}-\d{2})_(\d{2})$", stub)
        if not m:
            return None
        try:
            return datetime.fromisoformat(f"{m.group(1)}T{m.group(2)}:00:00").replace(tzinfo=AMSTERDAM_TZ)
        except Exception:
            return None

    def _write_daily_24h_report_if_ready(self, just_closed_end: datetime) -> None:
        if just_closed_end.hour != 0:
            return
        day_to_report = (just_closed_end - timedelta(days=1)).strftime("%Y-%m-%d")
        hourly_stubs = self.state.hourly_manifest.get(day_to_report, [])
        if len(hourly_stubs) < 24:
            self.logger.warning("Skipping 24H report for %s because only %d hourly reports are stored.", day_to_report, len(hourly_stubs))
            return
        frames: List[pd.DataFrame] = []
        for stub in hourly_stubs:
            end_dt = self._parse_window_end_from_stub(stub)
            if end_dt is None:
                continue
            _, articles_dir = self._day_dirs(end_dt)
            csv_path = articles_dir / f"{stub}.csv"
            if csv_path.exists():
                try:
                    df = pd.read_csv(csv_path)
                    if not df.empty:
                        frames.append(df)
                except Exception:
                    continue
        if frames:
            df24 = pd.concat(frames, ignore_index=True)
        else:
            df24 = pd.DataFrame()

        if not df24.empty:
            if "canonical_id" in df24.columns:
                canonical_count = int(df24["canonical_id"].nunique())
            else:
                canonical_count = int(len(df24))
            avg_market = float(df24["market_sentiment_score"].mean()) if "market_sentiment_score" in df24.columns else None
            pos_share = float((df24["market_sentiment_label"] == "positive").mean()) if "market_sentiment_label" in df24.columns else 0.0
            neg_share = float((df24["market_sentiment_label"] == "negative").mean()) if "market_sentiment_label" in df24.columns else 0.0
            neu_share = float((df24["market_sentiment_label"] == "neutral").mean()) if "market_sentiment_label" in df24.columns else 0.0
            if "topics" in df24.columns:
                df24["topics"] = df24["topics"].apply(lambda x: [t.strip() for t in str(x).split(",") if t.strip()])
        else:
            canonical_count = 0
            avg_market = None
            pos_share = neg_share = neu_share = 0.0

        window_start = datetime.fromisoformat(f"{day_to_report}T00:00:00").replace(tzinfo=AMSTERDAM_TZ)
        window_end = window_start + timedelta(days=1)
        summary24 = {
            "window_type": "daily_24h",
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "article_count": int(len(df24)),
            "canonical_article_count": canonical_count,
            "avg_market_sentiment": avg_market,
            "positive_share": pos_share,
            "negative_share": neg_share,
            "neutral_share": neu_share,
        }
        prediction_block = self._build_prediction_block(df24, cycle="24h", window_end=window_end)
        summary24["prediction_block"] = prediction_block
        summary24["news_sections_html"] = self._build_news_sections_html(df24)
        summary24["reddit_sections_html"] = self._build_reddit_sections_html(df24)
        transparency_rows = (
            df24[["title", "source_type", "final_sentiment_label", "market_sentiment_label", "sensitivity_analysis_score", "sentiment_model_used", "sentiment_model_note"]]
            .fillna("")
            .to_dict(orient="records")
            if not df24.empty and all(c in df24.columns for c in ["title", "source_type", "final_sentiment_label", "market_sentiment_label", "sensitivity_analysis_score", "sentiment_model_used", "sentiment_model_note"])
            else []
        )
        reports_dir, _ = self._day_dirs(window_end)
        stub24 = f"Report_24h_{day_to_report}"
        (reports_dir / f"{stub24}.html").write_text(
            self._build_24h_dashboard_html(stub24, summary24, prediction_block, transparency_rows, df24),
            encoding="utf-8",
        )
        self.logger.info("24H report finalized: %s", stub24)

    def _finalize(self) -> Dict[str, Any]:
        df = self._build_df()
        summary = {
            "window_start": self.active_start.isoformat(),
            "window_end": self.active_end.isoformat(),
            "article_count": int(len(df)),
            "canonical_article_count": int(df["canonical_id"].nunique()) if not df.empty else 0,
            "avg_market_sentiment": float(df["market_sentiment_score"].mean()) if not df.empty else None,
            "source_stats": self.source_stats,
            "positive_share": float((df["market_sentiment_label"] == "positive").mean()) if not df.empty else 0.0,
            "negative_share": float((df["market_sentiment_label"] == "negative").mean()) if not df.empty else 0.0,
            "neutral_share": float((df["market_sentiment_label"] == "neutral").mean()) if not df.empty else 0.0,
        }
        prediction_block = self._build_prediction_block(df, cycle="hourly", window_end=self.active_end)
        summary["prediction_block"] = prediction_block
        transparency_rows = (
            df[["title", "source_type", "final_sentiment_label", "market_sentiment_label", "sensitivity_analysis_score", "sentiment_model_used", "sentiment_model_note"]]
            .fillna("")
            .to_dict(orient="records")
            if not df.empty else []
        )
        self._save_hourly_report(df, summary, prediction_block, transparency_rows)
        self._write_daily_24h_report_if_ready(self.active_end)

        self.state.completed_windows.add(self.active_end.isoformat())
        self.state.run_history.append(summary)
        self.state.persist()

        stub = f"Report_{self.active_end.strftime('%Y-%m-%d')}_{self.active_end.strftime('%H')}"
        self.logger.info("Window finalized: %s", stub)
        self.buffer = []
        self.buffer_urls = set()
        self.source_stats = {}
        self.active_start = self.active_end
        self.active_end = self.active_start + timedelta(hours=1)
        gc.collect()
        return summary

    def run_forever(self, run_once: bool = False) -> None:
        self.logger.info("Started scraper. Active window: %s -> %s", self.active_start.isoformat(), self.active_end.isoformat())
        while True:
            try:
                if amsterdam_now() >= self.active_end and self.active_end.isoformat() not in self.state.completed_windows:
                    self._finalize()
                    if run_once:
                        return
                    continue
                for item in self._discover():
                    try:
                        processed = self._process_item(item)
                        if processed:
                            self.buffer.append(processed)
                    except Exception as exc:
                        self.logger.error("Item processing error: %s", exc)
                        self.logger.error(traceback.format_exc())
                sleep_for = min(POLL_INTERVAL_SECONDS, max(1, int((self.active_end - amsterdam_now()).total_seconds())))
                self.logger.info(
                    "Active window still open. Sleeping %s seconds until next poll. Window closes at %s",
                    sleep_for,
                    self.active_end.isoformat(),
                )
                if run_once:
                    self._finalize()
                    return
                time.sleep(sleep_for)
            except KeyboardInterrupt:
                self.logger.info("Stopped by user.")
                self.state.persist()
                break
            except Exception as exc:
                self.logger.error("Loop failure: %s", exc)
                self.logger.error(traceback.format_exc())
                self.state.persist()
                time.sleep(20)


def default_sources() -> List[FeedSource]:
    return [FeedSource(**cfg) for cfg in SOURCE_FEEDS + REDDIT_SOURCES]


def load_sources_from_file(path: Optional[str], logger: Optional[logging.Logger] = None) -> List[FeedSource]:
    if not path:
        return default_sources()
    p = Path(path)
    if not p.exists():
        if logger:
            logger.warning("Feeds file not found: %s. Falling back to defaults.", path)
        return default_sources()
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError("Feeds file must contain a list of sources")
        sources: List[FeedSource] = []
        for row in payload:
            if not isinstance(row, dict):
                continue
            if row.get("source_type") not in {"news", "reddit"}:
                continue
            if not row.get("feed_url", "").startswith(("http://", "https://")):
                continue
            sources.append(
                FeedSource(
                    publisher=row.get("publisher", "Unknown"),
                    country=row.get("country", "Unknown"),
                    region=row.get("region", ""),
                    feed_name=row.get("feed_name", row.get("publisher", "feed")),
                    feed_url=row["feed_url"],
                    source_type=row["source_type"],
                    priority=row.get("priority", "tier2"),
                    publisher_class=row.get("publisher_class", ""),
                    coverage_tags=row.get("coverage_tags", []),
                    subreddit=row.get("subreddit", ""),
                    community_type=row.get("community_type", ""),
                )
            )
        return sources or default_sources()
    except Exception:
        if logger:
            logger.warning("Could not parse feeds file: %s. Falling back to defaults.", path)
        return default_sources()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Continuous news + Reddit sentiment scraper")
    parser.add_argument("--run-once", action="store_true", help="Collect current window once and finalize immediately")
    parser.add_argument("--feeds-file", default=None, help="Optional JSON file with custom feeds list")
    args, _ = parser.parse_known_args()

    maybe_mount_drive()
    output_root = default_output_root()
    sources = load_sources_from_file(args.feeds_file)
    scraper = Scraper(output_root, sources=sources)
    scraper.run_forever(run_once=args.run_once)
