
# -*- coding: utf-8 -*-
"""
Colab-ready continuous global news and Reddit sentiment aggregator.

What it does:
- Mounts Google Drive at /content/drive
- Writes all outputs under: /content/drive/My Drive/Global News/
- Polls major US and UK news sources plus major global Reddit news/finance subreddits
- Collects items continuously throughout each Amsterdam hour
- Finalizes the report only at the end of the hour
- Computes article/post sentiment and market-direction sentiment
- Produces one hourly dashboard report in Amsterdam time with file name:
  Report_currentday_currenthour

Example:
- The 21 report covers 20:00:00 <= published_at < 21:00:00 Amsterdam time.
- Output names: Report_2026-04-08_21.html/.md/.json/.csv

Designed to run continuously in Google Colab.
"""

import os
import re
import gc
import sys
import json
import time
import math
import html
import hashlib
import logging
import traceback
import subprocess
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Tuple
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

AMSTERDAM_TZ = ZoneInfo("Europe/Amsterdam")
UTC = timezone.utc
DRIVE_ROOT = "/content/drive/My Drive/Global News/"

REQUIRED_PACKAGES = [
    "feedparser",
    "pandas",
    "numpy",
    "requests",
    "beautifulsoup4",
    "trafilatura",
    "lxml",
    "python-dateutil",
    "transformers",
    "torch",
    "sentencepiece",
]

POLL_INTERVAL_SECONDS = 180
REQUEST_TIMEOUT = 25
MAX_ARTICLES_PER_FEED = 100
MAX_ITEMS_PER_WINDOW = 1500


def _in_colab() -> bool:
    return "google.colab" in sys.modules


def install_packages() -> None:
    import importlib.util

    missing = []
    package_to_import = {
        "beautifulsoup4": "bs4",
        "python-dateutil": "dateutil",
    }

    for package in REQUIRED_PACKAGES:
        import_name = package_to_import.get(package, package)
        if importlib.util.find_spec(import_name) is None:
            missing.append(package)

    if missing:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q"] + missing)


def mount_google_drive() -> str:
    if _in_colab():
        from google.colab import drive
        drive.mount("/content/drive", force_remount=False)
    os.makedirs(DRIVE_ROOT, exist_ok=True)
    for folder in ["logs", "state", "reports", "articles"]:
        os.makedirs(os.path.join(DRIVE_ROOT, folder), exist_ok=True)
    return DRIVE_ROOT


install_packages()

import feedparser
import numpy as np
import pandas as pd
import requests
import trafilatura
from bs4 import BeautifulSoup
from dateutil import parser as dateparser


class AmsterdamFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=AMSTERDAM_TZ)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()


def build_logger() -> logging.Logger:
    logger = logging.getLogger("global_news_aggregator")
    logger.setLevel(logging.INFO)
    logger.handlers = []

    formatter = AmsterdamFormatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %Z",
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(
        os.path.join(DRIVE_ROOT, "logs", "aggregator.log"),
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


mount_google_drive()
LOGGER = build_logger()


@dataclass
class AgentSpec:
    name: str
    responsibility: str


AGENTS = [
    AgentSpec("Chief Research Orchestrator Agent", "Owns the end-to-end Amsterdam-time scheduling, hour-window collection, and report finalization."),
    AgentSpec("Source Universe Agent", "Maintains the source list, feed registry, country mapping, and subreddit registry."),
    AgentSpec("Discovery Agent", "Polls RSS feeds and Reddit feeds continuously during the active hour and admits only items inside the active hour window."),
    AgentSpec("Web Extraction Agent", "Fetches article pages, extracts clean text, and preserves source metadata."),
    AgentSpec("Normalization Agent", "Standardizes timestamps, text, URLs, publisher labels, subreddit labels, and schema fields."),
    AgentSpec("Deduplication Agent", "Assigns canonical IDs and suppresses duplicate content in the reporting layer."),
    AgentSpec("Geo and Topic Agent", "Infers country, tags subjects, and enriches entities from titles and bodies."),
    AgentSpec("Sentiment Research Agent", "Produces both textual sentiment and market-direction sentiment with context-aware rule adjustments."),
    AgentSpec("Aggregation Agent", "Builds country, subject, and source rollups for the dashboard."),
    AgentSpec("Report Writer Agent", "Writes HTML, markdown, CSV, and JSON reports using Amsterdam-time names."),
    AgentSpec("Monitoring and State Agent", "Persists seen items, completed windows, and continuous-run health."),
]

SOURCE_FEEDS = [
    {"publisher": "Reuters", "country": "US", "feed_name": "Reuters via Google News", "feed_url": "https://news.google.com/rss/search?q=site:reuters.com&hl=en-US&gl=US&ceid=US:en", "domain_hint": "reuters.com", "priority": 1, "source_type": "news"},
    {"publisher": "Associated Press", "country": "US", "feed_name": "AP Top News", "feed_url": "https://apnews.com/hub/apf-topnews?output=rss", "domain_hint": "apnews.com", "priority": 1, "source_type": "news"},
    {"publisher": "CNBC", "country": "US", "feed_name": "CNBC Business", "feed_url": "https://www.cnbc.com/id/10001147/device/rss/rss.html", "domain_hint": "cnbc.com", "priority": 1, "source_type": "news"},
    {"publisher": "New York Times", "country": "US", "feed_name": "NYT Homepage", "feed_url": "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml", "domain_hint": "nytimes.com", "priority": 2, "source_type": "news"},
    {"publisher": "New York Times", "country": "US", "feed_name": "NYT Business", "feed_url": "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml", "domain_hint": "nytimes.com", "priority": 2, "source_type": "news"},
    {"publisher": "Fox News", "country": "US", "feed_name": "Fox Latest", "feed_url": "https://moxie.foxnews.com/google-publisher/latest.xml", "domain_hint": "foxnews.com", "priority": 3, "source_type": "news"},
    {"publisher": "BBC", "country": "UK", "feed_name": "BBC Front Page", "feed_url": "http://feeds.bbci.co.uk/news/rss.xml", "domain_hint": "bbc.com", "priority": 1, "source_type": "news"},
    {"publisher": "BBC", "country": "UK", "feed_name": "BBC Business", "feed_url": "http://feeds.bbci.co.uk/news/business/rss.xml", "domain_hint": "bbc.com", "priority": 1, "source_type": "news"},
    {"publisher": "The Guardian", "country": "UK", "feed_name": "Guardian World", "feed_url": "https://www.theguardian.com/world/rss", "domain_hint": "theguardian.com", "priority": 1, "source_type": "news"},
    {"publisher": "The Guardian", "country": "UK", "feed_name": "Guardian Business", "feed_url": "https://www.theguardian.com/uk/business/rss", "domain_hint": "theguardian.com", "priority": 1, "source_type": "news"},
    {"publisher": "Sky News", "country": "UK", "feed_name": "Sky Home", "feed_url": "https://feeds.skynews.com/feeds/rss/home.xml", "domain_hint": "news.sky.com", "priority": 1, "source_type": "news"},
    {"publisher": "Sky News", "country": "UK", "feed_name": "Sky Business", "feed_url": "https://feeds.skynews.com/feeds/rss/business.xml", "domain_hint": "news.sky.com", "priority": 1, "source_type": "news"},
    {"publisher": "Financial Times", "country": "UK", "feed_name": "FT via Google News", "feed_url": "https://news.google.com/rss/search?q=site:ft.com&hl=en-GB&gl=GB&ceid=GB:en", "domain_hint": "ft.com", "priority": 2, "source_type": "news"},
]

REDDIT_SOURCES = [
    {"publisher": "Reddit", "country": "Global", "feed_name": "r/news", "subreddit": "news", "feed_url": "https://www.reddit.com/r/news/new/.rss", "priority": 1, "source_type": "reddit"},
    {"publisher": "Reddit", "country": "Global", "feed_name": "r/worldnews", "subreddit": "worldnews", "feed_url": "https://www.reddit.com/r/worldnews/new/.rss", "priority": 1, "source_type": "reddit"},
    {"publisher": "Reddit", "country": "Global", "feed_name": "r/business", "subreddit": "business", "feed_url": "https://www.reddit.com/r/business/new/.rss", "priority": 1, "source_type": "reddit"},
    {"publisher": "Reddit", "country": "Global", "feed_name": "r/stocks", "subreddit": "stocks", "feed_url": "https://www.reddit.com/r/stocks/new/.rss", "priority": 1, "source_type": "reddit"},
    {"publisher": "Reddit", "country": "Global", "feed_name": "r/investing", "subreddit": "investing", "feed_url": "https://www.reddit.com/r/investing/new/.rss", "priority": 1, "source_type": "reddit"},
    {"publisher": "Reddit", "country": "Global", "feed_name": "r/economics", "subreddit": "economics", "feed_url": "https://www.reddit.com/r/economics/new/.rss", "priority": 1, "source_type": "reddit"},
    {"publisher": "Reddit", "country": "Global", "feed_name": "r/finance", "subreddit": "finance", "feed_url": "https://www.reddit.com/r/finance/new/.rss", "priority": 1, "source_type": "reddit"},
    {"publisher": "Reddit", "country": "Global", "feed_name": "r/geopolitics", "subreddit": "geopolitics", "feed_url": "https://www.reddit.com/r/geopolitics/new/.rss", "priority": 2, "source_type": "reddit"},
    {"publisher": "Reddit", "country": "Global", "feed_name": "r/europe", "subreddit": "europe", "feed_url": "https://www.reddit.com/r/europe/new/.rss", "priority": 2, "source_type": "reddit"},
    {"publisher": "Reddit", "country": "Global", "feed_name": "r/unitedkingdom", "subreddit": "unitedkingdom", "feed_url": "https://www.reddit.com/r/unitedkingdom/new/.rss", "priority": 2, "source_type": "reddit"},
    {"publisher": "Reddit", "country": "Global", "feed_name": "r/ukpolitics", "subreddit": "ukpolitics", "feed_url": "https://www.reddit.com/r/ukpolitics/new/.rss", "priority": 2, "source_type": "reddit"},
    {"publisher": "Reddit", "country": "Global", "feed_name": "r/politics", "subreddit": "politics", "feed_url": "https://www.reddit.com/r/politics/new/.rss", "priority": 2, "source_type": "reddit"},
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    )
}

TOPIC_RULES = {
    "markets": ["stock", "stocks", "share", "shares", "market", "markets", "bond", "yield", "equity", "equities", "nasdaq", "s&p", "dow", "ftse", "stoxx"],
    "macro": ["inflation", "gdp", "unemployment", "jobs", "pmi", "recession", "economy", "economics", "central bank", "fed", "ecb", "boe", "rate cut", "interest rate", "treasury"],
    "commodities": ["oil", "gold", "silver", "copper", "commodity", "commodities", "gas", "crude", "brent", "wti"],
    "geopolitics": ["sanctions", "war", "military", "ceasefire", "tariff", "diplomatic", "election", "missile", "conflict"],
    "technology": ["ai", "artificial intelligence", "chip", "semiconductor", "software", "cloud", "tech"],
    "companies": ["earnings", "revenue", "profit", "guidance", "ceo", "merger", "acquisition", "ipo", "buyback"],
    "policy": ["regulator", "regulation", "policy", "government", "treasury", "parliament", "congress", "white house", "ministry"],
    "reddit_discussion": ["reddit", "subreddit", "thread", "comment"],
}

ENTITY_RULES = {
    "GLD": ["gld", "spdr gold shares"],
    "Gold": ["gold", "bullion"],
    "Oil": ["oil", "crude", "brent", "wti"],
    "Federal Reserve": ["federal reserve", "fed"],
    "ECB": ["european central bank", "ecb"],
    "Bank of England": ["bank of england", "boe"],
    "US": ["united states", "u.s.", "usa", "america"],
    "UK": ["united kingdom", "u.k.", "britain", "british", "england", "uk"],
    "China": ["china", "beijing", "chinese"],
    "EU": ["european union", "eu", "brussels"],
    "Apple": ["apple inc", "apple"],
    "Microsoft": ["microsoft"],
    "Nvidia": ["nvidia"],
    "Tesla": ["tesla"],
}

COUNTRY_KEYWORDS = {
    "US": ["united states", "u.s.", "usa", "american", "america", "white house", "congress", "federal reserve", "wall street", "washington"],
    "UK": ["united kingdom", "u.k.", "britain", "british", "england", "london", "parliament", "bank of england"],
    "China": ["china", "chinese", "beijing", "shanghai", "hong kong"],
    "Japan": ["japan", "japanese", "tokyo", "boj", "bank of japan"],
    "Germany": ["germany", "german", "berlin", "bundesbank"],
    "France": ["france", "french", "paris"],
    "India": ["india", "indian", "new delhi", "mumbai"],
    "Canada": ["canada", "canadian", "ottawa", "toronto"],
    "Australia": ["australia", "australian", "canberra", "sydney", "rba"],
    "EU": ["european union", "euro area", "eurozone", "brussels", "european commission", "ecb"],
    "Russia": ["russia", "russian", "moscow", "kremlin"],
    "Ukraine": ["ukraine", "ukrainian", "kyiv", "kiev"],
    "Middle East": ["israel", "gaza", "iran", "saudi", "uae", "qatar", "middle east"],
}

SUBREDDIT_COUNTRY_HINTS = {
    "unitedkingdom": "UK",
    "ukpolitics": "UK",
    "europe": "EU",
    "politics": "US",
}

POSITIVE_TERMS = {
    "beat", "beats", "above expectations", "raises guidance", "upgrade", "approves", "approval", "record profit",
    "eases", "cools", "soft landing", "stimulus", "rebound", "recovery", "growth", "growth accelerates",
    "surge", "surges", "strong demand", "ceasefire", "deal reached", "settlement", "disinflation", "rate cuts",
}
NEGATIVE_TERMS = {
    "miss", "misses", "cuts guidance", "downgrade", "warning", "fraud", "default", "bankruptcy", "layoffs",
    "recession", "war", "tariff", "sanctions", "escalation", "inflation accelerates", "hot inflation", "rate hike",
    "slump", "drops", "falls", "loss", "probe", "investigation", "weak demand", "supply shock", "geopolitical risk",
}

MARKET_POSITIVE_PHRASES = {
    "lower inflation": 0.7,
    "inflation slows": 0.7,
    "inflation cools": 0.8,
    "fed cuts": 0.8,
    "rate cuts": 0.8,
    "ceasefire": 0.7,
    "beats expectations": 0.8,
    "raises guidance": 0.8,
    "stimulus": 0.6,
    "soft landing": 0.8,
    "deal reached": 0.6,
    "settlement": 0.4,
    "recovery": 0.4,
}
MARKET_NEGATIVE_PHRASES = {
    "hot inflation": -0.9,
    "inflation accelerates": -0.9,
    "rate hike": -0.8,
    "higher rates": -0.7,
    "tariff": -0.7,
    "sanctions": -0.5,
    "war": -1.0,
    "missile": -1.0,
    "escalation": -0.8,
    "recession": -0.9,
    "downgrade": -0.6,
    "cuts guidance": -0.8,
    "bankruptcy": -1.0,
    "default": -1.0,
    "oil prices surge": -0.6,
    "crude surges": -0.5,
}

COMMODITY_CONTEXT_RULES = {
    "gold rises": -0.25,
    "gold hits record": -0.25,
    "oil rises": -0.35,
    "crude rises": -0.35,
    "bond yields rise": -0.45,
    "yields jump": -0.5,
    "bond yields fall": 0.35,
    "yields ease": 0.35,
}


def amsterdam_now() -> datetime:
    return datetime.now(AMSTERDAM_TZ)


def floor_to_hour(dt: datetime) -> datetime:
    return dt.astimezone(AMSTERDAM_TZ).replace(minute=0, second=0, microsecond=0)


def report_stub(window_end: datetime) -> str:
    window_end = window_end.astimezone(AMSTERDAM_TZ)
    return f"Report_{window_end.strftime('%Y-%m-%d')}_{window_end.strftime('%H')}"


def clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    text = BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_url(url: str) -> str:
    url = (url or "").strip()
    return url.split("#")[0] if url else ""


def domain_from_url(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def hash_text(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8", errors="ignore")).hexdigest()


def parse_datetime(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = dateparser.parse(str(value))
        except Exception:
            return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(AMSTERDAM_TZ)


def safe_get(d: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    for key in keys:
        if key in d and d[key] not in [None, ""]:
            return d[key]
    return default


def in_window(dt: Optional[datetime], window_start: datetime, window_end: datetime) -> bool:
    if dt is None:
        return False
    dt = dt.astimezone(AMSTERDAM_TZ)
    return window_start <= dt < window_end


def primary_topic(topics: List[str]) -> str:
    return topics[0] if topics else "general"


class StateManager:
    def __init__(self, state_dir: str):
        self.state_dir = state_dir
        self.seen_file = os.path.join(state_dir, "seen_urls.json")
        self.history_file = os.path.join(state_dir, "run_history.json")
        self.completed_windows_file = os.path.join(state_dir, "completed_windows.json")
        self.seen_urls = self._load_json(self.seen_file, default={})
        self.run_history = self._load_json(self.history_file, default=[])
        self.completed_windows = set(self._load_json(self.completed_windows_file, default=[]))

    def _load_json(self, path: str, default: Any) -> Any:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return default
        return default

    def _save_json(self, path: str, obj: Any) -> None:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)

    def mark_seen(self, url: str, seen_at: datetime) -> None:
        self.seen_urls[url] = seen_at.astimezone(AMSTERDAM_TZ).isoformat()

    def is_seen(self, url: str) -> bool:
        return url in self.seen_urls

    def mark_completed_window(self, window_end: datetime) -> None:
        self.completed_windows.add(window_end.astimezone(AMSTERDAM_TZ).isoformat())

    def is_completed_window(self, window_end: datetime) -> bool:
        return window_end.astimezone(AMSTERDAM_TZ).isoformat() in self.completed_windows

    def append_run_history(self, record: Dict[str, Any]) -> None:
        self.run_history.append(record)
        self.run_history = self.run_history[-2000:]

    def persist(self) -> None:
        self._save_json(self.seen_file, self.seen_urls)
        self._save_json(self.history_file, self.run_history)
        self._save_json(self.completed_windows_file, sorted(self.completed_windows))


STATE = StateManager(os.path.join(DRIVE_ROOT, "state"))


class MarketSentimentEngine:
    def __init__(self):
        self.pipeline = None
        self.model_name = "ProsusAI/finbert"
        self._load()

    def _load(self) -> None:
        try:
            from transformers import pipeline
            self.pipeline = pipeline(
                task="text-classification",
                model=self.model_name,
                tokenizer=self.model_name,
                truncation=True,
                max_length=512,
                device=-1,
            )
            LOGGER.info("Loaded sentiment model: %s", self.model_name)
        except Exception as exc:
            LOGGER.warning("Could not load FinBERT; using rule-based fallback. Error: %s", exc)
            self.pipeline = None

    def _base_score(self, text: str) -> Tuple[str, float, float]:
        text = clean_text(text)
        if not text:
            return "neutral", 0.5, 0.0

        if self.pipeline is None:
            return self._rule_text_score(text)

        try:
            result = self.pipeline(text[:3500])[0]
            label = str(result["label"]).lower()
            if label not in ["positive", "negative", "neutral"]:
                if "pos" in label:
                    label = "positive"
                elif "neg" in label:
                    label = "negative"
                else:
                    label = "neutral"
            score = float(result["score"])
            raw = score if label == "positive" else (-score if label == "negative" else 0.0)
            return label, score, raw
        except Exception:
            return self._rule_text_score(text)

    def _rule_text_score(self, text: str) -> Tuple[str, float, float]:
        words = re.findall(r"[A-Za-z']+", text.lower())
        if not words:
            return "neutral", 0.5, 0.0
        pos = 0
        neg = 0
        lowered = text.lower()
        for term in POSITIVE_TERMS:
            if term in lowered:
                pos += 2
        for term in NEGATIVE_TERMS:
            if term in lowered:
                neg += 2
        raw = (pos - neg) / max(1.0, math.sqrt(len(words)))
        if raw > 0.15:
            label = "positive"
        elif raw < -0.15:
            label = "negative"
        else:
            label = "neutral"
        confidence = float(min(0.98, 0.50 + abs(raw)))
        return label, confidence, float(raw)

    def _market_adjustment(self, text: str, topics: List[str], entities: List[str]) -> float:
        lowered = text.lower()
        adjustment = 0.0
        for phrase, value in MARKET_POSITIVE_PHRASES.items():
            if phrase in lowered:
                adjustment += value
        for phrase, value in MARKET_NEGATIVE_PHRASES.items():
            if phrase in lowered:
                adjustment += value
        for phrase, value in COMMODITY_CONTEXT_RULES.items():
            if phrase in lowered:
                adjustment += value

        if "geopolitics" in topics:
            if any(x in lowered for x in ["ceasefire", "truce", "de-escalation"]):
                adjustment += 0.5
            if any(x in lowered for x in ["war", "escalation", "missile", "attack", "sanctions"]):
                adjustment -= 0.8

        if "macro" in topics:
            if any(x in lowered for x in ["inflation cools", "lower inflation", "cpi slows", "pce cools"]):
                adjustment += 0.8
            if any(x in lowered for x in ["inflation rises", "hot cpi", "hot inflation", "higher for longer"]):
                adjustment -= 0.8
            if any(x in lowered for x in ["rate cuts", "cut rates", "easing cycle"]):
                adjustment += 0.7
            if any(x in lowered for x in ["rate hike", "hikes rates", "tightening"]):
                adjustment -= 0.7

        if "companies" in topics or "technology" in topics:
            if any(x in lowered for x in ["beats", "beat expectations", "raises guidance", "buyback", "approval"]):
                adjustment += 0.6
            if any(x in lowered for x in ["misses", "cuts guidance", "probe", "investigation", "fraud", "layoffs"]):
                adjustment -= 0.6

        if "commodities" in topics:
            if any(x in lowered for x in ["oil rises", "crude rises", "brent rises", "gold surges"]):
                adjustment -= 0.25
            if any(x in lowered for x in ["oil falls", "crude falls", "gold falls"]):
                adjustment += 0.10

        if "Federal Reserve" in entities and any(x in lowered for x in ["dovish", "cuts", "pause"]):
            adjustment += 0.5
        if "Federal Reserve" in entities and any(x in lowered for x in ["hawkish", "hike", "higher rates"]):
            adjustment -= 0.5

        return float(np.clip(adjustment, -2.5, 2.5))

    def score(self, text: str, topics: Optional[List[str]] = None, entities: Optional[List[str]] = None) -> Dict[str, Any]:
        topics = topics or []
        entities = entities or []
        label, confidence, raw = self._base_score(text)
        market_adjustment = self._market_adjustment(text, topics, entities)
        market_raw = float(np.clip(0.55 * raw + 0.45 * market_adjustment, -1.5, 1.5))
        if market_raw > 0.12:
            market_label = "positive"
        elif market_raw < -0.12:
            market_label = "negative"
        else:
            market_label = "neutral"

        return {
            "text_label": label,
            "text_confidence": float(confidence),
            "text_raw_score": float(raw),
            "market_label": market_label,
            "market_raw_score": market_raw,
            "market_adjustment": float(market_adjustment),
        }


SENTIMENT_ENGINE = MarketSentimentEngine()


@dataclass
class ArticleRecord:
    article_id: str
    canonical_id: str
    source_type: str
    publisher: str
    country: str
    origin_country: str
    feed_name: str
    subreddit: str
    source_feed_url: str
    url: str
    domain: str
    title: str
    summary: str
    body: str
    authors: str
    published_at: Optional[str]
    updated_at: Optional[str]
    first_seen_at: str
    headline_sentiment_label: str
    headline_sentiment_score: float
    body_sentiment_label: str
    body_sentiment_score: float
    final_sentiment_label: str
    final_sentiment_score: float
    market_sentiment_label: str
    market_sentiment_score: float
    market_sentiment_adjustment: float
    confidence_score: float
    topics: List[str]
    entities: List[str]
    duplicate_group_size: int


class DiscoveryAgent:
    def fetch_feed_entries(self, source_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        LOGGER.info("Polling %s feed: %s", source_cfg["source_type"], source_cfg["feed_name"])
        parsed = feedparser.parse(source_cfg["feed_url"])
        entries = parsed.entries[:MAX_ARTICLES_PER_FEED]
        out = []
        for entry in entries:
            link = normalize_url(safe_get(entry, ["link", "id"], ""))
            if not link:
                continue
            summary = clean_text(safe_get(entry, ["summary", "description"], ""))
            title = clean_text(safe_get(entry, ["title"], ""))
            published_at = parse_datetime(safe_get(entry, ["published", "updated", "pubDate", "created"], None))
            updated_at = parse_datetime(safe_get(entry, ["updated", "published", "pubDate"], None))
            authors = clean_text(safe_get(entry, ["author"], ""))
            out.append(
                {
                    "source_type": source_cfg["source_type"],
                    "publisher": source_cfg["publisher"],
                    "country": source_cfg["country"],
                    "origin_country": source_cfg["country"],
                    "feed_name": source_cfg["feed_name"],
                    "subreddit": source_cfg.get("subreddit", ""),
                    "source_feed_url": source_cfg["feed_url"],
                    "title": title,
                    "summary": summary,
                    "url": link,
                    "published_at": published_at,
                    "updated_at": updated_at,
                    "authors": authors,
                }
            )
        return out

    def discover_candidates_in_window(self, window_start: datetime, window_end: datetime) -> List[Dict[str, Any]]:
        candidates: List[Dict[str, Any]] = []
        for source_cfg in SOURCE_FEEDS + REDDIT_SOURCES:
            try:
                entries = self.fetch_feed_entries(source_cfg)
                for entry in entries:
                    if not in_window(entry.get("published_at"), window_start, window_end):
                        continue
                    if STATE.is_seen(entry["url"]):
                        continue
                    candidates.append(entry)
            except Exception as exc:
                LOGGER.error("Feed failed: %s | %s", source_cfg["feed_url"], exc)

        candidates = sorted(
            candidates,
            key=lambda x: x.get("published_at") or window_start,
            reverse=False,
        )
        if len(candidates) > MAX_ITEMS_PER_WINDOW:
            candidates = candidates[:MAX_ITEMS_PER_WINDOW]
        LOGGER.info(
            "Discovered %d unseen candidates in active window %s -> %s",
            len(candidates),
            window_start.isoformat(),
            window_end.isoformat(),
        )
        return candidates


class WebExtractionAgent:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def fetch_html(self, url: str) -> Optional[str]:
        for attempt in range(3):
            try:
                response = self.session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
                if response.status_code == 200 and response.text:
                    return response.text
            except Exception:
                if attempt == 2:
                    return None
                time.sleep(1.5 * (attempt + 1))
        return None

    def extract_news_article(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        html_text = self.fetch_html(candidate["url"])
        title = candidate.get("title", "")
        summary = candidate.get("summary", "")
        body = ""
        authors = candidate.get("authors", "")
        if html_text:
            try:
                downloaded = trafilatura.extract(
                    html_text,
                    include_comments=False,
                    include_tables=False,
                    include_images=False,
                    output_format="json",
                    with_metadata=True,
                    url=candidate["url"],
                )
                if downloaded:
                    parsed = json.loads(downloaded)
                    body = clean_text(parsed.get("text", ""))
                    title = clean_text(parsed.get("title", "")) or title
                    authors = clean_text(parsed.get("author", "")) or authors
                    if not summary:
                        summary = clean_text(parsed.get("description", ""))
            except Exception:
                pass
            if not body:
                soup = BeautifulSoup(html_text, "html.parser")
                paragraphs = [clean_text(p.get_text(" ", strip=True)) for p in soup.find_all("p")]
                paragraphs = [p for p in paragraphs if p]
                body = " ".join(paragraphs[:50])
        return {
            **candidate,
            "title": clean_text(title),
            "summary": clean_text(summary),
            "body": clean_text(body),
            "authors": clean_text(authors),
            "domain": domain_from_url(candidate["url"]),
        }

    def extract_reddit_post(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        summary = clean_text(candidate.get("summary", ""))
        title = clean_text(candidate.get("title", ""))
        return {
            **candidate,
            "title": title,
            "summary": summary,
            "body": summary,
            "authors": clean_text(candidate.get("authors", "")),
            "domain": domain_from_url(candidate["url"]) or "reddit.com",
        }

    def extract(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        if candidate.get("source_type") == "reddit":
            return self.extract_reddit_post(candidate)
        return self.extract_news_article(candidate)


class NormalizationAgent:
    def normalize(self, item: Dict[str, Any]) -> Dict[str, Any]:
        first_seen = amsterdam_now()
        published_at = item.get("published_at")
        updated_at = item.get("updated_at")
        item["published_at"] = published_at.isoformat() if isinstance(published_at, datetime) else item.get("published_at")
        item["updated_at"] = updated_at.isoformat() if isinstance(updated_at, datetime) else item.get("updated_at")
        item["first_seen_at"] = first_seen.isoformat()
        item["title"] = clean_text(item.get("title", ""))
        item["summary"] = clean_text(item.get("summary", ""))
        item["body"] = clean_text(item.get("body", ""))
        item["authors"] = clean_text(item.get("authors", ""))
        item["url"] = normalize_url(item.get("url", ""))
        item["domain"] = item.get("domain", "") or domain_from_url(item["url"])
        item["subreddit"] = item.get("subreddit", "")
        item["origin_country"] = item.get("origin_country") or item.get("country", "Unknown")
        return item


class GeoTopicAgent:
    def detect_topics(self, text: str, subreddit: str = "") -> List[str]:
        lowered = text.lower()
        topics: List[str] = []
        for topic, keywords in TOPIC_RULES.items():
            if any(keyword in lowered for keyword in keywords):
                topics.append(topic)
        if subreddit in {"stocks", "investing", "finance"} and "markets" not in topics:
            topics.insert(0, "markets")
        if subreddit == "economics" and "macro" not in topics:
            topics.insert(0, "macro")
        if subreddit in {"geopolitics", "worldnews", "news"} and "geopolitics" not in topics and "policy" not in topics:
            topics.append("geopolitics")
        return topics or ["general"]

    def detect_entities(self, text: str) -> List[str]:
        lowered = text.lower()
        entities: List[str] = []
        for entity, aliases in ENTITY_RULES.items():
            if any(alias in lowered for alias in aliases):
                entities.append(entity)
        return entities

    def infer_country(self, source_type: str, publisher_country: str, subreddit: str, text: str) -> str:
        if source_type == "news":
            return publisher_country
        if subreddit in SUBREDDIT_COUNTRY_HINTS:
            return SUBREDDIT_COUNTRY_HINTS[subreddit]
        lowered = text.lower()
        scores: Dict[str, int] = {}
        for country, keywords in COUNTRY_KEYWORDS.items():
            score = sum(1 for keyword in keywords if keyword in lowered)
            if score > 0:
                scores[country] = score
        if scores:
            return sorted(scores.items(), key=lambda x: (-x[1], x[0]))[0][0]
        return "Global/Unknown"

    def enrich(self, item: Dict[str, Any]) -> Dict[str, Any]:
        text = " ".join([item.get("title", ""), item.get("summary", ""), item.get("body", "")]).strip()
        subreddit = item.get("subreddit", "")
        item["topics"] = self.detect_topics(text, subreddit=subreddit)
        item["entities"] = self.detect_entities(text)
        item["origin_country"] = self.infer_country(item.get("source_type", "news"), item.get("country", "Unknown"), subreddit, text)
        return item


class DeduplicationAgent:
    def canonical_key(self, item: Dict[str, Any]) -> str:
        base = clean_text(item.get("title", "")).lower()
        if not base:
            base = normalize_url(item.get("url", ""))
        base = re.sub(r"[^a-z0-9 ]", " ", base)
        base = re.sub(r"\s+", " ", base).strip()
        return hash_text(base)

    def assign_canonical_ids(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for item in items:
            cid = self.canonical_key(item)
            item["canonical_id"] = cid
            groups.setdefault(cid, []).append(item)
        for _, grouped in groups.items():
            size = len(grouped)
            for item in grouped:
                item["duplicate_group_size"] = size
        return items


class SentimentResearchAgent:
    def enrich(self, item: Dict[str, Any]) -> Dict[str, Any]:
        topics = item.get("topics", [])
        entities = item.get("entities", [])
        headline_text = item.get("title", "")
        body_text = item.get("body", "") or item.get("summary", "")

        head = SENTIMENT_ENGINE.score(headline_text, topics=topics, entities=entities)
        body = SENTIMENT_ENGINE.score(body_text, topics=topics, entities=entities)

        final_text_raw = 0.40 * head["text_raw_score"] + 0.60 * body["text_raw_score"]
        final_market_raw = 0.40 * head["market_raw_score"] + 0.60 * body["market_raw_score"]
        final_market_adj = 0.40 * head["market_adjustment"] + 0.60 * body["market_adjustment"]

        if final_text_raw > 0.10:
            final_text_label = "positive"
        elif final_text_raw < -0.10:
            final_text_label = "negative"
        else:
            final_text_label = "neutral"

        if final_market_raw > 0.12:
            market_label = "positive"
        elif final_market_raw < -0.12:
            market_label = "negative"
        else:
            market_label = "neutral"

        confidence = float(np.clip((head["text_confidence"] + body["text_confidence"]) / 2.0, 0.0, 1.0))
        item.update(
            {
                "headline_sentiment_label": head["text_label"],
                "headline_sentiment_score": float(head["text_raw_score"]),
                "body_sentiment_label": body["text_label"],
                "body_sentiment_score": float(body["text_raw_score"]),
                "final_sentiment_label": final_text_label,
                "final_sentiment_score": float(final_text_raw),
                "market_sentiment_label": market_label,
                "market_sentiment_score": float(final_market_raw),
                "market_sentiment_adjustment": float(final_market_adj),
                "confidence_score": confidence,
            }
        )
        return item


@dataclass
class ArticleRecord:
    article_id: str
    canonical_id: str
    source_type: str
    publisher: str
    country: str
    origin_country: str
    feed_name: str
    subreddit: str
    source_feed_url: str
    url: str
    domain: str
    title: str
    summary: str
    body: str
    authors: str
    published_at: Optional[str]
    updated_at: Optional[str]
    first_seen_at: str
    headline_sentiment_label: str
    headline_sentiment_score: float
    body_sentiment_label: str
    body_sentiment_score: float
    final_sentiment_label: str
    final_sentiment_score: float
    market_sentiment_label: str
    market_sentiment_score: float
    market_sentiment_adjustment: float
    confidence_score: float
    topics: List[str]
    entities: List[str]
    duplicate_group_size: int


class AggregationAgent:
    def to_dataframe(self, items: List[Dict[str, Any]]) -> pd.DataFrame:
        if not items:
            return pd.DataFrame()
        rows = []
        for item in items:
            rows.append(
                ArticleRecord(
                    article_id=hash_text(item["url"]),
                    canonical_id=item["canonical_id"],
                    source_type=item["source_type"],
                    publisher=item["publisher"],
                    country=item["country"],
                    origin_country=item.get("origin_country", item.get("country", "Unknown")),
                    feed_name=item["feed_name"],
                    subreddit=item.get("subreddit", ""),
                    source_feed_url=item["source_feed_url"],
                    url=item["url"],
                    domain=item["domain"],
                    title=item["title"],
                    summary=item["summary"],
                    body=item["body"],
                    authors=item["authors"],
                    published_at=item["published_at"],
                    updated_at=item["updated_at"],
                    first_seen_at=item["first_seen_at"],
                    headline_sentiment_label=item["headline_sentiment_label"],
                    headline_sentiment_score=item["headline_sentiment_score"],
                    body_sentiment_label=item["body_sentiment_label"],
                    body_sentiment_score=item["body_sentiment_score"],
                    final_sentiment_label=item["final_sentiment_label"],
                    final_sentiment_score=item["final_sentiment_score"],
                    market_sentiment_label=item["market_sentiment_label"],
                    market_sentiment_score=item["market_sentiment_score"],
                    market_sentiment_adjustment=item["market_sentiment_adjustment"],
                    confidence_score=item["confidence_score"],
                    topics=item["topics"],
                    entities=item["entities"],
                    duplicate_group_size=item.get("duplicate_group_size", 1),
                )
            )
        return pd.DataFrame([asdict(row) for row in rows])

    def build_summary(self, df: pd.DataFrame, window_start: datetime, window_end: datetime) -> Dict[str, Any]:
        if df.empty:
            return {
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "article_count": 0,
                "canonical_article_count": 0,
                "source_count": 0,
                "avg_market_sentiment": None,
                "by_source_type": [],
                "country_dashboards": {},
                "reddit_dashboard": {},
            }

        label_share = df["market_sentiment_label"].value_counts(normalize=True).to_dict()
        by_source_type = (
            df.groupby("source_type", dropna=False)
            .agg(
                article_count=("article_id", "count"),
                canonical_article_count=("canonical_id", "nunique"),
                avg_market_sentiment=("market_sentiment_score", "mean"),
                avg_text_sentiment=("final_sentiment_score", "mean"),
            )
            .reset_index()
            .sort_values("article_count", ascending=False)
            .to_dict(orient="records")
        )

        news_df = df[df["source_type"] == "news"].copy()
        reddit_df = df[df["source_type"] == "reddit"].copy()
        country_dashboards = self._build_country_dashboards(news_df)
        reddit_dashboard = self._build_reddit_dashboard(reddit_df)

        return {
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "article_count": int(len(df)),
            "canonical_article_count": int(df["canonical_id"].nunique()),
            "source_count": int(df["publisher"].nunique()),
            "avg_market_sentiment": float(df["market_sentiment_score"].mean()),
            "positive_share": float(label_share.get("positive", 0.0)),
            "negative_share": float(label_share.get("negative", 0.0)),
            "neutral_share": float(label_share.get("neutral", 0.0)),
            "by_source_type": by_source_type,
            "country_dashboards": country_dashboards,
            "reddit_dashboard": reddit_dashboard,
        }

    def _subject_table(self, subset: pd.DataFrame) -> List[Dict[str, Any]]:
        if subset.empty:
            return []
        rows = []
        for _, row in subset.iterrows():
            rows.append(
                {
                    "subject": primary_topic(row["topics"]),
                    "article_id": row["article_id"],
                    "canonical_id": row["canonical_id"],
                    "market_sentiment_score": row["market_sentiment_score"],
                }
            )
        sub_df = pd.DataFrame(rows)
        return (
            sub_df.groupby("subject")
            .agg(
                article_count=("article_id", "count"),
                canonical_article_count=("canonical_id", "nunique"),
                avg_market_sentiment=("market_sentiment_score", "mean"),
            )
            .reset_index()
            .sort_values(["article_count", "avg_market_sentiment"], ascending=[False, False])
            .to_dict(orient="records")
        )

    def _highlights(self, subset: pd.DataFrame, limit: int = 8) -> List[Dict[str, Any]]:
        if subset.empty:
            return []
        ordered = (
            subset.sort_values(
                ["confidence_score", "market_sentiment_score"],
                ascending=[False, False],
            )
            .drop_duplicates(subset=["canonical_id"])
            .head(limit)
        )
        cols = ["publisher", "feed_name", "subreddit", "origin_country", "title", "market_sentiment_score", "market_sentiment_label", "url", "published_at", "topics"]
        return ordered[cols].to_dict(orient="records")

    def _build_country_dashboards(self, news_df: pd.DataFrame) -> Dict[str, Any]:
        dashboards: Dict[str, Any] = {}
        for country in ["US", "UK"]:
            subset = news_df[news_df["country"] == country].copy()
            dashboards[country] = {
                "article_count": int(len(subset)),
                "canonical_article_count": int(subset["canonical_id"].nunique()) if not subset.empty else 0,
                "avg_market_sentiment": float(subset["market_sentiment_score"].mean()) if not subset.empty else None,
                "subjects": self._subject_table(subset),
                "top_items": self._highlights(subset),
            }
        return dashboards

    def _build_reddit_dashboard(self, reddit_df: pd.DataFrame) -> Dict[str, Any]:
        if reddit_df.empty:
            return {
                "article_count": 0,
                "canonical_article_count": 0,
                "avg_market_sentiment": None,
                "by_country": {},
                "by_subreddit": [],
                "top_items": [],
            }
        by_subreddit = (
            reddit_df.groupby("subreddit")
            .agg(
                article_count=("article_id", "count"),
                canonical_article_count=("canonical_id", "nunique"),
                avg_market_sentiment=("market_sentiment_score", "mean"),
            )
            .reset_index()
            .sort_values(["article_count", "avg_market_sentiment"], ascending=[False, False])
            .to_dict(orient="records")
        )
        by_country: Dict[str, Any] = {}
        for country, subset in reddit_df.groupby("origin_country"):
            subset = subset.copy()
            by_country[country] = {
                "article_count": int(len(subset)),
                "canonical_article_count": int(subset["canonical_id"].nunique()),
                "avg_market_sentiment": float(subset["market_sentiment_score"].mean()),
                "subjects": self._subject_table(subset),
                "top_items": self._highlights(subset, limit=6),
            }
        return {
            "article_count": int(len(reddit_df)),
            "canonical_article_count": int(reddit_df["canonical_id"].nunique()),
            "avg_market_sentiment": float(reddit_df["market_sentiment_score"].mean()),
            "by_country": by_country,
            "by_subreddit": by_subreddit,
            "top_items": self._highlights(reddit_df, limit=12),
        }


class ReportWriterAgent:
    def write_outputs(self, df: pd.DataFrame, summary: Dict[str, Any], window_start: datetime, window_end: datetime) -> Dict[str, str]:
        stub = report_stub(window_end)
        report_dir = os.path.join(DRIVE_ROOT, "reports")
        article_dir = os.path.join(DRIVE_ROOT, "articles")
        os.makedirs(report_dir, exist_ok=True)
        os.makedirs(article_dir, exist_ok=True)

        html_path = os.path.join(report_dir, f"{stub}.html")
        md_path = os.path.join(report_dir, f"{stub}.md")
        csv_path = os.path.join(article_dir, f"{stub}.csv")
        json_path = os.path.join(report_dir, f"{stub}.json")

        out_df = df.copy() if not df.empty else pd.DataFrame()
        if not out_df.empty:
            out_df["topics"] = out_df["topics"].apply(lambda x: ", ".join(x) if isinstance(x, list) else "")
            out_df["entities"] = out_df["entities"].apply(lambda x: ", ".join(x) if isinstance(x, list) else "")
        out_df.to_csv(csv_path, index=False)

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        markdown = self._build_markdown(summary, window_start, window_end)
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(markdown)

        html_report = self._build_html(summary, window_start, window_end)
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_report)

        return {"html": html_path, "markdown": md_path, "csv": csv_path, "json": json_path}

    def _build_markdown(self, summary: Dict[str, Any], window_start: datetime, window_end: datetime) -> str:
        lines = []
        lines.append(f"# {report_stub(window_end)}")
        lines.append("")
        lines.append(f"- Window start (Amsterdam): {window_start.isoformat()}")
        lines.append(f"- Window end (Amsterdam): {window_end.isoformat()}")
        lines.append(f"- Total items: {summary['article_count']}")
        lines.append(f"- Canonical items: {summary['canonical_article_count']}")
        lines.append(f"- Average market-direction sentiment: {summary['avg_market_sentiment']}")
        lines.append("")
        lines.append("## Source type summary")
        lines.append("")
        if summary["by_source_type"]:
            lines.append("| Source type | Items | Canonical | Avg market sentiment | Avg text sentiment |")
            lines.append("|---|---:|---:|---:|---:|")
            for row in summary["by_source_type"]:
                lines.append(f"| {row['source_type']} | {row['article_count']} | {row['canonical_article_count']} | {row['avg_market_sentiment']:.4f} | {row['avg_text_sentiment']:.4f} |")
        else:
            lines.append("No items.")
        lines.append("")

        for country, dashboard in summary["country_dashboards"].items():
            lines.append(f"## {country} News Dashboard")
            lines.append("")
            lines.append(f"- Items: {dashboard['article_count']}")
            lines.append(f"- Canonical items: {dashboard['canonical_article_count']}")
            lines.append(f"- Avg market-direction sentiment: {dashboard['avg_market_sentiment']}")
            lines.append("")
            lines.append("### Subjects")
            lines.append("")
            if dashboard["subjects"]:
                lines.append("| Subject | Items | Canonical | Avg market sentiment |")
                lines.append("|---|---:|---:|---:|")
                for row in dashboard["subjects"]:
                    lines.append(f"| {row['subject']} | {row['article_count']} | {row['canonical_article_count']} | {row['avg_market_sentiment']:.4f} |")
            else:
                lines.append("No items.")
            lines.append("")
            lines.append("### Highlights")
            lines.append("")
            if dashboard["top_items"]:
                for row in dashboard["top_items"]:
                    topic = primary_topic(row["topics"])
                    lines.append(f"- [{row['publisher']}] {row['title']} | subject={topic} | market={row['market_sentiment_score']:.4f} | {row['url']}")
            else:
                lines.append("No highlights.")
            lines.append("")

        reddit = summary["reddit_dashboard"]
        lines.append("## Reddit Dashboard")
        lines.append("")
        lines.append(f"- Items: {reddit['article_count']}")
        lines.append(f"- Canonical items: {reddit['canonical_article_count']}")
        lines.append(f"- Avg market-direction sentiment: {reddit['avg_market_sentiment']}")
        lines.append("")
        lines.append("### By subreddit")
        lines.append("")
        if reddit["by_subreddit"]:
            lines.append("| Subreddit | Items | Canonical | Avg market sentiment |")
            lines.append("|---|---:|---:|---:|")
            for row in reddit["by_subreddit"]:
                lines.append(f"| r/{row['subreddit']} | {row['article_count']} | {row['canonical_article_count']} | {row['avg_market_sentiment']:.4f} |")
        else:
            lines.append("No Reddit items.")
        lines.append("")
        for country, block in reddit.get("by_country", {}).items():
            lines.append(f"### Reddit country: {country}")
            lines.append("")
            lines.append(f"- Items: {block['article_count']}")
            lines.append(f"- Canonical items: {block['canonical_article_count']}")
            lines.append(f"- Avg market-direction sentiment: {block['avg_market_sentiment']}")
            lines.append("")
            if block["subjects"]:
                lines.append("| Subject | Items | Canonical | Avg market sentiment |")
                lines.append("|---|---:|---:|---:|")
                for row in block["subjects"]:
                    lines.append(f"| {row['subject']} | {row['article_count']} | {row['canonical_article_count']} | {row['avg_market_sentiment']:.4f} |")
            else:
                lines.append("No subject summary.")
            lines.append("")
            if block["top_items"]:
                for row in block["top_items"]:
                    topic = primary_topic(row["topics"])
                    subreddit = f"r/{row['subreddit']}" if row["subreddit"] else "reddit"
                    lines.append(f"- [{subreddit}] {row['title']} | subject={topic} | market={row['market_sentiment_score']:.4f} | country={row['origin_country']} | {row['url']}")
            else:
                lines.append("No highlights.")
            lines.append("")
        return "\n".join(lines)

    def _build_html(self, summary: Dict[str, Any], window_start: datetime, window_end: datetime) -> str:
        def metric_card(label: str, value: Any) -> str:
            return f'<div class="metric"><div class="metric-label">{html.escape(str(label))}</div><div class="metric-value">{html.escape(str(value))}</div></div>'

        def subjects_table(rows: List[Dict[str, Any]]) -> str:
            if not rows:
                return "<p>No items.</p>"
            tr = "".join(
                f"<tr><td>{html.escape(str(r['subject']))}</td><td>{r['article_count']}</td><td>{r['canonical_article_count']}</td><td>{r['avg_market_sentiment']:.4f}</td></tr>"
                for r in rows
            )
            return f"<table><thead><tr><th>Subject</th><th>Items</th><th>Canonical</th><th>Avg market sentiment</th></tr></thead><tbody>{tr}</tbody></table>"

        def highlights_list(rows: List[Dict[str, Any]]) -> str:
            if not rows:
                return "<p>No highlights.</p>"
            items = []
            for r in rows:
                source_label = f"r/{r['subreddit']}" if r.get("subreddit") else r["publisher"]
                topic = primary_topic(r["topics"])
                items.append(
                    f"<li><a href='{html.escape(r['url'])}' target='_blank'>{html.escape(r['title'])}</a> "
                    f"<span class='muted'>[{html.escape(source_label)} | {html.escape(str(r['origin_country']))} | {html.escape(topic)} | market={r['market_sentiment_score']:.4f}]</span></li>"
                )
            return "<ul>" + "".join(items) + "</ul>"

        cards = [
            metric_card("Window start", window_start.isoformat()),
            metric_card("Window end", window_end.isoformat()),
            metric_card("Total items", summary["article_count"]),
            metric_card("Canonical items", summary["canonical_article_count"]),
            metric_card("Avg market sentiment", f"{summary['avg_market_sentiment']:.4f}" if summary["avg_market_sentiment"] is not None else "None"),
            metric_card("Positive share", f"{summary.get('positive_share', 0.0):.2%}"),
            metric_card("Negative share", f"{summary.get('negative_share', 0.0):.2%}"),
            metric_card("Neutral share", f"{summary.get('neutral_share', 0.0):.2%}"),
        ]

        country_sections = []
        for country, dashboard in summary["country_dashboards"].items():
            country_sections.append(f"""
            <section class="panel">
              <h2>{html.escape(country)} News</h2>
              <div class="metrics">
                {metric_card("Items", dashboard["article_count"])}
                {metric_card("Canonical", dashboard["canonical_article_count"])}
                {metric_card("Avg market sentiment", f"{dashboard['avg_market_sentiment']:.4f}" if dashboard["avg_market_sentiment"] is not None else "None")}
              </div>
              <h3>Subjects</h3>
              {subjects_table(dashboard["subjects"])}
              <h3>Highlights</h3>
              {highlights_list(dashboard["top_items"])}
            </section>
            """)

        reddit = summary["reddit_dashboard"]
        if reddit.get("by_subreddit"):
            subreddit_rows = "".join(
                f"<tr><td>r/{html.escape(r['subreddit'])}</td><td>{r['article_count']}</td><td>{r['canonical_article_count']}</td><td>{r['avg_market_sentiment']:.4f}</td></tr>"
                for r in reddit["by_subreddit"]
            )
            subreddit_table = f"<table><thead><tr><th>Subreddit</th><th>Items</th><th>Canonical</th><th>Avg market sentiment</th></tr></thead><tbody>{subreddit_rows}</tbody></table>"
        else:
            subreddit_table = "<p>No Reddit items.</p>"

        reddit_country_blocks = []
        for country, block in reddit.get("by_country", {}).items():
            reddit_country_blocks.append(f"""
            <div class="subpanel">
              <h3>{html.escape(country)}</h3>
              <div class="metrics">
                {metric_card("Items", block["article_count"])}
                {metric_card("Canonical", block["canonical_article_count"])}
                {metric_card("Avg market sentiment", f"{block['avg_market_sentiment']:.4f}" if block["avg_market_sentiment"] is not None else "None")}
              </div>
              {subjects_table(block["subjects"])}
              {highlights_list(block["top_items"])}
            </div>
            """)

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>{html.escape(report_stub(window_end))}</title>
<style>
body {{ font-family: Arial, sans-serif; background: #f5f7fb; color: #1f2937; margin: 0; padding: 24px; }}
h1, h2, h3 {{ margin-top: 0; }}
.panel, .subpanel {{ background: white; border-radius: 14px; padding: 20px; margin-bottom: 20px; box-shadow: 0 6px 20px rgba(0,0,0,0.06); }}
.metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-bottom: 18px; }}
.metric {{ background: #eef3ff; border-radius: 12px; padding: 12px; }}
.metric-label {{ font-size: 12px; color: #475569; margin-bottom: 6px; }}
.metric-value {{ font-size: 18px; font-weight: 700; }}
table {{ width: 100%; border-collapse: collapse; margin-bottom: 14px; }}
th, td {{ text-align: left; padding: 10px; border-bottom: 1px solid #e5e7eb; }}
th {{ background: #f8fafc; }}
ul {{ padding-left: 20px; }}
li {{ margin-bottom: 8px; }}
.muted {{ color: #64748b; font-size: 0.92em; }}
.grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
@media (max-width: 960px) {{ .grid {{ grid-template-columns: 1fr; }} }}
</style>
</head>
<body>
<section class="panel">
  <h1>{html.escape(report_stub(window_end))}</h1>
  <div class="metrics">
    {''.join(cards)}
  </div>
</section>

<div class="grid">
  {''.join(country_sections)}
</div>

<section class="panel">
  <h2>Reddit</h2>
  <div class="metrics">
    {metric_card("Items", reddit.get("article_count", 0))}
    {metric_card("Canonical", reddit.get("canonical_article_count", 0))}
    {metric_card("Avg market sentiment", f"{reddit['avg_market_sentiment']:.4f}" if reddit.get("avg_market_sentiment") is not None else "None")}
  </div>
  <h3>By subreddit</h3>
  {subreddit_table}
  <h3>Reddit by inferred country</h3>
  {''.join(reddit_country_blocks) if reddit_country_blocks else '<p>No Reddit country sections.</p>'}
</section>
</body>
</html>"""


class NewsSentimentAggregator:
    def __init__(self):
        self.discovery_agent = DiscoveryAgent()
        self.web_extraction_agent = WebExtractionAgent()
        self.normalization_agent = NormalizationAgent()
        self.geo_topic_agent = GeoTopicAgent()
        self.dedup_agent = DeduplicationAgent()
        self.sentiment_agent = SentimentResearchAgent()
        self.aggregation_agent = AggregationAgent()
        self.report_writer = ReportWriterAgent()
        self.active_window_start = floor_to_hour(amsterdam_now())
        self.active_window_end = self.active_window_start + timedelta(hours=1)
        self.buffer: List[Dict[str, Any]] = []
        self.buffer_urls: set = set()

    def _collect_once(self) -> int:
        candidates = self.discovery_agent.discover_candidates_in_window(self.active_window_start, self.active_window_end)
        processed = 0
        for candidate in candidates:
            if candidate["url"] in self.buffer_urls:
                continue
            try:
                item = self.web_extraction_agent.extract(candidate)
                item = self.normalization_agent.normalize(item)
                published_at = parse_datetime(item.get("published_at"))
                if not in_window(published_at, self.active_window_start, self.active_window_end):
                    continue
                item = self.geo_topic_agent.enrich(item)
                item = self.sentiment_agent.enrich(item)
                self.buffer.append(item)
                self.buffer_urls.add(item["url"])
                STATE.mark_seen(item["url"], amsterdam_now())
                processed += 1
            except Exception as exc:
                LOGGER.error("Item processing failed: %s | %s", candidate.get("url"), exc)
                LOGGER.error(traceback.format_exc())
        if processed:
            LOGGER.info(
                "Collected %d new items for active window %s -> %s. Window buffer size=%d",
                processed,
                self.active_window_start.isoformat(),
                self.active_window_end.isoformat(),
                len(self.buffer),
            )
        return processed

    def _finalize_active_window(self) -> Dict[str, Any]:
        LOGGER.info(
            "Finalizing window %s -> %s with %d collected items.",
            self.active_window_start.isoformat(),
            self.active_window_end.isoformat(),
            len(self.buffer),
        )
        items = self.dedup_agent.assign_canonical_ids(self.buffer.copy())
        df = self.aggregation_agent.to_dataframe(items)
        summary = self.aggregation_agent.build_summary(df, self.active_window_start, self.active_window_end)
        paths = self.report_writer.write_outputs(df, summary, self.active_window_start, self.active_window_end)

        run_record = {
            "window_start": self.active_window_start.isoformat(),
            "window_end": self.active_window_end.isoformat(),
            "report_name": report_stub(self.active_window_end),
            "article_count": int(summary["article_count"]),
            "canonical_article_count": int(summary["canonical_article_count"]),
            "source_count": int(summary["source_count"]),
            "output_paths": paths,
        }
        STATE.append_run_history(run_record)
        STATE.mark_completed_window(self.active_window_end)
        STATE.persist()

        LOGGER.info(
            "Window complete. Report=%s | Items=%s | Canonical=%s",
            paths["html"],
            summary["article_count"],
            summary["canonical_article_count"],
        )
        gc.collect()
        return {"summary": summary, "paths": paths, "run_record": run_record}

    def _advance_window(self) -> None:
        self.active_window_start = self.active_window_end
        self.active_window_end = self.active_window_start + timedelta(hours=1)
        self.buffer = []
        self.buffer_urls = set()
        LOGGER.info(
            "Started collecting for new active window %s -> %s",
            self.active_window_start.isoformat(),
            self.active_window_end.isoformat(),
        )

    def run_forever(self) -> None:
        LOGGER.info("Continuous Amsterdam-time news, Reddit, and market-sentiment aggregation started.")
        LOGGER.info(
            "Current active window is %s -> %s",
            self.active_window_start.isoformat(),
            self.active_window_end.isoformat(),
        )
        while True:
            try:
                now = amsterdam_now()
                if now >= self.active_window_end and not STATE.is_completed_window(self.active_window_end):
                    self._finalize_active_window()
                    self._advance_window()
                    continue

                self._collect_once()

                now = amsterdam_now()
                seconds_to_boundary = max(1, int((self.active_window_end - now).total_seconds()))
                sleep_seconds = min(POLL_INTERVAL_SECONDS, seconds_to_boundary)
                LOGGER.info(
                    "Active window still open. Sleeping %d seconds until next poll. Window closes at %s",
                    sleep_seconds,
                    self.active_window_end.isoformat(),
                )
                time.sleep(sleep_seconds)
            except KeyboardInterrupt:
                LOGGER.info("Stopped by user.")
                STATE.persist()
                break
            except Exception as exc:
                LOGGER.error("Fatal loop error: %s", exc)
                LOGGER.error(traceback.format_exc())
                STATE.persist()
                time.sleep(30)


if __name__ == "__main__":
    LOGGER.info("Google Drive root: %s", DRIVE_ROOT)
    LOGGER.info("Registered AI agents:")
    for agent in AGENTS:
        LOGGER.info("- %s: %s", agent.name, agent.responsibility)

    aggregator = NewsSentimentAggregator()
    aggregator.run_forever()
