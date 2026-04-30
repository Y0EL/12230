import time
import random
import re
from typing import Optional
from urllib.parse import urlparse

import httpx
from langchain_core.tools import tool
from loguru import logger
from rich.console import Console
from rich.progress import (
    Progress, SpinnerColumn, BarColumn,
    TextColumn, MofNCompleteColumn, TimeElapsedColumn,
)

from backend.core.config import get_settings

_console = Console(stderr=False)
_settings = get_settings()

_ENGINE_TIMEOUT: dict[str, int] = {
    "google":     45,
    "baidu":      90,
    "yandex":     90,
    "yahoo":      45,   # Yahoo Japan — dominant in Japan
    "naver":      45,   # Naver — dominant in South Korea
    "bing":       45,
    "duckduckgo": 45,
}

SEARCH_TEMPLATES = [
    "{query} exhibitors list",
    "{query} vendors directory",
    "{query} expo participants 2025 2026",
    "{query} conference exhibitors",
    "{query} tradeshow companies list",
    "{query} summit sponsors vendors",
    "site:10times.com {query}",
    "site:expodatabase.com {query}",
    "{query} floor plan exhibitors",
    "{query} expo hall vendors directory",
    "{query} event exhibitor list",
    "{query} technology providers directory",
]

EVENT_SITE_DOMAINS = [
    "10times.com", "expodatabase.com", "eventbrite.com", "meetup.com",
    "tradeshow.com", "exhibitorsonline.com", "tsnn.com",
    "tradeshowtalk.com", "expodirectory.com", "biztradeshows.com",
    "allconferences.com", "confguide.com",
    "informa.com", "reedexhibitions.com", "ubmasia.com",
    "spargo.com", "a2zinc.net", "eventinterface.com",
    "swapcard.com", "stova.io", "eventsair.com", "cvent.com",
    "globex360.com", "afcea.org", "isaca.org", "sans.org",
    "gartner.com", "idcevents.com", "rsaconference.com",
    "blackhat.com", "defcon.org", "cyberuk.uk",
]

CYBERSECURITY_EVENT_KEYWORDS = [
    "cyber", "cybersecurity", "infosec", "information security",
    "defense", "defence", "military", "nato", "security",
    "intelligence", "surveillance", "disa", "darpa",
    "homeland", "critical infrastructure", "threat", "soc",
    "pentest", "red team", "blue team", "ciso",
]

_REGION_MAP_BASE = [
    {
        "keywords": ["china", "cina", "tiongkok", "beijing", "shanghai", "shenzhen", "guangzhou"],
        "engine": "baidu",
        "extra_params": {},
        "label": "China",
    },
    {
        "keywords": ["jepang", "japan", "tokyo", "osaka", "kyoto", "nagoya", "fukuoka"],
        "engine": "yahoo",
        "extra_params": {},
        "label": "Japan",
    },
    {
        "keywords": ["korea", "korean", "seoul", "busan", "incheon", "daegu", "daejeon"],
        "engine": "naver",
        "extra_params": {},
        "label": "Korea",
    },
    {
        "keywords": [
            "usa", "us ", " us,", "america", "united states", "amerika",
            "new york", "washington", "los angeles", "san francisco", "chicago",
        ],
        "engine": "google",
        "extra_params": {"gl": "us", "hl": "en"},
        "label": "USA",
    },
    {
        "keywords": [
            "eropa", "europe", "european", "eu ", "german", "jerman", "prancis", "france",
            "inggris", "uk ", "belanda", "netherlands", "italy", "italia", "spain", "spanyol",
            "sweden", "swedia", "poland", "polandia", "brussels",
        ],
        "engine": "google",
        "extra_params": {},
        "label": "Europe",
    },
    {
        "keywords": ["greece", "greek", "yunani", "athens", "athena", "hellenic"],
        "engine": "google",
        "extra_params": {"gl": "gr", "hl": "el"},
        "label": "Greece",
    },
    {
        "keywords": ["russia", "rusia", "russian", "moskow", "moscow", "st. petersburg"],
        "engine": "yandex",
        "extra_params": {},
        "label": "Russia",
    },
    {
        "keywords": ["india", "mumbai", "new delhi", "delhi", "bangalore", "bengaluru", "hyderabad"],
        "engine": "google",
        "extra_params": {"gl": "in", "hl": "en"},
        "label": "India",
    },
    {
        "keywords": ["pakistan", "karachi", "islamabad", "lahore", "pakistani"],
        "engine": "google",
        "extra_params": {"gl": "pk", "hl": "en"},
        "label": "Pakistan",
    },
    {
        "keywords": [
            "singapura", "singapore", "asia tenggara", "southeast asia", "asean",
            "malaysia", "thailand", "filipina", "philippines", "vietnam",
        ],
        "engine": "google",
        "extra_params": {"gl": "sg", "hl": "en"},
        "label": "Southeast Asia",
    },
    {
        "keywords": [
            "oceania", "australia", "new zealand", "auckland", "sydney", "melbourne",
            "pacific", "pasifik", "papua",
        ],
        "engine": "google",
        "extra_params": {"gl": "au", "hl": "en"},
        "label": "Oceania",
    },
    {
        "keywords": [
            "arab", "arabic", "saudi", "dubai", "uae", "timur tengah", "middle east",
            "qatar", "kuwait", "bahrain", "oman", "abu dhabi",
        ],
        "engine": "google",
        "extra_params": {"gl": "ae", "hl": "ar"},
        "label": "Middle East",
    },
    {
        "keywords": ["asia ", " asia", ",asia", "asian"],
        "engine": "google",
        "extra_params": {},
        "label": "Asia",
    },
]

def _get_shuffled_region_map() -> list[dict]:
    """Shuffle region map to remove China-first bias. Return randomized copy."""
    shuffled = list(_REGION_MAP_BASE)
    random.shuffle(shuffled)
    return shuffled

REGION_MAP = _get_shuffled_region_map()

GLOBAL_KEYWORDS = [
    "global", "worldwide", "world", "seluruh dunia", "all regions",
    "covers over", "international", "internasional",
]

_GLOBAL_REGION: dict = {"engine": "google", "extra_params": {}, "label": "Global"}

_openserp_available: Optional[bool] = None


def _check_openserp() -> bool:
    """
    Cek apakah OpenSERP server nyala dengan ping root endpoint.
    Tidak melakukan real search — hanya cek konektivitas.
    Cache hasil per proses; di-reset oleh clear_openserp_cache() tiap run baru.
    """
    global _openserp_available
    if _openserp_available is not None:
        return _openserp_available
    if not _settings.openserp_enabled:
        _openserp_available = False
        return False
    base = _settings.openserp_base_url
    # Coba beberapa endpoint ringan — root atau health check
    for path in ("/", "/health", "/google/search"):
        try:
            params = {"text": "ping", "limit": 1} if "search" in path else {}
            resp = httpx.get(f"{base}{path}", params=params, timeout=4)
            # Server nyala = status apapun selain connection error
            _openserp_available = True
            logger.debug(f"[OPENSERP] Tersedia di {base} (via {path}, status={resp.status_code})")
            return True
        except Exception:
            continue
    _openserp_available = False
    logger.debug(f"[OPENSERP] Tidak dapat dijangkau di {base}")
    return False


def clear_openserp_cache() -> None:
    """Reset cache availability check — dipanggil tiap run baru."""
    global _openserp_available
    _openserp_available = None


def _openserp_search(
    engine: str,
    query: str,
    limit: int = 30,
    extra_params: Optional[dict] = None,
) -> list[dict]:
    base = _settings.openserp_base_url
    params: dict = {"text": query, "limit": limit}
    if extra_params:
        params.update(extra_params)
    timeout = _ENGINE_TIMEOUT.get(engine, 45)
    try:
        resp = httpx.get(
            f"{base}/{engine}/search",
            params=params,
            timeout=timeout,
        )
        if resp.status_code != 200:
            logger.warning(f"OpenSERP {engine} returned {resp.status_code} for query: {query[:50]}")
            return []
        data = resp.json()
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("organic", data.get("results", []))
        else:
            return []
        results = []
        for item in items:
            url = item.get("url", item.get("link", ""))
            if not url:
                continue
            results.append({
                "title":   item.get("title", ""),
                "url":     url,
                "snippet": item.get("description", item.get("snippet", "")),
            })
        return results
    except httpx.TimeoutException:
        logger.warning(f"OpenSERP {engine} timeout ({timeout}s) for query: {query[:50]}")
        return []
    except Exception as e:
        logger.warning(f"OpenSERP {engine} error for query: {query[:50]}: {e}")
        return []


def _extract_core_query(query: str) -> str:
    if "," in query:
        core = query.split(",")[0].strip()
    else:
        core = query
    return core[:100].strip()


def _detect_regions(query: str) -> list[dict]:
    query_lower = query.lower()
    is_global_mode = any(kw in query_lower for kw in GLOBAL_KEYWORDS)

    if is_global_mode:
        results = [_GLOBAL_REGION]
        seen_labels = {_GLOBAL_REGION["label"]}
        for entry in REGION_MAP:
            if entry["label"] not in seen_labels:
                results.append(entry)
                seen_labels.add(entry["label"])
        return results

    detected = []
    for entry in REGION_MAP:
        if any(kw in query_lower for kw in entry["keywords"]):
            detected.append(entry)

    results = [_GLOBAL_REGION]
    seen_labels = {_GLOBAL_REGION["label"]}
    for entry in detected:
        if entry["label"] not in seen_labels:
            results.append(entry)
            seen_labels.add(entry["label"])
    return results


def _score_seed_url(url: str, title: str = "", snippet: str = "") -> int:
    score = 0
    combined = (url + " " + title + " " + snippet).lower()
    parsed = urlparse(url)
    domain = parsed.netloc.lower()

    for kw in ["exhibitor", "vendor", "sponsor", "booth", "directory", "participant"]:
        if kw in combined:
            score += 3
    for kw in ["expo", "exhibition", "conference", "tradeshow", "summit"]:
        if kw in combined:
            score += 2
    for kw in CYBERSECURITY_EVENT_KEYWORDS:
        if kw in combined:
            score += 1
    for ed in EVENT_SITE_DOMAINS:
        if ed in domain:
            score += 5
            break
    if any(x in url.lower() for x in ["exhibitor", "vendor", "directory", "sponsor"]):
        score += 4
    if "list" in url.lower() or "directory" in url.lower():
        score += 2

    return score


def _deduplicate_urls(results: list[dict]) -> list[dict]:
    seen_domains: set[str] = set()
    seen_urls: set[str] = set()
    deduped = []
    for r in results:
        url = r.get("url", "")
        if not url:
            continue
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        norm_url = url.rstrip("/").lower()
        if norm_url in seen_urls:
            continue
        seen_urls.add(norm_url)
        if domain in seen_domains and r.get("score", 0) < 5:
            continue
        seen_domains.add(domain)
        deduped.append(r)
    return deduped


def _search_region(
    query: str,
    region: dict,
    templates: list[str],
    max_results_per_template: int = 15,
) -> list[dict]:
    engine = region["engine"]
    extra_params = region.get("extra_params", {})
    label = region["label"]

    results = []
    for template in templates:
        search_q = template.format(query=query)
        raw = _openserp_search(engine, search_q, limit=max_results_per_template, extra_params=extra_params)
        for r in raw:
            r["region_label"] = label
            r["region_engine"] = engine
        results.extend(raw)
        time.sleep(random.uniform(0.4, 1.0))

    return results


def _ddg_search(query: str, max_results: int = 20) -> list[dict]:
    """DuckDuckGo search via ddgs library — no API key, used as fallback."""
    try:
        from ddgs import DDGS
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results))
        return [
            {
                "url":     r.get("href", ""),
                "title":   r.get("title", ""),
                "snippet": r.get("body", "")[:300],
            }
            for r in results if r.get("href")
        ]
    except Exception as e:
        logger.warning(f"[DDG] Error: {e}")
        return []


def _search_with_ddg_fallback(query: str, max_seeds: int = 40) -> list[dict]:
    """DDG fallback when OpenSERP is down. No API key needed."""
    core = _extract_core_query(query)
    all_results: list[dict] = []
    for template in SEARCH_TEMPLATES[:6]:
        search_q = template.format(query=core)
        raw = _ddg_search(search_q, max_results=15)
        for r in raw:
            r["score"] = _score_seed_url(r["url"], r.get("title", ""), r.get("snippet", ""))
            r["domain"] = urlparse(r["url"]).netloc.lower()
            r["region_label"] = "Global (DDG)"
            r["region_engine"] = "duckduckgo"
        all_results.extend(raw)
        time.sleep(random.uniform(0.8, 1.5))

    all_results = _deduplicate_urls(all_results)
    all_results.sort(key=lambda x: x.get("score", 0), reverse=True)
    logger.info(f"[DDG] {len(all_results)} seed URLs (fallback mode)")
    return all_results[:max_seeds]


@tool
def search_exhibitor_events(query: str, max_seeds: int = 40) -> list[dict]:
    """
    Search for exhibitor event pages related to the query.
    Uses OpenSERP (multi-region) when available, automatically falls back to Tavily.
    Returns a list of seed URLs scored by relevance to vendor/exhibitor listings.
    Each result has: url, title, snippet, score, domain, region_label.
    """
    core = _extract_core_query(query)
    query_en = core

    # ── OpenSERP primary — DDG fallback (no API key needed for either) ────────
    if not _check_openserp():
        logger.warning("[SEARCH] OpenSERP tidak tersedia — fallback ke DuckDuckGo")
        return _search_with_ddg_fallback(query, max_seeds)

    logger.info("[SEARCH] OpenSERP aktif — multi-region mode")

    regions = _detect_regions(query)

    n_regions = len(regions)
    if n_regions <= 2:
        templates_to_use = SEARCH_TEMPLATES[:6]
        max_per_template = 15
        dynamic_max_seeds = 40  # Single/dual region — conservative
    elif n_regions <= 5:
        templates_to_use = SEARCH_TEMPLATES[:6]
        max_per_template = 12
        dynamic_max_seeds = 100  # Moderate multi-region
    else:
        # Multi-region (6+): use more templates for comprehensive coverage
        templates_to_use = SEARCH_TEMPLATES  # All 12 templates
        max_per_template = 12
        dynamic_max_seeds = 200  # Aggressive multi-region coverage

    # Use dynamic seeds instead of hardcoded 40
    effective_max_seeds = max(max_seeds, dynamic_max_seeds) if max_seeds > 0 else dynamic_max_seeds

    all_results: list[dict] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan][DISCOVER][/bold cyan] {task.description}"),
        BarColumn(bar_width=28),
        MofNCompleteColumn(),
        TextColumn("[dim]{task.fields[found]} URLs[/dim]"),
        TimeElapsedColumn(),
        console=_console,
        transient=True,
    ) as progress:
        task = progress.add_task("starting...", total=n_regions, found=0)
        for region in regions:
            label = region["label"]
            engine = region["engine"]
            progress.update(task, description=f"[{label}] ({engine})", found=len(all_results))
            region_results = _search_region(
                query=query_en,
                region=region,
                templates=templates_to_use,
                max_results_per_template=max_per_template,
            )
            for r in region_results:
                r["score"] = _score_seed_url(r["url"], r.get("title", ""), r.get("snippet", ""))
                r["domain"] = urlparse(r["url"]).netloc.lower()
            all_results.extend(region_results)
            progress.advance(task)

    all_results = _deduplicate_urls(all_results)
    all_results.sort(key=lambda x: x.get("score", 0), reverse=True)
    top_results = all_results[:effective_max_seeds]

    region_counts: dict[str, int] = {}
    for r in top_results:
        lbl = r.get("region_label", "Global")
        region_counts[lbl] = region_counts.get(lbl, 0) + 1

    logger.info(f"Found {len(top_results)} seed URLs: {region_counts}")

    return top_results


@tool
def search_vendor_directory(
    query: str,
    region: str = "global",
    year: Optional[str] = None,
) -> list[dict]:
    """
    Search specifically for vendor directories, exhibitor lists, and company directories
    related to a topic. Returns scored URLs.
    """
    year_str = year or "2025 2026"
    search_queries = [
        f"{query} exhibitor list {year_str}",
        f"{query} vendor directory {region}",
        f"{query} companies participating {year_str}",
        f"{query} technology providers {region}",
    ]

    all_results: list[dict] = []

    use_openserp = _check_openserp()

    for sq in search_queries[:3]:
        if use_openserp:
            results = _openserp_search("google", sq, limit=20)
        else:
            results = _ddg_search(sq, max_results=15)
        for r in results:
            r["score"] = _score_seed_url(r["url"], r.get("title", ""), r.get("snippet", ""))
            r["domain"] = urlparse(r["url"]).netloc.lower()
        all_results.extend(results)
        time.sleep(random.uniform(0.3, 1.0))

    all_results = _deduplicate_urls(all_results)
    all_results.sort(key=lambda x: x.get("score", 0), reverse=True)
    return all_results[:30]


def _engine_for_target(domain: str = "", country: str = "", name: str = "") -> tuple[str, dict]:
    """
    Pick the right search engine based on domain TLD, country, or company name.

    Rules (checked in order of specificity):
      • .cn / .com.cn TLD          → baidu  (China)
      • .ru / .su TLD              → yandex (Russia)
      • .jp TLD                    → google gl=jp hl=ja
      • .kr TLD                    → google gl=kr hl=ko
      • Chinese chars in name      → baidu
      • Cyrillic chars in name     → yandex
      • country keyword China/Russia → baidu/yandex
      • everything else            → google (no geo params)

    Returns (engine_name, extra_params_dict).
    """
    # ── TLD detection (most reliable signal) ─────────────────────────────────
    tld = ""
    if domain:
        clean = domain.lower().replace("www.", "").rstrip("/")
        # strip path/query if full URL was passed by mistake
        tld = clean.split("/")[0].rsplit(".", 1)[-1]  # "cn", "ru", "jp", …

    if tld in ("cn",) or ".com.cn" in domain.lower():
        return "baidu", {}
    if tld in ("ru", "su"):
        return "yandex", {}
    if tld == "jp":
        return "yahoo", {}   # Yahoo Japan — far more relevant than Google for .jp domains
    if tld == "kr":
        return "naver", {}   # Naver — dominant Korean search engine

    # ── Script detection in company name ─────────────────────────────────────
    if name:
        has_cjk = any('\u4e00' <= c <= '\u9fff' for c in name)
        has_cyrillic = any('\u0400' <= c <= '\u04ff' for c in name)
        if has_cjk:
            return "baidu", {}
        if has_cyrillic:
            return "yandex", {}

    # ── Country / name keyword fallback ──────────────────────────────────────
    _CN_KEYWORDS = (
        "china", "chinese", "prc", "beijing", "shanghai", "shenzhen",
        "guangzhou", "chengdu", "wuhan", "tianjin", "hangzhou", "nanjing",
        "qingdao", "xi'an", "xian", "chongqing", "dalian", "suzhou",
    )
    _RU_KEYWORDS = (
        "russia", "russian", "moscow", "moskow", "st. petersburg",
        "saint petersburg", "novosibirsk", "ekaterinburg",
    )
    _JP_KEYWORDS = (
        "japan", "japanese", "tokyo", "osaka", "kyoto", "nagoya",
        "fukuoka", "sapporo", "kobe", "hiroshima",
    )
    _KR_KEYWORDS = (
        "korea", "korean", "south korea", "seoul", "busan",
        "incheon", "daegu", "daejeon", "gwangju",
    )
    combined = (country + " " + name).lower()
    if any(kw in combined for kw in _CN_KEYWORDS):
        return "baidu", {}
    if any(kw in combined for kw in _RU_KEYWORDS):
        return "yandex", {}
    if any(kw in combined for kw in _JP_KEYWORDS):
        return "yahoo", {}
    if any(kw in combined for kw in _KR_KEYWORDS):
        return "naver", {}

    return "google", {}


@tool
def search_company_info(company_name: str, domain: str = "", country: str = "") -> dict:
    """
    Search for basic info about a specific company. Used during enrichment phase.
    Automatically selects the right search engine based on domain TLD, country,
    or script in company name (Baidu for China, Yandex for Russia, Google otherwise).
    Returns structured info: website, linkedin, description, country.
    """
    engine, extra_params = _engine_for_target(domain=domain, country=country, name=company_name)
    logger.debug(f"[SEARCH-CO] {company_name!r} domain={domain!r} → engine={engine}")

    queries = [
        f'"{company_name}" official website',
        f'"{company_name}" company',
    ]
    if domain:
        queries.insert(0, f"site:{domain} {company_name}")

    results: list[dict] = []
    for q in queries[:2]:
        r = _openserp_search(engine, q, limit=5, extra_params=extra_params)
        results.extend(r)
        time.sleep(0.3)

    linkedin_url = ""
    website_url = domain if domain else ""
    description = ""

    for r in results:
        url = r.get("url", "")
        snippet = r.get("snippet", "")
        if "linkedin.com/company" in url and not linkedin_url:
            linkedin_url = url
        if domain and domain in url and not website_url:
            website_url = url
        if snippet and not description:
            description = snippet[:300]

    return {
        "company_name":        company_name,
        "website":             website_url,
        "linkedin":            linkedin_url,
        "description":         description,
        "search_results_count": len(results),
    }


ALL_SEARCH_TOOLS = [search_exhibitor_events, search_vendor_directory, search_company_info]
