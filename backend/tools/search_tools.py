import time
import random
from typing import Optional
from urllib.parse import urlparse

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

SEARCH_TEMPLATES = [
    '{query} exhibitors list site',
    '{query} vendors directory',
    '{query} expo participants 2024 2025',
    '{query} conference exhibitors',
    '{query} tradeshow companies list',
    '{query} summit sponsors vendors',
    'site:10times.com {query}',
    'site:eventbrite.com {query} exhibitors',
    'site:expodatabase.com {query}',
    '{query} floor plan exhibitors',
    '{query} expo hall vendors directory',
    '{query} event exhibitor list filetype:html',
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

# Multi-region config: keyword patterns → (ddgs_region, translate_to, label)
# Mendukung keyword Indonesia DAN Inggris
REGION_MAP = [
    # East Asia
    (["china", "cina", "tiongkok", "beijing", "shanghai", "shenzhen", "guangzhou"],
     "cn-zh", "zh-CN", "China"),
    (["jepang", "japan", "tokyo", "osaka", "kyoto"],
     "jp-ja", "ja", "Japan"),
    (["korea", "korean", "seoul", "busan"],
     "kr-ko", "ko", "Korea"),

    # Americas
    (["usa", "us ", " us,", "america", "united states", "amerika",
      "new york", "washington", "los angeles", "san francisco", "chicago"],
     "us-en", "en", "USA"),

    # Europe (general)
    (["eropa", "europe", "european", "eu ", "german", "jerman", "prancis", "france",
      "inggris", "uk ", "belanda", "netherlands", "italy", "italia", "spain", "spanyol",
      "sweden", "swedia", "poland", "polandia", "belanda", "brussels"],
     "xl-en", "en", "Europe"),

    # Greece (terpisah supaya lebih akurat)
    (["greece", "greek", "yunani", "athens", "athena", "hellenic"],
     "gr-el", "en", "Greece"),

    # Russia / Eastern Europe
    (["russia", "rusia", "russian", "moskow", "moscow", "st. petersburg"],
     "ru-ru", "ru", "Russia"),

    # South Asia
    (["india", "mumbai", "new delhi", "delhi", "bangalore", "bengaluru", "hyderabad"],
     "in-en", "en", "India"),
    (["pakistan", "karachi", "islamabad", "lahore", "pakistani"],
     "pk-en", "en", "Pakistan"),

    # Southeast Asia
    (["singapura", "singapore", "asia tenggara", "southeast asia", "asean",
      "malaysia", "thailand", "filipina", "philippines", "vietnam"],
     "sg-en", "en", "Southeast Asia"),

    # Oceania
    (["oceania", "australia", "new zealand", "auckland", "sydney", "melbourne",
      "pacific", "pasifik", "papua"],
     "au-en", "en", "Oceania"),

    # Middle East
    (["arab", "arabic", "saudi", "dubai", "uae", "timur tengah", "middle east",
      "qatar", "kuwait", "bahrain", "oman", "abu dhabi"],
     "xa-en", "en", "Middle East"),

    # Asia general (fallback jika tidak spesifik)
    (["asia ", " asia", ",asia", "asian"],
     "wt-wt", "en", "Asia"),
]

# Jika query mengandung kata "global/worldwide/world/seluruh dunia" → aktifkan semua region
GLOBAL_KEYWORDS = [
    "global", "worldwide", "world", "seluruh dunia", "all regions",
    "covers over", "international", "internasional",
]


def _extract_core_query(query: str) -> str:
    """
    Ambil bagian inti query untuk diterjemahkan.
    Kalau ada koma (biasanya list region/negara di belakang), ambil segmen pertama.
    Contoh: "cyber defense 2026, CHINA, USA, OCEANIA" → "cyber defense 2026"
    """
    if "," in query:
        core = query.split(",")[0].strip()
    else:
        core = query
    return core[:100].strip()


def _translate_query(text: str, src: str, dest: str) -> str:
    if not text or src == dest:
        return text
    try:
        from deep_translator import GoogleTranslator
        return GoogleTranslator(source=src, target=dest).translate(text[:500]) or text
    except Exception as e:
        logger.debug(f"Translation failed ({src}→{dest}), using original: {e}")
        return text


def _detect_regions(query: str) -> list[tuple[str, str, str]]:
    """
    Deteksi target region dari teks query (mendukung keyword Indonesia & Inggris).
    Jika query mengandung kata global/worldwide → aktifkan semua region.
    Selalu include Global (wt-wt) sebagai entry pertama.
    Returns list of (ddgs_region, translate_to, label).
    """
    query_lower = query.lower()

    # Global mode: aktifkan semua region
    is_global_mode = any(kw in query_lower for kw in GLOBAL_KEYWORDS)

    if is_global_mode:
        results = [("wt-wt", "en", "Global")]
        seen = {("wt-wt", "en", "Global")}
        for _, ddgs_region, translate_to, label in REGION_MAP:
            entry = (ddgs_region, translate_to, label)
            if entry not in seen:
                results.append(entry)
                seen.add(entry)
        return results

    # Normal mode: deteksi berdasarkan keyword
    detected = []
    for keywords, ddgs_region, translate_to, label in REGION_MAP:
        if any(kw in query_lower for kw in keywords):
            detected.append((ddgs_region, translate_to, label))

    results = [("wt-wt", "en", "Global")]
    for entry in detected:
        if entry not in results:
            results.append(entry)
    return results


def _ddg_search(query: str, max_results: int = 30, region: str = "wt-wt") -> list[dict]:
    try:
        try:
            from ddgs import DDGS
        except ImportError:
            from duckduckgo_search import DDGS
        results = []
        with DDGS() as ddgs:
            for r in ddgs.text(
                query,
                region=region,
                safesearch="off",
                timelimit=None,
                max_results=max_results,
            ):
                results.append({
                    "title": r.get("title", ""),
                    "url": r.get("href", ""),
                    "snippet": r.get("body", ""),
                })
                if len(results) >= max_results:
                    break
        return results
    except Exception as e:
        logger.warning(f"DDG search failed for '{query}' [region={region}]: {e}")
        return []


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
    seen_domains = set()
    seen_urls = set()
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
        if domain in seen_domains:
            if r.get("score", 0) < 5:
                continue
        seen_domains.add(domain)
        deduped.append(r)
    return deduped


def _search_region(query_en: str, query_orig: str, ddgs_region: str,
                   translate_to: str, label: str, templates: list[str],
                   max_results_per_template: int = 15) -> list[dict]:
    """Run search for one region using the appropriate translated query."""
    if translate_to == "en":
        query = query_en
    elif translate_to in ("zh-CN", "ja", "ko", "ru"):
        # Selalu translate dari English → target, bukan dari Indonesian langsung
        # supaya benar meski query aslinya Inggris atau Indonesia
        query = _translate_query(query_en, "en", translate_to)
        if not query or query == query_en:
            query = query_en
    else:
        query = query_en

    logger.debug(f"[{label}] region={ddgs_region}, query={query[:60]}")

    results = []
    for template in templates:
        search_q = template.format(query=query)
        raw = _ddg_search(search_q, max_results=max_results_per_template, region=ddgs_region)
        for r in raw:
            r["region_label"] = label
            r["region_code"] = ddgs_region
        results.extend(raw)
        time.sleep(random.uniform(0.4, 1.0))

    return results


@tool
def search_exhibitor_events(query: str, max_seeds: int = 40) -> list[dict]:
    """
    Search for exhibitor event pages related to the query using DuckDuckGo.
    Auto-detects regional keywords (supports Indonesian) and searches multiple
    regional DDG instances (cn-zh, jp-ja, kr-ko, etc.) when relevant.
    Returns a list of seed URLs scored by relevance to vendor/exhibitor listings.
    Each result has: url, title, snippet, score, domain, region_label.
    """
    # Ambil core query (strip list region di belakang koma) sebelum translate
    core = _extract_core_query(query)
    query_en = _translate_query(core, "auto", "en")
    if not query_en or query_en == core:
        query_en = core

    regions = _detect_regions(query)
    logger.info(f"Multi-region search: {[r[2] for r in regions]} for query: {query[:60]}")

    # Makin banyak region → kurangi template per region supaya tidak terlalu lambat
    n_regions = len(regions)
    if n_regions <= 2:
        templates_to_use = SEARCH_TEMPLATES[:6]
        max_per_template = 15
    elif n_regions <= 5:
        templates_to_use = SEARCH_TEMPLATES[:4]
        max_per_template = 10
    else:
        templates_to_use = SEARCH_TEMPLATES[:3]
        max_per_template = 8

    logger.info(f"Multi-region: {n_regions} region × {len(templates_to_use)} template × {max_per_template} results")
    all_results = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan][DISCOVER][/bold cyan] {task.description}"),
        BarColumn(bar_width=28),
        MofNCompleteColumn(),
        TextColumn("[dim]•[/dim] {task.fields[found]} URLs"),
        TimeElapsedColumn(),
        console=_console,
        transient=True,
    ) as progress:
        task = progress.add_task(
            f"starting...", total=n_regions, found=0
        )
        for ddgs_region, translate_to, label in regions:
            progress.update(task, description=f"[{label}]  ({ddgs_region})", found=len(all_results))
            region_results = _search_region(
                query_en=query_en,
                query_orig=query,
                ddgs_region=ddgs_region,
                translate_to=translate_to,
                label=label,
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
    top_results = all_results[:max_seeds]

    region_counts = {}
    for r in top_results:
        lbl = r.get("region_label", "Global")
        region_counts[lbl] = region_counts.get(lbl, 0) + 1
    logger.info(f"Found {len(top_results)} seed URLs — breakdown: {region_counts}")

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
    year_str = year or "2024 2025"
    query_en = _translate_query(query, "id", "en")
    search_queries = [
        f"{query_en} exhibitor list {year_str}",
        f"{query_en} vendor directory {region}",
        f"{query_en} companies participating {year_str}",
        f"{query_en} technology providers {region}",
        f"site:linkedin.com/company {query_en} exhibitor",
    ]

    all_results = []
    for sq in search_queries[:3]:
        results = _ddg_search(sq, max_results=20)
        for r in results:
            r["score"] = _score_seed_url(r["url"], r.get("title", ""), r.get("snippet", ""))
            r["domain"] = urlparse(r["url"]).netloc.lower()
        all_results.extend(results)
        time.sleep(random.uniform(0.3, 1.0))

    all_results = _deduplicate_urls(all_results)
    all_results.sort(key=lambda x: x.get("score", 0), reverse=True)
    return all_results[:30]


@tool
def search_company_info(company_name: str, domain: str = "") -> dict:
    """
    Search for basic info about a specific company. Used during enrichment phase.
    Returns structured info: website, linkedin, description, country.
    """
    queries = [
        f'"{company_name}" official website',
        f'"{company_name}" company cybersecurity defense',
    ]
    if domain:
        queries.insert(0, f'site:{domain} {company_name}')

    results = []
    for q in queries[:2]:
        r = _ddg_search(q, max_results=5)
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
        "company_name": company_name,
        "website": website_url,
        "linkedin": linkedin_url,
        "description": description,
        "search_results_count": len(results),
    }


ALL_SEARCH_TOOLS = [search_exhibitor_events, search_vendor_directory, search_company_info]
