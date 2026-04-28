import re
from typing import Optional
from urllib.parse import urlparse, urljoin, urlunparse, parse_qs, urlencode

import tldextract
from bs4 import BeautifulSoup
from langchain_core.tools import tool
from loguru import logger

try:
    from url_normalize import url_normalize
    HAS_URL_NORMALIZE = True
except ImportError:
    HAS_URL_NORMALIZE = False

from backend.core.config import get_settings

_settings = get_settings()

NEWS_MEDIA_DOMAINS = {
    "weforum.org", "cnn.com", "bbc.com", "bbc.co.uk", "reuters.com",
    "bloomberg.com", "ft.com", "wsj.com", "nytimes.com", "theguardian.com",
    "washingtonpost.com", "forbes.com", "businessinsider.com", "techcrunch.com",
    "wired.com", "zdnet.com", "theregister.com", "arstechnica.com",
    "securityweek.com", "darkreading.com", "threatpost.com", "bleepingcomputer.com",
    "cyberscoop.com", "helpnetsecurity.com", "infosecurity-magazine.com",
    "govinfosecurity.com", "bankinfosecurity.com", "healthcareinfosecurity.com",
    "medium.com", "substack.com", "wordpress.com", "blogger.com",
    "wikipedia.org", "wikimedia.org", "reddit.com", "quora.com",
    "twitter.com", "x.com", "facebook.com", "instagram.com", "youtube.com",
    "linkedin.com", "flickr.com", "pinterest.com",
    "amazon.com", "ebay.com", "aliexpress.com", "etsy.com",
    "google.com", "bing.com", "yahoo.com", "duckduckgo.com",
    "github.com", "gitlab.com", "stackoverflow.com", "npmjs.com",
    "apple.com", "microsoft.com", "docs.microsoft.com",
    "qualtrics.com", "typeform.com", "surveymonkey.com",
    "share.flipboard.com",
}

EXHIBITOR_KEYWORDS_STRONG = [
    "exhibitor", "exhibitors", "vendor", "vendors", "sponsor", "sponsors",
    "participant", "participants", "booth", "booths", "stand", "stands",
    "floor-plan", "floorplan", "floor_plan", "hall-map", "hallmap",
    "companies", "company-list", "brand-list", "brands",
    "solution-provider", "solution_provider", "tech-provider",
    "member-directory", "members-directory", "directory",
]

EXHIBITOR_KEYWORDS_MEDIUM = [
    "partner", "partners", "showcase", "profiles", "listing",
    "attendee", "attendees", "delegate", "delegates",
    "pavilion", "hall", "zone", "sector", "segment",
    "portfolio", "lineup", "featured-companies",
]

EXHIBITOR_KEYWORDS_WEAK = [
    "about", "company", "organisation", "organization", "org",
    "group", "corp", "inc", "ltd", "llc", "gmbh",
]

PAGINATION_PATTERNS = [
    r'/page/\d+', r'[?&]page=\d+', r'[?&]p=\d+',
    r'[?&]offset=\d+', r'[?&]start=\d+',
    r'/exhibitors/\d+', r'/list/\d+',
]

IGNORED_PATH_PATTERNS = [
    r'^/cdn-cgi/', r'^/wp-admin/', r'^/wp-login',
    r'#', r'javascript:', r'mailto:', r'tel:', r'fax:',
    r'/login', r'/register', r'/signup', r'/cart', r'/checkout',
    r'/privacy', r'/terms', r'/cookie', r'/sitemap',
    r'\.(pdf|doc|docx|xls|xlsx|ppt|pptx|zip|rar|exe|dmg)$',
    r'\.(jpg|jpeg|png|gif|svg|ico|webp|mp4|mp3|avi)$',
    r'\.(css|js|json|xml|txt|csv)$',
]

_ignored_compiled = [re.compile(p, re.IGNORECASE) for p in IGNORED_PATH_PATTERNS]


def _normalize_url(url: str, base_url: str = "") -> str:
    if not url:
        return ""
    url = url.strip()
    if url.startswith("//"):
        parsed_base = urlparse(base_url)
        url = f"{parsed_base.scheme}:{url}"
    if base_url and not url.startswith(("http://", "https://")):
        url = urljoin(base_url, url)
    if not url.startswith(("http://", "https://")):
        return ""
    if HAS_URL_NORMALIZE:
        try:
            url = url_normalize(url)
        except Exception:
            pass
    parsed = urlparse(url)
    clean = urlunparse((
        parsed.scheme,
        parsed.netloc.lower(),
        parsed.path.rstrip("/") if parsed.path != "/" else "/",
        parsed.params,
        parsed.query,
        "",
    ))
    return clean


def _is_ignored_url(url: str) -> bool:
    for pattern in _ignored_compiled:
        if pattern.search(url):
            return True
    return False


def _is_news_media_domain(url: str) -> bool:
    try:
        domain = urlparse(url).netloc.lower().replace("www.", "")
        return any(domain == nd or domain.endswith("." + nd) for nd in NEWS_MEDIA_DOMAINS)
    except Exception:
        return False


def _score_link(url: str, text: str = "", title: str = "") -> dict:
    score = 0
    reasons = []
    url_lower = url.lower()
    combined = (url_lower + " " + text.lower() + " " + title.lower())

    if _is_news_media_domain(url):
        return {"score": -10, "reasons": ["news_media_blocked"]}

    for kw in EXHIBITOR_KEYWORDS_STRONG:
        if kw in url_lower:
            score += 5
            reasons.append(f"url_strong:{kw}")
        elif kw in combined:
            score += 2
            reasons.append(f"text_strong:{kw}")

    for kw in EXHIBITOR_KEYWORDS_MEDIUM:
        if kw in url_lower:
            score += 3
            reasons.append(f"url_medium:{kw}")
        elif kw in combined:
            score += 1
            reasons.append(f"text_medium:{kw}")

    if re.search(r'/page/\d+|[?&]page=\d+', url_lower):
        score += 2
        reasons.append("pagination")

    parsed = urlparse(url)
    path_depth = len([p for p in parsed.path.split("/") if p])
    if path_depth == 1:
        score += 1
    elif path_depth >= 4:
        score -= 1

    return {"score": score, "reasons": reasons[:5]}


def _is_external_link(url: str, base_domain: str) -> bool:
    extracted = tldextract.extract(url)
    link_domain = f"{extracted.domain}.{extracted.suffix}".lower()
    return link_domain != base_domain


def _extract_all_links_from_html(html: str, base_url: str) -> list[dict]:
    if not html:
        return []

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            return []

    base_extracted = tldextract.extract(base_url)
    base_domain = f"{base_extracted.domain}.{base_extracted.suffix}".lower()

    links = []
    seen_urls = set()

    for tag in soup.find_all("a", href=True):
        href = tag.get("href", "").strip()
        if not href:
            continue
        normalized = _normalize_url(href, base_url)
        if not normalized or normalized in seen_urls:
            continue
        if _is_ignored_url(normalized):
            continue
        if _is_news_media_domain(normalized):
            continue
        seen_urls.add(normalized)

        text = tag.get_text(strip=True)[:200]
        title = tag.get("title", "")[:100]
        is_external = _is_external_link(normalized, base_domain)

        score_data = _score_link(normalized, text, title)

        links.append({
            "url": normalized,
            "text": text,
            "title": title,
            "is_external": is_external,
            "score": score_data["score"],
            "score_reasons": score_data["reasons"],
            "depth_hint": len([p for p in urlparse(normalized).path.split("/") if p]),
        })

    return links


def _detect_event_name(soup: BeautifulSoup) -> str:
    candidates = []
    og_title = soup.find("meta", {"property": "og:title"})
    if og_title:
        candidates.append(og_title.get("content", ""))
    title_tag = soup.find("title")
    if title_tag:
        candidates.append(title_tag.get_text(strip=True))
    h1 = soup.find("h1")
    if h1:
        candidates.append(h1.get_text(strip=True))
    for c in candidates:
        if c and len(c) > 5:
            return c[:200]
    return ""


def _detect_event_location(soup: BeautifulSoup) -> str:
    location_patterns = [
        soup.find(attrs={"class": re.compile(r"location|venue|city|place", re.I)}),
        soup.find("meta", {"name": re.compile(r"location|venue", re.I)}),
        soup.find(attrs={"itemprop": "location"}),
    ]
    for elem in location_patterns:
        if elem:
            text = elem.get_text(strip=True) if hasattr(elem, "get_text") else elem.get("content", "")
            if text and len(text) > 2:
                return text[:200]

    location_keywords = ["venue:", "location:", "held at", "taking place at"]
    for tag in soup.find_all(["p", "div", "span", "li"]):
        text = tag.get_text(strip=True).lower()
        for kw in location_keywords:
            if kw in text:
                return tag.get_text(strip=True)[:200]

    return ""


def _detect_event_date(soup: BeautifulSoup) -> str:
    date_meta = soup.find("meta", {"name": re.compile(r"date|event.date", re.I)})
    if date_meta:
        return date_meta.get("content", "")[:50]
    date_elem = soup.find(attrs={"itemprop": re.compile(r"startDate|endDate|datePublished", re.I)})
    if date_elem:
        return date_elem.get("content", date_elem.get_text(strip=True))[:50]
    date_pattern = re.compile(
        r'\b(\d{1,2}[\s\-/]\w+[\s\-/]\d{2,4}|\w+\s+\d{1,2}[-–]\d{1,2},?\s*\d{4}|\d{4})\b'
    )
    for tag in soup.find_all(["time", "span", "div", "p"]):
        text = tag.get_text(strip=True)
        match = date_pattern.search(text)
        if match and len(text) < 100:
            return text[:80]
    return ""


def _find_pagination_urls(soup: BeautifulSoup, base_url: str) -> list[str]:
    pagination_urls = []
    pagination_selectors = [
        soup.find(attrs={"class": re.compile(r"pagination|pager|page-nav", re.I)}),
        soup.find("nav", attrs={"aria-label": re.compile(r"pagination|page", re.I)}),
    ]
    for container in pagination_selectors:
        if not container:
            continue
        for a in container.find_all("a", href=True):
            href = a.get("href", "")
            normalized = _normalize_url(href, base_url)
            if normalized and normalized != base_url:
                pagination_urls.append(normalized)

    parsed = urlparse(base_url)
    query_params = parse_qs(parsed.query)
    if "page" in query_params:
        try:
            current_page = int(query_params["page"][0])
            for offset in range(1, 6):
                new_params = {**query_params, "page": [str(current_page + offset)]}
                new_query = urlencode(new_params, doseq=True)
                new_url = urlunparse(parsed._replace(query=new_query))
                pagination_urls.append(new_url)
        except (ValueError, IndexError):
            pass

    return list(set(pagination_urls))


@tool
def extract_links(html: str, base_url: str) -> list[dict]:
    """
    Extract all hyperlinks from HTML page. Returns list of links with url, text,
    score (relevance to exhibitor/vendor content), is_external, depth_hint.
    """
    if not html:
        return []
    links = _extract_all_links_from_html(html, base_url)
    links.sort(key=lambda x: x["score"], reverse=True)
    return links[:2000]


@tool
def classify_exhibitor_links(links: list[dict], threshold: int = 2) -> list[dict]:
    """
    Filter and rank links that likely lead to exhibitor/vendor profile pages.
    Returns top-scored links above threshold score. Includes pagination URLs.
    """
    if not links:
        return []
    qualified = [l for l in links if l.get("score", 0) >= threshold]
    qualified.sort(key=lambda x: x.get("score", 0), reverse=True)
    return qualified[:500]


@tool
def extract_page_metadata(html: str, url: str) -> dict:
    """
    Extract event metadata from a page: event name, location, date,
    organizer, pagination URLs. Used to enrich vendor context.
    """
    if not html:
        return {"event_name": "", "event_location": "", "event_date": "", "organizer": "", "pagination_urls": []}

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    pagination_urls = _find_pagination_urls(soup, url)

    organizer = ""
    org_elem = soup.find(attrs={"itemprop": "organizer"})
    if org_elem:
        organizer = org_elem.get_text(strip=True)[:200]
    if not organizer:
        org_meta = soup.find("meta", {"name": re.compile(r"author|organizer|publisher", re.I)})
        if org_meta:
            organizer = org_meta.get("content", "")[:200]

    return {
        "event_name": _detect_event_name(soup),
        "event_location": _detect_event_location(soup),
        "event_date": _detect_event_date(soup),
        "organizer": organizer,
        "pagination_urls": pagination_urls[:20],
    }


@tool
def extract_vendor_domain_links(html: str, base_url: str) -> list[str]:
    """
    Find links pointing to external vendor/company websites from an event page.
    Filters out social media and returns domain-level external links.
    """
    if not html:
        return []

    settings = get_settings()
    links = _extract_all_links_from_html(html, base_url)
    external_links = [l for l in links if l.get("is_external") and l.get("score", 0) >= 0]

    vendor_domains = []
    seen = set()
    for link in external_links:
        url = link["url"]
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if not domain or domain in seen:
            continue
        if settings.is_ignored_domain(domain):
            continue
        seen.add(domain)
        vendor_domains.append(url)

    return vendor_domains[:200]


@tool
def score_page_as_event(html: str, url: str) -> dict:
    """
    Heuristic scoring of whether a page is an event/expo listing page
    with exhibitor data. Returns score 0-100 and evidence list.
    """
    if not html:
        return {"score": 0, "is_event_page": False, "evidence": []}

    evidence = []
    score = 0

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    text = soup.get_text(separator=" ", strip=True).lower()
    url_lower = url.lower()

    event_keywords = ["exhibitor", "vendor", "sponsor", "booth", "tradeshow", "expo", "exhibition"]
    for kw in event_keywords:
        count = text.count(kw)
        if count > 5:
            score += 15
            evidence.append(f"high_freq:{kw}={count}")
        elif count > 1:
            score += 5
            evidence.append(f"low_freq:{kw}={count}")

    company_elements = len(soup.find_all(attrs={"class": re.compile(r"company|exhibitor|vendor|sponsor", re.I)}))
    if company_elements > 10:
        score += 20
        evidence.append(f"company_elements={company_elements}")
    elif company_elements > 3:
        score += 10
        evidence.append(f"company_elements={company_elements}")

    logo_count = len(soup.find_all("img", attrs={"class": re.compile(r"logo|brand", re.I)}))
    if logo_count > 10:
        score += 15
        evidence.append(f"logo_count={logo_count}")

    links = soup.find_all("a", href=True)
    ext_links = 0
    for a in links[:200]:
        href = a.get("href", "")
        if href.startswith("http") and urlparse(href).netloc != urlparse(url).netloc:
            ext_links += 1
    if ext_links > 20:
        score += 10
        evidence.append(f"external_links={ext_links}")

    for kw in ["exhibitor", "vendor", "directory", "sponsor"]:
        if kw in url_lower:
            score += 10
            evidence.append(f"url_keyword:{kw}")

    pagination = bool(soup.find(attrs={"class": re.compile(r"pagination|pager", re.I)}))
    if pagination:
        score += 5
        evidence.append("has_pagination")

    score = min(score, 100)
    return {
        "score": score,
        "is_event_page": score >= 25,
        "evidence": evidence[:10],
        "url": url,
    }


@tool
def find_exhibitor_list_pages(links: list[dict], base_url: str) -> list[dict]:
    """
    From a list of extracted links, identify pages that are likely to be
    exhibitor list/directory pages. Returns ranked candidates.
    """
    if not links:
        return []

    candidates = []
    parsed_base = urlparse(base_url)
    base_domain = parsed_base.netloc.lower()

    for link in links:
        url = link.get("url", "")
        if not url:
            continue
        parsed = urlparse(url)
        if parsed.netloc.lower() != base_domain:
            continue

        url_score = 0
        url_lower = url.lower()
        text_lower = link.get("text", "").lower()

        for kw in EXHIBITOR_KEYWORDS_STRONG:
            if kw in url_lower:
                url_score += 8
            elif kw in text_lower:
                url_score += 4

        for kw in EXHIBITOR_KEYWORDS_MEDIUM:
            if kw in url_lower:
                url_score += 4
            elif kw in text_lower:
                url_score += 2

        if url_score > 0:
            candidates.append({
                "url": url,
                "text": link.get("text", ""),
                "url_score": url_score,
                "link_score": link.get("score", 0),
                "total_score": url_score + link.get("score", 0),
            })

    candidates.sort(key=lambda x: x["total_score"], reverse=True)
    seen_patterns = set()
    deduped = []
    for c in candidates:
        path = urlparse(c["url"]).path.lower().rstrip("/")
        if path not in seen_patterns:
            seen_patterns.add(path)
            deduped.append(c)

    return deduped[:100]


ALL_PARSE_TOOLS = [
    extract_links,
    classify_exhibitor_links,
    extract_page_metadata,
    extract_vendor_domain_links,
    score_page_as_event,
    find_exhibitor_list_pages,
]
