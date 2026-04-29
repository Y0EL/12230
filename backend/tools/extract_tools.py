import asyncio
import re
import json
from typing import Optional, Any
from urllib.parse import urlparse

import tldextract
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field
from langchain_core.tools import tool
from loguru import logger

from backend.core.config import get_settings

_settings = get_settings()

# ── Translation cache ────────────────────────────────────────────────────────
_translation_cache: dict[str, str] = {}


def _translate(text: str, src: str, dest: str = "en") -> str:
    if not text or src in ("en", "id", "unknown", ""):
        return text
    key = f"{src}:{text[:200]}"
    if key in _translation_cache:
        return _translation_cache[key]
    try:
        from deep_translator import GoogleTranslator
        result = GoogleTranslator(source="auto", target=dest).translate(text[:500]) or text
        _translation_cache[key] = result
        return result
    except Exception as e:
        logger.debug(f"[TRANSLATE] {src}→{dest} failed: {e}")
        return text


def _detect_lang(soup: BeautifulSoup) -> str:
    html_tag = soup.find("html")
    if html_tag and html_tag.get("lang"):
        lang = html_tag["lang"].split("-")[0].lower()
        if lang:
            return lang
    # Content-based fallback — count CJK / Cyrillic chars in page text
    text_sample = soup.get_text()[:1000]
    cjk_count = sum(1 for c in text_sample if '一' <= c <= '鿿')
    if cjk_count > 5:
        return "zh"
    cyrillic_count = sum(1 for c in text_sample if 'Ѐ' <= c <= 'ӿ')
    if cyrillic_count > 5:
        return "ru"
    return "unknown"


# Label → field name mapping (English + Chinese keys)
_LABEL_FIELD_MAP: dict[str, str] = {
    # Organizer
    "organizer": "organizer", "organiser": "organizer", "organized by": "organizer",
    "organizing unit": "organizer", "host organization": "organizer",
    "co-organizer": "organizer", "organizers": "organizer",
    "主办单位": "organizer", "主办方": "organizer", "主办": "organizer",
    "协办单位": "organizer",

    # City / location
    "host city": "city", "venue city": "city", "city": "city",
    "host region": "city", "host area": "city", "event city": "city",
    "host location": "city", "location": "city",
    "举办地区": "city", "举办地": "city", "举办城市": "city", "城市": "city",

    # Country
    "country": "country", "nation": "country", "host country": "country",
    "举办国家": "country", "国家": "country",

    # Address
    "address": "address", "venue address": "address", "hall address": "address",
    "exhibition hall address": "address", "展馆地址": "address", "地址": "address",

    # Category / Industry / Domain
    "industry": "category", "sector": "category", "category": "category",
    "type": "category", "fair type": "category", "domain": "category",
    "所属行业": "category", "行业": "category", "类型": "category",

    # Event location / hall
    "exhibition hall": "event_location", "venue": "event_location", "hall": "event_location",
    "exhibition venue": "event_location", "event venue": "event_location",
    "举办展馆": "event_location", "展馆": "event_location", "展览馆": "event_location",
    "举办地点": "event_location",

    # Event date
    "date": "event_date", "period": "event_date", "exhibition date": "event_date",
    "event date": "event_date", "fair date": "event_date", "exhibition time": "event_date",
    "show time": "event_date", "event time": "event_date",
    "展会时间": "event_date", "展览时间": "event_date", "时间": "event_date",

    # Scale / area
    "scale": "scale", "area": "scale", "exhibition area": "scale",
    "floor area": "scale", "gross area": "scale",
    "展览面积": "scale", "面积": "scale", "展区面积": "scale",

    # Exhibitor count
    "exhibitors": "exhibitor_count", "number of exhibitors": "exhibitor_count",
    "参展商": "exhibitor_count", "参展商数量": "exhibitor_count", "展商数量": "exhibitor_count",

    # Visitor count
    "visitors": "visitor_count", "attendance": "visitor_count",
    "number of visitors": "visitor_count", "visitor count": "visitor_count",
    "观众人数": "visitor_count", "观众数量": "visitor_count", "观众": "visitor_count",

    # Website / contact
    "website": "website", "web": "website", "official site": "website",
    "official website": "website",
    "展会网站": "website", "官网": "website", "网站": "website",
    "phone": "phone", "telephone": "phone", "tel": "phone",
    "电话": "phone", "联系电话": "phone",
    "email": "email", "e-mail": "email",
    "邮箱": "email", "电子邮件": "email",

    # Booth / stand
    "booth": "booth_number", "booth number": "booth_number", "booth no": "booth_number",
    "stand": "booth_number", "stand number": "booth_number", "stand no": "booth_number",
    "stand no.": "booth_number", "hall": "booth_number", "pavilion": "booth_number",
    "展位": "booth_number", "展位号": "booth_number", "摊位": "booth_number",
}


def _extract_label_value_pairs(soup: BeautifulSoup, src_lang: str) -> dict:
    """
    Extract key-value structured info blocks (table rows, dl/dt/dd, label+value divs).
    Translates labels to English before mapping to field names.
    """
    result: dict[str, str] = {}

    def _map_pair(label_text: str, value_text: str) -> None:
        if not label_text or not value_text:
            return
        if len(label_text) >= 60 or len(value_text.strip()) < 2:
            return
        # Try original label first (catches Chinese/Japanese/etc. keys directly)
        label_orig = label_text.strip().rstrip(":：").strip()
        field = _LABEL_FIELD_MAP.get(label_orig)
        if not field:
            # Fallback: translate label to English then look up
            label_en = _translate(label_text, src_lang).lower().strip().rstrip(":：")
            field = _LABEL_FIELD_MAP.get(label_en)
        if field and not result.get(field):
            translated_value = _translate(_clean_text(value_text, 300), src_lang)
            result[field] = translated_value

    # Pattern 1: <table> with 2-cell rows (label | value)
    for row in soup.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) == 2:
            _map_pair(cells[0].get_text(strip=True), cells[1].get_text(strip=True))

    # Pattern 2: <dl><dt>label</dt><dd>value</dd></dl>
    for dl in soup.find_all("dl"):
        for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
            _map_pair(dt.get_text(strip=True), dd.get_text(strip=True))

    # Pattern 3: elements where class contains "item", "info", "detail", "row"
    for container in soup.find_all(class_=re.compile(r'item|info.?row|detail|meta.?row|field', re.I)):
        children = [c for c in container.children if hasattr(c, "get_text")]
        if len(children) == 2:
            _map_pair(children[0].get_text(strip=True), children[1].get_text(strip=True))

    # Pattern 4: <p>Label:Value</p> — inline key:value in paragraph (e.g. WDS vendor profiles)
    for p in soup.find_all("p"):
        text = p.get_text(strip=True)
        if ":" not in text or len(text) > 150:
            continue
        parts = text.split(":", 1)
        label = parts[0].strip()
        value = parts[1].strip()
        if label and value and len(label) < 40:
            _map_pair(label, value)

    return result

EMAIL_PATTERN = re.compile(
    r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b'
)
PHONE_PATTERN = re.compile(
    r'(?:(?:\+|00)[1-9]\d{0,3}[\s\-\.]?)?'
    r'(?:\(?\d{1,4}\)?[\s\-\.]?)?'
    r'\d{3,4}[\s\-\.]?\d{3,4}'
    r'(?:[\s\-\.]?\d{3,4})?'
)
LINKEDIN_PATTERN = re.compile(
    r'https?://(?:www\.)?linkedin\.com/company/([A-Za-z0-9\-_]+)/?'
)
TWITTER_PATTERN = re.compile(
    # Exclude share/intent/utility paths: intent, share, home, search, hashtag, i/, settings, etc.
    r'https?://(?:www\.)?(?:twitter\.com|x\.com)/(?!intent|share|home|search|hashtag|i/|settings|notifications|messages|explore|login|signup|about)([A-Za-z0-9_]{1,50})(?:[/?]|$)'
)
COUNTRY_NAMES = {
    # ── North America ─────────────────────────────────────────────────────────
    "united states": "United States", "usa": "United States", "u.s.a": "United States",
    "u.s.a.": "United States", "us": "United States",
    "canada": "Canada", "mexico": "Mexico",

    # ── South America ─────────────────────────────────────────────────────────
    "brazil": "Brazil", "brasil": "Brazil",
    "argentina": "Argentina", "colombia": "Colombia", "chile": "Chile",
    "peru": "Peru", "venezuela": "Venezuela", "ecuador": "Ecuador",
    "uruguay": "Uruguay", "paraguay": "Paraguay", "bolivia": "Bolivia",

    # ── Western Europe ────────────────────────────────────────────────────────
    "united kingdom": "United Kingdom", "uk": "United Kingdom",
    "great britain": "United Kingdom", "england": "United Kingdom",
    "germany": "Germany", "deutschland": "Germany",
    "france": "France", "spain": "Spain", "italy": "Italy",
    "netherlands": "Netherlands", "holland": "Netherlands",
    "belgium": "Belgium", "switzerland": "Switzerland",
    "austria": "Austria", "sweden": "Sweden", "norway": "Norway",
    "finland": "Finland", "denmark": "Denmark", "ireland": "Ireland",
    "portugal": "Portugal", "luxembourg": "Luxembourg",
    "iceland": "Iceland", "liechtenstein": "Liechtenstein",
    "monaco": "Monaco", "andorra": "Andorra", "malta": "Malta",
    "cyprus": "Cyprus",

    # ── Eastern Europe ────────────────────────────────────────────────────────
    "poland": "Poland", "czech republic": "Czech Republic",
    "czech": "Czech Republic", "czechia": "Czech Republic",
    "slovakia": "Slovakia", "hungary": "Hungary",
    "romania": "Romania", "bulgaria": "Bulgaria",
    "croatia": "Croatia", "slovenia": "Slovenia",
    "serbia": "Serbia", "bosnia": "Bosnia and Herzegovina",
    "bosnia and herzegovina": "Bosnia and Herzegovina",
    "north macedonia": "North Macedonia", "albania": "Albania",
    "kosovo": "Kosovo", "montenegro": "Montenegro",
    "ukraine": "Ukraine", "belarus": "Belarus",
    "moldova": "Moldova", "russia": "Russia",
    "estonia": "Estonia", "latvia": "Latvia", "lithuania": "Lithuania",

    # ── Middle East ───────────────────────────────────────────────────────────
    "israel": "Israel", "turkey": "Turkey", "türkiye": "Turkey",
    "united arab emirates": "UAE", "uae": "UAE",
    "saudi arabia": "Saudi Arabia", "qatar": "Qatar",
    "kuwait": "Kuwait", "bahrain": "Bahrain", "oman": "Oman",
    "jordan": "Jordan", "iraq": "Iraq", "iran": "Iran",
    "lebanon": "Lebanon", "syria": "Syria", "yemen": "Yemen",
    "palestine": "Palestine",

    # ── South Asia ────────────────────────────────────────────────────────────
    "india": "India", "pakistan": "Pakistan",
    "bangladesh": "Bangladesh", "sri lanka": "Sri Lanka",
    "nepal": "Nepal", "bhutan": "Bhutan", "maldives": "Maldives",
    "afghanistan": "Afghanistan",

    # ── East Asia ─────────────────────────────────────────────────────────────
    "china": "China", "prc": "China",
    "japan": "Japan", "south korea": "South Korea",
    "korea": "South Korea", "north korea": "North Korea",
    "taiwan": "Taiwan", "hong kong": "Hong Kong", "macau": "Macau",
    "mongolia": "Mongolia",

    # ── Southeast Asia ────────────────────────────────────────────────────────
    "singapore": "Singapore", "malaysia": "Malaysia",
    "indonesia": "Indonesia", "thailand": "Thailand",
    "philippines": "Philippines", "vietnam": "Vietnam",
    "myanmar": "Myanmar", "burma": "Myanmar",
    "cambodia": "Cambodia", "laos": "Laos", "brunei": "Brunei",
    "timor-leste": "Timor-Leste", "east timor": "Timor-Leste",

    # ── Central Asia ──────────────────────────────────────────────────────────
    "kazakhstan": "Kazakhstan", "uzbekistan": "Uzbekistan",
    "kyrgyzstan": "Kyrgyzstan", "tajikistan": "Tajikistan",
    "turkmenistan": "Turkmenistan", "azerbaijan": "Azerbaijan",
    "georgia": "Georgia", "armenia": "Armenia",

    # ── Africa ────────────────────────────────────────────────────────────────
    "south africa": "South Africa", "egypt": "Egypt",
    "nigeria": "Nigeria", "kenya": "Kenya", "ethiopia": "Ethiopia",
    "ghana": "Ghana", "tanzania": "Tanzania", "uganda": "Uganda",
    "morocco": "Morocco", "algeria": "Algeria", "tunisia": "Tunisia",
    "libya": "Libya", "sudan": "Sudan", "cameroon": "Cameroon",
    "senegal": "Senegal", "zimbabwe": "Zimbabwe", "zambia": "Zambia",

    # ── Oceania ───────────────────────────────────────────────────────────────
    "australia": "Australia", "new zealand": "New Zealand",
    "papua new guinea": "Papua New Guinea", "fiji": "Fiji",

    # ── Multi-word aliases ────────────────────────────────────────────────────
    "republic of korea": "South Korea", "rok": "South Korea",
    "people's republic of china": "China",
    "russian federation": "Russia",
}

CSS_SELECTORS_NAME = [
    "[itemprop='name']", "[class*='company-name']", "[class*='exhibitor-name']",
    "[class*='vendor-name']", "[class*='brand-name']", "[class*='org-name']",
    "[class*='organization-name']", "[class*='sponsor-name']",
    "[data-field='name']", "[data-type='company']",
    "h1.company", "h2.company", "h3.company",
    ".company h1", ".company h2", ".company h3",
    ".exhibitor h1", ".exhibitor h2", ".exhibitor h3",
    ".vendor h1", ".vendor h2",
    "[class*='profile'] h1", "[class*='profile'] h2",
    "[class*='detail'] h1", "[class*='detail'] h2",
    "meta[property='og:title']",
]

CSS_SELECTORS_EMAIL = [
    "[itemprop='email']", "a[href^='mailto:']",
    "[class*='email']", "[class*='contact-email']",
    "[data-field='email']",
]

CSS_SELECTORS_PHONE = [
    "[itemprop='telephone']", "a[href^='tel:']",
    "[class*='phone']", "[class*='telephone']",
    "[class*='contact-phone']", "[data-field='phone']",
]

CSS_SELECTORS_ADDRESS = [
    "[itemprop='address']", "[itemtype*='PostalAddress']",
    "[class*='address']",
    # NOTE: [class*='location'] intentionally omitted — it too often matches
    # the venue/event location section on expo pages, not the company address.
    "[data-field='address']", "[class*='contact-address']",
]

CSS_SELECTORS_WEBSITE = [
    "[itemprop='url']", "a[class*='website']",
    "a[class*='web-link']", "a[class*='official']",
    "[data-field='website']", "[class*='official-site']",
]

CSS_SELECTORS_DESCRIPTION = [
    "[itemprop='description']",
    "[class*='company-desc']", "[class*='exhibitor-desc']",
    "[class*='about-us']", "[class*='overview']",
    "[class*='profile-desc']", "[class*='short-desc']",
    "[class*='company-about']", "[class*='exhibitor-about']",
    "[data-field='description']",
    # og:description only — meta[name='description'] is almost always site-level and handled below
    "meta[property='og:description']",
]

CSS_SELECTORS_CATEGORY = [
    "[itemprop='jobTitle']", "[class*='category']",
    "[class*='industry']", "[class*='sector']",
    "[class*='product-category']", "[class*='solution-type']",
    "[data-field='category']", "[class*='tag']", "[class*='tags']",
    "[class*='keywords']",
]

CSS_SELECTORS_COUNTRY = [
    "[itemprop='addressCountry']",
    "[class*='country']", "[class*='nation']",
    "[data-field='country']",
]

CSS_SELECTORS_BOOTH = [
    "[class*='booth']", "[class*='stand-number']",
    "[class*='hall']", "[class*='pavilion']",
    "[data-field='booth']", "[class*='exhibiting-at']",
]


def _clean_text(text: str, max_len: int = 500) -> str:
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:max_len]


def _extract_by_selectors(soup: BeautifulSoup, selectors: list[str]) -> str:
    for selector in selectors:
        try:
            elem = soup.select_one(selector)
            if elem:
                if elem.name == "meta":
                    val = elem.get("content", "")
                elif elem.name == "a" and selector.startswith("a[href^='mailto:']"):
                    val = elem.get("href", "").replace("mailto:", "")
                elif elem.name == "a" and selector.startswith("a[href^='tel:']"):
                    val = elem.get("href", "").replace("tel:", "")
                else:
                    val = elem.get_text(strip=True) or elem.get("content", "") or elem.get("href", "")
                if val and len(val.strip()) > 1:
                    return _clean_text(val)
        except Exception:
            continue
    return ""


def _extract_emails_from_text(text: str) -> list[str]:
    found = EMAIL_PATTERN.findall(text)
    cleaned = []
    for e in found:
        e = e.lower().strip(".,;")
        if "example" not in e and "noreply" not in e and "no-reply" not in e:
            cleaned.append(e)
    return list(dict.fromkeys(cleaned))[:3]


def _extract_phones_from_text(text: str) -> list[str]:
    found = PHONE_PATTERN.findall(text)
    cleaned = []
    for p in found:
        p = re.sub(r'\s+', ' ', p).strip()
        if len(re.sub(r'\D', '', p)) >= 7:
            cleaned.append(p)
    return list(dict.fromkeys(cleaned))[:2]


def _detect_country_from_text(text: str) -> str:
    """Return the country whose name appears earliest in the text.
    Position-aware: avoids bias from dictionary insertion order."""
    text_lower = text.lower()
    earliest_pos = len(text_lower) + 1
    earliest_country = ""
    for key, value in COUNTRY_NAMES.items():
        m = re.search(r'\b' + re.escape(key) + r'\b', text_lower)
        if m and m.start() < earliest_pos:
            earliest_pos = m.start()
            earliest_country = value
    return earliest_country


_WEBSITE_BLOCKLIST = {
    "twitter.com", "x.com", "facebook.com", "linkedin.com", "instagram.com",
    "youtube.com", "flickr.com", "pinterest.com", "reddit.com", "tumblr.com",
    "t.co", "bit.ly", "tinyurl.com", "ow.ly", "buff.ly", "hootsuite.com",
    "share.flipboard.com", "qualtrics.com", "typeform.com",
    "surveymonkey.com", "google.com", "apple.com", "microsoft.com",
    "amazon.com", "github.com", "wikipedia.org",
    # Calendar / meeting / invite links — never a company website
    "outlook.live.com", "outlook.com", "calendar.google.com",
    "calendar.yahoo.com", "calendly.com", "zoom.us", "teams.microsoft.com",
    "meet.google.com", "webex.com", "gotomeeting.com",
    # China ICP / government registration — never a company website
    "beian.gov.cn", "beian.miit.gov.cn", "mps.gov.cn",
    "gongan.gov.cn", "icp.gov.cn",
}


def _extract_website_from_links(soup: BeautifulSoup, base_domain: str) -> str:
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if not href.startswith("http"):
            continue
        parsed = urlparse(href)
        link_domain = parsed.netloc.lower().replace("www.", "")
        if not link_domain:
            continue
        if link_domain == base_domain.replace("www.", ""):
            continue
        if any(blocked in link_domain for blocked in _WEBSITE_BLOCKLIST):
            continue
        if "intent/tweet" in href or "share?" in href or "sharer" in href:
            continue
        text = a.get_text(strip=True).lower()
        title = a.get("title", "").lower()
        combined = text + " " + title + " " + href.lower()
        if any(kw in combined for kw in ["website", "www", "official", "visit", "homepage"]):
            return href
    return ""


def _parse_schema_org_organization(data: dict) -> dict:
    result = {}
    type_val = data.get("@type", "")
    if isinstance(type_val, list):
        type_val = type_val[0] if type_val else ""

    def get_field(d: dict, *keys: str) -> str:
        for k in keys:
            val = d.get(k, "")
            if isinstance(val, dict):
                val = val.get("name", val.get("@value", ""))
            if isinstance(val, list):
                val = val[0] if val else ""
            if val and isinstance(val, str):
                return _clean_text(val)
        return ""

    result["name"] = get_field(data, "name", "legalName")
    result["website"] = get_field(data, "url", "sameAs")
    result["email"] = get_field(data, "email")
    result["phone"] = get_field(data, "telephone", "phone")
    result["description"] = get_field(data, "description")

    address = data.get("address", {})
    if isinstance(address, dict):
        parts = [
            address.get("streetAddress", ""),
            address.get("addressLocality", ""),
            address.get("addressRegion", ""),
            address.get("postalCode", ""),
            address.get("addressCountry", ""),
        ]
        result["address"] = _clean_text(", ".join(p for p in parts if p))
        result["city"] = _clean_text(address.get("addressLocality", ""))
        result["country"] = _clean_text(address.get("addressCountry", ""))
    elif isinstance(address, str):
        result["address"] = _clean_text(address)

    return {k: v for k, v in result.items() if v}


def _count_populated(data: dict) -> int:
    core_fields = ["name", "website", "email", "phone", "address", "city", "country", "description", "category"]
    return sum(1 for f in core_fields if data.get(f))


# ── Private helper implementations ───────────────────────────────────────────

def _extract_schema_org(html: str, url: str) -> dict:
    if not html:
        return {}
    try:
        import extruct
        data = extruct.extract(
            html,
            base_url=url,
            syntaxes=["json-ld", "microdata", "opengraph"],
            uniform=True,
        )
    except Exception as e:
        logger.debug(f"extruct failed for {url}: {e}")
        return {}

    result = {}

    for item in data.get("json-ld", []):
        parsed = _parse_schema_org_organization(item)
        if _count_populated(parsed) > _count_populated(result):
            result.update(parsed)
        if isinstance(item.get("@graph"), list):
            for sub in item["@graph"]:
                if isinstance(sub, dict):
                    parsed_sub = _parse_schema_org_organization(sub)
                    if _count_populated(parsed_sub) > _count_populated(result):
                        result.update(parsed_sub)

    for item in data.get("microdata", []):
        if not result.get("name"):
            parsed = _parse_schema_org_organization(item)
            if _count_populated(parsed) > 0:
                result.update({k: v for k, v in parsed.items() if not result.get(k)})

    og_data = {}
    for item in data.get("opengraph", []):
        if item.get("og:type", "").lower() in ["website", "profile", "business.business"]:
            og_data["name"] = og_data.get("name") or item.get("og:site_name", "")
            og_data["description"] = og_data.get("description") or item.get("og:description", "")
            og_data["website"] = og_data.get("website") or item.get("og:url", "")

    for k, v in og_data.items():
        if v and not result.get(k):
            result[k] = v

    # Translate text fields if page is non-English
    if result:
        try:
            soup_tmp = BeautifulSoup(html[:2000], "lxml")
        except Exception:
            soup_tmp = BeautifulSoup(html[:2000], "html.parser")
        src_lang = _detect_lang(soup_tmp)
        for field in ("name", "description", "category", "address", "city", "country"):
            if result.get(field):
                result[field] = _translate(result[field], src_lang)

    if result:
        result["source_url"] = url
        result["extraction_method"] = "schema_org"
        populated = _count_populated(result)
        result["confidence_score"] = min(populated / 5.0, 1.0)

    return result


def _extract_rule_based(html: str, url: str) -> dict:
    if not html:
        return {}

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    src_lang = _detect_lang(soup)
    parsed_url = urlparse(url)
    base_domain = parsed_url.netloc.lower()
    full_text = soup.get_text(separator=" ", strip=True)
    result = {}

    name = _extract_by_selectors(soup, CSS_SELECTORS_NAME)
    # Guard: CSS selectors like [class*='detail'] h2 may match section headings
    # (e.g. "<h2>About</h2>" inside a vendor profile page). Discard and use h-tag fallback.
    _SECTION_HEADING_RE = re.compile(
        r'^(about|overview|contact|news|home|services?|products?|solutions?|'
        r'resources?|support|faq|privacy|terms|menu|search|'
        r'quick\s+links?|our\s+\w+|all\s+\w+)$',
        re.IGNORECASE,
    )
    if name and _SECTION_HEADING_RE.match(name.strip()):
        name = ""
    if not name:
        # Walk h1→h2→h3→h4; try main/article scope first then anywhere
        main_scope = soup.find("main") or soup.find("article")
        for tag in ("h1", "h2", "h3", "h4"):
            h = (main_scope.find(tag) if main_scope else None) or soup.find(tag)
            if h:
                candidate = _clean_text(h.get_text(strip=True))
                if candidate:
                    name = candidate
                    break
    if not name:
        # Page <title> ONLY when it contains a separator ("Company | Site Name")
        title_tag = soup.find("title")
        if title_tag:
            title_text = _clean_text(title_tag.get_text(strip=True))
            for sep in (" | ", " - ", " — ", " – "):
                if sep in title_text:
                    candidate = title_text.split(sep)[0].strip()
                    if 2 <= len(candidate) <= 100:
                        name = candidate
                        break
    if name:
        name = _translate(name, src_lang)
        # Strip trailing site name if still present (e.g. translated og:title)
        for sep in (" | ", " - ", " — ", " – "):
            if sep in name:
                candidate = name.split(sep)[0].strip()
                if 2 <= len(candidate) <= 100:
                    name = candidate
                    break
        if len(name) > 100:
            name = ""
    result["name"] = name

    email_from_selector = _extract_by_selectors(soup, CSS_SELECTORS_EMAIL)
    if email_from_selector:
        result["email"] = email_from_selector
    else:
        emails = _extract_emails_from_text(full_text)
        if emails:
            result["email"] = emails[0]

    phone_from_selector = _extract_by_selectors(soup, CSS_SELECTORS_PHONE)
    if phone_from_selector:
        result["phone"] = phone_from_selector
    else:
        phones = _extract_phones_from_text(full_text)
        if phones:
            result["phone"] = phones[0]

    # Scope address to <main>/<article> — prevents matching footer-level venue
    # address elements (e.g. class="saha-footer__address"). No full-page fallback:
    # a blank address is better than the wrong venue address.
    _main_for_addr = soup.find("main") or soup.find("article")
    if _main_for_addr:
        _addr_scope_soup = BeautifulSoup(str(_main_for_addr), "lxml")
        address = _extract_by_selectors(_addr_scope_soup, CSS_SELECTORS_ADDRESS)
    else:
        address = _extract_by_selectors(soup, CSS_SELECTORS_ADDRESS)

    # Fallback: find address as a <span> in an exhibitor contact card,
    # near the mailto: link. Common pattern on expo sites:
    #   div.exhibitor-info-card > div > div:nth-child(N) > span  (address plain text)
    # We walk up from the mailto link to find a shared card container, then
    # scan sibling <span> elements for address-like text.
    if not address:
        _mailto_el = soup.find("a", href=lambda h: h and h.startswith("mailto:"))
        if _mailto_el:
            _container = _mailto_el
            for _ in range(6):  # walk up at most 6 levels
                _container = getattr(_container, "parent", None)
                if _container is None:
                    break
                # Only inspect containers that hold multiple child divs (contact card)
                _child_divs = _container.find_all("div", recursive=False)
                if len(_child_divs) < 2:
                    continue
                for _span in _container.find_all("span"):
                    _stxt = _span.get_text(strip=True)
                    # Address-like: has digits AND spaces, not an email/URL/domain
                    _is_domain = re.search(r'\.\w{2,6}(/|$)', _stxt)
                    if (15 < len(_stxt) < 250
                            and re.search(r'\d', _stxt)
                            and " " in _stxt              # addresses have spaces; URLs don't
                            and "@" not in _stxt
                            and "://" not in _stxt
                            and not _is_domain            # not a bare domain like www.x.com
                            and not re.match(r'^(?:Hall|Stand|Booth)\b', _stxt, re.I)):
                        address = _stxt
                        break
                if address:
                    break

    if address:
        result["address"] = _translate(address, src_lang)

    website = _extract_by_selectors(soup, CSS_SELECTORS_WEBSITE)
    # Discard if it's a calendar/tracking/redirect URL (>200 chars or known bad domain)
    if website:
        _w_dom = urlparse(website).netloc.lower().replace("www.", "")
        if len(website) > 200 or any(bd in _w_dom for bd in _WEBSITE_BLOCKLIST):
            website = ""
    if not website:
        website = _extract_website_from_links(soup, base_domain)
    if website and website.startswith("http") and base_domain not in website:
        result["website"] = website

    # Strip leading section-heading words that get concatenated when a block extractor
    # grabs e.g. <div class="about-us"><h2>About</h2><p>Company text...</p></div>
    # via get_text(strip=True) → "About Company text..."
    _STRIP_HEADING_PREFIX_RE = re.compile(
        r'^(?:about|overview|profile|description|summary|introduction|'
        r'info|information|history|vision|mission|company|who we are)\s*',
        re.IGNORECASE,
    )

    description = _extract_by_selectors(soup, CSS_SELECTORS_DESCRIPTION)
    if description:
        description = _STRIP_HEADING_PREFIX_RE.sub("", description).strip()
    if not description:
        # Look for description as a sibling of the heading's parent container
        # (common in Next.js vendor profiles: h4 is in a titles div, description is a sibling div)
        _hx = soup.find(["h1", "h2", "h3", "h4"])
        if _hx:
            for _sib in _hx.parent.find_next_siblings():
                _sib_text = _sib.get_text(separator=" ", strip=True)
                if len(_sib_text) > 80 and _sib.name not in ("script", "style", "nav", "footer", "header"):
                    description = _STRIP_HEADING_PREFIX_RE.sub("", _clean_text(_sib_text, 400)).strip()
                    break
    if not description:
        # Fallback: first long <p> in main content
        _scope = soup.find("main") or soup.find("article") or soup
        for p in _scope.find_all("p"):
            p_text = p.get_text(strip=True)
            if len(p_text) > 80:
                description = _clean_text(p_text, 400)
                break
    if not description:
        # Last resort: meta[name='description'] — often site-level, only use if >30 chars
        _meta_desc = soup.find("meta", {"name": "description"})
        if _meta_desc:
            _meta_val = (_meta_desc.get("content") or "").strip()
            if len(_meta_val) > 30:
                description = _meta_val
    if description:
        result["description"] = _translate(description, src_lang)

    category = _extract_by_selectors(soup, CSS_SELECTORS_CATEGORY)
    if category:
        # Reject if it looks like an event/expo title (too long, or contains exhibition keywords)
        _is_event_name = re.search(
            r'\b(exhibition|expo|fair|congress|summit|conference|symposium|forum|trade\s+show)\b',
            category, re.IGNORECASE
        )
        if _is_event_name or len(category) > 60:
            category = ""
    if category:
        result["category"] = _translate(category, src_lang)

    country = _extract_by_selectors(soup, CSS_SELECTORS_COUNTRY)
    if country and len(country) > 50:  # nav menu / long list, not a real country value
        country = ""
    if not country:
        # Only run text-based country detection when name was found via structured selector
        # (not h-tag fallback) to avoid false positives on listing pages with many country names
        _has_structured_name = bool(_extract_by_selectors(soup, CSS_SELECTORS_NAME))
        if _has_structured_name or result.get("address") or result.get("booth_number"):
            text_for_detection = full_text[:2000]
            if src_lang not in ("en", "id", "unknown"):
                text_for_detection = _translate(text_for_detection[:500], src_lang) + " " + text_for_detection
            country = _detect_country_from_text(text_for_detection)
    if country:
        result["country"] = country

    booth = _extract_by_selectors(soup, CSS_SELECTORS_BOOTH)
    # Reject if booth text is too long — likely a marketing block, not a booth number
    if booth and len(booth) <= 60:
        result["booth_number"] = _translate(booth, src_lang)

    # Social links — scope to main content to avoid grabbing site-wide footer links
    _main = soup.find("main") or soup.find("article") or soup.find(id=re.compile(r"^(content|main)$", re.I))
    _social_html = str(_main) if _main else html
    linkedin_match = LINKEDIN_PATTERN.search(_social_html)
    if linkedin_match:
        result["linkedin"] = linkedin_match.group(0)

    twitter_match = TWITTER_PATTERN.search(_social_html)
    if twitter_match:
        result["twitter"] = twitter_match.group(0)

    # Extract structured label-value pairs (works well on directory/expo sites)
    label_pairs = _extract_label_value_pairs(soup, src_lang)
    for field, value in label_pairs.items():
        if value and not result.get(field):
            # For count fields, keep only the number
            if field in ("exhibitor_count", "visitor_count", "scale"):
                num_match = re.search(r'[\d,\.]+', value)
                if num_match:
                    unit = ""
                    if "㎡" in value or "m²" in value.lower() or "sqm" in value.lower():
                        unit = "㎡"
                    value = num_match.group(0).replace(",", "") + unit
            result[field] = value

    # Booth / hall / stand — regex fallback on full text (handles "Hall: 2 | Stand: 2C-05")
    if not result.get("booth_number"):
        _booth_re = re.search(
            r'(?:Hall\s*[:\-]\s*\w[\w\-]*(?:\s*[|,]\s*Stand\s*[:\-]\s*[\w\-]+)?'
            r'|Stand\s*[:\-]\s*[\w\-]+'
            r'|Booth\s*[:\-]\s*[\w\-]+)',
            full_text, re.IGNORECASE
        )
        if _booth_re:
            result["booth_number"] = _booth_re.group(0).strip()[:60]

    # City from address fallback
    address_val = result.get("address", "")
    if address_val and not result.get("city"):
        parts = [p.strip() for p in address_val.split(",")]
        if len(parts) >= 2:
            result["city"] = parts[-2].strip()[:100]

    result = {k: v for k, v in result.items() if v}
    if result:
        result["source_url"] = url
        result["extraction_method"] = "rule_based"
        populated = _count_populated(result)
        result["confidence_score"] = min(populated / 6.0, 1.0)

    return result


def _extract_with_jina_llm(url: str, context: str = "") -> dict:
    """
    Fetch clean Markdown from Jina AI Reader (free, no key required), then pass to LLM.
    Falls back to empty dict if Jina fetch fails or LLM is disabled.
    """
    import asyncio
    from backend.tools.fetch_tools import _fetch_jina_markdown

    settings = get_settings()

    # Run the async Jina fetch in whatever context we're in
    try:
        try:
            loop = asyncio.get_event_loop()
            is_running = loop.is_running()
        except RuntimeError:
            is_running = False

        if is_running:
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                markdown = pool.submit(asyncio.run, _fetch_jina_markdown(url)).result(timeout=90)
        else:
            markdown = asyncio.run(_fetch_jina_markdown(url))
    except Exception as e:
        logger.warning(f"[JINA-LLM] Jina fetch failed for {url}: {e}")
        return {}

    if not markdown or len(markdown) < 100:
        logger.debug(f"[JINA-LLM] Markdown too short for {url}")
        return {}

    logger.info(f"[JINA-LLM] Got {len(markdown):,} chars of markdown from {url}")
    return _extract_with_llm_text(markdown, url, context, source_label="jina")


def _extract_with_llm_text(text: str, url: str, context: str = "", source_label: str = "llm") -> dict:
    """
    Run LLM vendor extraction on arbitrary text (markdown or plain text).
    Trims to token budget, returns vendor dict or empty.
    """
    settings = get_settings()
    if not settings.effective_llm_enabled:
        logger.debug("LLM fallback disabled — skipping")
        return {}

    try:
        import tiktoken
        from openai import OpenAI

        lines = [ln.strip() for ln in text.split("\n") if ln.strip() and len(ln.strip()) > 3]
        cleaned = "\n".join(lines)
        cleaned = cleaned[:settings.llm_max_input_chars]

        enc = tiktoken.encoding_for_model("gpt-4o-mini")
        tokens = len(enc.encode(cleaned))
        if tokens > 400:
            cleaned = enc.decode(enc.encode(cleaned)[:400])

        client = OpenAI(api_key=settings.openai_api_key)

        system_prompt = (
            "You are a data extraction assistant. Extract ALL company/vendor information "
            "visible on this page. Return ONLY a valid JSON object — no explanation, no markdown. "
            "Use snake_case keys. Include any field you find: name, website, email, phone, "
            "address, city, country, category, description, linkedin, twitter, booth_number, "
            "event_name, event_location, event_date, specialized, classification, products, "
            "services, certifications, founded_year, employees, revenue, or any other relevant field. "
            "Omit keys you are not confident about. Values must be strings."
        )
        context_suffix = f"\nEvent context: {context[:200]}" if context else ""
        user_prompt = f"URL: {url}{context_suffix}\n\n{cleaned}"

        create_kwargs: dict = {
            "model": settings.openai_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "max_completion_tokens": 4000,
        }
        if settings.model_supports_temperature:
            create_kwargs["temperature"] = 0.0
            create_kwargs["response_format"] = {"type": "json_object"}

        response = client.chat.completions.create(**create_kwargs)
        content = (response.choices[0].message.content or "").strip()

        if not content:
            logger.warning(f"[{source_label.upper()}] Empty response for {url}")
            return {}

        try:
            raw = json.loads(content)
        except json.JSONDecodeError:
            m = re.search(r"\{[^{}]*\}", content, re.DOTALL)
            if not m:
                logger.warning(f"[{source_label.upper()}] No JSON found in response for {url}")
                return {}
            raw = json.loads(m.group(0))

        # Accept ALL fields the LLM returns — no whitelist filter
        # Normalize keys to snake_case, drop empty values, cap at 2000 chars per field
        _SKIP = {"source_url", "extraction_method", "confidence_score"}
        result = {}
        for k, v in raw.items():
            k_norm = re.sub(r"[^a-z0-9_]", "_", k.lower().strip()).strip("_")
            if k_norm and k_norm not in _SKIP and v and str(v).strip():
                result[k_norm] = str(v).strip()[:2000]

        if result:
            result["source_url"] = url
            result["extraction_method"] = source_label
            populated = _count_populated(result)
            result["confidence_score"] = min(populated / 5.0, 1.0)
            logger.info(f"[{source_label.upper()}] Extracted {populated} fields from {url} (~{tokens} tokens in)")

        return result

    except Exception as e:
        logger.warning(f"[{source_label.upper()}] Extraction failed for {url}: {type(e).__name__}: {e}")
        return {}


def _extract_with_llm(html: str, url: str, context: str = "") -> dict:
    """Convert HTML → plain text, then run LLM vendor extraction."""
    try:
        import html2text as h2t
        converter = h2t.HTML2Text()
        converter.ignore_links = True
        converter.ignore_images = True
        converter.body_width = 0
        text = converter.handle(html)
    except Exception as e:
        logger.warning(f"[LLM] html2text conversion failed for {url}: {e}")
        text = html

    return _extract_with_llm_text(text, url, context, source_label="llm")


def _merge_vendor_data(sources: list[dict]) -> dict:
    if not sources:
        return {}
    if len(sources) == 1:
        return sources[0]

    priority = {"schema_org": 3, "rule_based": 2, "llm": 1, "enrichment": 1, "unknown": 0}
    sources_sorted = sorted(sources, key=lambda x: priority.get(x.get("extraction_method", ""), 0), reverse=True)

    merged = {}
    fields = [
        "name", "website", "email", "phone", "address", "city", "country",
        "category", "description", "linkedin", "twitter", "booth_number",
        "event_name", "event_location", "event_date",
    ]

    for field in fields:
        for source in sources_sorted:
            val = source.get(field, "")
            if val and isinstance(val, str) and len(val.strip()) > 1:
                merged[field] = val.strip()
                break

    for source in sources_sorted:
        for k, v in source.items():
            if k not in merged and v and k not in ["extraction_method", "confidence_score", "source_url"]:
                merged[k] = v

    methods_used = list(dict.fromkeys(
        s.get("extraction_method", "unknown") for s in sources if s.get("extraction_method")
    ))
    merged["extraction_method"] = "+".join(methods_used) if len(methods_used) > 1 else (methods_used[0] if methods_used else "unknown")

    if sources_sorted:
        merged["source_url"] = sources_sorted[0].get("source_url", "")

    populated = _count_populated(merged)
    merged["confidence_score"] = min(populated / 7.0, 1.0)

    return merged


def _validate_vendor(vendor: dict) -> dict:
    if not vendor:
        return {}

    BAD_NAME_PATTERNS = re.compile(
        r'^(partner articles?|read more|click here|learn more|see more|view all|'
        r'show more|load more|next|prev(ious)?|page \d+|\d+ of \d+|'
        r'home|about|contact|news|blog|events?|resources?|solutions?|products?|'
        r'services?|support|faq|sitemap|privacy|terms|cookies?|'
        r'sign in|log in|register|subscribe|newsletter|follow us|share|'
        r'twitter|facebook|linkedin|instagram|youtube|flickr|'
        r'menu|navigation|header|footer|sidebar|search|'
        r'copyright|all rights reserved|\d{4}|'
        # Listing/directory page titles — not real company names
        r'exhibitors?(\s+(list|directory|search|results?|profile|information|details?))?|'
        r'vendors?(\s+(list|directory|search|results?|profile|information))?|'
        r'companies|company\s+(list|directory|profile|information|details?|overview|introduction)?|'
        # Generic page/section titles that are never a real company name
        r'profile|company\s*profile|exhibitor\s*profile|vendor\s*profile|'
        r'company\s*(info(rmation)?|details?|overview|introduction)|'
        r'exhibitor\s*(info(rmation)?|details?|overview)|'
        r'vendor\s*(info(rmation)?|details?)|'
        r'about\s+(the\s+)?company|about\s+(the\s+)?exhibitor|'
        r'enterprise\s*(profile|info(rmation)?|introduction)|'
        r'participants?(\s+list)?|sponsors?(\s+list)?|'
        r'our\s+(exhibitors?|vendors?|sponsors?|partners?)|'
        r'all\s+(exhibitors?|companies|vendors?)|'
        r'quick\s+links?|'
        r'exhibitor\s+list|vendor\s+list|company\s+list|'
        # Translated Chinese navigation menu items (post-translation catches these)
        r'overseas\s+(exhibition|expo|fair|show|event|engineering)|'
        r'domestic\s+(exhibition|expo|fair|show)|'
        r'(exhibition|booth|stand)\s+(construction|engineering|setup|service)|'
        r'(organizational?|organisation(al)?)\s+(structure|chart)|'
        r'corporate\s+(culture|values?|history|mission|vision)|'
        r'company\s+culture|'
        r'(join|recruit)\s+us|careers?|recruitment|'
        r'news\s+(center|centre)|company\s+news|latest\s+news|'
        r'more\s+(details?|info(rmation)?)|see\s+all|'
        r'(aviation|aerospace|defense|automotive|maritime|marine|security|'
        r'fire\s*fighting|surveillance).*(series|sector|type|category|zone)|'
        # Chinese navigation items (direct match — fallback when translation fails)
        r'公司介绍|企业介绍|关于我们|联系我们|加入我们|组织结构|企业文化|'
        r'境外展|自办展|国内展示工程|海外搭建工程|展会新闻|新闻中心|展会动态|'
        r'更多|查看更多|了解更多|返回首页|网站地图|'
        r'航空.*防务|汽车.*汽配|船舶.*海事|安防.*消防|'
        # Chinese news-headline patterns (verb + object constructs = sentences, not company names)
        r'.*召开.*会议|.*举行.*会议|.*举办.*活动|.*签署.*协议|.*开展.*合作|.*发布.*报告|'
        r'.*举行.*仪式|.*完成.*工作|.*取得.*成果|.*实现.*目标)$',
        re.IGNORECASE,
    )

    SOCIAL_SHARING_DOMAINS = {
        "twitter.com", "x.com", "facebook.com", "flickr.com", "instagram.com",
        "youtube.com", "pinterest.com", "reddit.com", "tumblr.com",
        "t.co", "bit.ly", "tinyurl.com", "ow.ly", "buff.ly",
        "share.flipboard.com", "qualtrics.com", "typeform.com",
        "surveymonkey.com", "forms.gle", "docs.google.com",
    }

    cleaned = dict(vendor)

    name = cleaned.get("name", "")
    if name:
        name = re.sub(r'\s+', ' ', name).strip()

        # ── Translate non-English names BEFORE pattern matching ───────────────
        # This is the last line of defence: even if upstream _extract_rule_based
        # skipped translation (e.g. lang="unknown"), we force-translate here so
        # BAD_NAME_PATTERNS can do its job on the English equivalent.
        _has_cjk = any('一' <= c <= '鿿' for c in name)
        _has_cyr = any('Ѐ' <= c <= 'ӿ' for c in name)
        if _has_cjk:
            _translated = _translate(name, "zh")
            if _translated and _translated != name:
                name = _translated
                cleaned["name"] = name
        elif _has_cyr:
            _translated = _translate(name, "ru")
            if _translated and _translated != name:
                name = _translated
                cleaned["name"] = name

        # ── Standard validation ───────────────────────────────────────────────
        if len(name) < 2 or len(name) > 300:
            cleaned.pop("name", None)
        elif re.match(r'^\d+$', name):
            cleaned.pop("name", None)
        elif BAD_NAME_PATTERNS.match(name):
            cleaned.pop("name", None)
        elif len(name.split()) > 10:
            # Long sentences (e.g. news headlines translated from Chinese) → not company names
            cleaned.pop("name", None)
        # Reject names ending in exhibition/category suffixes — never a real company name
        elif re.search(r'\b(series|zone|pavilion|sector|segment|division|corridor|gallery|court)\s*$', name, re.IGNORECASE):
            cleaned.pop("name", None)
        # Reject slash-separated category shorthand: "X/Y" or "X/Y/Z"
        # Real company names rarely contain slashes; exhibition categories always do
        elif name.count("/") >= 2:
            cleaned.pop("name", None)
        elif name.count("/") == 1 and len(name) <= 40 and not re.search(r'[A-Z][a-z]{2,}.*[A-Z][a-z]{2,}', name):
            # "Energy/Power" (len<40, no mixed-case proper nouns) → category, not a company
            cleaned.pop("name", None)
        else:
            cleaned["name"] = name

    if not cleaned.get("name"):
        return {}

    email = cleaned.get("email", "")
    if email and not EMAIL_PATTERN.match(email):
        cleaned.pop("email", None)

    website = cleaned.get("website", "")
    if website:
        if not website.startswith(("http://", "https://")):
            cleaned.pop("website", None)
        elif len(website) > 200:
            # Calendar links, tracking URLs, redirect chains — not real vendor websites
            cleaned.pop("website", None)
        else:
            w_domain = urlparse(website).netloc.lower().replace("www.", "")
            if any(sd in w_domain for sd in SOCIAL_SHARING_DOMAINS):
                cleaned.pop("website", None)

    phone = cleaned.get("phone", "")
    if phone:
        digits = re.sub(r'\D', '', phone)
        if len(digits) < 7 or len(digits) > 20:
            cleaned.pop("phone", None)

    country = cleaned.get("country", "")
    if country:
        country_lower = country.lower().strip()
        normalized = COUNTRY_NAMES.get(country_lower, "")
        if not normalized:
            word_match = next(
                (COUNTRY_NAMES[k] for k in COUNTRY_NAMES if re.search(r'\b' + re.escape(k) + r'\b', country_lower)),
                ""
            )
            normalized = word_match
        if normalized:
            cleaned["country"] = normalized
        else:
            cleaned.pop("country", None)

    description = cleaned.get("description", "")
    if description:
        if len(description) < 20:
            cleaned.pop("description", None)
        else:
            cleaned["description"] = description[:1000]

    for field in ["city", "address", "category", "booth_number", "event_name"]:
        val = cleaned.get(field, "")
        if val:
            cleaned[field] = re.sub(r'\s+', ' ', str(val)).strip()[:500]

    populated = _count_populated(cleaned)
    cleaned["confidence_score"] = min(populated / 7.0, 1.0)
    cleaned["is_valid"] = bool(cleaned.get("name"))

    return cleaned


# ── Public @tool wrappers ─────────────────────────────────────────────────────

class VendorRecord(BaseModel):
    name: str = Field(default="", description="Company name")
    website: Optional[str] = Field(default=None, description="Official website URL")
    email: Optional[str] = Field(default=None, description="Contact email")
    phone: Optional[str] = Field(default=None, description="Phone number")
    address: Optional[str] = Field(default=None, description="Full address")
    city: Optional[str] = Field(default=None, description="City")
    country: Optional[str] = Field(default=None, description="Country")
    category: Optional[str] = Field(default=None, description="Industry or product category")
    description: Optional[str] = Field(default=None, description="Company description")
    linkedin: Optional[str] = Field(default=None, description="LinkedIn company URL")
    twitter: Optional[str] = Field(default=None, description="Twitter/X URL")
    booth_number: Optional[str] = Field(default=None, description="Booth or stand number")
    event_name: Optional[str] = Field(default=None, description="Event where exhibiting")
    event_location: Optional[str] = Field(default=None, description="Event venue/city")
    event_date: Optional[str] = Field(default=None, description="Event date")
    source_url: str = Field(default="", description="URL where vendor was found")
    extraction_method: str = Field(default="unknown", description="schema_org|rule_based|llm")
    confidence_score: float = Field(default=0.0, ge=0.0, le=1.0)


@tool
def extract_schema_org(html: str, url: str) -> dict:
    """
    Extract vendor data using schema.org JSON-LD, microdata, and OpenGraph markup.
    Zero LLM — pure structured data extraction. Returns VendorRecord dict or empty.
    """
    return _extract_schema_org(html, url)


@tool
def extract_rule_based(html: str, url: str) -> dict:
    """
    Extract vendor data using 100+ CSS selectors, regex, and heuristics.
    Zero LLM — rule-based extraction. Returns VendorRecord dict.
    """
    return _extract_rule_based(html, url)


@tool
def extract_with_llm(html: str, url: str, context: str = "") -> dict:
    """
    FALLBACK ONLY: Extract vendor data using GPT when schema_org and rule_based fail.
    Sends max 400 tokens of cleaned text to minimize token usage.
    """
    return _extract_with_llm(html, url, context)


@tool
def merge_vendor_data(sources: list[dict]) -> dict:
    """
    Merge vendor data from multiple extraction sources.
    Priority: schema_org > rule_based > llm > enrichment.
    Returns the best combined record.
    """
    return _merge_vendor_data(sources)


@tool
def validate_vendor(vendor: dict) -> dict:
    """
    Validate and clean a vendor record. Normalizes fields, removes invalid data,
    computes final confidence score. Returns cleaned vendor dict.
    """
    return _validate_vendor(vendor)


@tool
def discover_vendor_urls(url: str, max_urls: int = 100) -> list[str]:
    """
    Discover individual vendor/exhibitor profile URLs from a listing or expo page.
    The page must have been fetched first with fetch_page or fetch_pages_batch.

    Strategy:
    - Finds all same-domain internal links
    - Scores each URL by how likely it is a vendor profile (keyword matching + pattern repetition)
    - Returns up to max_urls ranked URLs, excluding the source URL itself

    Returns: list of candidate vendor profile URLs (strings)
    """
    from collections import defaultdict
    from urllib.parse import urlparse, urljoin
    from backend.tools.fetch_tools import get_cached_html

    html = get_cached_html(url)
    if not html:
        logger.warning(f"[DISCOVER] No cached HTML for {url} — fetch it first")
        return []

    soup = BeautifulSoup(html, "lxml")
    base_parsed = urlparse(url)
    base_domain = base_parsed.netloc
    source_path = url.rstrip("/")

    # Keywords that strongly suggest a vendor/exhibitor profile URL path
    VENDOR_PATH_KEYWORDS = [
        "exhibitor", "vendor", "company", "booth", "brand", "participant",
        "sponsor", "supplier", "partner", "profile", "member", "katilimci",
        "firma", "expositor", "empresa", "exposant", "aussteller", "unternehmen",
        "detail", "listing", "directory",
    ]

    # Keywords that suggest navigation / utility pages (not vendor profiles)
    NAV_PATH_KEYWORDS = [
        "login", "register", "signup", "sign-up", "contact", "about", "faq",
        "privacy", "terms", "cookie", "sitemap", "search", "tag", "category",
        "news", "blog", "press", "media", "career", "jobs", "help",
    ]

    def make_absolute(href: str) -> str | None:
        href = href.strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:") or href.startswith("javascript:"):
            return None
        if href.startswith("//"):
            return "https:" + href
        if href.startswith("/"):
            return f"{base_parsed.scheme}://{base_domain}{href}"
        if href.startswith("http"):
            return href
        return urljoin(url, href)

    # Collect all internal links
    candidate_scores: dict[str, int] = {}
    pattern_groups: dict[str, list[str]] = defaultdict(list)

    for a in soup.find_all("a", href=True):
        href = make_absolute(a.get("href", ""))
        if not href:
            continue
        parsed = urlparse(href)
        # Must be same domain
        if base_domain not in parsed.netloc:
            continue
        # Skip source URL itself
        if href.rstrip("/") == source_path:
            continue
        # Skip URLs with query strings that look like search/filter
        if len(parsed.query) > 30:
            continue

        path_lower = parsed.path.lower()

        # Skip obvious nav/utility pages
        if any(kw in path_lower for kw in NAV_PATH_KEYWORDS):
            continue

        # Score based on path keyword match
        score = 0
        for kw in VENDOR_PATH_KEYWORDS:
            if kw in path_lower:
                score += 3

        # Detect repeated patterns (listing grid) — group by first 2 path segments
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) >= 2:
            pattern_key = "/" + "/".join(parts[:2])
        elif len(parts) == 1:
            pattern_key = "/" + parts[0]
        else:
            pattern_key = "/"

        pattern_groups[pattern_key].append(href)

        # Start accumulating with at least 0 score for valid internal links
        if href not in candidate_scores:
            candidate_scores[href] = score

    # Boost URLs that share a pattern with many others (likely a listing grid)
    for pattern, hrefs in pattern_groups.items():
        if len(hrefs) >= 3:  # ≥3 URLs with same path prefix = likely individual profiles
            for href in hrefs:
                if href in candidate_scores:
                    candidate_scores[href] += 5  # strong listing signal

    # Filter: only keep URLs with score > 0 (some signal)
    filtered = [(href, score) for href, score in candidate_scores.items() if score > 0]
    filtered.sort(key=lambda x: x[1], reverse=True)

    result = [href for href, _ in filtered[:max_urls]]
    logger.info(f"[DISCOVER] {url} → {len(result)} candidate vendor URLs (from {len(candidate_scores)} total links)")
    return result


def _parse_exhibitor_pdf_table(markdown: str, source_url: str) -> list[dict]:
    """
    Parse markdown table format returned by Firecrawl for structured PDFs.

    Handles formats like:
      | NO. | COMPANY NAME | COUNTRY | BOOTH NO. |   (ADAS-style)
      |  | Exhibitor | Country | Stand Number | Pavillion |  (DSA-style, empty NO col)

    Column positions are auto-detected from the header row using the FULL cell
    list (including empty cells), so an empty number column doesn't shift indices.
    Headers repeat after page breaks ('* * *') and are re-detected each time.
    """
    vendors: list[dict] = []
    seen_names: set[str] = set()

    # Column indices into the inner cell list (split('|')[1:-1])
    col_name = col_country = col_booth = -1
    col_name_found = False

    def _inner_cells(line: str) -> list[str]:
        """Split '| a | b | c |' → ['a', 'b', 'c'] (inner cells, stripped)."""
        parts = line.split('|')
        return [p.strip() for p in parts[1:-1]]  # skip leading/trailing ''

    def _get(cells: list[str], idx: int) -> str:
        return cells[idx].strip() if 0 <= idx < len(cells) else ""

    def _resolve_country(raw: str) -> str:
        """Normalise 'UK / SINGAPORE' → 'United Kingdom', 'TURKEY' → 'Turkey' etc."""
        if not raw:
            return ""
        for part in re.split(r'[/,&]', raw):
            candidate = part.strip().lower()
            direct = COUNTRY_NAMES.get(candidate)
            if direct:
                return direct
            # Word-boundary search
            match = next((v for k, v in COUNTRY_NAMES.items()
                          if re.search(r'\b' + re.escape(k) + r'\b', candidate)), "")
            if match:
                return match
        return raw.split('/')[0].strip()  # fallback: first segment as-is

    HEADER_NAME_VALS = {'COMPANY NAME', 'COMPANY', 'EXHIBITOR', 'EXHIBITOR NAME',
                        'NAME', 'FIRM', 'PARTICIPANT', 'EXHIBITOR/PARTICIPANT'}
    HEADER_NO_VALS   = {'NO.', 'NO', 'SR.NO.', 'S.NO', '#', 'NUMBER', 'NUM', 'SN'}
    HEADER_CTY_VALS  = {'COUNTRY', 'NATION', 'NATIONALITY'}
    HEADER_BOOTH_KWS = ('BOOTH', 'STAND', 'HALL', 'PAVILION', 'STALL', 'STAND NUMBER')

    for line in markdown.split('\n'):
        stripped = line.strip()
        if not stripped.startswith('|'):
            continue

        inner = _inner_cells(stripped)  # preserves empty cells → correct indices

        # ── separator row ─────────────────────────────────────────────────────
        if all(not c or re.match(r'^-+$', c) for c in inner):
            continue

        upper_inner = [c.upper() for c in inner]

        # ── header detection ──────────────────────────────────────────────────
        has_name_col = any(c in HEADER_NAME_VALS for c in upper_inner)
        if has_name_col:
            # Re-detect column positions each time a header appears (page repeats)
            col_name = col_country = col_booth = -1
            for j, cu in enumerate(upper_inner):
                if cu in HEADER_NAME_VALS:
                    col_name = j
                elif cu in HEADER_CTY_VALS:
                    col_country = j
                elif any(kw in cu for kw in HEADER_BOOTH_KWS):
                    col_booth = j
            col_name_found = col_name >= 0
            continue

        if not col_name_found:
            continue

        # ── data row ──────────────────────────────────────────────────────────
        name = _get(inner, col_name)
        if not name or len(name) < 2 or re.match(r'^-+$', name):
            continue
        # Reject re-rendered header labels
        if name.upper() in HEADER_NAME_VALS | HEADER_NO_VALS:
            continue

        name_key = name.lower()
        if name_key in seen_names:
            continue
        seen_names.add(name_key)

        country = _resolve_country(_get(inner, col_country))
        booth   = _get(inner, col_booth)[:60]

        vendor: dict = {"name": name, "source_url": source_url, "extraction_method": "pdf_table"}
        if country:
            vendor["country"] = country
        if booth:
            vendor["booth_number"] = booth

        populated = sum(1 for k in ("name", "country", "booth_number") if vendor.get(k))
        vendor["confidence_score"] = min(populated / 3.0, 1.0) * 0.85
        vendors.append(vendor)

    logger.info(f"[PDF-TABLE] Parsed {len(vendors)} vendors from markdown table")
    return vendors


def _parse_exhibitor_pdf_markdown(markdown: str, source_url: str) -> list[dict]:
    """
    Parse Jina-rendered PDF exhibitor list markdown into a list of vendor dicts.

    Jina renders the PDF as double-newline-separated blocks:
        '1 SADAS  Afghanistan \\n\\n'
        '2 AMDA Foundation Limited  Australia  4500A  Team Defence \\n\\n'
        'Australia (Australia) \\n\\n'
        '3 ARB 4x4 ACCESSORIES  Australia \\n\\n'

    Strategy:
    1. Split content by \\n\\n → get paragraph blocks
    2. A block starting with a number = new exhibitor entry
    3. Non-numbered continuation blocks belong to the previous entry
    4. Within each entry, split by 2+ spaces → columns: name, country, booth, pavilion
    """
    # Extract markdown content section
    md_start = markdown.find("Markdown Content:")
    content   = markdown[md_start + len("Markdown Content:"):] if md_start >= 0 else markdown

    # Split into paragraph blocks (Jina uses \n\n as block separator)
    blocks = [b.strip() for b in re.split(r'\n\n+', content)]
    blocks = [b for b in blocks if b]

    ENTRY_START = re.compile(r'^(\d+)\s{1,2}(.+)', re.DOTALL)
    SKIP_RE     = re.compile(
        r'EXHIBITOR LIST|PARTICIPATING COMPANIES|Updated as of|'
        r'Country\s*/\s*Region|Exhibitor\s+Country|^Page \d+$|'
        r'DEFENCE SERVICE|NATIONAL SECURITY|aca\s+Pav',
        re.IGNORECASE,
    )

    def _parse_entry(text: str) -> dict | None:
        """Parse combined entry text: '{name}  {country}  {booth}  {pavilion}'"""
        # Collapse inner newlines to single space first
        text = re.sub(r'\s*\n\s*', ' ', text).strip()
        # Split on 2+ spaces → columns
        parts = [p.strip() for p in re.split(r'\s{2,}', text) if p.strip()]
        if not parts:
            return None

        name = parts[0]
        # Reject very short/long names or pure digits
        if len(name) < 2 or len(name) > 200 or re.match(r'^\d+$', name):
            return None
        # Reject header/footer lines that slipped through
        if SKIP_RE.search(name):
            return None

        country = ""
        booth   = ""

        if len(parts) >= 2:
            raw = re.sub(r'\s*\([^)]+\)\s*$', '', parts[1]).strip()
            raw_lower = raw.lower()
            # Check if it's a known country
            is_known_country = (
                raw_lower in COUNTRY_NAMES or
                any(re.search(r'\b' + re.escape(k) + r'\b', raw_lower) for k in COUNTRY_NAMES)
            )
            # Check if it looks like a booth code (e.g. "4500A", "H1-23")
            is_booth_code = bool(re.match(r'^[A-Z0-9][\w,\-\s]{0,15}$', raw) and len(raw) <= 12 and not re.search(r'[a-z]', raw))

            if is_booth_code:
                booth = raw
            elif is_known_country:
                country = raw
            else:
                # Unrecognized — likely continuation of company name (wrapped line)
                name = (name + " " + raw).strip()

        if len(parts) >= 3:
            candidate = parts[2].strip()
            cand_lower = candidate.lower()
            cand_is_country = (
                cand_lower in COUNTRY_NAMES or
                any(re.search(r'\b' + re.escape(k) + r'\b', cand_lower) for k in COUNTRY_NAMES)
            )
            if cand_is_country and not country:
                country = candidate   # e.g. "Australia" that landed in col 3 due to wrapping
            elif len(candidate) <= 25 and not booth:
                booth = candidate

        # Last resort: infer country from company name (e.g. "Australian..." → Australia)
        if not country:
            country = _detect_country_from_text(name)

        vendor: dict = {
            "name":       name,
            "source_url": source_url,
            "extraction_method": "pdf_rule_based",
        }
        if country:
            vendor["country"] = country
        if booth:
            vendor["booth_number"] = booth[:60]

        populated = sum(1 for k in ["name", "country", "booth_number"] if vendor.get(k))
        vendor["confidence_score"] = min(populated / 3.0, 1.0) * 0.7

        return vendor

    # ── Group blocks into entries ─────────────────────────────────────────────
    entries: list[str] = []
    current: str | None = None

    for block in blocks:
        # Collapse internal newlines within a block to single space
        block_flat = re.sub(r'\s*\n\s*', ' ', block).strip()
        if not block_flat:
            continue
        if SKIP_RE.search(block_flat):
            continue

        m = ENTRY_START.match(block_flat)
        if m:
            if current is not None:
                entries.append(current)
            current = m.group(2).strip()
        elif current is not None:
            # Continuation block (pavilion/country name on next line)
            # Append with double space so column-split still works
            current += "  " + block_flat

    if current is not None:
        entries.append(current)

    vendors = [v for v in (_parse_entry(e) for e in entries) if v]
    logger.info(f"[PDF] Parsed {len(vendors)} vendor entries from PDF markdown")
    return vendors


def _run_coro_sync(coro) -> object:
    """Run an async coroutine from sync code, handling already-running event loops."""
    import asyncio
    try:
        loop = asyncio.get_event_loop()
        is_running = loop.is_running()
    except RuntimeError:
        is_running = False

    if is_running:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, coro).result(timeout=180)
    else:
        return asyncio.run(coro)


def _extract_event_metadata_from_pdf(markdown: str, source_url: str) -> dict:
    """Extract event_name, event_location, event_date from the first lines of PDF markdown."""
    import re as _re
    from urllib.parse import urlparse

    lines = [l.strip() for l in markdown.split("\n")[:40] if l.strip() and not l.startswith("|")]
    header_text = " ".join(lines[:15])

    name = location = date = ""

    # Event name: first meaningful heading (skip generic/short lines)
    for line in lines[:10]:
        clean = _re.sub(r"[#*_\[\]]", "", line).strip()
        clean = _re.sub(r"\s+", " ", clean)
        if len(clean) > 8 and not _re.match(r"^\d", clean) and "www." not in clean.lower():
            name = clean[:80]
            break

    # Location: city name only
    loc_pat = _re.search(
        r"\b(Kuala Lumpur|Dubai|Abu Dhabi|Paris|London|Berlin|Seoul|Singapore|Jakarta|Tokyo|Washington|Moscow|Beijing|Istanbul|Sydney|Ankara|Riyadh|Athens)\b",
        header_text, _re.IGNORECASE
    )
    if loc_pat:
        location = loc_pat.group(0).strip()

    # Date: look for date patterns
    date_pat = _re.search(
        r"\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*[\s\-–]+\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}"
        r"|\d{1,2}[-/]\d{1,2}[-/]\d{2,4}"
        r"|\d{4}-\d{2}-\d{2}",
        header_text, _re.IGNORECASE
    )
    if date_pat:
        date = date_pat.group(0).strip()

    # Fallback: use domain to infer event name
    if not name:
        domain = urlparse(source_url).netloc.replace("www.", "")
        name = domain.split(".")[0].upper()

    return {
        "event_name": name or "",
        "event_location": location or "",
        "event_date": date or "",
    }


def _extract_vendors_from_pdf(url: str) -> list[dict]:
    """
    Fetch a PDF and extract all vendor/exhibitor records.

    Parser priority:
      1. Firecrawl /v2/parse (Rust engine, 5x faster) — if FIRECRAWL_API_KEY is set
      2. Jina AI Reader (free, no key needed) — fallback
      3. LLM on first 3000 chars — last resort if rule-based yields nothing

    All valid vendors are automatically registered in the global registry.
    """
    from backend.tools.fetch_tools import _fetch_firecrawl_parse, _fetch_jina_markdown

    settings = get_settings()
    markdown = ""

    # ── 1. Firecrawl /parse (preferred when key is set) ──────────────────────
    if settings.has_firecrawl_key:
        try:
            markdown = _run_coro_sync(_fetch_firecrawl_parse(url))
            if markdown:
                logger.info(f"[PDF] Firecrawl returned {len(markdown):,} chars for {url}")
        except Exception as e:
            logger.warning(f"[PDF] Firecrawl failed for {url}: {e}")
            markdown = ""

    # ── 2. Jina fallback ──────────────────────────────────────────────────────
    if not markdown:
        try:
            markdown = _run_coro_sync(_fetch_jina_markdown(url))
            if markdown:
                logger.info(f"[PDF] Jina returned {len(markdown):,} chars for {url}")
        except Exception as e:
            logger.warning(f"[PDF] Jina fetch failed for {url}: {e}")
            markdown = ""

    if not markdown or len(markdown) < 50:
        logger.warning(f"[PDF] No content from any parser for {url}")
        return []

    # ── Extract event metadata from PDF header ────────────────────────────────
    event_meta = _extract_event_metadata_from_pdf(markdown, url)

    def _inject_event(vendors_list: list[dict]) -> list[dict]:
        for v in vendors_list:
            for k, val in event_meta.items():
                if val and not v.get(k):
                    v[k] = val
        return vendors_list

    # ── Parser 1: Markdown table (Firecrawl output) ───────────────────────────
    table_vendors = _parse_exhibitor_pdf_table(markdown, url)
    if table_vendors and len(table_vendors) >= 3:
        _inject_event(table_vendors)
        validated = [v for v in (_validate_vendor(v) for v in table_vendors) if v]
        logger.info(f"[PDF] Table parser: {len(validated)} valid vendors")
        from backend.tools.vendor_registry import register_vendors
        total = register_vendors(validated)
        logger.info(f"[PDF] Registry now has {total} vendors total")
        return validated

    # ── Parser 2: Numbered text (Jina / DSA-style) ───────────────────────────
    numbered_vendors = _parse_exhibitor_pdf_markdown(markdown, url)
    if numbered_vendors and len(numbered_vendors) >= 3:
        _inject_event(numbered_vendors)
        validated = [v for v in (_validate_vendor(v) for v in numbered_vendors) if v]
        logger.info(f"[PDF] Numbered parser: {len(validated)} valid vendors")
        from backend.tools.vendor_registry import register_vendors
        total = register_vendors(validated)
        logger.info(f"[PDF] Registry now has {total} vendors total")
        return validated

    # ── Both parsers found few/zero — merge and try ───────────────────────────
    merged = table_vendors + numbered_vendors
    if len(merged) >= 1:
        validated = [v for v in (_validate_vendor(v) for v in merged) if v]
        if validated:
            from backend.tools.vendor_registry import register_vendors
            register_vendors(validated)
            return validated

    # ── Parser 3: LLM fallback on first 6000 chars ───────────────────────────
    if settings.effective_llm_enabled:
        logger.info(f"[PDF] Both rule-based parsers failed — trying LLM fallback for {url}")
        llm_result = _extract_with_llm_text(markdown[:6000], url, source_label="pdf_llm")
        if llm_result:
            validated_single = _validate_vendor(llm_result)
            if validated_single:
                from backend.tools.vendor_registry import register_vendor
                register_vendor(validated_single)
                return [validated_single]

    return []


@tool
def extract_vendors_from_pdf(url: str) -> dict:
    """
    Extract ALL vendor/exhibitor records from a PDF exhibitor list.
    Fetches the PDF via Jina AI Reader (no API key required) and parses the
    numbered list format common in defense/industry expo exhibitor PDFs.

    Use this when you find a PDF link like:
      /exhibitor-list/companies.pdf
      /data/participating-companies.pdf

    ALL extracted vendors are AUTOMATICALLY saved to the global registry.
    You do NOT need to pass them to deduplicate_vendors or export tools manually.

    Returns: summary dict with keys:
      - "registered": number of vendors extracted from this PDF
      - "total_in_registry": total vendors accumulated so far (all sources)
      - "sample": first 3 vendor records as preview
      - "message": human-readable summary
    """
    vendors = _extract_vendors_from_pdf(url)
    from backend.tools.vendor_registry import get_count
    total = get_count()
    sample = vendors[:3]
    return {
        "registered": len(vendors),
        "total_in_registry": total,
        "sample": sample,
        "message": (
            f"{len(vendors)} vendors extracted from PDF and added to registry. "
            f"Registry now has {total} vendors total."
        ),
    }


@tool
def run_extraction_pipeline(url: str, event_context: dict = None) -> dict:
    """
    Run the full extraction pipeline on a URL. Automatically fetches HTML from
    internal cache (populated by fetch_page/fetch_pages_batch) or re-fetches if
    not cached. Runs schema_org → rule_based → Jina Reader + LLM fallback.
    If JINA_API_KEY is set, uses Jina AI Reader for clean markdown before LLM.
    Pass url only. Returns vendor dict with name, website, email, phone, country, etc.

    Extracted vendors are AUTOMATICALLY saved to the global registry.
    You do NOT need to collect or pass them manually.

    NOTE: For PDF URLs (ending in .pdf), call extract_vendors_from_pdf(url) instead
    to get ALL vendors from the PDF list (potentially hundreds of records).
    """
    import asyncio
    from backend.tools.fetch_tools import get_cached_html, fetch_page_async
    from backend.tools.vendor_registry import register_vendor

    def _reg(v: dict) -> dict:
        """Register a vendor and return it (pass-through helper)."""
        if v:
            register_vendor(v)
        return v

    # ── PDF shortcut ──────────────────────────────────────────────────────────
    if url.lower().split("?")[0].rstrip("/").endswith(".pdf"):
        logger.info(f"[EXTRACT] PDF URL detected — use extract_vendors_from_pdf for full list. Extracting first vendor only: {url}")
        vendors = _extract_vendors_from_pdf(url)
        # _extract_vendors_from_pdf already registers all vendors; just return first
        return vendors[0] if vendors else {}

    html = get_cached_html(url)
    if not html:
        try:
            try:
                loop = asyncio.get_event_loop()
                is_running = loop.is_running()
            except RuntimeError:
                is_running = False
            if is_running:
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    res = pool.submit(asyncio.run, fetch_page_async(url)).result(timeout=60)
            else:
                res = asyncio.run(fetch_page_async(url))
            html = res.get("html", "")
        except Exception as e:
            logger.warning(f"[EXTRACT] Failed to fetch {url}: {e}")
            return {}

    if not html:
        return {}

    settings = get_settings()
    event_ctx = event_context or {}

    result = _extract_schema_org(html, url)
    # Check AFTER validate so bad names (e.g. "Company Profile") are caught early
    _validated_schema = _validate_vendor(result) if _count_populated(result) >= settings.min_vendor_fields else {}
    if _validated_schema.get("name"):
        if event_ctx:
            _validated_schema.update({k: v for k, v in event_ctx.items() if v and not _validated_schema.get(k)})
        return _reg(_validated_schema)

    rule_result = _extract_rule_based(html, url)
    _validated_rule = _validate_vendor(_merge_vendor_data([result, rule_result])) if _count_populated(rule_result) >= settings.min_vendor_fields else {}
    if _validated_rule.get("name"):
        if event_ctx:
            _validated_rule.update({k: v for k, v in event_ctx.items() if v and not _validated_rule.get(k)})
        return _reg(_validated_rule)

    # Either not enough fields OR name was a generic phrase → fall through to LLM
    combined = _merge_vendor_data([r for r in [result, rule_result] if r])
    # Try LLM if: not enough populated fields, OR the name is missing/empty in combined result
    _combined_has_valid_name = bool(_validate_vendor(dict(combined)).get("name") if combined else False)
    if _count_populated(combined) < settings.min_vendor_fields or not _combined_has_valid_name:
        context_str = event_ctx.get("event_name", "") if event_ctx else ""

        # Prefer Jina AI Reader (clean markdown → LLM) over raw HTML → LLM
        # Jina works without API key (free, rate-limited); key only needed for higher limits
        if settings.effective_llm_enabled:
            logger.info(f"[EXTRACT] Rule-based insufficient — trying Jina Reader for {url}")
            jina_result = _extract_with_jina_llm(url, context_str)
            if jina_result:
                combined = _merge_vendor_data([combined, jina_result])
            else:
                # Jina unavailable — fallback to raw HTML
                llm_result = _extract_with_llm(html, url, context_str)
                if llm_result:
                    combined = _merge_vendor_data([combined, llm_result])

    if event_ctx:
        combined.update({k: v for k, v in event_ctx.items() if v and not combined.get(k)})

    return _reg(_validate_vendor(combined))


async def _llm_classify_and_extract(url: str, html: str, event_ctx: dict) -> dict:
    """
    One LLM call that does TWO things at once:
      1. Classify: is this page a real vendor/company profile?
      2. If yes: extract all available company fields.
    Returns vendor dict (with extraction_method="llm_classify") or {} if not a vendor page.
    """
    settings = get_settings()
    if not settings.effective_llm_enabled:
        return {}

    try:
        import html2text as h2t
        converter = h2t.HTML2Text()
        converter.ignore_links = False
        converter.ignore_images = True
        converter.body_width = 0
        text = converter.handle(html)
    except Exception:
        text = html

    lines = [ln.strip() for ln in text.split("\n") if ln.strip() and len(ln.strip()) > 3]
    cleaned = "\n".join(lines[:100])[:2500]   # ~600 tokens max

    event_name = event_ctx.get("event_name", "")

    system = (
        "You are a vendor data extractor for a trade-show exhibitor database.\n\n"
        "STEP 1 — CLASSIFY this page:\n"
        "  VENDOR: a real company/exhibitor profile page (company info, products, contacts)\n"
        "  NOT_VENDOR: navigation item, exhibition category/series, news article, "
        "organizer page, government page, generic topic page\n\n"
        "STEP 2 — If VENDOR: extract every available field into JSON.\n"
        "  Fields: name, website, email, phone, address, city, country, category, "
        "description, linkedin, twitter, booth_number, and any other relevant field.\n\n"
        "STEP 3 — Return ONLY valid JSON:\n"
        "  If VENDOR  → {\"name\": \"...\", \"website\": \"...\", ...}\n"
        "  If NOT_VENDOR → {}\n\n"
        "No explanation. No markdown. Pure JSON only."
    )
    user = f"URL: {url}\nEvent: {event_name}\n\nPage content:\n{cleaned}"

    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=settings.openai_api_key)
        kwargs: dict = {
            "model": settings.openai_model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "max_completion_tokens": 600,
        }
        if settings.model_supports_temperature:
            kwargs["temperature"] = 0.0
            kwargs["response_format"] = {"type": "json_object"}

        response = await client.chat.completions.create(**kwargs)
        content = (response.choices[0].message.content or "").strip()

        if not content or content in ("{}", "{ }"):
            return {}

        try:
            raw = json.loads(content)
        except json.JSONDecodeError:
            m = re.search(r"\{[^{}]*\}", content, re.DOTALL)
            raw = json.loads(m.group(0)) if m else {}

        if not raw:
            return {}

        _SKIP = {"source_url", "extraction_method", "confidence_score"}
        result = {}
        for k, v in raw.items():
            k_norm = re.sub(r"[^a-z0-9_]", "_", k.lower().strip()).strip("_")
            if k_norm and k_norm not in _SKIP and v and str(v).strip():
                result[k_norm] = str(v).strip()[:2000]

        if result:
            result["source_url"] = url
            result["extraction_method"] = "llm_classify"
            result["confidence_score"] = min(_count_populated(result) / 5.0, 1.0)

        return result

    except Exception as e:
        logger.debug(f"[LLM-CLASSIFY] {url}: {e}")
        return {}


@tool
async def extract_all_vendor_profiles(
    vendor_urls: list[str],
    event_context: str = "{}",
    max_concurrent: int = 8,
) -> dict:
    """
    GUNAKAN INI INSTEAD OF memanggil run_extraction_pipeline() dalam loop!

    Ekstrak BANYAK URL vendor sekaligus secara PARALEL dengan LLM sebagai primary extractor.
    Setiap URL diproses oleh LLM yang:
      1. Memvalidasi: "apakah ini halaman vendor/perusahaan beneran?"
      2. Jika ya: ekstrak semua field yang tersedia
      3. Jika tidak (navigasi, kategori, berita): otomatis diskip

    Keuntungan vs run_extraction_pipeline loop:
      - 8 URL diproses BERSAMAAN (bukan satu-satu)
      - LLM membaca konten → tidak ada lagi "Food series" atau kategori abal-abal
      - 1 tool call mengganti 20+ tool calls

    vendor_urls: list URL profil vendor dari discover_vendor_urls()
    event_context: JSON string dengan event_name, event_location, event_date
    max_concurrent: URL yang diproses bersamaan (default 8, max 15)

    Return: {registered, skipped, total_in_registry, elapsed, sample}
    """
    import time as _time

    ctx: dict = {}
    if isinstance(event_context, str) and event_context.strip():
        try:
            ctx = json.loads(event_context)
        except Exception:
            pass
    elif isinstance(event_context, dict):
        ctx = event_context

    from backend.tools.fetch_tools import get_cached_html, fetch_page_async
    from backend.tools.vendor_registry import register_vendor, get_count

    settings = get_settings()
    sem = asyncio.Semaphore(min(max_concurrent, 15))
    t0 = _time.time()
    registered: list[dict] = []

    async def _process_one(url: str) -> dict | None:
        async with sem:
            # ── 1. Get HTML (cache-first, then async fetch) ───────────────────
            html = get_cached_html(url)
            if not html:
                try:
                    res = await fetch_page_async(url)
                    html = res.get("html", "")
                except Exception as e:
                    logger.debug(f"[BATCH-EXTRACT] fetch failed {url}: {e}")
                    return None

            if not html or len(html) < 200:
                return None

            # ── 2. schema.org fast path ───────────────────────────────────────
            schema = _extract_schema_org(html, url)
            v_schema = _validate_vendor(schema) if _count_populated(schema) >= 3 else {}
            if v_schema.get("name"):
                if ctx:
                    v_schema.update({k: v for k, v in ctx.items() if v and not v_schema.get(k)})
                register_vendor(v_schema)
                return v_schema

            # ── 3. LLM classify + extract (primary, not fallback) ─────────────
            if settings.effective_llm_enabled:
                llm_result = await _llm_classify_and_extract(url, html, ctx)
                if llm_result:
                    validated = _validate_vendor(llm_result)
                    if validated.get("name"):
                        if ctx:
                            validated.update({k: v for k, v in ctx.items() if v and not validated.get(k)})
                        register_vendor(validated)
                        return validated

            # ── 4. rule_based supplement only (last resort, no LLM) ───────────
            rule = _extract_rule_based(html, url)
            v_rule = _validate_vendor(_merge_vendor_data([schema, rule])) if _count_populated(rule) >= 3 else {}
            if v_rule.get("name"):
                if ctx:
                    v_rule.update({k: v for k, v in ctx.items() if v and not v_rule.get(k)})
                register_vendor(v_rule)
                return v_rule

            return None

    tasks = [_process_one(url) for url in vendor_urls]
    raw = await asyncio.gather(*tasks, return_exceptions=True)

    for r in raw:
        if isinstance(r, dict) and r.get("name"):
            registered.append(r)

    elapsed = round(_time.time() - t0, 1)
    total = get_count()
    skipped = len(vendor_urls) - len(registered)

    logger.debug(
        f"[BATCH-EXTRACT] {len(registered)}/{len(vendor_urls)} valid vendors "
        f"({skipped} skipped as non-vendor) in {elapsed}s"
    )

    return {
        "registered": len(registered),
        "skipped": skipped,
        "total_in_registry": total,
        "elapsed": elapsed,
        "sample": registered[:3],
        "message": (
            f"Parallel LLM extraction: {len(registered)} valid vendors "
            f"from {len(vendor_urls)} URLs ({skipped} skipped as non-vendor) in {elapsed}s"
        ),
    }


from backend.tools.vendor_registry import get_vendor_count
from backend.tools.dynamic_parser_tool import generate_and_run_parser

ALL_EXTRACT_TOOLS = [
    run_extraction_pipeline,
    extract_all_vendor_profiles,
    discover_vendor_urls,
    extract_vendors_from_pdf,
    get_vendor_count,
    generate_and_run_parser,
]
