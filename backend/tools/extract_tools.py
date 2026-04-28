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
    r'https?://(?:www\.)?(?:twitter\.com|x\.com)/([A-Za-z0-9_]+)/?'
)
COUNTRY_NAMES = {
    "united states": "United States", "usa": "United States", "u.s.a": "United States",
    "united kingdom": "United Kingdom", "uk": "United Kingdom", "great britain": "United Kingdom",
    "germany": "Germany", "deutschland": "Germany",
    "france": "France", "israel": "Israel",
    "australia": "Australia", "canada": "Canada",
    "japan": "Japan", "china": "China",
    "south korea": "South Korea", "korea": "South Korea",
    "singapore": "Singapore", "india": "India",
    "netherlands": "Netherlands", "sweden": "Sweden",
    "norway": "Norway", "finland": "Finland",
    "denmark": "Denmark", "switzerland": "Switzerland",
    "austria": "Austria", "spain": "Spain", "italy": "Italy",
    "poland": "Poland", "czech republic": "Czech Republic",
    "russia": "Russia", "ukraine": "Ukraine",
    "united arab emirates": "UAE", "uae": "UAE",
    "saudi arabia": "Saudi Arabia", "brazil": "Brazil",
    "new zealand": "New Zealand", "belgium": "Belgium",
    "portugal": "Portugal", "turkey": "Turkey",
    "malaysia": "Malaysia", "indonesia": "Indonesia",
    "philippines": "Philippines", "thailand": "Thailand",
    "taiwan": "Taiwan", "hong kong": "Hong Kong",
    "south africa": "South Africa", "nigeria": "Nigeria",
    "kenya": "Kenya", "egypt": "Egypt",
    "mexico": "Mexico", "argentina": "Argentina",
    "colombia": "Colombia", "chile": "Chile",
    "ireland": "Ireland", "czech": "Czech Republic",
    "estonia": "Estonia", "latvia": "Latvia", "lithuania": "Lithuania",
    "romania": "Romania", "bulgaria": "Bulgaria",
    "greece": "Greece", "hungary": "Hungary", "slovakia": "Slovakia",
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
    "[class*='address']", "[class*='location']",
    "[data-field='address']", "[class*='contact-address']",
]

CSS_SELECTORS_WEBSITE = [
    "[itemprop='url']", "a[class*='website']",
    "a[class*='web-link']", "a[rel='noopener'][href^='http']",
    "[data-field='website']", "[class*='official-site']",
]

CSS_SELECTORS_DESCRIPTION = [
    "[itemprop='description']", "meta[name='description']",
    "meta[property='og:description']",
    "[class*='company-desc']", "[class*='exhibitor-desc']",
    "[class*='about-us']", "[class*='overview']",
    "[class*='profile-desc']", "[class*='short-desc']",
    "[data-field='description']",
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
    text_lower = text.lower()
    for key, value in COUNTRY_NAMES.items():
        if re.search(r'\b' + re.escape(key) + r'\b', text_lower):
            return value
    return ""


_WEBSITE_BLOCKLIST = {
    "twitter.com", "x.com", "facebook.com", "linkedin.com", "instagram.com",
    "youtube.com", "flickr.com", "pinterest.com", "reddit.com", "tumblr.com",
    "t.co", "bit.ly", "tinyurl.com", "ow.ly", "buff.ly", "hootsuite.com",
    "share.flipboard.com", "qualtrics.com", "typeform.com",
    "surveymonkey.com", "google.com", "apple.com", "microsoft.com",
    "amazon.com", "github.com", "wikipedia.org",
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
    if not any(t in str(type_val) for t in ["Organization", "Corporation", "LocalBusiness", "Company"]):
        if not any(t in str(type_val) for t in ["Person", "Event"]):
            pass

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

    if result:
        result["source_url"] = url
        result["extraction_method"] = "schema_org"
        populated = _count_populated(result)
        result["confidence_score"] = min(populated / 5.0, 1.0)

    return result


@tool
def extract_rule_based(html: str, url: str) -> dict:
    """
    Extract vendor data using 100+ CSS selectors, regex, and heuristics.
    Zero LLM — rule-based extraction. Returns VendorRecord dict.
    """
    if not html:
        return {}

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    parsed_url = urlparse(url)
    base_domain = parsed_url.netloc.lower()

    full_text = soup.get_text(separator=" ", strip=True)

    result = {}

    name = _extract_by_selectors(soup, CSS_SELECTORS_NAME)
    if not name:
        h1 = soup.find("h1")
        if h1:
            name = _clean_text(h1.get_text(strip=True))
    if name and len(name) > 100:
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

    address = _extract_by_selectors(soup, CSS_SELECTORS_ADDRESS)
    if address:
        result["address"] = address

    website = _extract_by_selectors(soup, CSS_SELECTORS_WEBSITE)
    if not website:
        website = _extract_website_from_links(soup, base_domain)
    if website and website.startswith("http") and base_domain not in website:
        result["website"] = website

    description = _extract_by_selectors(soup, CSS_SELECTORS_DESCRIPTION)
    if not description:
        for p in soup.find_all("p")[:10]:
            p_text = p.get_text(strip=True)
            if len(p_text) > 80:
                description = _clean_text(p_text, 400)
                break
    if description:
        result["description"] = description

    category = _extract_by_selectors(soup, CSS_SELECTORS_CATEGORY)
    if category:
        result["category"] = category

    country = _extract_by_selectors(soup, CSS_SELECTORS_COUNTRY)
    if not country:
        country = _detect_country_from_text(full_text[:2000])
    if country:
        result["country"] = country

    booth = _extract_by_selectors(soup, CSS_SELECTORS_BOOTH)
    if booth:
        result["booth_number"] = booth

    linkedin_match = LINKEDIN_PATTERN.search(html)
    if linkedin_match:
        result["linkedin"] = linkedin_match.group(0)

    twitter_match = TWITTER_PATTERN.search(html)
    if twitter_match:
        result["twitter"] = twitter_match.group(0)

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


@tool
def extract_with_llm(html: str, url: str, context: str = "") -> dict:
    """
    FALLBACK ONLY: Extract vendor data using GPT-4o when schema_org and rule_based fail.
    Sends max 1500 chars of cleaned text to minimize token usage.
    Only called when other methods return < 3 fields.
    """
    settings = get_settings()
    if not settings.effective_llm_enabled:
        logger.debug("LLM fallback disabled — skipping")
        return {}

    try:
        import html2text as h2t
        import tiktoken
        import json
        import re
        from openai import OpenAI

        converter = h2t.HTML2Text()
        converter.ignore_links = True
        converter.ignore_images = True
        converter.body_width = 0
        text = converter.handle(html)

        lines = [ln.strip() for ln in text.split("\n") if ln.strip() and len(ln.strip()) > 3]
        text = "\n".join(lines)
        text = text[:settings.llm_max_input_chars]

        enc = tiktoken.encoding_for_model("gpt-4o-mini")
        tokens = len(enc.encode(text))
        if tokens > 400:
            text = enc.decode(enc.encode(text)[:400])

        client = OpenAI(api_key=settings.openai_api_key)

        system_prompt = (
            "Extract company/vendor info from the page text. "
            "Output ONLY a valid JSON object with any of these keys: "
            "name, website, email, phone, city, country, category, description. "
            "Omit keys you are not confident about. No explanation, just JSON."
        )
        context_suffix = f" (source: {context[:100]})" if context else ""
        user_prompt = f"URL: {url}{context_suffix}\n\n{text}"

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
            logger.warning(f"[LLM] Empty response for {url}")
            return {}

        try:
            raw = json.loads(content)
        except json.JSONDecodeError:
            m = re.search(r"\{[^{}]*\}", content, re.DOTALL)
            if not m:
                logger.warning(f"[LLM] No JSON found in response for {url}")
                return {}
            raw = json.loads(m.group(0))

        valid_keys = {"name", "website", "email", "phone", "city", "country", "category", "description"}
        result = {k: str(v).strip() for k, v in raw.items() if k in valid_keys and v and str(v).strip()}

        if result:
            result["source_url"] = url
            result["extraction_method"] = "llm"
            populated = _count_populated(result)
            result["confidence_score"] = min(populated / 5.0, 1.0)
            logger.info(f"[LLM] Extracted {populated} fields from {url} (~{tokens} tokens in)")

        return result

    except Exception as e:
        logger.warning(f"[LLM] Extraction failed for {url}: {type(e).__name__}: {e}")
        return {}


@tool
def merge_vendor_data(sources: list[dict]) -> dict:
    """
    Merge vendor data from multiple extraction sources.
    Priority: schema_org > rule_based > llm > enrichment.
    Returns the best combined record.
    """
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


@tool
def validate_vendor(vendor: dict) -> dict:
    """
    Validate and clean a vendor record. Normalizes fields, removes invalid data,
    computes final confidence score. Returns cleaned vendor dict.
    """
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
        r'copyright|all rights reserved|\d{4})$',
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
        if len(name) < 2 or len(name) > 300:
            cleaned.pop("name", None)
        elif re.match(r'^\d+$', name):
            cleaned.pop("name", None)
        elif BAD_NAME_PATTERNS.match(name):
            cleaned.pop("name", None)
        elif len(name.split()) > 10:
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


@tool
def run_extraction_pipeline(html: str, url: str, event_context: dict = None) -> dict:
    """
    Run the full extraction pipeline: schema_org → rule_based → llm (fallback).
    Returns the best result with extraction_method indicating which succeeded.
    """
    if not html:
        return {}

    settings = get_settings()
    event_ctx = event_context or {}

    result = extract_schema_org.invoke({"html": html, "url": url})
    if _count_populated(result) >= settings.min_vendor_fields:
        if event_ctx:
            result.update({k: v for k, v in event_ctx.items() if v and not result.get(k)})
        return validate_vendor.invoke({"vendor": result})

    rule_result = extract_rule_based.invoke({"html": html, "url": url})
    if _count_populated(rule_result) >= settings.min_vendor_fields:
        merged = merge_vendor_data.invoke({"sources": [result, rule_result]})
        if event_ctx:
            merged.update({k: v for k, v in event_ctx.items() if v and not merged.get(k)})
        return validate_vendor.invoke({"vendor": merged})

    combined = merge_vendor_data.invoke({"sources": [r for r in [result, rule_result] if r]})
    if settings.effective_llm_enabled and _count_populated(combined) < settings.min_vendor_fields:
        context_str = event_ctx.get("event_name", "") if event_ctx else ""
        llm_result = extract_with_llm.invoke({"html": html, "url": url, "context": context_str})
        if llm_result:
            combined = merge_vendor_data.invoke({"sources": [combined, llm_result]})

    if event_ctx:
        combined.update({k: v for k, v in event_ctx.items() if v and not combined.get(k)})

    return validate_vendor.invoke({"vendor": combined})


ALL_EXTRACT_TOOLS = [
    extract_schema_org,
    extract_rule_based,
    extract_with_llm,
    merge_vendor_data,
    validate_vendor,
    run_extraction_pipeline,
]
