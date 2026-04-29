import asyncio
import json
import re
import time
from urllib.parse import urlparse

import httpx
from langchain_core.tools import tool
from loguru import logger

from backend.core.config import get_settings
from backend.tools.vendor_registry import get_all_vendors, replace_all, get_count

_DESCRIBE_PROMPT = """\
You are writing a company profile for a defense/security industry database.

Company: {name}
Country: {country}
Event: {event_name}

Based on the search result and webpage content below, write a concise 1-2 sentence description
of what this company does. Be specific — mention their products, services, or specialization.
If content is insufficient, write what you can infer from the company name and context.

Content:
{content}

Return ONLY a JSON object:
{{
  "description": "1-2 sentence company profile",
  "website": "official URL if clearly found",
  "email": "email if clearly found",
  "phone": "phone if clearly found",
  "city": "city if clearly found",
  "linkedin": "LinkedIn URL if clearly found",
  "twitter": "Twitter/X URL if clearly found",
  "category": "industry category if clearly found"
}}
Return only fields you are confident about. Never invent data.
"""

_SKIP_DOMAINS = {
    "linkedin.com", "facebook.com", "twitter.com", "wikipedia.org",
    "bloomberg.com", "reuters.com", "crunchbase.com", "dnb.com",
    "yellowpages.com", "yelp.com", "glassdoor.com", "indeed.com",
    "zoominfo.com", "dun.com", "manta.com",
}


def _is_skip_domain(url: str) -> bool:
    try:
        d = urlparse(url).netloc.lower().replace("www.", "")
        return any(s in d for s in _SKIP_DOMAINS)
    except Exception:
        return False


async def _search_company(name: str, country: str = "", is_cn_ru: bool = False) -> tuple[str, str]:
    """Returns (best_url, snippet_text)"""
    settings = get_settings()
    query = f"{name} {country} official website contact"

    # OpenSERP for China/Russia
    if is_cn_ru and settings.openserp_enabled:
        try:
            engine = "baidu" if "china" in country.lower() else "yandex"
            async with httpx.AsyncClient(timeout=8.0) as client:
                r = await client.get(
                    f"{settings.openserp_base_url}/{engine}/search",
                    params={"text": query, "limit": 5},
                )
                if r.status_code == 200:
                    results = r.json()
                    for item in results:
                        url = item.get("url", "")
                        if url and not _is_skip_domain(url):
                            snippet = item.get("description", "") or item.get("title", "")
                            return url, snippet
        except Exception as e:
            logger.debug(f"[ENRICH] OpenSERP failed: {e}")

    # Tavily
    if settings.tavily_api_key:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.post(
                    "https://api.tavily.com/search",
                    json={
                        "api_key": settings.tavily_api_key,
                        "query": query,
                        "max_results": 5,
                        "search_depth": "basic",
                    },
                )
                results = r.json().get("results", [])
                snippets = []
                for item in results:
                    url = item.get("url", "")
                    snippet = item.get("content", "") or item.get("snippet", "")
                    if snippet:
                        snippets.append(snippet)
                    if url and not _is_skip_domain(url) and not item.get("url", "").endswith(".pdf"):
                        return url, " ".join(snippets[:3])
                if snippets:
                    return "", " ".join(snippets[:3])
        except Exception as e:
            logger.debug(f"[ENRICH] Tavily failed for {name}: {e}")

    return "", ""


async def _scrape_page(url: str) -> str:
    if not url:
        return ""
    settings = get_settings()

    # Firecrawl scrape
    if settings.has_firecrawl_key:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.post(
                    f"{settings.firecrawl_base_url}/v1/scrape",
                    headers={"Authorization": f"Bearer {settings.firecrawl_api_key}"},
                    json={"url": url, "formats": ["markdown"], "onlyMainContent": True},
                )
                if r.status_code == 200:
                    md = (r.json().get("data") or {}).get("markdown", "")
                    if md and len(md) > 100:
                        return md[:4000]
        except Exception as e:
            logger.debug(f"[ENRICH] Firecrawl failed for {url}: {e}")

    # Jina fallback
    try:
        from backend.tools.fetch_tools import _fetch_jina_markdown
        md = await _fetch_jina_markdown(url)
        if md:
            return md[:4000]
    except Exception:
        pass

    # Plain httpx fallback
    try:
        async with httpx.AsyncClient(timeout=12.0, follow_redirects=True,
                                     headers={"User-Agent": "Mozilla/5.0"}) as client:
            r = await client.get(url)
            text = re.sub(r"<[^>]+>", " ", r.text)
            text = re.sub(r"\s+", " ", text).strip()
            return text[:4000]
    except Exception:
        return ""


async def _enrich_with_llm(vendor: dict, content: str) -> dict:
    if not content or len(content) < 30:
        return {}
    settings = get_settings()
    if not settings.has_openai_key:
        return {}
    from openai import AsyncOpenAI
    client = AsyncOpenAI(api_key=settings.openai_api_key)

    prompt = _DESCRIBE_PROMPT.format(
        name=vendor.get("name", ""),
        country=vendor.get("country", ""),
        event_name=vendor.get("event_name", ""),
        content=content[:3000],
    )
    try:
        supports_temp = settings.openai_model not in settings.MODELS_NO_TEMPERATURE
        kwargs: dict = dict(
            model=settings.openai_model,
            messages=[
                {"role": "system", "content": "Extract and summarize company info. Return ONLY valid JSON."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            max_tokens=400,
        )
        if supports_temp:
            kwargs["temperature"] = 0.0
        resp = await client.chat.completions.create(**kwargs)
        raw = resp.choices[0].message.content or "{}"
        return json.loads(raw)
    except Exception as e:
        logger.debug(f"[ENRICH] LLM failed for {vendor.get('name')}: {e}")
        return {}


def _needs_enrichment(v: dict) -> bool:
    empty = sum(1 for f in ("website", "email", "phone", "description", "linkedin", "category") if not v.get(f))
    return empty >= 3


def _is_china_russia(v: dict) -> bool:
    c = (v.get("country") or "").lower()
    n = (v.get("name") or "").lower()
    return any(kw in c or kw in n for kw in ("china", "russia", "chinese", "russian", "beijing", "moscow"))


def _merge(vendor: dict, extracted: dict) -> dict:
    allowed = {"website", "email", "phone", "address", "city", "description",
               "linkedin", "twitter", "category"}
    updated = dict(vendor)
    for k, v in extracted.items():
        if k in allowed and v and isinstance(v, str) and len(v.strip()) > 1:
            if not updated.get(k):
                updated[k] = v.strip()[:500]
    return updated


async def _enrich_one(vendor: dict, sem: asyncio.Semaphore) -> dict:
    async with sem:
        name = vendor.get("name", "")
        website = vendor.get("website", "")
        is_cn_ru = _is_china_russia(vendor)

        # Step 1: find website + get search snippet
        url, snippet = await _search_company(name, vendor.get("country", ""), is_cn_ru)
        if url and not website:
            vendor = dict(vendor, website=url)
            website = url

        # Step 2: scrape page for rich content
        page_content = await _scrape_page(website or url)

        # Step 3: combine snippet + page content for LLM
        combined = ((snippet + "\n\n") if snippet else "") + (page_content or "")
        if not combined.strip():
            return vendor

        # Step 4: LLM generate description + extract fields
        extracted = await _enrich_with_llm(vendor, combined)
        if extracted:
            vendor = _merge(vendor, extracted)
            filled = [k for k in extracted if extracted[k]]
            logger.debug(f"[ENRICH] {name[:40]} -> filled: {filled}")

        return vendor


@tool
def enrich_vendors_parallel(max_concurrent: int = 15, max_vendors: int = 200) -> dict:
    """
    Enrich vendors in registry: search website, scrape via Firecrawl/Jina, generate description via LLM.
    Runs PARALLEL — max_concurrent vendors at once. Uses OpenSERP Baidu/Yandex for China/Russia vendors.

    Args:
        max_concurrent: parallel workers (default 15)
        max_vendors: how many vendors to enrich (default 200, prioritizes vendors with most empty fields)

    Returns: {enriched, skipped, failed, elapsed_seconds, registry_total}
    """
    async def _run():
        vendors = get_all_vendors()
        # Prioritize: most empty fields first, China/Russia vendors get priority
        needs = sorted(
            [v for v in vendors if _needs_enrichment(v)],
            key=lambda v: (_is_china_russia(v) * -1, -sum(1 for f in ("website","email","phone","description","linkedin") if not v.get(f)))
        )[:max_vendors]

        skip_count = len(vendors) - len(needs)
        if not needs:
            return {"enriched": 0, "skipped": len(vendors), "failed": 0,
                    "elapsed_seconds": 0, "registry_total": get_count(),
                    "message": "All vendors already enriched"}

        logger.info(f"[ENRICH] {len(needs)} vendors to enrich ({max_concurrent} parallel) — {sum(1 for v in needs if _is_china_russia(v))} China/Russia via OpenSERP")
        t0 = time.time()

        sem = asyncio.Semaphore(max_concurrent)
        results = await asyncio.gather(*[_enrich_one(v, sem) for v in needs], return_exceptions=True)

        failed = 0
        result_map: dict[str, dict] = {}
        for orig, result in zip(needs, results):
            key = orig.get("name", "") + orig.get("source_url", "")
            if isinstance(result, Exception):
                failed += 1
                result_map[key] = orig
            else:
                result_map[key] = result

        final = []
        for v in vendors:
            key = v.get("name", "") + v.get("source_url", "")
            final.append(result_map.get(key, v))

        replace_all(final)
        elapsed = round(time.time() - t0, 1)
        enriched_count = len(needs) - failed
        logger.info(f"[ENRICH] Done: {enriched_count} enriched, {failed} failed in {elapsed}s")
        return {
            "enriched": enriched_count,
            "skipped": skip_count,
            "failed": failed,
            "elapsed_seconds": elapsed,
            "registry_total": get_count(),
            "message": f"{enriched_count} vendors enriched in {elapsed}s",
        }

    # Fix: always create a fresh event loop — LangGraph runs tools in threads
    new_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(new_loop)
    try:
        return new_loop.run_until_complete(_run())
    except Exception as e:
        logger.error(f"[ENRICH] Failed: {e}")
        return {"enriched": 0, "skipped": 0, "failed": 0, "error": str(e)}
    finally:
        new_loop.close()
