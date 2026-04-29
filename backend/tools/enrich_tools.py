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
    """
    Returns (best_url, snippet_text).
    Search order: OpenSERP (Google/Baidu/Yandex) → Tavily → DuckDuckGo.
    All three are tried until one returns a usable result.
    """
    settings = get_settings()
    query = f"{name} {country} official website contact"

    # ── 1. OpenSERP ─────────────────────────────────────────────────────────
    if settings.openserp_enabled:
        # CN/RU → Baidu/Yandex; everyone else → Google
        if is_cn_ru:
            engine = "baidu" if "china" in country.lower() else "yandex"
        else:
            engine = "google"
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                r = await client.get(
                    f"{settings.openserp_base_url}/{engine}/search",
                    params={"text": query, "limit": 5},
                )
                if r.status_code == 200:
                    results = r.json() or []
                    snippets = []
                    best_url = ""
                    for item in results:
                        url  = item.get("url", "")
                        snip = item.get("description", "") or item.get("title", "")
                        if snip:
                            snippets.append(snip)
                        if url and not _is_skip_domain(url) and not url.endswith(".pdf") and not best_url:
                            best_url = url
                    if best_url or snippets:
                        logger.debug(f"[ENRICH] OpenSERP({engine}) found: {best_url[:60]}")
                        return best_url, " ".join(snippets[:3])
        except Exception as e:
            logger.debug(f"[ENRICH] OpenSERP({engine}) failed for {name}: {e}")

    # ── 2. Tavily ────────────────────────────────────────────────────────────
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
                    url  = item.get("url", "")
                    snip = item.get("content", "") or item.get("snippet", "")
                    if snip:
                        snippets.append(snip)
                    if url and not _is_skip_domain(url) and not url.endswith(".pdf"):
                        logger.debug(f"[ENRICH] Tavily found: {url[:60]}")
                        return url, " ".join(snippets[:3])
                if snippets:
                    return "", " ".join(snippets[:3])
        except Exception as e:
            logger.debug(f"[ENRICH] Tavily failed for {name}: {e}")

    # ── 3. DuckDuckGo — always available, no API key ─────────────────────────
    try:
        from ddgs import DDGS
        import asyncio as _asyncio
        loop = _asyncio.get_event_loop()
        def _ddg():
            with DDGS() as ddgs:
                return list(ddgs.text(query, max_results=5))
        results = await loop.run_in_executor(None, _ddg)
        snippets = []
        best_url = ""
        for item in results:
            url  = item.get("href", "")
            snip = item.get("body", "") or item.get("title", "")
            if snip:
                snippets.append(snip)
            if url and not _is_skip_domain(url) and not url.endswith(".pdf") and not best_url:
                best_url = url
        if best_url or snippets:
            logger.debug(f"[ENRICH] DDG found: {best_url[:60]}")
            return best_url, " ".join(snippets[:3])
    except Exception as e:
        logger.debug(f"[ENRICH] DDG failed for {name}: {e}")

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

    # Plain httpx fallback — pakai stealth headers dari browserforge
    try:
        from backend.tools.stealth_tools import get_realistic_headers
        stealth_headers = get_realistic_headers(url)
        async with httpx.AsyncClient(timeout=12.0, follow_redirects=True,
                                     headers=stealth_headers) as client:
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


def _is_readable(s: str) -> bool:
    """Return False if string looks like binary garbage."""
    if not s or len(s) < 3:
        return False
    # Control chars check
    non_print = sum(1 for c in s if ord(c) < 32 or 0x7F <= ord(c) <= 0x9F)
    if non_print / len(s) > 0.05:
        return False
    return True


_SENTENCE_PUNCT = frozenset('.,;:!?-\'"()（）【】。，、：；！？…—·')


def _is_clean_token(w: str) -> bool:
    """A 'clean' token has only alphabetic chars and common sentence punctuation.
    Binary garbage always contains symbols like ~  [  )  $  +  *  @  ^  |  =
    that never appear inside real words."""
    return len(w) >= 2 and all(c.isalpha() or c in _SENTENCE_PUNCT for c in w)


def _looks_like_text(s: str) -> bool:
    """
    Check if a string looks like human-readable text (English, Chinese, etc.).
    Real text has >= 2 clean space-separated tokens; binary garbage does not
    because decoded bytes produce tokens with symbols like ~, [, $, +, *.
    """
    if not s or len(s) < 8:
        return False
    # Must be mostly letters + spaces
    letter_space = sum(1 for c in s if c.isalpha() or c == ' ')
    if (letter_space / len(s)) < 0.60:
        return False
    # At least 2 clean tokens (handles both "word word" and "中文文字 更多")
    clean = [w for w in s.split() if _is_clean_token(w)]
    # For single-token CJK text (no spaces), the whole string is one token
    if not clean and _is_clean_token(s.replace(' ', '')):
        clean = [s]
    return len(clean) >= 2 or (len(clean) == 1 and len(clean[0]) >= 4)


def _clean_field(v: str, field: str = "") -> str:
    """Strip control chars; for description/category also check text structure."""
    cleaned = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', v).strip()
    if not cleaned:
        return ""
    if not _is_readable(cleaned):
        return ""
    # Extra check for free-text fields — must look like real words
    if field in ("description", "category", "address") and len(cleaned) > 10:
        if not _looks_like_text(cleaned):
            return ""
    return cleaned


def _merge(vendor: dict, extracted: dict) -> dict:
    allowed = {"website", "email", "phone", "address", "city", "description",
               "linkedin", "twitter", "category"}
    updated = dict(vendor)
    # Clean existing fields that may contain garbage from prior extraction
    for k in list(updated.keys()):
        if isinstance(updated[k], str) and updated[k]:
            cleaned = _clean_field(updated[k], field=k)
            if not cleaned:
                updated[k] = ""   # was garbage → clear so enrichment can fill it
    # Merge newly extracted fields (only fill empty slots)
    for k, v in extracted.items():
        if k in allowed and v and isinstance(v, str):
            v_clean = _clean_field(v, field=k)
            if v_clean and len(v_clean) > 1 and not updated.get(k):
                updated[k] = v_clean[:500]
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
def enrich_vendors_parallel(max_concurrent: int = 15, max_vendors: int = 10_000) -> dict:
    """
    Enrich vendors in registry: search website, scrape via Firecrawl/Jina, generate description via LLM.
    Runs PARALLEL — max_concurrent vendors at once. Uses OpenSERP Baidu/Yandex for China/Russia vendors.

    Args:
        max_concurrent: parallel workers (default 15)
        max_vendors: max vendors to enrich (default 10000 = effectively unlimited)

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

        # Second pass: OpenAI web search enrichment for vendors still missing key fields
        try:
            from backend.tools.websearch_enrichment import websearch_enrich_batch
            final_after_ws = await websearch_enrich_batch(final, max_workers=5)   # 5 workers to respect TPM limit
            replace_all(final_after_ws)
            final = final_after_ws
            logger.info(f"[ENRICH] Web search enrichment pass complete ({len(final)} vendors)")
        except Exception as ws_err:
            logger.debug(f"[ENRICH] Web search enrichment skipped: {ws_err}")

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

    def _run_in_fresh_loop():
        """Run _run() in a brand-new event loop (must be called from a plain thread)."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(_run())
        except Exception as e:
            logger.error(f"[ENRICH] Failed: {e}")
            return {"enriched": 0, "skipped": 0, "failed": 0, "error": str(e)}
        finally:
            loop.close()

    # If called from within an already-running event loop (e.g. test scripts using
    # asyncio.run()), spawning a new loop in the same thread raises
    # "Cannot run the event loop while another loop is running".
    # Fix: detect this case and offload to a fresh thread instead.
    try:
        asyncio.get_running_loop()
        _in_async_ctx = True
    except RuntimeError:
        _in_async_ctx = False

    if _in_async_ctx:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as _pool:
            return _pool.submit(_run_in_fresh_loop).result()
    else:
        return _run_in_fresh_loop()
