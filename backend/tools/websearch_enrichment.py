"""
Enrichment pipeline: DDGS context → OpenAI gpt-5-nano web_search → pure JSON.

Flow per vendor:
  1. DDGS search  →  snippets + best URL  (free, fast, context)
  2. OpenAI gpt-5-nano + web_search_preview  →  JSON: url, name, category,
     specialized, description (+ any other field found)

Only vendors with 2+ missing core fields are processed.
"""
from __future__ import annotations

import asyncio
import json
import re
from typing import Any

from loguru import logger

_CORE_FIELDS = ["email", "phone", "website", "linkedin", "description"]
_ALL_FIELDS  = ["email", "phone", "website", "linkedin", "twitter",
                "description", "address", "city", "category",
                "specialized", "products", "certifications",
                "company_size", "founded_year"]

_WS_MODEL = "gpt-5-nano"   # cheapest, user-mandated


def _missing(vendor: dict) -> list[str]:
    return [f for f in _CORE_FIELDS if not vendor.get(f)]


# ── Step 1: DDGS context ──────────────────────────────────────────────────────

async def _ddgs_context(name: str, country: str, event: str) -> tuple[str, str]:
    """
    Search DDGS for the company.
    Returns (best_url, combined_snippets).
    Runs blocking DDGS in a thread pool so it doesn't block the event loop.
    """
    query = f"{name} {country} {event} official website contact"
    try:
        from ddgs import DDGS

        def _search():
            with DDGS() as d:
                return list(d.text(query, max_results=6))

        loop   = asyncio.get_event_loop()
        items  = await loop.run_in_executor(None, _search)

        snippets = []
        best_url = ""
        for item in items:
            url  = item.get("href", "")
            body = item.get("body", "") or item.get("title", "")
            if body:
                snippets.append(body[:200])
            if url and not best_url and not _is_skip(url):
                best_url = url

        return best_url, "\n".join(snippets[:4])
    except Exception as e:
        logger.debug(f"[WS-DDGS] {name}: {e}")
        return "", ""


def _is_skip(url: str) -> bool:
    skip = {"linkedin.com", "facebook.com", "wikipedia.org",
            "bloomberg.com", "crunchbase.com", "glassdoor.com"}
    try:
        from urllib.parse import urlparse
        d = urlparse(url).netloc.lower().replace("www.", "")
        return any(s in d for s in skip)
    except Exception:
        return False


# ── Step 2: OpenAI gpt-5-nano + web_search_preview ───────────────────────────

def _build_prompt(vendor: dict, ddgs_url: str, ddgs_context: str) -> str:
    name    = vendor.get("name", "")
    country = vendor.get("country", "")
    event   = vendor.get("event_name", "")
    missing = _missing(vendor)

    ctx_block = ""
    if ddgs_context:
        ctx_block = f"\nContext from web search:\n{ddgs_context}\n"
    if ddgs_url:
        ctx_block += f"Candidate website: {ddgs_url}\n"

    return (
        f"Company: {name}\n"
        f"Country: {country}\n"
        f"Event: {event}\n"
        f"{ctx_block}\n"
        f"Find and return ONLY a valid JSON object with these fields "
        f"(include only fields you are confident about):\n"
        f'{{"url":"official website","name":"company name",'
        f'"category":"industry category","specialized":"products or specialization",'
        f'"description":"1-2 sentence company profile",'
        f'"email":"contact email","phone":"phone number",'
        f'"linkedin":"LinkedIn URL","city":"city",'
        f'"company_size":"number of employees or size category (e.g., SME, 50-100, 1000+)",'
        f'"founded_year":"year company was founded (integer, e.g., 2015)"}}\n'
        f"Missing fields to find: {', '.join(missing)}\n"
        f"NO explanation. PURE JSON only."
    )


def _get_client():
    from openai import OpenAI
    from backend.core.config import get_settings
    return OpenAI(api_key=get_settings().openai_api_key)


def _extract_text(response) -> str:
    text = ""
    # `response.output` may exist but be None — guard against that
    for item in (getattr(response, "output", None) or []):
        if hasattr(item, "content"):
            for c in (item.content or []):
                if hasattr(c, "text"):
                    text += c.text
        elif hasattr(item, "text"):
            text += item.text
    return text


def _parse_json(text: str) -> dict:
    match = re.search(r'\{[^{}]{5,}\}', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return {}


# ── Main per-vendor enrichment ────────────────────────────────────────────────

async def websearch_enrich_vendor(vendor: dict) -> dict:
    from backend.core.config import get_settings
    settings = get_settings()

    if not getattr(settings, "openai_websearch_enrichment", True):
        return vendor
    if not settings.openai_api_key:
        return vendor
    if len(_missing(vendor)) < 2:
        return vendor   # already well-populated

    name    = vendor.get("name", "")
    country = vendor.get("country", "")
    event   = vendor.get("event_name", "")

    # ── Step 1: DDGS → context ───────────────────────────────────────────────
    ddgs_url, ddgs_ctx = await _ddgs_context(name, country, event)

    # ── Step 2: OpenAI gpt-5-nano + web_search_preview ──────────────────────
    prompt = _build_prompt(vendor, ddgs_url, ddgs_ctx)
    try:
        client = _get_client()
        loop   = asyncio.get_event_loop()

        # Retry up to 3× on 429 rate-limit with exponential backoff
        response = None
        for _attempt in range(3):
            try:
                response = await loop.run_in_executor(
                    None,
                    lambda: client.responses.create(
                        model=_WS_MODEL,
                        tools=[{"type": "web_search_preview"}],
                        input=prompt,
                    ),
                )
                break   # success
            except Exception as _e:
                _es = str(_e).lower()
                if ("429" in _es or "rate_limit" in _es or "rate limit" in _es) and _attempt < 2:
                    _wait = 5.0 * (2 ** _attempt)   # 5 s → 10 s
                    logger.debug(f"[WS] {name[:30]} 429 rate-limit, retry in {_wait:.0f}s (attempt {_attempt+1}/3)")
                    await asyncio.sleep(_wait)
                else:
                    raise   # not 429 or last attempt — let outer except handle

        if response is None:
            return vendor

        text  = _extract_text(response)
        found = _parse_json(text)

        if found:
            filled = []
            for field, val in found.items():
                if val and isinstance(val, str) and not vendor.get(field):
                    vendor[field] = val.strip()[:500]
                    filled.append(field)
            if filled:
                existing = vendor.get("extraction_method", "")
                if "+websearch" not in existing:
                    vendor["extraction_method"] = existing + "+websearch"
                logger.info(f"[WS] {name[:40]} filled: {filled}")
        else:
            logger.debug(f"[WS] {name[:40]} — no JSON in response")

    except Exception as e:
        logger.debug(f"[WS] skipped {name}: {e}")

    return vendor


# ── Batch ─────────────────────────────────────────────────────────────────────

async def websearch_enrich_batch(
    vendors: list[dict],
    max_workers: int = 5,   # reduced from 10 to avoid 429 TPM limit
) -> list[dict]:
    if not vendors:
        return vendors

    sem = asyncio.Semaphore(max_workers)

    async def _enrich(v: dict) -> dict:
        async with sem:
            return await websearch_enrich_vendor(v)

    results = await asyncio.gather(*[_enrich(v) for v in vendors], return_exceptions=True)
    return [vendors[i] if isinstance(r, Exception) else r for i, r in enumerate(results)]
