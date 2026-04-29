import asyncio
import concurrent.futures

from langchain_core.tools import tool
from loguru import logger

from backend.tools.vendor_registry import register_vendors, get_count
from backend.tools.fetch_tools import get_cached_html, fetch_page_async
from openhands_parser.generator import ParserGenerator

_generator = ParserGenerator()


def _run_coro_sync(coro):
    try:
        loop = asyncio.get_event_loop()
        is_running = loop.is_running()
    except RuntimeError:
        is_running = False
    if is_running:
        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, coro).result(timeout=300)
    else:
        return asyncio.run(coro)


@tool
def generate_and_run_parser(url: str, hint: str = "") -> dict:
    """
    AI-generates a Python parser for this URL's domain (OpenHands or LLM fallback).
    Caches working parsers per domain — reused instantly on subsequent calls.
    Auto-registers extracted vendors into the registry.

    Use when run_extraction_pipeline returns 0 vendors on a listing/directory page,
    or when the page has a complex structure not handled by standard extractors.

    Args:
        url: Page URL to parse (HTML must be fetchable)
        hint: Optional hint for the AI (e.g. "table with columns: name, country, booth")

    Returns:
        {registered, total_in_registry, cache_hit, domain, sample}
    """
    html = get_cached_html(url)
    if not html:
        logger.info(f"[DYN] fetching {url} (not in cache)")
        try:
            fetch_result = _run_coro_sync(fetch_page_async(url))
            html = get_cached_html(url) or ""
        except Exception as e:
            logger.error(f"[DYN] fetch failed for {url}: {e}")
            return {
                "registered": 0,
                "total_in_registry": get_count(),
                "cache_hit": False,
                "domain": url,
                "error": str(e),
            }

    if not html:
        return {
            "registered": 0,
            "total_in_registry": get_count(),
            "cache_hit": False,
            "domain": url,
            "error": "Empty HTML",
        }

    result = _run_coro_sync(_generator.generate_and_run(url, html, hint))
    vendors = result["vendors"]

    if vendors:
        register_vendors(vendors)

    return {
        "registered": len(vendors),
        "total_in_registry": get_count(),
        "cache_hit": result["cache_hit"],
        "domain": result["domain"],
        "sample": vendors[:3],
    }
