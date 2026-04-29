import asyncio
import random
import re
import sys
import time
import hashlib
import warnings
from typing import Optional, Any
from urllib.parse import urlparse, urljoin

# Suppress harmless Windows asyncio + Playwright subprocess cleanup noise.
# These fire in __del__ after the event loop is already closed — not real errors.
def _suppress_playwright_cleanup(exc_info: object) -> None:
    tp = getattr(exc_info, "exc_type", None)
    msg = str(getattr(exc_info, "exc_value", ""))
    if tp in (RuntimeError, ValueError) and any(
        kw in msg for kw in ("Event loop is closed", "I/O operation on closed pipe")
    ):
        return
    sys.__unraisablehook__(exc_info)

sys.unraisablehook = _suppress_playwright_cleanup
warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed transport")

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
from fake_useragent import UserAgent
from langchain_core.tools import tool
from loguru import logger

from backend.core.config import get_settings

_ua = UserAgent(fallback="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36")
_settings = get_settings()

_response_cache: dict[str, dict] = {}
_page_html_store: dict[str, str] = {}

_domain_semaphores: dict[str, asyncio.Semaphore] = {}
_global_semaphore: asyncio.Semaphore | None = None


def get_cached_html(url: str) -> str:
    key = url.rstrip("/").lower()
    return _page_html_store.get(key, "")


def _store_and_strip(result: dict) -> dict:
    html = result.get("html", "")
    url = result.get("url", "")
    final_url = result.get("final_url", url)
    if html:
        _page_html_store[url.rstrip("/").lower()] = html
        if final_url and final_url != url:
            _page_html_store[final_url.rstrip("/").lower()] = html
    return {
        "url": url,
        "final_url": final_url,
        "status": result.get("status", 0),
        "success": result.get("success", False),
        "content_length": len(html),
        "is_js_rendered": result.get("is_js_rendered", False),
        "response_time": result.get("response_time", 0.0),
        "error": result.get("error", ""),
    }


def _get_global_semaphore() -> asyncio.Semaphore:
    global _global_semaphore
    try:
        loop = asyncio.get_running_loop()
        existing = getattr(loop, "_crawler_global_sem", None)
        if existing is None:
            existing = asyncio.Semaphore(_settings.max_concurrent_requests)
            loop._crawler_global_sem = existing
        return existing
    except RuntimeError:
        return asyncio.Semaphore(_settings.max_concurrent_requests)


def _get_domain_semaphore(domain: str) -> asyncio.Semaphore:
    try:
        loop = asyncio.get_running_loop()
        key = f"_crawler_domain_{domain}"
        existing = getattr(loop, key, None)
        if existing is None:
            existing = asyncio.Semaphore(2)
            setattr(loop, key, existing)
        return existing
    except RuntimeError:
        return asyncio.Semaphore(2)


def _random_headers(url: str = "") -> dict[str, str]:
    ua_string = _ua.random
    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.netloc else ""
    headers = {
        "User-Agent": ua_string,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Cache-Control": "max-age=0",
    }
    if origin:
        headers["Origin"] = origin
        headers["Referer"] = origin + "/"
    return headers


def _url_cache_key(url: str) -> str:
    return hashlib.md5(url.strip().lower().encode()).hexdigest()


def _is_cached(url: str) -> Optional[dict]:
    key = _url_cache_key(url)
    entry = _response_cache.get(key)
    if entry and (time.time() - entry["cached_at"]) < 3600:
        return entry["data"]
    return None


def _set_cache(url: str, data: dict) -> None:
    if len(_response_cache) > 5000:
        oldest_keys = list(_response_cache.keys())[:500]
        for k in oldest_keys:
            del _response_cache[k]
    key = _url_cache_key(url)
    _response_cache[key] = {"data": data, "cached_at": time.time()}


def _empty_result(url: str, error: str, status: int = 0) -> dict:
    return {
        "url": url,
        "html": "",
        "text": "",
        "status": status,
        "is_js_rendered": False,
        "response_time": 0.0,
        "content_type": "",
        "final_url": url,
        "error": error,
        "success": False,
    }


def _success_result(url: str, html: str, status: int, response_time: float,
                    content_type: str = "", final_url: str = "",
                    is_js_rendered: bool = False) -> dict:
    import html2text as h2t
    converter = h2t.HTML2Text()
    converter.ignore_links = False
    converter.ignore_images = True
    converter.ignore_emphasis = False
    converter.body_width = 0
    try:
        text = converter.handle(html)
    except Exception:
        text = ""
    return {
        "url": url,
        "html": html,
        "text": text,
        "status": status,
        "is_js_rendered": is_js_rendered,
        "response_time": response_time,
        "content_type": content_type,
        "final_url": final_url or url,
        "error": "",
        "success": True,
    }


async def _fetch_with_httpx(url: str, client: Optional[httpx.AsyncClient] = None) -> dict:
    start = time.time()
    close_client = client is None
    if close_client:
        from backend.utils.proxy import get_proxy_rotator
        proxy = get_proxy_rotator().next()
        client = httpx.AsyncClient(
            http2=True,
            follow_redirects=True,
            timeout=httpx.Timeout(_settings.request_timeout),
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
            proxy=proxy,
        )
    try:
        delay = random.uniform(_settings.request_delay_min, _settings.request_delay_max)
        await asyncio.sleep(delay)
        response = await client.get(url, headers=_random_headers(url))
        elapsed = time.time() - start
        content_type = response.headers.get("content-type", "")
        if "text/html" not in content_type and "application/xhtml" not in content_type:
            return _empty_result(url, f"Non-HTML content-type: {content_type}", response.status_code)
        html = response.text
        return _success_result(
            url, html, response.status_code, elapsed,
            content_type, str(response.url)
        )
    except httpx.TooManyRedirects:
        return _empty_result(url, "Too many redirects")
    except httpx.TimeoutException:
        return _empty_result(url, "Timeout")
    except httpx.ConnectError as e:
        return _empty_result(url, f"Connection error: {e}")
    except Exception as e:
        return _empty_result(url, str(e))
    finally:
        if close_client:
            await client.aclose()


async def _fetch_with_curl_cffi(url: str) -> dict:
    try:
        from curl_cffi.requests import AsyncSession
    except ImportError:
        return _empty_result(url, "curl_cffi not available")

    start = time.time()
    try:
        delay = random.uniform(_settings.request_delay_min, _settings.request_delay_max)
        await asyncio.sleep(delay)
        from backend.utils.proxy import get_proxy_rotator
        proxy = get_proxy_rotator().next()
        proxy_dict = {"http": proxy, "https": proxy} if proxy else None
        async with AsyncSession(impersonate="chrome120") as session:
            response = await session.get(
                url,
                headers=_random_headers(url),
                timeout=_settings.request_timeout,
                allow_redirects=True,
                proxies=proxy_dict,
            )
        elapsed = time.time() - start
        content_type = response.headers.get("content-type", "")
        if "text/html" not in content_type and "application/xhtml" not in content_type:
            return _empty_result(url, f"Non-HTML: {content_type}", response.status_code)
        return _success_result(url, response.text, response.status_code, elapsed, content_type, url)
    except Exception as e:
        return _empty_result(url, f"curl_cffi error: {e}")


async def _get_loop_browser():
    """
    Playwright browser disimpan per event-loop, bukan global.
    Tiap asyncio.run() punya loop sendiri → browser sendiri → tidak ada cross-loop NoneType.
    """
    from playwright.async_api import async_playwright
    loop = asyncio.get_running_loop()

    browser = getattr(loop, "_crawler_pw_browser", None)
    pw_inst = getattr(loop, "_crawler_pw_instance", None)

    dead = browser is None or not getattr(browser, "is_connected", lambda: False)()
    if dead:
        if pw_inst is not None:
            try:
                await pw_inst.stop()
            except Exception:
                pass
        pw_inst = await async_playwright().start()
        browser = await pw_inst.chromium.launch(
            headless=_settings.playwright_headless,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-extensions",
                "--disable-gpu",
            ],
        )
        loop._crawler_pw_instance = pw_inst
        loop._crawler_pw_browser = browser

    return browser


async def _fetch_with_playwright(url: str) -> dict:
    start = time.time()
    try:
        browser = await _get_loop_browser()
        from backend.utils.proxy import get_proxy_rotator
        proxy = get_proxy_rotator().next()
        context_kwargs: dict = {
            "viewport": {"width": 1280, "height": 800},
            "user_agent": _ua.random,
            "java_script_enabled": True,
            "accept_downloads": False,
        }
        if proxy:
            parsed_proxy = urlparse(proxy)
            pw_proxy: dict = {"server": f"{parsed_proxy.scheme}://{parsed_proxy.hostname}:{parsed_proxy.port}"}
            if parsed_proxy.username:
                pw_proxy["username"] = parsed_proxy.username
            if parsed_proxy.password:
                pw_proxy["password"] = parsed_proxy.password
            context_kwargs["proxy"] = pw_proxy
        context = await browser.new_context(**context_kwargs)
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = {runtime: {}};
        """)

        page = await context.new_page()
        page.set_default_timeout(_settings.playwright_timeout)

        await asyncio.sleep(random.uniform(0.5, 1.5))
        try:
            response = await page.goto(url, wait_until="domcontentloaded")
            # "load" jauh lebih cepat dari "networkidle" — tidak perlu tunggu analytics/ads selesai
            await page.wait_for_load_state("load", timeout=_settings.playwright_timeout)
            html = await page.content()
            status = response.status if response else 200
            elapsed = time.time() - start
            await context.close()
            return _success_result(url, html, status, elapsed, "text/html", url, is_js_rendered=True)
        except Exception as e:
            try:
                await context.close()
            except Exception:
                pass
            raise

    except Exception as e:
        err_str = str(e).lower()
        if "nonetype" in err_str or "has no attribute" in err_str or "'send'" in err_str or "closed" in err_str:
            try:
                loop = asyncio.get_running_loop()
                loop._crawler_pw_browser = None
                loop._crawler_pw_instance = None
            except Exception:
                pass
            logger.debug("Playwright browser reset for loop")

        domain = urlparse(url).netloc
        if "timeout" in err_str:
            logger.warning(f"[FETCH] {domain} tidak merespons (timeout), URL dilewati")
        elif "net::err_name_not_resolved" in err_str or "name not resolved" in err_str:
            logger.warning(f"[FETCH] {domain} tidak ditemukan (domain mungkin tidak aktif), URL dilewati")
        elif "net::err" in err_str or "connection" in err_str:
            logger.warning(f"[FETCH] {domain} tidak dapat dijangkau, URL dilewati")
        else:
            logger.warning(f"[FETCH] {domain} gagal diakses via Playwright")
        logger.debug(f"Playwright detail [{domain}]: {e}")
        return _empty_result(url, f"Playwright error: {e}")


def _needs_playwright(result: dict) -> bool:
    html = result.get("html", "")
    if not html:
        return True
    lower = html.lower()
    js_indicators = [
        # Next.js (Pages Router)
        'id="__next"', "data-reactroot",
        # Next.js (App Router) — /_next/static/ appears in CSS/JS link tags
        "/_next/static/",
        # Other SPA frameworks
        'id="root">', 'id="app">',
        "react-app", "ng-version", "vue-app", "__nuxt",
        # Anti-bot / loading gates
        "please enable javascript", "javascript is required",
        "cloudflare", "checking your browser",
        "<noscript>",
    ]
    js_count = sum(1 for ind in js_indicators if ind in lower)

    # Text density: strip script/style blocks first (including their inline content),
    # then strip remaining tags — what's left is only visible text
    text_stripped = re.sub(r"<(script|style)[^>]*>.*?</(script|style)>", " ", html,
                           flags=re.DOTALL | re.IGNORECASE)
    text_only = re.sub(r"<[^>]+>", " ", text_stripped)
    text_only = re.sub(r"\s+", " ", text_only).strip()
    content_sparse = len(text_only) < 3000

    return js_count >= 2 or (js_count >= 1 and content_sparse)


async def _fetch_jina_markdown(url: str) -> str:
    """
    Fetch a page (or PDF) via Jina AI Reader (r.jina.ai) and return clean markdown.
    Works without an API key (free, rate-limited). Key adds higher rate limits.
    Handles HTML pages and PDFs transparently.
    """
    settings = get_settings()
    jina_url = f"https://r.jina.ai/{url}"

    # Auth header only when key is available — not required
    headers: dict[str, str] = {
        "X-Return-Format": "markdown",
        "Accept": "text/markdown, text/plain",
    }
    if settings.has_jina_key:
        headers["Authorization"] = f"Bearer {settings.jina_api_key}"

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(90.0),   # PDFs can be slow
            follow_redirects=True,
            headers=headers,
        ) as client:
            resp = await client.get(jina_url)
            if resp.status_code == 200:
                text = resp.text.strip()
                logger.debug(f"[JINA] Fetched {len(text):,} chars from {url}")
                return text
            else:
                logger.warning(f"[JINA] status={resp.status_code} for {url}")
                return ""
    except Exception as e:
        logger.warning(f"[JINA] Failed for {url}: {type(e).__name__}: {e}")
        return ""


async def _fetch_firecrawl_parse(url: str) -> str:
    """
    Download a PDF (or any document) from a public URL and parse it via
    Firecrawl /v2/parse (multipart/form-data).  5x faster than Jina for
    large PDFs thanks to Firecrawl's Rust engine.

    Requires FIRECRAWL_API_KEY in .env.
    Falls back gracefully (returns "") if key missing or request fails.

    Flow:
        1. Download raw bytes from `url` with httpx
        2. POST bytes to https://api.firecrawl.dev/v2/parse
        3. Return `data.markdown` from the JSON response
    """
    import json as _json
    settings = get_settings()
    if not settings.has_firecrawl_key:
        return ""

    # ── Step 1: download PDF bytes ────────────────────────────────────────────
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(120.0),
            follow_redirects=True,
            headers={"User-Agent": _ua.random},
        ) as dl_client:
            dl_resp = await dl_client.get(url)
            dl_resp.raise_for_status()
            pdf_bytes = dl_resp.content
    except Exception as e:
        logger.warning(f"[FC-PARSE] Failed to download {url}: {type(e).__name__}: {e}")
        return ""

    if not pdf_bytes:
        logger.warning(f"[FC-PARSE] Empty bytes from {url}")
        return ""

    logger.info(f"[FC-PARSE] Downloaded {len(pdf_bytes):,} bytes from {url}")

    # ── Step 2: POST to Firecrawl /v2/parse ──────────────────────────────────
    filename = url.rstrip("/").split("/")[-1].split("?")[0] or "document.pdf"
    parse_options = _json.dumps({
        "formats": [{"type": "markdown"}],
        "parsers": [{"type": "pdf", "mode": "auto"}],
        "onlyMainContent": False,
    })

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(180.0)) as fc_client:
            resp = await fc_client.post(
                f"{settings.firecrawl_base_url}/v2/parse",
                headers={"Authorization": f"Bearer {settings.firecrawl_api_key}"},
                files=[
                    ("file",    (filename, pdf_bytes, "application/pdf")),
                    ("options", (None, parse_options, "application/json")),
                ],
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        logger.warning(f"[FC-PARSE] API call failed for {url}: {type(e).__name__}: {e}")
        return ""

    markdown = (data.get("data") or {}).get("markdown", "")
    if markdown:
        logger.info(f"[FC-PARSE] Got {len(markdown):,} chars of markdown for {url}")
    else:
        logger.warning(f"[FC-PARSE] No markdown in response for {url}. success={data.get('success')}")

    return markdown


async def fetch_page_async(url: str) -> dict:
    cached = _is_cached(url)
    if cached:
        return cached

    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return _empty_result(url, "Invalid URL")

    settings = get_settings()
    if settings.is_ignored_extension(url):
        return _empty_result(url, "Ignored file extension")

    domain = parsed.netloc.lower()
    global_sem = _get_global_semaphore()
    domain_sem = _get_domain_semaphore(domain)

    async with global_sem:
        async with domain_sem:
            result = await _fetch_with_httpx(url)

            if not result["success"] and "ssl" in result.get("error", "").lower():
                logger.debug(f"Retrying with curl_cffi (TLS issue): {url}")
                result = await _fetch_with_curl_cffi(url)

            if not result["success"] and result.get("status") in {403, 429}:
                logger.debug(f"Retrying with curl_cffi (blocked): {url}")
                await asyncio.sleep(random.uniform(2.0, 5.0))
                result = await _fetch_with_curl_cffi(url)

            if result["success"] and _needs_playwright(result):
                logger.debug(f"Upgrading to Playwright (JS-heavy): {url}")
                pw_result = await _fetch_with_playwright(url)
                if pw_result["success"]:
                    result = pw_result

    if result["success"]:
        _set_cache(url, result)

    return result


async def fetch_pages_batch_async(urls: list[str], on_done=None) -> list[dict]:
    """
    on_done(url, result) dipanggil tiap URL selesai — untuk live progress update.
    """
    semaphore = asyncio.Semaphore(_settings.max_concurrent_requests)
    results = [None] * len(urls)

    async def bounded_fetch(i: int, url: str) -> None:
        async with semaphore:
            try:
                r = await fetch_page_async(url)
            except Exception as e:
                r = _empty_result(url, str(e))
            results[i] = r
            if on_done:
                try:
                    on_done(url, r)
                except Exception:
                    pass

    await asyncio.gather(*[bounded_fetch(i, url) for i, url in enumerate(urls)])
    return results


@tool
def fetch_page(url: str) -> dict:
    """
    Fetch a single web page. Returns metadata only (url, status, success, content_length).
    HTML is stored in internal cache and used automatically by run_extraction_pipeline.
    """
    try:
        try:
            loop = asyncio.get_event_loop()
            is_running = loop.is_running()
        except RuntimeError:
            is_running = False
        if is_running:
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, fetch_page_async(url))
                result = future.result(timeout=60)
        else:
            result = asyncio.run(fetch_page_async(url))
        return _store_and_strip(result)
    except Exception as e:
        logger.error(f"fetch_page failed for {url}: {e}")
        return _store_and_strip(_empty_result(url, str(e)))


@tool
def fetch_pages_batch(urls: list[str]) -> list[dict]:
    """
    Fetch multiple pages concurrently. Returns metadata list only (url, status, success,
    content_length, is_js_rendered). HTML is stored in internal cache and used automatically
    by run_extraction_pipeline — do NOT pass html to other tools.
    """
    if not urls:
        return []
    urls = list(dict.fromkeys(urls))[:_settings.batch_size]
    try:
        try:
            loop = asyncio.get_event_loop()
            is_running = loop.is_running()
        except RuntimeError:
            is_running = False
        if is_running:
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, fetch_pages_batch_async(urls))
                results = future.result(timeout=300)
        else:
            results = asyncio.run(fetch_pages_batch_async(urls))
        return [_store_and_strip(r) for r in results]
    except Exception as e:
        logger.error(f"fetch_pages_batch failed: {e}")
        return [_store_and_strip(_empty_result(u, str(e))) for u in urls]


@tool
def check_robots_txt(domain: str) -> dict:
    """
    Fetch and parse robots.txt for a domain.
    Returns allowed/disallowed paths and crawl-delay.
    """
    import urllib.robotparser
    url = f"https://{domain}/robots.txt"
    result = {
        "domain": domain,
        "crawl_delay": None,
        "disallowed_paths": [],
        "allowed": True,
        "error": "",
    }
    try:
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(url)
        rp.read()
        result["crawl_delay"] = rp.crawl_delay("*")
        result["allowed"] = rp.can_fetch("*", f"https://{domain}/")
        disallowed = []
        for entry in rp.entries:
            for rule in entry.rulelines:
                if not rule.allowance:
                    disallowed.append(rule.path)
        result["disallowed_paths"] = disallowed[:50]
    except Exception as e:
        result["error"] = str(e)
    return result


@tool
def resolve_final_url(url: str) -> dict:
    """
    Follow redirects and return the final URL after all redirects.
    Useful for tracking canonical URLs.
    """
    try:
        import httpx as _httpx
        with _httpx.Client(follow_redirects=True, timeout=15) as client:
            resp = client.head(url, headers=_random_headers(url))
            return {
                "original_url": url,
                "final_url": str(resp.url),
                "status": resp.status_code,
                "redirect_count": len(resp.history),
                "error": "",
            }
    except Exception as e:
        return {
            "original_url": url,
            "final_url": url,
            "status": 0,
            "redirect_count": 0,
            "error": str(e),
        }


async def close_playwright() -> None:
    try:
        loop = asyncio.get_running_loop()
        browser = getattr(loop, "_crawler_pw_browser", None)
        pw_inst = getattr(loop, "_crawler_pw_instance", None)
        if browser:
            try:
                await browser.close()
            except Exception:
                pass
        if pw_inst:
            try:
                await pw_inst.stop()
            except Exception:
                pass
        loop._crawler_pw_browser = None
        loop._crawler_pw_instance = None
    except Exception as e:
        logger.debug(f"close_playwright: {e}")


def get_cache_stats() -> dict:
    return {
        "cached_urls": len(_response_cache),
        "domain_limiters": len(_domain_limiters),
    }


ALL_FETCH_TOOLS = [fetch_page, fetch_pages_batch, check_robots_txt, resolve_final_url]
