"""
Stealth helpers: browser fingerprinting, cookie session persistence,
infinite-scroll simulation, and sticky-proxy-per-domain.
"""
from __future__ import annotations

import asyncio
import random
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backend.utils.proxy import ProxyRotator

# ── Lazy-import guards (libraries optional at import time) ─────────────────────
_header_gen = None
_fp_gen = None


def _get_header_gen():
    global _header_gen
    if _header_gen is None:
        try:
            from browserforge.headers import HeaderGenerator
            _header_gen = HeaderGenerator(
                browser=["chrome", "edge"],
                os=["windows", "macos"],
                http_version=2,
            )
        except Exception:
            _header_gen = False  # mark as unavailable
    return _header_gen if _header_gen else None


def _get_fp_gen():
    global _fp_gen
    if _fp_gen is None:
        try:
            from browserforge.fingerprints import FingerprintGenerator, Screen
            _fp_gen = FingerprintGenerator(
                browser="chrome",
                os="windows",
                screen=Screen(min_width=1280, max_width=1920, min_height=720, max_height=1080),
            )
        except Exception:
            _fp_gen = False
    return _fp_gen if _fp_gen else None


# ── Chrome version pool for curl_cffi rotation ────────────────────────────────
CHROME_VERSIONS = ["chrome116", "chrome120", "chrome124", "chrome131"]


def random_chrome_version() -> str:
    return random.choice(CHROME_VERSIONS)


# ── Realistic header generation ───────────────────────────────────────────────
_FALLBACK_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
}


def get_realistic_headers(url: str = "") -> dict:
    gen = _get_header_gen()
    if gen:
        try:
            return dict(gen.generate())
        except Exception:
            pass
    return dict(_FALLBACK_HEADERS)


def get_fingerprint() -> dict | None:
    gen = _get_fp_gen()
    if gen:
        try:
            fp = gen.generate()
            return fp.__dict__ if hasattr(fp, "__dict__") else dict(fp)
        except Exception:
            pass
    return None


# ── Session cookie store (per-domain, in-memory) ──────────────────────────────
_cookie_store: dict[str, list[dict]] = {}


def get_cookies(domain: str) -> list[dict]:
    return list(_cookie_store.get(domain, []))


def save_cookies(domain: str, cookies: list[dict]) -> None:
    if cookies:
        _cookie_store[domain] = cookies


def clear_cookies(domain: str) -> None:
    _cookie_store.pop(domain, None)


# ── Playwright stealth patch ───────────────────────────────────────────────────
async def apply_stealth(page) -> None:
    """Patch a Playwright page with full stealth (playwright-stealth + manual JS)."""
    try:
        from playwright_stealth import stealth_async
        await stealth_async(page)
        return
    except ImportError:
        pass

    # Fallback: manual minimal stealth when playwright-stealth not installed
    await page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
        Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
        window.chrome = {
            runtime: {id: undefined, connect: function(){}, sendMessage: function(){}},
            loadTimes: function(){},
            csi: function(){},
        };
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) =>
            parameters.name === 'notifications'
                ? Promise.resolve({state: Notification.permission})
                : originalQuery(parameters);
    """)


# ── Infinite scroll simulation ─────────────────────────────────────────────────
async def scroll_to_bottom(page, steps: int = 5) -> None:
    """Gradually scroll to bottom; waits for network idle after each step."""
    for _ in range(steps):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(random.uniform(0.6, 1.4))
        try:
            await page.wait_for_load_state("networkidle", timeout=4000)
        except Exception:
            pass  # timeout is acceptable — dynamic content may never fully settle


# ── Sticky proxy per domain ────────────────────────────────────────────────────
_sticky_proxy: dict[str, str] = {}


def get_sticky_proxy(domain: str, rotator: "ProxyRotator | None") -> str | None:
    if not rotator or not getattr(rotator, "active", 0):
        return None
    if domain not in _sticky_proxy:
        proxy = rotator.get()
        if proxy:
            _sticky_proxy[domain] = proxy
    return _sticky_proxy.get(domain)


def clear_sticky_proxies() -> None:
    _sticky_proxy.clear()


# ── Smart scroll — detect new items after each round ─────────────────────────
async def smart_scroll_and_wait(page, max_rounds: int = 10) -> int:
    """
    Scroll loop that detects whether new items appear after each scroll.
    Each round: scroll → wait → count vendor-like elements → compare.
    Stops when item count doesn't increase for 2 consecutive rounds.
    Returns: number of scroll rounds performed.
    """
    prev_count = 0
    stable_rounds = 0
    rounds_done = 0

    _COUNT_JS = """
        () => {
            const selectors = [
                'li[class*="exhibitor"]', 'li[class*="vendor"]',
                '.exhibitor-item', '.vendor-item', '.company-item',
                '[data-exhibitor]', '[data-vendor]', '[data-company]',
                'article', '.card', '.booth', '.stand',
                'tr[data-row]', '.list-item', '.grid-item',
                'li[class*="company"]', 'li[class*="brand"]',
            ];
            let max_count = 0;
            for (const sel of selectors) {
                try {
                    const n = document.querySelectorAll(sel).length;
                    if (n > max_count) max_count = n;
                } catch(e) {}
            }
            return max_count;
        }
    """

    for i in range(max_rounds):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(random.uniform(1.0, 2.0))
        try:
            await page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass

        try:
            count = await page.evaluate(_COUNT_JS)
        except Exception:
            count = 0

        rounds_done = i + 1
        if count > prev_count:
            stable_rounds = 0
            prev_count = count
        else:
            stable_rounds += 1
            if stable_rounds >= 2:
                break

    return rounds_done
