"""
URL Worker Agent — autonomous per-URL crawler with parallel pool.

Architecture:
  - crawl_urls_parallel: orchestrator-facing tool; spawns up to 15 workers in parallel
  - crawl_url_deep: single-URL deep crawl (also callable directly)
  - _URLWorkerAgent: internal class — one autonomous LLM agent per URL
  - check_pagination: worker tool wrapping detect_next_button with HTML-cache auto-read
  - click_and_extract: worker tool for JS "Load More" buttons via Playwright
"""
from __future__ import annotations

import asyncio
import json
import time
from urllib.parse import urlparse

from langchain_core.messages import HumanMessage, ToolMessage
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
from loguru import logger

from backend.core.config import get_settings
from backend.tools.temp_store import (
    get_or_create_session_id,
    load_worker_progress,
    save_worker_progress,
)
from backend.tools.vendor_registry import get_count

_settings = get_settings()

_WORKER_MAX_OUTPUT_CHARS = 8_000
_WORKER_RECURSION_LIMIT = 60
_WORKER_TIMEOUT_SECONDS = 600   # 10 minutes per URL

# ── Pre-model hook: trim oversized tool outputs ───────────────────────────────

def _worker_pre_model_hook(state: dict) -> dict:
    trimmed = []
    for msg in state.get("messages", []):
        if isinstance(msg, ToolMessage) and isinstance(msg.content, str):
            if len(msg.content) > _WORKER_MAX_OUTPUT_CHARS:
                content = (
                    msg.content[:_WORKER_MAX_OUTPUT_CHARS]
                    + "\n[output dipotong karena terlalu panjang]"
                )
                msg = ToolMessage(
                    content=content,
                    tool_call_id=msg.tool_call_id,
                    name=getattr(msg, "name", ""),
                    id=msg.id,
                )
        trimmed.append(msg)
    return {"messages": trimmed}


# ── Worker-specific tools ─────────────────────────────────────────────────────

@tool
def check_pagination(url: str) -> dict:
    """
    Cari URL halaman berikutnya atau selector tombol Load More dari halaman yang sudah difetch.
    WAJIB: URL harus sudah difetch sebelumnya via fetch_page().
    Return: {next_url, needs_click, selector}
    - next_url: URL halaman berikutnya (kosong jika tidak ada)
    - needs_click: True jika ada tombol JS yang perlu diklik
    - selector: CSS selector tombol jika needs_click=True
    """
    from backend.tools.fetch_tools import get_cached_html
    from backend.tools.parse_tools import detect_next_button

    html = get_cached_html(url)
    if not html:
        return {
            "next_url": "",
            "needs_click": False,
            "selector": "",
            "message": "Halaman belum difetch — panggil fetch_page(url) dulu.",
        }

    try:
        result = detect_next_button.invoke({"html": html[:60_000], "base_url": url})
        if isinstance(result, dict):
            return result
    except Exception as e:
        logger.debug(f"[WORKER] check_pagination error for {url}: {e}")

    return {"next_url": "", "needs_click": False, "selector": ""}


@tool
async def click_and_extract(url: str, selector: str) -> dict:
    """
    Klik tombol 'Load More' / 'See More' di halaman via Playwright,
    tunggu konten baru, update HTML cache, lalu ekstrak vendor baru.
    Gunakan ketika check_pagination() mengembalikan needs_click=True.
    Return: {html_updated, message, hint}
    """
    from backend.tools.fetch_tools import _get_loop_browser, _page_html_store  # noqa: PLC2701
    from backend.tools.stealth_tools import apply_stealth, smart_scroll_and_wait

    try:
        browser = await _get_loop_browser()
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()
        await apply_stealth(page)

        await page.goto(url, timeout=30_000, wait_until="domcontentloaded")
        try:
            await page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            pass

        # Click the Load More / See More button
        try:
            await page.click(selector, timeout=5_000)
            await asyncio.sleep(1.5)
            try:
                await page.wait_for_load_state("networkidle", timeout=8_000)
            except Exception:
                pass
            await smart_scroll_and_wait(page, max_rounds=5)
        except Exception as click_err:
            await context.close()
            return {
                "html_updated": False,
                "message": f"Click gagal ({click_err}). Coba selector lain atau navigasi manual.",
                "hint": f"fetch_page({url!r}) lalu check_pagination lagi",
            }

        # Store updated HTML so run_extraction_pipeline can use it
        new_html = await page.content()
        cache_key = url.rstrip("/").lower()
        _page_html_store[cache_key] = new_html
        await context.close()

        return {
            "html_updated": True,
            "message": (
                f"Tombol '{selector}' diklik, konten baru dimuat. "
                f"HTML cache diupdate untuk {url}."
            ),
            "hint": (
                f"Panggil discover_vendor_urls({url!r}) untuk ekstrak semua item baru, "
                f"atau run_extraction_pipeline({url!r}) untuk satu vendor."
            ),
        }

    except Exception as e:
        logger.warning(f"[WORKER] click_and_extract failed for {url}: {e}")
        return {
            "html_updated": False,
            "message": f"Error: {e}",
            "hint": "Coba fetch_page() manual lalu discover_vendor_urls()",
        }


# ── Worker system prompt ──────────────────────────────────────────────────────

def _build_worker_prompt(url: str, event_context: dict, max_per_worker: int = 0) -> str:
    event_name = event_context.get("event_name", "unknown event")
    event_location = event_context.get("event_location", "")
    event_date = event_context.get("event_date", "")
    url_domain = _domain_of(url)

    limit_line = (
        f"\nBATAS VENDOR URL INI: {max_per_worker} vendor. "
        f"Panggil get_vendor_count() secara berkala. "
        f"BERHENTI segera saat jumlah vendormu dari URL ini sudah mencapai {max_per_worker}."
        if max_per_worker > 0
        else "\nKumpulkan SEMUA vendor tanpa batas."
    )

    return f"""Kamu adalah URL Worker Agent yang sangat tekun dan tidak mudah menyerah.
SATU-SATUNYA TUGASMU: Ekstrak vendor/exhibitor dari domain ini.{limit_line}

URL AWAL: {url}
Event: {event_name} | {event_location} | {event_date}

VENDOR REGISTRY: Semua vendor yang diekstrak OTOMATIS tersimpan di registry internal.
Gunakan get_vendor_count() untuk memantau progress.

═══════════════════════════════════════════════════════
STRATEGI EKSTRAKSI (IKUTI URUTAN INI!)
═══════════════════════════════════════════════════════

LANGKAH 1 — FETCH HALAMAN:
  fetch_page(url) → lihat apakah ini halaman daftar exhibitor.

  Jika bukan halaman daftar (404 / About / Contact / bukan listing):
    → Coba path umum situs event:
        /exhibitors, /exhibitor-list, /participants, /companies, /directory
        /vendors, /brand, /sponsors, /members, /showfloor, /katilimcilar
    → fetch_page(kandidat) → evaluasi lagi
    → Jika tidak ketemu: search_company_info(name="site:{url_domain} exhibitor list", country="")
    → fetch_page(hasil search) → evaluasi

LANGKAH 2 ★ PATH UTAMA — EXTRACT LANGSUNG DARI LISTING:
  ★★★ GUNAKAN INI SEBAGAI LANGKAH PERTAMA setelah menemukan halaman listing! ★★★

  extract_vendors_from_listing(
      url=<url halaman listing>,
      event_context='{{"event_name":"{event_name}","event_location":"{event_location}","event_date":"{event_date}"}}'
  )
  Tool ini:
  • Jina AI Reader → otomatis render JavaScript (React/Vue/Angular tidak masalah!)
  • LLM ekstrak SEMUA exhibitor dari halaman sekaligus (bukan satu-satu)
  • Kuliti semuanya: booth_number, pavilion, country, products, dll — tidak ada schema tetap
  • Jauh lebih efektif dari discover_vendor_urls → extract_all_vendor_profiles

  Lihat return: vendors_found=N
  Jika N > 0 → sukses! Lanjut ke LANGKAH 3 (pagination).
  Jika vendors_found=0 (Jina gagal/rate limit/diblokir) → LANGSUNG ke LANGKAH 2B.
  JANGAN coba extract_vendors_from_listing berkali-kali pada URL berbeda kalau sudah 429!

LANGKAH 2B — SEARCH DULU kalau Jina gagal:
  Jika extract_vendors_from_listing gagal (error/vendors_found=0):
  a. Cari halaman exhibitor via search:
       search_company_info(
           name="site:{url_domain} exhibitor list OR participants OR peserta 2025 2026",
           country=""
       )
     → Ambil URL paling relevan dari hasil search
     → fetch_page(url_hasil_search)
     → extract_vendors_from_listing(url_hasil_search, event_context=...)
  b. Atau coba path umum langsung (TANPA Jina dulu, pakai fetch_page biasa):
       Coba: /exhibitors, /participants, /directory, /companies, /vendors
       fetch_page(kandidat) → kalau dapat HTML dengan banyak nama perusahaan →
       extract_vendors_from_listing(kandidat, event_context=...)

LANGKAH 3 — KEJAR SEMUA PAGINATION (WAJIB!):
  Setelah setiap halaman, cek pagination:
  check_pagination(url=<url halaman yang baru diproses>)

  Jika next_url ada:
    → fetch_page(next_url)
    → extract_vendors_from_listing(url=next_url, event_context=...)
    → Ulangi check_pagination untuk next_url berikutnya
    → Terus sampai next_url KOSONG

  Jika needs_click=True (tombol Load More / See More):
    → click_and_extract(url=..., selector=...)
    → extract_vendors_from_listing(url=..., event_context=...) lagi pada konten baru

LANGKAH 4 — FALLBACK TERAKHIR (hanya jika semua di atas gagal):
  a. Coba intercept API: intercept_api_vendors(url=...) → untuk SPA yang pakai XHR
  b. Coba discover profil individual (HANYA jika Jina tidak rate-limited):
       discover_vendor_urls(url=..., max_urls=200)
       → Jika dapat ≥5 URL profil yang JELAS exhibitor (bukan about/joinus/culture/dll):
         extract_all_vendor_profiles(vendor_urls=[...], event_context=..., max_concurrent=8)
       → JANGAN panggil ini untuk URL navigasi/junk!

  ⚠ PERINGATAN: Jika Jina sudah return 429 (rate limit), BERHENTI panggil Jina!
     Gunakan search_company_info untuk cari alternatif, bukan brute-force Jina.

LANGKAH 5 — SELESAI:
  get_vendor_count() → laporan final
  BERHENTI hanya jika:
  ✓ Semua halaman listing sudah diproses via extract_vendors_from_listing
  ✓ Semua pagination sudah diikuti (check_pagination return kosong)
  ✓ Tidak ada Load More yang tersisa

═══════════════════════════════════════════════════════
TOOLS YANG TERSEDIA
═══════════════════════════════════════════════════════
- fetch_page(url): Fetch satu halaman, simpan ke cache
- fetch_pages_batch(urls): Fetch BANYAK halaman PARALEL sekaligus → lebih cepat
- extract_vendors_from_listing(url, event_context):
    ★★★ PRIMARY EXTRACTOR ★★★ — Jina + LLM, ekstrak SEMUA vendor dari listing page
    Otomatis render JavaScript. Kuliti semuanya — tidak ada schema tetap.
    SELALU coba ini PERTAMA pada setiap halaman listing!
- check_pagination(url): Cari next page URL atau Load More selector
- click_and_extract(url, selector): Klik Load More → update HTML cache
- intercept_api_vendors(url): Intersep XHR/API vendor di halaman SPA
- extract_all_vendor_profiles(vendor_urls, event_context, max_concurrent=8):
    FALLBACK — untuk situs yang punya URL profil individual per vendor
- run_extraction_pipeline(url): Fallback terakhir — satu URL spesifik
- discover_vendor_urls(url, max_urls=200): Temukan URL profil individual di halaman listing
- get_vendor_count(): Cek total vendor di registry
- search_company_info(name, country): Cari info/URL via search engine

PERINGATAN KERAS:
- JANGAN berhenti setelah 1 halaman — kejar SEMUA pagination!
- JANGAN gunakan run_extraction_pipeline pada listing page (akan ekstrak organizer, bukan exhibitor!)
- JANGAN export atau dedup — itu tugas orchestrator
- JANGAN pass vendor list ke LLM — semua otomatis ke registry
- Kamu punya 10 menit. Manfaatkan sepenuhnya!
"""


# ── URL Worker Agent ──────────────────────────────────────────────────────────

class _URLWorkerAgent:
    """Internal class — one autonomous LLM agent per URL."""

    def __init__(
        self,
        url: str,
        event_context: dict,
        session_id: str,
        worker_id: int,
        max_per_worker: int = 0,
    ) -> None:
        self.url = url
        self.event_context = event_context
        self.session_id = session_id
        self.worker_id = worker_id
        self.max_per_worker = max_per_worker
        self._domain = _domain_of(url)

        # Import tools here to avoid top-level circular imports
        from backend.tools.fetch_tools import fetch_page, fetch_pages_batch
        from backend.tools.extract_tools import (
            discover_vendor_urls,
            run_extraction_pipeline,
            extract_all_vendor_profiles,
            extract_vendors_from_listing,
        )
        from backend.tools.parse_tools import intercept_api_vendors
        from backend.tools.search_tools import search_company_info
        from backend.tools.vendor_registry import get_vendor_count

        worker_tools = [
            fetch_page,
            fetch_pages_batch,              # batch-fetch before extraction (speed)
            check_pagination,
            intercept_api_vendors,
            extract_vendors_from_listing,   # ★ PRIMARY: Jina+LLM batch listing extractor
            extract_all_vendor_profiles,    # FALLBACK: parallel LLM on individual profile URLs
            run_extraction_pipeline,        # LAST RESORT: single-URL fallback
            discover_vendor_urls,
            get_vendor_count,
            search_company_info,
            click_and_extract,
        ]

        llm = ChatOpenAI(
            model=_settings.openai_model,
            api_key=_settings.openai_api_key,
            streaming=False,   # workers don't need streaming
        )

        self._agent = create_react_agent(
            model=llm,
            tools=worker_tools,
            prompt=_build_worker_prompt(url, event_context, max_per_worker),
            pre_model_hook=_worker_pre_model_hook,
        )

    async def run(self) -> dict:
        # Resume from temp file if already done
        prev = load_worker_progress(self.session_id, self.url)
        if prev and prev.get("status") == "done":
            logger.debug(
                f"[W{self.worker_id:02d}] {self._domain} — skipped (already done, "
                f"{prev.get('vendors_found', 0)} vendors)"
            )
            return prev

        start_time = time.time()
        status = "error"
        error_msg = ""

        # Snapshot existing vendor keys BEFORE this worker starts.
        # Used at the end to count only what THIS worker added (not other parallel workers).
        from backend.tools.vendor_registry import get_all_vendors as _gv
        _existing_keys: frozenset = frozenset(
            f"{v.get('name', '')}|{v.get('source_url', '')}"
            for v in _gv()
        )
        start_count = get_count()   # kept for heartbeat delta (approximate, fast)

        logger.debug(f"[W{self.worker_id:02d}] {self._domain} — started: {self.url}")
        save_worker_progress(
            self.session_id, self.url,
            {"status": "running", "vendors_found": 0},
        )

        # Wrap ainvoke in a Task so the monitor can cancel it when limit is hit.
        # Use ainvoke (not astream_events) → worker events don't bubble up to orchestrator.
        invoke_task = asyncio.create_task(
            self._agent.ainvoke(
                {"messages": [HumanMessage(content=f"Mulai deep crawl: {self.url}")]},
                config={"recursion_limit": _WORKER_RECURSION_LIMIT},
            )
        )

        # Monitor: heartbeat every 10 s + cancel when per-worker vendor limit is reached.
        async def _monitor():
            try:
                while not invoke_task.done():
                    await asyncio.sleep(10)
                    current = get_count() - start_count
                    elapsed_so_far = round(time.time() - start_time, 0)
                    logger.debug(
                        f"[W{self.worker_id:02d}] {self._domain} — "
                        f"running {elapsed_so_far:.0f}s · {current} vendors"
                    )
                    if current > 0:
                        save_worker_progress(
                            self.session_id, self.url,
                            {"status": "running", "vendors_found": current},
                        )
                    # Cancel agent if per-worker limit reached
                    if self.max_per_worker > 0 and current >= self.max_per_worker:
                        logger.debug(
                            f"[W{self.worker_id:02d}] {self._domain} — "
                            f"limit {self.max_per_worker} vendors reached → stopping"
                        )
                        invoke_task.cancel()
                        break
            except asyncio.CancelledError:
                pass

        monitor_task = asyncio.create_task(_monitor())

        try:
            await invoke_task
            status = "done"

        except asyncio.CancelledError:
            # Could be limit-reached (monitor cancelled) or external timeout cancel
            current = get_count() - start_count
            if self.max_per_worker > 0 and current >= self.max_per_worker:
                status = "limit_reached"
            else:
                status = "cancelled"
        except asyncio.TimeoutError:
            status = "timeout"
            logger.warning(f"[W{self.worker_id:02d}] {self._domain} — timeout after {_WORKER_TIMEOUT_SECONDS}s")
        except Exception as exc:
            status = "error"
            error_msg = str(exc)
            logger.warning(f"[W{self.worker_id:02d}] {self._domain} — error: {exc}")
        finally:
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

        # Accurate per-worker count: compare against snapshot taken before this worker ran.
        _current_vendors = _gv()
        vendors_found = sum(
            1 for v in _current_vendors
            if f"{v.get('name', '')}|{v.get('source_url', '')}" not in _existing_keys
        )
        elapsed = round(time.time() - start_time, 1)

        logger.debug(
            f"[W{self.worker_id:02d}] {self._domain} — {status}: "
            f"{vendors_found} vendors in {elapsed}s"
        )

        result = {
            "url": self.url,
            "domain": self._domain,
            "status": status,
            "vendors_found": vendors_found,
            "pages_crawled": 0,   # not trackable with ainvoke; registry is the source of truth
            "elapsed": elapsed,
        }
        if error_msg:
            result["error"] = error_msg

        save_worker_progress(self.session_id, self.url, result)
        return result


# ── Logging helpers ───────────────────────────────────────────────────────────

def _domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "")[:30]
    except Exception:
        return url[:30]


_LOGGED_TOOLS = {
    "fetch_page", "check_pagination", "discover_vendor_urls",
    "run_extraction_pipeline", "click_and_extract", "intercept_api_vendors",
    "get_vendor_count", "search_company_info",
}


def _log_tool_start(event: dict, worker_id: int, domain: str) -> None:
    """Kept for debugging; not called in production (workers use ainvoke)."""
    name = event.get("name", "")
    if name not in _LOGGED_TOOLS:
        return
    args = event.get("data", {}).get("input", {})
    url_arg = ""
    if isinstance(args, dict):
        url_arg = args.get("url", args.get("name", ""))
    elif isinstance(args, str):
        url_arg = args[:80]
    logger.debug(
        f"[W{worker_id:02d}] {domain} → {name}"
        + (f"({str(url_arg)[:70]})" if url_arg else "")
    )


def _log_tool_end(event: dict, worker_id: int, domain: str) -> None:
    """Kept for debugging; not called in production (workers use ainvoke)."""
    name = event.get("name", "")
    if name not in _LOGGED_TOOLS:
        return
    raw = event.get("data", {}).get("output")
    summary = ""
    if isinstance(raw, dict):
        if "total_vendors" in raw:
            summary = f"{raw['total_vendors']} total vendors"
        elif "registered" in raw:
            summary = f"+{raw.get('registered', 0)} vendors registered"
        elif "next_url" in raw:
            nu = raw.get("next_url", "")
            nc = raw.get("needs_click", False)
            summary = f"next={nu[:50]!r}" if nu else ("needs_click" if nc else "no next page")
        elif "html_updated" in raw:
            summary = "HTML updated ✓" if raw.get("html_updated") else "click failed"
        elif name == "run_extraction_pipeline" and "name" in raw:
            summary = f"vendor: {str(raw.get('name',''))[:40]}"
    elif isinstance(raw, list):
        summary = f"{len(raw)} items"
    elif isinstance(raw, str):
        summary = raw[:60]
    if summary:
        logger.debug(f"[W{worker_id:02d}] {domain}  ↳ {name}: {summary}")


# ── Public tools for orchestrator ────────────────────────────────────────────

@tool
async def crawl_url_deep(
    url: str,
    event_context: str = "{}",
    max_per_worker: int = 0,
) -> dict:
    """
    Deep-crawl satu URL menggunakan autonomous LLM URL Worker Agent.
    Worker secara mandiri menavigasi halaman yang salah/404, mengikuti semua
    pagination, scroll, klik Load More, dan mengekstrak semua vendor hingga habis.
    event_context: JSON string dengan key event_name, event_location, event_date (opsional).
    max_per_worker: batas vendor per URL ini (0 = unlimited).
    Return: {url, domain, status, vendors_found, pages_crawled, elapsed}
    """
    ctx: dict = {}
    if isinstance(event_context, str) and event_context.strip():
        try:
            ctx = json.loads(event_context)
        except Exception:
            pass
    elif isinstance(event_context, dict):
        ctx = event_context

    session_id = get_or_create_session_id()
    worker = _URLWorkerAgent(url, ctx, session_id, worker_id=0, max_per_worker=max_per_worker)

    try:
        return await asyncio.wait_for(worker.run(), timeout=_WORKER_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        return {
            "url": url,
            "domain": _domain_of(url),
            "status": "timeout",
            "vendors_found": 0,
            "pages_crawled": 0,
            "elapsed": _WORKER_TIMEOUT_SECONDS,
        }


@tool
async def crawl_urls_parallel(
    urls: list[str],
    max_workers: int = 15,
    event_context: str = "{}",
    max_per_worker: int = 0,
) -> dict:
    """
    Deep-crawl banyak URL secara paralel menggunakan pool URL Worker Agents.
    Setiap worker mendapat satu URL dan bekerja OTONOM (navigasi cerdas, scroll,
    pagination tak terbatas, Load More) sampai semua vendor terekstrak.

    urls: daftar seed URLs dari search_exhibitor_events
    max_workers: jumlah worker paralel (default 15, max disarankan 15)
    event_context: JSON string dengan event_name, event_location, event_date (opsional)
    max_per_worker: batas vendor per URL (0 = unlimited)

    Return: {total_vendors, total_pages, completed, failed, timeout, elapsed, worker_results}
    """
    if not urls:
        return {
            "total_vendors": 0, "total_pages": 0,
            "completed": 0, "failed": 0, "timeout": 0, "elapsed": 0.0,
            "message": "Tidak ada URL yang diberikan.",
        }

    ctx: dict = {}
    if isinstance(event_context, str) and event_context.strip():
        try:
            ctx = json.loads(event_context)
        except Exception:
            pass
    elif isinstance(event_context, dict):
        ctx = event_context

    session_id = get_or_create_session_id()
    sem = asyncio.Semaphore(min(max_workers, 15))
    start_time = time.time()
    start_count = get_count()

    logger.debug(
        f"[POOL] Starting {len(urls)} URL workers "
        f"(max_workers={min(max_workers, 15)}, session={session_id})"
    )

    from backend.utils.display import console
    console.print(
        f"\n  [bold cyan]URL Worker Pool[/bold cyan] — "
        f"{len(urls)} URLs, {min(max_workers, 15)} parallel workers\n",
        highlight=False,
    )

    async def _run_one(url: str, worker_id: int) -> dict:
        domain = _domain_of(url)
        try:
            async with sem:
                worker = _URLWorkerAgent(url, ctx, session_id, worker_id,
                                         max_per_worker=max_per_worker)
                try:
                    return await asyncio.wait_for(
                        worker.run(), timeout=_WORKER_TIMEOUT_SECONDS
                    )
                except asyncio.TimeoutError:
                    return {
                        "url": url,
                        "domain": domain,
                        "status": "timeout",
                        "vendors_found": 0,
                        "pages_crawled": 0,
                        "elapsed": _WORKER_TIMEOUT_SECONDS,
                    }
        except Exception as exc:
            import traceback as _tb
            logger.warning(
                f"[W{worker_id:02d}] {domain} — FATAL init/run error: {exc}\n"
                + _tb.format_exc()
            )
            return {
                "url": url,
                "domain": domain,
                "status": "error",
                "vendors_found": 0,
                "pages_crawled": 0,
                "elapsed": 0.0,
                "error": str(exc),
            }

    tasks = [_run_one(url, i) for i, url in enumerate(urls)]
    raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    # Summarise results
    worker_results: list[dict] = []
    completed = failed = timeout_count = total_pages = 0

    for i, res in enumerate(raw_results):
        if isinstance(res, Exception):
            failed += 1
            err_url = urls[i] if i < len(urls) else "?"
            logger.warning(f"[POOL] Worker {i} ({_domain_of(err_url)}) unhandled exception: {res}")
            worker_results.append({
                "url": err_url,
                "domain": _domain_of(err_url),
                "status": "error",
                "vendors_found": 0,
                "elapsed": 0.0,
                "error": str(res),
            })
        else:
            r = res
            worker_results.append(r)
            s = r.get("status", "error")
            if s in ("done", "limit_reached"):
                completed += 1
            elif s == "timeout":
                timeout_count += 1
            else:
                failed += 1
            total_pages += r.get("pages_crawled", 0)

    # Sum of per-worker accurate counts (each worker already deduplicated against its own snapshot)
    total_vendors = sum(
        r.get("vendors_found", 0)
        for r in worker_results
        if isinstance(r, dict) and r.get("status") not in ("error",)
    )
    elapsed = round(time.time() - start_time, 1)

    logger.debug(
        f"[POOL] Done — {total_vendors} new vendors (accurate per-worker sum), "
        f"{completed} completed, {failed} failed, {timeout_count} timeout, "
        f"{elapsed}s"
    )

    console.print(
        f"\n  [bold green]Worker Pool Selesai![/bold green] "
        f"{total_vendors} vendor baru | {completed}/{len(urls)} workers OK | {elapsed}s\n",
        highlight=False,
    )

    return {
        "total_vendors": total_vendors,
        "total_pages": total_pages,
        "completed": completed,
        "failed": failed,
        "timeout": timeout_count,
        "elapsed": elapsed,
        "worker_results": worker_results,
    }
