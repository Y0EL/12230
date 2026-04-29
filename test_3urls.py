#!/usr/bin/env python3
"""
Test focused: 3 regional expo URLs, max 50 vendors each.
Bypass orchestrator search phase → langsung crawl → enrich → export.

Expos:
  [CN] Security China 2026   — securitychina.com.cn (CCPIT, paling gede di Asia)
  [RU] Interpolitex Moscow   — interpolitex.ru     (polisi & keamanan terbesar Russia)
  [GL] Gartner Security      — gartner.com          (global security conference)
"""
import warnings
warnings.filterwarnings("ignore")

import sys
import asyncio
import json
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.core.config import get_settings
from backend.utils.display import (
    console, print_banner, print_separator, print_info,
    print_tool_start, print_tool_end,
    start_thinking, update_thinking, stop_thinking,
    print_vendor_preview_table, print_export_result,
    THEME,
)
from rich.rule import Rule

# get_settings() calls setup_logging() internally which adds an INFO-level
# stderr handler. Re-apply WARNING-only AFTER get_settings() so all the
# logger.info() calls inside workers never reach the console.
_settings = get_settings()

from loguru import logger
logger.remove()  # wipe whatever setup_logging() installed
# Console: WARNING+ only — keeps Rich spinner clean (no INFO noise)
logger.add(sys.stderr, level="WARNING", colorize=True,
           format="  <yellow>[WARN]</yellow>  {message}")
# File: full DEBUG/INFO for post-run debugging
logger.add("output/test.log", level="DEBUG", rotation="50 MB", encoding="utf-8",
           format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{line} — {message}")

TARGET_URLS = [
    "https://www.chgie.com/aad.htm",                                                         # China / CCPIT
    "https://interpolitex.ru/en/participants/",                                              # Russia / Interpolitex
    "https://www.gartner.com/en/conferences/na/security-risk-management-us/exhibitors",     # Global / Gartner Security
]

EVENT_CONTEXT = {
    "event_name": "Security China + Interpolitex + Gartner Security 2025-2026",
    "event_location": "China / Russia / USA",
    "event_date": "2025-2026",
}

MAX_PER_WORKER = 50   # vendor limit per URL
MAX_WORKERS    = 3    # 1 worker per URL (paralel)
ENRICH_CONCUR  = 5    # hati-hati TPM limit saat test


async def _spinner_updater(get_count_fn, start_count: int):
    """Background task — updates the 'Berpikir...' spinner with live vendor count."""
    try:
        while True:
            await asyncio.sleep(8)
            current = get_count_fn() - start_count
            update_thinking(f"Workers aktif — {current} vendor terkumpul sejauh ini...")
    except asyncio.CancelledError:
        pass


async def run_test():
    from backend.tools.vendor_registry import clear_registry, get_all_vendors, get_count
    from backend.tools.temp_store import reset_session
    from backend.tools.url_worker import crawl_urls_parallel
    from backend.tools.enrich_tools import enrich_vendors_parallel
    from backend.tools.export_tools import (
        export_to_excel, export_to_csv, export_to_json, deduplicate_vendors,
    )

    # ── Clean state ───────────────────────────────────────────────────────────
    clear_registry()
    reset_session()
    t0 = time.time()

    # ════════════════════════════════════════════════════════════════
    # STEP 1 — CRAWL
    # ════════════════════════════════════════════════════════════════
    console.print(Rule("[bold cyan]STEP 1 — DEEP CRAWL[/bold cyan]", style="dim blue"))
    print_tool_start("crawl_urls_parallel", {
        "urls": TARGET_URLS,
        "max_workers": MAX_WORKERS,
        "max_per_worker": MAX_PER_WORKER,
    })

    start_count = get_count()
    start_thinking()
    spinner_task = asyncio.create_task(_spinner_updater(get_count, start_count))

    try:
        crawl_result = await crawl_urls_parallel.ainvoke({
            "urls": TARGET_URLS,
            "max_workers": MAX_WORKERS,
            "max_per_worker": MAX_PER_WORKER,
            "event_context": json.dumps(EVENT_CONTEXT),
        })
    finally:
        spinner_task.cancel()
        try:
            await spinner_task
        except asyncio.CancelledError:
            pass

    total_vendors = crawl_result.get("total_vendors", 0)
    completed     = crawl_result.get("completed", 0)
    elapsed_crawl = crawl_result.get("elapsed", 0)

    stop_thinking(
        f"Worker pool selesai! {total_vendors} vendor baru dari "
        f"{completed}/{len(TARGET_URLS)} workers dalam {elapsed_crawl}s."
    )
    print_tool_end("crawl_urls_parallel",
                   f"{total_vendors} vendors | {completed}/{len(TARGET_URLS)} OK | {elapsed_crawl}s")

    # Per-worker breakdown
    console.print()
    for wr in crawl_result.get("worker_results", []):
        st = wr.get("status", "?")
        color = "green" if st in ("done", "limit_reached") else "yellow" if st == "timeout" else "red"
        icon  = "✓" if st in ("done", "limit_reached") else "⏱" if st == "timeout" else "✗"
        console.print(
            f"    [{color}]{icon} [{color}][bold]{wr.get('domain','?'):<32}[/bold]"
            f"[dim]{wr.get('vendors_found', 0):>4} vendors[/dim]  "
            f"[{color}]{st}[/{color}]  "
            f"[dim]{wr.get('elapsed', 0):.0f}s[/dim]",
            highlight=False,
        )
    console.print()

    total_after_crawl = get_count()
    console.print(
        f"  [bold cyan]Registry total:[/bold cyan] "
        f"[bold white]{total_after_crawl}[/bold white] vendors\n"
    )

    if total_after_crawl == 0:
        console.print("[bold red]  No vendors found. Check URLs / network.[/bold red]")
        return

    # Vendor preview table
    print_vendor_preview_table(get_all_vendors(), max_rows=10)

    # ════════════════════════════════════════════════════════════════
    # STEP 2 — ENRICH
    # ════════════════════════════════════════════════════════════════
    console.print(Rule("[bold cyan]STEP 2 — ENRICHMENT[/bold cyan]", style="dim blue"))
    print_tool_start("enrich_vendors_parallel", {
        "max_concurrent": ENRICH_CONCUR,
        "max_vendors": total_after_crawl,
    })
    start_thinking()
    update_thinking(f"Enriching {total_after_crawl} vendors via Firecrawl + web search...")

    enrich_result = enrich_vendors_parallel.invoke({
        "max_concurrent": ENRICH_CONCUR,
        "max_vendors": total_after_crawl,
    })

    stop_thinking(
        f"Enrichment selesai: {enrich_result.get('enriched', 0)} diperkaya, "
        f"{enrich_result.get('skipped', 0)} dilewati."
    )
    print_tool_end("enrich_vendors_parallel",
                   f"enriched={enrich_result.get('enriched',0)}  "
                   f"skipped={enrich_result.get('skipped',0)}  "
                   f"failed={enrich_result.get('failed',0)}  "
                   f"in {enrich_result.get('elapsed_seconds',0)}s")
    console.print()

    # STEP 3 — DEDUP di-skip untuk keperluan demo/testing
    # (dedup aktif di run.py production flow; di sini kita mau lihat semua vendor raw)

    # Preview final vendor list before export
    print_vendor_preview_table(get_all_vendors(), max_rows=15)

    # ════════════════════════════════════════════════════════════════
    # STEP 3 — EXPORT
    # ════════════════════════════════════════════════════════════════
    console.print(Rule("[bold cyan]STEP 4 — EXPORT[/bold cyan]", style="dim blue"))
    query_label = "Security China + Interpolitex + Gartner Security test"
    title_label = "3-Region Security Expo Test"

    print_tool_start("export_to_excel", {"query": query_label, "title": title_label})
    excel_path = export_to_excel.invoke({"query": query_label, "title": title_label})
    print_tool_end("export_to_excel", excel_path or "failed")

    print_tool_start("export_to_csv", {"query": query_label, "title": title_label})
    csv_path = export_to_csv.invoke({"query": query_label, "title": title_label})
    print_tool_end("export_to_csv", csv_path or "failed")

    print_tool_start("export_to_json", {"query": query_label, "title": title_label})
    json_path = export_to_json.invoke({"query": query_label, "title": title_label})
    print_tool_end("export_to_json", json_path or "failed")
    console.print()

    # ════════════════════════════════════════════════════════════════
    # SUMMARY
    # ════════════════════════════════════════════════════════════════
    total_elapsed = round(time.time() - t0, 1)
    final_count   = get_count()

    print_separator()
    console.print(
        f"\n  [bold green]✓ DONE![/bold green]  "
        f"[bold white]{final_count}[/bold white] vendors  ·  "
        f"[dim]{total_elapsed}s total[/dim]\n"
    )
    if excel_path:
        console.print(f"  [bold cyan]Excel :[/bold cyan] {excel_path}")
    if csv_path:
        console.print(f"  [bold cyan]CSV   :[/bold cyan] {csv_path}")
    if json_path:
        console.print(f"  [bold cyan]JSON  :[/bold cyan] {json_path}")
    console.print()


def main():
    print_banner("3-URL Regional Test: China + Russia + Global (max 50 each)")
    print_separator()
    print_info("TEST", f"model={_settings.openai_model}")
    print_info("TEST", f"URLs = {len(TARGET_URLS)}  |  max_per_worker = {MAX_PER_WORKER}")
    for i, url in enumerate(TARGET_URLS, 1):
        print_info(f"URL {i}", url)
    print_separator()

    try:
        asyncio.run(run_test())
    except KeyboardInterrupt:
        console.print("\n[yellow]  Interrupted. Check output/ for partial results.[/yellow]\n")
    finally:
        try:
            from backend.tools.fetch_tools import close_playwright
            loop = asyncio.new_event_loop()
            loop.run_until_complete(close_playwright())
            loop.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
