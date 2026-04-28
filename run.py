#!/usr/bin/env python3
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=ResourceWarning)  # unclosed transport/pipe (Windows asyncio cleanup)
warnings.filterwarnings("ignore", message=".*urllib3.*")
warnings.filterwarnings("ignore", message=".*chardet.*")
warnings.filterwarnings("ignore", message=".*charset_normalizer.*")
warnings.filterwarnings("ignore", message=".*RequestsDependencyWarning.*")
warnings.filterwarnings("ignore", message=".*google.generativeai.*")

import sys
import time
import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.core.config import get_settings
from backend.utils.display import (
    print_banner, print_separator, console,
    print_info, print_error, print_warning,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="run.py",
        description="Mega Crawler Bot — LangGraph + OpenAI vendor listing engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run.py "cyber defense exhibition global 2025"
  python run.py "cybersecurity expo europe" --depth 2 --batch 200
  python run.py "defense technology summit" --no-enrich --verbose
        """,
    )
    parser.add_argument(
        "query",
        type=str,
        help="Search query describing the type of events/vendors to find",
    )
    parser.add_argument(
        "--depth", "-d",
        type=int,
        default=None,
        help="Maximum crawl depth (default: from .env MAX_DEPTH)",
    )
    parser.add_argument(
        "--batch", "-b",
        type=int,
        default=None,
        help="Batch size for crawling (default: from .env BATCH_SIZE)",
    )
    parser.add_argument(
        "--no-enrich",
        action="store_true",
        help="Skip the domain enrichment phase (faster, less data)",
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="Disable LLM fallback entirely (pure zero-LLM mode)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output including DEBUG logs",
    )
    parser.add_argument(
        "--max", "-m",
        type=int,
        default=None,
        help="Batas maksimum vendor yang dikumpulkan, misal --max 100 untuk testing cepat",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=None,
        help="Output directory override (default: ./output)",
    )
    return parser.parse_args()


def validate_environment(settings) -> bool:
    ok = True

    if not settings.has_openai_key:
        print_warning(
            "ENV",
            "OPENAI_API_KEY not set — LLM fallback disabled. "
            "Copy .env.example to .env and add your key."
        )

    output_path = settings.output_path
    if not output_path.exists():
        try:
            output_path.mkdir(parents=True, exist_ok=True)
            print_info("ENV", f"Created output directory: {output_path}")
        except Exception as e:
            print_error("ENV", f"Cannot create output directory: {e}")
            ok = False

    try:
        from duckduckgo_search import DDGS
    except ImportError:
        print_error("ENV", "duckduckgo-search not installed. Run: pip install duckduckgo-search")
        ok = False

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print_warning("ENV", "playwright not installed — JS rendering disabled. Run: pip install playwright && playwright install chromium")

    return ok


def apply_overrides(settings, args: argparse.Namespace) -> None:
    if args.depth is not None:
        settings.max_depth = args.depth
    if args.batch is not None:
        settings.batch_size = args.batch
    if args.max is not None:
        settings.max_total_vendors = args.max
        settings.max_vendors_per_event = args.max
    if args.no_llm:
        settings.llm_fallback_enabled = False
        settings.llm_supervisor_enabled = False
    if args.output:
        settings.output_dir = args.output
    if args.verbose:
        settings.log_level = "DEBUG"


def main() -> int:
    args = parse_args()
    settings = get_settings()
    apply_overrides(settings, args)

    print_banner(args.query)
    print_separator()

    if not validate_environment(settings):
        console.print("\n[bold red]Environment validation failed. Fix the issues above and retry.[/bold red]\n")
        return 1

    print_info("CONFIG", f"depth={settings.max_depth}  batch={settings.batch_size}  concurrent={settings.max_concurrent_requests}")
    print_info("CONFIG", f"llm_fallback={settings.effective_llm_enabled}  model={settings.openai_model if settings.has_openai_key else 'N/A'}")
    print_info("CONFIG", f"output={settings.output_path}")
    print_separator()

    start_time = time.time()

    try:
        if args.no_enrich:
            console.print("[dim]Enrichment phase disabled (--no-enrich)[/dim]\n")
            _patch_skip_enrich()

        from backend.graph.workflow import run_crawler
        result = run_crawler(args.query)

    except KeyboardInterrupt:
        console.print("\n\n[bold yellow]Interrupted by user. Partial results may be in output/[/bold yellow]\n")
        return 130
    except Exception as e:
        print_error("MAIN", f"Crawler failed: {e}")
        import traceback
        if args.verbose:
            console.print_exception()
        return 1
    finally:
        try:
            import asyncio
            from backend.tools.fetch_tools import close_playwright
            loop = asyncio.new_event_loop()
            loop.run_until_complete(close_playwright())
            loop.close()
        except Exception:
            pass

    elapsed = time.time() - start_time
    vendor_count = len(result.get("vendors", []))
    excel_path = result.get("output_excel", "")
    csv_path = result.get("output_csv", "")

    print_separator()

    if vendor_count == 0:
        console.print("\n[bold yellow]No vendors found. Try a different query or check your network.[/bold yellow]\n")
        return 1

    console.print(f"\n[bold green]Done![/bold green]  {vendor_count} vendors in {elapsed:.1f}s\n")
    if excel_path:
        console.print(f"  Excel: [bold cyan]{excel_path}[/bold cyan]")
    if csv_path:
        console.print(f"  CSV:   [bold cyan]{csv_path}[/bold cyan]")
    console.print()

    return 0


def _patch_skip_enrich() -> None:
    import backend.graph.nodes as nodes_module

    def _skip_enrich(state) -> dict:
        raw_vendors = state.get("raw_vendors", [])
        print_info("enrich_domains", "Skipped (--no-enrich)")
        return {"vendors": raw_vendors, "phase": "export"}

    nodes_module.node_enrich_domains = _skip_enrich


if __name__ == "__main__":
    sys.exit(main())
