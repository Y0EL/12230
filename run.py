#!/usr/bin/env python3
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=ResourceWarning)
warnings.filterwarnings("ignore", message=".*unclosed.*pipe.*")
warnings.filterwarnings("ignore", message=".*I/O operation on closed pipe.*")
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

from loguru import logger
from backend.core.config import get_settings
from backend.utils.display import (
    print_banner, print_separator, console,
    print_info, print_error, print_warning,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="run.py",
        description="Mega Crawler Bot — LangGraph ReAct Agent + OpenAI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run.py "cyber defense exhibition global 2025"
  python run.py "cybersecurity expo europe" --max 50
  python run.py "defense technology summit" --no-enrich --verbose
        """,
    )
    parser.add_argument(
        "query",
        type=str,
        help="Search query describing the type of events/vendors to find",
    )
    parser.add_argument(
        "--no-enrich",
        action="store_true",
        help="Skip the domain enrichment phase (faster, less data)",
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="[DEPRECATED] ReAct agent requires LLM. This flag will cause an error.",
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


def validate_environment(settings, args) -> bool:
    ok = True

    if not settings.has_openai_key:
        print_error(
            "ENV",
            "OPENAI_API_KEY tidak ditemukan. ReAct agent membutuhkan OpenAI key. "
            "Salin .env.example ke .env lalu isi OPENAI_API_KEY."
        )
        ok = False

    if args.no_llm:
        print_error(
            "ENV",
            "--no-llm tidak kompatibel dengan ReAct agent. "
            "Agent ini menggunakan LLM untuk semua keputusan."
        )
        ok = False

    output_path = settings.output_path
    if not output_path.exists():
        try:
            output_path.mkdir(parents=True, exist_ok=True)
            print_info("ENV", f"Created output directory: {output_path}")
        except Exception as e:
            print_error("ENV", f"Cannot create output directory: {e}")
            ok = False

    # ── Search engine availability ────────────────────────────────────────────
    openserp_ok = False
    try:
        import httpx
        # Ping root endpoint saja — tidak nge-Google beneran (cepat, tidak timeout)
        for _path in ("/", "/health", "/google/search"):
            try:
                _params = {"text": "ping", "limit": 1} if "search" in _path else {}
                resp = httpx.get(f"{settings.openserp_base_url}{_path}",
                                 params=_params, timeout=3)
                openserp_ok = True
                break
            except Exception:
                continue
    except Exception:
        pass

    tavily_ok = bool(settings.tavily_api_key)

    if tavily_ok:
        print_info("ENV", f"Search engine: Tavily ✅ (primary)" + (f"  |  OpenSERP ✅ (backup)" if openserp_ok else ""))
    elif openserp_ok:
        print_info("ENV", f"Search engine: OpenSERP ✅  {settings.openserp_base_url}  (set TAVILY_API_KEY untuk hasil lebih baik)")
    else:
        print_warning(
            "ENV",
            "Tidak ada search engine aktif. Crawler akan pakai DuckDuckGo (lambat).\n"
            "  Untuk hasil maksimal jalankan OpenSERP: start_openserp.bat\n"
            "  Download binary: https://github.com/karust/openserp/releases"
        )

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print_warning("ENV", "playwright not installed. JS rendering disabled. pip install playwright && playwright install chromium")

    return ok


def apply_overrides(settings, args: argparse.Namespace) -> None:
    if args.max is not None:
        settings.max_total_vendors = args.max
        settings.max_vendors_per_event = args.max
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

    if not validate_environment(settings, args):
        console.print("\n[bold red]Environment validation failed. Fix the issues above and retry.[/bold red]\n")
        return 1

    print_info("CONFIG", f"model={settings.openai_model}  max_vendors={settings.max_total_vendors}")
    print_info("CONFIG", f"enrich={'off' if args.no_enrich else 'on'}")
    pdf_parser = "Firecrawl ✅" if settings.has_firecrawl_key else ("Jina ✅ (free)" if True else "")
    print_info("CONFIG", f"tavily={'✅' if settings.tavily_api_key else '❌'}  pdf={pdf_parser}  jina={'✅ (key)' if settings.has_jina_key else '✅ (free)'}  llm={'✅' if settings.effective_llm_enabled else '❌'}")
    print_info("CONFIG", f"output={settings.output_path}")
    print_separator()

    start_time = time.time()

    try:
        from backend.graph.workflow import run_crawler
        result = run_crawler(
            args.query,
            max_vendors=settings.max_total_vendors,
            skip_enrich=args.no_enrich,
        )

    except KeyboardInterrupt:
        console.print("\n\n[bold yellow]Interrupted by user. Partial results may be in output/[/bold yellow]\n")
        return 130
    except Exception as e:
        logger.exception("Crawler failed")
        print_error("MAIN", f"Crawler failed: {e}")
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
    csv_path   = result.get("output_csv", "")
    json_path  = result.get("output_json", "")

    print_separator()

    if vendor_count == 0:
        console.print("\n[bold yellow]No vendors found. Try a different query or check your network.[/bold yellow]\n")
        return 1

    console.print(f"\n[bold green]Done![/bold green]  {vendor_count} vendors in {elapsed:.1f}s\n")
    if excel_path:
        console.print(f"  Excel : [bold cyan]{excel_path}[/bold cyan]")
    if csv_path:
        console.print(f"  CSV   : [bold cyan]{csv_path}[/bold cyan]")
    if json_path:
        console.print(f"  JSON  : [bold cyan]{json_path}[/bold cyan]")
    console.print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
