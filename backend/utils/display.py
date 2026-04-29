import time
from datetime import datetime
from typing import Optional
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TaskProgressColumn,
    MofNCompleteColumn,
)
from rich.live import Live
from rich.status import Status
from rich.layout import Layout
from rich.text import Text
from rich.columns import Columns
from rich.rule import Rule
from rich.align import Align
from rich import box
from rich.style import Style
from rich.markup import escape


THEME = {
    "primary": "bold cyan",
    "secondary": "dim cyan",
    "success": "bold green",
    "warning": "bold yellow",
    "error": "bold red",
    "info": "white",
    "dim": "dim white",
    "accent": "bold magenta",
    "schema": "green",
    "rule_based": "yellow",
    "llm": "red",
    "header_bg": "on blue",
    "vendor_name": "bold white",
    "vendor_country": "cyan",
    "vendor_category": "magenta",
}

console = Console(highlight=False)


def _safe(text: str, max_len: int = 60) -> str:
    if not text:
        return ""
    text = str(text).strip()
    if len(text) > max_len:
        text = text[: max_len - 3] + "..."
    return escape(text)


def print_banner(query: str) -> None:
    title = Text()
    title.append("  MEGA CRAWLER BOT  ", style="bold white on blue")

    subtitle = Text()
    subtitle.append("  LangGraph + OpenAI + Zero-LLM Extraction  ", style="dim white on blue")

    query_line = Text()
    query_line.append("Query: ", style="bold cyan")
    query_line.append(query, style="bold white")

    ts_line = Text()
    ts_line.append("Started: ", style="dim")
    ts_line.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), style="dim white")

    content = Align.center(
        "\n".join([
            str(title),
            str(subtitle),
            "",
            str(query_line),
            str(ts_line),
        ])
    )

    console.print()
    console.print(Panel(content, border_style="blue", padding=(1, 4), expand=False))
    console.print()


def print_section(title: str, style: str = "bold cyan") -> None:
    console.print(Rule(f"[{style}]{title}[/{style}]", style="dim blue"))


def print_discover_start(query: str) -> None:
    console.print(f"  [{THEME['primary']}][DISCOVER][/{THEME['primary']}]  Searching for events: [bold white]{_safe(query)}[/bold white]")


def print_discover_result(seed_count: int, elapsed: float, sources: list[str]) -> None:
    console.print(
        f"  [{THEME['success']}][DISCOVER][/{THEME['success']}]  "
        f"Found [bold]{seed_count}[/bold] seed URLs in [dim]{elapsed:.1f}s[/dim]"
    )
    if sources:
        for i, src in enumerate(sources[:5], 1):
            console.print(f"             [dim]{i}.[/dim] {_safe(src, 80)}")
        if len(sources) > 5:
            console.print(f"             [dim]... and {len(sources) - 5} more[/dim]")
    console.print()


def print_crawl_batch_start(batch_num: int, queue_size: int) -> None:
    console.print(
        f"  [{THEME['primary']}][CRAWL]  [/{THEME['primary']}]  "
        f"Batch [bold]{batch_num}[/bold] — [bold]{queue_size}[/bold] URLs queued"
    )


def print_crawl_progress(crawled: int, total: int, errors: int, rps: float, vendor_pages: int) -> None:
    pct = (crawled / total * 100) if total > 0 else 0
    bar_len = 20
    filled = int(bar_len * pct / 100)
    bar = "█" * filled + "░" * (bar_len - filled)

    console.print(
        f"             [{THEME['secondary']}]{bar}[/{THEME['secondary']}] "
        f"[bold]{pct:.0f}%[/bold]  "
        f"{crawled}/{total}  |  "
        f"[{THEME['error']}]{errors} errors[/{THEME['error']}]  |  "
        f"[dim]{rps:.1f} req/s[/dim]  |  "
        f"[{THEME['success']}]{vendor_pages} vendor pages[/{THEME['success']}]"
    )


def print_extract_stats(schema_count: int, rule_count: int, llm_count: int, total: int) -> None:
    if total == 0:
        return

    schema_pct = schema_count / total * 100
    rule_pct = rule_count / total * 100
    llm_pct = llm_count / total * 100

    def bar(pct: float, width: int = 15) -> str:
        filled = int(width * pct / 100)
        return "█" * filled + "░" * (width - filled)

    console.print(f"\n  [{THEME['primary']}][EXTRACT][/{THEME['primary']}]  Extraction methods breakdown:")
    console.print(
        f"    [{THEME['schema']}]schema.org [/{THEME['schema']}]  "
        f"[{THEME['schema']}]{bar(schema_pct)}[/{THEME['schema']}]  "
        f"[bold]{schema_pct:.0f}%[/bold]  ({schema_count} vendors)  "
        f"[dim]zero LLM[/dim]"
    )
    console.print(
        f"    [{THEME['rule_based']}]rule_based [/{THEME['rule_based']}]  "
        f"[{THEME['rule_based']}]{bar(rule_pct)}[/{THEME['rule_based']}]  "
        f"[bold]{rule_pct:.0f}%[/bold]  ({rule_count} vendors)  "
        f"[dim]zero LLM[/dim]"
    )
    console.print(
        f"    [{THEME['llm']}]llm_fallbk [/{THEME['llm']}]  "
        f"[{THEME['llm']}]{bar(llm_pct)}[/{THEME['llm']}]  "
        f"[bold]{llm_pct:.0f}%[/bold]  ({llm_count} vendors)  "
        f"[{THEME['llm']}]GPT calls[/{THEME['llm']}]"
    )
    console.print()


def print_vendor_preview_table(vendors: list[dict], max_rows: int = 8) -> None:
    if not vendors:
        return

    table = Table(
        box=box.ROUNDED,
        border_style="dim blue",
        header_style="bold white on blue",
        show_lines=False,
        padding=(0, 1),
        expand=False,
    )

    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Company Name", style="bold white", min_width=25, max_width=35)
    table.add_column("Country", style="cyan", width=12)
    table.add_column("Category", style="magenta", min_width=15, max_width=22)
    table.add_column("Website", style="dim blue", min_width=20, max_width=30)
    table.add_column("Method", style="dim", width=10)

    for i, v in enumerate(vendors[:max_rows], 1):
        method = v.get("extraction_method", "unknown")
        method_style = {
            "schema_org": "green",
            "rule_based": "yellow",
            "llm": "red",
        }.get(method, "dim")

        table.add_row(
            str(i),
            _safe(v.get("name", ""), 33),
            _safe(v.get("country", ""), 12),
            _safe(v.get("category", ""), 20),
            _safe(v.get("website", ""), 28),
            f"[{method_style}]{method}[/{method_style}]",
        )

    if len(vendors) > max_rows:
        table.add_row(
            "...",
            f"[dim]... and {len(vendors) - max_rows} more vendors[/dim]",
            "", "", "", "",
        )

    console.print(f"\n  [{THEME['primary']}][VENDORS][/{THEME['primary']}]  Latest extracted:")
    console.print(table)
    console.print()


def print_supervisor_decision(action: str, reason: str, errors_seen: int) -> None:
    color_map = {
        "continue": "green",
        "retry": "yellow",
        "skip_domain": "yellow",
        "adjust_depth": "magenta",
        "abort": "red",
    }
    color = color_map.get(action, "white")
    console.print(
        f"  [bold magenta][SUPERVISOR][/bold magenta]  "
        f"[{color}]{action.upper()}[/{color}]  —  [dim]{_safe(reason, 80)}[/dim]  "
        f"[dim]({errors_seen} errors seen)[/dim]"
    )


def print_enrich_progress(current: int, total: int, domain: str) -> None:
    console.print(
        f"  [{THEME['secondary']}][ENRICH] [/{THEME['secondary']}]  "
        f"[dim]{current}/{total}[/dim]  {_safe(domain, 50)}"
    )


def print_export_result(path: str, vendor_count: int, countries: int, events: int) -> None:
    console.print()
    console.print(
        Panel(
            f"[bold green]Export complete![/bold green]\n\n"
            f"  [bold white]{_safe(path, 100)}[/bold white]\n\n"
            f"  [bold]{vendor_count}[/bold] vendors  |  "
            f"[bold]{countries}[/bold] countries  |  "
            f"[bold]{events}[/bold] events",
            title="[bold green] OUTPUT [/bold green]",
            border_style="green",
            padding=(1, 4),
            expand=False,
        )
    )
    console.print()


def print_final_summary(stats: dict) -> None:
    table = Table(
        box=box.SIMPLE_HEAVY,
        border_style="cyan",
        show_header=False,
        padding=(0, 2),
        expand=False,
    )
    table.add_column("Metric", style="dim", min_width=28)
    table.add_column("Value", style="bold white", justify="right", min_width=15)

    table.add_row("Pages crawled", str(stats.get("total_crawled", 0)))
    table.add_row("Vendor pages found", str(stats.get("total_vendor_pages", 0)))
    table.add_row("Vendors extracted", f"[bold green]{stats.get('total_vendors_extracted', 0)}[/bold green]")
    table.add_row("— via schema.org", f"[green]{stats.get('extraction_schema_org', 0)}[/green]")
    table.add_row("— via rule_based", f"[yellow]{stats.get('extraction_rule_based', 0)}[/yellow]")
    table.add_row("— via LLM fallback", f"[red]{stats.get('extraction_llm', 0)}[/red]")
    table.add_row("Extraction failed", str(stats.get("extraction_failed", 0)))
    table.add_row("LLM usage %", f"{stats.get('llm_percentage', 0):.1f}%")
    table.add_row("Est. LLM cost (USD)", f"${stats.get('estimated_llm_cost_usd', 0):.4f}")
    table.add_row("LLM tokens used", str(stats.get("llm_tokens_total", 0)))
    table.add_row("Countries found", str(stats.get("countries_found", 0)))
    table.add_row("Events found", str(stats.get("events_found", 0)))
    table.add_row("Domains crawled", str(stats.get("domains_crawled", 0)))
    table.add_row("Total errors", str(stats.get("total_errors", 0)))
    table.add_row("Success rate", f"{stats.get('success_rate', 0):.1f}%")
    table.add_row("Elapsed time", f"{stats.get('elapsed_seconds', 0):.1f}s")
    table.add_row("Avg req/s", f"{stats.get('requests_per_second', 0):.1f}")

    console.print()
    console.print(
        Panel(
            Align.center(table),
            title="[bold cyan] FINAL SUMMARY [/bold cyan]",
            border_style="cyan",
            padding=(1, 2),
        )
    )
    console.print()


def print_error(context: str, message: str, url: str = "") -> None:
    url_part = f"  [dim]{_safe(url, 60)}[/dim]" if url else ""
    console.print(
        f"  [{THEME['error']}][ERROR]  [{THEME['error']}]  "
        f"[dim]{_safe(context, 20)}[/dim]  {_safe(message, 80)}"
        f"{url_part}",
        highlight=False,
    )


def print_warning(context: str, message: str) -> None:
    console.print(
        f"  [{THEME['warning']}][WARN]   [{THEME['warning']}]  "
        f"[dim]{_safe(context, 20)}[/dim]  {_safe(message, 80)}",
        highlight=False,
    )


def print_info(context: str, message: str) -> None:
    console.print(
        f"  [{THEME['info']}][INFO]   [{THEME['info']}]  "
        f"[dim]{_safe(context, 20)}[/dim]  {_safe(message, 80)}",
        highlight=False,
    )


def print_thinking(agent: str, thought: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    console.print(
        f"  [dim]{ts}[/dim]  [dim italic]berpikir[/dim italic]  "
        f"[dim]{_safe(agent, 18):<18}[/dim]  [dim italic]{_safe(thought, 100)}[/dim italic]",
        highlight=False,
    )


_status_instance: Optional[Status] = None


def start_thinking() -> None:
    global _status_instance
    _status_instance = Status(
        "  [dim italic]Berpikir...[/dim italic]",
        console=console,
        spinner="dots",
    )
    _status_instance.start()


def update_thinking(text: str) -> None:
    if _status_instance:
        _status_instance.update(f"  [dim italic]{escape(text[:100])}[/dim italic]")


def stop_thinking(final_text: str = "") -> None:
    global _status_instance
    if _status_instance:
        _status_instance.stop()
        _status_instance = None
    if final_text and final_text.strip():
        console.print(
            f"\n  [white]{escape(final_text.strip()[:300])}[/white]\n",
            highlight=False,
        )


def print_tool_start(tool_name: str, args: dict) -> None:
    arg_preview = "  ".join(
        f"[dim]{k}[/dim]=[dim white]{escape(str(v)[:60])}[/dim white]"
        for k, v in list(args.items())[:3]
    )
    console.print(
        f"  [bold cyan][TOOL]  [/bold cyan]"
        f"[bold white]{tool_name}[/bold white]  {arg_preview}",
        highlight=False,
    )


def print_tool_end(tool_name: str, result_summary: str) -> None:
    console.print(
        f"  [dim green][TOOL]  [/dim green]"
        f"[dim]{tool_name}[/dim]  "
        f"[dim green]selesai: {escape(result_summary[:100])}[/dim green]",
        highlight=False,
    )


def print_llm_call(purpose: str, url: str, input_tokens: int, output_tokens: int) -> None:
    console.print(
        f"  [{THEME['llm']}][LLM]    [{THEME['llm']}]  "
        f"[dim]{_safe(purpose, 15)}[/dim]  {_safe(url, 50)}  "
        f"[dim]in={input_tokens} out={output_tokens} tokens[/dim]",
        highlight=False,
    )


def make_crawl_progress_bar() -> Progress:
    return Progress(
        SpinnerColumn(style="cyan"),
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(bar_width=25, style="cyan", complete_style="bold cyan"),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TextColumn("[dim]{task.fields[rps]:.1f} req/s[/dim]"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=False,
    )


def make_simple_progress_bar(description: str = "") -> Progress:
    return Progress(
        SpinnerColumn(style="cyan"),
        TextColumn(f"[cyan]{description}"),
        BarColumn(bar_width=20, style="dim cyan"),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
        transient=True,
    )


class LiveCrawlDisplay:
    def __init__(self) -> None:
        self._start_time: float = time.time()
        self._progress = make_crawl_progress_bar()
        self._task_id = None

    def start(self, total: int, description: str = "Crawling") -> None:
        self._progress.start()
        self._task_id = self._progress.add_task(description, total=total, rps=0.0)

    def update(self, advance: int = 1, rps: float = 0.0) -> None:
        if self._task_id is not None:
            self._progress.update(self._task_id, advance=advance, rps=rps)

    def stop(self) -> None:
        self._progress.stop()

    def elapsed(self) -> float:
        return time.time() - self._start_time


def print_node_transition(from_node: str, to_node: str) -> None:
    console.print(
        f"  [dim blue][GRAPH]   [/dim blue]  "
        f"[dim]{from_node}[/dim] [bold blue]→[/bold blue] [bold cyan]{to_node}[/bold cyan]",
        highlight=False,
    )


def print_queue_status(pending: int, processing: int, done: int, failed: int) -> None:
    console.print(
        f"  [dim][QUEUE]   [/dim]  "
        f"pending=[bold]{pending}[/bold]  "
        f"processing=[yellow]{processing}[/yellow]  "
        f"done=[green]{done}[/green]  "
        f"failed=[red]{failed}[/red]",
        highlight=False,
    )


def print_batch_complete(batch_num: int, crawled: int, vendor_pages: int, new_links: int) -> None:
    console.print(
        f"\n  [{THEME['success']}][BATCH]  [{THEME['success']}]  "
        f"Batch [bold]{batch_num}[/bold] complete  —  "
        f"crawled=[bold]{crawled}[/bold]  "
        f"vendor_pages=[bold green]{vendor_pages}[/bold green]  "
        f"new_links=[bold cyan]{new_links}[/bold cyan]\n",
        highlight=False,
    )


def print_separator() -> None:
    console.print(Rule(style="dim blue"))
