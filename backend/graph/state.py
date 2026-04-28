from typing import TypedDict, Optional, Annotated
import operator
from backend.core.config import CrawlStats


class CrawlerState(TypedDict):
    query: str
    seed_urls: list[str]
    seed_metadata: list[dict]
    crawl_queue: list[dict]
    visited_urls: set[str]
    vendor_pages: list[dict]
    page_metadata: dict[str, dict]
    raw_vendors: list[dict]
    vendors: list[dict]
    current_batch: int
    total_crawled: int
    total_errors: int
    stats: Optional[CrawlStats]
    supervisor_actions: list[dict]
    output_excel: str
    output_csv: str
    error_log: list[str]
    done: bool
    phase: str


def initial_state(query: str) -> CrawlerState:
    return CrawlerState(
        query=query,
        seed_urls=[],
        seed_metadata=[],
        crawl_queue=[],
        visited_urls=set(),
        vendor_pages=[],
        page_metadata={},
        raw_vendors=[],
        vendors=[],
        current_batch=0,
        total_crawled=0,
        total_errors=0,
        stats=CrawlStats(),
        supervisor_actions=[],
        output_excel="",
        output_csv="",
        error_log=[],
        done=False,
        phase="init",
    )
