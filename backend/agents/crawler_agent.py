import asyncio
import time
from collections import defaultdict
from typing import Any
from urllib.parse import urlparse

import tldextract
from loguru import logger

from backend.agents.base_agent import BaseAgent
from backend.tools.fetch_tools import fetch_pages_batch_async, ALL_FETCH_TOOLS
from backend.tools.parse_tools import (
    extract_links, classify_exhibitor_links, extract_page_metadata,
    score_page_as_event, find_exhibitor_list_pages, ALL_PARSE_TOOLS,
)
from backend.core.config import get_settings
from backend.utils.display import (
    print_crawl_batch_start, print_crawl_progress,
    print_batch_complete, print_error, print_queue_status,
)


class CrawlQueue:
    def __init__(self) -> None:
        self._queue: list[dict] = []
        self._visited: set[str] = set()
        self._processing: set[str] = set()

    def add(self, url: str, depth: int = 0, source: str = "", priority: int = 0) -> bool:
        norm = url.strip().rstrip("/")
        if norm in self._visited or norm in self._processing:
            return False
        self._visited.add(norm)
        self._queue.append({"url": norm, "depth": depth, "source": source, "priority": priority})
        return True

    def add_many(self, urls: list[dict]) -> int:
        added = 0
        for item in urls:
            url = item.get("url", "")
            if self.add(url, item.get("depth", 0), item.get("source", ""), item.get("priority", 0)):
                added += 1
        return added

    def pop_batch(self, size: int) -> list[dict]:
        self._queue.sort(key=lambda x: x.get("priority", 0), reverse=True)
        batch = self._queue[:size]
        self._queue = self._queue[size:]
        for item in batch:
            self._processing.add(item["url"])
        return batch

    def mark_done(self, url: str) -> None:
        self._processing.discard(url.strip().rstrip("/"))

    def mark_failed(self, url: str) -> None:
        self._processing.discard(url.strip().rstrip("/"))

    @property
    def pending(self) -> int:
        return len(self._queue)

    @property
    def processing(self) -> int:
        return len(self._processing)

    @property
    def visited_count(self) -> int:
        return len(self._visited)

    def is_empty(self) -> bool:
        return len(self._queue) == 0


class CrawlerAgent(BaseAgent):
    agent_name = "crawler_agent"

    def __init__(self, **kwargs) -> None:
        tools = ALL_FETCH_TOOLS + ALL_PARSE_TOOLS
        super().__init__(tools=tools, **kwargs)
        self._settings = get_settings()

    def run(self, input_data: dict) -> dict:
        seed_urls = input_data.get("seed_urls", [])
        query = input_data.get("query", "")
        max_depth = input_data.get("max_depth", self._settings.max_depth)
        batch_size = input_data.get("batch_size", self._settings.batch_size)
        existing_visited = input_data.get("visited_urls", set())

        queue = CrawlQueue()
        for url in existing_visited:
            queue._visited.add(url)

        for url in seed_urls:
            queue.add(url, depth=0, source="seed", priority=10)

        all_vendor_pages: list[dict] = []
        all_page_metadata: dict[str, dict] = {}
        total_crawled = 0
        total_errors = 0
        batch_num = 0
        start_time = time.time()

        while not queue.is_empty() and total_crawled < self._settings.batch_size * 10:
            batch = queue.pop_batch(min(batch_size, 100))
            if not batch:
                break

            batch_num += 1
            batch_urls = [item["url"] for item in batch]
            depth_map = {item["url"]: item["depth"] for item in batch}

            print_crawl_batch_start(batch_num, len(batch_urls))

            # Counter untuk live progress saat fetch berlangsung
            _done_count = [0]
            _err_count = [0]

            def _on_url_done(url: str, result: dict) -> None:
                _done_count[0] += 1
                if not result.get("success"):
                    _err_count[0] += 1
                elapsed = time.time() - start_time
                rps = (total_crawled + _done_count[0]) / elapsed if elapsed > 0 else 0
                status = result.get("status", 0)
                method = "pw" if result.get("is_js_rendered") else "http"
                print_crawl_progress(
                    total_crawled + _done_count[0],
                    total_crawled + _done_count[0] + queue.pending,
                    total_errors + _err_count[0],
                    rps,
                    len(all_vendor_pages),
                )

            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    results = loop.run_until_complete(
                        fetch_pages_batch_async(batch_urls, on_done=_on_url_done)
                    )
                finally:
                    # Tutup Playwright dulu sebelum loop ditutup
                    # supaya Chromium process ter-terminate dengan bersih
                    from backend.tools.fetch_tools import close_playwright
                    try:
                        loop.run_until_complete(close_playwright())
                    except Exception:
                        pass
                    # Cancel semua task yang masih pending lalu drain
                    pending = asyncio.all_tasks(loop)
                    for t in pending:
                        t.cancel()
                    if pending:
                        try:
                            loop.run_until_complete(
                                asyncio.gather(*pending, return_exceptions=True)
                            )
                        except Exception:
                            pass
                    loop.close()
            except Exception as e:
                logger.error(f"[CrawlerAgent] batch fetch failed: {e}")
                results = []
                total_errors += len(batch_urls)
                for url in batch_urls:
                    queue.mark_failed(url)
                continue

            new_vendor_pages = 0
            new_links_added = 0

            for page_result, item in zip(results, batch):
                url = item["url"]
                current_depth = depth_map.get(url, 0)
                queue.mark_done(url)

                if not page_result.get("success"):
                    total_errors += 1
                    queue.mark_failed(url)
                    continue

                total_crawled += 1
                html = page_result.get("html", "")
                final_url = page_result.get("final_url", url)

                try:
                    event_score = score_page_as_event.invoke({"html": html, "url": final_url})
                    is_event_page = event_score.get("is_event_page", False)
                    page_score = event_score.get("score", 0)
                except Exception:
                    is_event_page = False
                    page_score = 0

                try:
                    metadata = extract_page_metadata.invoke({"html": html, "url": final_url})
                    all_page_metadata[final_url] = metadata
                    for pag_url in metadata.get("pagination_urls", []):
                        if queue.add(pag_url, depth=current_depth, source=final_url, priority=8):
                            new_links_added += 1
                except Exception as e:
                    logger.debug(f"metadata extraction failed for {url}: {e}")

                try:
                    raw_links = extract_links.invoke({"html": html, "base_url": final_url})
                    exhibitor_links = classify_exhibitor_links.invoke({"links": raw_links, "threshold": 2})
                except Exception as e:
                    logger.debug(f"link extraction failed for {url}: {e}")
                    raw_links = []
                    exhibitor_links = []

                if is_event_page and current_depth < max_depth:
                    try:
                        list_pages = find_exhibitor_list_pages.invoke({
                            "links": raw_links, "base_url": final_url
                        })
                        for lp in list_pages[:20]:
                            lp_url = lp.get("url", "")
                            if queue.add(lp_url, depth=current_depth, source=final_url, priority=9):
                                new_links_added += 1
                    except Exception as e:
                        logger.debug(f"find_exhibitor_list_pages failed: {e}")

                for link in exhibitor_links[:200]:
                    link_url = link.get("url", "")
                    link_depth = current_depth + 1
                    link_score = link.get("score", 0)
                    link_priority = min(max(link_score, 1), 10)

                    if link_score >= 1:
                        all_vendor_pages.append({
                            "url": link_url,
                            "score": link_score,
                            "source_page": final_url,
                            "depth": link_depth,
                            "event_metadata": all_page_metadata.get(final_url, {}),
                        })
                        new_vendor_pages += 1

                    if link_depth <= max_depth and link_score >= 0:
                        if queue.add(link_url, depth=link_depth, source=final_url, priority=link_priority):
                            new_links_added += 1


            print_batch_complete(batch_num, total_crawled, new_vendor_pages, new_links_added)
            print_queue_status(queue.pending, queue.processing, total_crawled, total_errors)

            if total_errors > self._settings.llm_error_threshold * 3:
                logger.warning("[CrawlerAgent] High error rate — stopping crawl")
                break

        seen_vendor_urls: set[str] = set()
        deduped_vendor_pages = []
        for vp in sorted(all_vendor_pages, key=lambda x: x.get("score", 0), reverse=True):
            vp_url = vp.get("url", "")
            if vp_url and vp_url not in seen_vendor_urls:
                seen_vendor_urls.add(vp_url)
                deduped_vendor_pages.append(vp)

        return {
            "vendor_pages": deduped_vendor_pages[:5000],
            "page_metadata": all_page_metadata,
            "visited_urls": queue._visited,
            "total_crawled": total_crawled,
            "total_errors": total_errors,
            "batches_run": batch_num,
            "elapsed": time.time() - start_time,
        }
