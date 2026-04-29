import asyncio
import time
from typing import Any
from loguru import logger

from backend.agents.base_agent import BaseAgent
from backend.tools.fetch_tools import fetch_pages_batch_async, ALL_FETCH_TOOLS
from backend.tools.extract_tools import run_extraction_pipeline, validate_vendor, ALL_EXTRACT_TOOLS
from backend.core.config import get_settings, CrawlStats
from backend.utils.display import (
    print_extract_stats, print_vendor_preview_table, print_error
)


class ExtractorAgent(BaseAgent):
    agent_name = "extractor_agent"

    def __init__(self, **kwargs) -> None:
        tools = ALL_FETCH_TOOLS + ALL_EXTRACT_TOOLS
        super().__init__(tools=tools, **kwargs)
        self._settings = get_settings()

    def run(self, input_data: dict) -> dict:
        vendor_pages = input_data.get("vendor_pages", [])
        page_metadata = input_data.get("page_metadata", {})
        stats: CrawlStats = input_data.get("stats", CrawlStats())
        batch_size = input_data.get("batch_size", 50)

        if not vendor_pages:
            logger.warning("[ExtractorAgent] No vendor pages to extract")
            return {"vendors": [], "stats": stats}

        all_vendors: list[dict] = []
        failed_count = 0
        total = len(vendor_pages)
        start_time = time.time()

        chunks = [vendor_pages[i:i + batch_size] for i in range(0, total, batch_size)]

        for chunk_idx, chunk in enumerate(chunks):
            urls = [vp["url"] for vp in chunk]
            logger.info(f"[ExtractorAgent] Extracting chunk {chunk_idx + 1}/{len(chunks)} — {len(urls)} pages")

            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    page_results = loop.run_until_complete(fetch_pages_batch_async(urls))
                finally:
                    loop.close()
            except Exception as e:
                logger.error(f"[ExtractorAgent] batch fetch failed: {e}")
                failed_count += len(chunk)
                continue

            for vp, page_result in zip(chunk, page_results):
                url = vp.get("url", "")
                if not page_result.get("success") or not page_result.get("html"):
                    failed_count += 1
                    stats.extraction_failed += 1
                    continue

                html = page_result["html"]
                source_page = vp.get("source_page", "")
                event_meta = vp.get("event_metadata") or page_metadata.get(source_page, {})

                event_context = {
                    "event_name": event_meta.get("event_name", ""),
                    "event_location": event_meta.get("event_location", ""),
                    "event_date": event_meta.get("event_date", ""),
                }

                try:
                    vendor = run_extraction_pipeline.invoke({
                        "html": html,
                        "url": url,
                        "event_context": event_context,
                    })
                except Exception as e:
                    logger.debug(f"[ExtractorAgent] extraction failed for {url}: {e}")
                    vendor = {}
                    failed_count += 1
                    stats.extraction_failed += 1
                    continue

                if not vendor or not vendor.get("name"):
                    failed_count += 1
                    stats.extraction_failed += 1
                    continue

                if vendor.get("confidence_score", 0) < 0.10:
                    logger.debug(f"[ExtractorAgent] low confidence ({vendor.get('confidence_score', 0):.2f}) skipped: {url}")
                    failed_count += 1
                    stats.extraction_failed += 1
                    continue

                method = vendor.get("extraction_method", "unknown")
                if "schema_org" in method:
                    stats.extraction_schema_org += 1
                elif "rule_based" in method:
                    stats.extraction_rule_based += 1
                elif "llm" in method:
                    stats.extraction_llm += 1

                stats.total_vendors_extracted += 1
                stats.total_vendor_pages += 1

                if vendor.get("country"):
                    stats.countries_found.add(vendor["country"])
                if event_context.get("event_name"):
                    stats.events_found.add(event_context["event_name"])

                all_vendors.append(vendor)

            if (chunk_idx + 1) % 3 == 0 or chunk_idx == len(chunks) - 1:
                print_extract_stats(
                    stats.extraction_schema_org,
                    stats.extraction_rule_based,
                    stats.extraction_llm,
                    stats.total_vendors_extracted,
                )
                if all_vendors:
                    print_vendor_preview_table(all_vendors[-8:])

        elapsed = time.time() - start_time
        logger.info(
            f"[ExtractorAgent] Done — {len(all_vendors)} vendors extracted, "
            f"{failed_count} failed in {elapsed:.1f}s"
        )

        return {
            "vendors": all_vendors,
            "failed_count": failed_count,
            "stats": stats,
            "elapsed": elapsed,
        }
