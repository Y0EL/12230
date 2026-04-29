import time
from typing import Any
from loguru import logger

from backend.agents.base_agent import BaseAgent
from backend.tools.search_tools import search_exhibitor_events, search_vendor_directory, ALL_SEARCH_TOOLS
from backend.utils.display import print_discover_start, print_discover_result, print_warning, print_thinking


class SearchAgent(BaseAgent):
    agent_name = "search_agent"

    def __init__(self, **kwargs) -> None:
        super().__init__(tools=ALL_SEARCH_TOOLS, **kwargs)

    def run(self, input_data: dict) -> dict:
        query = input_data.get("query", "")
        max_seeds = input_data.get("max_seeds", 40)

        if not query:
            logger.error("[SearchAgent] No query provided")
            return {"seed_urls": [], "query": query, "error": "No query"}

        print_thinking("SearchAgent", f"query diterima: {query!r}")
        print_discover_start(query)
        start = time.time()

        print_thinking("SearchAgent", f"memanggil search_exhibitor_events max_seeds={max_seeds}")
        try:
            results = self._call_tool("search_exhibitor_events", query=query, max_seeds=max_seeds)
        except Exception as e:
            logger.error(f"[SearchAgent] search_exhibitor_events failed: {e}")
            results = []

        print_thinking("SearchAgent", f"seed URLs awal: {len(results)}")

        if len(results) < 10:
            print_thinking("SearchAgent", "hasil kurang dari 10, coba vendor_directory sebagai suplemen")
            try:
                extra = self._call_tool("search_vendor_directory", query=query)
                seen = {r["url"] for r in results}
                for r in extra:
                    if r.get("url") and r["url"] not in seen:
                        results.append(r)
                        seen.add(r["url"])
            except Exception as e:
                print_warning("SearchAgent", f"vendor_directory search failed: {e}")

        results.sort(key=lambda x: x.get("score", 0), reverse=True)
        results = results[:max_seeds]

        seed_urls = [r["url"] for r in results if r.get("url")]
        elapsed = time.time() - start

        print_thinking("SearchAgent", f"selesai: {len(seed_urls)} seed URLs dalam {elapsed:.1f}s")
        print_discover_result(len(seed_urls), elapsed, seed_urls[:5])

        return {
            "seed_urls": seed_urls,
            "seed_metadata": results,
            "query": query,
            "elapsed": elapsed,
        }
