import time
from typing import Any
from loguru import logger

from backend.agents.base_agent import BaseAgent
from backend.tools.export_tools import export_to_excel, export_to_csv, deduplicate_vendors, ALL_EXPORT_TOOLS
from backend.core.config import get_settings, CrawlStats
from backend.utils.display import print_export_result, print_final_summary, print_info


class ExportAgent(BaseAgent):
    agent_name = "export_agent"

    def __init__(self, **kwargs) -> None:
        super().__init__(tools=ALL_EXPORT_TOOLS, **kwargs)
        self._settings = get_settings()

    def run(self, input_data: dict) -> dict:
        vendors = input_data.get("vendors", [])
        query = input_data.get("query", "export")
        stats: CrawlStats = input_data.get("stats", CrawlStats())
        skip_dedup = input_data.get("skip_dedup", False)

        if not vendors:
            logger.warning("[ExportAgent] No vendors to export")
            return {"excel_path": "", "csv_path": "", "vendor_count": 0}

        start_time = time.time()

        if not skip_dedup:
            print_info("ExportAgent", f"Deduplicating {len(vendors)} vendors...")
            vendors = deduplicate_vendors.invoke({"vendors": vendors})
            print_info("ExportAgent", f"After dedup: {len(vendors)} vendors")

        stats_dict = stats.to_dict()
        stats_dict["total_vendors_extracted"] = len(vendors)

        print_info("ExportAgent", f"Exporting {len(vendors)} vendors to Excel...")
        excel_path = export_to_excel.invoke({
            "vendors": vendors,
            "query": query,
            "stats": stats_dict,
        })

        print_info("ExportAgent", "Exporting to CSV...")
        csv_path = export_to_csv.invoke({
            "vendors": vendors,
            "query": query,
        })

        countries = len({v.get("country") for v in vendors if v.get("country")})
        events = len({v.get("event_name") for v in vendors if v.get("event_name")})

        if excel_path:
            print_export_result(excel_path, len(vendors), countries, events)

        stats.elapsed_seconds = time.time() - start_time
        stats_dict = stats.to_dict()
        stats_dict["total_vendors_extracted"] = len(vendors)
        stats_dict["countries_found"] = countries
        stats_dict["events_found"] = events
        print_final_summary(stats_dict)

        return {
            "excel_path": excel_path,
            "csv_path": csv_path,
            "vendor_count": len(vendors),
            "countries": countries,
            "events": events,
            "stats": stats_dict,
            "elapsed": time.time() - start_time,
        }
