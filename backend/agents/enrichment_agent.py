import asyncio
import time
from urllib.parse import urlparse
from typing import Any
from loguru import logger

import tldextract

from backend.agents.base_agent import BaseAgent
from backend.tools.fetch_tools import fetch_page_async, ALL_FETCH_TOOLS
from backend.tools.parse_tools import extract_links, extract_vendor_domain_links, ALL_PARSE_TOOLS
from backend.tools.extract_tools import extract_rule_based, extract_schema_org, merge_vendor_data, ALL_EXTRACT_TOOLS
from backend.tools.search_tools import search_company_info, ALL_SEARCH_TOOLS
from backend.core.config import get_settings
from backend.utils.display import print_enrich_progress, print_info


class EnrichmentAgent(BaseAgent):
    agent_name = "enrichment_agent"

    def __init__(self, **kwargs) -> None:
        tools = ALL_FETCH_TOOLS + ALL_PARSE_TOOLS + ALL_EXTRACT_TOOLS + ALL_SEARCH_TOOLS
        super().__init__(tools=tools, **kwargs)
        self._settings = get_settings()

    def _get_vendor_domain(self, vendor: dict) -> str:
        website = vendor.get("website", "")
        if not website:
            return ""
        try:
            extracted = tldextract.extract(website)
            if extracted.domain and extracted.suffix:
                return f"{extracted.domain}.{extracted.suffix}"
        except Exception:
            pass
        return ""

    def _enrich_from_domain(self, vendor: dict) -> dict:
        domain = self._get_vendor_domain(vendor)
        if not domain:
            return vendor

        homepage_url = f"https://{domain}"
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                page_result = loop.run_until_complete(fetch_page_async(homepage_url))
            finally:
                loop.close()
        except Exception as e:
            logger.debug(f"[EnrichmentAgent] fetch failed for {homepage_url}: {e}")
            return vendor

        if not page_result.get("success") or not page_result.get("html"):
            return vendor

        html = page_result["html"]
        enriched_data = {}

        schema_result = extract_schema_org.invoke({"html": html, "url": homepage_url})
        if schema_result:
            enriched_data.update(schema_result)

        if not enriched_data.get("email") or not enriched_data.get("phone"):
            rule_result = extract_rule_based.invoke({"html": html, "url": homepage_url})
            if rule_result:
                for field in ["email", "phone", "address", "city", "country", "description", "linkedin", "twitter"]:
                    if rule_result.get(field) and not enriched_data.get(field):
                        enriched_data[field] = rule_result[field]

        enriched_data["extraction_method"] = "enrichment"
        merged = merge_vendor_data.invoke({"sources": [vendor, enriched_data]})
        return merged

    def run(self, input_data: dict) -> dict:
        vendors = input_data.get("vendors", [])
        max_to_enrich = input_data.get("max_to_enrich", 200)
        enrich_missing_only = input_data.get("enrich_missing_only", True)

        if not vendors:
            return {"vendors": [], "enriched_count": 0}

        to_enrich = []
        skip_enrich = []

        for v in vendors:
            if enrich_missing_only:
                missing_fields = sum(1 for f in ["email", "phone", "description", "country"] if not v.get(f))
                has_domain = bool(self._get_vendor_domain(v))
                if missing_fields >= 2 and has_domain:
                    to_enrich.append(v)
                else:
                    skip_enrich.append(v)
            else:
                if self._get_vendor_domain(v):
                    to_enrich.append(v)
                else:
                    skip_enrich.append(v)

        to_enrich = to_enrich[:max_to_enrich]
        logger.info(f"[EnrichmentAgent] Enriching {len(to_enrich)}/{len(vendors)} vendors")

        enriched_vendors = list(skip_enrich)
        enriched_count = 0
        start_time = time.time()

        for idx, vendor in enumerate(to_enrich):
            domain = self._get_vendor_domain(vendor)
            print_enrich_progress(idx + 1, len(to_enrich), domain)

            enriched = self._enrich_from_domain(vendor)
            enriched_vendors.append(enriched)

            if enriched.get("email") and not vendor.get("email"):
                enriched_count += 1
            elif enriched.get("country") and not vendor.get("country"):
                enriched_count += 1

        elapsed = time.time() - start_time
        logger.info(
            f"[EnrichmentAgent] Enriched {enriched_count} vendors in {elapsed:.1f}s"
        )

        return {
            "vendors": enriched_vendors,
            "enriched_count": enriched_count,
            "elapsed": elapsed,
        }
