import json
import time
from typing import Any
from loguru import logger

from backend.graph.state import CrawlerState
from backend.agents.search_agent import SearchAgent
from backend.agents.crawler_agent import CrawlerAgent
from backend.agents.extractor_agent import ExtractorAgent
from backend.agents.enrichment_agent import EnrichmentAgent
from backend.agents.export_agent import ExportAgent
from backend.core.config import get_settings, CrawlStats
from backend.utils.display import (
    print_node_transition, print_supervisor_decision,
    print_info, print_warning, print_error,
)

_settings = get_settings()

_search_agent = None
_crawler_agent = None
_extractor_agent = None
_enrichment_agent = None
_export_agent = None


def _get_search_agent() -> SearchAgent:
    global _search_agent
    if _search_agent is None:
        _search_agent = SearchAgent()
    return _search_agent


def _get_crawler_agent() -> CrawlerAgent:
    global _crawler_agent
    if _crawler_agent is None:
        _crawler_agent = CrawlerAgent()
    return _crawler_agent


def _get_extractor_agent() -> ExtractorAgent:
    global _extractor_agent
    if _extractor_agent is None:
        _extractor_agent = ExtractorAgent()
    return _extractor_agent


def _get_enrichment_agent() -> EnrichmentAgent:
    global _enrichment_agent
    if _enrichment_agent is None:
        _enrichment_agent = EnrichmentAgent()
    return _enrichment_agent


def _get_export_agent() -> ExportAgent:
    global _export_agent
    if _export_agent is None:
        _export_agent = ExportAgent()
    return _export_agent


def node_discover_seeds(state: CrawlerState) -> dict:
    print_node_transition("START", "discover_seeds")
    query = state["query"]
    stats: CrawlStats = state["stats"] or CrawlStats()

    agent = _get_search_agent()
    result = agent.run({"query": query, "max_seeds": 40})

    seed_urls = result.get("seed_urls", [])
    if not seed_urls:
        print_warning("discover_seeds", "No seeds found — check your query or network")

    stats.domains_crawled.update(seed_urls)
    print_node_transition("discover_seeds", "crawl_batch")

    return {
        "seed_urls": seed_urls,
        "seed_metadata": result.get("seed_metadata", []),
        "crawl_queue": [{"url": u, "depth": 0, "source": "seed", "priority": 10} for u in seed_urls],
        "stats": stats,
        "phase": "crawl",
    }


def node_crawl_batch(state: CrawlerState) -> dict:
    print_node_transition("discover_seeds", "crawl_batch")
    seed_urls = state.get("seed_urls", [])
    visited = state.get("visited_urls", set())
    existing_vendor_pages = state.get("vendor_pages", [])
    existing_metadata = state.get("page_metadata", {})
    stats: CrawlStats = state["stats"] or CrawlStats()
    current_batch = state.get("current_batch", 0)

    agent = _get_crawler_agent()
    result = agent.run({
        "seed_urls": seed_urls,
        "query": state["query"],
        "max_depth": _settings.max_depth,
        "batch_size": _settings.batch_size,
        "visited_urls": visited,
    })

    new_vendor_pages = result.get("vendor_pages", [])
    new_metadata = result.get("page_metadata", {})

    seen = {vp["url"] for vp in existing_vendor_pages}
    for vp in new_vendor_pages:
        if vp.get("url") and vp["url"] not in seen:
            existing_vendor_pages.append(vp)
            seen.add(vp["url"])
    existing_metadata.update(new_metadata)

    stats.total_crawled += result.get("total_crawled", 0)
    stats.total_errors += result.get("total_errors", 0)
    stats.domains_crawled.update(result.get("visited_urls", set()))

    print_info("crawl_batch", f"Total vendor pages found: {len(existing_vendor_pages)}")
    print_node_transition("crawl_batch", "extract_vendors")

    return {
        "vendor_pages": existing_vendor_pages,
        "page_metadata": existing_metadata,
        "visited_urls": result.get("visited_urls", visited),
        "total_crawled": state.get("total_crawled", 0) + result.get("total_crawled", 0),
        "total_errors": state.get("total_errors", 0) + result.get("total_errors", 0),
        "current_batch": current_batch + 1,
        "stats": stats,
        "phase": "extract",
    }


def node_supervisor_check(state: CrawlerState) -> dict:
    errors = state.get("error_log", [])
    total_errors = state.get("total_errors", 0)
    total_crawled = state.get("total_crawled", 0)
    vendors_found = len(state.get("raw_vendors", []) or state.get("vendors", []))
    phase = state.get("phase", "")
    supervisor_actions = state.get("supervisor_actions", [])

    if total_errors == 0 and total_crawled > 0:
        action_data = {
            "action": "continue",
            "reason": "No errors detected",
            "phase": phase,
            "timestamp": time.time(),
        }
        supervisor_actions.append(action_data)
        print_supervisor_decision("continue", "No errors — proceeding", total_errors)
        return {"supervisor_actions": supervisor_actions}

    if not _settings.has_openai_key or not _settings.llm_supervisor_enabled:
        action_data = {
            "action": "continue",
            "reason": "LLM supervisor disabled",
            "phase": phase,
            "timestamp": time.time(),
        }
        supervisor_actions.append(action_data)
        print_supervisor_decision("continue", "LLM supervisor disabled — auto-continue", total_errors)
        return {"supervisor_actions": supervisor_actions}

    try:
        from openai import OpenAI
        client = OpenAI(api_key=_settings.openai_api_key)

        recent_errors = errors[-_settings.llm_supervisor_context_lines:]
        error_summary = "\n".join(recent_errors) if recent_errors else "None"

        summary = (
            f"Phase: {phase}\n"
            f"Crawled: {total_crawled} pages\n"
            f"Errors: {total_errors}\n"
            f"Vendors found: {vendors_found}\n"
            f"Recent errors (last {len(recent_errors)}):\n{error_summary[:800]}"
        )

        supervisor_kwargs = {
            "model": _settings.openai_model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a crawler supervisor. Based on the crawl summary, decide an action. "
                        "Return JSON only: {\"action\": \"continue\"|\"retry\"|\"skip_domain\"|\"adjust_depth\", \"reason\": \"brief reason\"}"
                    ),
                },
                {"role": "user", "content": summary},
            ],
            "max_completion_tokens": 60,
        }
        if _settings.model_supports_temperature:
            supervisor_kwargs["temperature"] = 0.0

        response = client.chat.completions.create(**supervisor_kwargs)

        content = response.choices[0].message.content.strip()
        try:
            decision = json.loads(content)
        except json.JSONDecodeError:
            import re
            match = re.search(r'\{.*?\}', content, re.DOTALL)
            decision = json.loads(match.group()) if match else {"action": "continue", "reason": "parse error"}

        action = decision.get("action", "continue")
        reason = decision.get("reason", "")

        action_data = {
            "action": action,
            "reason": reason,
            "phase": phase,
            "timestamp": time.time(),
            "input_summary": summary[:200],
        }
        supervisor_actions.append(action_data)
        print_supervisor_decision(action, reason, total_errors)

        return {"supervisor_actions": supervisor_actions}

    except Exception as e:
        logger.warning(f"[Supervisor] LLM call failed: {e}")
        supervisor_actions.append({"action": "continue", "reason": f"supervisor error: {e}", "phase": phase})
        return {"supervisor_actions": supervisor_actions}


def node_extract_vendors(state: CrawlerState) -> dict:
    print_node_transition("crawl_batch", "extract_vendors")
    vendor_pages = state.get("vendor_pages", [])
    page_metadata = state.get("page_metadata", {})
    stats: CrawlStats = state["stats"] or CrawlStats()

    if not vendor_pages:
        print_warning("extract_vendors", "No vendor pages — skipping extraction")
        return {"raw_vendors": [], "phase": "enrich"}

    agent = _get_extractor_agent()
    result = agent.run({
        "vendor_pages": vendor_pages,
        "page_metadata": page_metadata,
        "stats": stats,
        "batch_size": 50,
    })

    new_vendors = result.get("vendors", [])
    updated_stats: CrawlStats = result.get("stats", stats)

    print_info("extract_vendors", f"Extracted {len(new_vendors)} vendors")
    print_node_transition("extract_vendors", "enrich_domains")

    return {
        "raw_vendors": new_vendors,
        "stats": updated_stats,
        "phase": "enrich",
    }


def node_enrich_domains(state: CrawlerState) -> dict:
    print_node_transition("extract_vendors", "enrich_domains")
    raw_vendors = state.get("raw_vendors", [])
    stats: CrawlStats = state["stats"] or CrawlStats()

    if not raw_vendors:
        print_warning("enrich_domains", "No vendors to enrich")
        return {"vendors": [], "phase": "export"}

    agent = _get_enrichment_agent()
    result = agent.run({
        "vendors": raw_vendors,
        "max_to_enrich": 150,
        "enrich_missing_only": True,
    })

    enriched_vendors = result.get("vendors", raw_vendors)
    print_info("enrich_domains", f"Enrichment complete — {result.get('enriched_count', 0)} vendors improved")
    print_node_transition("enrich_domains", "export_results")

    return {
        "vendors": enriched_vendors,
        "stats": stats,
        "phase": "export",
    }


def node_export_results(state: CrawlerState) -> dict:
    print_node_transition("enrich_domains", "export_results")
    vendors = state.get("vendors", [])
    query = state.get("query", "export")
    stats: CrawlStats = state["stats"] or CrawlStats()

    agent = _get_export_agent()
    result = agent.run({
        "vendors": vendors,
        "query": query,
        "stats": stats,
        "skip_dedup": False,
    })

    print_node_transition("export_results", "END")

    return {
        "output_excel": result.get("excel_path", ""),
        "output_csv": result.get("csv_path", ""),
        "vendors": vendors,
        "done": True,
        "phase": "done",
    }


def should_run_supervisor(state: CrawlerState) -> str:
    total_errors = state.get("total_errors", 0)
    phase = state.get("phase", "")
    threshold = _settings.llm_error_threshold

    if total_errors > threshold:
        return "supervisor"
    if phase == "crawl" and total_errors > threshold // 2:
        return "supervisor"
    return "skip_supervisor"


def should_continue_crawl(state: CrawlerState) -> str:
    vendor_pages = state.get("vendor_pages", [])
    total_crawled = state.get("total_crawled", 0)
    current_batch = state.get("current_batch", 0)
    supervisor_actions = state.get("supervisor_actions", [])

    if supervisor_actions:
        last_action = supervisor_actions[-1].get("action", "continue")
        if last_action == "abort":
            return "extract"

    if len(vendor_pages) >= 250:
        return "extract"
    if total_crawled >= _settings.batch_size * 5:
        return "extract"
    if current_batch >= 3:
        return "extract"

    return "extract"
