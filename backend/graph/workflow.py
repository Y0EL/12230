from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from backend.graph.state import CrawlerState, initial_state
from backend.graph.nodes import (
    node_discover_seeds,
    node_crawl_batch,
    node_supervisor_check,
    node_extract_vendors,
    node_enrich_domains,
    node_export_results,
    should_run_supervisor,
    should_continue_crawl,
)
from backend.core.config import get_settings
from loguru import logger


def build_crawler_graph(use_checkpointer: bool = False) -> StateGraph:
    settings = get_settings()

    builder = StateGraph(CrawlerState)

    builder.add_node("discover_seeds", node_discover_seeds)
    builder.add_node("crawl_batch", node_crawl_batch)
    builder.add_node("supervisor_check", node_supervisor_check)
    builder.add_node("extract_vendors", node_extract_vendors)
    builder.add_node("enrich_domains", node_enrich_domains)
    builder.add_node("export_results", node_export_results)

    builder.set_entry_point("discover_seeds")

    builder.add_edge("discover_seeds", "crawl_batch")

    builder.add_conditional_edges(
        "crawl_batch",
        should_run_supervisor,
        {
            "supervisor": "supervisor_check",
            "skip_supervisor": "extract_vendors",
        },
    )

    builder.add_conditional_edges(
        "supervisor_check",
        should_continue_crawl,
        {
            "extract": "extract_vendors",
        },
    )

    builder.add_edge("extract_vendors", "enrich_domains")
    builder.add_edge("enrich_domains", "export_results")
    builder.add_edge("export_results", END)

    if use_checkpointer:
        checkpointer = MemorySaver()
        return builder.compile(checkpointer=checkpointer)

    return builder.compile()


def run_crawler(query: str) -> dict:
    logger.info(f"Building crawler graph for query: {query}")
    graph = build_crawler_graph()

    state = initial_state(query)
    logger.info("Starting LangGraph execution")

    final_state = graph.invoke(state)

    return {
        "vendors": final_state.get("vendors", []),
        "output_excel": final_state.get("output_excel", ""),
        "output_csv": final_state.get("output_csv", ""),
        "total_crawled": final_state.get("total_crawled", 0),
        "total_errors": final_state.get("total_errors", 0),
        "stats": final_state.get("stats"),
    }
