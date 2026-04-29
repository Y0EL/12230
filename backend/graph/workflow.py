import asyncio
import json
from typing import Any

from langchain_core.messages import HumanMessage, ToolMessage
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
from loguru import logger

from backend.core.config import get_settings
from backend.graph.prompts import build_system_prompt
from backend.tools.search_tools import ALL_SEARCH_TOOLS
from backend.tools.fetch_tools import ALL_FETCH_TOOLS
from backend.tools.extract_tools import ALL_EXTRACT_TOOLS
from backend.tools.export_tools import ALL_EXPORT_TOOLS
from backend.tools.enrich_tools import enrich_vendors_parallel
from backend.utils.display import start_thinking, update_thinking, stop_thinking, print_tool_start, print_tool_end

_settings = get_settings()

# Increased from 6000 — search results and fetch metadata need room;
# vendor lists are no longer passed through the LLM (registry pattern)
MAX_TOOL_OUTPUT_CHARS = 10000


def _pre_model_hook(state: dict) -> dict:
    trimmed = []
    for msg in state.get("messages", []):
        if isinstance(msg, ToolMessage) and isinstance(msg.content, str):
            if len(msg.content) > MAX_TOOL_OUTPUT_CHARS:
                content = msg.content[:MAX_TOOL_OUTPUT_CHARS] + "\n[output dipotong karena terlalu panjang]"
                msg = ToolMessage(
                    content=content,
                    tool_call_id=msg.tool_call_id,
                    name=getattr(msg, "name", ""),
                    id=msg.id,
                )
        trimmed.append(msg)
    return {"messages": trimmed}


def _build_llm() -> ChatOpenAI:
    kwargs: dict[str, Any] = {
        "model": _settings.openai_model,
        "api_key": _settings.openai_api_key,
        "streaming": True,
    }
    if _settings.model_supports_temperature:
        kwargs["temperature"] = 0.0
    return ChatOpenAI(**kwargs)


def build_react_agent(system_prompt: str):
    llm = _build_llm()
    all_tools = ALL_SEARCH_TOOLS + ALL_FETCH_TOOLS + ALL_EXTRACT_TOOLS + ALL_EXPORT_TOOLS + [enrich_vendors_parallel]
    return create_react_agent(
        model=llm,
        tools=all_tools,
        prompt=system_prompt,
        pre_model_hook=_pre_model_hook,
    )


def _unwrap_output(output: Any) -> Any:
    if hasattr(output, "content"):
        raw = output.content
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except Exception:
                return raw
        return raw
    return output


def _summarize_tool_output(name: str, output: Any) -> str:
    output = _unwrap_output(output)
    if isinstance(output, list):
        return f"{len(output)} item"
    if isinstance(output, dict):
        # Registry-pattern tool summaries
        if "total_in_registry" in output:
            return f"registered={output.get('registered', '?')}  registry_total={output['total_in_registry']}"
        if "deduped_count" in output:
            return f"{output.get('original_count','?')} → {output['deduped_count']} vendors"
        if "total_vendors" in output:
            return f"total_vendors={output['total_vendors']}"
        for key in ("excel_path", "csv_path", "vendor_count"):
            if key in output:
                return f"{key}={output[key]}"
        if "url" in output:
            return str(output["url"])[:80]
        return f"{len(output)} field"
    if isinstance(output, str):
        return output[:80]
    return str(output)[:80]


def _capture_export_result(name: str, raw_output: Any, result: dict) -> None:
    output = _unwrap_output(raw_output)
    if name == "export_to_excel" and isinstance(output, str) and output:
        result["output_excel"] = output
    elif name == "export_to_csv" and isinstance(output, str) and output:
        result["output_csv"] = output
    elif name == "deduplicate_vendors":
        # New registry-based dedup returns a summary dict, not the full list.
        # result["vendors"] is populated from the registry at the end of _run_async.
        if isinstance(output, dict):
            result["_dedup_count"] = output.get("deduped_count", 0)
        elif isinstance(output, list):
            # Legacy path — still capture if old-style list returned
            result["vendors"] = output


_TOP_LEVEL_TOOLS = {
    "search_exhibitor_events", "search_vendor_directory", "search_company_info",
    "fetch_page", "fetch_pages_batch", "check_robots_txt", "resolve_final_url",
    "run_extraction_pipeline", "discover_vendor_urls", "extract_vendors_from_pdf",
    "get_vendor_count", "generate_and_run_parser", "enrich_vendors_parallel",
    "deduplicate_vendors", "export_to_excel", "export_to_csv",
}

_agent_text_buf: list[str] = []


def _handle_event(event: dict, result: dict) -> None:
    global _agent_text_buf
    kind = event.get("event", "")

    if kind == "on_chat_model_start":
        start_thinking()
        _agent_text_buf = []

    elif kind == "on_chat_model_stream":
        chunk = event["data"].get("chunk")
        if chunk and hasattr(chunk, "content") and chunk.content:
            _agent_text_buf.append(chunk.content)
            last_line = "".join(_agent_text_buf).split("\n")[-1].strip()
            if last_line:
                update_thinking(last_line)

    elif kind == "on_chat_model_end":
        final_text = "".join(_agent_text_buf).strip()
        stop_thinking(final_text)
        _agent_text_buf = []

    elif kind == "on_tool_start":
        name = event.get("name", "")
        if name in _TOP_LEVEL_TOOLS:
            args = event["data"].get("input", {})
            print_tool_start(name, args if isinstance(args, dict) else {"input": str(args)[:80]})

    elif kind == "on_tool_end":
        name = event.get("name", "")
        if name in _TOP_LEVEL_TOOLS:
            raw_output = event["data"].get("output")
            print_tool_end(name, _summarize_tool_output(name, raw_output))
            _capture_export_result(name, raw_output, result)


async def _run_async(query: str, max_vendors: int, skip_enrich: bool) -> dict:
    from backend.tools.vendor_registry import clear_registry, get_all_vendors

    # Fresh registry for each run — prevents vendors from a previous run leaking in
    clear_registry()

    _settings.max_total_vendors = max_vendors

    system_prompt = build_system_prompt(max_vendors=max_vendors, skip_enrich=skip_enrich)
    agent = build_react_agent(system_prompt=system_prompt)
    result: dict = {"vendors": [], "output_excel": "", "output_csv": ""}

    logger.info(f"ReAct agent starting for query: {query!r}")

    async for event in agent.astream_events(
        {"messages": [HumanMessage(content=query)]},
        version="v2",
        config={"recursion_limit": 200},
    ):
        _handle_event(event, result)

    # Always populate vendors from the registry (the authoritative source).
    # This captures all vendors regardless of what the LLM passed around.
    registry_vendors = get_all_vendors()
    if registry_vendors:
        result["vendors"] = registry_vendors
        logger.info(f"[REGISTRY] Final vendor count from registry: {len(registry_vendors)}")

    logger.info(f"ReAct agent finished. vendors={len(result['vendors'])} excel={result['output_excel']!r}")
    return result


def run_crawler(query: str, max_vendors: int = 10000, skip_enrich: bool = False) -> dict:
    return asyncio.run(_run_async(query, max_vendors, skip_enrich))
