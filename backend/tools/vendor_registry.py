"""
Global in-memory vendor registry.

Every extraction tool (run_extraction_pipeline, extract_vendors_from_pdf)
automatically registers vendors here.  This means the LangGraph agent
NEVER needs to pass huge vendor lists between tool calls — it just calls
export_to_excel(query="...") and deduplicate_vendors() without arguments
and the data comes from this registry.
"""
from loguru import logger
from langchain_core.tools import tool

_REGISTRY: list[dict] = []


# ── Internal API (used by extract/export tools) ───────────────────────────────

def register_vendor(vendor: dict) -> None:
    """Add one validated vendor to the registry. Silently skips empties."""
    if vendor and isinstance(vendor, dict) and vendor.get("name"):
        _REGISTRY.append(vendor)


def register_vendors(vendors: list[dict]) -> int:
    """
    Bulk-add vendors to the registry.
    Returns the new total registry size.
    """
    before = len(_REGISTRY)
    for v in vendors:
        if v and isinstance(v, dict) and v.get("name"):
            _REGISTRY.append(v)
    added = len(_REGISTRY) - before
    logger.info(f"[REGISTRY] +{added} vendors  |  total={len(_REGISTRY)}")
    return len(_REGISTRY)


def get_all_vendors() -> list[dict]:
    """Return a copy of all vendors in the registry."""
    return list(_REGISTRY)


def get_count() -> int:
    """Return the number of vendors currently in the registry."""
    return len(_REGISTRY)


def replace_all(vendors: list[dict]) -> None:
    """
    Replace the entire registry with a new list.
    Called by deduplicate_vendors after deduplication.
    """
    _REGISTRY.clear()
    _REGISTRY.extend(vendors)
    logger.info(f"[REGISTRY] Replaced with {len(_REGISTRY)} vendors (after dedup)")


def clear_registry() -> None:
    """Clear the registry. Called at the start of each crawler run."""
    _REGISTRY.clear()
    logger.info("[REGISTRY] Cleared for new run")


# ── LangChain tool ────────────────────────────────────────────────────────────

@tool
def get_vendor_count() -> dict:
    """
    Check how many vendors have been collected in the registry so far.
    Use this to monitor crawling progress without passing vendor lists around.
    Returns: {"total_vendors": N}
    """
    count = get_count()
    return {"total_vendors": count}
