"""
Per-worker temp file persistence for the URL Worker Agent pool.
Files are written to output/temp/{session_id}/{url_hash}.json.
Used for: crash recovery, debug inspection, per-URL progress logging.
"""
from __future__ import annotations

import hashlib
import json
import shutil
from datetime import datetime
from pathlib import Path

from loguru import logger

_TEMP_BASE = Path("output/temp")
_SESSION_ID: str | None = None


def get_or_create_session_id() -> str:
    """Return the current session ID, creating one if needed (once per run)."""
    global _SESSION_ID
    if not _SESSION_ID:
        _SESSION_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
    return _SESSION_ID


def reset_session() -> None:
    """Reset session ID — call this at the start of each new crawl run."""
    global _SESSION_ID
    _SESSION_ID = None


def _url_hash(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:12]


def _temp_path(session_id: str, url: str) -> Path:
    return _TEMP_BASE / session_id / f"{_url_hash(url)}.json"


def save_worker_progress(session_id: str, url: str, data: dict) -> None:
    """Write worker state to output/temp/{session_id}/{md5(url)[:12]}.json"""
    path = _temp_path(session_id, url)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "url": url,
            "session_id": session_id,
            "last_updated": datetime.now().isoformat(),
            **data,
        }
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        logger.debug(f"[TEMP] Failed to save progress for {url}: {e}")


def load_worker_progress(session_id: str, url: str) -> dict | None:
    """Read temp file. Returns None if not found or unreadable."""
    path = _temp_path(session_id, url)
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.debug(f"[TEMP] Failed to load progress for {url}: {e}")
    return None


def get_all_worker_data(session_id: str) -> list[dict]:
    """Read all temp files for a session (for debugging / merging)."""
    session_dir = _TEMP_BASE / session_id
    results: list[dict] = []
    if not session_dir.exists():
        return results
    for path in session_dir.glob("*.json"):
        try:
            results.append(json.loads(path.read_text(encoding="utf-8")))
        except Exception:
            pass
    return results


def cleanup_session(session_id: str) -> None:
    """Delete all temp files for a session (call after export if desired)."""
    session_dir = _TEMP_BASE / session_id
    try:
        if session_dir.exists():
            shutil.rmtree(session_dir)
            logger.info(f"[TEMP] Cleaned up session {session_id}")
    except Exception as e:
        logger.debug(f"[TEMP] Failed to cleanup session {session_id}: {e}")
