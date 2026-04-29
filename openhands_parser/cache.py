import json
import re
from datetime import datetime
from pathlib import Path

from loguru import logger


def _safe_name(domain: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", domain)


class ParserCache:
    def __init__(self, cache_dir: Path | None = None):
        if cache_dir is None:
            from backend.core.config import BASE_DIR
            cache_dir = BASE_DIR / "output" / "parser_cache"
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _py_path(self, domain: str) -> Path:
        return self.cache_dir / f"{_safe_name(domain)}.py"

    def _meta_path(self, domain: str) -> Path:
        return self.cache_dir / f"{_safe_name(domain)}.json"

    def get(self, domain: str) -> str | None:
        p = self._py_path(domain)
        if p.exists():
            code = p.read_text(encoding="utf-8")
            logger.debug(f"[CACHE] hit: {domain}")
            return code
        return None

    def save(self, domain: str, code: str, metadata: dict | None = None) -> None:
        self._py_path(domain).write_text(code, encoding="utf-8")
        meta = {
            "domain": domain,
            "created_at": datetime.utcnow().isoformat(),
            "success_count": 0,
            **(metadata or {}),
        }
        self._meta_path(domain).write_text(json.dumps(meta, indent=2), encoding="utf-8")
        logger.info(f"[CACHE] saved parser for {domain}")

    def bump_success(self, domain: str) -> None:
        p = self._meta_path(domain)
        if not p.exists():
            return
        try:
            meta = json.loads(p.read_text(encoding="utf-8"))
            meta["success_count"] = meta.get("success_count", 0) + 1
            meta["last_used"] = datetime.utcnow().isoformat()
            p.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        except Exception:
            pass

    def invalidate(self, domain: str) -> None:
        for p in (self._py_path(domain), self._meta_path(domain)):
            p.unlink(missing_ok=True)
        logger.info(f"[CACHE] invalidated: {domain}")

    def list_cached(self) -> list[dict]:
        results = []
        for meta_file in sorted(self.cache_dir.glob("*.json")):
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
                meta["has_code"] = meta_file.with_suffix(".py").exists()
                results.append(meta)
            except Exception:
                pass
        return results
