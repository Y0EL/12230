import asyncio
import json
import re

import httpx
from loguru import logger

from openhands_parser.schema import VENDOR_SCHEMA, SCHEMA_JSON_EXAMPLE


_GENERATE_PROMPT = """\
You are a Python web scraping expert. Write a single Python function:

def parse(html: str) -> list[dict]:

The function must extract company/vendor records from this HTML page: {url}

Each record must follow this schema (only include fields you can actually find):
{schema_json}

Example output record:
{example}

Rules:
- Use only: stdlib, bs4 (BeautifulSoup), lxml, re (all are pre-installed)
- Import libraries INSIDE the function body
- Return empty list [] if no vendors found — never raise exceptions
- The "name" and "source_url" fields are required; set source_url = "{url}"
- Output ONLY the function definition — no main block, no test code, no markdown
- Clean company names: strip whitespace, remove leading numbers like "1." or "(1)"
- If the page has a table, parse table rows; if cards, parse card elements

HTML of the page (first 8000 chars):
{html_sample}
"""


def _extract_code_block(text: str) -> str:
    # Full code block — may contain classes/helpers above def parse()
    match = re.search(r"```python\s*([\s\S]+?)```", text)
    if match:
        block = match.group(1).strip()
        if "def parse" in block:
            return block
    match = re.search(r"```\s*([\s\S]+?)```", text)
    if match:
        block = match.group(1).strip()
        if "def parse" in block:
            return block
    # No markdown fences — grab everything from first meaningful line to end
    if "def parse" in text:
        lines = text.splitlines()
        start = next((i for i, l in enumerate(lines)
                      if l.strip() and not l.startswith("#")), 0)
        return "\n".join(lines[start:]).strip()
    return ""


class OpenHandsClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def _setup_settings(self, api_key: str) -> None:
        try:
            httpx.post(
                f"{self.base_url}/api/settings",
                json={
                    "llm_model": "gpt-4o-mini",   # must be chat-completions compatible
                    "llm_api_key": api_key,
                },
                timeout=5.0,
            )
        except Exception:
            pass

    def is_available(self) -> bool:
        try:
            r = httpx.get(f"{self.base_url}/api/health", timeout=3.0)
            return r.status_code == 200
        except Exception:
            return False

    async def generate_parser(
        self, html_sample: str, url: str, extra_hint: str = ""
    ) -> str:
        from backend.core.config import get_settings as _get_settings
        cfg = _get_settings()

        self._setup_settings(cfg.openai_api_key)

        prompt = _GENERATE_PROMPT.format(
            url=url,
            schema_json=json.dumps(VENDOR_SCHEMA, indent=2),
            example=SCHEMA_JSON_EXAMPLE,
            html_sample=html_sample[:6000],
        )
        if extra_hint:
            prompt += f"\n\nAdditional hint: {extra_hint}"

        system_instr = (
            "You are a Python web scraping expert. "
            "Output ONLY a valid Python function named parse(html: str) -> list[dict]. "
            "No explanation, no markdown, no test code."
        )

        async with httpx.AsyncClient(timeout=httpx.Timeout(300.0)) as client:
            # Correct schema: InitSessionRequest (additionalProperties: false)
            resp = await client.post(
                f"{self.base_url}/api/conversations",
                json={
                    "initial_user_msg": prompt,
                    "conversation_instructions": system_instr,
                },
            )
            resp.raise_for_status()
            conv_id = resp.json().get("conversation_id", "")
            if not conv_id:
                raise RuntimeError("OpenHands did not return conversation_id")

            logger.info(f"[OH] conversation {conv_id} started for {url}")

            for i in range(15):  # max 30s — fallback cepat ke LLM
                await asyncio.sleep(2)
                events_resp = await client.get(
                    f"{self.base_url}/api/conversations/{conv_id}/events"
                )
                events = events_resp.json().get("events", [])
                kinds = [e.get("kind", e.get("action", "")) for e in events]

                # Detect runtime error (Docker-in-Docker fail, etc.)
                for e in events:
                    err = e.get("error", "") or e.get("message", "")
                    if isinstance(err, str) and any(kw in err for kw in (
                        "docker", "DockerException", "ConnectionError", "runtime"
                    )):
                        raise RuntimeError(f"OpenHands runtime error: {err[:120]}")

                is_done = any(k in ("finish", "agent_finish") for k in kinds)

                if is_done or i % 10 == 9:
                    # Collect all text candidates from all events
                    candidates: list[str] = []
                    for e in events:
                        for field in (
                            "message",          # FinishAction.message
                            "llm_message",      # MessageEvent.llm_message
                            "extended_content", # MessageEvent.extended_content
                            "thought",          # ActionEvent.thought
                            "summary",          # ActionEvent.summary
                        ):
                            val = e.get(field, "")
                            if val and isinstance(val, str) and len(val) > 10:
                                candidates.append(val)
                            elif isinstance(val, list):
                                candidates.extend(str(v) for v in val if v)

                    for text in candidates:
                        if "def parse" in text:
                            code = _extract_code_block(text)
                            if code:
                                logger.info(f"[OH] got parser code ({len(code)} chars)")
                                return code

                if is_done:
                    logger.warning("[OH] agent finished but no parse() found in any event")
                    return ""

        logger.warning(f"[OH] timed out after 4min for {url}")
        return ""
