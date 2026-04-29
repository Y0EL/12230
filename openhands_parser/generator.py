import json
import re
from urllib.parse import urlparse

from loguru import logger

from openhands_parser.cache import ParserCache
from openhands_parser.executor import SafeExecutor
from openhands_parser.schema import VENDOR_SCHEMA, SCHEMA_JSON_EXAMPLE, validate_parser_output
from openhands_parser.client import _GENERATE_PROMPT, _extract_code_block


def _extract_domain(url: str) -> str:
    try:
        host = urlparse(url).netloc or url
        return host.replace("www.", "").lower()
    except Exception:
        return url[:50]


def _trim_html(html: str, max_chars: int = 5000) -> str:
    import re as _re
    html = _re.sub(r"<script[\s\S]*?</script>", "", html, flags=_re.IGNORECASE)
    html = _re.sub(r"<style[\s\S]*?</style>", "", html, flags=_re.IGNORECASE)
    html = _re.sub(r"<head[\s\S]*?</head>", "", html, flags=_re.IGNORECASE)
    html = _re.sub(r"<!--[\s\S]*?-->", "", html)
    for kw in ("exhibitor", "vendor", "company", "participant", "booth", "sponsor", "listing"):
        m = _re.search(
            rf"<(?:div|section|ul|ol|table|article)[^>]*{kw}[^>]*>",
            html, _re.IGNORECASE
        )
        if m:
            snippet = html[m.start():m.start() + max_chars]
            if len(snippet) > 300:
                return snippet
    for tag in (r"<table\b", r"<ul\b", r"<ol\b"):
        m = _re.search(tag, html, _re.IGNORECASE)
        if m:
            snippet = html[m.start():m.start() + max_chars]
            if len(snippet) > 300:
                return snippet
    body = _re.search(r"<body[\s>]", html, _re.IGNORECASE)
    start = body.start() if body else 0
    return html[start:start + max_chars]


async def _generate_with_llm(html_sample: str, url: str, extra_hint: str = "") -> str:
    from openai import AsyncOpenAI
    from backend.core.config import get_settings

    settings = get_settings()
    client = AsyncOpenAI(api_key=settings.openai_api_key)

    prompt = _GENERATE_PROMPT.format(
        url=url,
        schema_json=json.dumps(VENDOR_SCHEMA, indent=2),
        example=SCHEMA_JSON_EXAMPLE,
        html_sample=_trim_html(html_sample),
    )
    if extra_hint:
        prompt += f"\n\nAdditional hint: {extra_hint}"

    parser_model = settings.openhands_parser_model
    supports_temp = parser_model not in settings.MODELS_NO_TEMPERATURE

    kwargs: dict = dict(
        model=parser_model,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are an expert Python web scraper. "
                    "Output ONLY valid Python code — no markdown, no explanation."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        max_tokens=2000,
    )
    if supports_temp:
        kwargs["temperature"] = 0.0

    response = await client.chat.completions.create(**kwargs)
    raw = response.choices[0].message.content or ""

    code = _extract_code_block(raw)
    if not code and "def parse" in raw:
        lines = raw.splitlines()
        start = next((i for i, l in enumerate(lines) if l.strip() and not l.startswith("#")), 0)
        code = "\n".join(lines[start:]).strip()
    if not code:
        logger.debug(f"[LLM] raw ({len(raw)} chars): {raw[:300]!r}")
    return code


class ParserGenerator:
    def __init__(self):
        self.cache = ParserCache()
        self.executor = SafeExecutor()

    async def generate_and_run(self, url: str, html: str, hint: str = "") -> dict:
        domain = _extract_domain(url)

        cached_code = self.cache.get(domain)
        if cached_code:
            try:
                vendors = self.executor.run(cached_code, html, url)
                if vendors:
                    self.cache.bump_success(domain)
                    logger.info(f"[GEN] cache hit {domain} -> {len(vendors)} vendors")
                    return {"vendors": vendors, "cache_hit": True, "domain": domain}
            except Exception as e:
                logger.warning(f"[GEN] cached parser broken for {domain}: {e}")
                self.cache.invalidate(domain)

        last_error = ""
        for attempt in range(3):
            extra = hint
            if last_error:
                extra = f"Previous attempt failed: {last_error}. Fix and regenerate. {hint}"

            try:
                code = await _generate_with_llm(html, url, extra)

                if not code or "def parse" not in code:
                    last_error = "Generated code has no def parse() function"
                    logger.warning(f"[GEN] attempt {attempt+1}: {last_error}")
                    continue

                vendors = self.executor.run(code, html, url)
                is_valid, err = validate_parser_output(vendors, url)

                if is_valid:
                    self.cache.save(domain, code, {"attempt": attempt + 1})
                    logger.info(f"[GEN] cached parser for {domain} -> {len(vendors)} vendors")
                    return {"vendors": vendors, "cache_hit": False, "domain": domain}

                last_error = err
                logger.warning(f"[GEN] attempt {attempt+1} invalid: {err}")

            except Exception as e:
                last_error = str(e)[:300]
                logger.warning(f"[GEN] attempt {attempt+1} error: {last_error}")

        logger.error(f"[GEN] all 3 attempts failed for {domain}: {last_error}")
        return {"vendors": [], "cache_hit": False, "domain": domain}
