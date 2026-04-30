import os
import sys
from pathlib import Path
from typing import Optional
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from loguru import logger


BASE_DIR = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        env_ignore_empty=True,
    )

    openai_api_key: str = Field(default="", description="OpenAI API key")
    openai_model: str = Field(default="gpt-5-nano", description="OpenAI model for extraction fallback")
    openai_temperature: float = Field(default=0.0, ge=0.0, le=2.0)

    jina_api_key: str = Field(default="", description="Jina AI Reader API key")
    tavily_api_key: str = Field(default="", description="Tavily Search API key")
    firecrawl_api_key: str = Field(default="", description="Firecrawl API key (fc-...)")
    firecrawl_base_url: str = Field(default="https://api.firecrawl.dev", description="Firecrawl API base URL")

    max_concurrent_requests: int = Field(default=20, ge=1, le=100)
    max_depth: int = Field(default=3, ge=1, le=10)
    batch_size: int = Field(default=500, ge=10, le=5000)
    request_timeout: int = Field(default=30, ge=5, le=120)
    request_delay_min: float = Field(default=0.5, ge=0.0, le=10.0)
    request_delay_max: float = Field(default=2.0, ge=0.0, le=30.0)
    max_retries: int = Field(default=3, ge=0, le=10)

    llm_fallback_enabled: bool = Field(default=True)
    llm_max_input_chars: int = Field(default=1500, ge=500, le=8000)
    llm_supervisor_enabled: bool = Field(default=True)
    llm_error_threshold: int = Field(default=10, ge=1, le=100)
    llm_supervisor_context_lines: int = Field(default=50, ge=10, le=200)

    output_dir: str = Field(default="./output")
    log_level: str = Field(default="INFO")
    log_file: str = Field(default="./output/crawler.log")

    playwright_headless: bool = Field(default=True)
    playwright_timeout: int = Field(default=30000, ge=5000, le=120000)
    playwright_max_browsers: int = Field(default=5, ge=1, le=20)

    proxy_enabled: bool = Field(default=False)
    proxy_list: list[str] = Field(default=[])
    proxy_rotate: bool = Field(default=True)

    # Stealth / camouflage settings
    stealth_enabled: bool = Field(default=True, description="Enable playwright-stealth + browserforge fingerprinting")
    sticky_proxy_per_domain: bool = Field(default=True, description="Reuse same proxy for same domain within a session")
    scroll_to_bottom_enabled: bool = Field(default=True, description="Simulate infinite scroll in Playwright")
    scroll_steps: int = Field(default=5, ge=1, le=20, description="Number of scroll steps per page")

    # OpenAI web search enrichment
    openai_websearch_enrichment: bool = Field(default=True, description="Use OpenAI Responses API web_search to enrich missing vendor fields")

    @field_validator("proxy_list", mode="before")
    @classmethod
    def parse_proxy_list(cls, v):
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return []
            import json as _json
            try:
                parsed = _json.loads(v)
                return parsed if isinstance(parsed, list) else [v]
            except Exception:
                return [x.strip() for x in v.split(",") if x.strip()]
        return []

    openserp_base_url: str = Field(default="http://localhost:7000")
    openserp_enabled: bool = Field(default=True)

    openhands_base_url: str = Field(default="http://localhost:3000")
    openhands_parser_enabled: bool = Field(default=True)
    openhands_parser_model: str = Field(default="gpt-4o-mini", description="Model khusus untuk generate parser code")

    min_vendor_fields: int = Field(default=7, description="Min fields for extraction to be considered success (raised from 3 to avoid accepting incomplete vendors)")
    vendor_dedup_fields: list[str] = Field(default=["name", "website"])
    max_vendors_per_event: int = Field(default=2000)
    max_total_vendors: int = Field(default=10000)

    exhibitor_url_keywords: list[str] = Field(default=[
        "exhibitor", "vendor", "sponsor", "booth", "company", "participant",
        "member", "partner", "directory", "showcase", "floor-plan", "floorplan",
        "hall", "pavilion", "stand", "profile", "listing", "attendee",
        "solution-provider", "solution_provider", "tech-provider",
    ])

    event_search_suffixes: list[str] = Field(default=[
        "exhibition", "expo", "conference", "summit", "congress", "tradeshow",
        "trade show", "forum", "symposium", "convention", "fair",
    ])

    ignored_domains: set[str] = Field(default={
        "google.com", "facebook.com", "twitter.com", "linkedin.com",
        "youtube.com", "instagram.com", "wikipedia.org", "amazon.com",
        "microsoft.com", "apple.com", "github.com",
    })

    ignored_extensions: set[str] = Field(default={
        ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
        ".zip", ".rar", ".tar", ".gz", ".exe", ".dmg", ".pkg",
        ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico", ".webp",
        ".mp4", ".mp3", ".avi", ".mov", ".wmv", ".flv",
        ".css", ".js", ".json", ".xml", ".txt", ".csv",
    })

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v = v.upper()
        if v not in valid:
            raise ValueError(f"log_level must be one of {valid}")
        return v

    MODELS_NO_TEMPERATURE: set[str] = {
        "gpt-5-nano", "gpt-5-mini", "gpt-5-mini-2025-08-07",
        "gpt-5", "gpt-5-2025-06-05",
        "o1", "o1-mini", "o1-preview", "o3", "o3-mini", "o4-mini",
    }

    @field_validator("openai_model")
    @classmethod
    def validate_model(cls, v: str) -> str:
        valid_models = {
            "gpt-4o", "gpt-4o-mini", "gpt-4-turbo", "gpt-3.5-turbo",
            "gpt-4o-2024-08-06", "gpt-4o-mini-2024-07-18",
            "gpt-5-nano", "gpt-5-mini", "gpt-5-mini-2025-08-07",
            "gpt-5", "gpt-5-2025-06-05",
            "o1", "o1-mini", "o1-preview", "o3", "o3-mini", "o4-mini",
        }
        if v not in valid_models:
            logger.warning(f"Model '{v}' not in known list — proceeding anyway")
        return v

    @property
    def model_supports_temperature(self) -> bool:
        return self.openai_model not in self.MODELS_NO_TEMPERATURE

    @model_validator(mode="after")
    def validate_delay_range(self) -> "Settings":
        if self.request_delay_min > self.request_delay_max:
            raise ValueError("request_delay_min must be <= request_delay_max")
        return self

    @property
    def output_path(self) -> Path:
        p = Path(self.output_dir)
        if not p.is_absolute():
            p = BASE_DIR / p
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def log_file_path(self) -> Path:
        p = Path(self.log_file)
        if not p.is_absolute():
            p = BASE_DIR / p
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def has_openai_key(self) -> bool:
        return bool(self.openai_api_key and self.openai_api_key != "your_openai_api_key_here")

    @property
    def has_jina_key(self) -> bool:
        return bool(self.jina_api_key and self.jina_api_key not in ("", "your_jina_api_key_here"))

    @property
    def has_firecrawl_key(self) -> bool:
        return bool(self.firecrawl_api_key and self.firecrawl_api_key not in ("", "your_firecrawl_api_key_here"))

    @property
    def effective_llm_enabled(self) -> bool:
        return self.llm_fallback_enabled and self.has_openai_key

    def exhibitor_pattern_score(self, url: str, link_text: str = "") -> int:
        score = 0
        combined = (url + " " + link_text).lower()
        for keyword in self.exhibitor_url_keywords:
            if keyword in combined:
                score += 2 if keyword in url.lower() else 1
        return score

    def is_ignored_extension(self, url: str) -> bool:
        lower = url.lower().split("?")[0]
        return any(lower.endswith(ext) for ext in self.ignored_extensions)

    def is_ignored_domain(self, domain: str) -> bool:
        domain = domain.lower().strip()
        return any(domain == d or domain.endswith("." + d) for d in self.ignored_domains)


def setup_logging(settings: Settings) -> None:
    logger.remove()

    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{line}</cyan> — "
        "<level>{message}</level>"
    )

    logger.add(
        sys.stderr,
        format=log_format,
        level="INFO",
        colorize=True,
        backtrace=False,
        diagnose=False,
    )

    logger.add(
        str(settings.log_file_path),
        format=log_format,
        level="DEBUG",
        rotation="10 MB",
        retention="7 days",
        compression="zip",
        backtrace=True,
        diagnose=True,
        encoding="utf-8",
    )

    logger.info(f"Logging initialized — level={settings.log_level}, file={settings.log_file_path}")


_settings_instance: Optional[Settings] = None


def get_settings() -> Settings:
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = Settings()
        setup_logging(_settings_instance)
        _validate_environment(_settings_instance)
    return _settings_instance


def _validate_environment(settings: Settings) -> None:
    warnings = []
    errors = []

    if not settings.has_openai_key:
        if settings.llm_fallback_enabled:
            warnings.append(
                "OPENAI_API_KEY not set — LLM fallback disabled. "
                "Set LLM_FALLBACK_ENABLED=false to suppress this warning."
            )
            settings.llm_fallback_enabled = False
            settings.llm_supervisor_enabled = False

    output_path = settings.output_path
    if not os.access(str(output_path), os.W_OK):
        errors.append(f"Output directory not writable: {output_path}")

    for w in warnings:
        logger.warning(f"[CONFIG] {w}")

    for e in errors:
        logger.error(f"[CONFIG] {e}")

    if errors:
        raise RuntimeError(f"Configuration errors: {'; '.join(errors)}")

    logger.info(
        f"[CONFIG] Settings loaded — model={settings.openai_model}, "
        f"concurrent={settings.max_concurrent_requests}, "
        f"depth={settings.max_depth}, "
        f"llm_fallback={settings.effective_llm_enabled}"
    )


class CrawlStats:
    def __init__(self) -> None:
        self.total_crawled: int = 0
        self.total_vendor_pages: int = 0
        self.total_vendors_extracted: int = 0
        self.extraction_schema_org: int = 0
        self.extraction_rule_based: int = 0
        self.extraction_llm: int = 0
        self.extraction_failed: int = 0
        self.total_errors: int = 0
        self.llm_tokens_input: int = 0
        self.llm_tokens_output: int = 0
        self.domains_crawled: set[str] = set()
        self.events_found: set[str] = set()
        self.countries_found: set[str] = set()
        self.elapsed_seconds: float = 0.0

    @property
    def llm_call_count(self) -> int:
        return self.extraction_llm

    @property
    def llm_percentage(self) -> float:
        if self.total_vendors_extracted == 0:
            return 0.0
        return (self.extraction_llm / self.total_vendors_extracted) * 100

    @property
    def success_rate(self) -> float:
        total = self.total_vendors_extracted + self.extraction_failed
        if total == 0:
            return 0.0
        return (self.total_vendors_extracted / total) * 100

    @property
    def estimated_llm_cost_usd(self) -> float:
        input_cost = (self.llm_tokens_input / 1_000_000) * 0.15
        output_cost = (self.llm_tokens_output / 1_000_000) * 0.60
        return input_cost + output_cost

    @property
    def requests_per_second(self) -> float:
        if self.elapsed_seconds == 0:
            return 0.0
        return self.total_crawled / self.elapsed_seconds

    def to_dict(self) -> dict:
        return {
            "total_crawled": self.total_crawled,
            "total_vendor_pages": self.total_vendor_pages,
            "total_vendors_extracted": self.total_vendors_extracted,
            "extraction_schema_org": self.extraction_schema_org,
            "extraction_rule_based": self.extraction_rule_based,
            "extraction_llm": self.extraction_llm,
            "extraction_failed": self.extraction_failed,
            "total_errors": self.total_errors,
            "llm_tokens_total": self.llm_tokens_input + self.llm_tokens_output,
            "estimated_llm_cost_usd": round(self.estimated_llm_cost_usd, 4),
            "llm_percentage": round(self.llm_percentage, 1),
            "success_rate": round(self.success_rate, 1),
            "domains_crawled": len(self.domains_crawled),
            "events_found": len(self.events_found),
            "countries_found": len(self.countries_found),
            "elapsed_seconds": round(self.elapsed_seconds, 1),
            "requests_per_second": round(self.requests_per_second, 1),
        }
