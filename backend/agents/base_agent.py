from abc import ABC, abstractmethod
from typing import Any, Optional
from langchain_core.tools import BaseTool
from langchain_openai import ChatOpenAI
from loguru import logger

from backend.core.config import get_settings


class BaseAgent(ABC):
    agent_name: str = "base"

    def __init__(
        self,
        tools: Optional[list[BaseTool]] = None,
        llm: Optional[ChatOpenAI] = None,
        verbose: bool = True,
    ) -> None:
        self._settings = get_settings()
        self._tools = tools or []
        self._tool_map = {t.name: t for t in self._tools}
        self._verbose = verbose
        self._llm = llm or self._build_llm()

    def _build_llm(self) -> Optional[ChatOpenAI]:
        if not self._settings.has_openai_key:
            return None
        kwargs = {
            "model": self._settings.openai_model,
            "api_key": self._settings.openai_api_key,
            "max_retries": 2,
        }
        if self._settings.model_supports_temperature:
            kwargs["temperature"] = self._settings.openai_temperature
        return ChatOpenAI(**kwargs)

    def _call_tool(self, tool_name: str, **kwargs) -> Any:
        tool = self._tool_map.get(tool_name)
        if not tool:
            raise ValueError(f"Tool '{tool_name}' not registered in {self.agent_name}")
        if self._verbose:
            logger.debug(f"[{self.agent_name}] calling tool: {tool_name}")
        return tool.invoke(kwargs)

    def add_tool(self, tool: BaseTool) -> None:
        self._tools.append(tool)
        self._tool_map[tool.name] = tool

    @property
    def tool_names(self) -> list[str]:
        return list(self._tool_map.keys())

    @abstractmethod
    def run(self, input_data: Any) -> Any:
        pass
