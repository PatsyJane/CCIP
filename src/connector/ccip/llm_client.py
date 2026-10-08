"""Gemini LLM client adapter (design component ``llm_client.py``).

Provides :class:`GeminiLLMClient`, a concrete implementation of the
:class:`connector.ccip.narrative.LLMClient` protocol
(``async def complete(self, prompt: str) -> str``) that wraps LangChain's
``langchain_google_genai.ChatGoogleGenerativeAI``.

The LangChain Google integration knows Google's endpoint internally, so there is
no base URL to configure — the client is built from the model id plus the API
key. Imports are lazy so merely importing this module requires neither the
LangChain package nor credentials. The adapter raises on failure (auth / network
/ empty response); the narrative stage wraps the call with a timeout and maps any
failure to a deterministic templated narrative (Req 9.6).

Requirements: 9.4.
"""

from __future__ import annotations

from typing import Any, Optional

__all__ = ["GeminiLLMClient"]


class GeminiLLMClient:
    """Concrete LLM client backed by LangChain ``ChatGoogleGenerativeAI``."""

    def __init__(
        self,
        api_key: str,
        model: str,
        *,
        chat_model: Optional[Any] = None,
    ) -> None:
        self._api_key = api_key
        self._model = model
        self._chat_model = chat_model

    def _ensure_chat_model(self) -> Any:
        if self._chat_model is None:
            from langchain_google_genai import ChatGoogleGenerativeAI

            self._chat_model = ChatGoogleGenerativeAI(
                model=self._model,
                google_api_key=self._api_key,
            )
        return self._chat_model

    async def complete(self, prompt: str) -> str:
        """Send ``prompt`` to the model and return the response text.

        Raises on any failure (the LangChain client raises on auth/transport
        errors; an empty response raises :class:`ValueError`). The narrative stage
        maps any raised exception to its deterministic templated fallback.
        """
        chat_model = self._ensure_chat_model()
        message = await chat_model.ainvoke(prompt)
        return self._extract_text(message)

    @staticmethod
    def _extract_text(message: Any) -> str:
        content = getattr(message, "content", None)
        if isinstance(content, str):
            text = content
        elif isinstance(content, list):
            parts: list[str] = []
            for block in content:
                if isinstance(block, str):
                    parts.append(block)
                elif isinstance(block, dict) and isinstance(block.get("text"), str):
                    parts.append(block["text"])
            text = "".join(parts)
        else:
            text = ""
        if not text.strip():
            raise ValueError("LLM response content was empty or not text")
        return text
