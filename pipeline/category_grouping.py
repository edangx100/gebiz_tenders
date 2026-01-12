"""
LLM-based category grouping for tender titles.

Maps verbose tender category titles to a stable set of high-level groups
using the OpenAI API with a local cache to avoid repeat calls.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OpenAI = None  # type: ignore
    OPENAI_AVAILABLE = False

from pipeline.config import OpenAIConfig


DEFAULT_CATEGORY_GROUPS = [
    "IT Services & Software",
    "Construction & Engineering",
    "Facilities Management & Maintenance",
    "Professional Services",
    "Legal & Compliance",
    "Security",
    "Training & Education",
    "Healthcare",
    "Logistics & Transport",
    "Audit & Assurance",
    "Research & Advisory",
    "Marketing & Communications",
    "Environmental Services",
    "Finance & HR Services",
    "Other",
]


@dataclass
class CategoryGroupResult:
    """Parsed LLM response for category grouping."""
    group: str
    rationale: str | None = None


class CategoryGrouper:
    """Classify tender category titles into high-level groups with caching."""

    def __init__(
        self,
        config: OpenAIConfig,
        cache_path: Path,
        allowed_groups: list[str] | None = None,
    ) -> None:
        if not OPENAI_AVAILABLE:
            raise ImportError("openai package is not installed. Install with: pip install openai")

        # Keep an on-disk cache to reduce API calls and stabilize outputs.
        self._cache_path = cache_path
        self._cache: dict[str, dict[str, Any]] = self._load_cache()
        self._allowed_groups = allowed_groups or DEFAULT_CATEGORY_GROUPS

        self._client = OpenAI(
            api_key=config.api_key,
            timeout=config.timeout_seconds,
            max_retries=config.max_retries,
        )
        self._model = config.model
        self._temperature = config.temperature

    def classify(self, category_name: str) -> CategoryGroupResult:
        """Return a category group for the provided tender category title."""
        normalized = category_name.strip()
        if not normalized:
            return CategoryGroupResult(group="Other")

        cached = self._cache.get(normalized)
        if cached and isinstance(cached, dict) and cached.get("group"):
            return CategoryGroupResult(group=str(cached["group"]), rationale=cached.get("rationale"))

        result = self._classify_with_openai(normalized)
        self._cache[normalized] = {
            "group": result.group,
            "rationale": result.rationale,
            "model": self._model,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._write_cache()
        return result

    def _classify_with_openai(self, category_name: str) -> CategoryGroupResult:
        """Call the OpenAI API and parse a structured group response."""
        system_prompt = (
            "You classify procurement tender titles into a single high-level group. "
            "Respond with JSON only."
        )
        user_prompt = (
            "Select exactly one group from the list below for the given tender category title.\n"
            f"Groups: {', '.join(self._allowed_groups)}\n"
            f"Category title: {category_name}\n"
            'Return JSON like: {"group": "...", "rationale": "..."}'
        )

        response = self._client.chat.completions.create(
            model=self._model,
            temperature=self._temperature,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        content = response.choices[0].message.content or ""
        parsed = self._parse_json_response(content)
        group = self._normalize_group(parsed.get("group", "Other"))
        rationale = parsed.get("rationale")
        if rationale is not None:
            rationale = str(rationale).strip()
        return CategoryGroupResult(group=group, rationale=rationale or None)

    def _normalize_group(self, group: str) -> str:
        """Force the output to one of the allowed groups."""
        candidate = str(group).strip()
        for allowed in self._allowed_groups:
            if candidate.lower() == allowed.lower():
                return allowed
        return "Other"

    def _parse_json_response(self, content: str) -> dict[str, Any]:
        """Extract the JSON object from the LLM response."""
        try:
            result: dict[str, Any] = json.loads(content)
            return result
        except json.JSONDecodeError:
            # Some models add prose; extract the first JSON object defensively.
            match = re.search(r"\{.*\}", content, re.DOTALL)
            if match:
                result: dict[str, Any] = json.loads(match.group(0))  # type: ignore[no-redef]
                return result
        return {"group": "Other", "rationale": "Parsing failed"}

    def _load_cache(self) -> dict[str, dict[str, Any]]:
        if not self._cache_path.exists():
            return {}
        try:
            result: dict[str, dict[str, Any]] = json.loads(self._cache_path.read_text())
            return result
        except json.JSONDecodeError:
            return {}

    def _write_cache(self) -> None:
        self._cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._cache_path.write_text(json.dumps(self._cache, indent=2, sort_keys=True))
