"""
Text normalization functions for entity data quality.

Normalizes keywords, requirements, money strings, and dates to reduce
graph bloat and improve consistency.
"""

import re
import string
from typing import Any, Callable


# Common English stopwords for filtering junk tokens
STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
    "has", "he", "in", "is", "it", "its", "of", "on", "that", "the",
    "to", "was", "will", "with", "the", "this", "but", "they", "have",
    "had", "what", "when", "where", "who", "which", "why", "how"
}


def normalize_text(text: str, min_length: int = 3) -> str | None:
    """
    Normalize a text string for consistent entity matching.

    Steps:
    1. Lowercase
    2. Strip whitespace
    3. Remove most punctuation (keep hyphens, underscores, slashes)
    4. Filter if too short or is a stopword

    Args:
        text: Input text to normalize
        min_length: Minimum length for valid tokens (default: 3)

    Returns:
        Normalized text or None if filtered out
    """
    if not text or not isinstance(text, str):
        return None

    # Lowercase and strip
    normalized = text.lower().strip()

    # Remove punctuation except hyphens, underscores, slashes, and spaces
    # This preserves compound terms like "e-system" or "audio/video"
    allowed_chars = string.ascii_lowercase + string.digits + " -_/"
    normalized = "".join(c for c in normalized if c in allowed_chars)

    # Collapse multiple spaces
    normalized = " ".join(normalized.split())

    # Filter short tokens
    if len(normalized) < min_length:
        return None

    # Filter stopwords (single-word check)
    if " " not in normalized and normalized in STOPWORDS:
        return None

    return normalized


def normalize_keyword(keyword: str | dict[str, Any]) -> str | None:
    """
    Normalize a keyword entity.

    Handles both string and dict (with 'text' key) formats from GLiNER2.

    Args:
        keyword: Keyword as string or dict with 'text' key

    Returns:
        Normalized keyword or None if filtered
    """
    # Handle dict format from GLiNER2
    text: str
    if isinstance(keyword, dict):
        text = keyword.get("text", "")
    else:
        text = keyword

    return normalize_text(text, min_length=3)


def normalize_requirement(requirement: str | dict[str, Any]) -> str | None:
    """
    Normalize a requirement entity.

    Handles both string and dict (with 'text' key) formats from GLiNER2.

    Args:
        requirement: Requirement as string or dict with 'text' key

    Returns:
        Normalized requirement or None if filtered
    """
    # Handle dict format from GLiNER2
    text: str
    if isinstance(requirement, dict):
        text = requirement.get("text", "")
    else:
        text = requirement

    return normalize_text(text, min_length=3)


def normalize_money(money_str: str | int | float) -> float | None:
    """
    Normalize money strings to numeric values.

    Handles:
    - String representations: "$1,234.56", "1234.56", "1,234"
    - Already numeric values: 1234, 1234.56

    Args:
        money_str: Money value as string, int, or float

    Returns:
        Normalized float value or None if invalid
    """
    if money_str is None:
        return None

    # Already numeric
    if isinstance(money_str, (int, float)):
        return float(money_str)

    if not isinstance(money_str, str):
        return None

    # Remove common currency symbols and whitespace
    cleaned = money_str.strip().replace("$", "").replace(",", "").replace(" ", "")

    # Try to convert to float
    try:
        return float(cleaned)
    except ValueError:
        return None


def normalize_date(date_str: str) -> str | None:
    """
    Normalize date strings to a consistent format.

    This is a best-effort normalization. It attempts to standardize
    common date formats but doesn't parse all possible variations.

    Supported formats:
    - DD/MM/YYYY (e.g., "10/11/2020")
    - MM/DD/YYYY (ambiguous, treated as DD/MM/YYYY per Singapore convention)
    - YYYY-MM-DD (ISO format)

    Args:
        date_str: Date string to normalize

    Returns:
        Normalized date in YYYY-MM-DD format, or original if not recognized
    """
    if not date_str or not isinstance(date_str, str):
        return None

    date_str = date_str.strip()

    # Already in ISO format (YYYY-MM-DD)
    if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        return date_str

    # DD/MM/YYYY format (Singapore convention)
    match = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", date_str)
    if match:
        day, month, year = match.groups()
        try:
            # Validate ranges
            day_int = int(day)
            month_int = int(month)
            if 1 <= day_int <= 31 and 1 <= month_int <= 12:
                return f"{year}-{month_int:02d}-{day_int:02d}"
        except ValueError:
            pass

    # Return original if we can't normalize
    return date_str


def normalize_entity_list(
    entities: list[str | dict[str, Any]],
    normalize_func: Callable[[str | dict[str, Any]], str | None],
    dedupe: bool = True
) -> list[str]:
    """
    Normalize a list of entities using a normalization function.

    Args:
        entities: List of entities (strings or dicts)
        normalize_func: Function to apply to each entity
        dedupe: Whether to deduplicate after normalization (default: True)

    Returns:
        List of normalized, non-None entities
    """
    normalized = []
    for entity in entities:
        norm = normalize_func(entity)
        if norm is not None:
            normalized.append(norm)

    # Deduplicate while preserving order
    if dedupe:
        seen = set()
        deduped = []
        for item in normalized:
            if item not in seen:
                seen.add(item)
                deduped.append(item)
        return deduped

    return normalized


def cap_entity_list(
    entities: list[str],
    max_count: int
) -> list[str]:
    """
    Cap the number of entities to reduce graph bloat.

    Takes the first N entities after normalization and deduplication.
    This reduces the number of nodes and edges in the graph while
    preserving the most relevant entities.

    Args:
        entities: List of normalized entities
        max_count: Maximum number of entities to keep

    Returns:
        Capped list of entities
    """
    if max_count <= 0:
        return entities
    return entities[:max_count]


def normalize_entities_in_extraction(
    extraction: dict[str, Any],
    max_keywords: int = 10,
    max_requirements: int = 10
) -> dict[str, Any]:
    """
    Normalize all entity lists in an extraction result.

    Applies normalization to Keyword and Requirement entities.
    Other entity types (Tender, Agency, Supplier, etc.) are left unchanged
    as they need to match source data exactly for traceability.

    Args:
        extraction: Extraction dict with 'entities' key
        max_keywords: Maximum number of keywords to keep per tender (default: 10)
        max_requirements: Maximum number of requirements to keep per tender (default: 10)

    Returns:
        Extraction dict with normalized entities
    """
    if "entities" not in extraction:
        return extraction

    entities = extraction["entities"]

    # Normalize keywords
    if "Keyword" in entities:
        normalized = normalize_entity_list(
            entities["Keyword"],
            normalize_keyword,
            dedupe=True
        )
        entities["Keyword"] = cap_entity_list(normalized, max_keywords)

    # Normalize requirements
    if "Requirement" in entities:
        normalized = normalize_entity_list(
            entities["Requirement"],
            normalize_requirement,
            dedupe=True
        )
        entities["Requirement"] = cap_entity_list(normalized, max_requirements)

    # Normalize money field if present in source data
    if "awarded_amt" in extraction:
        normalized_amt = normalize_money(extraction["awarded_amt"])
        if normalized_amt is not None:
            extraction["awarded_amt_normalized"] = normalized_amt

    # Normalize date field if present
    if "award_date" in extraction:
        normalized_date = normalize_date(extraction["award_date"])
        if normalized_date is not None:
            extraction["award_date_normalized"] = normalized_date

    return extraction
