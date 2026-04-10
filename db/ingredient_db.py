"""Built-in ingredient concern database loader and lookup engine."""

import json
import os
import re
from typing import Optional

_DB_PATH = os.path.join(os.path.dirname(__file__), "ingredients.json")

# Loaded once at import time
_INGREDIENTS: dict = {}


def _load() -> None:
    global _INGREDIENTS
    with open(_DB_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    _INGREDIENTS = {k: v for k, v in data.items() if not k.startswith("_")}


_load()


def _normalize(name: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    name = name.lower()
    name = re.sub(r"[^a-z0-9 \-/]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def lookup(name: str) -> Optional[dict]:
    """Return the ingredient record for the given name, or None if not found."""
    normalized = _normalize(name)
    # Direct key match
    if normalized in _INGREDIENTS:
        return _INGREDIENTS[normalized]
    # Search aliases
    for key, record in _INGREDIENTS.items():
        aliases = [_normalize(a) for a in record.get("aliases", [])]
        if normalized in aliases:
            return record
        # Partial match: ingredient list name contains a flagged key
        if normalized == key or key in normalized:
            return record
    return None


def scan_ingredients(ingredient_list: list[str]) -> list[tuple[str, dict]]:
    """
    Scan a list of ingredient names against the built-in database.
    Returns a list of (original_name, record) tuples for ingredients with
    at least one concern (score > 1 or concerns list non-empty).
    """
    matches = []
    seen_keys = set()
    for ing in ingredient_list:
        record = lookup(ing)
        if record:
            # Only flag if there are actual concerns
            if not record.get("concerns") and record.get("score", 0) <= 1:
                continue
            key = record["name"]
            if key not in seen_keys:
                seen_keys.add(key)
                matches.append((ing, record))
    return matches
