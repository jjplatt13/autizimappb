"""
Controlled vocabulary for service normalization.

Purpose:
- Centralize normalization of service search terms
- Support Option A (lightweight passthrough)
- Prepare for Option B (canonical mapping + fuzzy matching)

Safe to import even if only normalize_query() is used.
"""

from __future__ import annotations
from typing import Dict, List


CONTROLLED_TERMS: Dict[str, List[str]] = {
    "aba": [
        "aba",
        "applied behavior analysis",
        "behavior",
        "behavioral therapy",
    ],
    "speech": [
        "speech therapy",
        "speech",
        "slp",
        "language",
        "speech-language pathology",
    ],
    "ot": [
        "occupational therapy",
        "occupational",
        "ot",
    ],
    "pt": [
        "physical therapy",
        "physical",
        "pt",
    ],
}


def normalize_query(query: str) -> str:
    """
    Normalize a raw search query.

    Option A:
    - Simple lowercase + strip passthrough

    Option B (future):
    - Fuzzy matching
    - Synonym expansion
    - Locale-aware normalization
    """
    return (query or "").strip().lower()


def canonical_service(query: str) -> str:
    """
    Convert a query into a canonical service key if possible.

    Examples:
        "slp" -> "speech"
        "Applied Behavior Analysis" -> "aba"

    If no canonical match is found, returns the normalized query.
    """
    q = normalize_query(query)
    if not q:
        return ""

    for canonical, aliases in CONTROLLED_TERMS.items():
        if q == canonical:
            return canonical

        for alias in aliases:
            alias_norm = alias.strip().lower()
            if q == alias_norm:
                return canonical

    return q
