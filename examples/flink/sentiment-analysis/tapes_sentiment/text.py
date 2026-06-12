"""Text extraction and tokenization utilities for tapes node content."""

from __future__ import annotations

import json
import re
from typing import Iterable

_TOKEN_RE = re.compile(r"[a-z][a-z0-9_'-]*", re.IGNORECASE)


def extract_text(content_json: str | None) -> str:
    """Extract text fields from tapes node bucket content JSON.

    The Kafka event contains node.bucket.content as a JSON array of content blocks. This
    helper keeps the inference library independent from Flink's JSON functions and safely
    handles malformed or non-text content.
    """
    if not content_json:
        return ""

    try:
        content = json.loads(content_json)
    except json.JSONDecodeError:
        return content_json

    if isinstance(content, list):
        return " ".join(_extract_block_text(block) for block in content).strip()

    if isinstance(content, dict):
        return _extract_block_text(content).strip()

    return str(content)


def tokenize(text: str) -> list[str]:
    """Tokenize text into lowercase word features."""
    return [token.lower() for token in _TOKEN_RE.findall(text or "")]


def _extract_block_text(block: object) -> str:
    if not isinstance(block, dict):
        return str(block)

    text_parts: list[str] = []
    for key in ("text", "thinking", "tool_output"):
        value = block.get(key)
        if isinstance(value, str):
            text_parts.append(value)
        elif value is not None:
            text_parts.append(str(value))

    return " ".join(text_parts)


def ngrams(tokens: Iterable[str]) -> list[str]:
    """Return unigram and adjacent bigram features."""
    token_list = list(tokens)
    features = list(token_list)
    features.extend(f"{left} {right}" for left, right in zip(token_list, token_list[1:]))
    return features
