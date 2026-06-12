"""A tiny pure-Python sentiment model for the Flink demo.

This intentionally avoids heavyweight ML dependencies. The model trains a multinomial
Naive Bayes classifier at import time from a compact in-repo dataset, then uses it for
streaming inference inside PyFlink Python UDF workers.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
import json
import math
from typing import Iterable

from .text import extract_text, ngrams, tokenize

MODEL_NAME = "tiny-naive-bayes-sentiment"
MODEL_VERSION = "v1"

_TRAINING_DATA: tuple[tuple[str, str], ...] = (
    ("thanks this is great and really helpful", "positive"),
    ("thank you that worked perfectly", "positive"),
    ("excellent answer this solved my problem", "positive"),
    ("awesome support I love this", "positive"),
    ("happy with the result and the guidance", "positive"),
    ("perfect that is exactly what I needed", "positive"),
    ("this is terrible and broken", "negative"),
    ("I hate this it failed again", "negative"),
    ("bad answer I am frustrated", "negative"),
    ("awful result and the bug is worse", "negative"),
    ("this error makes me angry", "negative"),
    ("the system failed and nothing works", "negative"),
    ("please summarize this conversation", "neutral"),
    ("what is the current status", "neutral"),
    ("show me the available options", "neutral"),
    ("can you explain how this works", "neutral"),
    ("please remember this value for later", "neutral"),
    ("tell me about kafka and flink", "neutral"),
)


@dataclass(frozen=True)
class SentimentPrediction:
    label: str
    confidence: float
    scores: dict[str, float]
    text: str

    def scores_json(self) -> str:
        return json.dumps(self.scores, sort_keys=True, separators=(",", ":"))


class TinyNaiveBayesSentimentModel:
    def __init__(self, training_data: Iterable[tuple[str, str]]) -> None:
        self._label_counts: Counter[str] = Counter()
        self._feature_counts: dict[str, Counter[str]] = defaultdict(Counter)
        self._total_features: Counter[str] = Counter()
        self._vocabulary: set[str] = set()

        for text, label in training_data:
            self._label_counts[label] += 1
            features = ngrams(tokenize(text))
            self._feature_counts[label].update(features)
            self._total_features[label] += len(features)
            self._vocabulary.update(features)

        self._labels = tuple(sorted(self._label_counts))
        self._training_rows = sum(self._label_counts.values())
        self._vocabulary_size = max(len(self._vocabulary), 1)

    def predict(self, content_json: str | None) -> SentimentPrediction:
        text = extract_text(content_json)
        features = ngrams(tokenize(text))
        if not features:
            scores = {label: 0.0 for label in self._labels}
            scores["neutral"] = 1.0
            return SentimentPrediction("neutral", 1.0, scores, text)

        log_scores = {label: self._log_probability(label, features) for label in self._labels}
        scores = _softmax(log_scores)
        label = max(scores, key=scores.get)

        # Avoid overclaiming confidence for a tiny demo model.
        confidence = min(scores[label], 0.99)
        return SentimentPrediction(label, round(confidence, 4), scores, text)

    def _log_probability(self, label: str, features: list[str]) -> float:
        class_prior = (self._label_counts[label] + 1) / (self._training_rows + len(self._labels))
        total = self._total_features[label] + self._vocabulary_size
        score = math.log(class_prior)

        for feature in features:
            count = self._feature_counts[label][feature]
            score += math.log((count + 1) / total)

        return score


def _softmax(log_scores: dict[str, float]) -> dict[str, float]:
    max_score = max(log_scores.values())
    exp_scores = {label: math.exp(score - max_score) for label, score in log_scores.items()}
    total = sum(exp_scores.values()) or 1.0
    return {label: round(value / total, 4) for label, value in exp_scores.items()}


_MODEL = TinyNaiveBayesSentimentModel(_TRAINING_DATA)


def predict_sentiment(content_json: str | None) -> SentimentPrediction:
    return _MODEL.predict(content_json)
