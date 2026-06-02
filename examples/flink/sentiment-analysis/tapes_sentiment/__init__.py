"""Tiny sentiment inference helpers for the Flink demo."""

from .model import MODEL_NAME, MODEL_VERSION, SentimentPrediction, predict_sentiment

__all__ = [
    "MODEL_NAME",
    "MODEL_VERSION",
    "SentimentPrediction",
    "predict_sentiment",
]
