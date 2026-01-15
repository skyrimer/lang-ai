"""
Evaluation module for deleaking methodology assessment.

Provides tools for comparing baseline and sanitized corpora through:
- Geometric analysis (centroid distances, silhouette scores)
- Stylometric analysis (function words, punctuation, adverbs)
- Classification performance metrics
- Feature comparison
"""

from .deleaking_evaluator import DeleakingEvaluator, EvaluationConfig, EvaluationResults
from .metrics import (
    FeatureComparator,
    GeometricAnalyzer,
    StylometricAnalyzer,
)

__all__ = [
    "DeleakingEvaluator",
    "EvaluationConfig",
    "EvaluationResults",
    "GeometricAnalyzer",
    "StylometricAnalyzer",
    "FeatureComparator",
]
