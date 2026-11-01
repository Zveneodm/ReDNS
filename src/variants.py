"""Orthogonal cache cooperation and prefetch capabilities for each model."""

BASELINE_MODELS = ("baseline", "baseline-prefetch")
COOPERATIVE_MODELS = ("ReDNS", "ReDNS-prefetch")
PREFETCH_MODELS = ("baseline-prefetch", "ReDNS-prefetch")
MODELS = (*BASELINE_MODELS, *COOPERATIVE_MODELS)
