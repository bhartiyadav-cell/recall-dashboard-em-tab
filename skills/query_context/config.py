"""
Configuration for Query Context Enrichment skill.
"""

from dataclasses import dataclass, field
from typing import Optional, List
import pandas as pd


@dataclass
class QueryContextInput:
    """
    Input configuration for query context enrichment.

    Attributes:
        queries: List of queries to enrich (or DataFrame with 'query' column)
        concurrency_limit: Max concurrent API requests (default: 100)
        retry_limit: Max retries per query (default: 5)
        timeout_seconds: Timeout per request (default: 5)
        include_pt_features: Include product type features (default: True)
    """
    queries: List[str] | pd.DataFrame
    concurrency_limit: int = 100
    retry_limit: int = 5
    timeout_seconds: int = 5
    include_pt_features: bool = True


@dataclass
class QueryContextOutput:
    """
    Output from query context enrichment.

    Attributes:
        enriched_df: DataFrame with query context features
        queries_processed: Number of queries successfully processed
        queries_failed: Number of queries that failed
        features_extracted: List of feature names extracted
    """
    enriched_df: pd.DataFrame
    queries_processed: int
    queries_failed: int
    features_extracted: List[str] = field(default_factory=list)
