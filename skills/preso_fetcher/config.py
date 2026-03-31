"""
Configuration classes for Preso Fetcher.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import pandas as pd


@dataclass
class PresoFetcherInput:
    """
    Input configuration for Preso fetcher.

    Args:
        qip_scores_file: Path to qip_scores.parquet file
        contextual_queries_file: Path to sample JSONL file with full contextualQuery data
        experiment_config: Experiment config JSON string or dict
        experiment_config_file: Path to experiment config JSON file
        access_key: Preso API access key (defaults to standard key)
        qps: Queries per second rate limit (default: 3)
        max_workers: Number of parallel workers (default: 6)
        top_n: Number of results to fetch per query (default: 40)
        start_idx: Start index for processing queries (default: 0)
        end_idx: End index for processing queries (default: None = all)
    """
    qip_scores_file: str
    contextual_queries_file: str
    experiment_config: Optional[str] = None
    experiment_config_file: Optional[str] = None
    access_key: str = "532c28d5412dd75bf975fb951c740a30"
    qps: int = 3
    max_workers: int = 6
    top_n: int = 40
    start_idx: int = 0
    end_idx: Optional[int] = None


@dataclass
class PresoFetcherOutput:
    """
    Output from Preso fetcher.

    Args:
        results_df: DataFrame with fetched results
        control_urls: List of control URLs
        variant_urls: List of variant URLs
        status: Status of the operation ('success' or 'error')
        message: Status message
        queries_processed: Number of queries processed
        queries_failed: Number of queries that failed
    """
    results_df: Optional[pd.DataFrame] = None
    control_urls: List[str] = field(default_factory=list)
    variant_urls: List[str] = field(default_factory=list)
    status: str = "success"
    message: str = ""
    queries_processed: int = 0
    queries_failed: int = 0
