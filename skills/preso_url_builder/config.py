"""Configuration classes for Preso URL Builder skill."""

from dataclasses import dataclass
from typing import Optional, Dict, List
import pandas as pd


@dataclass
class PresoUrlBuilderInput:
    """Input configuration for Preso URL Builder."""

    # Option 1: Provide JSON config directly
    config_json: Optional[str] = None  # JSON string from email

    # Option 2: Provide config file path
    config_file: Optional[str] = None  # Path to JSON file

    # Option 3: Extract from email
    email_file: Optional[str] = None  # Path to .eml file
    email_text: Optional[str] = None  # Raw email text

    # Queries to build URLs for
    queries: Optional[List[str]] = None  # List of queries
    queries_file: Optional[str] = None  # Path to file with queries (CSV/parquet/txt)

    # Output options
    add_to_dataframe: Optional[pd.DataFrame] = None  # If provided, add URL columns to this df
    output_file: Optional[str] = None  # Save results to file


@dataclass
class PresoUrlBuilderOutput:
    """Output from Preso URL Builder."""

    # Parsed configuration
    control_config: Dict
    variant_config: Dict
    variant_engine_name: str

    # Extracted GCS path (if found in email)
    gcs_path: Optional[str] = None

    # Generated URLs (if queries provided)
    urls_df: Optional[pd.DataFrame] = None  # DataFrame with query, control_url, variant_url columns

    # Success indicators
    status: str = "success"
    message: str = ""
