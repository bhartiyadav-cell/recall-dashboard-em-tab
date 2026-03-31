"""Configuration for L1 Recall Analysis skill"""

from pydantic import BaseModel, Field
from typing import Optional


class L1RecallAnalysisInput(BaseModel):
    """Input configuration for L1 Recall Analysis pipeline"""

    email_file: str = Field(
        ...,
        description="Path to the experiment email (.eml file)"
    )

    # Pipeline options
    skip_download: bool = Field(
        default=False,
        description="Skip GCS download if files already exist"
    )

    skip_enrichment: bool = Field(
        default=False,
        description="Skip Perceive API enrichment if already done"
    )

    skip_preso: bool = Field(
        default=False,
        description="Skip Preso top-40 crawl if already done"
    )

    # Filter parameters
    queries: Optional[int] = Field(
        default=None,
        description="Number of queries to sample (default: all)"
    )

    min_total: int = Field(
        default=5,
        description="Minimum total items required (default: 5)"
    )

    max_total_diff: int = Field(
        default=5,
        description="Max difference in total items between engines (default: 5)"
    )

    min_gain: int = Field(
        default=1,
        description="Minimum 4s gain required (default: 1)"
    )

    # Preso options
    preso_qps: int = Field(
        default=3,
        description="Queries per second for Preso API (default: 3)"
    )

    # Variant engine (optional, auto-detected if not specified)
    variant: Optional[str] = Field(
        default=None,
        description="Variant engine name (auto-detected from data if not specified)"
    )


class L1RecallAnalysisOutput(BaseModel):
    """Output from L1 Recall Analysis pipeline"""

    status: str = Field(
        description="Status: 'success' or 'error'"
    )

    message: str = Field(
        description="Status message or error description"
    )

    experiment_id: Optional[str] = Field(
        default=None,
        description="Experiment ID"
    )

    html_report: Optional[str] = Field(
        default=None,
        description="Path to generated HTML report"
    )

    output_files: Optional[dict] = Field(
        default=None,
        description="Dictionary of all generated output files"
    )

    statistics: Optional[dict] = Field(
        default=None,
        description="Pipeline statistics (4s gained, top-40 coverage, etc.)"
    )
