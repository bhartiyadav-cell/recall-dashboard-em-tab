"""
Configuration dataclasses for GCS Download skill.
"""

from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class GCSDownloadInput:
    """Input configuration for GCS download."""
    gs_path: str                    # GCS path (file or directory)
    local_dir: str = './temp'       # Where to save downloaded files
    auto_discover: bool = True      # Auto-find qip_scores, config, metadata
    recursive: bool = True          # Search subdirectories (sample-5000, etc.)

    def __post_init__(self):
        """Validate input."""
        if not self.gs_path.startswith('gs://'):
            raise ValueError(f"gs_path must start with 'gs://': {self.gs_path}")


@dataclass
class GCSDownloadOutput:
    """Output from GCS download."""
    qip_scores_path: Optional[str] = None    # Path to qip_scores.parquet
    config_path: Optional[str] = None        # Path to config JSON
    metadata_path: Optional[str] = None      # Path to metadata CSV
    download_dir: str = ""                   # Directory where files were saved
    all_files: List[str] = field(default_factory=list)  # All downloaded files

    def __post_init__(self):
        """Validate that we found required files."""
        if self.qip_scores_path is None:
            raise ValueError("Failed to find qip_scores.parquet file")
