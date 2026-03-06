"""
GCS Download Skill - Main Logic

Downloads qip_scores.parquet, config JSON, and metadata CSV from GCS bucket.
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional
import gcsfs

from .config import GCSDownloadInput, GCSDownloadOutput

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def discover_files(fs: gcsfs.GCSFileSystem, gs_path: str, recursive: bool = True) -> Dict[str, Optional[str]]:
    """
    Discover required files in GCS bucket by pattern matching.

    Looks for:
    - *qip_scores*.parquet or *.parquet
    - *config*.json or *preso*.json
    - *metadata*.csv

    Searches recursively through subdirectories (sample-5000, sample-1000, etc.)

    Args:
        fs: GCS filesystem instance
        gs_path: GCS path (can be file or directory)
        recursive: If True, search subdirectories

    Returns:
        Dict mapping file types to GCS paths
    """
    discovered = {
        'qip_scores': None,
        'config': None,
        'metadata': None
    }

    # If gs_path is a file, just return it
    if gs_path.endswith('.parquet'):
        discovered['qip_scores'] = gs_path
        # Try to find config and metadata in same directory
        directory = '/'.join(gs_path.split('/')[:-1])
    else:
        # It's a directory, list all files
        directory = gs_path.rstrip('/')

    logger.info(f"Discovering files in: {directory}")
    if recursive:
        logger.info(f"  Searching recursively through subdirectories...")

    try:
        # Collect all files (including from subdirectories)
        all_files = []

        # List items in current directory
        items = fs.ls(directory)
        logger.info(f"Found {len(items)} items in directory")

        for item in items:
            # Check if it's a directory
            try:
                item_info = fs.info(item)
                is_directory = item_info.get('type') == 'directory'
            except:
                # If we can't get info, check if it has a file extension
                is_directory = '.' not in item.split('/')[-1]

            if is_directory and recursive:
                # It's a subdirectory (like sample-5000, sample-1000)
                subdir_name = item.split('/')[-1]
                logger.info(f"  Searching subdirectory: {subdir_name}")

                # List files in subdirectory
                try:
                    subdir_files = fs.ls(item)
                    all_files.extend(subdir_files)
                    logger.info(f"    Found {len(subdir_files)} files in {subdir_name}")
                except Exception as e:
                    logger.warning(f"    Could not list {subdir_name}: {e}")
            else:
                # It's a file, add it
                all_files.append(item)

        logger.info(f"Total files to search: {len(all_files)}")

        # Now search through all collected files
        for file_path in all_files:
            file_name = file_path.split('/')[-1].lower()

            # Match qip_scores parquet file
            if 'qip_scores' in file_name and file_name.endswith('.parquet'):
                discovered['qip_scores'] = f"gs://{file_path}"
                logger.info(f"  ✓ Found qip_scores: {file_name} (in {'/'.join(file_path.split('/')[-2:])})")
            elif file_name.endswith('.parquet') and discovered['qip_scores'] is None:
                # Fallback: any parquet file
                discovered['qip_scores'] = f"gs://{file_path}"
                logger.info(f"  ✓ Found parquet file: {file_name} (in {'/'.join(file_path.split('/')[-2:])})")

            # Match config JSON file (optional)
            if ('config' in file_name or 'preso' in file_name) and file_name.endswith('.json'):
                discovered['config'] = f"gs://{file_path}"
                logger.info(f"  ✓ Found config: {file_name} (in {'/'.join(file_path.split('/')[-2:])})")

            # Match metadata CSV or JSONL file
            if 'metadata' in file_name and file_name.endswith('.csv'):
                discovered['metadata'] = f"gs://{file_path}"
                logger.info(f"  ✓ Found metadata CSV: {file_name} (in {'/'.join(file_path.split('/')[-2:])})")
            elif 'item_attributes' in file_name and file_name.endswith('.jsonl'):
                discovered['metadata'] = f"gs://{file_path}"
                logger.info(f"  ✓ Found metadata JSONL: {file_name} (in {'/'.join(file_path.split('/')[-2:])})")

    except Exception as e:
        logger.error(f"Error listing directory {directory}: {e}")
        raise

    return discovered


def download_file(fs: gcsfs.GCSFileSystem, gs_file: str, local_path: str) -> str:
    """
    Download a single file from GCS.

    Args:
        fs: GCS filesystem instance
        gs_file: GCS file path (gs://...)
        local_path: Local destination path

    Returns:
        Local file path
    """
    # Remove gs:// prefix for gcsfs
    gcs_path = gs_file.replace('gs://', '')

    # Create parent directory if needed
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    logger.info(f"  Downloading: {gs_file.split('/')[-1]}")

    try:
        fs.get(gcs_path, local_path)
        file_size = os.path.getsize(local_path)
        logger.info(f"    → {file_size / 1024 / 1024:.2f} MB")
        return local_path
    except Exception as e:
        logger.error(f"  ✗ Failed to download {gs_file}: {e}")
        raise


def run(input_config: GCSDownloadInput) -> GCSDownloadOutput:
    """
    Main entry point for GCS Download skill.

    Downloads qip_scores.parquet, config JSON, and metadata CSV from GCS.

    Args:
        input_config: GCSDownloadInput with gs_path and options

    Returns:
        GCSDownloadOutput with paths to downloaded files
    """
    logger.info("="*60)
    logger.info("GCS Download Skill - Starting")
    logger.info("="*60)
    logger.info(f"Source: {input_config.gs_path}")
    logger.info(f"Destination: {input_config.local_dir}")

    # Create local directory
    os.makedirs(input_config.local_dir, exist_ok=True)

    # Initialize GCS filesystem
    logger.info("Connecting to GCS...")
    fs = gcsfs.GCSFileSystem()

    # Discover files
    discovered = discover_files(fs, input_config.gs_path, recursive=input_config.recursive)

    # Check what we found
    if discovered['qip_scores'] is None:
        raise FileNotFoundError(
            f"Could not find qip_scores.parquet file in {input_config.gs_path}"
        )

    # Download files
    logger.info("\nDownloading files...")
    downloaded_files = []

    qip_scores_path = None
    config_path = None
    metadata_path = None

    # Download qip_scores (required)
    if discovered['qip_scores']:
        local_file = os.path.join(
            input_config.local_dir,
            discovered['qip_scores'].split('/')[-1]
        )
        qip_scores_path = download_file(fs, discovered['qip_scores'], local_file)
        downloaded_files.append(qip_scores_path)

    # Download config (optional)
    if discovered['config']:
        local_file = os.path.join(
            input_config.local_dir,
            discovered['config'].split('/')[-1]
        )
        config_path = download_file(fs, discovered['config'], local_file)
        downloaded_files.append(config_path)
    else:
        logger.warning("  ⚠ No config file found (optional)")

    # Download metadata (optional)
    if discovered['metadata']:
        local_file = os.path.join(
            input_config.local_dir,
            discovered['metadata'].split('/')[-1]
        )
        metadata_path = download_file(fs, discovered['metadata'], local_file)
        downloaded_files.append(metadata_path)
    else:
        logger.warning("  ⚠ No metadata file found (optional)")

    logger.info("\n" + "="*60)
    logger.info(f"✓ Downloaded {len(downloaded_files)} file(s)")
    logger.info("="*60)

    return GCSDownloadOutput(
        qip_scores_path=qip_scores_path,
        config_path=config_path,
        metadata_path=metadata_path,
        download_dir=input_config.local_dir,
        all_files=downloaded_files
    )


if __name__ == '__main__':
    """
    Command-line usage for testing.

    Example:
        python -m skills.gcs_download.main gs://bucket/path/
    """
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m skills.gcs_download.main <gs_path> [local_dir]")
        sys.exit(1)

    gs_path = sys.argv[1]
    local_dir = sys.argv[2] if len(sys.argv) > 2 else './temp'

    input_config = GCSDownloadInput(
        gs_path=gs_path,
        local_dir=local_dir
    )

    result = run(input_config)

    print("\nDownload Summary:")
    print(f"  QIP Scores: {result.qip_scores_path}")
    print(f"  Config: {result.config_path}")
    print(f"  Metadata: {result.metadata_path}")
