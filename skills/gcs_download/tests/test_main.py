"""
Unit tests for GCS Download skill.
"""

import pytest
import os
from unittest.mock import Mock, patch, MagicMock
from skills.gcs_download.main import discover_files, download_file, run
from skills.gcs_download.config import GCSDownloadInput, GCSDownloadOutput


def test_gcs_download_input_validation():
    """Test that GCSDownloadInput validates gs:// prefix."""
    # Valid input
    valid_input = GCSDownloadInput(gs_path='gs://bucket/path')
    assert valid_input.gs_path == 'gs://bucket/path'

    # Invalid input - missing gs:// prefix
    with pytest.raises(ValueError, match="gs_path must start with 'gs://'"):
        GCSDownloadInput(gs_path='/local/path')


def test_discover_files():
    """Test file discovery logic."""
    # Mock GCS filesystem
    mock_fs = Mock()
    mock_fs.ls.return_value = [
        'bucket/path/qip_scores_v1.parquet',
        'bucket/path/config_crawl_preso.json',
        'bucket/path/metadata.csv',
        'bucket/path/other_file.txt'
    ]
    mock_fs.info.return_value = {'type': 'file'}

    discovered = discover_files(mock_fs, 'gs://bucket/path/', recursive=False)

    assert discovered['qip_scores'] == 'gs://bucket/path/qip_scores_v1.parquet'
    assert discovered['config'] == 'gs://bucket/path/config_crawl_preso.json'
    assert discovered['metadata'] == 'gs://bucket/path/metadata.csv'


def test_discover_files_recursive():
    """Test recursive subdirectory search."""
    mock_fs = Mock()

    # First call to ls() returns subdirectories
    mock_fs.ls.side_effect = [
        ['bucket/path/sample-5000', 'bucket/path/sample-1000'],  # Top level
        ['bucket/path/sample-5000/qip_scores.parquet',  # sample-5000 contents
         'bucket/path/sample-5000/config_crawl_preso.json',
         'bucket/path/sample-5000/metadata.csv'],
        ['bucket/path/sample-1000/other.parquet']  # sample-1000 contents
    ]

    # Mock info() to indicate directories
    def mock_info(path):
        if 'sample-5000' in path or 'sample-1000' in path:
            return {'type': 'directory'}
        return {'type': 'file'}

    mock_fs.info.side_effect = mock_info

    discovered = discover_files(mock_fs, 'gs://bucket/path/', recursive=True)

    assert discovered['qip_scores'] == 'gs://bucket/path/sample-5000/qip_scores.parquet'
    assert discovered['config'] == 'gs://bucket/path/sample-5000/config_crawl_preso.json'
    assert discovered['metadata'] == 'gs://bucket/path/sample-5000/metadata.csv'


def test_discover_files_fallback_parquet():
    """Test that any .parquet file is used as fallback."""
    mock_fs = Mock()
    mock_fs.ls.return_value = [
        'bucket/path/some_data.parquet',  # No 'qip_scores' in name
        'bucket/path/readme.txt'
    ]

    # Mock info to indicate files (not directories)
    mock_fs.info.return_value = {'type': 'file'}

    discovered = discover_files(mock_fs, 'gs://bucket/path/', recursive=False)

    assert discovered['qip_scores'] == 'gs://bucket/path/some_data.parquet'


def test_discover_files_prefers_qip_scores():
    """Test that qip_scores file is preferred over generic parquet."""
    mock_fs = Mock()
    mock_fs.ls.return_value = [
        'bucket/path/other.parquet',
        'bucket/path/qip_scores.parquet',  # Should be selected
    ]
    mock_fs.info.return_value = {'type': 'file'}

    discovered = discover_files(mock_fs, 'gs://bucket/path/', recursive=False)

    assert discovered['qip_scores'] == 'gs://bucket/path/qip_scores.parquet'


@patch('skills.gcs_download.main.gcsfs.GCSFileSystem')
def test_run_success(mock_gcsfs_class):
    """Test successful download workflow."""
    # Mock the filesystem
    mock_fs = MagicMock()
    mock_gcsfs_class.return_value = mock_fs

    # Mock file listing
    mock_fs.ls.return_value = [
        'bucket/path/qip_scores.parquet',
        'bucket/path/config.json'
    ]

    # Mock file download (fs.get)
    mock_fs.get.return_value = None  # get() doesn't return anything

    # Mock file size
    with patch('os.path.getsize', return_value=1024 * 1024):
        with patch('os.makedirs'):
            input_config = GCSDownloadInput(
                gs_path='gs://bucket/path/',
                local_dir='./test_temp'
            )

            result = run(input_config)

            assert result.qip_scores_path.endswith('qip_scores.parquet')
            assert result.config_path.endswith('config.json')
            assert result.download_dir == './test_temp'


def test_run_missing_qip_scores():
    """Test that missing qip_scores raises error."""
    with patch('skills.gcs_download.main.gcsfs.GCSFileSystem') as mock_gcsfs:
        mock_fs = MagicMock()
        mock_gcsfs.return_value = mock_fs

        # No parquet files in listing
        mock_fs.ls.return_value = [
            'bucket/path/config.json',
            'bucket/path/readme.txt'
        ]

        input_config = GCSDownloadInput(gs_path='gs://bucket/path/')

        with pytest.raises(FileNotFoundError, match="Could not find qip_scores.parquet"):
            run(input_config)


def test_gcs_download_output_validation():
    """Test that GCSDownloadOutput validates required files."""
    # Missing qip_scores should raise error
    with pytest.raises(ValueError, match="Failed to find qip_scores.parquet"):
        GCSDownloadOutput(
            qip_scores_path=None,
            config_path='./config.json'
        )

    # Valid output
    output = GCSDownloadOutput(
        qip_scores_path='./qip_scores.parquet',
        config_path='./config.json'
    )
    assert output.qip_scores_path == './qip_scores.parquet'
