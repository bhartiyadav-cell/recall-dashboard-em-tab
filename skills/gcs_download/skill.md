# GCS Download Skill

## Purpose

Download L1 ranker recall analysis files from Google Cloud Storage bucket.

Automatically discovers and downloads:
- **qip_scores.parquet** - Query-item-position scores (required)
- **config.json** - Config crawl preso file with ptss/trsp (optional)
- **metadata.csv** - Item metadata (optional)

## Usage

### Python API

```python
from skills.gcs_download import run, GCSDownloadInput

# Download from GCS directory
result = run(GCSDownloadInput(
    gs_path='gs://bucket/path/directory/',
    local_dir='./temp'
))

print(f"QIP Scores: {result.qip_scores_path}")
print(f"Config: {result.config_path}")
print(f"Metadata: {result.metadata_path}")
```

### Command Line

```bash
# Download from directory
python -m skills.gcs_download.main gs://bucket/path/directory/

# Download with custom local directory
python -m skills.gcs_download.main gs://bucket/path/ ./custom_temp
```

### Wibey Invocation

```bash
/gcs-download gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383
```

## Input

**GCSDownloadInput:**
- `gs_path` (str, required): GCS path to file or directory
  - Must start with `gs://`
  - Can be direct file path: `gs://bucket/qip_scores.parquet`
  - Or directory: `gs://bucket/analysis_dir/`
- `local_dir` (str, optional): Local directory to save files
  - Default: `./temp`
- `auto_discover` (bool, optional): Automatically find files by pattern
  - Default: `True`

## Output

**GCSDownloadOutput:**
- `qip_scores_path` (str): Path to downloaded qip_scores.parquet
- `config_path` (str or None): Path to config JSON file
- `metadata_path` (str or None): Path to metadata CSV file
- `download_dir` (str): Directory containing all downloaded files
- `all_files` (list): List of all downloaded file paths

## File Discovery Logic

The skill looks for files matching these patterns:

1. **QIP Scores** (required):
   - `*qip_scores*.parquet` (preferred)
   - `*.parquet` (fallback - any parquet file)

2. **Config** (optional):
   - `*config*.json`
   - `*preso*.json`

3. **Metadata** (optional):
   - `*metadata*.csv`

## Error Handling

- **Missing qip_scores**: Raises `FileNotFoundError` if no parquet file found
- **Missing config/metadata**: Logs warning but continues (optional files)
- **Invalid gs_path**: Raises `ValueError` if path doesn't start with `gs://`
- **Download failure**: Raises exception with details

## Examples

### Example 1: Download from directory
```python
result = run(GCSDownloadInput(
    gs_path='gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383',
    local_dir='./temp/nlf_v2_v3'
))
```

### Example 2: Download specific file
```python
result = run(GCSDownloadInput(
    gs_path='gs://bucket/analysis/qip_scores.parquet',
    local_dir='./data'
))
```

### Example 3: Use in workflow
```python
from skills.gcs_download import run, GCSDownloadInput
from skills.config_parser import run as parse_config

# Download files
download_result = run(GCSDownloadInput(
    gs_path='gs://bucket/analysis/'
))

# Use downloaded config in next skill
if download_result.config_path:
    config = parse_config(ConfigParserInput(
        config_path=download_result.config_path
    ))
```

## Dependencies

- `gcsfs` - GCS filesystem access
- `os`, `pathlib` - File operations
- `logging` - Progress logging

## Testing

Run unit tests:
```bash
cd /Users/p0y01cc/l1ranker_recall_comparison
python -m pytest skills/gcs_download/tests/
```

Test with real GCS path:
```bash
python test_gcs_download.py
```

## Notes

- Requires GCS credentials configured (gcloud auth or service account)
- Downloads are resumable for large files
- Creates local directory if it doesn't exist
- File names are preserved from GCS
- Safe to run multiple times (overwrites existing files)

## Version

1.0.0 - Initial release
