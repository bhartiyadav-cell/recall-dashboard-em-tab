# Skill 1: GCS Download - Update (Subdirectory Support)

## 🔧 What Changed

Added **recursive subdirectory search** to handle cases where files are in subdirectories like:
- `sample-5000/`
- `sample-1000/`
- Or any other subdirectories

## 🎯 Problem

The original implementation only looked in the top-level directory. If files were in subdirectories, it would fail with:
```
❌ ERROR: Could not find qip_scores.parquet file in gs://...
```

## ✅ Solution

Now the skill:
1. Lists items in the top-level directory
2. Detects which items are subdirectories
3. Recursively searches through each subdirectory
4. Finds files regardless of which subdirectory they're in

## 📊 Example Structure

Your GCS bucket structure:
```
gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383/
├── sample-5000/
│   ├── qip_scores.parquet      ← Will be found!
│   ├── config_crawl_preso.json ← Will be found!
│   └── metadata.csv            ← Will be found!
├── sample-1000/
│   └── ... (other files)
└── other-dir/
    └── ... (other files)
```

## 🚀 How to Use

### Default (Recursive Search Enabled)
```python
from skills.gcs_download import run, GCSDownloadInput

result = run(GCSDownloadInput(
    gs_path='gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383',
    recursive=True  # Default is True
))
```

### Disable Recursive Search (Only Top Level)
```python
result = run(GCSDownloadInput(
    gs_path='gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383',
    recursive=False  # Only search top level
))
```

## 📝 Updated Code

### `config.py`
Added `recursive: bool = True` parameter to `GCSDownloadInput`

### `main.py`
Updated `discover_files()` to:
- Accept `recursive` parameter
- List subdirectories
- Search files in each subdirectory
- Log which subdirectory files were found in

## 🧪 Test It

```bash
cd /Users/p0y01cc/l1ranker_recall_comparison
python test_gcs_download.py
```

## 📋 Expected Output

```
Testing GCS Download Skill
Source: gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383
Will search recursively through subdirectories (sample-5000, sample-1000, etc.)

============================================================
GCS Download Skill - Starting
============================================================
Source: gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383
Destination: ./temp/downloaded_files

Connecting to GCS...
Discovering files in: p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383
  Searching recursively through subdirectories...
Found X items in directory
  Searching subdirectory: sample-5000
    Found 3 files in sample-5000
  Searching subdirectory: sample-1000
    Found 3 files in sample-1000
Total files to search: 6
  ✓ Found qip_scores: qip_scores.parquet (in sample-5000/qip_scores.parquet)
  ✓ Found config: config_crawl_preso.json (in sample-5000/config_crawl_preso.json)
  ✓ Found metadata: metadata.csv (in sample-5000/metadata.csv)

Downloading files...
  Downloading: qip_scores.parquet
    → XX.XX MB
  Downloading: config_crawl_preso.json
    → 0.01 MB
  Downloading: metadata.csv
    → X.XX MB

============================================================
✓ Downloaded 3 file(s)
============================================================

DOWNLOAD SUCCESSFUL!
```

## ✨ Features

- ✅ **Automatic subdirectory detection**
- ✅ **Searches all subdirectories** (sample-5000, sample-1000, etc.)
- ✅ **Logs which directory files came from**
- ✅ **Backward compatible** (recursive=True by default)
- ✅ **Can be disabled** if you only want top-level files

## 🎓 What This Teaches

- Recursive directory traversal
- Handling different GCS bucket structures
- Flexible configuration with sensible defaults
- Clear logging to show what's happening

---

**Ready to test?** Run `python test_gcs_download.py` to see it in action! 🚀
