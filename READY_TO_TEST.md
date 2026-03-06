# 🚀 Ready to Test: Skill 1 (GCS Download with Subdirectory Support)

## ✅ What's Updated

Fixed the skill to **handle files in subdirectories** like `sample-5000/` and `sample-1000/`.

### Changes Made:

1. **config.py** - Added `recursive: bool = True` parameter
2. **main.py** - Updated `discover_files()` to search subdirectories
3. **test_main.py** - Added test for recursive search
4. **test_gcs_download.py** - Updated to show recursive search message

## 🎯 Problem Solved

**Before:** ❌ Failed to find files in subdirectories
```
ERROR: Could not find qip_scores.parquet file in gs://...
```

**After:** ✅ Finds files in any subdirectory
```
✓ Found qip_scores: qip_scores.parquet (in sample-5000/qip_scores.parquet)
```

## 📂 Your Directory Structure

```
gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383/
├── sample-5000/         ← Will search here
│   ├── qip_scores.parquet
│   ├── config_crawl_preso.json
│   └── metadata.csv
└── sample-1000/         ← And here
    └── ... (other files)
```

## 🧪 Test It Now

```bash
cd /Users/p0y01cc/l1ranker_recall_comparison
python test_gcs_download.py
```

## 📊 Expected Output

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
Found 2 items in directory
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
    → 25.43 MB
  Downloading: config_crawl_preso.json
    → 0.01 MB
  Downloading: metadata.csv
    → 5.21 MB

============================================================
✓ Downloaded 3 file(s)
============================================================

DOWNLOAD SUCCESSFUL!
============================================================

📦 Downloaded Files:
  • QIP Scores:  ./temp/downloaded_files/qip_scores.parquet
  • Config:      ./temp/downloaded_files/config_crawl_preso.json
  • Metadata:    ./temp/downloaded_files/metadata.csv

📁 Location:     ./temp/downloaded_files
📊 Total files:  3
```

## ✨ What It Does

1. **Connects to GCS** - Authenticates with your GCS credentials
2. **Lists top-level items** - Sees `sample-5000/`, `sample-1000/`, etc.
3. **Detects subdirectories** - Identifies which items are folders
4. **Searches each subdirectory** - Lists files in each one
5. **Finds files** - Matches patterns for qip_scores, config, metadata
6. **Downloads files** - Saves to local directory with progress

## 🎛️ How to Use

### Default (Recursive - Recommended)
```python
from skills.gcs_download import run, GCSDownloadInput

result = run(GCSDownloadInput(
    gs_path='gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383'
))
```

### Non-Recursive (Top Level Only)
```python
result = run(GCSDownloadInput(
    gs_path='gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383',
    recursive=False  # Don't search subdirectories
))
```

## 🔍 Verify Success

After running, check:

```bash
ls -lh ./temp/downloaded_files/

# Should show:
# qip_scores.parquet       (20-30 MB)
# config_crawl_preso.json  (few KB)
# metadata.csv             (5-10 MB)
```

## 🐛 Still Having Issues?

### Issue: "Authentication failed"
```bash
gcloud auth application-default login
```

### Issue: "Permission denied"
Check bucket permissions:
```bash
gsutil ls -l gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383
```

### Issue: Files not found
Verify the directory structure:
```bash
gsutil ls -r gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383
```

## 📚 Documentation

- **SKILL_1_UPDATE.md** - Detailed explanation of the update
- **skills/gcs_download/skill.md** - Complete API documentation
- **skills/gcs_download/README.md** - Quick start guide

## 🎉 Once This Works

We can move to **Skill 2: Config Parser** to extract the experiment ID from the downloaded config file!

---

**Ready?** Run `python test_gcs_download.py` now! 🚀

The skill will automatically find your files in the subdirectories and download them.
