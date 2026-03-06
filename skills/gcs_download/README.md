# GCS Download Skill - Quick Start

## What This Skill Does

Downloads files from Google Cloud Storage for L1 ranker recall analysis:
- **qip_scores.parquet** - Required recall data
- **config.json** - Config crawl preso file (optional)
- **metadata.csv** - Item metadata (optional)

## Quick Test

Run this to download your files:

```bash
cd /Users/p0y01cc/l1ranker_recall_comparison
python test_gcs_download.py
```

This will download files from:
```
gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383
```

To:
```
./temp/downloaded_files/
```

## What to Expect

You should see output like:
```
GCS Download Skill - Starting
============================================================
Source: gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383
Destination: ./temp/downloaded_files

Connecting to GCS...
Discovering files in: gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383
Found X files in bucket
  ✓ Found qip_scores: qip_scores.parquet
  ✓ Found config: config_crawl_preso.json
  ✓ Found metadata: metadata.csv

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
============================================================

📦 Downloaded Files:
  • QIP Scores:  ./temp/downloaded_files/qip_scores.parquet
  • Config:      ./temp/downloaded_files/config_crawl_preso.json
  • Metadata:    ./temp/downloaded_files/metadata.csv

📁 Location:     ./temp/downloaded_files
📊 Total files:  3
```

## Use in Python

```python
from skills.gcs_download import run, GCSDownloadInput

# Download files
result = run(GCSDownloadInput(
    gs_path='gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383',
    local_dir='./my_data'
))

# Access downloaded files
print(f"QIP Scores: {result.qip_scores_path}")
print(f"Config: {result.config_path}")
print(f"Metadata: {result.metadata_path}")
```

## Next Steps

After downloading:
1. ✅ Files are in `./temp/downloaded_files/`
2. 📊 Use qip_scores.parquet with existing `recall_analyser.py`
3. 🔧 Parse config.json with next skill (config_parser)
4. 📋 Use metadata.csv for item enrichment

## Troubleshooting

**Error: "Could not find qip_scores.parquet"**
- Check that the GCS path exists
- Verify you have read access to the bucket
- Try listing files manually: `gsutil ls gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383`

**Error: Authentication issues**
- Ensure gcloud is configured: `gcloud auth application-default login`
- Or set GOOGLE_APPLICATION_CREDENTIALS environment variable

**Files download but are empty**
- Check your GCS permissions
- Verify the files exist in GCS: `gsutil ls -lh gs://...`
