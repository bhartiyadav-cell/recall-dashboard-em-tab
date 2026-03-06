# Quick Start: Test Skill 1 (GCS Download)

## Step 1: Check Setup

```bash
cd /Users/p0y01cc/l1ranker_recall_comparison
python check_setup.py
```

This verifies:
- ✓ All dependencies installed
- ✓ Skill files created
- ✓ GCS authentication working
- ✓ Skill can be imported

## Step 2: Run the Test

```bash
python test_gcs_download.py
```

This downloads files from:
```
gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383
```

## Expected Result

You should see files downloaded to:
```
./temp/downloaded_files/
├── qip_scores.parquet      (or similar .parquet file)
├── config_crawl_preso.json (if exists)
└── metadata.csv            (if exists)
```

## Troubleshooting

### If check_setup.py fails on dependencies:
```bash
pip install gcsfs pandas pyarrow
```

### If GCS authentication fails:
```bash
gcloud auth application-default login
```

### If files don't download:
Check GCS access:
```bash
gsutil ls gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383
```

## What's Next?

Once the download works:
1. ✅ **Skill 1 (GCS Download)** - DONE!
2. 🔜 **Skill 2 (Config Parser)** - Parse the downloaded config JSON
3. 🔜 **Skill 3 (Model Diff)** - Compare model configurations
4. 🔜 **Skill 4 (Query Scorer)** - Score queries with Perceive API
5. 🔜 **Skill 5 (Report Builder)** - Generate enhanced reports
6. 🔜 **Skill 6 (Orchestrator)** - Chain everything together

---

## Quick Commands Reference

```bash
# Check setup
python check_setup.py

# Download files
python test_gcs_download.py

# Use as CLI
python -m skills.gcs_download.main gs://bucket/path/

# Use in Python
python
>>> from skills.gcs_download import run, GCSDownloadInput
>>> result = run(GCSDownloadInput(gs_path='gs://...'))
>>> print(result.qip_scores_path)

# Run unit tests
pytest skills/gcs_download/tests/ -v
```

---

**Ready?** Run `python check_setup.py` to begin! 🚀
