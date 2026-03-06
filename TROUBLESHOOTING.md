# Troubleshooting Guide

## Issue: "QIP scores not found"

### Problem
```
❌ QIP scores not found: ./temp/downloaded_files/output_qips_total.parquet
Run test_gcs_download.py first to download files!
```

### Solution Steps

#### Step 1: Check if you downloaded files
```bash
python check_downloaded_files.py
```

This will:
- ✅ Show what files are in `./temp/downloaded_files/`
- ✅ Identify QIP scores, item attributes, config files
- ✅ Suggest correct file paths to use

#### Step 2: Download files if needed
```bash
python test_gcs_download.py
```

This downloads from:
```
gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383
```

#### Step 3: Verify download succeeded
```bash
ls -lh ./temp/downloaded_files/
```

You should see:
- A `.parquet` file (QIP scores, 100-200 MB)
- A `.jsonl` file (item attributes, 50-100 MB)

#### Step 4: Check actual file names
The downloaded files might have different names than expected:
- Instead of `output_qips_total.parquet`, it might be named differently
- Instead of `item_attributes_sample-5000.jsonl`, it might have a different pattern

Run:
```bash
python check_downloaded_files.py
```

It will show the actual file names and suggest correct paths.

#### Step 5: Update paths in load_data_example.py

If files are named differently, edit `load_data_example.py`:

```python
# Find these lines (around line 180)
qip_scores_path = f'{download_dir}/output_qips_total.parquet'
item_attrs_path = f'{download_dir}/item_attributes_sample-5000.jsonl'

# Replace with actual file names from check_downloaded_files.py
qip_scores_path = f'{download_dir}/actual_file_name.parquet'
item_attrs_path = f'{download_dir}/actual_file_name.jsonl'
```

#### Step 6: Try loading again
```bash
python load_data_example.py
```

---

## Issue: Files download but can't be found

### Possible Causes

1. **Files in subdirectory**
   - Downloaded files might be in nested directories
   - Check: `find ./temp -name "*.parquet"`

2. **Different file names**
   - Files might have timestamps or different names
   - Use: `python check_downloaded_files.py`

3. **Download directory different**
   - Check what directory test_gcs_download.py uses
   - Look in the success message after download

### Solution

Use the flexible file finder:

```python
import os
from pathlib import Path

download_dir = './temp/downloaded_files'

# Find any parquet file
parquet_files = list(Path(download_dir).rglob('*.parquet'))
if parquet_files:
    qip_scores_path = str(parquet_files[0])
    print(f"Found: {qip_scores_path}")

# Find any jsonl file
jsonl_files = list(Path(download_dir).rglob('*.jsonl'))
if jsonl_files:
    item_attrs_path = str(jsonl_files[0])
    print(f"Found: {item_attrs_path}")
```

---

## Issue: TypeError when unpacking result

### Problem
```
TypeError: cannot unpack non-iterable int object
```

### Cause
The script returns `1` (error code) when file not found, but tries to unpack as tuple.

### Solution
Already fixed! Update your `load_data_example.py`:

```python
if __name__ == '__main__':
    result = main()

    if result and result != 1:  # Check it's not an error code
        df, config, metadata = result
        # ... rest of code
```

---

## Issue: GCS authentication failed

### Problem
```
Error: Could not authenticate to GCS
```

### Solution
```bash
# Authenticate with gcloud
gcloud auth application-default login

# Or set service account key
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

---

## Issue: Permission denied on GCS bucket

### Problem
```
Error: Permission denied for gs://p0y01cc/...
```

### Solution

1. **Check bucket permissions:**
   ```bash
   gsutil ls -L gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383
   ```

2. **Verify your account:**
   ```bash
   gcloud auth list
   ```

3. **Request access:**
   - Ask bucket owner to grant read access
   - Need: `storage.objects.get` and `storage.objects.list` permissions

---

## Issue: Files are empty after download

### Problem
Files download but are 0 bytes.

### Solution

1. **Check GCS file sizes:**
   ```bash
   gsutil ls -lh gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383/**
   ```

2. **Verify download succeeded:**
   ```bash
   ls -lh ./temp/downloaded_files/
   ```

3. **Try manual download:**
   ```bash
   gsutil cp gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383/sample-5000/*.parquet ./temp/
   ```

---

## Quick Diagnostic Commands

```bash
# 1. Check if download directory exists
ls -la ./temp/downloaded_files/

# 2. Find all downloaded files
find ./temp -type f

# 3. Check file sizes
du -sh ./temp/downloaded_files/*

# 4. Check Python can import the skill
python -c "from skills.gcs_download import run; print('✓ Import works')"

# 5. Check GCS access
gsutil ls gs://p0y01cc/

# 6. Run diagnostics
python check_downloaded_files.py
```

---

## Common File Name Patterns

Your files might be named:
- QIP Scores: `output_qips_total.parquet`, `qip_scores.parquet`, `inference_output.parquet`
- Item Attrs: `item_attributes_sample-5000.jsonl`, `item_metadata.jsonl`, `products.jsonl`
- Config: `config_crawl_preso.json`, `config.json`, `experiment_config.json`

Use `check_downloaded_files.py` to see actual names!

---

## Still Having Issues?

1. **Show file structure:**
   ```bash
   tree ./temp/downloaded_files/ || find ./temp/downloaded_files/ -type f
   ```

2. **Check skill output:**
   ```bash
   python test_gcs_download.py 2>&1 | tee download.log
   ```

3. **Verify Python environment:**
   ```bash
   python --version
   pip list | grep -E "(gcsfs|pandas|pyarrow)"
   ```

4. **Check logs:**
   Look at the output from `test_gcs_download.py` - it shows what files were found and downloaded.

---

**Most common fix:** Run `python check_downloaded_files.py` to see actual file paths! 🔍
