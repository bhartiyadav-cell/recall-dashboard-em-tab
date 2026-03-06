# 🚀 START HERE - Quick Start Guide

## Current Situation

✅ **Skill 1 (GCS Download)** is complete and tested
❌ **Files not downloaded yet** - You need to download them first!

## Step-by-Step Instructions

### Step 1: Download Files from GCS

```bash
python test_gcs_download.py
```

**What this does:**
- Connects to your GCS bucket: `gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383`
- Recursively searches through subdirectories (sample-5000, sample-1000, etc.)
- Downloads:
  - `output_qips_total.parquet` (~100-200 MB) - QIP scores with ratings
  - `item_attributes_sample-5000.jsonl` (~50-100 MB) - Item metadata
- Saves to: `./temp/downloaded_files/`

**Expected output:**
```
Testing GCS Download Skill
Source: gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383
Will search recursively through subdirectories...

============================================================
DOWNLOAD SUCCESSFUL!
============================================================

📦 Downloaded Files:
  • QIP Scores:  ./temp/downloaded_files/output_qips_total.parquet
  • Config:      Not found
  • Metadata:    ./temp/downloaded_files/item_attributes_sample-5000.jsonl

📁 Location:     ./temp/downloaded_files
📊 Total files:  2
```

---

### Step 2: Verify Download

```bash
python check_downloaded_files.py
```

**What this does:**
- Lists all downloaded files with sizes
- Identifies QIP scores, item attributes, and config files
- Suggests correct paths for next steps

**Expected output:**
```
============================================================
Checking Downloaded Files
============================================================

Found 2 file(s):

  📄 output_qips_total.parquet
     Size: 156.23 MB
     Type: QIP Scores ✓

  📄 item_attributes_sample-5000.jsonl
     Size: 78.45 MB
     Type: Item Attributes ✓

============================================================
Summary
============================================================

✓ QIP Scores found:
  ./temp/downloaded_files/output_qips_total.parquet

✓ Item attributes found:
  ./temp/downloaded_files/item_attributes_sample-5000.jsonl

⚠ No config file found (optional)
  Will extract from QIP scores instead
```

---

### Step 3: Load and Explore Data

```bash
python load_data_example.py
```

**What this does:**
- Loads QIP scores (1.1M rows with ratings 1-4)
- Loads item attributes (785K items with descriptions)
- Extracts engine configuration (control vs variant)
- Joins data on `pg_prod_id`
- Saves merged file: `qip_scores_with_attributes.parquet`

**Expected output:**
```
============================================================
Loading Data from Downloaded Files
============================================================

Loading QIP scores from: ./temp/downloaded_files/output_qips_total.parquet
  Loaded 1,106,194 rows
  Unique queries: 5,000
  Unique items: 100,000

Extracting ptss/trsp parameters...
  Found engines: ['control', 'nlfv3_alp1_utbeta05_w0_4']

Extracting metadata for Sunlight URLs...
  Metadata: {'stores': 100, 'zipcode': '72712', 'state': 'AR', ...}

Loading item attributes from: ./temp/downloaded_files/item_attributes_sample-5000.jsonl
  Loaded 785,128 items

Joining QIP scores with item attributes...
  Merged 1,106,194 rows
  Items with attributes: 1,050,000 (95.0%)

✓ Saved merged data to: ./temp/downloaded_files/qip_scores_with_attributes.parquet

Summary
============================================================
✓ Loaded data successfully
✓ Total rows: 1,106,194
✓ Config info extracted: True
✓ Sunlight metadata extracted: True

You can now use this data for recall analysis!
```

---

### Step 4: Run Existing Recall Analysis

```bash
python -m recall_analyser analyse ./temp/downloaded_files/output_qips_total.parquet \
    --output-dir ./output \
    --control-engine control \
    --variant-engine nlfv3_alp1_utbeta05_w0_4
```

**What this does:**
- Runs your existing recall comparison tool
- Compares control vs variant engine
- Generates analysis outputs in `./output/`

---

## 🐛 Troubleshooting

### Issue: "QIP scores not found"

**Cause:** Files haven't been downloaded yet

**Solution:** Run Step 1 first:
```bash
python test_gcs_download.py
```

---

### Issue: "Authentication error"

**Cause:** Not authenticated to GCS

**Solution:**
```bash
gcloud auth application-default login
```

---

### Issue: File paths don't match

**Cause:** Downloaded files have different names

**Solution:** Run the diagnostic tool:
```bash
python check_downloaded_files.py
```

It will show actual file names and suggest correct paths.

---

## 📚 More Information

- **QUICK_REFERENCE.md** - Command reference card
- **DATA_STRUCTURE.md** - Complete data schema
- **TROUBLESHOOTING.md** - Detailed troubleshooting guide
- **STATUS.md** - Current capabilities and roadmap
- **NEXT_STEPS.md** - What to do after loading data

---

## ✅ Success Checklist

After completing all steps, you should have:

- [ ] Downloaded files to `./temp/downloaded_files/`
- [ ] Verified files with `check_downloaded_files.py`
- [ ] Loaded data successfully with `load_data_example.py`
- [ ] Created merged file: `qip_scores_with_attributes.parquet`
- [ ] Extracted engine configuration
- [ ] Ready to build enhanced skills!

---

## 🎯 What's Next?

Once you've completed these steps successfully, we can move on to building:

- **Skill 2:** Config Extractor (structure the configuration data)
- **Skill 3:** LLM-based Query Analysis (analyze rating changes with AI)
- **Skill 4:** Enhanced Report Generator (rich HTML reports)
- **Skill 5:** Orchestrator (chain everything together)

---

**Right now, run this command:**

```bash
python test_gcs_download.py
```

And let me know what happens! 🚀
