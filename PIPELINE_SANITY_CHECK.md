# End-to-End Pipeline Sanity Check

## ✅ Complete Workflow Verified

```bash
python run_analysis_pipeline.py \
    --gcs-path gs://k0k01ls/l1_recall_analysis/ltr_ab_candidates/1773333304 \
    --queries 2000
```

## 📋 Pipeline Steps

### Step 1: Download from GCS
**Script:** `skills/gcs_download/main.py`

**What it does:**
- Downloads `qip_scores.parquet` (required)
- Downloads `item_attributes*.jsonl` as metadata (required)
- Downloads config JSON and metadata CSV (optional)

**Parameters used:**
- `gs_path`: GCS path to experiment
- `local_dir`: `./temp/downloaded_files`
- `auto_discover`: `True` (automatically finds files)
- `recursive`: `True` (searches subdirectories like sample-5000)

**Output:**
- `qip_scores_path`: Path to downloaded qip_scores
- `metadata_path`: Path to downloaded item_attributes

**Files created:**
- `1773333304_qip_scores.parquet`
- `1773333304_item_attributes.jsonl`

---

### Step 2: Enrich Queries
**Script:** `enrich_qip_scores.py`

**What it does:**
- Samples N unique queries (e.g., 2000)
- Calls Perceive API for each query
- Extracts structured intent annotations (product_type, brand, color, gender)
- Joins with item attributes
- Saves enriched parquet

**Parameters:**
- `--input`: `1773333304_qip_scores.parquet`
- `--output`: `1773333304_qip_scores_enriched.parquet`
- `--queries`: 2000
- `--concurrency`: 10

**Output columns added:**
- `product_type_intent`, `brand_intent`, `color_intent`, `gender_intent` (JSON strings)
- `n_product_types`, `n_brands`, `n_colors`, `n_genders` (counts)
- Item attributes: `product_type`, `brand`, `color`, `gender`, `title`, `description`

**Files created:**
- `1773333304_qip_scores_enriched.parquet`

---

### Step 3: Filter to 4s Gain Queries
**Script:** `filter_4s_gain_queries.py`

**What it does:**
- Applies recall_analyser filtering (min_total=400, max_total_diff=5)
- Auto-detects variant engine (first non-control engine)
- Identifies queries where variant has MORE 4s than control
- Filters to only these queries and engines

**Parameters:**
- `--input`: `1773333304_qip_scores_enriched.parquet`
- `--output`: `1773333304_qip_4s_gain_filtered.parquet`
- `--variant`: Auto-detected (e.g., `nlfv3_alp1_utbeta05_w0_4`)
- `--min-total`: 400
- `--max-total-diff`: 5
- `--min-gain`: 1

**Files created:**
- `1773333304_qip_4s_gain_filtered.parquet`
- `1773333304_qip_4s_gain_filtered_summary.json` (contains variant_engine name)

---

### Step 4: Create QI Pairs
**Script:** `create_qip_pairs.py`

**What it does:**
- Reads filtered data (control + variant only)
- Creates side-by-side QI pairs
- Categorizes each pair:
  - `4_gained`: Items rated 4 in variant, not in control
  - `non4_removed`: Items rated 1/2/3 in control, removed in variant
  - `4_lost`: Items rated 4 in control, missing in variant
  - `no_change`: Same rating in both
  - `rating_changed`: Different ratings

**Parameters:**
- `--input`: `1773333304_qip_4s_gain_filtered.parquet`
- `--output`: `1773333304_qip_pairs.parquet`

**Files created:**
- `1773333304_qip_pairs.parquet`
- `1773333304_qip_pairs_summary.json`

---

### Step 5: Compute Attribute Matching
**Script:** `skills/attribute_matching/main.py`

**What it does:**
- Uses NLTK Porter Stemmer for text tokenization
- Computes matching scores for each QI pair:
  - **Exact matches**: Product type, brand, color, gender
  - **Partial matches**: Substring matching
  - **Text matches**: Stemmed tokens in title/description
  - **Overall match**: Average of exact attribute matches

**Parameters:**
- `--input`: `1773333304_qip_pairs.parquet`
- `--output`: `1773333304_qip_pairs_with_matching.parquet`

**Matching logic:**
- Query tokens: Extracted from raw query text only (e.g., "table" → {"tabl"})
- Title/description matching: Ratio of query tokens found in item text
- Structured attribute matching: Query intents vs item attributes

**Files created:**
- `1773333304_qip_pairs_with_matching.parquet`

---

### Step 6: Generate HTML Report
**Script:** `generate_4s_report.py`

**What it does:**
- Creates interactive HTML report
- Shows queries with 4s added by variant
- Displays attribute matching scores
- Beautiful UI with color-coded scores

**Parameters:**
- `--input`: `1773333304_qip_pairs_with_matching.parquet`
- `--output`: `reports/1773333304_nlfv3_alp1_utbeta05_w0_4_4s_added_report.html`
- `--with-matching`: Use pre-computed scores

**Report features:**
- Overall summary (total 4s, avg matching scores)
- Key insights comparing 4s added vs non-4s removed
- Interactive query dropdown
- Query intents display
- Item cards with attributes and match scores

**Files created:**
- `reports/1773333304_nlfv3_alp1_utbeta05_w0_4_4s_added_report.html`
- `reports/1773333304_nlfv3_alp1_utbeta05_w0_4_pipeline_summary.json`

---

## 📊 All Generated Files

### In `./temp/downloaded_files/`:
1. `1773333304_qip_scores.parquet` (raw from GCS)
2. `1773333304_item_attributes.jsonl` (raw from GCS)
3. `1773333304_qip_scores_enriched.parquet` (with Perceive API intents + item attrs)
4. `1773333304_qip_4s_gain_filtered.parquet` (only 4s gain queries)
5. `1773333304_qip_4s_gain_filtered_summary.json` (filter summary)
6. `1773333304_qip_pairs.parquet` (control vs variant pairs)
7. `1773333304_qip_pairs_summary.json` (pair summary)
8. `1773333304_qip_pairs_with_matching.parquet` (with attribute scores)

### In `./reports/`:
1. `1773333304_nlfv3_alp1_utbeta05_w0_4_4s_added_report.html` (interactive report)
2. `1773333304_nlfv3_alp1_utbeta05_w0_4_pipeline_summary.json` (complete pipeline summary)

---

## ✅ All Files Use Experiment ID

Every file is prefixed with the experiment ID (`1773333304`) extracted from the GCS path, making it easy to:
- Track which experiment the files belong to
- Run multiple experiments without conflicts
- Organize results by experiment

---

## 🎯 Auto-Detection Features

**Variant Engine:**
- Automatically detected as first non-control engine
- Can be explicitly specified with `--variant` if needed

**Column Names:**
- Handles both `contextualQuery` and `query` column names
- Automatically uses correct column suffix based on pair type

**File Discovery:**
- Recursively searches subdirectories (sample-5000, sample-1000, etc.)
- Automatically finds qip_scores, config, metadata, item_attributes

---

## 🚀 Command Options

**Basic (all defaults):**
```bash
python run_analysis_pipeline.py \
    --gcs-path gs://k0k01ls/l1_recall_analysis/ltr_ab_candidates/1773333304
```

**With custom query count:**
```bash
python run_analysis_pipeline.py \
    --gcs-path gs://k0k01ls/l1_recall_analysis/ltr_ab_candidates/1773333304 \
    --queries 2000
```

**Specify variant explicitly:**
```bash
python run_analysis_pipeline.py \
    --gcs-path gs://k0k01ls/l1_recall_analysis/ltr_ab_candidates/1773333304 \
    --variant nlfv3_alp1_utbeta05_w0_4 \
    --queries 2000
```

**Skip steps if already done:**
```bash
python run_analysis_pipeline.py \
    --gcs-path gs://k0k01ls/l1_recall_analysis/ltr_ab_candidates/1773333304 \
    --skip-download \
    --skip-enrichment
```

**Custom filtering:**
```bash
python run_analysis_pipeline.py \
    --gcs-path gs://k0k01ls/l1_recall_analysis/ltr_ab_candidates/1773333304 \
    --queries 2000 \
    --min-total 500 \
    --max-total-diff 10 \
    --min-gain 5
```

---

## ✅ Sanity Check Complete

All components verified:
- ✅ GCS download with correct parameters
- ✅ Query enrichment with Perceive API
- ✅ Auto-detection of variant engine
- ✅ QI pair creation
- ✅ Attribute matching with NLTK stemming
- ✅ HTML report generation
- ✅ All files named with experiment ID
- ✅ No hardcoded file names or engine names
- ✅ Graceful error handling at each step

**Ready to run end-to-end!** 🎯
