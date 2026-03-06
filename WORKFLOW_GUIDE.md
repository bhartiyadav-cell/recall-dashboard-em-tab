# Complete Workflow Guide

## Overview

This guide shows the complete workflow from downloading GCS data to enriching it with query context.

---

## Step 1: Download Data from GCS

**Script:** `test_gcs_download.py`

```bash
python test_gcs_download.py
```

**What it does:**
- Downloads `qip_scores.parquet` (3.6M rows)
- Downloads `item_attributes_sample-5000.jsonl` (785K items)
- Saves to: `./temp/downloaded_files/`

**Output:**
```
✓ QIP Scores:  ./temp/downloaded_files/qip_scores.parquet
✓ Metadata:    ./temp/downloaded_files/item_attributes_sample-5000.jsonl
```

---

## Step 2: Load and Join Data

**Script:** `load_data_example.py`

```bash
python load_data_example.py
```

**What it does:**
- Loads QIP scores (ratings)
- Loads item attributes (product details)
- Extracts engine configuration
- Joins on `pg_prod_id`
- Saves merged file

**Output:**
```
✓ Saved: ./temp/downloaded_files/qip_scores_with_attributes.parquet
```

**Data schema:**
```
contextualQuery | engine | label | pg_prod_id | product_name | brand | color | ...
```

---

## Step 3: Enrich with Query Context

**Script:** `enrich_qip_scores.py`

```bash
# Test with sample first
python enrich_qip_scores.py --sample 1000

# Full enrichment
python enrich_qip_scores.py
```

**What it does:**
- Extracts unique queries from QIP scores
- Fetches query context from Perceive API
  - Semanticity (scount, bcount, acount)
  - Specificity scores
  - Traffic segment
  - Category/vertical
  - Product type features
- Merges context back into QIP scores

**Output:**
```
✓ Saved: ./temp/downloaded_files/qip_scores_enriched.parquet
```

**New features added:**
```
scount | specificity | segment | vertical | max_pt | n_pt
```

---

## Step 4: Analyze with Context

Now you can analyze impact patterns using query context:

```python
import pandas as pd

# Load enriched data
df = pd.read_parquet('./temp/downloaded_files/qip_scores_enriched.parquet')

# Example 1: High-specificity queries with low ratings
high_spec_low_rating = df[
    (df['specificity'] > 0.7) &
    (df['engine'] == 'control') &
    (df['label'] < 3)
]

print(f"Found {len(high_spec_low_rating)} high-spec queries with low ratings")

# Example 2: Impact by vertical
vertical_avg = df.groupby(['vertical', 'engine'])['label'].mean().unstack()
print(vertical_avg)

# Example 3: High-traffic queries (segment > 80)
high_traffic = df[df['segment'] > 80]
impact_by_engine = high_traffic.groupby('engine')['label'].mean()
print(f"High-traffic query performance:\n{impact_by_engine}")
```

---

## Complete Data Pipeline

```
┌─────────────────────────────────────────────────────────────┐
│ 1. GCS Download (Skill 1)                                   │
│    python test_gcs_download.py                              │
│    ↓                                                         │
│    qip_scores.parquet + item_attributes.jsonl               │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. Load & Join                                               │
│    python load_data_example.py                              │
│    ↓                                                         │
│    qip_scores_with_attributes.parquet                       │
│    (ratings + product details)                               │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. Query Context Enrichment (Skill 2)                       │
│    python enrich_qip_scores.py                              │
│    ↓                                                         │
│    qip_scores_enriched.parquet                              │
│    (ratings + products + query context)                      │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. Analysis & Reporting (Coming: Skills 3-5)                │
│    - LLM-based impact analysis                              │
│    - Enhanced HTML reports                                   │
│    - Actionable insights                                     │
└─────────────────────────────────────────────────────────────┘
```

---

## Files Generated

After completing all steps:

```
./temp/downloaded_files/
├── qip_scores.parquet                      # Raw QIP scores from GCS
├── item_attributes_sample-5000.jsonl       # Product metadata from GCS
├── qip_scores_with_attributes.parquet      # Joined data (ratings + products)
└── qip_scores_enriched.parquet             # Full enrichment (+ query context)
```

---

## Quick Start (All Steps)

Run all steps in sequence:

```bash
# 1. Download from GCS
python test_gcs_download.py

# 2. Load and join
python load_data_example.py

# 3. Enrich with query context (test with sample first)
python enrich_qip_scores.py --sample 1000

# 4. Full enrichment
python enrich_qip_scores.py
```

---

## Data Schema Evolution

### After Step 1 (Download)
```
qip_scores.parquet:
  contextualQuery, engine, label, pg_prod_id, stores, zipcode, state, ...

item_attributes.jsonl:
  item_id, pg_prod_id, product_name, brand, color, description, ...
```

### After Step 2 (Join)
```
qip_scores_with_attributes.parquet:
  contextualQuery, engine, label, pg_prod_id,
  product_name, brand, color, description, image, ...
  [3.6M rows × ~65 columns]
```

### After Step 3 (Enrich)
```
qip_scores_enriched.parquet:
  contextualQuery, engine, label, pg_prod_id,
  product_name, brand, color, description, image,
  scount, bcount, acount, specificity, segment, vertical, max_pt, n_pt
  [3.6M rows × ~73 columns]
```

---

## Next Steps: Building Skills 3-5

### Skill 3: LLM-based Query Impact Analyzer
- Analyze rating changes with LLM reasoning
- Understand WHY queries are impacted
- Score queries by impact likelihood
- Generate hypotheses about user intent

### Skill 4: Enhanced Report Generator
- Rich HTML reports with product images
- Sunlight URLs for debugging
- Query context visualizations
- LLM-generated insights

### Skill 5: Orchestrator
- Chain all skills together
- Single command workflow
- Configurable pipeline
- End-to-end automation

---

## Performance Notes

### Processing Times (Approximate)

| Step | Data Size | Time       |
|------|-----------|------------|
| 1. Download | ~1.5 GB | 2-5 min    |
| 2. Join     | 3.6M rows | 30-60 sec  |
| 3. Enrich   | 5K queries | 45-60 sec  |

### Optimization Tips

**For faster enrichment:**
```bash
# Increase concurrency (more parallel requests)
python enrich_qip_scores.py --concurrency 200

# Process in batches
python enrich_qip_scores.py --sample 10000 --output batch1.parquet
python enrich_qip_scores.py --sample 20000 --output batch2.parquet
```

**For development:**
```bash
# Always use --sample during development
python enrich_qip_scores.py --sample 500
```

---

## Troubleshooting

### "File not found" errors
Run steps in order. Each step depends on the previous one.

### Perceive API timeouts
Reduce concurrency:
```bash
python enrich_qip_scores.py --concurrency 50
```

### Out of memory
Process in batches using `--sample`

---

**Status**: Steps 1-3 complete ✅

**Next**: Building Skills 3-5 for advanced analysis and reporting
