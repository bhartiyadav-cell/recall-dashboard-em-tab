# Data Structure Guide

## Overview

Your recall analysis data consists of two main files that need to be joined:

1. **QIP Scores** (`output_qips_total.parquet`) - Query-Item-Position scores with ratings
2. **Item Attributes** (`item_attributes_*.jsonl`) - Detailed product metadata

## File 1: QIP Scores (Parquet)

### Location
```
gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383/sample-5000/
  └── df61e2d8a2424e66bbb5bb1cd79b2c09/
      └── cross_encoder_eval/
          └── inference_op/
              └── output_qips_total.parquet
```

### Key Columns

| Column | Type | Description | Used For |
|--------|------|-------------|----------|
| `query` | str | Search query text | Analysis grouping |
| `item_id` | str | Walmart item ID | Item identification |
| `pg_prod_id` | str | Product group ID | Joining with attributes |
| `rating_score` | int | 1-4 rating | Label (4=excellent, 1=poor) |
| `brand` | str | Product brand | Basic filtering |
| `gender` | str | Target gender | Basic filtering |
| `product_type` | str | Category | Basic filtering |
| `source` | str | Data source (hubble/uber) | Tracking |
| `title` | str | Product title | Display |
| `description` | str | Short description | Display |
| `color` | str | Product color | Attribute |
| `attrsuccess` | float | Attribute quality | Data quality |
| `stores` | int | Store ID | **Sunlight URLs** |
| `zipcode` | str | ZIP code | **Sunlight URLs** |
| `state` | str | State code | **Sunlight URLs** |
| `polarisUrl` | str | Original URL | **Extract ptss/trsp** |
| `engine` | str | Engine variant | **Control vs variant** |

### Example Row
```python
query: "milk"
item_id: "44391061"
pg_prod_id: "4ONKSWBD5WA8"
rating_score: 4  # Excellent match
brand: "Hiland"
product_type: "Milks"
stores: 100
zipcode: "72712"
state: "AR"
engine: "nlfv3_alp1_utbeta05_w0_4"  # Variant name
```

### Size
- ~1.1M rows in sample-5000
- Each row = one query-item pair with rating

---

## File 2: Item Attributes (JSONL)

### Location
```
gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383/sample-5000/
  └── item_attributes_sample-5000.jsonl
```

### Key Columns

| Column | Type | Description | Used For |
|--------|------|-------------|----------|
| `item_id` | str | Walmart item ID | Joining |
| `pg_prod_id` | str | Product group ID | Joining (preferred) |
| `product_name` | str | Full product name | Display & analysis |
| `brand` | str | Product brand | Filtering |
| `color` | str | Product color | Attribute-based analysis |
| `gender` | str | Target gender | Filtering |
| `product_type` | str | Category | Filtering |
| `description` | str | Detailed description | **LLM reasoning** |
| `source` | str | Data source | Tracking |
| `attrsuccess` | int | 0/1 attribute quality | Data quality |
| `url` | str | Product URL | Links |
| `image` | str | Product image URL | Display |

### Example Row
```python
item_id: "821225909"
pg_prod_id: "5BJPH2R6HZDX"
product_name: "(20 pack) Barilla Classic Non-GMO, Kosher Certified Linguine Pasta..."
brand: "Barilla"
color: "Multicolor"
gender: "Unisex"
product_type: "Pasta"
description: "<ul><li>Al dente perfection in 9-10 minutes</li>..."
url: "https://www.walmart.com/ip/Barilla-Pasta-Linguine..."
image: "https://i5.walmartimages.com/asr/565c428f..."
```

### Size
- ~785K unique items in sample-5000
- Much richer product information than QIP scores

---

## How to Join the Data

### Join Key: `pg_prod_id`

```python
merged = qip_df.merge(
    attr_df,
    on='pg_prod_id',
    how='left',  # Keep all QIP rows
    suffixes=('', '_attr')  # Avoid column name conflicts
)
```

### Why Join?

**QIP Scores have:**
- ✓ Ratings (label 1-4)
- ✓ Query context
- ✓ Basic item info
- ✓ Stores/zipcode/state for Sunlight

**Item Attributes have:**
- ✓ Detailed product descriptions
- ✓ Full product names
- ✓ Product images
- ✓ Rich metadata for LLM reasoning

**Merged data has:**
- ✅ Everything! Ratings + rich attributes

---

## Extracting Config Info (ptss/trsp)

### No Separate Config File Needed!

The `polarisUrl` column in QIP scores contains ptss/trsp:

```python
polarisUrl: "http://preso-usgm-wcnp.prod.walmart.com/v1/search?
  prg=desktop&
  stores=100&
  ptss=l1_ranker_use_legacy_config:on;l1_ranker_unified_config:on&
  trsp=l1_ranker_unified_config.expt_id:nlfv3_alp1_utbeta05_w0_4&
  ..."
```

**Extract:**
- `ptss`: Permanent test switches
- `trsp`: Traffic split parameters
- `expt_id`: Variant engine name

### Engine Detection

Alternatively, use the `engine` column:

```python
engines = qip_df['engine'].unique()
# ['control', 'nlfv3_alp1_utbeta05_w0_4']

control = 'control'
variant = [e for e in engines if e != 'control'][0]
```

---

## Building Sunlight URLs

### Required from QIP Scores:
- `query` - Search query text
- `stores` - Store ID (e.g., 100)
- `zipcode` - ZIP code (e.g., "72712")
- `state` - State code (e.g., "AR")
- `pg_prod_id` - Item IDs to highlight
- `engine` - Variant name (for trsp parameter)

### URL Format:
```
https://sunlight.walmart.com/debugReport?
  q={query}&
  endpoint=http://preso-usgm-wcnp.prod.walmart.com/v1/search?
    prg=desktop&
    stores={stores}&
    stateOrProvinceCode={state}&
    zipcode={zipcode}&
    trsp=l1_ranker_unified_config.expt_id:{engine}&
  items_affStack1_SBE_EMM={pg_prod_ids}
```

See existing `build_sunlight_url()` in `recall_analyser.py`

---

## Data Flow

```
1. Download Files
   ↓
   ├─ qip_scores.parquet          (1.1M rows)
   └─ item_attributes.jsonl       (785K items)

2. Load Data
   ↓
   ├─ QIP: ratings + basic info
   └─ Attrs: detailed product info

3. Extract Config
   ↓
   ├─ Engine: "nlfv3_alp1_utbeta05_w0_4"
   ├─ Control: "control"
   └─ Metadata: stores, zipcode, state

4. Join Data
   ↓
   merged_df = qip + attrs (on pg_prod_id)

5. Use for Analysis
   ↓
   ├─ Recall comparison (existing)
   ├─ LLM-based reasoning (new)
   └─ Sunlight URL generation
```

---

## Usage Example

```python
from skills.gcs_download import run, GCSDownloadInput

# 1. Download files
result = run(GCSDownloadInput(
    gs_path='gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383'
))

# 2. Load QIP scores
qip_df = pd.read_parquet(result.qip_scores_path)

# 3. Load item attributes
attr_df = pd.read_json(result.metadata_path, lines=True)

# 4. Extract config
engines = qip_df['engine'].unique()
control = 'control'
variant = [e for e in engines if e != 'control'][0]

# 5. Join data
merged = qip_df.merge(attr_df, on='pg_prod_id', how='left')

# 6. Use for analysis
# - Split by engine
# - Compare ratings
# - Build Sunlight URLs
# - Run LLM reasoning on descriptions
```

---

## Key Insights

1. **No config file needed** - Extract from `polarisUrl` or `engine` column
2. **Join on `pg_prod_id`** - More reliable than `item_id`
3. **Rich attributes enable LLM reasoning** - Use `description` field
4. **Sunlight metadata in QIP scores** - No separate metadata file needed
5. **JSONL format** - Use `pd.read_json(..., lines=True)`

---

## Next Steps

See `load_data_example.py` for complete working code!
