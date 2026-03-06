# Quick Reference Card

## 🚀 Commands to Run

```bash
# 1. Download files from GCS (handles subdirectories automatically)
python test_gcs_download.py

# 2. Load and explore the data
python load_data_example.py

# 3. Run existing recall analysis
python -m recall_analyser analyse ./temp/downloaded_files/output_qips_total.parquet \
    --variant-engine nlfv3_alp1_utbeta05_w0_4
```

## 📂 What Gets Downloaded

| File | Size | Contains |
|------|------|----------|
| `output_qips_total.parquet` | ~150 MB | Ratings (1-4), engine names, Sunlight metadata |
| `item_attributes_*.jsonl` | ~75 MB | Product descriptions, images, URLs |

## 📊 Key Data Fields

### QIP Scores (Parquet)
```python
query           # "milk"
item_id         # "44391061"
pg_prod_id      # "4ONKSWBD5WA8"
rating_score    # 4 (excellent) to 1 (poor)
engine          # "control" or "nlfv3_alp1_utbeta05_w0_4"
stores          # 100
zipcode         # "72712"
state           # "AR"
```

### Item Attributes (JSONL)
```python
pg_prod_id      # "4ONKSWBD5WA8"
product_name    # "Hiland Whole, Vitamin D Milk, Gallon"
brand           # "Hiland"
description     # Full HTML description
image           # Image URL
url             # Product URL
```

## 🔗 Joining Data

```python
import pandas as pd

# Load both files
qip_df = pd.read_parquet('output_qips_total.parquet')
attr_df = pd.read_json('item_attributes_*.jsonl', lines=True)

# Join on pg_prod_id
merged = qip_df.merge(attr_df, on='pg_prod_id', how='left')

# Now you have ratings + rich attributes!
```

## 🎯 Extract Configuration

```python
# Get engine names
engines = qip_df['engine'].unique()
# ['control', 'nlfv3_alp1_utbeta05_w0_4']

control = 'control'
variant = [e for e in engines if e != 'control'][0]

# Get Sunlight metadata
sample = qip_df.iloc[0]
stores = sample['stores']     # 100
zipcode = sample['zipcode']   # "72712"
state = sample['state']       # "AR"
```

## 🔍 Common Queries

### Find items that lost rating 4
```python
# Items rated 4 in control
control_4s = merged[
    (merged['engine'] == 'control') &
    (merged['rating_score'] == 4)
]

# Check their rating in variant
for _, item in control_4s.iterrows():
    variant = merged[
        (merged['pg_prod_id'] == item['pg_prod_id']) &
        (merged['query'] == item['query']) &
        (merged['engine'] == variant_engine)
    ]
    if not variant.empty and variant.iloc[0]['rating_score'] < 4:
        print(f"Lost 4: {item['product_name']}")
```

### Get queries with biggest rating drops
```python
comparison = merged.groupby(['query', 'engine'])['rating_score'].mean().unstack()
comparison['drop'] = comparison['control'] - comparison[variant_engine]
biggest_drops = comparison.nlargest(10, 'drop')
```

## 🌐 Building Sunlight URLs

```python
from recall_analyser import build_sunlight_url

url = build_sunlight_url(
    query="milk",
    stores=100,
    zipcode="72712",
    state="AR",
    item_ids=["4ONKSWBD5WA8", "4MGTRUJY3K1R"],
    engine="nlfv3_alp1_utbeta05_w0_4"
)
```

## 📁 File Locations

```
./temp/downloaded_files/
├── output_qips_total.parquet           ← QIP scores
├── item_attributes_sample-5000.jsonl   ← Item metadata
└── qip_scores_with_attributes.parquet  ← Merged (created by load_data_example.py)

./output/                                ← Analysis results
├── comparison.csv
├── ttest_overall.csv
├── label_comparison.png
└── recall_ranker_comparison.html
```

## 🐛 Troubleshooting

| Issue | Solution |
|-------|----------|
| Files not found | Check GCS path and run permissions |
| Authentication error | Run `gcloud auth application-default login` |
| Join returns no matches | Check if `pg_prod_id` exists in both files |
| Missing columns | File format may have changed, check schema |

## 📖 Documentation Files

- **STATUS.md** - Current status and what works
- **DATA_STRUCTURE.md** - Complete schema guide
- **NEXT_STEPS.md** - What to do after download
- **load_data_example.py** - Working code examples

## ✅ Success Criteria

After running the commands, you should have:
- [x] Files downloaded to `./temp/downloaded_files/`
- [x] QIP scores loaded (1M+ rows)
- [x] Item attributes loaded (785K items)
- [x] Data joined successfully
- [x] Engine configuration extracted
- [x] Ready for analysis

---

**Start here:** `python test_gcs_download.py`
