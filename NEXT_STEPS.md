# Next Steps After Successful Download

## ✅ What You Should Have Now

After running `python test_gcs_download.py`, you should have downloaded:

1. **QIP Scores** (`output_qips_total.parquet`)
   - ~1.1M query-item-rating rows
   - Has: ratings (1-4), basic item info, stores/zipcode/state
   - Contains engine name (control vs variant)

2. **Item Attributes** (`item_attributes_*.jsonl`)
   - ~785K unique items
   - Has: detailed descriptions, images, URLs
   - Rich metadata for LLM-based reasoning

## 🔍 Verify Download

```bash
# Check what was downloaded
ls -lh ./temp/downloaded_files/

# Should show:
# output_qips_total.parquet       (~100-200 MB)
# item_attributes_sample-5000.jsonl (~50-100 MB)
```

## 🧪 Test Loading the Data

```bash
python load_data_example.py
```

This will:
- ✅ Load QIP scores
- ✅ Load item attributes
- ✅ Extract engine names (control vs variant)
- ✅ Extract Sunlight metadata (stores, zipcode, state)
- ✅ Join the data on `pg_prod_id`
- ✅ Save merged file: `qip_scores_with_attributes.parquet`

## 📊 Expected Output

```
============================================================
Loading Data from Downloaded Files
============================================================
Loading QIP scores from: ./temp/downloaded_files/output_qips_total.parquet
  Loaded 1,106,194 rows
  Columns: ['query', 'item_id', 'brand', 'gender', 'product_type', 'source',
            'pg_prod_id', 'title', 'long_descr', 'description', 'color',
            'attrsuccess', 'query_pt', 'query_pt_scores', 'rating_score',
            'stores', 'zipcode', 'state', 'polarisUrl', 'engine', ...]
  Unique queries: 5,000
  Unique items: 100,000

Extracting ptss/trsp parameters...
  Found engines: ['control', 'nlfv3_alp1_utbeta05_w0_4']

Extracting metadata for Sunlight URLs...
  Metadata: {'stores': 100, 'zipcode': '72712', 'state': 'AR', ...}

Loading item attributes from: ./temp/downloaded_files/item_attributes_sample-5000.jsonl
  Loaded 785,128 items
  Columns: ['item_id', 'product_name', 'brand', 'color', 'gender',
            'product_type', 'description', 'source', 'pg_prod_id', ...]

Joining QIP scores with item attributes...
  Joining on pg_prod_id...
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

## 🎯 What This Gets You

### For Existing Recall Analysis
```python
# Use with recall_analyser.py (existing code)
from recall_analyser import RecallAnalyser

analyser = RecallAnalyser(
    qip_scores_path='./temp/downloaded_files/output_qips_total.parquet',
    control_engine='control',
    variant_engine='nlfv3_alp1_utbeta05_w0_4'
)

analyser.run_analysis('./output')
```

### For Enhanced LLM-Based Analysis (New!)
```python
# Use merged data with rich attributes
import pandas as pd

merged_df = pd.read_parquet('./temp/downloaded_files/qip_scores_with_attributes.parquet')

# Now you have:
# - Ratings (1-4) from QIP scores
# - Full product descriptions for LLM reasoning
# - Images and URLs for display
# - All metadata for Sunlight URLs

# Example: Get items that lost rating 4 in variant
lost_4s = merged_df[
    (merged_df['rating_score'] == 4) &
    (merged_df['engine'] == 'control')
]

# Check if they appear in variant with lower rating
for _, item in lost_4s.iterrows():
    variant_row = merged_df[
        (merged_df['pg_prod_id'] == item['pg_prod_id']) &
        (merged_df['query'] == item['query']) &
        (merged_df['engine'] == 'nlfv3_alp1_utbeta05_w0_4')
    ]

    if not variant_row.empty:
        new_rating = variant_row.iloc[0]['rating_score']
        if new_rating < 4:
            print(f"Query: {item['query']}")
            print(f"Product: {item['product_name']}")
            print(f"Description: {item['description'][:200]}...")
            print(f"Rating dropped: 4 → {new_rating}")
            print()
```

## 🔧 Configuration Extracted

From the data, you now have:

```python
config = {
    'control_engine': 'control',
    'variant_engine': 'nlfv3_alp1_utbeta05_w0_4',
    'stores': 100,
    'zipcode': '72712',
    'state': 'AR',
    'sample_size': 5000
}
```

No separate config file needed! ✅

## 🚀 Ready for Skill 2: Enhanced Analysis

Now that we have:
- ✅ Downloaded files from GCS
- ✅ Loaded and joined the data
- ✅ Extracted engine configuration
- ✅ Have rich item attributes

We can move to building skills for:

### Skill 2: Config Extractor
Extract and structure the configuration from QIP scores
- Engine names (control vs variants)
- Metadata for Sunlight URLs
- Experiment parameters

### Skill 3: LLM-Based Query Analysis
Use LLM to analyze why ratings changed
- Compare descriptions of gained vs lost items
- Identify patterns in rating changes
- Generate hypotheses about experiment impact

### Skill 4: Enhanced Report Generator
Create reports with:
- Model comparison section
- LLM reasoning for rating changes
- Rich item details (images, descriptions)
- Sunlight URLs

## 📚 Documentation

- **DATA_STRUCTURE.md** - Complete data schema guide
- **load_data_example.py** - Working code to load and join data
- **recall_analyser.py** - Existing recall analysis (still works!)

## 🎉 Success Checklist

- [ ] Downloaded files successfully
- [ ] Loaded QIP scores (1M+ rows)
- [ ] Loaded item attributes (785K items)
- [ ] Joined data on pg_prod_id
- [ ] Extracted engine names
- [ ] Extracted Sunlight metadata
- [ ] Saved merged parquet file

Once all checked, you're ready to build the enhanced analysis! 🚀

---

**Next command to run:**
```bash
python load_data_example.py
```
