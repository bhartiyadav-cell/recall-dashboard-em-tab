# Analysis Workflow: Query-Item Attribute Matching

## Overview

This workflow filters QIP scores to the most relevant queries and items for comparison, then analyzes attribute matching patterns.

---

## Step 1: Understand the Filtering Logic

The existing `recall_analyser` identifies relevant queries using:

1. **Query must exist in BOTH engines** (control and variant)
2. **Minimum total items**: `min_total >= 400` (ensures statistical significance)
3. **Similar total counts**: `|total_ctrl - total_var| <= 5` (fair comparison)
4. **Focus on impactful changes**:
   - Missing 4s (highly-rated in control, not in variant)
   - Extra 4s (highly-rated in variant, not in control)
   - Rating changes (different ratings between engines)

---

## Step 2: Filter to Relevant QI Pairs

```bash
python filter_relevant_qips.py
```

**What this does:**
- Loads enriched QIP scores
- Applies same filters as recall_analyser
- Identifies interesting items (missing/extra 4s, rating changes)
- Saves filtered dataset

**Output:**
```
Relevant queries: 50
Total items (filtered): 25,000

Interesting items:
  Missing 4s: 150
  Extra 4s: 120
  Rating changes: 500
    Upgrades: 200
    Downgrades: 300
```

**Saved to:** `./temp/downloaded_files/qip_scores_filtered.parquet`

---

## Step 3: Check Attribute Coverage

Now that we have relevant queries, let's see which have rich attribute annotations:

```bash
python view_enriched_data.py --file ./temp/downloaded_files/qip_scores_filtered.parquet --rows 50 --random --with-intent-only
```

**What to look for:**
- Queries with brand intent (e.g., "nike shoes")
- Queries with color intent (e.g., "red dress")
- Queries with gender intent (e.g., "men's jeans")
- Items with matching attributes in the data

---

## Step 4: Analyze Attribute Matching

```bash
python analyze_query_item_match.py --enriched ./temp/downloaded_files/qip_scores_filtered.parquet --show-examples 20
```

**What this does:**
- Compares query intents with item attributes
- Calculates match scores
- Shows mismatches (query wants X, item has Y)
- Analyzes rating patterns by match score

**Key insights to look for:**
1. **Do mismatches correlate with missing 4s?**
   - Query wants "Nike", item is "Adidas" → missing from variant?

2. **Do mismatches correlate with rating changes?**
   - Color mismatch → rating drops from 4 to 3?

3. **Are there patterns by attribute type?**
   - Brand mismatches more impactful than color?

---

## Step 5: Deep Dive Analysis

### 5a. Focus on Missing 4s

Items that were highly rated in control but missing in variant:

```python
import pandas as pd
import json

df = pd.read_parquet('./temp/downloaded_files/qip_scores_filtered.parquet')

# Filter to missing 4s
missing_4s = df[df['is_missing_4'] == True]

print(f"Missing 4s: {len(missing_4s)}")

# Check attribute matching for these items
for idx, row in missing_4s.head(20).iterrows():
    query = row['contextualQuery']
    product = row['product_name']

    # Parse intents
    brand_intent = json.loads(row.get('brand_intent', '[]'))
    color_intent = json.loads(row.get('color_intent', '[]'))

    print(f"\nQuery: {query}")
    print(f"Product: {product}")

    if brand_intent:
        print(f"  Query wants brand: {brand_intent[0]['value']}")
        print(f"  Item has: {row.get('brand', 'N/A')}")

    if color_intent:
        print(f"  Query wants color: {color_intent[0]['value']}")
        print(f"  Item has: {row.get('color', 'N/A')}")
```

### 5b. Focus on Rating Downgrades

Items that got worse ratings in variant:

```python
# Filter to items in both engines with rating drops
downgrades = df[
    (df['contextualQuery'].isin(df['contextualQuery'].unique())) &
    (df['label'] < 4)  # Lower rating
]

# Compare with control ratings
# (Need to join control and variant data)
```

---

## Expected Insights

### Hypothesis 1: Attribute Mismatch → Rating Drop

**Test:**
- Do items with attribute mismatches get lower ratings?
- Compare match_score vs rating

**Expected:**
```
Match Score    Avg Rating
0-25%          2.3  ← Mismatches = low ratings
75-100%        3.7  ← Good matches = high ratings
```

### Hypothesis 2: Variant Ranker More Sensitive to Attributes

**Test:**
- Compare control vs variant ratings by match score

**Expected:**
```
Engine   Match Score   Avg Rating
control  0-25%        2.5
control  75-100%      3.8
variant  0-25%        2.1  ← Bigger drop!
variant  75-100%      3.7
```

**Interpretation:** Variant ranker punishes attribute mismatches more

### Hypothesis 3: Certain Attributes More Important

**Test:**
- Compare impact of brand vs color vs gender mismatches

**Expected:**
```
Mismatch Type    Avg Rating Drop
Brand            -1.2  ← Most impactful
Product Type     -0.8
Color            -0.4
Gender           -0.3  ← Least impactful
```

---

## Complete Workflow Commands

```bash
# 1. Enrich queries with Perceive API
python enrich_qip_scores.py --queries 100

# 2. Filter to relevant QI pairs (same logic as recall_analyser)
python filter_relevant_qips.py

# 3. View filtered data
python view_enriched_data.py --file ./temp/downloaded_files/qip_scores_filtered.parquet --rows 50 --random --with-intent-only

# 4. Analyze attribute matching
python analyze_query_item_match.py --enriched ./temp/downloaded_files/qip_scores_filtered.parquet --show-examples 20

# 5. Run original recall analysis for comparison
./run_recall_analysis.sh ./temp/downloaded_files/qip_scores.parquet
```

---

## Why This Workflow?

1. **Focus on what matters**: Only analyze queries that actually show differences between engines
2. **Statistical validity**: Use same filters as production recall_analyser (min_total, max_diff)
3. **Actionable insights**: Focus on missing 4s and rating changes
4. **Attribute-aware**: Understand WHY queries are impacted based on attribute matching

---

**Ready to start?**

```bash
# Step 1: Filter relevant QI pairs
python filter_relevant_qips.py

# Step 2: Analyze them
python analyze_query_item_match.py --enriched ./temp/downloaded_files/qip_scores_filtered.parquet
```
