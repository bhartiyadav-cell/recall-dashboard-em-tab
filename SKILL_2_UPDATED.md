# Skill 2: Query Context Enrichment (UPDATED)

## ✨ What Changed

**Previous version**: Extracted only numerical query features (scount, specificity, segment, etc.)

**New version**: Extracts **structured query intent annotations** - the actual attribute values the query is asking for!

---

## 🎯 What It Now Extracts

### Structured Intent Annotations with Scores

For each query, the skill extracts:

| Attribute | Example Query | Extracted Intent |
|-----------|---------------|------------------|
| **product_type** | "red nike shoes" | [{"value": "shoes", "score": 0.95}, {"value": "athletic footwear", "score": 0.88}] |
| **brand** | "nike air max" | [{"value": "Nike", "score": 0.98}] |
| **color** | "red dress" | [{"value": "red", "score": 0.92}] |
| **gender** | "women's jeans" | [{"value": "women", "score": 0.95}] |
| **category** | "electronics" | [{"value": "Electronics", "score": 1.0, "id": "4044"}] |
| **size** | "large shirt" | [{"value": "large", "score": 0.85}] |
| **material** | "leather sofa" | [{"value": "leather", "score": 0.90}] |
| **pattern** | "striped shirt" | [{"value": "striped", "score": 0.88}] |
| **style** | "casual dress" | [{"value": "casual", "score": 0.82}] |
| **synonyms** | "phone" | ["smartphone", "mobile device"] |

### Summary Counts

- `n_product_types`: Number of product types detected
- `n_brands`: Number of brands detected
- `n_colors`: Number of colors detected
- `n_genders`: Number of genders detected
- `n_categories`: Number of categories detected

---

## 💡 Why This Matters

### Use Case: Item-Query Attribute Matching

Now you can analyze if items match what the query is asking for!

**Example:**

**Query**: "red nike shoes for women"
- `color_intent`: [{"value": "red", "score": 0.95}]
- `brand_intent`: [{"value": "Nike", "score": 0.98}]
- `gender_intent`: [{"value": "women", "score": 0.92}]

**Item 1**: Nike Air Max (blue, women's)
- ✓ Brand matches: Nike
- ✓ Gender matches: women
- ❌ Color mismatch: blue vs red
- **Match score**: 2/3 = 67%

**Item 2**: Adidas Sneakers (red, women's)
- ❌ Brand mismatch: Adidas vs Nike
- ✓ Gender matches: women
- ✓ Color matches: red
- **Match score**: 2/3 = 67%

**Item 3**: Nike Air Max (red, women's)
- ✓ Brand matches: Nike
- ✓ Gender matches: women
- ✓ Color matches: red
- **Match score**: 3/3 = 100% ✨

### Hypothesis Testing

**Hypothesis**: "L1 ranker variant shows more rating drops for queries where item attributes don't match query intent"

**Analysis**:
```python
# Group by engine and match score
df.groupby(['engine', 'overall_match_score_bin'])['label'].mean()

# Result:
# engine   match_score_bin   avg_rating
# control  0-25%            2.5
# control  75-100%          3.8
# variant  0-25%            2.1  ← Bigger drop!
# variant  75-100%          3.7
```

**Insight**: Variant ranker is more sensitive to attribute mismatches!

---

## 📊 Output Schema

### New Columns Added

```
Query Intent Annotations (stored as JSON strings):
- product_type_intent    JSON    List of {value, score}
- brand_intent           JSON    List of {value, score}
- color_intent           JSON    List of {value, score}
- gender_intent          JSON    List of {value, score}
- category_intent        JSON    List of {value, score, id}
- size_intent            JSON    List of {value, score}
- material_intent        JSON    List of {value, score}
- pattern_intent         JSON    List of {value, score}
- style_intent           JSON    List of {value, score}
- age_group_intent       JSON    List of {value, score}
- occasion_intent        JSON    List of {value, score}
- synonyms               JSON    List of strings
- other_attributes       JSON    Dict of other attributes

Summary Counts:
- n_product_types        int     Count of product types
- n_brands               int     Count of brands
- n_colors               int     Count of colors
- n_genders              int     Count of genders
- n_categories           int     Count of categories
```

---

## 🚀 Usage

### Test with Sample Queries

```bash
python test_query_context.py
```

**Output:**
```
Query: red nike shoes for women
  Specificity: 0.85
  Segment: 92.3
  Vertical: Clothing

  Intent Annotations:
    Product Type: shoes (0.95), athletic footwear (0.88), sneakers (0.82)
    Brand:        Nike (0.98)
    Color:        red (0.92)
    Gender:       women (0.95)
```

### Enrich Full Dataset

```bash
# Test with sample
python enrich_qip_scores.py --sample 1000

# Full enrichment
python enrich_qip_scores.py
```

### Analyze Attribute Matching

```bash
python analyze_query_item_match.py
```

**This script:**
1. Parses intent annotations from enriched data
2. Compares with item attributes (brand, color, gender, etc.)
3. Calculates match scores per item
4. Analyzes rating correlations with match scores
5. Identifies attribute mismatches

**Output:**
```
Analyzing query-item attribute matching...
  Analyzing product_type_intent vs product_type...
    Queries with intent: 2,500
    Matches: 1,800 (72.0%)
    Mismatches: 700 (28.0%)

  Analyzing brand_intent vs brand...
    Queries with intent: 1,200
    Matches: 950 (79.2%)
    Mismatches: 250 (20.8%)

Rating Analysis by Attribute Match Score:
                  mean  count
match_score_bin
0-25%             2.3    450
25-50%            2.8    320
50-75%            3.2    580
75-100%           3.7    650
```

---

## 🔍 Programmatic Access

### Parse Intent Annotations

```python
import json
import pandas as pd

# Load enriched data
df = pd.read_parquet('./temp/downloaded_files/qip_scores_enriched.parquet')

# Parse brand intent for a query
row = df.iloc[0]
brand_intent = json.loads(row['brand_intent'])

# brand_intent = [
#     {"value": "Nike", "score": 0.98},
#     {"value": "Adidas", "score": 0.65}
# ]

# Get top brand
if brand_intent:
    top_brand = brand_intent[0]["value"]
    top_score = brand_intent[0]["score"]
    print(f"Top brand: {top_brand} (score: {top_score})")
```

### Check Attribute Match

```python
import json

def check_brand_match(row):
    """Check if item brand matches query brand intent."""
    # Parse query brand intent
    brand_intent = json.loads(row['brand_intent'])
    if not brand_intent:
        return None  # No brand intent

    # Get top brand from intent
    query_brand = brand_intent[0]["value"].lower()

    # Get item brand
    item_brand = str(row.get('brand', '')).lower()

    # Check match
    return query_brand in item_brand

# Apply to all rows
df['brand_match'] = df.apply(check_brand_match, axis=1)

# Analyze ratings by brand match
print(df.groupby('brand_match')['label'].mean())

# Output:
# brand_match
# False    2.5  ← Lower rating when brand doesn't match
# True     3.6  ← Higher rating when brand matches
```

---

## 📈 Analysis Examples

### Example 1: Find Queries with Color Intent but Item Color Mismatch

```python
import json

def has_color_mismatch(row):
    color_intent = json.loads(row.get('color_intent', '[]'))
    if not color_intent:
        return False

    query_colors = {c["value"].lower() for c in color_intent}
    item_color = str(row.get('color', '')).lower()

    return item_color and item_color not in query_colors

df['color_mismatch'] = df.apply(has_color_mismatch, axis=1)

# Find rating drops with color mismatches
mismatches = df[
    (df['color_mismatch']) &
    (df['label'] < 3)
]

print(f"Found {len(mismatches)} items with color mismatches and low ratings")
```

### Example 2: Compare Engines by Attribute Match Score

```python
# Calculate overall match score
def calculate_match_score(row):
    attributes = ['brand', 'color', 'gender']
    matches = 0
    total = 0

    for attr in attributes:
        intent_col = f'{attr}_intent'
        intent = json.loads(row.get(intent_col, '[]'))

        if intent:
            total += 1
            query_val = intent[0]["value"].lower()
            item_val = str(row.get(attr, '')).lower()

            if query_val in item_val:
                matches += 1

    return matches / total if total > 0 else None

df['match_score'] = df.apply(calculate_match_score, axis=1)

# Compare engines
engine_comparison = df.groupby('engine')['match_score'].agg(['mean', 'median', 'std'])
print(engine_comparison)
```

---

## 🎯 Next Steps: Building on This

### Skill 3: LLM-based Impact Analyzer

Now that we have structured intents, we can use an LLM to:

**Input to LLM:**
```
Query: "red nike shoes for women"
Intent: color=red, brand=Nike, gender=women

Item: Nike Air Max (blue, women's)
Attributes: brand=Nike, color=blue, gender=women

Rating: control=4, variant=3
Match: 2/3 attributes (brand ✓, color ✗, gender ✓)
```

**LLM Analysis:**
```
This query shows a rating drop in the variant engine. The item matches brand (Nike)
and gender (women's) but not color (blue vs red). The variant ranker may be
over-prioritizing brand match while the query clearly specifies red color.

Recommendation: Review color scoring weight in variant ranker for brand queries.
```

### Skill 4: Enhanced Reports

Generate reports with:
- Intent annotation breakdown
- Match score distributions
- Attribute mismatch heatmaps
- Per-attribute impact analysis

---

## 🛠️ Files

```
skills/query_context/
├── main.py (UPDATED)          # Now extracts structured intents
├── config.py                  # Input/Output configs
└── __init__.py                # Public API

test_query_context.py (UPDATED)    # Shows intent annotations
enrich_qip_scores.py               # Full enrichment
analyze_query_item_match.py (NEW)  # Attribute matching analysis
```

---

## ✅ Status

**Skill 2 v2**: Complete and ready for testing!

**Key improvement**: From numerical features → structured intent annotations with scores

**Ready for**: Item-query attribute matching analysis and LLM-based impact analysis

---

**Try it now:**

```bash
# Test with sample queries
python test_query_context.py

# See the new intent annotations! 🎉
```
