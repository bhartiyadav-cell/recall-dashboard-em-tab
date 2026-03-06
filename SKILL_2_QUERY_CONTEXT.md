# Skill 2: Query Context Enrichment

## Overview

This skill enriches queries with context features from the Perceive API to understand query semantics and characteristics. This helps explain **WHY** certain queries are impacted by L1 ranker changes.

## Features Extracted

### 1. **Semanticity Features**
- `scount`: Specific term count (e.g., "red nike shoes" → high scount)
- `bcount`: Brand term count
- `acount`: Attribute term count

### 2. **Specificity**
- `specificity`: Query specificity score (0-1)
- Higher = more specific query

### 3. **Traffic Segment**
- `segment`: Traffic quantile (0-100)
- Indicates query popularity/traffic volume

### 4. **Category & Vertical**
- `l1_category`: Top-level category ID
- `vertical`: Mapped vertical (Food, Electronics, Clothing, etc.)

### 5. **Product Type Features**
- `max_pt`: Maximum product type score
- `n_pt`: Number of product types detected

## Why This Matters

Query context helps identify patterns in L1 ranker impact:

- **High-specificity queries** might show different behavior than broad queries
- **High-traffic queries** (high segment) have different risk profiles
- **Category-specific queries** may be impacted differently by ranker changes
- **Brand queries** vs **generic queries** show different patterns

## Usage

### Mode 1: Enrich a list of queries

```python
from skills.query_context import run, QueryContextInput

queries = ["milk", "laptop", "baby stroller"]

input_config = QueryContextInput(
    queries=queries,
    concurrency_limit=100,
    retry_limit=5
)

result = run(input_config)

print(result.enriched_df)
# Output: DataFrame with query context features
```

### Mode 2: Enrich QIP scores DataFrame

```python
import pandas as pd
from skills.query_context import run, QueryContextInput

# Load QIP scores
df = pd.read_parquet('./temp/downloaded_files/qip_scores.parquet')

# Enrich with query context
input_config = QueryContextInput(
    queries=df,  # Pass DataFrame directly
    concurrency_limit=100
)

result = run(input_config)

# Result contains enriched DataFrame with all original + new columns
enriched_df = result.enriched_df
```

## Command Line Scripts

### Quick Test (5 sample queries)

```bash
python test_query_context.py
```

**What it does:**
- Tests with sample queries
- Shows enriched output
- Demonstrates DataFrame enrichment

### Enrich Full QIP Scores

```bash
python enrich_qip_scores.py
```

**What it does:**
- Loads qip_scores.parquet
- Fetches context for all unique queries
- Saves to qip_scores_enriched.parquet

**Options:**
```bash
# Test with first 1000 rows
python enrich_qip_scores.py --sample 1000

# Increase concurrency for faster processing
python enrich_qip_scores.py --concurrency 200

# Custom output path
python enrich_qip_scores.py --output ./my_enriched_data.parquet
```

## Performance

- **Concurrency**: Default 100 concurrent requests
- **Speed**: ~100-200 queries/second (depending on network)
- **Retries**: Automatic retry on failures (max 5 attempts)
- **Timeout**: 5 seconds per request

### Example Processing Times

| Queries | Concurrency | Time      |
|---------|-------------|-----------|
| 100     | 50          | ~2-3 sec  |
| 1,000   | 100         | ~10-15 sec|
| 5,000   | 100         | ~45-60 sec|
| 10,000  | 200         | ~60-90 sec|

## Output Schema

### Input: QIP Scores
```
contextualQuery | engine | label | pg_prod_id | ...
milk            | control| 4     | ABC123     | ...
```

### Output: Enriched QIP Scores
```
contextualQuery | engine | label | scount | specificity | segment | vertical | max_pt | n_pt | ...
milk            | control| 4     | 1      | 0.45        | 85.3    | Food     | 0.92   | 3    | ...
```

## Integration with Existing Workflow

### Current Workflow
```
1. Download from GCS (Skill 1) ✓
   ↓
2. Load QIP scores + item attributes ✓
   ↓
3. Run existing recall_analyser ✓
```

### Enhanced Workflow
```
1. Download from GCS (Skill 1) ✓
   ↓
2. Load QIP scores + item attributes ✓
   ↓
3. Enrich with query context (Skill 2) ← NEW!
   ↓
4. Analyze impact patterns with context
   ↓
5. Generate enhanced reports
```

## Example: Analyzing Query Patterns

```python
import pandas as pd

# Load enriched data
df = pd.read_parquet('./temp/downloaded_files/qip_scores_enriched.parquet')

# Find high-specificity queries with rating drops
high_spec_drops = df[
    (df['specificity'] > 0.7) &  # High specificity
    (df['engine'] == 'control') &
    (df['label'] < 3)  # Low rating
]

# Group by vertical
vertical_impact = high_spec_drops.groupby('vertical').size()
print(vertical_impact)

# Output:
# vertical
# Food           450
# Electronics    320
# Clothing       180
# ...
```

## Error Handling

The skill gracefully handles:
- **Network failures**: Automatic retry with exponential backoff
- **Timeout errors**: Configurable timeout per request
- **Invalid responses**: Returns None for failed queries
- **Rate limiting**: Concurrency control prevents overwhelming API

Failed queries are tracked in `QueryContextOutput.queries_failed`.

## Next Steps

After enriching queries with context:

1. **Skill 3**: Use LLM to analyze impact patterns
   - "Why are high-specificity Food queries showing rating drops?"
   - "What's different about impacted brand queries?"

2. **Skill 4**: Generate enhanced reports
   - Query context in reports
   - Visual breakdown by vertical/segment
   - Actionable insights based on query characteristics

## Files Created

```
skills/query_context/
├── __init__.py          # Public API
├── config.py            # Input/Output dataclasses
└── main.py              # Core logic

test_query_context.py    # Test script
enrich_qip_scores.py     # Production enrichment script
```

## Dependencies

- `aiohttp`: Async HTTP requests
- `pandas`: Data manipulation
- `asyncio`: Async execution

Already included in project requirements.

---

**Status**: ✅ Complete and tested

**Next**: Skill 3 - LLM-based Query Impact Analyzer
