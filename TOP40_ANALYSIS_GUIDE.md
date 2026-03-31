# Top-40 Analysis Guide

## Overview

This guide shows how to enrich your QIP scores with top-40 ranking information from Preso API, enabling you to identify which 4s score changes have the highest impact on user-visible results.

## Impact Categories

### High Impact ⭐⭐⭐
- **4s Added**: Item appears in variant top-40 but NOT in control top-40
  - *User impact*: New 4s rating item becomes visible
- **4s Removed**: Item was in control top-40 but NOT in variant top-40
  - *User impact*: 4s rating item disappears from results

### Medium Impact ⭐⭐
- **4s Added**: Item in top-40 of both control and variant
  - *User impact*: Item already visible, now has better rating
- **4s Removed**: Item in top-40 of both control and variant
  - *User impact*: Item still visible, but rating degraded

### Low Impact ⭐
- **4s Added/Removed**: Item not in top-40 of either control or variant
  - *User impact*: User won't see this change (item not in top results)

## Usage

### Step 1: Filter for 4s Gains/Losses

First, create the filtered QIP file (if you haven't already):

```bash
python filter_4s_gain_queries.py \
  --qip-scores temp/downloaded_files/1773649932_qip_scores.parquet \
  --output temp/downloaded_files/1773649932_qip_4s_filtered.parquet
```

### Step 2: Enrich with Top-40 Rankings

```bash
python enrich_qip_with_top40.py \
  --qip-scores temp/downloaded_files/1773649932_qip_4s_filtered.parquet \
  --contextual-queries contextualQueryfiles/sample-20240901-20250831-5000.jsonl \
  --experiment-config temp/downloaded_files/1773649932_experiment_config.json \
  --output temp/downloaded_files/1773649932_qip_4s_with_top40.parquet \
  --qps 3 \
  --max-workers 6
```

**Note**: This will take ~11 minutes for 56 overlapping queries (2 fetches per query / 3 QPS).

### Step 3: Generate HTML Report

Update your report generation to use the enriched file:

```bash
python generate_4s_report.py \
  --qip-scores temp/downloaded_files/1773649932_qip_4s_with_top40.parquet \
  --experiment-id 1773649932 \
  --variant-name model_16_030_rerankdocs_8000_after_fix \
  --output temp/downloaded_files/1773649932_4s_report_with_top40.html
```

## Output Columns

The enriched QIP file contains:

| Column | Description |
|--------|-------------|
| `in_control_top40` | Boolean - Is item in control top-40? |
| `control_rank` | Int (1-40) or None - Rank in control top-40 |
| `in_variant_top40` | Boolean - Is item in variant top-40? |
| `variant_rank` | Int (1-40) or None - Rank in variant top-40 |
| `top40_impact` | String - Impact category (High/Medium/Low/None) |

## Example Queries

### Find High Impact 4s Gains

```python
import pandas as pd

df = pd.read_parquet('temp/downloaded_files/1773649932_qip_4s_with_top40.parquet')

# High impact 4s added
high_impact_gains = df[
    (df['is_4s_added'] == True) &
    (df['top40_impact'] == 'High')
]

print(f"High impact 4s gains: {len(high_impact_gains)}")
print(high_impact_gains[['query', 'product_id', 'control_rank', 'variant_rank']])
```

### Find High Impact 4s Losses

```python
# High impact 4s removed
high_impact_losses = df[
    (df['is_4s_removed'] == True) &
    (df['top40_impact'] == 'High')
]

print(f"High impact 4s losses: {len(high_impact_losses)}")
print(high_impact_losses[['query', 'product_id', 'control_rank', 'variant_rank']])
```

### Summary Statistics

```python
# Impact distribution
print("Impact distribution:")
print(df['top40_impact'].value_counts())

# Top-40 presence
print(f"\nItems in control top-40: {df['in_control_top40'].sum()}")
print(f"Items in variant top-40: {df['in_variant_top40'].sum()}")
```

## HTML Report Enhancements

The updated HTML report will show:

1. **Impact Badge**: High/Medium/Low colored badges
2. **Control Rank**: Position in control top-40 (or "Not in top-40")
3. **Variant Rank**: Position in variant top-40 (or "Not in top-40")
4. **Rank Change**: Visual indicator of ranking improvement/degradation
5. **Sorting**: Default sort by impact (High first), then by matching score

## Performance Notes

- **Rate Limit**: 3 QPS (safe for Preso API)
- **Parallel Workers**: 6 (balances speed and rate limits)
- **Time Estimate**: ~11 minutes for 56 queries
- **Queries Fetched**: Only those that overlap between qip_scores and JSONL file

## Troubleshooting

### "No overlapping queries"
- Check that your JSONL file contains queries from your qip_scores
- You may need a different sample file for your experiment

### "Rate limit exceeded"
- Reduce `--qps` to 2
- Reduce `--max-workers` to 4

### "Timeouts"
- Check network connectivity
- Verify Preso API access key is valid

## Next Steps

After enrichment, you can:

1. **Focus on High Impact Items**: Prioritize fixing/validating high impact changes
2. **Analyze Ranking Changes**: Study why items moved in/out of top-40
3. **Compare with Metrics**: Correlate top-40 changes with CTR, conversion, etc.
4. **Create Dashboards**: Visualize top-40 impact across queries
