# Preso Fetcher Guide

## Overview

The Preso Fetcher skill fetches top-40 search results from Preso API for both control and variant configurations. It:

- ✅ Respects rate limits (default: 3 QPS)
- ✅ Maps queries to full contextualQuery data (prg, stores, zipcode, etc.)
- ✅ Fetches results in parallel with thread pool
- ✅ Returns structured DataFrame with product IDs, ranks, and URLs

## Requirements

1. **qip_scores.parquet** - Contains queries to fetch
2. **sample JSONL file** - Contains full contextualQuery data (e.g., `sample-20240901-20250831-5000.jsonl`)
3. **Experiment config** - JSON with control and variant engine configurations

## Usage

### Basic Example

```python
from skills.preso_fetcher import fetch_preso_results, PresoFetcherInput

input_config = PresoFetcherInput(
    qip_scores_file="temp/downloaded_files/1773649932_qip_scores.parquet",
    contextual_queries_file="contextualQueryfiles/sample-20240901-20250831-5000.jsonl",
    experiment_config_file="temp/downloaded_files/1773649932_experiment_config.json",
    qps=3,           # Queries per second (stay at 3 for safety)
    max_workers=6,   # Parallel workers
    top_n=40,        # Results per query
    start_idx=0,     # Start from first query
    end_idx=None     # Process all queries
)

result = fetch_preso_results(input_config)

if result.status == "success":
    print(f"Fetched {result.queries_processed} queries")
    print(f"Results: {len(result.results_df)} rows")

    # Save results
    result.results_df.to_parquet("preso_results.parquet", index=False)
```

### Test with Subset

To test with just a few queries first:

```python
input_config = PresoFetcherInput(
    qip_scores_file="temp/downloaded_files/1773649932_qip_scores.parquet",
    contextual_queries_file="contextualQueryfiles/sample-20240901-20250831-5000.jsonl",
    experiment_config_file="temp/downloaded_files/1773649932_experiment_config.json",
    qps=3,
    max_workers=6,
    top_n=40,
    start_idx=0,
    end_idx=10  # Test with first 10 queries
)
```

### Command Line

```bash
# Test with sample data
python test_preso_fetcher.py

# The test script processes first 5 queries and saves results
```

## Output Structure

The results DataFrame contains:

| Column | Description |
|--------|-------------|
| `query` | Search query string |
| `contextualQuery` | Full contextual query dict (prg, stores, zipcode, etc.) |
| `rank` | Result rank (1-40) |
| `product_id` | Product/Item ID |
| `title` | Product title |
| `engine` | 'control' or 'variant' |
| `url` | Full Preso API URL used to fetch results |

Example:
```
   query  contextualQuery  rank  product_id  title         engine  url
0  milk   {...}            1     12345678    Whole Milk    control http://...
1  milk   {...}            2     87654321    2% Milk       control http://...
2  milk   {...}            1     98765432    Organic Milk  variant http://...
```

## Rate Limiting

**Important**: Stay at **3 QPS or lower** to avoid overwhelming the Preso API.

- QPS=3 with 6 workers is safe and efficient
- For 1000 queries: ~11 minutes (2 fetches per query = control + variant)
- The system automatically throttles requests using a thread-safe rate limiter

## Contextual Query Mapping

The skill automatically maps queries from `qip_scores.parquet` to full contextual queries:

1. Loads sample JSONL file with full contextualQuery data
2. For each query in qip_scores:
   - Tries to find exact match in sample file
   - Falls back to minimal context (desktop, no store) if not found
3. Uses full context for Preso API calls

This ensures proper context (prg, stores, zipcode) is used for each query.

## Error Handling

- Failed queries are logged and counted
- Other queries continue processing
- Check `result.queries_failed` to see how many failed
- Individual errors are printed during execution

## Integration with Pipeline

To integrate into your analysis pipeline:

```python
# After creating qip_scores.parquet
from skills.preso_fetcher import fetch_preso_results, PresoFetcherInput

# Fetch Preso results
fetcher_input = PresoFetcherInput(
    qip_scores_file=qip_file,
    contextual_queries_file="contextualQueryfiles/sample-20240901-20250831-5000.jsonl",
    experiment_config_file=config_file,
    qps=3,
    max_workers=6,
    top_n=40
)

fetcher_result = fetch_preso_results(fetcher_input)

if fetcher_result.status == "success":
    # Save results
    preso_results_file = "temp/preso_results.parquet"
    fetcher_result.results_df.to_parquet(preso_results_file, index=False)

    # Continue with analysis using preso_results.parquet
    # This file now contains actual top-40 results from both engines
```

## Parameters Reference

### PresoFetcherInput

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `qip_scores_file` | str | Required | Path to qip_scores.parquet |
| `contextual_queries_file` | str | Required | Path to sample JSONL with contextualQuery data |
| `experiment_config` | str | None | Config JSON string (alternative to file) |
| `experiment_config_file` | str | None | Path to experiment config JSON |
| `access_key` | str | "532c..." | Preso API access key |
| `qps` | int | 3 | Queries per second rate limit |
| `max_workers` | int | 6 | Number of parallel workers |
| `top_n` | int | 40 | Results per query |
| `start_idx` | int | 0 | Start index for queries |
| `end_idx` | int | None | End index (None = all queries) |

### PresoFetcherOutput

| Attribute | Type | Description |
|-----------|------|-------------|
| `results_df` | DataFrame | Results with query, rank, product_id, etc. |
| `control_urls` | List[str] | List of control URLs |
| `variant_urls` | List[str] | List of variant URLs |
| `status` | str | 'success' or 'error' |
| `message` | str | Status message |
| `queries_processed` | int | Number of successful queries |
| `queries_failed` | int | Number of failed queries |

## Troubleshooting

### "No contextual queries found"
- Check that the JSONL file path is correct
- Verify the JSONL file has `contextualQuery` fields

### "Rate limit exceeded"
- Reduce `qps` parameter to 2 or lower
- Reduce `max_workers` to 4 or lower

### "Timeout errors"
- Increase timeout in the code (default: 30 seconds)
- Check network connectivity to Preso API

### "No results returned"
- Check if queries exist in sample JSONL file
- Verify experiment config has valid engine configurations
- Check Preso API access key is valid

## Next Steps

After fetching Preso results, you can:

1. **Compare with QIP scores** - Join results with qip_scores on query + product_id
2. **Analyze ranking differences** - Compare ranks between control and variant
3. **Generate reports** - Create visualizations of ranking changes
4. **Calculate metrics** - Compute NDCG, MRR, or custom metrics
