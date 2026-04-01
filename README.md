# L1 Ranker Recall Analysis Pipeline

End-to-end pipeline for analyzing L1 ranker recall differences between control and variant models, with query enrichment, attribute matching, and top-40 Preso results analysis.

## Features

- **Two Pipeline Versions:**
  - **V1 (GCS-based):** Direct GCS path input, includes pgls_id fetching
  - **V2 (Email-based):** Extracts config from experiment emails, includes Preso top-40 crawl and pgls_id fetching

- **Query Enrichment:** Perceive API integration for query context (product type, brand, color, gender)
- **Attribute Matching:** Semantic matching between query intents and item attributes
- **Top-40 Analysis:** Preso API crawl for control vs variant top-40 results comparison
- **pgls_id Fetching:** Solr integration to generate Walmart.com product URLs
- **Interactive HTML Reports:** Detailed analysis with filtering, sorting, and Walmart links

## Quick Start

### Prerequisites

```bash
# Python 3.8+
pip install -r requirements.txt

# GCS access (for v1)
# Wibey with required skills (gcs_download, query_context, attribute_matching, preso_fetcher)
```

### V1 Pipeline (GCS-based)

```bash
python run_analysis_pipeline.py \
  --gcs-path "gs://k0k01ls/l1_recall_analysis/experiment_id/sample-5000/" \
```

**Optional flags:**
- `--skip-download`: Skip GCS download (use existing files)
- `--skip-enrichment`: Skip query enrichment (use existing enriched data)
- `--skip-pgls`: Skip pgls_id fetching

### V2 Pipeline (Email-based)

```bash
python run_analysis_pipeline_v2.py \
  --email ./emails/experiment.eml \
```

**Optional flags:**
- `--skip-download`: Skip GCS download
- `--skip-enrichment`: Skip query enrichment
- `--skip-preso`: Skip Preso top-40 crawl
- `--skip-pgls`: Skip pgls_id fetching

## Pipeline Steps

### V1 Pipeline
1. Download files from GCS (qip_scores.parquet, item_attributes.jsonl, contextualQuery.jsonl)
2. Enrich queries with Perceive API (product type, brand, color, gender intents)
3. Merge item attributes from JSONL
4. Calculate attribute matching scores
5. Filter 4s gained/lost queries
6. Create QIP pairs with matching analysis
7. Fetch pgls_id from Solr for Walmart.com links
8. Generate HTML report

### V2 Pipeline
1. Extract experiment config from email
2. Download files from GCS
3. Enrich queries with Perceive API
4. Merge item attributes from JSONL
5. Calculate attribute matching scores
6. Filter 4s gained/lost queries
7. Create QIP pairs with matching analysis
8. Crawl Preso API for top-40 results (control vs variant)
9. Merge top-40 data into QIP pairs
10. Fetch pgls_id from Solr
11. Generate HTML report with top-40 analysis

## Output

```
reports/
  ├── {experiment_id}_4s_added_report.html          # V1 report
  └── {experiment_id}_4s_added_report_v2.html       # V2 report with top-40

temp/
  └── downloaded_files/
      ├── {experiment_id}_qip_scores.parquet
      ├── {experiment_id}_qip_scores_enriched.parquet
      ├── {experiment_id}_qip_4s_gain_filtered.parquet
      ├── {experiment_id}_qip_pairs_with_matching.parquet
      ├── {experiment_id}_qip_pairs_with_matching_with_pgls.parquet  # V1
      ├── {experiment_id}_preso_top40.parquet                        # V2
      ├── {experiment_id}_qip_pairs_with_top40.parquet               # V2
      └── {experiment_id}_qip_pairs_with_top40_with_pgls.parquet     # V2
```

## HTML Report Features

- **Summary Statistics:** Total queries, items, 4s gained/lost counts
- **Attribute Match Analysis:** Percentage of queries matching on product_type, color, gender
- **Top-40 Analysis (V2 only):** Control vs variant position comparison
- **Interactive Filtering:** Filter by query, item, match scores, positions
- **Sortable Columns:** Click headers to sort by any metric
- **Walmart Links:** Direct links to product pages (via pgls_id)
- **Query Context:** Full contextualQuery details (stores, zipcode, prg, state)

## Key Scripts

- `run_analysis_pipeline.py` - V1 pipeline (GCS-based)
- `run_analysis_pipeline_v2.py` - V2 pipeline (email-based)
- `enrich_qip_scores.py` - Query enrichment and attribute merging
- `filter_4s_gain_queries.py` - Filter 4s gained/lost queries
- `create_qip_pairs.py` - Create QIP pairs with matching scores
- `fetch_pgls_ids.py` - Fetch pgls_id from Solr
- `generate_4s_report.py` - Generate V1 HTML report
- `generate_4s_report_v2.py` - Generate V2 HTML report with top-40

## Skills

- `gcs_download` - Download files from GCS
- `query_context` - Perceive API query enrichment
- `attribute_matching` - Semantic attribute matching
- `preso_fetcher` - Preso API top-40 crawl (V2 only)
- `preso_url_builder` - Build Preso URLs from experiment config (V2 only)

## Contextual Query Files

Sample contextual queries are provided in `contextualQueryfiles/`:
- `sample-20240901-20250831-5000.jsonl` (5000 queries)
- `sample-20250301-20260228-5000.jsonl` (5000 queries)

These are used for Preso API requests in V2 pipeline.

## Configuration

### Email Format (V2)
Experiment emails should contain:
- GCS path to experiment data
- Experiment parameters (control/variant engines, stores, etc.)
- Must be in .eml format

### GCS Structure
```
gs://bucket/l1_recall_analysis/{experiment_name}/{experiment_id}/sample-{N}/
  ├── qip_scores.parquet
  ├── item_attributes.jsonl
  └── contextualQuery.jsonl
```

## Troubleshooting

**Issue:** 0% matching scores
- **Cause:** Item attributes not merged
- **Fix:** Ensure item_attributes.jsonl exists and is loaded in enrich_qip_scores.py

**Issue:** Unicode decode error reading parquet as JSONL
- **Cause:** Path pattern mismatch with `_with_pgls` suffix
- **Fix:** Already fixed in generate_4s_report.py with if-elif pattern matching

**Issue:** Preso API failures
- **Cause:** Invalid contextualQuery format or rate limiting
- **Fix:** Check contextualQuery.jsonl format, reduce batch size

## License

Internal Walmart tool for L1 ranker analysis.

## Contact

For questions or issues, contact the Search Relevance team.
