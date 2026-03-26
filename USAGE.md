# L1 Ranker Recall Analysis - Usage Guide

## Quick Start

Since there's no Wibey Outlook integration available, you'll need to manually extract the experiment configuration from your defect rate analysis email.

### Step 1: Get the Experiment Configuration

Open your defect rate analysis email and copy the JSON configuration that contains the `engines` section. It should look like this:

```json
{
  "comments": "...",
  "engines": {
    "control": {
      "host": "http://preso-usgm-wcnp.prod.walmart.com",
      "request_params": {
        "stores": "...",
        "zipcode": "...",
        "ptss": "..."
      }
    },
    "ltr_ab_candidates": {
      "host": "http://preso-usgm-wcnp.prod.walmart.com",
      "request_params": {
        "ptss": "use_variant_solr:on;l1_ranker_disable_bfs:on;..."
      }
    }
  }
}
```

Save this to a file, for example: `experiment_config.json`

### Step 2: Get the GCS Path

From the same email, copy the GCS bucket path. It should look like:
```
gs://k0k01ls/l1_recall_analysis/ltr_ab_candidates/1773649932
```

### Step 3: Run the Pipeline

```bash
python run_pipeline.py \
  --gcs-path "gs://k0k01ls/l1_recall_analysis/ltr_ab_candidates/1773649932" \
  --config-file experiment_config.json
```

Or provide the config directly as JSON:

```bash
python run_pipeline.py \
  --gcs-path "gs://k0k01ls/l1_recall_analysis/ltr_ab_candidates/1773649932" \
  --config-json '{"engines": {...}}'
```

## What the Pipeline Does

1. **Downloads data from GCS**
   - Queries parquet file
   - Results parquet files

2. **Enriches queries with Perceive API**
   - Extracts intents, attributes, and product types
   - Adds contextual information

3. **Creates QI pairs and computes matching**
   - Pairs queries with items
   - Computes intent, attribute, and overall matching scores

4. **Builds Preso search URLs**
   - Constructs URLs with experiment parameters (ptss, trsp)
   - Adds both control and variant URLs

5. **Generates interactive HTML report**
   - Defect rate analysis
   - Clickable search URLs
   - Intent and attribute comparison
   - Sortable tables

## Output

All files are saved to `temp/downloaded_files/` by default:

```
temp/downloaded_files/
├── 1773649932_experiment_config.json
├── 1773649932_queries.parquet
├── 1773649932_queries_enriched.parquet
├── 1773649932_qip_scores.parquet
├── 1773649932_qip_scores_with_urls.parquet
└── 1773649932_defect_rate_report.html  ← Open this!
```

## Options

```bash
# Skip GCS download if files already exist
python run_pipeline.py --gcs-path gs://... --config-file config.json --skip-download

# Skip query enrichment if already done
python run_pipeline.py --gcs-path gs://... --config-file config.json --skip-enrichment

# Custom output directory
python run_pipeline.py --gcs-path gs://... --config-file config.json --output-dir my_results/
```

## Example Workflow

```bash
# 1. Create config file from email
cat > experiment_config.json << 'EOF'
{
  "comments": "L1 Ranker AB Test",
  "engines": {
    "control": {
      "host": "http://preso-usgm-wcnp.prod.walmart.com",
      "request_params": {
        "stores": "4108",
        "zipcode": "94086",
        "ptss": "l1_ranker_disable_bfs:on"
      }
    },
    "ltr_ab_candidates": {
      "host": "http://preso-usgm-wcnp.prod.walmart.com",
      "request_params": {
        "ptss": "use_variant_solr:on;l1_ranker_disable_bfs:on"
      }
    }
  }
}
EOF

# 2. Run the pipeline
python run_pipeline.py \
  --gcs-path "gs://k0k01ls/l1_recall_analysis/ltr_ab_candidates/1773649932" \
  --config-file experiment_config.json

# 3. Open the report
open temp/downloaded_files/1773649932_defect_rate_report.html
```

## Troubleshooting

### "Config file not found"
- Make sure you saved the JSON to the correct path
- Use absolute path if relative path doesn't work

### "Invalid config: missing 'engines' section"
- Verify your JSON has the `engines` key
- Check for JSON syntax errors (missing commas, quotes, etc.)

### "Queries file not found"
- Make sure the GCS download completed successfully
- Check that the experiment ID matches the GCS path

### "Enrichment failed"
- Verify you have access to the Perceive API
- Check network connectivity

## Notes

- The experiment ID is automatically extracted from the GCS path (last component)
- All intermediate files are saved for debugging and reuse
- Use `--skip-download` and `--skip-enrichment` flags to rerun only specific steps
- The HTML report includes clickable Preso URLs for both control and variant
