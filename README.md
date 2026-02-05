# Recall Ranker Comparison

A tool for comparing recall between control and variant L1 ranker engines.

## Assumptions
When generating the recall analysis report, the engine name used was the same as the model name in config_search_usgm.

## Setup

```bash
pip install -r requirements.txt
```

## Usage

```bash
# Run analysis on local file (all engines)
bash run_recall_analysis.sh qip_scores.parquet

# Run analysis on local file (specific engine)
bash run_recall_analysis.sh qip_scores.parquet engine_name

# Download from GCS and run analysis
bash run_recall_analysis.sh gs://p0y01cc/l1_recall_analysis/nlfv3-utbeta/1768984441/sample-5000/qip_scores.parquet

# Download from GCS and run analysis (specific engine)
bash run_recall_analysis.sh gs://p0y01cc/l1_recall_analysis/nlfv3-utbeta/1768984441/sample-5000/qip_scores.parquet engine_name
```

## Output

The script generates a timestamped output folder:

```
output/recall_ranker_comparison_YYYYMMDD_HHMMSS/
├── <engine_name>/
│   ├── recall_ranker_comparison.html   # Interactive report
│   ├── comparison.csv                   # Query-level comparison
│   ├── ttest_overall.csv                # Filtered t-test results
│   └── ttest_unfiltered.csv             # Unfiltered t-test results
└── index.html                           # Index page (when running all engines)
```

## Features

- **Filtered T-Test**: Filters queries with min_total ≥ 400 and total_diff < 5 (since ranker improvements shouldn't change match size)
- **Good/Bad Query Breakdown**: Shows gained/lost 4s and removed/added non-4s per query
- **Sunlight Links**: Pre-populated links for quick debugging:
  - Pre-config release (using _seh)
  - Config released