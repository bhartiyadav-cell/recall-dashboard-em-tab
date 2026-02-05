#!/bin/bash
# filepath: /Users/p0y01cc/Retrieval/recall_ranker_comparison/run_recall_analysis.sh

# Recall Ranker Comparison Runner
# Usage: 
#   ./run_recall_analysis.sh <parquet_file_or_gs_path> [engine_name]
#   ./run_recall_analysis.sh download <gs_path>

set -e

# Parse arguments
FIRST_ARG="${1:-}"
SECOND_ARG="${2:-all}"

# Show usage
show_usage() {
    echo "Recall Ranker Comparison Runner"
    echo ""
    echo "Usage:"
    echo "  ./run_recall_analysis.sh <parquet_file> [engine_name]    Run analysis on local file"
    echo "  ./run_recall_analysis.sh <gs_path> [engine_name]         Download from GCS and run analysis"
    echo "  ./run_recall_analysis.sh download <gs_path>              Download only (no analysis)"
    echo ""
    echo "Arguments:"
    echo "  parquet_file  Path to local qip_scores.parquet file"
    echo "  gs_path       GCS path (e.g., gs://p0y01cc/l1_recall_analysis/.../qip_scores.parquet)"
    echo "  engine_name   Name of the variant engine (default: 'all' for all engines)"
    echo ""
    echo "Examples:"
    echo "  ./run_recall_analysis.sh qip_scores.parquet"
    echo "  ./run_recall_analysis.sh qip_scores.parquet nlfv3_alp1_utbeta05_w0_4"
    echo "  ./run_recall_analysis.sh gs://p0y01cc/l1_recall_analysis/nlfv3-utbeta/123/sample-5000/qip_scores.parquet"
    echo "  ./run_recall_analysis.sh download gs://p0y01cc/l1_recall_analysis/nlfv3-utbeta/123/sample-5000/qip_scores.parquet"
}

# Download from GCS function
download_from_gcs() {
    local GS_PATH="$1"
    
    # Extract path after l1_recall_analysis/
    LOCAL_PATH=$(echo "$GS_PATH" | sed 's|gs://[^/]*/l1_recall_analysis/||')
    LOCAL_FILE="./data/$LOCAL_PATH"
    LOCAL_DIR=$(dirname "$LOCAL_FILE")
    
    echo "Downloading from GCS..."
    echo "  Source: $GS_PATH"
    echo "  Target: $LOCAL_FILE"
    echo ""
    
    mkdir -p "$LOCAL_DIR"
    gsutil cp "$GS_PATH" "$LOCAL_FILE"
    
    echo "Download complete!"
    echo "$LOCAL_FILE"
}

# Validate arguments
if [[ -z "$FIRST_ARG" ]]; then
    show_usage
    exit 1
fi

# Handle download-only command
if [[ "$FIRST_ARG" == "download" ]]; then
    if [[ -z "$SECOND_ARG" ]] || [[ "$SECOND_ARG" == "all" ]]; then
        echo "Error: Missing GCS path for download"
        echo ""
        show_usage
        exit 1
    fi
    download_from_gcs "$SECOND_ARG"
    exit 0
fi

# Determine if input is GCS path or local file
PARQUET_FILE="$FIRST_ARG"
ENGINE_NAME="$SECOND_ARG"

if [[ "$FIRST_ARG" == gs://* ]]; then
    # Download from GCS first
    PARQUET_FILE=$(download_from_gcs "$FIRST_ARG")
    echo ""
fi

# Check if parquet file exists
if [[ ! -f "$PARQUET_FILE" ]]; then
    echo "Error: Parquet file not found: $PARQUET_FILE"
    exit 1
fi

# Create timestamped output directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_DIR="./output/recall_ranker_comparison_${TIMESTAMP}"
mkdir -p "$OUTPUT_DIR"

echo "========================================"
echo "Recall Ranker Comparison Runner"
echo "========================================"
echo ""
echo "Parquet file: $PARQUET_FILE"
echo "Engine:       $ENGINE_NAME"
echo "Output dir:   $OUTPUT_DIR"
echo ""

# Run the analysis
if [[ "$ENGINE_NAME" == "all" ]]; then
    echo "Running comparison for all variant engines..."
    python3 << EOF
from recall_analyser import RecallAnalyser

analyser = RecallAnalyser(
    '${PARQUET_FILE}',
    min_total=400,
    max_total_diff=5
)
analyser.run_analysis('${OUTPUT_DIR}')
EOF
else
    echo "Running comparison for engine: $ENGINE_NAME"
    
    # Create engine-specific output directory
    ENGINE_OUTPUT_DIR="$OUTPUT_DIR/$ENGINE_NAME"
    mkdir -p "$ENGINE_OUTPUT_DIR"
    
    python3 << EOF
from recall_analyser import RecallAnalyser

analyser = RecallAnalyser(
    '${PARQUET_FILE}',
    min_total=400,
    max_total_diff=5
)
analyser.load_data()
analyser.set_variant_engine('${ENGINE_NAME}')
analyser.find_missing_extra_items()
analyser.compute_comparison()
analyser.run_statistical_tests()
analyser.generate_html_visualization('${ENGINE_OUTPUT_DIR}/recall_ranker_comparison.html')

# Save CSVs
analyser.comparison.to_csv('${ENGINE_OUTPUT_DIR}/comparison.csv', index=False)
analyser.ttest_overall.to_csv('${ENGINE_OUTPUT_DIR}/ttest_overall.csv', index=False)
analyser.ttest_unfiltered.to_csv('${ENGINE_OUTPUT_DIR}/ttest_unfiltered.csv', index=False)

print('Comparison complete!')
EOF
fi

echo ""
echo "========================================"
echo "Recall Ranker Comparison Complete!"
echo "========================================"
echo ""
echo "Output saved to: $OUTPUT_DIR"
echo ""

# List output files
ls -la "$OUTPUT_DIR"

# Open in browser (macOS)
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo ""
    read -p "Open in browser? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        if [[ "$ENGINE_NAME" == "all" ]]; then
            open "$OUTPUT_DIR/index.html"
        else
            open "$OUTPUT_DIR/$ENGINE_NAME/recall_ranker_comparison.html"
        fi
    fi
fi