#!/usr/bin/env python3
"""
End-to-End L1 Ranker Recall Analysis Pipeline - Simplified Version

This script automates the entire workflow:
1. Download data from GCS (you provide the path)
2. Enrich queries with Perceive API
3. Create QI pairs and compute matching
4. Build Preso URLs with experiment parameters (from config)
5. Generate interactive HTML report

Usage:
    # Provide GCS path and config file
    python run_pipeline.py \
        --gcs-path "gs://k0k01ls/l1_recall_analysis/ltr_ab_candidates/1773649932" \
        --config-file experiment_config.json

    # Or provide config as JSON string
    python run_pipeline.py \
        --gcs-path "gs://k0k01ls/l1_recall_analysis/ltr_ab_candidates/1773649932" \
        --config-json '{"engines": {...}}'
"""

import argparse
import subprocess
import sys
import json
from pathlib import Path
from skills.preso_url_builder import run as build_preso_urls, PresoUrlBuilderInput


def main():
    parser = argparse.ArgumentParser(
        description='Run L1 Ranker Recall Analysis Pipeline'
    )
    parser.add_argument(
        '--gcs-path',
        type=str,
        required=True,
        help='GCS bucket path (e.g., gs://k0k01ls/l1_recall_analysis/ltr_ab_candidates/1773649932)'
    )
    parser.add_argument(
        '--config-file',
        type=str,
        help='Path to experiment config JSON file'
    )
    parser.add_argument(
        '--config-json',
        type=str,
        help='Experiment config as JSON string (alternative to --config-file)'
    )
    parser.add_argument(
        '--skip-download',
        action='store_true',
        help='Skip GCS download if files already exist'
    )
    parser.add_argument(
        '--skip-enrichment',
        action='store_true',
        help='Skip query enrichment if already done'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='temp/downloaded_files',
        help='Output directory for downloaded files'
    )

    args = parser.parse_args()

    if not args.config_file and not args.config_json:
        print("❌ Error: Must provide either --config-file or --config-json")
        print("\nUsage:")
        print("  python run_pipeline.py --gcs-path gs://... --config-file config.json")
        print("  python run_pipeline.py --gcs-path gs://... --config-json '{...}'")
        return 1

    print("="*80)
    print("L1 Ranker Recall Analysis Pipeline")
    print("="*80)

    # Extract experiment ID from GCS path
    gcs_path = args.gcs_path
    experiment_id = gcs_path.rstrip('/').split('/')[-1]

    print(f"\n🔍 Experiment ID: {experiment_id}")
    print(f"📦 GCS Path: {gcs_path}")

    # Step 1: Parse experiment config
    print("\n⚙️  Step 1: Parsing experiment configuration...")
    print("-"*80)

    config_json = None
    if args.config_file:
        config_path = Path(args.config_file)
        if not config_path.exists():
            print(f"❌ Config file not found: {args.config_file}")
            return 1
        with open(config_path, 'r') as f:
            config_json = f.read()
    else:
        config_json = args.config_json

    # Validate config
    try:
        config_data = json.loads(config_json)
        if 'engines' not in config_data:
            print("❌ Invalid config: missing 'engines' section")
            return 1
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON config: {e}")
        return 1

    # Parse config to extract variant engine name
    input_config = PresoUrlBuilderInput(config_json=config_json)
    result = build_preso_urls(input_config)

    if result.status != "success":
        print(f"❌ Failed to parse config: {result.message}")
        return 1

    variant_engine = result.variant_engine_name
    print(f"✅ Parsed config successfully")
    print(f"✅ Control engine: control")
    print(f"✅ Variant engine: {variant_engine}")

    # Save config for later use
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    config_file = output_dir / f'{experiment_id}_experiment_config.json'
    with open(config_file, 'w') as f:
        f.write(config_json)
    print(f"💾 Saved config to: {config_file}")

    # Step 2: Download data from GCS
    if not args.skip_download:
        print("\n📥 Step 2: Downloading data from GCS...")
        print("-"*80)

        cmd = [
            'python', 'download_from_gcs.py',
            '--bucket-path', gcs_path,
            '--experiment-id', experiment_id,
            '--output-dir', str(output_dir)
        ]

        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            print(f"❌ Download failed!")
            print(result.stderr)
            return 1

        print(result.stdout)
    else:
        print("\n⏭️  Step 2: Skipping GCS download (--skip-download)")

    # Step 3: Enrich queries with Perceive API
    if not args.skip_enrichment:
        print("\n🔍 Step 3: Enriching queries with Perceive API...")
        print("-"*80)

        # Find the downloaded queries file
        queries_file = output_dir / f'{experiment_id}_queries.parquet'
        if not queries_file.exists():
            print(f"❌ Queries file not found: {queries_file}")
            print("   Did the download step complete successfully?")
            return 1

        enriched_file = output_dir / f'{experiment_id}_queries_enriched.parquet'

        cmd = [
            'python', 'enrich_queries.py',
            '--input', str(queries_file),
            '--output', str(enriched_file)
        ]

        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            print(f"❌ Enrichment failed!")
            print(result.stderr)
            return 1

        print(result.stdout)
    else:
        print("\n⏭️  Step 3: Skipping query enrichment (--skip-enrichment)")
        enriched_file = output_dir / f'{experiment_id}_queries_enriched.parquet'

    # Step 4: Create QI pairs and compute matching
    print("\n🔗 Step 4: Creating QI pairs and computing matching scores...")
    print("-"*80)

    qip_file = output_dir / f'{experiment_id}_qip_scores.parquet'

    cmd = [
        'python', 'create_qip_and_compute_matching.py',
        '--enriched-queries', str(enriched_file),
        '--experiment-id', experiment_id,
        '--output-dir', str(output_dir)
    ]

    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"❌ QIP creation and matching failed!")
        print(result.stderr)
        return 1

    print(result.stdout)

    # Step 5: Build Preso URLs
    print("\n🔗 Step 5: Building Preso search URLs...")
    print("-"*80)

    import pandas as pd
    qip_df = pd.read_parquet(qip_file)

    # Build URLs and add to dataframe
    input_config = PresoUrlBuilderInput(
        config_json=config_json,
        add_to_dataframe=qip_df
    )

    result = build_preso_urls(input_config)

    if result.status != "success":
        print(f"❌ Failed to build Preso URLs: {result.message}")
        return 1

    # Save enriched dataframe with URLs
    qip_with_urls_file = output_dir / f'{experiment_id}_qip_scores_with_urls.parquet'
    result.urls_df.to_parquet(qip_with_urls_file, index=False)

    print(f"✅ Added Preso URLs to dataframe")
    print(f"💾 Saved to: {qip_with_urls_file}")

    # Step 6: Generate HTML report
    print("\n📊 Step 6: Generating interactive HTML report...")
    print("-"*80)

    html_file = output_dir / f'{experiment_id}_defect_rate_report.html'

    cmd = [
        'python', 'generate_defect_rate_report.py',
        '--qip-scores', str(qip_with_urls_file),
        '--experiment-id', experiment_id,
        '--variant-name', variant_engine,
        '--output', str(html_file)
    ]

    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"❌ Report generation failed!")
        print(result.stderr)
        return 1

    print(result.stdout)

    # Final summary
    print("\n" + "="*80)
    print("✅ Pipeline completed successfully!")
    print("="*80)
    print(f"""
📁 Output Directory: {output_dir}

📄 Generated Files:
  - Config: {config_file.name}
  - Queries: {experiment_id}_queries.parquet
  - Enriched: {experiment_id}_queries_enriched.parquet
  - QI Pairs: {experiment_id}_qip_scores.parquet
  - With URLs: {experiment_id}_qip_scores_with_urls.parquet
  - HTML Report: {html_file.name}

🌐 View the report:
  open {html_file}

The report includes:
  ✅ Defect rate analysis with matching scores
  ✅ Clickable Preso search URLs (Control vs Variant)
  ✅ Intent and attribute comparison
  ✅ Interactive sorting and filtering
""")

    return 0


if __name__ == '__main__':
    sys.exit(main())
