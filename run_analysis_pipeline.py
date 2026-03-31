#!/usr/bin/env python3
"""
End-to-End L1 Ranker Recall Analysis Pipeline

Runs the complete workflow from GCS download to HTML report generation.
All intermediate files are named using the experiment ID from the GCS path.

Usage:
    python run_analysis_pipeline.py \\
        --gcs-path gs://k0k01ls/l1_recall_analysis/ltr_ab_candidates/1773333304 \\
        --variant nlfv3_alp1_utbeta05_w0_4 \\
        --queries 100

Steps:
    1. Download files from GCS (qip_scores, item_attributes)
    2. Enrich queries with Perceive API
    3. Filter to queries with 4s gains
    4. Create QI pairs
    5. Compute attribute matching scores
    6. Generate HTML report

All files will be named: <experiment_id>_<file_type>.parquet
Example: 1773333304_qip_scores_enriched.parquet
"""

import argparse
import subprocess
import sys
import json
from pathlib import Path
from datetime import datetime
import re


def extract_experiment_id(gcs_path: str) -> str:
    """Extract experiment ID from GCS path."""
    # gs://k0k01ls/l1_recall_analysis/ltr_ab_candidates/1773333304 -> 1773333304
    parts = gcs_path.rstrip('/').split('/')
    return parts[-1]


def run_command(cmd: list, description: str) -> int:
    """Run a shell command and print status."""
    print("\n" + "="*80)
    print(f"STEP: {description}")
    print("="*80)
    print(f"Command: {' '.join(cmd)}")
    print()

    result = subprocess.run(cmd, capture_output=False, text=True)

    if result.returncode != 0:
        print(f"\n❌ Failed: {description}")
        return result.returncode
    else:
        print(f"\n✅ Completed: {description}")
        return 0


def main():
    parser = argparse.ArgumentParser(
        description='Run end-to-end L1 ranker recall analysis pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
    python run_analysis_pipeline.py \\
        --gcs-path gs://k0k01ls/l1_recall_analysis/ltr_ab_candidates/1773333304 \\
        --variant nlfv3_alp1_utbeta05_w0_4 \\
        --queries 100
        """
    )

    parser.add_argument(
        '--gcs-path',
        type=str,
        required=True,
        help='GCS path to experiment data (e.g., gs://bucket/path/1773333304)'
    )
    parser.add_argument(
        '--variant',
        type=str,
        default=None,
        help='Variant engine name to compare against control (auto-detect if not specified)'
    )
    parser.add_argument(
        '--queries',
        type=int,
        default=None,
        help='Number of queries to sample for enrichment (default: all queries)'
    )
    parser.add_argument(
        '--min-total',
        type=int,
        default=400,
        help='Minimum total items per query for filtering (default: 400)'
    )
    parser.add_argument(
        '--max-total-diff',
        type=int,
        default=5,
        help='Max difference in total items between engines (default: 5)'
    )
    parser.add_argument(
        '--min-gain',
        type=int,
        default=1,
        help='Minimum 4s gain required (default: 1)'
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
        '--skip-pgls',
        action='store_true',
        help='Skip pgls_id fetching from Solr if already done'
    )

    args = parser.parse_args()

    # Extract experiment ID
    experiment_id = extract_experiment_id(args.gcs_path)
    print(f"\n🎯 Experiment ID: {experiment_id}")

    # Variant will be auto-detected if not specified
    variant_engine = args.variant
    if variant_engine:
        print(f"🎯 Variant Engine: {variant_engine} (user-specified)")
    else:
        print(f"🎯 Variant Engine: (will auto-detect from data)")
    print(f"🎯 Control Engine: control")

    # Create output directories
    temp_dir = Path('./temp/downloaded_files')
    temp_dir.mkdir(parents=True, exist_ok=True)

    reports_dir = Path('./reports')
    reports_dir.mkdir(parents=True, exist_ok=True)

    # Define file paths with experiment ID
    base_name = experiment_id
    qip_scores_raw = temp_dir / f'{base_name}_qip_scores.parquet'
    item_attributes = temp_dir / f'{base_name}_item_attributes.jsonl'
    qip_scores_enriched = temp_dir / f'{base_name}_qip_scores_enriched.parquet'
    qip_scores_filtered = temp_dir / f'{base_name}_qip_4s_gain_filtered.parquet'
    qip_scores_filtered_summary = temp_dir / f'{base_name}_qip_4s_gain_filtered_summary.json'
    qip_pairs = temp_dir / f'{base_name}_qip_pairs.parquet'
    qip_pairs_summary = temp_dir / f'{base_name}_qip_pairs_summary.json'
    qip_pairs_with_matching = temp_dir / f'{base_name}_qip_pairs_with_matching.parquet'
    # html_report path will be set after variant is detected

    # Track timing
    start_time = datetime.now()
    steps_completed = []

    try:
        # Step 1: Download from GCS
        if not args.skip_download:
            print("\n" + "="*80)
            print("STEP 1: Download files from GCS")
            print("="*80)

            # Import and run GCS download skill
            from skills.gcs_download.main import run as gcs_download_run
            from skills.gcs_download.config import GCSDownloadInput

            input_config = GCSDownloadInput(
                gs_path=args.gcs_path,
                local_dir=str(temp_dir),
                auto_discover=True,
                recursive=True
            )

            output = gcs_download_run(input_config)

            # Rename downloaded files to include experiment ID
            files_to_rename = [
                (output.qip_scores_path, qip_scores_raw),
                (output.metadata_path, item_attributes),  # metadata is item_attributes
            ]

            for old_file, new_file in files_to_rename:
                if old_file and Path(old_file).exists():
                    old_path = Path(old_file)
                    # Rename/move file if different
                    if old_path != new_file:
                        old_path.rename(new_file)
                        print(f"  ✅ Renamed: {old_path.name} -> {new_file.name}")

            steps_completed.append("Download from GCS")
        else:
            print("\n⏭️  Skipping download (--skip-download)")

        # Step 2: Enrich queries with Perceive API
        if not args.skip_enrichment:
            cmd = [
                'python', 'enrich_qip_scores.py',
                '--input', str(qip_scores_raw),
                '--output', str(qip_scores_enriched),
                '--item-attributes', str(item_attributes),
                '--concurrency', '10'
            ]

            # Add --queries flag only if specified (otherwise enrich all)
            if args.queries:
                cmd.extend(['--queries', str(args.queries)])

            result = run_command(cmd, "Enrich queries with Perceive API")
            if result != 0:
                return result

            steps_completed.append("Query enrichment")
        else:
            print("\n⏭️  Skipping enrichment (--skip-enrichment)")

        # Step 3: Filter to queries with 4s gains
        cmd = [
            'python', 'filter_4s_gain_queries.py',
            '--input', str(qip_scores_enriched),
            '--output', str(qip_scores_filtered),
            '--min-total', str(args.min_total),
            '--max-total-diff', str(args.max_total_diff),
            '--min-gain', str(args.min_gain)
        ]

        # Add variant if specified
        if args.variant:
            cmd.extend(['--variant', args.variant])

        result = run_command(cmd, "Filter to queries with 4s gains")
        if result != 0:
            return result

        steps_completed.append("Filter queries with 4s gains")

        # Auto-detect variant engine from summary if not specified
        if not variant_engine:
            with open(qip_scores_filtered_summary, 'r') as f:
                filter_summary = json.load(f)
                variant_engine = filter_summary.get('variant_engine', 'variant')
                print(f"\n✅ Auto-detected variant engine: {variant_engine}")

        # Now that we have variant_engine, define the HTML report path
        html_report = reports_dir / f'{base_name}_{variant_engine}_4s_added_report.html'
        print(f"📊 HTML report will be: {html_report.name}")

        # Step 4: Create QI pairs
        cmd = [
            'python', 'create_qip_pairs.py',
            '--input', str(qip_scores_filtered),
            '--output', str(qip_pairs)
        ]

        result = run_command(cmd, "Create QI pairs (control vs variant)")
        if result != 0:
            return result

        steps_completed.append("Create QI pairs")

        # Step 5: Compute attribute matching scores
        cmd = [
            'python', 'skills/attribute_matching/main.py',
            '--input', str(qip_pairs),
            '--output', str(qip_pairs_with_matching)
        ]

        result = run_command(cmd, "Compute attribute matching scores")
        if result != 0:
            return result

        steps_completed.append("Attribute matching analysis")

        # Step 6: Fetch pgls_id from Solr
        qip_pairs_with_pgls = temp_dir / f"{base_name}_qip_pairs_with_matching_with_pgls.parquet"

        if not args.skip_pgls:
            print("\n" + "="*80)
            print("STEP 6: Fetching pgls_id from Solr")
            print("="*80)

            # Import pgls fetcher
            from fetch_pgls_ids import enrich_with_pgls_id

            try:
                enrich_with_pgls_id(
                    input_file=str(qip_pairs_with_matching),
                    output_file=str(qip_pairs_with_pgls),
                    batch_size=50
                )
            except Exception as e:
                print(f"\n⚠️  Error fetching pgls_id: {e}")
                print("Continuing with report generation without pgls_id...")
                qip_pairs_with_pgls = qip_pairs_with_matching

            steps_completed.append("Fetch pgls_id from Solr")
        else:
            print("\n⏭️  Skipping pgls_id fetching (--skip-pgls)")
            # Check if file already exists
            if qip_pairs_with_pgls.exists():
                print(f"✅ Using existing pgls_id file: {qip_pairs_with_pgls}")
            else:
                print(f"⚠️  pgls_id file not found, using file without pgls_id")
                qip_pairs_with_pgls = qip_pairs_with_matching

        # Step 7: Generate HTML report
        cmd = [
            'python', 'generate_4s_report.py',
            '--input', str(qip_pairs_with_pgls),
            '--output', str(html_report),
            '--with-matching'
        ]

        result = run_command(cmd, "Generate HTML report")
        if result != 0:
            return result

        steps_completed.append("HTML report generation")

        # Success!
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print("\n" + "="*80)
        print("✅ PIPELINE COMPLETED SUCCESSFULLY")
        print("="*80)
        print(f"\nExperiment ID: {experiment_id}")
        print(f"Variant: {variant_engine}")
        print(f"Duration: {duration:.1f} seconds")
        print(f"\nSteps completed ({len(steps_completed)}):")
        for i, step in enumerate(steps_completed, 1):
            print(f"  {i}. {step}")

        print(f"\n📊 Generated Files:")
        print(f"  Raw QIP Scores:       {qip_scores_raw}")
        print(f"  Item Attributes:      {item_attributes}")
        print(f"  Enriched QIP Scores:  {qip_scores_enriched}")
        print(f"  Filtered QIP Scores:  {qip_scores_filtered}")
        print(f"  QI Pairs:             {qip_pairs}")
        print(f"  QI Pairs w/ Matching: {qip_pairs_with_matching}")
        print(f"  QI Pairs w/ pgls_id:  {qip_pairs_with_pgls}")
        print(f"  HTML Report:          {html_report}")

        print(f"\n🌐 Open report in browser:")
        print(f"  file://{html_report.absolute()}")

        # Write pipeline summary
        summary = {
            'experiment_id': experiment_id,
            'gcs_path': args.gcs_path,
            'variant_engine': variant_engine,
            'control_engine': 'control',
            'pipeline_config': {
                'queries_sampled': args.queries,
                'min_total': args.min_total,
                'max_total_diff': args.max_total_diff,
                'min_4s_gain': args.min_gain
            },
            'execution': {
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'steps_completed': steps_completed
            },
            'output_files': {
                'qip_scores_raw': str(qip_scores_raw),
                'item_attributes': str(item_attributes),
                'qip_scores_enriched': str(qip_scores_enriched),
                'qip_scores_filtered': str(qip_scores_filtered),
                'qip_pairs': str(qip_pairs),
                'qip_pairs_with_matching': str(qip_pairs_with_matching),
                'qip_pairs_with_pgls': str(qip_pairs_with_pgls),
                'html_report': str(html_report)
            }
        }

        summary_file = reports_dir / f'{base_name}_{variant_engine}_pipeline_summary.json'
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)

        print(f"\n📄 Pipeline summary: {summary_file}")

        return 0

    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    exit(main())
