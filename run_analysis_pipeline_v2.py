#!/usr/bin/env python3
"""
End-to-End L1 Ranker Recall Analysis Pipeline v2 (with Top-40 Preso Crawl)

Runs the complete workflow from GCS download to HTML report generation,
including Preso crawl to identify which items are in top-40 results.

Usage:
    python run_analysis_pipeline_v2.py \\
        --email ./path/to/experiment_email.eml

Steps:
    0. Extract experiment config from email (.eml file)
    1. Download files from GCS (qip_scores, item_attributes)
    2. Enrich queries with Perceive API
    3. Filter to queries with 4s gains
    4. Create QI pairs
    5. Compute attribute matching scores
    6. **NEW** Fetch top-40 from Preso for control and variant
    7. **NEW** Add top-40 flags to QI pairs
    8. Generate HTML report v2 (with top-40 visualization)

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
        description='Run end-to-end L1 ranker recall analysis pipeline v2 (with top-40 analysis)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
    python run_analysis_pipeline_v2.py \\
        --email ./path/to/experiment_email.eml

    python run_analysis_pipeline_v2.py \\
        --email ./path/to/experiment_email.eml \\
        --queries 100 \\
        --preso-qps 5
        """
    )

    parser.add_argument(
        '--email',
        type=str,
        required=True,
        help='Path to .eml email file with experiment config (REQUIRED for v2 pipeline with Preso crawl)'
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
        '--skip-preso',
        action='store_true',
        help='Skip Preso top-40 crawl if already done'
    )
    parser.add_argument(
        '--skip-pgls',
        action='store_true',
        help='Skip pgls_id fetching from Solr if already done'
    )
    parser.add_argument(
        '--preso-qps',
        type=int,
        default=3,
        help='Queries per second for Preso API (default: 3)'
    )

    args = parser.parse_args()

    # v2 pipeline REQUIRES email for experiment config
    print(f"\n🎯 Pipeline v2 - Using email file: {args.email}")
    experiment_id = None

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

    # Track timing
    start_time = datetime.now()
    steps_completed = []

    try:
        # Step 0: Extract experiment config from email (REQUIRED for v2)
        print("\n" + "="*80)
        print("STEP 0: Extract experiment config from email")
        print("="*80)

        from skills.preso_url_builder.main import run as preso_url_run
        from skills.preso_url_builder.config import PresoUrlBuilderInput

        email_input = PresoUrlBuilderInput(
            email_file=args.email
        )

        email_output = preso_url_run(email_input)

        if email_output.status != "success":
            print(f"\n❌ Failed to extract config from email: {email_output.message}")
            return 1

        # Extract experiment ID from GCS path
        gcs_path = email_output.gcs_path
        if not gcs_path:
            print("\n❌ No GCS path found in email")
            return 1

        experiment_id = extract_experiment_id(gcs_path)
        variant_name = email_output.variant_engine_name

        print(f"\n✅ Extracted experiment ID: {experiment_id}")
        print(f"✅ Variant engine: {variant_name}")
        print(f"✅ GCS path: {gcs_path}")

        # Save experiment config to file for Preso fetcher
        # Format: {"engines": {"control": {...}, "variant_name": {...}}}
        import json
        experiment_config_file = temp_dir / f'{experiment_id}_experiment_config.json'

        config_data = {
            'engines': {
                'control': email_output.control_config,
                variant_name: email_output.variant_config
            }
        }

        with open(experiment_config_file, 'w') as f:
            json.dump(config_data, f, indent=2)

        print(f"✅ Saved experiment config: {experiment_config_file}")

        steps_completed.append("Extract config from email")

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
        qip_pairs_with_top40 = temp_dir / f'{base_name}_qip_pairs_with_top40.parquet'
        preso_top40_results = temp_dir / f'{base_name}_preso_top40.parquet'

        # Step 1: Download from GCS
        if not args.skip_download:
            print("\n" + "="*80)
            print("STEP 1: Download files from GCS")
            print("="*80)

            # Import and run GCS download skill
            from skills.gcs_download.main import run as gcs_download_run
            from skills.gcs_download.config import GCSDownloadInput

            input_config = GCSDownloadInput(
                gs_path=gcs_path,
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
        html_report = reports_dir / f'{base_name}_{variant_engine}_4s_added_report_v2.html'
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

        # Step 6: Fetch top-40 from Preso (NEW in v2)
        if not args.skip_preso:
            print("\n" + "="*80)
            print("STEP 6: Fetch top-40 from Preso API")
            print("="*80)

            from skills.preso_fetcher.main import fetch_preso_results
            from skills.preso_fetcher.config import PresoFetcherInput

            # Get experiment config file (created in Step 0)
            experiment_config_file = temp_dir / f'{experiment_id}_experiment_config.json'

            if not experiment_config_file.exists():
                print(f"❌ Experiment config file not found: {experiment_config_file}")
                print("   This should have been created in Step 0 (Extract config from email)")
                return 1

            print(f"✅ Using experiment config: {experiment_config_file}")

            # Use contextualQueryfiles directory for query data
            contextual_queries_dir = Path('./contextualQueryfiles')
            if not contextual_queries_dir.exists():
                print(f"❌ contextualQueryfiles directory not found: {contextual_queries_dir}")
                return 1

            preso_input = PresoFetcherInput(
                qip_scores_file=str(qip_scores_filtered),
                contextual_queries_file=str(contextual_queries_dir),
                experiment_config_file=str(experiment_config_file),
                qps=args.preso_qps,
                top_n=40
            )

            preso_output = fetch_preso_results(preso_input)

            # Check if fetch was successful
            if preso_output.status != "success":
                print(f"\n❌ Preso fetch failed: {preso_output.message}")
                return 1

            # Save results DataFrame to parquet
            if preso_output.results_df is not None and len(preso_output.results_df) > 0:
                preso_output.results_df.to_parquet(preso_top40_results, index=False)
                print(f"\n✅ Saved Preso top-40 results: {preso_top40_results}")
                print(f"   Total results: {len(preso_output.results_df):,} rows")
            else:
                print("\n❌ No Preso results to save")
                return 1

            steps_completed.append("Fetch top-40 from Preso")
        else:
            print("\n⏭️  Skipping Preso crawl (--skip-preso)")

        # Step 7: Add top-40 flags to QI pairs (NEW in v2)
        print("\n" + "="*80)
        print("STEP 7: Add top-40 flags to QI pairs")
        print("="*80)

        import pandas as pd

        # Load QI pairs with matching
        pairs_df = pd.read_parquet(qip_pairs_with_matching)
        print(f"Loaded {len(pairs_df):,} QI pairs")

        # Load Preso top-40 results
        preso_df = pd.read_parquet(preso_top40_results)
        print(f"Loaded {len(preso_df):,} Preso top-40 results")

        # Create lookup: (query, product_id, engine) -> rank
        preso_lookup = {}
        for _, row in preso_df.iterrows():
            query = row['query']
            product_id = str(row['product_id'])
            rank = row['rank']
            engine = row['engine']

            preso_lookup[(query, product_id, engine)] = rank

        print(f"Created lookup with {len(preso_lookup):,} entries")

        # Add top-40 flags to pairs
        def add_top40_flags(row):
            # Use the simple 'query' column if available, otherwise extract from contextualQuery
            if 'query' in pairs_df.columns:
                query = row['query']
            elif 'contextualQuery' in pairs_df.columns:
                # Extract simple query from contextualQuery string like "milk (stores=4430, ...)"
                cq_str = row['contextualQuery']
                if '(' in cq_str:
                    query = cq_str.split('(')[0].strip()
                else:
                    query = cq_str.strip()
            else:
                query = None

            product_id = str(row['pg_prod_id'])

            # Control
            control_key = (query, product_id, 'control')
            in_control_top40 = control_key in preso_lookup
            control_rank = preso_lookup.get(control_key)

            # Variant (Preso results always use 'variant' as engine name)
            variant_key = (query, product_id, 'variant')
            in_variant_top40 = variant_key in preso_lookup
            variant_rank = preso_lookup.get(variant_key)

            return pd.Series({
                'in_control_top40': in_control_top40,
                'control_rank': control_rank,
                'in_variant_top40': in_variant_top40,
                'variant_rank': variant_rank
            })

        top40_flags = pairs_df.apply(add_top40_flags, axis=1)
        pairs_df = pd.concat([pairs_df, top40_flags], axis=1)

        # Save enriched pairs
        pairs_df.to_parquet(qip_pairs_with_top40, index=False)
        print(f"\n✅ Saved QI pairs with top-40 flags: {qip_pairs_with_top40}")

        # Print statistics
        total_pairs = len(pairs_df)
        in_control = pairs_df['in_control_top40'].sum()
        in_variant = pairs_df['in_variant_top40'].sum()

        print(f"\n📊 Top-40 Statistics:")
        print(f"  Total pairs: {total_pairs:,}")
        print(f"  In control top-40: {in_control:,} ({in_control/total_pairs*100:.1f}%)")
        print(f"  In variant top-40: {in_variant:,} ({in_variant/total_pairs*100:.1f}%)")

        # Statistics for 4s gained
        fours_gained = pairs_df[pairs_df['change_type'] == '4_gained']
        if len(fours_gained) > 0:
            in_variant_top40 = fours_gained['in_variant_top40'].sum()
            new_in_top40 = fours_gained[(fours_gained['in_variant_top40'] == True) &
                                       (fours_gained['in_control_top40'] == False)].shape[0]
            print(f"\n  4s Added Analysis:")
            print(f"    Total 4s added: {len(fours_gained):,}")
            print(f"    In variant top-40: {in_variant_top40:,} ({in_variant_top40/len(fours_gained)*100:.1f}%)")
            print(f"    Newly in top-40: {new_in_top40:,} ({new_in_top40/len(fours_gained)*100:.1f}%) ← HIGH IMPACT")

        # Statistics for non-4s removed
        non4s_removed = pairs_df[pairs_df['change_type'] == 'non4_removed']
        if len(non4s_removed) > 0:
            was_in_top40 = non4s_removed['in_control_top40'].sum()
            left_top40 = non4s_removed[(non4s_removed['in_control_top40'] == True) &
                                      (non4s_removed['in_variant_top40'] == False)].shape[0]
            print(f"\n  Non-4s Removed Analysis:")
            print(f"    Total non-4s removed: {len(non4s_removed):,}")
            print(f"    Was in control top-40: {was_in_top40:,} ({was_in_top40/len(non4s_removed)*100:.1f}%)")
            print(f"    Left top-40: {left_top40:,} ({left_top40/len(non4s_removed)*100:.1f}%) ← HIGH IMPACT")

        steps_completed.append("Add top-40 flags to pairs")

        # Step 8: Fetch pgls_id from Solr
        qip_pairs_with_pgls = temp_dir / f"{experiment_id}_qip_pairs_with_top40_with_pgls.parquet"

        if not args.skip_pgls:
            print("\n" + "="*80)
            print("STEP 8: Fetching pgls_id from Solr")
            print("="*80)

            # Import pgls fetcher
            from fetch_pgls_ids import enrich_with_pgls_id

            try:
                enrich_with_pgls_id(
                    input_file=str(qip_pairs_with_top40),
                    output_file=str(qip_pairs_with_pgls),
                    batch_size=50
                )
            except Exception as e:
                print(f"\n⚠️  Error fetching pgls_id: {e}")
                print("Continuing with report generation without pgls_id...")
                qip_pairs_with_pgls = qip_pairs_with_top40

            steps_completed.append("Fetch pgls_id from Solr")
        else:
            print("\n⏭️  Skipping pgls_id fetching (--skip-pgls)")
            # Check if file already exists
            if qip_pairs_with_pgls.exists():
                print(f"✅ Using existing pgls_id file: {qip_pairs_with_pgls}")
            else:
                print(f"⚠️  pgls_id file not found, using file without pgls_id")
                qip_pairs_with_pgls = qip_pairs_with_top40

        # Step 9: Generate HTML report v2 (with top-40 visualization and Walmart links)
        cmd = [
            'python', 'generate_4s_report_v2.py',
            '--input', str(qip_pairs_with_pgls),
            '--output', str(html_report),
            '--with-matching'
        ]

        result = run_command(cmd, "Generate HTML report v2 (with top-40 and pgls_id)")
        if result != 0:
            return result

        steps_completed.append("HTML report v2 generation")

        # Success!
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print("\n" + "="*80)
        print("✅ PIPELINE V2 COMPLETED SUCCESSFULLY")
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
        print(f"  Preso Top-40:         {preso_top40_results}")
        print(f"  QI Pairs w/ Top-40:   {qip_pairs_with_top40}")
        print(f"  QI Pairs w/ pgls_id:  {qip_pairs_with_pgls}")
        print(f"  HTML Report v2:       {html_report}")

        print(f"\n🌐 Open report in browser:")
        print(f"  file://{html_report.absolute()}")

        # Write pipeline summary
        summary = {
            'experiment_id': experiment_id,
            'gcs_path': gcs_path,
            'email_file': args.email,
            'variant_engine': variant_engine,
            'control_engine': 'control',
            'pipeline_version': 'v2',
            'pipeline_config': {
                'queries_sampled': args.queries,
                'min_total': args.min_total,
                'max_total_diff': args.max_total_diff,
                'min_4s_gain': args.min_gain,
                'preso_qps': args.preso_qps,
                'top_n': 40
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
                'preso_top40_results': str(preso_top40_results),
                'qip_pairs_with_top40': str(qip_pairs_with_top40),
                'qip_pairs_with_pgls': str(qip_pairs_with_pgls),
                'html_report': str(html_report)
            },
            'top40_statistics': {
                'total_pairs': int(total_pairs),
                'in_control_top40': int(in_control),
                'in_variant_top40': int(in_variant),
                'pct_in_control_top40': float(in_control/total_pairs*100),
                'pct_in_variant_top40': float(in_variant/total_pairs*100)
            }
        }

        summary_file = reports_dir / f'{base_name}_{variant_engine}_pipeline_v2_summary.json'
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
