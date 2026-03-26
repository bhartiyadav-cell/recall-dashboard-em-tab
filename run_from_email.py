#!/usr/bin/env python3
"""
End-to-End L1 Ranker Recall Analysis Pipeline - Starting from Email

This script automates the entire workflow:
1. Extract GCS path and experiment config from email
2. Download data from GCS
3. Enrich queries with Perceive API
4. Create QI pairs and compute matching
5. Build Preso URLs with experiment parameters
6. Generate interactive HTML report

Usage:
    python run_from_email.py --email path/to/email.eml
    python run_from_email.py --email-text "paste email content here"
"""

import argparse
import subprocess
import sys
from pathlib import Path
from skills.preso_url_builder import run as build_preso_urls, PresoUrlBuilderInput


def main():
    parser = argparse.ArgumentParser(
        description='Run L1 Ranker Recall Analysis from email'
    )
    parser.add_argument(
        '--email',
        type=str,
        help='Path to email file (.eml or .txt)'
    )
    parser.add_argument(
        '--email-text',
        type=str,
        help='Email content as text (alternative to --email)'
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

    args = parser.parse_args()

    if not args.email and not args.email_text:
        print("❌ Error: Must provide either --email or --email-text")
        print("\nUsage:")
        print("  python run_from_email.py --email path/to/email.eml")
        print("  python run_from_email.py --email-text 'email content...'")
        return 1

    print("="*80)
    print("L1 Ranker Recall Analysis Pipeline - Email Mode")
    print("="*80)

    # Step 1: Extract GCS path and experiment config from email
    print("\n📧 Step 1: Extracting information from email...")
    print("-"*80)

    if args.email:
        if not Path(args.email).exists():
            print(f"❌ Email file not found: {args.email}")
            return 1

        input_config = PresoUrlBuilderInput(email_file=args.email)
    else:
        input_config = PresoUrlBuilderInput(email_text=args.email_text)

    result = build_preso_urls(input_config)

    if result.status != "success":
        print(f"❌ Failed to extract email information: {result.message}")
        return 1

    gcs_path = result.gcs_path
    variant_engine = result.variant_engine_name

    if not gcs_path:
        print("❌ Could not extract GCS path from email")
        print("   Make sure the email contains a gs:// URL")
        return 1

    print(f"✅ Extracted GCS path: {gcs_path}")
    print(f"✅ Variant engine: {variant_engine}")

    # Save experiment config for later use
    experiment_id = gcs_path.split('/')[-1]
    config_file = Path(f'temp/downloaded_files/{experiment_id}_experiment_config.json')
    config_file.parent.mkdir(parents=True, exist_ok=True)

    # Save the config JSON
    import json
    full_config = {
        "engines": {
            "control": result.control_config,
            variant_engine: result.variant_config
        }
    }

    with open(config_file, 'w') as f:
        json.dump(full_config, f, indent=2)

    print(f"✅ Saved experiment config to: {config_file}")

    # Step 2: Run the main pipeline with extracted GCS path
    print("\n🚀 Step 2: Running analysis pipeline...")
    print("-"*80)

    pipeline_cmd = [
        'python', 'run_analysis_pipeline.py',
        '--gcs-path', gcs_path
    ]

    if args.skip_download:
        pipeline_cmd.append('--skip-download')

    if args.skip_enrichment:
        pipeline_cmd.append('--skip-enrichment')

    print(f"Command: {' '.join(pipeline_cmd)}")
    print()

    try:
        result = subprocess.run(pipeline_cmd, check=True)

        if result.returncode == 0:
            print("\n✅ Pipeline completed successfully!")
        else:
            print(f"\n❌ Pipeline failed with exit code: {result.returncode}")
            return 1

    except subprocess.CalledProcessError as e:
        print(f"\n❌ Pipeline execution failed: {e}")
        return 1

    # Step 3: Add Preso URLs to the pairs file
    print("\n🔗 Step 3: Adding Preso URLs to analysis...")
    print("-"*80)

    pairs_file = Path(f'temp/downloaded_files/{experiment_id}_qip_pairs_with_matching.parquet')

    if not pairs_file.exists():
        print(f"⚠️  Pairs file not found: {pairs_file}")
        print("   Skipping URL generation")
    else:
        print(f"Loading pairs file: {pairs_file}")

        import pandas as pd
        df = pd.read_parquet(pairs_file)

        # Build URLs for all queries in the pairs
        url_input = PresoUrlBuilderInput(
            config_json=json.dumps(full_config),
            add_to_dataframe=df
        )

        url_result = build_preso_urls(url_input)

        if url_result.status == "success" and url_result.urls_df is not None:
            # Save updated pairs with URLs
            url_result.urls_df.to_parquet(pairs_file, index=False)
            print(f"✅ Added Preso URLs to {len(url_result.urls_df)} pairs")
            print(f"   Saved to: {pairs_file}")
        else:
            print(f"⚠️  Could not add URLs: {url_result.message}")

    # Step 4: Regenerate HTML report with URLs
    print("\n📊 Step 4: Regenerating HTML report with Preso URLs...")
    print("-"*80)

    report_cmd = [
        'python', 'generate_4s_report.py',
        '--input', str(pairs_file),
        '--with-matching'
    ]

    try:
        subprocess.run(report_cmd, check=True)
        print("\n✅ HTML report regenerated with Preso URLs!")
    except subprocess.CalledProcessError as e:
        print(f"\n⚠️  Report generation failed: {e}")
        print("   The analysis is complete, but report may not have URLs")

    # Summary
    print("\n" + "="*80)
    print("✅ PIPELINE COMPLETE!")
    print("="*80)
    print(f"""
Experiment ID: {experiment_id}
GCS Path: {gcs_path}
Variant Engine: {variant_engine}

Output Files:
  - Enriched Data: temp/downloaded_files/{experiment_id}_qip_scores_enriched.parquet
  - QI Pairs: temp/downloaded_files/{experiment_id}_qip_pairs_with_matching.parquet
  - HTML Report: reports/{experiment_id}_{variant_engine}_4s_added_report.html
  - Experiment Config: {config_file}

Next Steps:
  - Open the HTML report to view 4s added by variant
  - Use Preso URLs to view actual search results
  - Analyze attribute matching patterns
""")

    return 0


if __name__ == '__main__':
    sys.exit(main())
