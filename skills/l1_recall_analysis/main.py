"""
L1 Ranker Recall Analysis Skill

Run end-to-end L1 ranker recall analysis pipeline with top-40 Preso crawl,
pgls_id fetching, and HTML report generation.

Usage:
    /l1-recall-analysis --email ./emails/experiment.eml
    /l1-recall-analysis --email ./emails/experiment.eml --skip-download --skip-preso
"""

import subprocess
import sys
from pathlib import Path
from .config import L1RecallAnalysisInput, L1RecallAnalysisOutput


def run(input_data: L1RecallAnalysisInput) -> L1RecallAnalysisOutput:
    """
    Run the L1 Recall Analysis pipeline.

    Args:
        input_data: Configuration for the pipeline

    Returns:
        L1RecallAnalysisOutput with status and results
    """
    try:
        # Get the project root (where run_analysis_pipeline_v2.py is located)
        skill_dir = Path(__file__).parent
        project_root = skill_dir.parent.parent  # skills/l1_recall_analysis/ -> skills/ -> project_root/

        pipeline_script = project_root / "run_analysis_pipeline_v2.py"

        if not pipeline_script.exists():
            return L1RecallAnalysisOutput(
                status="error",
                message=f"Pipeline script not found: {pipeline_script}"
            )

        # Build command
        cmd = [
            sys.executable,  # Use current Python interpreter
            str(pipeline_script),
            "--email", input_data.email_file
        ]

        # Add optional flags
        if input_data.skip_download:
            cmd.append("--skip-download")

        if input_data.skip_enrichment:
            cmd.append("--skip-enrichment")

        if input_data.skip_preso:
            cmd.append("--skip-preso")

        # Add optional parameters
        if input_data.queries is not None:
            cmd.extend(["--queries", str(input_data.queries)])

        if input_data.min_total != 5:  # Only add if non-default
            cmd.extend(["--min-total", str(input_data.min_total)])

        if input_data.max_total_diff != 5:
            cmd.extend(["--max-total-diff", str(input_data.max_total_diff)])

        if input_data.min_gain != 1:
            cmd.extend(["--min-gain", str(input_data.min_gain)])

        if input_data.preso_qps != 3:
            cmd.extend(["--preso-qps", str(input_data.preso_qps)])

        if input_data.variant:
            cmd.extend(["--variant", input_data.variant])

        print(f"\n🚀 Running L1 Recall Analysis Pipeline v2")
        print(f"📧 Email: {input_data.email_file}")
        print(f"⚙️  Command: {' '.join(cmd)}\n")

        # Run the pipeline
        result = subprocess.run(
            cmd,
            cwd=str(project_root),  # Run from project root
            capture_output=False,
            text=True
        )

        if result.returncode != 0:
            return L1RecallAnalysisOutput(
                status="error",
                message=f"Pipeline failed with exit code {result.returncode}"
            )

        # Try to read the pipeline summary to get output details
        temp_dir = project_root / "temp" / "downloaded_files"
        summary_files = list(temp_dir.glob("*_pipeline_summary.json"))

        if summary_files:
            import json
            with open(summary_files[-1], 'r') as f:
                summary = json.load(f)

            return L1RecallAnalysisOutput(
                status="success",
                message="Pipeline completed successfully",
                experiment_id=summary.get('experiment_id'),
                html_report=summary.get('output_files', {}).get('html_report'),
                output_files=summary.get('output_files'),
                statistics=summary.get('top40_statistics')
            )
        else:
            return L1RecallAnalysisOutput(
                status="success",
                message="Pipeline completed successfully (summary not found)"
            )

    except Exception as e:
        return L1RecallAnalysisOutput(
            status="error",
            message=f"Error running pipeline: {str(e)}"
        )


# CLI support for direct testing
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="L1 Recall Analysis Pipeline Skill")
    parser.add_argument("--email", required=True, help="Path to experiment email file")
    parser.add_argument("--skip-download", action="store_true", help="Skip GCS download")
    parser.add_argument("--skip-enrichment", action="store_true", help="Skip enrichment")
    parser.add_argument("--skip-preso", action="store_true", help="Skip Preso crawl")
    parser.add_argument("--queries", type=int, help="Number of queries to sample")
    parser.add_argument("--min-total", type=int, default=5, help="Min total items")
    parser.add_argument("--max-total-diff", type=int, default=5, help="Max total diff")
    parser.add_argument("--min-gain", type=int, default=1, help="Min 4s gain")
    parser.add_argument("--preso-qps", type=int, default=3, help="Preso QPS")
    parser.add_argument("--variant", help="Variant engine name")

    args = parser.parse_args()

    input_data = L1RecallAnalysisInput(
        email_file=args.email,
        skip_download=args.skip_download,
        skip_enrichment=args.skip_enrichment,
        skip_preso=args.skip_preso,
        queries=args.queries,
        min_total=args.min_total,
        max_total_diff=args.max_total_diff,
        min_gain=args.min_gain,
        preso_qps=args.preso_qps,
        variant=args.variant
    )

    output = run(input_data)
    print(f"\n{'='*80}")
    print(f"Status: {output.status}")
    print(f"Message: {output.message}")
    if output.html_report:
        print(f"HTML Report: {output.html_report}")
    print(f"{'='*80}")
