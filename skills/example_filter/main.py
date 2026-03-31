#!/usr/bin/env python3
"""
Example Filter Skill - Filter best examples of 4s added and non-4s removed

Filters items based on matching quality to identify the best examples:
- 4s added: Items with HIGH match quality (good attribute + title matches)
- Non-4s removed: Items with LOW match quality (missing attributes or poor title match)

This helps identify the most clear-cut cases where the variant made obviously good/bad decisions.

Input: QIP pairs DataFrame with matching scores (output from attribute_matching skill)
Output: Filtered DataFrame with top N examples per query based on matching criteria
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from pathlib import Path


@dataclass
class ExampleFilterInput:
    """Input configuration for example filter."""
    pairs_file: str  # Path to qip_pairs_with_matching.parquet
    output_file: str = './temp/downloaded_files/filtered_examples.parquet'

    # Filtering parameters for 4s added
    fours_added_criteria: Dict[str, Any] = field(default_factory=lambda: {
        'min_pt_match': 0.5,           # Minimum product type match
        'min_title_match': 0.3,        # Minimum title match score
        'min_matched_attributes': 2,   # At least N attributes match well (>0.5)
        'top_n_per_query': 5,          # Top N items per query
        'sort_by': 'overall_match'     # Sort by this column (descending)
    })

    # Filtering parameters for non-4s removed
    # Uses OR logic: if ANY criterion is met, item is selected
    non4s_removed_criteria: Dict[str, Any] = field(default_factory=lambda: {
        'max_pt_match': 0.3,           # Product type mismatch
        'max_title_match': 0.3,        # Poor title match
        'max_matched_attributes': 1,   # Very few attributes match well (<=N)
        'top_n_per_query': 5,          # Top N items per query
        'sort_by': 'overall_match'     # Sort by this column (ascending for worst matches)
    })

    # Include these change types
    include_change_types: List[str] = field(default_factory=lambda: ['4_gained', 'non4_removed'])


@dataclass
class ExampleFilterOutput:
    """Output from example filter."""
    filtered_df: pd.DataFrame
    summary: Dict[str, Any]
    status: str = "success"
    message: str = ""


class ExampleFilterSkill:
    """Filter best examples of 4s added and non-4s removed based on matching quality."""

    def __init__(self, verbose: bool = True):
        self.name = "example_filter"
        self.version = "1.0.0"
        self.verbose = verbose

    def count_matched_attributes(self, row: pd.Series, threshold: float = 0.5) -> int:
        """
        Count how many structured attributes have good matches.

        Attributes considered: pt, brand, color, gender
        """
        count = 0

        if row.get('pt_exact_match', 0) >= threshold:
            count += 1
        if row.get('brand_exact_match', 0) >= threshold:
            count += 1
        if row.get('color_exact_match', 0) >= threshold:
            count += 1
        if row.get('gender_exact_match', 0) >= threshold:
            count += 1

        return count

    def filter_4s_added(
        self,
        df: pd.DataFrame,
        criteria: Dict[str, Any],
        query_col: str
    ) -> pd.DataFrame:
        """
        Filter 4s added to get best examples (HIGH match quality).

        Criteria (ALL must be met):
        - High pt_match (product type alignment)
        - High title_match (query tokens in title)
        - At least N attributes match well
        """
        if self.verbose:
            print(f"\n🎯 Filtering 4s Added (seeking HIGH match quality)...")

        # Filter to 4s_gained
        fours = df[df['change_type'] == '4_gained'].copy()

        if len(fours) == 0:
            print("  ⚠️  No 4s added found")
            return pd.DataFrame()

        print(f"  Starting with {len(fours):,} 4s added")

        # Count matched attributes for each row
        fours['matched_attributes_count'] = fours.apply(
            lambda row: self.count_matched_attributes(row, threshold=0.5),
            axis=1
        )

        # Apply minimum thresholds (AND logic - all must pass)
        min_pt = criteria.get('min_pt_match', 0.5)
        min_title = criteria.get('min_title_match', 0.3)
        min_attrs = criteria.get('min_matched_attributes', 2)

        fours = fours[
            (fours['pt_exact_match'] >= min_pt) &
            (fours['title_match'] >= min_title) &
            (fours['matched_attributes_count'] >= min_attrs)
        ]

        print(f"  After quality filters: {len(fours):,} items")
        print(f"    - pt_exact_match >= {min_pt}")
        print(f"    - title_match >= {min_title}")
        print(f"    - matched_attributes >= {min_attrs}")

        if len(fours) == 0:
            print("  ⚠️  No items pass quality thresholds")
            return pd.DataFrame()

        # Get top N per query
        top_n = criteria.get('top_n_per_query', 5)
        sort_by = criteria.get('sort_by', 'overall_match')

        # Sort descending (best matches first)
        fours = fours.sort_values([query_col, sort_by], ascending=[True, False])

        # Take top N per query
        filtered = fours.groupby(query_col).head(top_n)

        print(f"  Final selection: {len(filtered):,} items ({top_n} per query)")

        # Show statistics
        print(f"\n  📊 Quality Statistics:")
        print(f"    Avg overall_match: {filtered['overall_match'].mean():.2f}")
        print(f"    Avg pt_exact_match: {filtered['pt_exact_match'].mean():.2f}")
        print(f"    Avg title_match: {filtered['title_match'].mean():.2f}")
        print(f"    Avg brand_exact_match: {filtered['brand_exact_match'].mean():.2f}")
        print(f"    Avg matched_attributes: {filtered['matched_attributes_count'].mean():.1f}")

        return filtered

    def filter_non4s_removed(
        self,
        df: pd.DataFrame,
        criteria: Dict[str, Any],
        query_col: str
    ) -> pd.DataFrame:
        """
        Filter non-4s removed to get worst examples (LOW match quality).

        Criteria (OR logic - ANY can trigger selection):
        - Low pt_match (product type mismatch)
        - Low title_match (query tokens NOT in title)
        - Very few attributes match well
        """
        if self.verbose:
            print(f"\n🔻 Filtering Non-4s Removed (seeking LOW match quality)...")

        # Filter to non4_removed
        non4s = df[df['change_type'] == 'non4_removed'].copy()

        if len(non4s) == 0:
            print("  ⚠️  No non-4s removed found")
            return pd.DataFrame()

        print(f"  Starting with {len(non4s):,} non-4s removed")

        # Count matched attributes for each row
        non4s['matched_attributes_count'] = non4s.apply(
            lambda row: self.count_matched_attributes(row, threshold=0.5),
            axis=1
        )

        # Apply maximum thresholds (OR logic - any criterion can trigger)
        max_pt = criteria.get('max_pt_match', 0.3)
        max_title = criteria.get('max_title_match', 0.3)
        max_attrs = criteria.get('max_matched_attributes', 1)

        # Select items that meet ANY of these criteria
        non4s = non4s[
            (non4s['pt_exact_match'] <= max_pt) |
            (non4s['title_match'] <= max_title) |
            (non4s['matched_attributes_count'] <= max_attrs)
        ]

        print(f"  After quality filters (OR logic): {len(non4s):,} items")
        print(f"    - pt_exact_match <= {max_pt} OR")
        print(f"    - title_match <= {max_title} OR")
        print(f"    - matched_attributes <= {max_attrs}")

        if len(non4s) == 0:
            print("  ⚠️  No items pass quality thresholds")
            return pd.DataFrame()

        # Get top N per query
        top_n = criteria.get('top_n_per_query', 5)
        sort_by = criteria.get('sort_by', 'overall_match')

        # Sort ascending (worst matches first)
        non4s = non4s.sort_values([query_col, sort_by], ascending=[True, True])

        # Take top N per query (worst N matches)
        filtered = non4s.groupby(query_col).head(top_n)

        print(f"  Final selection: {len(filtered):,} items ({top_n} per query)")

        # Show statistics
        print(f"\n  📊 Quality Statistics:")
        print(f"    Avg overall_match: {filtered['overall_match'].mean():.2f}")
        print(f"    Avg pt_exact_match: {filtered['pt_exact_match'].mean():.2f}")
        print(f"    Avg title_match: {filtered['title_match'].mean():.2f}")
        print(f"    Avg brand_exact_match: {filtered['brand_exact_match'].mean():.2f}")
        print(f"    Avg matched_attributes: {filtered['matched_attributes_count'].mean():.1f}")

        return filtered

    def run(self, input_config: ExampleFilterInput) -> ExampleFilterOutput:
        """
        Main execution function.

        Steps:
        1. Load pairs with matching scores
        2. Filter 4s added (high match quality)
        3. Filter non-4s removed (low match quality)
        4. Combine and save results
        """

        print("="*80)
        print("Example Filter - Select Best Examples by Match Quality")
        print("="*80)

        # Load input data
        print(f"\n📂 Loading: {input_config.pairs_file}")

        try:
            df = pd.read_parquet(input_config.pairs_file)
            print(f"✅ Loaded {len(df):,} QIP pairs")
        except Exception as e:
            return ExampleFilterOutput(
                filtered_df=pd.DataFrame(),
                summary={},
                status="error",
                message=f"Failed to load input file: {e}"
            )

        # Detect query column
        query_col = 'contextualQuery' if 'contextualQuery' in df.columns else 'query'
        print(f"   Using query column: {query_col}")

        # Check required columns
        required_cols = ['change_type', 'overall_match', 'title_match',
                        'pt_exact_match', 'brand_exact_match']
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            return ExampleFilterOutput(
                filtered_df=pd.DataFrame(),
                summary={},
                status="error",
                message=f"Missing required columns: {missing_cols}"
            )

        # Filter by change types
        include_types = input_config.include_change_types
        df = df[df['change_type'].isin(include_types)]
        print(f"   Filtered to change types: {include_types}")
        print(f"   {len(df):,} items remaining")

        # Filter 4s added
        fours_filtered = pd.DataFrame()
        if '4_gained' in include_types:
            fours_filtered = self.filter_4s_added(
                df,
                input_config.fours_added_criteria,
                query_col
            )

        # Filter non-4s removed
        non4s_filtered = pd.DataFrame()
        if 'non4_removed' in include_types:
            non4s_filtered = self.filter_non4s_removed(
                df,
                input_config.non4s_removed_criteria,
                query_col
            )

        # Combine results
        print("\n" + "="*80)
        print("📦 Combining Results")
        print("="*80)

        filtered_dfs = []
        if len(fours_filtered) > 0:
            filtered_dfs.append(fours_filtered)
            print(f"  4s added: {len(fours_filtered):,} items")

        if len(non4s_filtered) > 0:
            filtered_dfs.append(non4s_filtered)
            print(f"  Non-4s removed: {len(non4s_filtered):,} items")

        if not filtered_dfs:
            return ExampleFilterOutput(
                filtered_df=pd.DataFrame(),
                summary={'total_items': 0},
                status="success",
                message="No items passed filtering criteria"
            )

        result_df = pd.concat(filtered_dfs, ignore_index=True)
        print(f"\n  Total filtered items: {len(result_df):,}")

        # Calculate summary statistics
        summary = {
            'total_items': len(result_df),
            'unique_queries': result_df[query_col].nunique(),
            'by_change_type': result_df['change_type'].value_counts().to_dict(),
            'avg_overall_match': float(result_df['overall_match'].mean()),
            'avg_title_match': float(result_df['title_match'].mean()),
            'avg_pt_match': float(result_df['pt_exact_match'].mean()),
            'avg_brand_match': float(result_df['brand_exact_match'].mean()),
        }

        # By change type
        print(f"\n  📊 Breakdown by Change Type:")
        for change_type, count in summary['by_change_type'].items():
            print(f"    {change_type}: {count:,}")

        print(f"\n  📊 Overall Statistics:")
        print(f"    Avg overall_match: {summary['avg_overall_match']:.2f}")
        print(f"    Avg title_match: {summary['avg_title_match']:.2f}")
        print(f"    Avg pt_match: {summary['avg_pt_match']:.2f}")
        print(f"    Avg brand_match: {summary['avg_brand_match']:.2f}")

        # Save results
        print(f"\n💾 Saving filtered results...")
        output_path = Path(input_config.output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        result_df.to_parquet(output_path, index=False)
        print(f"✅ Saved: {output_path}")

        return ExampleFilterOutput(
            filtered_df=result_df,
            summary=summary,
            status="success",
            message=f"Filtered {len(result_df):,} examples from {len(df):,} pairs"
        )


def main():
    """CLI interface for example filter."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Filter best examples of 4s added and non-4s removed'
    )
    parser.add_argument(
        '--input',
        type=str,
        required=True,
        help='Input parquet file with matching scores (qip_pairs_with_matching.parquet)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='./temp/downloaded_files/filtered_examples.parquet',
        help='Output parquet file for filtered examples'
    )
    parser.add_argument(
        '--fours-min-pt',
        type=float,
        default=0.5,
        help='Minimum pt_exact_match for 4s added (default: 0.5)'
    )
    parser.add_argument(
        '--fours-min-title',
        type=float,
        default=0.3,
        help='Minimum title_match for 4s added (default: 0.3)'
    )
    parser.add_argument(
        '--fours-min-attrs',
        type=int,
        default=2,
        help='Minimum matched attributes for 4s added (default: 2)'
    )
    parser.add_argument(
        '--fours-top-n',
        type=int,
        default=5,
        help='Top N 4s added per query (default: 5)'
    )
    parser.add_argument(
        '--non4s-max-pt',
        type=float,
        default=0.3,
        help='Maximum pt_exact_match for non-4s removed (default: 0.3)'
    )
    parser.add_argument(
        '--non4s-max-title',
        type=float,
        default=0.3,
        help='Maximum title_match for non-4s removed (default: 0.3)'
    )
    parser.add_argument(
        '--non4s-max-attrs',
        type=int,
        default=1,
        help='Maximum matched attributes for non-4s removed (default: 1)'
    )
    parser.add_argument(
        '--non4s-top-n',
        type=int,
        default=5,
        help='Top N non-4s removed per query (default: 5)'
    )

    args = parser.parse_args()

    # Create input config
    input_config = ExampleFilterInput(
        pairs_file=args.input,
        output_file=args.output,
        fours_added_criteria={
            'min_pt_match': args.fours_min_pt,
            'min_title_match': args.fours_min_title,
            'min_matched_attributes': args.fours_min_attrs,
            'top_n_per_query': args.fours_top_n,
            'sort_by': 'overall_match'
        },
        non4s_removed_criteria={
            'max_pt_match': args.non4s_max_pt,
            'max_title_match': args.non4s_max_title,
            'max_matched_attributes': args.non4s_max_attrs,
            'top_n_per_query': args.non4s_top_n,
            'sort_by': 'overall_match'
        }
    )

    # Run filter
    skill = ExampleFilterSkill(verbose=True)
    output = skill.run(input_config)

    if output.status == "success":
        print(f"\n✅ {output.message}")
        return 0
    else:
        print(f"\n❌ {output.message}")
        return 1


if __name__ == '__main__':
    exit(main())
