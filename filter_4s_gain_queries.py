#!/usr/bin/env python3
"""
Filter queries with 4s gains in variant vs control.

This script:
1. Applies recall_analyser filtering logic (min_total=400, max_total_diff=5)
2. Identifies queries where variant has MORE 4s than control
3. Filters dataset to only these queries
4. Outputs filtered data for downstream analysis

Usage:
    python filter_4s_gain_queries.py --variant nlfv3_alp1_utbeta05_w0_4
    python filter_4s_gain_queries.py --variant nltrain_alp1_w0_4 --min-gain 5
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import json

# Import recall analysis functions
import recall_analysis_lib as ral


def compute_4s_per_query(df: pd.DataFrame, query_col: str = 'contextualQuery') -> pd.DataFrame:
    """
    Compute count of 4-rated items per query per engine.

    Returns DataFrame with:
    - query
    - engine
    - total_items
    - count_4s
    - pct_4s
    """
    # Total items per query per engine
    total = df.groupby([query_col, 'engine']).size().reset_index(name='total_items')

    # Count 4s
    df_4s = df[df['label'] == 4].groupby([query_col, 'engine']).size().reset_index(name='count_4s')

    # Merge
    stats = total.merge(df_4s, on=[query_col, 'engine'], how='left')
    stats['count_4s'] = stats['count_4s'].fillna(0).astype(int)
    stats['pct_4s'] = (stats['count_4s'] / stats['total_items'] * 100).round(2)

    return stats


def apply_recall_analyser_filtering(
    df: pd.DataFrame,
    control_engine: str,
    variant_engine: str,
    min_total: int = 400,
    max_total_diff: int = 5,
    query_col: str = 'contextualQuery'
) -> pd.DataFrame:
    """
    Apply recall_analyser filtering logic to find comparable queries.

    Returns DataFrame with queries that pass filtering.
    """
    print("\n" + "="*80)
    print("Applying recall_analyser Filtering Logic")
    print("="*80)

    # Split by engine
    df_ctrl = df[df['engine'] == control_engine].copy()
    df_var = df[df['engine'] == variant_engine].copy()

    print(f"\nControl engine: {control_engine} ({len(df_ctrl):,} rows)")
    print(f"Variant engine: {variant_engine} ({len(df_var):,} rows)")

    # Compute distributions
    ctrl_dist = ral.compute_distribution_by_query(df_ctrl)
    var_dist = ral.compute_distribution_by_query(df_var)

    print(f"\nControl queries: {len(ctrl_dist)}")
    print(f"Variant queries: {len(var_dist)}")

    # Merge with filtering
    comparison = ral.merge_control_variant_distributions(
        ctrl_dist, var_dist,
        min_total=min_total,
        max_total_diff=max_total_diff
    )

    print(f"\nQueries passing filter: {len(comparison)}")
    print(f"  min_total >= {min_total}")
    print(f"  |total_ctrl - total_var| <= {max_total_diff}")

    return comparison


def filter_queries_with_4s_gain(
    df: pd.DataFrame,
    control_engine: str,
    variant_engine: str,
    min_gain: int = 1,
    query_col: str = 'contextualQuery'
) -> list:
    """
    Identify queries where variant has more 4s than control.

    Returns list of queries with 4s gain.
    """
    print("\n" + "="*80)
    print("Identifying Queries with 4s Gain")
    print("="*80)

    # Compute 4s per query
    stats = compute_4s_per_query(df, query_col)

    # Pivot to compare control vs variant
    pivot = stats.pivot_table(
        index=query_col,
        columns='engine',
        values='count_4s',
        fill_value=0
    )

    if control_engine not in pivot.columns or variant_engine not in pivot.columns:
        print(f"❌ Missing engine columns in pivot")
        return []

    # Calculate gain
    pivot['4s_gain'] = pivot[variant_engine] - pivot[control_engine]

    # Filter to queries with gain >= min_gain
    queries_with_gain = pivot[pivot['4s_gain'] >= min_gain].index.tolist()

    print(f"\nQueries with 4s gain >= {min_gain}: {len(queries_with_gain)}")

    if len(queries_with_gain) > 0:
        gain_stats = pivot[pivot['4s_gain'] >= min_gain]['4s_gain']
        print(f"  Average gain: {gain_stats.mean():.1f} 4s")
        print(f"  Median gain: {gain_stats.median():.1f} 4s")
        print(f"  Max gain: {gain_stats.max():.0f} 4s")
        print(f"  Total 4s added: {gain_stats.sum():.0f}")

        # Show top queries
        top_queries = pivot[pivot['4s_gain'] >= min_gain].nlargest(10, '4s_gain')
        print(f"\n  Top 10 queries by 4s gain:")
        for idx, row in top_queries.iterrows():
            print(f"    {idx[:60]:60s} | Gain: +{row['4s_gain']:.0f} (Ctrl: {row[control_engine]:.0f}, Var: {row[variant_engine]:.0f})")

    return queries_with_gain


def main():
    parser = argparse.ArgumentParser(
        description='Filter queries with 4s gains in variant vs control'
    )
    parser.add_argument(
        '--input',
        type=str,
        default='./temp/downloaded_files/qip_scores_enriched.parquet',
        help='Input enriched QIP scores parquet file'
    )
    parser.add_argument(
        '--control',
        type=str,
        default='control',
        help='Control engine name'
    )
    parser.add_argument(
        '--variant',
        type=str,
        default=None,
        help='Variant engine name (auto-detect if not specified, e.g., nlfv3_alp1_utbeta05_w0_4)'
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
        help='Max difference in total items between control and variant (default: 5)'
    )
    parser.add_argument(
        '--min-gain',
        type=int,
        default=1,
        help='Minimum 4s gain required (default: 1)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='./temp/downloaded_files/qip_4s_gain_filtered.parquet',
        help='Output filtered parquet file'
    )

    args = parser.parse_args()

    # Check input file
    if not Path(args.input).exists():
        print(f"❌ Input file not found: {args.input}")
        return 1

    # Load data
    print(f"\n📁 Loading: {args.input}")
    df = pd.read_parquet(args.input)
    print(f"   Loaded {len(df):,} rows")

    # Check engines exist
    engines = df['engine'].unique()
    print(f"\n🔧 Available engines: {list(engines)}")

    if args.control not in engines:
        print(f"❌ Control engine '{args.control}' not found")
        return 1

    # Auto-detect variant if not specified
    if args.variant is None:
        variant_engines = [e for e in engines if e != args.control]
        if len(variant_engines) == 0:
            print(f"❌ No variant engine found (all engines are '{args.control}')")
            return 1
        args.variant = variant_engines[0]
        print(f"\n✅ Auto-detected variant engine: {args.variant}")
    else:
        if args.variant not in engines:
            print(f"❌ Variant engine '{args.variant}' not found")
            return 1

    query_col = 'contextualQuery' if 'contextualQuery' in df.columns else 'query'

    # Step 1: Apply recall_analyser filtering
    comparison = apply_recall_analyser_filtering(
        df,
        args.control,
        args.variant,
        min_total=args.min_total,
        max_total_diff=args.max_total_diff,
        query_col=query_col
    )

    if len(comparison) == 0:
        print("\n❌ No queries passed recall_analyser filtering")
        return 1

    # Filter df to only these comparable queries
    comparable_queries = comparison[query_col].unique()
    df_comparable = df[df[query_col].isin(comparable_queries)].copy()

    print(f"\n✅ Filtered to {len(df_comparable):,} rows with comparable queries")

    # Step 2: Filter to queries with 4s gain
    queries_with_gain = filter_queries_with_4s_gain(
        df_comparable,
        args.control,
        args.variant,
        min_gain=args.min_gain,
        query_col=query_col
    )

    if len(queries_with_gain) == 0:
        print(f"\n❌ No queries with 4s gain >= {args.min_gain}")
        return 1

    # Filter to only control and variant engines with 4s gain queries
    df_filtered = df_comparable[
        (df_comparable[query_col].isin(queries_with_gain)) &
        (df_comparable['engine'].isin([args.control, args.variant]))
    ].copy()

    print(f"\n✅ Final filtered dataset:")
    print(f"   Rows: {len(df_filtered):,}")
    print(f"   Queries: {df_filtered[query_col].nunique()}")
    print(f"   Engines: {list(df_filtered['engine'].unique())}")
    print(f"   Items: {df_filtered['pg_prod_id'].nunique():,}")

    # Save filtered data
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    df_filtered.to_parquet(args.output, index=False)
    print(f"\n💾 Saved to: {args.output}")

    # Save summary
    summary = {
        'control_engine': args.control,
        'variant_engine': args.variant,
        'filtering': {
            'min_total': args.min_total,
            'max_total_diff': args.max_total_diff,
            'min_4s_gain': args.min_gain
        },
        'results': {
            'total_rows': len(df_filtered),
            'unique_queries': int(df_filtered[query_col].nunique()),
            'unique_items': int(df_filtered['pg_prod_id'].nunique()),
            'queries_with_4s_gain': len(queries_with_gain)
        },
        'queries': queries_with_gain
    }

    summary_file = args.output.replace('.parquet', '_summary.json')
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"📊 Summary saved to: {summary_file}")

    print("\n" + "="*80)
    print("✅ FILTERING COMPLETE")
    print("="*80)
    print(f"\nNext steps:")
    print(f"  1. View filtered data: python view_enriched_data.py --input {args.output}")
    print(f"  2. Create QI pairs for analysis")
    print(f"  3. Analyze attribute matching patterns")

    return 0


if __name__ == '__main__':
    exit(main())
