#!/usr/bin/env python3
"""
Create Query-Item-Pair (QIP) comparisons between control and variant.

For each query, identifies:
1. 4s GAINED: Items rated 4 in variant but not in control results (or lower rated in control)
2. Non-4s REMOVED: Items rated 1/2/3 in control but not in variant results
3. 4s LOST: Items rated 4 in control but not in variant results
4. Rating CHANGES: Items in both with different ratings

Usage:
    python create_qip_pairs.py --input ./temp/downloaded_files/qip_4s_gain_filtered.parquet
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import json


def create_qip_pairs(
    df: pd.DataFrame,
    control_engine: str,
    variant_engine: str,
    query_col: str = 'contextualQuery'
) -> pd.DataFrame:
    """
    Create QI pairs comparing control vs variant.

    Returns DataFrame with one row per (query, item) pair showing:
    - What happened: 4_gained, non4_removed, 4_lost, rating_changed, no_change
    - Control rating (if exists)
    - Variant rating (if exists)
    - All query intent attributes
    - All item attributes
    """
    print("\n" + "="*80)
    print("Creating QI Pairs: Control vs Variant")
    print("="*80)

    # Split by engine
    df_ctrl = df[df['engine'] == control_engine].copy()
    df_var = df[df['engine'] == variant_engine].copy()

    print(f"\nControl rows: {len(df_ctrl):,}")
    print(f"Variant rows: {len(df_var):,}")

    # Get all columns (exclude engine column)
    all_cols = [c for c in df.columns if c != 'engine']

    # Key columns for merging
    key_cols = [query_col, 'pg_prod_id']

    # Columns to keep from each engine
    cols_to_keep = [c for c in all_cols if c not in key_cols]

    # Prepare dataframes with suffixes
    ctrl_cols = key_cols + cols_to_keep
    var_cols = key_cols + cols_to_keep

    # Outer merge on query + item
    print(f"\nMerging on: {key_cols}")
    pairs = df_ctrl[ctrl_cols].merge(
        df_var[var_cols],
        on=key_cols,
        how='outer',
        suffixes=('_ctrl', '_var'),
        indicator=True
    )

    print(f"Total pairs: {len(pairs):,}")

    # Determine pair types
    pairs['pair_type'] = pairs['_merge'].map({
        'left_only': 'control_only',
        'right_only': 'variant_only',
        'both': 'both_engines'
    })

    # Get ratings
    label_ctrl = 'label_ctrl' if 'label_ctrl' in pairs.columns else 'label'
    label_var = 'label_var' if 'label_var' in pairs.columns else 'label'

    # Categorize what happened
    def categorize_pair(row):
        """Categorize what happened to this QI pair."""
        pair_type = row['pair_type']

        if pair_type == 'variant_only':
            # Item only in variant
            rating = row.get(label_var, np.nan)
            if rating == 4:
                return '4_gained'
            else:
                return 'other_gained'

        elif pair_type == 'control_only':
            # Item only in control
            rating = row.get(label_ctrl, np.nan)
            if rating == 4:
                return '4_lost'
            else:
                return 'non4_removed'

        elif pair_type == 'both_engines':
            # Item in both - ratings should be the same
            # (ratings don't change between engines, only recall changes)
            ctrl_rating = row.get(label_ctrl, np.nan)
            var_rating = row.get(label_var, np.nan)

            if pd.isna(ctrl_rating) or pd.isna(var_rating):
                return 'unknown'

            if ctrl_rating == var_rating:
                return 'no_change'
            else:
                # This shouldn't happen - ratings should be same for same item
                # But if it does, just mark as unknown
                return 'rating_mismatch'

        return 'unknown'

    pairs['change_type'] = pairs.apply(categorize_pair, axis=1)

    # Consolidate duplicate columns (keep one version)
    # For query-level attributes (same across all items in a query), use either _ctrl or _var
    query_level_cols = [
        'brand_intent', 'color_intent', 'gender_intent', 'product_type_intent',
        'n_brands', 'n_colors', 'n_genders', 'n_product_types',
        'scount', 'bcount', 'acount', 'specificity', 'segment',
        'l1_category', 'vertical'
    ]

    for col in query_level_cols:
        ctrl_col = f'{col}_ctrl'
        var_col = f'{col}_var'

        # Use variant version if available, otherwise control
        if var_col in pairs.columns and ctrl_col in pairs.columns:
            pairs[col] = pairs[var_col].fillna(pairs[ctrl_col])
            pairs.drop(columns=[ctrl_col, var_col], inplace=True, errors='ignore')
        elif ctrl_col in pairs.columns:
            pairs.rename(columns={ctrl_col: col}, inplace=True)
        elif var_col in pairs.columns:
            pairs.rename(columns={var_col: col}, inplace=True)

    # For item-level attributes, keep both versions with suffixes
    # (they should be the same, but good to verify)

    print("\n📊 Pair Type Distribution:")
    print(pairs['pair_type'].value_counts())

    print("\n🔄 Change Type Distribution:")
    change_counts = pairs['change_type'].value_counts()
    print(change_counts)

    # Summary by query
    print("\n📝 Summary by Query:")
    summary = pairs.groupby(query_col).agg({
        'change_type': 'count',
        'pg_prod_id': 'nunique'
    }).rename(columns={'change_type': 'total_pairs', 'pg_prod_id': 'unique_items'})

    # Count each change type per query
    change_summary = pairs.groupby([query_col, 'change_type']).size().unstack(fill_value=0)
    summary = summary.join(change_summary)

    print(f"\nQueries with pairs: {len(summary)}")
    print(f"Average pairs per query: {summary['total_pairs'].mean():.1f}")

    if '4_gained' in summary.columns:
        print(f"\nQueries with 4s gained: {(summary['4_gained'] > 0).sum()}")
        print(f"Total 4s gained: {summary['4_gained'].sum()}")

    if 'non4_removed' in summary.columns:
        print(f"Queries with non-4s removed: {(summary['non4_removed'] > 0).sum()}")
        print(f"Total non-4s removed: {summary['non4_removed'].sum()}")

    if '4_lost' in summary.columns:
        print(f"Queries with 4s lost: {(summary['4_lost'] > 0).sum()}")
        print(f"Total 4s lost: {summary['4_lost'].sum()}")

    return pairs


def main():
    parser = argparse.ArgumentParser(
        description='Create QI pairs comparing control vs variant'
    )
    parser.add_argument(
        '--input',
        type=str,
        default='./temp/downloaded_files/qip_4s_gain_filtered.parquet',
        help='Input filtered QIP scores'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='./temp/downloaded_files/qip_pairs.parquet',
        help='Output QI pairs file'
    )

    args = parser.parse_args()

    # Check input
    if not Path(args.input).exists():
        print(f"❌ Input file not found: {args.input}")
        return 1

    # Load summary to get engine names
    summary_file = args.input.replace('.parquet', '_summary.json')
    if not Path(summary_file).exists():
        print(f"❌ Summary file not found: {summary_file}")
        print("Run filter_4s_gain_queries.py first")
        return 1

    with open(summary_file, 'r') as f:
        summary = json.load(f)

    control_engine = summary['control_engine']
    variant_engine = summary['variant_engine']

    print(f"\n📁 Loading: {args.input}")
    df = pd.read_parquet(args.input)
    print(f"   Loaded {len(df):,} rows")

    print(f"\n🔧 Engines:")
    print(f"   Control: {control_engine}")
    print(f"   Variant: {variant_engine}")

    # Create pairs
    query_col = 'contextualQuery' if 'contextualQuery' in df.columns else 'query'
    pairs = create_qip_pairs(df, control_engine, variant_engine, query_col)

    # Save pairs
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    pairs.to_parquet(args.output, index=False)
    print(f"\n💾 Saved {len(pairs):,} pairs to: {args.output}")

    # Save detailed summary
    output_summary = {
        'control_engine': control_engine,
        'variant_engine': variant_engine,
        'total_pairs': len(pairs),
        'unique_queries': int(pairs[query_col].nunique()),
        'unique_items': int(pairs['pg_prod_id'].nunique()),
        'change_type_counts': pairs['change_type'].value_counts().to_dict(),
        'pair_type_counts': pairs['pair_type'].value_counts().to_dict()
    }

    summary_out = args.output.replace('.parquet', '_summary.json')
    with open(summary_out, 'w') as f:
        json.dump(output_summary, f, indent=2)
    print(f"📊 Summary saved to: {summary_out}")

    print("\n" + "="*80)
    print("✅ QI PAIRS CREATED")
    print("="*80)
    print(f"\nNext steps:")
    print(f"  1. View pairs: python view_qip_pairs.py")
    print(f"  2. Analyze 4s gained: python view_qip_pairs.py --filter 4_gained")
    print(f"  3. Analyze non-4s removed: python view_qip_pairs.py --filter non4_removed")

    return 0


if __name__ == '__main__':
    exit(main())
