#!/usr/bin/env python3
"""
Quick viewer for enriched QIP scores.

Usage:
    python view_enriched_data.py
    python view_enriched_data.py --rows 50
"""

import argparse
import pandas as pd
import json
from pathlib import Path


def pretty_print_intent(intent_json: str, max_items: int = 3) -> str:
    """Format intent JSON for display."""
    try:
        intent_list = json.loads(intent_json)
        if not intent_list:
            return "None"

        items = [f"{item['value']} ({item['score']:.2f})" for item in intent_list[:max_items]]
        result = ", ".join(items)

        if len(intent_list) > max_items:
            result += f" +{len(intent_list) - max_items} more"

        return result
    except:
        return "Error"


def main():
    parser = argparse.ArgumentParser(description='View enriched QIP scores')
    parser.add_argument('--file', type=str,
                        default='./temp/downloaded_files/qip_scores_enriched.parquet',
                        help='Enriched parquet file')
    parser.add_argument('--rows', type=int, default=20,
                        help='Number of rows to display')
    parser.add_argument('--random', action='store_true',
                        help='Random sample instead of first N rows')
    parser.add_argument('--with-intent-only', action='store_true',
                        help='Show only rows with brand/color/gender intent')

    args = parser.parse_args()

    if not Path(args.file).exists():
        print(f"❌ File not found: {args.file}")
        print("\nRun: python enrich_qip_scores.py --queries 100")
        return 1

    print("="*80)
    print("Enriched QIP Scores Viewer")
    print("="*80)

    # Load data
    print(f"\nLoading: {args.file}")
    df = pd.read_parquet(args.file)
    print(f"Loaded {len(df):,} rows × {len(df.columns)} columns")

    # Basic info
    query_col = 'contextualQuery' if 'contextualQuery' in df.columns else 'query'
    print(f"\nUnique queries: {df[query_col].nunique():,}")
    print(f"Engines: {df['engine'].unique().tolist() if 'engine' in df.columns else 'N/A'}")

    # Intent coverage
    print("\n" + "="*80)
    print("Intent Coverage")
    print("="*80)

    intent_cols = {
        'Product Types': 'n_product_types',
        'Brands': 'n_brands',
        'Colors': 'n_colors',
        'Genders': 'n_genders',
        'Categories': 'n_categories'
    }

    for label, col in intent_cols.items():
        if col in df.columns:
            has_intent = (df[col] > 0).sum()
            pct = has_intent / len(df) * 100
            print(f"  {label:15s}: {has_intent:7,} / {len(df):,} rows ({pct:.1f}%)")

    # Filter if requested
    if args.with_intent_only:
        df = df[
            (df['n_brands'].fillna(0) > 0) |
            (df['n_colors'].fillna(0) > 0) |
            (df['n_genders'].fillna(0) > 0)
        ].copy()
        print(f"\nFiltered to {len(df):,} rows with brand/color/gender intent")

    # Sample rows
    if args.random:
        df_display = df.sample(n=min(args.rows, len(df)), random_state=None)
        sample_type = "Random Sample"
    else:
        df_display = df.head(args.rows)
        sample_type = "First Rows"

    # Show sample rows
    print("\n" + "="*80)
    print(f"{sample_type} (showing {len(df_display)})")
    print("="*80)

    for i, (idx, row) in enumerate(df_display.iterrows()):
        print(f"\n{'─'*80}")
        print(f"Row #{i+1}")
        print(f"{'─'*80}")

        # Basic info
        print(f"Query:    {row[query_col]}")
        print(f"Engine:   {row.get('engine', 'N/A')}")
        print(f"Rating:   {row.get('label', 'N/A')}")

        # Debug: Check if product_type column exists
        if i == 0:  # Only for first row
            item_cols = [c for c in row.index if c in ['product_type', 'brand', 'color', 'gender', 'title', 'description']]
            print(f"[DEBUG] Item attribute columns present: {item_cols}")

        if 'product_name' in row.index:
            print(f"Product:  {row['product_name'][:60]}...")

        # Query features
        if 'specificity' in row.index:
            print(f"\nQuery Features:")
            print(f"  Specificity: {row.get('specificity', 'N/A')}")
            print(f"  Segment:     {row.get('segment', 'N/A')}")
            print(f"  Vertical:    {row.get('vertical', 'N/A')}")

        # Intent annotations
        has_any_intent = False

        if 'product_type_intent' in row.index:
            pt = pretty_print_intent(row['product_type_intent'], max_items=3)
            if pt != "None":
                if not has_any_intent:
                    print(f"\nIntent Annotations:")
                    has_any_intent = True
                print(f"  Product Type: {pt}")

        if 'brand_intent' in row.index:
            brand = pretty_print_intent(row['brand_intent'], max_items=2)
            if brand != "None":
                if not has_any_intent:
                    print(f"\nIntent Annotations:")
                    has_any_intent = True
                print(f"  Brand:        {brand}")

        if 'color_intent' in row.index:
            color = pretty_print_intent(row['color_intent'], max_items=2)
            if color != "None":
                if not has_any_intent:
                    print(f"\nIntent Annotations:")
                    has_any_intent = True
                print(f"  Color:        {color}")

        if 'gender_intent' in row.index:
            gender = pretty_print_intent(row['gender_intent'], max_items=2)
            if gender != "None":
                if not has_any_intent:
                    print(f"\nIntent Annotations:")
                    has_any_intent = True
                print(f"  Gender:       {gender}")

        # Item attributes
        if 'brand' in row.index or 'product_type' in row.index:
            print(f"\nItem Attributes:")

            # Always try to show product_type if it exists
            pt_value = row.get('product_type', 'N/A')
            if pd.notna(pt_value) and str(pt_value) not in ['N/A', 'nan', '']:
                print(f"  Product Type: {pt_value}")

            print(f"  Brand:        {row.get('brand', 'N/A')}")
            print(f"  Color:        {row.get('color', 'N/A')}")
            print(f"  Gender:       {row.get('gender', 'N/A')}")

            # Show title and description for BM25 relevance
            if 'title' in row.index:
                title = str(row.get('title', ''))[:80]
                print(f"\n  Title:       {title}...")
            if 'description' in row.index:
                desc = str(row.get('description', ''))[:100]
                if desc and desc != 'nan':
                    print(f"  Description: {desc}...")

    print("\n" + "="*80)
    print("Summary")
    print("="*80)
    print(f"Displayed {len(df_display)} / {len(df):,} rows")
    print(f"\nTo see more:")
    print(f"  python view_enriched_data.py --rows 50 --random")
    print(f"  python view_enriched_data.py --with-intent-only --rows 50 --random")

    return 0


if __name__ == '__main__':
    exit(main())
