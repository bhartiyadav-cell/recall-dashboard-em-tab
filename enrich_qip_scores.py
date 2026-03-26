#!/usr/bin/env python3
"""
Enrich QIP scores with query context from Perceive API.

This script:
1. Loads qip_scores.parquet
2. Samples N unique queries (not rows!)
3. Fetches query context features from Perceive API
4. Merges context back to ALL rows with those queries
5. Saves enriched file

Usage:
    python enrich_qip_scores.py --queries 100
    python enrich_qip_scores.py --queries 100 --sample-rows 10000
"""

import argparse
import pandas as pd
from pathlib import Path
from skills.query_context import run, QueryContextInput


def main():
    parser = argparse.ArgumentParser(description='Enrich QIP scores with query context')
    parser.add_argument('--queries', type=int, default=None,
                        help='Number of unique queries to enrich (default: all queries)')
    parser.add_argument('--sample-rows', type=int,
                        help='Optional: Sample N rows first (for faster loading), then extract unique queries')
    parser.add_argument('--concurrency', type=int, default=100,
                        help='Max concurrent requests (default: 100)')
    parser.add_argument('--output', type=str,
                        default='./temp/downloaded_files/qip_scores_enriched.parquet',
                        help='Output file path')
    parser.add_argument('--input', type=str,
                        default='./temp/downloaded_files/qip_scores.parquet',
                        help='Input QIP scores file')
    parser.add_argument('--item-attributes', type=str,
                        default='./temp/downloaded_files/item_attributes_sample-5000.jsonl',
                        help='Item attributes JSONL file')

    args = parser.parse_args()

    print("="*70)
    print("Enriching QIP Scores with Query Context")
    print("="*70)

    # Check if input file exists
    if not Path(args.input).exists():
        print(f"\n❌ Input file not found: {args.input}")
        print("Run test_gcs_download.py first to download files!")
        return 1

    # Load QIP scores
    print(f"\nLoading QIP scores from: {args.input}")
    df = pd.read_parquet(args.input)
    print(f"  Loaded {len(df):,} rows")

    # Sample rows first if requested (for faster processing)
    if args.sample_rows:
        df = df.sample(n=min(args.sample_rows, len(df)), random_state=42)
        print(f"  Randomly sampled {len(df):,} rows")

    # Identify query column
    query_col = 'contextualQuery' if 'contextualQuery' in df.columns else 'query'
    print(f"  Query column: {query_col}")

    # Extract unique queries
    all_queries = df[query_col].dropna().unique()
    print(f"  Total unique queries: {len(all_queries):,}")

    # Sample queries
    if args.queries and args.queries < len(all_queries):
        import random
        sampled_queries = random.sample(list(all_queries), args.queries)
        print(f"  Randomly sampled {len(sampled_queries):,} queries to enrich")
    else:
        sampled_queries = all_queries
        print(f"  Enriching all {len(sampled_queries):,} unique queries")

    # Filter to rows with sampled queries
    df_filtered = df[df[query_col].isin(sampled_queries)].copy()
    print(f"  Rows with sampled queries: {len(df_filtered):,} / {len(df):,}")

    # Create input config with just the unique queries
    print(f"\nFetching query context for {len(sampled_queries):,} unique queries...")
    print(f"  Concurrency: {args.concurrency}")
    input_config = QueryContextInput(
        queries=list(sampled_queries),  # Pass list of unique queries
        concurrency_limit=args.concurrency,
        retry_limit=5,
        timeout_seconds=5,
        include_pt_features=True
    )

    # Run enrichment
    result = run(input_config)

    # Display results
    print("\n" + "="*70)
    print("Enrichment Results")
    print("="*70)
    print(f"✓ Successfully processed: {result.queries_processed:,} queries")
    print(f"✗ Failed: {result.queries_failed:,} queries")

    # The result.enriched_df now has query-level data
    query_features = result.enriched_df

    # Rename query column for merging if needed
    if query_col == 'contextualQuery':
        query_features = query_features.rename(columns={'query': 'contextualQuery'})

    # Merge query features back to ALL rows
    print(f"\nMerging query features back to {len(df_filtered):,} rows...")
    df_enriched = df_filtered.merge(
        query_features,
        on=query_col,
        how='left'
    )

    print(f"  Enriched {len(df_enriched):,} rows with query context")

    # Join with item attributes
    item_attrs_path = args.item_attributes
    if Path(item_attrs_path).exists():
        print(f"\nJoining with item attributes...")
        attrs_df = pd.read_json(item_attrs_path, lines=True)
        print(f"  Loaded {len(attrs_df):,} items")

        if 'pg_prod_id' in df_enriched.columns and 'pg_prod_id' in attrs_df.columns:
            df_enriched = df_enriched.merge(
                attrs_df,
                on='pg_prod_id',
                how='left',
                suffixes=('', '_item_attr')
            )
            print(f"  Joined {len(df_enriched):,} rows with item attributes")

            # Check coverage
            has_product_name = df_enriched['product_name'].notna().sum()
            print(f"  Items with attributes: {has_product_name:,} / {len(df_enriched):,} ({has_product_name/len(df_enriched)*100:.1f}%)")
        else:
            print(f"  ⚠️ Cannot join: missing pg_prod_id column")
    else:
        print(f"\n⚠️ Item attributes not found: {item_attrs_path}")
        print("  Skipping item attribute join...")

    # Show sample
    print("\nSample of enriched data:")
    display_cols = [query_col, 'engine', 'label', 'scount', 'specificity', 'segment', 'vertical',
                    'n_product_types', 'n_brands', 'n_colors', 'n_genders']
    available_cols = [col for col in display_cols if col in df_enriched.columns]
    print(df_enriched[available_cols].head(10))

    # Show feature statistics
    print("\n" + "="*70)
    print("Feature Statistics")
    print("="*70)

    intent_features = {
        'Product Types': 'n_product_types',
        'Brands': 'n_brands',
        'Colors': 'n_colors',
        'Genders': 'n_genders',
        'Categories': 'n_categories'
    }

    print(f"\nIntent Coverage (across {len(df_enriched):,} rows):")
    for label, col in intent_features.items():
        if col in df_enriched.columns:
            has_intent = (df_enriched[col] > 0).sum()
            pct = has_intent / len(df_enriched) * 100
            print(f"  {label:15s}: {has_intent:7,} rows ({pct:.1f}%)")

    print(f"\nQuery-level coverage (across {len(sampled_queries):,} queries):")
    for label, col in intent_features.items():
        if col in query_features.columns:
            has_intent = (query_features[col] > 0).sum()
            pct = has_intent / len(query_features) * 100
            print(f"  {label:15s}: {has_intent:7,} queries ({pct:.1f}%)")

    # Show some example queries with rich annotations
    print("\n" + "="*70)
    print("Sample Queries with Intent Annotations")
    print("="*70)

    rich_queries = query_features[
        (query_features['n_brands'] > 0) |
        (query_features['n_colors'] > 0) |
        (query_features['n_genders'] > 0)
    ]

    if len(rich_queries) > 0:
        print(f"\nFound {len(rich_queries)} queries with brand/color/gender intent:")
        for idx, row in rich_queries.head(5).iterrows():
            q = row[query_col] if query_col in row.index else row['query']
            print(f"\n  Query: {q}")
            if row['n_brands'] > 0:
                print(f"    Brands: {row['n_brands']}")
            if row['n_colors'] > 0:
                print(f"    Colors: {row['n_colors']}")
            if row['n_genders'] > 0:
                print(f"    Genders: {row['n_genders']}")
            if row['n_product_types'] > 0:
                print(f"    Product Types: {row['n_product_types']}")
    else:
        print("\n⚠ No queries with brand/color/gender intent found in this sample")
        print("  This might indicate:")
        print("  - Queries are too generic (e.g., 'milk', 'water')")
        print("  - Try with more queries: --queries 500")

    # Save enriched data
    print(f"\n" + "="*70)
    print("Saving Results")
    print("="*70)
    print(f"Output: {args.output}")
    df_enriched.to_parquet(args.output, index=False)
    print(f"✓ Saved {len(df_enriched):,} rows")

    # Summary
    print("\n" + "="*70)
    print("Summary")
    print("="*70)
    print(f"✓ Enriched {len(sampled_queries):,} unique queries")
    print(f"✓ Applied to {len(df_enriched):,} rows")
    print(f"✓ Output: {args.output}")

    print("\nNext steps:")
    print("  1. Analyze query-item matches:")
    print(f"     python analyze_query_item_match.py")
    print("  2. Enrich more queries:")
    print(f"     python enrich_qip_scores.py --queries 500")

    return 0


if __name__ == '__main__':
    exit(main())
