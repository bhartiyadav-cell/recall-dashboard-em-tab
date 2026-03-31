#!/usr/bin/env python3
"""
Enrich QIP scores with query context from Perceive API.

This script adds contextual information (product type, brand, color, gender)
to each query to improve attribute matching.
"""

import argparse
import sys
import pandas as pd
from pathlib import Path

# Import the query_context skill
from skills.query_context.main import run as query_context_run
from skills.query_context.config import QueryContextInput


def main():
    parser = argparse.ArgumentParser(description='Enrich QIP scores with Perceive API context')
    parser.add_argument('--input', required=True, help='Input QIP scores parquet file')
    parser.add_argument('--output', required=True, help='Output enriched parquet file')
    parser.add_argument('--queries', type=int, help='Number of queries to sample (default: all)')
    parser.add_argument('--item-attributes', help='Item attributes file (optional, for compatibility)')
    parser.add_argument('--concurrency', type=int, default=10, help='Concurrency level (default: 10)')

    args = parser.parse_args()

    print(f"\nEnriching QIP scores: {args.input}")
    print(f"Output: {args.output}")
    if args.queries:
        print(f"Sampling {args.queries} queries")
    if args.item_attributes:
        print(f"Item attributes: {args.item_attributes}")

    # Load input data
    print(f"\nLoading QIP scores...")
    df = pd.read_parquet(args.input)
    print(f"Loaded {len(df):,} rows")

    # Determine query column
    query_col = 'contextualQuery' if 'contextualQuery' in df.columns else 'query'
    print(f"Query column: {query_col}")

    # Get unique queries
    unique_queries = df[query_col].unique().tolist()
    print(f"Unique queries: {len(unique_queries):,}")

    # Sample if requested
    if args.queries and args.queries < len(unique_queries):
        import random
        random.seed(42)
        sampled_queries = random.sample(unique_queries, args.queries)
        print(f"Sampled {len(sampled_queries)} queries")
    else:
        sampled_queries = unique_queries

    # Call query_context skill
    print(f"\nEnriching queries with Perceive API (concurrency={args.concurrency})...")
    input_data = QueryContextInput(
        queries=sampled_queries,
        concurrency_limit=args.concurrency,
        retry_limit=5,
        timeout_seconds=10
    )

    output = query_context_run(input_data)

    # Merge enrichment back to original data
    print(f"\nMerging enrichment back to original data...")
    enriched_df = output.enriched_df

    # Rename 'query' column to match original query column if needed
    if 'query' in enriched_df.columns and query_col != 'query':
        enriched_df = enriched_df.rename(columns={'query': query_col})

    # Merge with original data
    df_enriched = df.merge(enriched_df, on=query_col, how='left')

    # Load and merge item attributes if provided
    if args.item_attributes and Path(args.item_attributes).exists():
        print(f"\nLoading item attributes: {args.item_attributes}")
        import json

        # Load JSONL file
        items_data = []
        with open(args.item_attributes, 'r') as f:
            for line in f:
                try:
                    items_data.append(json.loads(line))
                except:
                    continue

        if items_data:
            items_df = pd.DataFrame(items_data)
            print(f"Loaded {len(items_df):,} item attributes")

            # Merge item attributes with QIP scores on item_id or pg_prod_id
            item_id_col = 'pg_prod_id' if 'pg_prod_id' in df_enriched.columns else 'item_id'
            item_attr_id_col = 'pg_prod_id' if 'pg_prod_id' in items_df.columns else 'item_id'

            if item_id_col in df_enriched.columns and item_attr_id_col in items_df.columns:
                # Keep only useful columns from items_df
                item_cols_to_keep = [item_attr_id_col]
                for col in ['product_type', 'color', 'gender', 'description', 'category']:
                    if col in items_df.columns:
                        item_cols_to_keep.append(col)

                items_df_subset = items_df[item_cols_to_keep].drop_duplicates(subset=[item_attr_id_col])

                # Merge
                df_enriched = df_enriched.merge(
                    items_df_subset,
                    left_on=item_id_col,
                    right_on=item_attr_id_col,
                    how='left',
                    suffixes=('', '_item')
                )
                print(f"✅ Merged item attributes for {df_enriched[item_id_col].nunique():,} unique items")
            else:
                print(f"⚠️  Could not merge: item ID column not found")

    # Save enriched data
    df_enriched.to_parquet(args.output, index=False)
    print(f"\n✅ Enrichment complete!")
    print(f"   Queries processed: {output.queries_processed}")
    print(f"   Queries failed: {output.queries_failed}")
    print(f"   Features extracted: {len(output.features_extracted)}")
    print(f"   Output: {args.output}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
