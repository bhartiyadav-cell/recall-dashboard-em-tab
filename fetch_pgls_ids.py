#!/usr/bin/env python3
"""
Fetch pgls_id from Solr for products in the pairs data.
"""

import pandas as pd
import requests
from pathlib import Path
import argparse
from tqdm import tqdm
import time


def fetch_pgls_ids_batch(product_ids: list, batch_size: int = 50) -> dict:
    """
    Fetch pgls_id from Solr for a list of product IDs.

    Args:
        product_ids: List of pg_prod_id values
        batch_size: Number of products to fetch per request

    Returns:
        Dict mapping pg_prod_id -> pgls_id
    """
    solr_base = "http://app.b2cprodb.solr.polaris.glb.us.walmart.net/solr/polaris/select"

    pgls_map = {}

    # Process in batches
    total_batches = (len(product_ids) + batch_size - 1) // batch_size

    print(f"\nFetching pgls_id for {len(product_ids):,} products in {total_batches} batches...")

    for i in tqdm(range(0, len(product_ids), batch_size), desc="Fetching pgls_id"):
        batch = product_ids[i:i + batch_size]

        # Build OR query for batch
        or_query = " OR ".join(batch)

        params = {
            'fl': 'pgls_id,pg_prod_id',
            'fq': f'pg_prod_id: ({or_query})',
            'indent': 'true',
            'q.op': 'OR',
            'q': '*:*',
            'wt': 'json',
            'rows': batch_size
        }

        try:
            response = requests.get(solr_base, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            docs = data.get('response', {}).get('docs', [])

            # Extract pgls_id for each product
            for doc in docs:
                pg_prod_id = doc.get('pg_prod_id')
                pgls_id = doc.get('pgls_id')

                if pg_prod_id and pgls_id:
                    pgls_map[pg_prod_id] = pgls_id

        except Exception as e:
            print(f"\n⚠️  Error fetching batch {i//batch_size + 1}: {e}")
            continue

        # Small delay to avoid overwhelming Solr
        time.sleep(0.1)

    print(f"\n✅ Successfully fetched pgls_id for {len(pgls_map):,} / {len(product_ids):,} products")

    return pgls_map


def enrich_with_pgls_id(input_file: str, output_file: str, batch_size: int = 50):
    """
    Enrich pairs data with pgls_id from Solr.

    Args:
        input_file: Input parquet file with pairs data
        output_file: Output parquet file with pgls_id added
        batch_size: Batch size for Solr requests
    """
    print(f"\nLoading: {input_file}")
    df = pd.read_parquet(input_file)
    print(f"Loaded {len(df):,} pairs")

    # Get unique product IDs
    unique_products = df['pg_prod_id'].unique().tolist()
    print(f"Found {len(unique_products):,} unique products")

    # Fetch pgls_id for all products
    pgls_map = fetch_pgls_ids_batch(unique_products, batch_size=batch_size)

    # Add pgls_id column to dataframe
    df['pgls_id'] = df['pg_prod_id'].map(pgls_map)

    # Report results
    found_count = df['pgls_id'].notna().sum()
    missing_count = df['pgls_id'].isna().sum()

    print(f"\n📊 Results:")
    print(f"  Found pgls_id: {found_count:,} ({found_count/len(df)*100:.1f}%)")
    print(f"  Missing pgls_id: {missing_count:,} ({missing_count/len(df)*100:.1f}%)")

    # Save enriched data
    df.to_parquet(output_file, index=False)
    print(f"\n✅ Saved enriched data: {output_file}")

    return output_file


def main():
    parser = argparse.ArgumentParser(description='Fetch pgls_id from Solr and enrich pairs data')
    parser.add_argument(
        '--input',
        type=str,
        required=True,
        help='Input parquet file with pairs data'
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Output parquet file (default: adds _with_pgls to input filename)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=50,
        help='Batch size for Solr requests (default: 50)'
    )

    args = parser.parse_args()

    # Default output filename
    if not args.output:
        input_path = Path(args.input)
        output_path = input_path.parent / f"{input_path.stem}_with_pgls{input_path.suffix}"
        args.output = str(output_path)

    enrich_with_pgls_id(args.input, args.output, batch_size=args.batch_size)


if __name__ == '__main__':
    main()
