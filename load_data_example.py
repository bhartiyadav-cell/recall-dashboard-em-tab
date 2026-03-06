#!/usr/bin/env python3
"""
Example: How to load and work with downloaded files from GCS.

This shows how to:
1. Load qip_scores.parquet (has ratings and metadata like stores, zipcode, state)
2. Load item_attributes JSONL (has item details like brand, color, description)
3. Extract ptss/trsp from qip_scores (no separate config file needed)
4. Join the data for analysis
"""

import pandas as pd
import json
from pathlib import Path

def load_qip_scores(file_path: str) -> pd.DataFrame:
    """
    Load QIP scores parquet file.

    This file contains:
    - contextualQuery, item_id, pg_prod_id
    - label (rating score)
    - stores, zipcode, state (for Sunlight URLs)
    - polarisUrl (for ptss/trsp extraction)
    - brand, engine, etc.
    """
    print(f"Loading QIP scores from: {file_path}")
    df = pd.read_parquet(file_path)

    print(f"  Loaded {len(df):,} rows")
    print(f"  Columns: {df.columns.tolist()}")

    # Handle different column names
    query_col = 'contextualQuery' if 'contextualQuery' in df.columns else 'query'
    item_col = 'item_id' if 'item_id' in df.columns else 'pg_prod_id'

    print(f"  Unique queries: {df[query_col].nunique():,}")
    print(f"  Unique items: {df[item_col].nunique():,}")

    return df


def load_item_attributes(file_path: str) -> pd.DataFrame:
    """
    Load item attributes from JSONL file.

    This file contains detailed item metadata:
    - item_id, pg_prod_id
    - product_name, brand, color, gender
    - description, url, image
    """
    print(f"\nLoading item attributes from: {file_path}")
    df = pd.read_json(file_path, lines=True)

    print(f"  Loaded {len(df):,} items")
    print(f"  Columns: {df.columns.tolist()}")

    return df


def extract_ptss_trsp_from_qip_scores(df: pd.DataFrame) -> dict:
    """
    Extract ptss and trsp parameters from qip_scores.

    These are typically in the 'polarisUrl' column or similar fields.
    Used for building Sunlight URLs.
    """
    print("\nExtracting ptss/trsp parameters...")

    # Check what columns we have that might contain this info
    potential_cols = [col for col in df.columns if 'url' in col.lower() or 'ptss' in col.lower() or 'trsp' in col.lower()]
    print(f"  Potential columns: {potential_cols}")

    # Try to extract from polarisUrl if available
    if 'polarisUrl' in df.columns:
        sample_url = df['polarisUrl'].dropna().iloc[0] if len(df['polarisUrl'].dropna()) > 0 else None
        if sample_url:
            print(f"  Sample polarisUrl: {sample_url[:100]}...")

            # Extract ptss and trsp from URL
            # Format: ...?ptss=param1:val1;param2:val2&trsp=param3:val3
            ptss = {}
            trsp = {}

            if 'ptss=' in sample_url:
                ptss_part = sample_url.split('ptss=')[1].split('&')[0]
                for param in ptss_part.split(';'):
                    if ':' in param:
                        key, val = param.split(':', 1)
                        ptss[key] = val

            if 'trsp=' in sample_url:
                trsp_part = sample_url.split('trsp=')[1].split('&')[0]
                for param in trsp_part.split(';'):
                    if ':' in param:
                        key, val = param.split(':', 1)
                        trsp[key] = val

            return {
                'ptss': ptss,
                'trsp': trsp,
                'sample_url': sample_url
            }

    # Fallback: check for engine column (common in recall analysis)
    if 'engine' in df.columns:
        engines = df['engine'].unique().tolist()
        print(f"  Found engines: {engines}")
        return {
            'engines': engines,
            'control': 'control' if 'control' in engines else engines[0],
            'variants': [e for e in engines if e != 'control']
        }

    return {}


def extract_metadata_for_sunlight(df: pd.DataFrame) -> dict:
    """
    Extract metadata needed for Sunlight URL generation.

    From qip_scores: stores, zipcode, state, polarisUrl
    """
    print("\nExtracting metadata for Sunlight URLs...")

    # Get a sample row
    sample_row = df.iloc[0]

    metadata = {}

    if 'stores' in df.columns:
        metadata['stores'] = int(sample_row['stores']) if pd.notna(sample_row['stores']) else 100

    if 'zipcode' in df.columns:
        metadata['zipcode'] = str(sample_row['zipcode']) if pd.notna(sample_row['zipcode']) else '72712'

    if 'state' in df.columns:
        metadata['state'] = str(sample_row['state']) if pd.notna(sample_row['state']) else 'AR'

    if 'polarisUrl' in df.columns:
        metadata['polarisUrl'] = sample_row['polarisUrl'] if pd.notna(sample_row['polarisUrl']) else None

    print(f"  Metadata: {metadata}")
    return metadata


def join_qip_with_attributes(qip_df: pd.DataFrame, attr_df: pd.DataFrame) -> pd.DataFrame:
    """
    Join QIP scores with item attributes.

    Args:
        qip_df: QIP scores DataFrame
        attr_df: Item attributes DataFrame

    Returns:
        Merged DataFrame with both ratings and detailed attributes
    """
    print("\nJoining QIP scores with item attributes...")

    # Join on pg_prod_id (or item_id if pg_prod_id not available)
    if 'pg_prod_id' in qip_df.columns and 'pg_prod_id' in attr_df.columns:
        print("  Joining on pg_prod_id...")
        merged = qip_df.merge(
            attr_df,
            on='pg_prod_id',
            how='left',
            suffixes=('', '_attr')
        )
    elif 'item_id' in qip_df.columns and 'item_id' in attr_df.columns:
        print("  Joining on item_id...")
        merged = qip_df.merge(
            attr_df,
            on='item_id',
            how='left',
            suffixes=('', '_attr')
        )
    else:
        print("  Warning: No common join key found!")
        return qip_df

    print(f"  Merged {len(merged):,} rows")

    # Count how many items got attributes
    if 'product_name' in merged.columns:
        matched = merged['product_name'].notna().sum()
        print(f"  Items with attributes: {matched:,} ({matched/len(merged)*100:.1f}%)")

    return merged


def main():
    """Example workflow."""

    # Paths to downloaded files
    download_dir = './temp/downloaded_files'

    qip_scores_path = f'{download_dir}/qip_scores.parquet'
    item_attrs_path = f'{download_dir}/item_attributes_sample-5000.jsonl'

    print("="*60)
    print("Loading Data from Downloaded Files")
    print("="*60)

    # Check if files exist
    if not Path(qip_scores_path).exists():
        print(f"\n❌ QIP scores not found: {qip_scores_path}")
        print("Run test_gcs_download.py first to download files!")
        return 1

    # 1. Load QIP scores (has ratings + basic item info + metadata)
    qip_df = load_qip_scores(qip_scores_path)

    print(f"\nSample QIP scores data:")
    print(qip_df.head())

    # 2. Extract ptss/trsp (for Sunlight URLs)
    config_info = extract_ptss_trsp_from_qip_scores(qip_df)
    print(f"\nExtracted config info:")
    print(json.dumps(config_info, indent=2, default=str))

    # 3. Extract metadata for Sunlight URLs
    sunlight_metadata = extract_metadata_for_sunlight(qip_df)

    # 4. Load item attributes (detailed product info) if available
    if Path(item_attrs_path).exists():
        attr_df = load_item_attributes(item_attrs_path)

        print(f"\nSample item attributes:")
        print(attr_df.head())

        # 5. Join QIP scores with attributes
        merged_df = join_qip_with_attributes(qip_df, attr_df)

        print(f"\nMerged data sample:")
        # Use actual column names from the data
        sample_cols = []
        if 'contextualQuery' in merged_df.columns:
            sample_cols.append('contextualQuery')
        elif 'query' in merged_df.columns:
            sample_cols.append('query')

        sample_cols.append('item_id')

        if 'label' in merged_df.columns:
            sample_cols.append('label')
        elif 'rating_score' in merged_df.columns:
            sample_cols.append('rating_score')

        if 'product_name' in merged_df.columns:
            sample_cols.append('product_name')
        if 'brand' in merged_df.columns:
            sample_cols.append('brand')
        if 'color' in merged_df.columns:
            sample_cols.append('color')

        print(merged_df[sample_cols].head())

        # Save merged data
        output_path = f'{download_dir}/qip_scores_with_attributes.parquet'
        merged_df.to_parquet(output_path, index=False)
        print(f"\n✓ Saved merged data to: {output_path}")

        return merged_df, config_info, sunlight_metadata
    else:
        print(f"\n⚠ Item attributes not found: {item_attrs_path}")
        print("  Continuing with QIP scores only...")

        return qip_df, config_info, sunlight_metadata


if __name__ == '__main__':
    result = main()

    if result and result != 1:  # Check it's not an error code
        df, config, metadata = result

        print("\n" + "="*60)
        print("Summary")
        print("="*60)
        print(f"✓ Loaded data successfully")
        print(f"✓ Total rows: {len(df):,}")
        print(f"✓ Config info extracted: {bool(config)}")
        print(f"✓ Sunlight metadata extracted: {bool(metadata)}")
        print("\nYou can now use this data for recall analysis!")
    elif result == 1:
        print("\n" + "="*60)
        print("Please download files first:")
        print("  python test_gcs_download.py")
        print("="*60)
