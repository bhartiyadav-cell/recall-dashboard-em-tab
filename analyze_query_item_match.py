#!/usr/bin/env python3
"""
Analyze query-item attribute matching.

This script:
1. Loads enriched QIP scores (with query intent annotations)
2. Loads and joins item attributes
3. Compares query intents with item attributes
4. Shows detailed QI pair examples
5. Analyzes rating patterns

Usage:
    python analyze_query_item_match.py
    python analyze_query_item_match.py --sample 100
"""

import argparse
import pandas as pd
import json
from typing import List, Dict, Set
from pathlib import Path


def parse_intent(intent_json_str: str) -> List[Dict]:
    """Parse JSON intent string to list of dicts."""
    try:
        return json.loads(intent_json_str) if intent_json_str else []
    except:
        return []


def extract_intent_values(intent_list: List[Dict], top_n: int = 3) -> Set[str]:
    """Extract top N intent values as a set (case-insensitive)."""
    values = [item["value"].lower() for item in intent_list[:top_n] if "value" in item]
    return set(values)


def normalize_value(value) -> Set[str]:
    """Normalize item attribute value to set of lowercase strings."""
    if value is None or pd.isna(value):
        return set()

    if isinstance(value, list):
        return {str(v).lower().strip() for v in value if v}

    # Handle string that might be list-like "[value1, value2]"
    value_str = str(value).strip()

    if value_str.startswith('[') and value_str.endswith(']'):
        try:
            # Parse list string
            items = value_str[1:-1].split(',')
            return {v.strip(' "\'').lower() for v in items if v.strip()}
        except:
            return {value_str.lower()}
    else:
        return {value_str.lower()}


def check_attribute_match(item_attr_value, query_intent_values: Set[str]) -> dict:
    """Check if item attribute matches query intent."""
    has_intent = len(query_intent_values) > 0
    item_values = normalize_value(item_attr_value)
    has_item_value = len(item_values) > 0

    # Check for match (any overlap)
    match = False
    if has_intent and has_item_value:
        match = bool(item_values & query_intent_values)

    return {
        "has_intent": has_intent,
        "has_item_value": has_item_value,
        "match": match,
        "item_values": list(item_values),
        "query_values": list(query_intent_values)
    }


def print_qi_pair_example(row: pd.DataFrame, idx: int = 0):
    """Print detailed query-item pair with intent matching."""
    query_col = 'contextualQuery' if 'contextualQuery' in row.index else 'query'

    print(f"\n{'='*70}")
    print(f"QI Pair #{idx + 1}")
    print(f"{'='*70}")

    # Query info
    print(f"\n📝 QUERY: {row[query_col]}")
    print(f"   Engine: {row.get('engine', 'N/A')}")
    print(f"   Rating: {row.get('label', 'N/A')}")
    print(f"   Specificity: {row.get('specificity', 'N/A')}")

    # Query intents
    print(f"\n🎯 QUERY INTENTS:")

    intent_attrs = [
        ('product_type_intent', 'Product Type'),
        ('brand_intent', 'Brand'),
        ('color_intent', 'Color'),
        ('gender_intent', 'Gender'),
    ]

    query_wants = {}
    for intent_col, label in intent_attrs:
        if intent_col in row.index:
            intent_list = parse_intent(row[intent_col])
            if intent_list:
                top_values = [f"{item['value']} ({item['score']:.2f})" for item in intent_list[:2]]
                print(f"   {label:15s}: {', '.join(top_values)}")
                query_wants[label.lower().replace(' ', '_')] = [item['value'].lower() for item in intent_list[:3]]

    # Item info
    print(f"\n📦 ITEM:")
    print(f"   Product Name: {row.get('product_name', 'N/A')[:60]}...")
    print(f"   Product Type: {row.get('product_type', 'N/A')}")
    print(f"   Brand:        {row.get('brand', 'N/A')}")
    print(f"   Color:        {row.get('color', 'N/A')}")
    print(f"   Gender:       {row.get('gender', 'N/A')}")

    # Show title and description (important for BM25 scoring)
    if 'title' in row.index:
        title = str(row.get('title', ''))
        if title and title != 'nan':
            print(f"\n   Title: {title[:80]}...")

    if 'description' in row.index:
        desc = str(row.get('description', ''))
        if desc and desc != 'nan':
            print(f"   Description: {desc[:100]}...")

    # Match analysis
    print(f"\n🔍 MATCH ANALYSIS:")

    attribute_mappings = [
        ('product_type_intent', 'product_type', 'Product Type'),
        ('brand_intent', 'brand', 'Brand'),
        ('color_intent', 'color', 'Color'),
        ('gender_intent', 'gender', 'Gender'),
    ]

    matches = []
    mismatches = []

    for intent_col, item_col, label in attribute_mappings:
        if intent_col in row.index and item_col in row.index:
            intent_list = parse_intent(row[intent_col])
            intent_values = extract_intent_values(intent_list, top_n=3)

            if intent_values:
                item_value = row[item_col]
                result = check_attribute_match(item_value, intent_values)

                status = "✓" if result["match"] else "✗"
                match_str = "MATCH" if result["match"] else "MISMATCH"

                print(f"   {status} {label:15s}: {match_str}")
                print(f"      Query wants: {result['query_values']}")
                print(f"      Item has:    {result['item_values']}")

                if result["match"]:
                    matches.append(label)
                else:
                    mismatches.append(label)

    # Overall score
    if matches or mismatches:
        total = len(matches) + len(mismatches)
        score = len(matches) / total if total > 0 else 0
        print(f"\n   Overall Match Score: {score:.1%} ({len(matches)}/{total} attributes)")

        if mismatches:
            print(f"   ⚠️  Mismatched: {', '.join(mismatches)}")
    else:
        print(f"\n   ℹ️  No intent attributes to match")


def main():
    parser = argparse.ArgumentParser(description='Analyze query-item attribute matching')
    parser.add_argument('--enriched', type=str,
                        default='./temp/downloaded_files/qip_scores_enriched.parquet',
                        help='Enriched QIP scores (with query intent)')
    parser.add_argument('--attributes', type=str,
                        default='./temp/downloaded_files/item_attributes_sample-5000.jsonl',
                        help='Item attributes JSONL file')
    parser.add_argument('--sample', type=int, default=100,
                        help='Number of QI pairs to analyze')
    parser.add_argument('--show-examples', type=int, default=10,
                        help='Number of detailed examples to print')

    args = parser.parse_args()

    print("="*70)
    print("Query-Item Attribute Match Analysis")
    print("="*70)

    # Load enriched QIP scores
    if not Path(args.enriched).exists():
        print(f"\n❌ Enriched file not found: {args.enriched}")
        print("\nRun these commands first:")
        print("  1. python test_gcs_download.py")
        print("  2. python enrich_qip_scores.py --sample 1000")
        return 1

    print(f"\nLoading enriched QIP scores: {args.enriched}")
    df = pd.read_parquet(args.enriched)
    print(f"  Loaded {len(df):,} rows")

    # Sample
    if args.sample:
        df = df.head(args.sample)
        print(f"  Using sample of {len(df):,} rows")

    # Load item attributes
    if not Path(args.attributes).exists():
        print(f"\n❌ Item attributes not found: {args.attributes}")
        print("Run: python test_gcs_download.py")
        return 1

    print(f"\nLoading item attributes: {args.attributes}")
    attrs_df = pd.read_json(args.attributes, lines=True)
    print(f"  Loaded {len(attrs_df):,} items")
    print(f"  Columns: {attrs_df.columns.tolist()}")

    # Join with item attributes
    print(f"\nJoining QIP scores with item attributes...")
    if 'pg_prod_id' in df.columns and 'pg_prod_id' in attrs_df.columns:
        df = df.merge(attrs_df, on='pg_prod_id', how='left', suffixes=('', '_attr'))
        print(f"  Joined on pg_prod_id")
        print(f"  Columns after merge: {[col for col in df.columns if col in ['product_type', 'brand', 'color', 'gender', 'title', 'description', 'product_name']]}")
    else:
        print(f"  ⚠️  Cannot join: missing pg_prod_id column")
        return 1

    # Check how many have item attributes
    has_product_name = df['product_name'].notna().sum()
    print(f"  Items with attributes: {has_product_name:,} / {len(df):,} ({has_product_name/len(df)*100:.1f}%)")

    # Filter to rows with item attributes
    df = df[df['product_name'].notna()].copy()
    print(f"  Analyzing {len(df):,} rows with complete data")

    # Show detailed examples
    print("\n" + "="*70)
    print(f"Detailed QI Pair Examples (showing {args.show_examples})")
    print("="*70)

    # Prioritize rows with intent annotations
    query_col = 'contextualQuery' if 'contextualQuery' in df.columns else 'query'

    # Find rows with any intent annotations (including product type)
    df['has_intent'] = (
        (df['n_brands'].fillna(0) > 0) |
        (df['n_colors'].fillna(0) > 0) |
        (df['n_genders'].fillna(0) > 0) |
        (df['n_product_types'].fillna(0) > 0)
    )

    df_with_intent = df[df['has_intent']].copy()

    if len(df_with_intent) == 0:
        print("\n⚠️  No rows with intent annotations found")
        print("The enriched data doesn't have any intent annotations.")
        print("This might happen if:")
        print("  - Sample has only 1 generic query")
        print("  - Perceive API didn't return intent data")
        print("\nTry with more diverse queries:")
        print("  python enrich_qip_scores.py --sample 1000")
        return 1

    print(f"\nFound {len(df_with_intent):,} query-item pairs with intent annotations")
    print(f"  Unique queries: {df_with_intent[query_col].nunique()}")
    print(f"  With brand intent: {(df_with_intent['n_brands'] > 0).sum()}")
    print(f"  With color intent: {(df_with_intent['n_colors'] > 0).sum()}")
    print(f"  With gender intent: {(df_with_intent['n_genders'] > 0).sum()}")
    print(f"  With product type: {(df_with_intent['n_product_types'] > 0).sum()}")

    # Show examples
    n_examples = min(args.show_examples, len(df_with_intent))

    for i in range(n_examples):
        print_qi_pair_example(df_with_intent.iloc[i], idx=i)

    # Calculate overall statistics
    print("\n" + "="*70)
    print("Overall Statistics")
    print("="*70)

    print(f"\nIntent Coverage:")
    print(f"  Queries with brand intent:   {(df['n_brands'] > 0).sum():,} ({(df['n_brands'] > 0).sum()/len(df)*100:.1f}%)")
    print(f"  Queries with color intent:   {(df['n_colors'] > 0).sum():,} ({(df['n_colors'] > 0).sum()/len(df)*100:.1f}%)")
    print(f"  Queries with gender intent:  {(df['n_genders'] > 0).sum():,} ({(df['n_genders'] > 0).sum()/len(df)*100:.1f}%)")
    print(f"  Queries with product types:  {(df['n_product_types'] > 0).sum():,} ({(df['n_product_types'] > 0).sum()/len(df)*100:.1f}%)")

    # Quick match analysis for brand
    if 'n_brands' in df.columns and (df['n_brands'] > 0).sum() > 0:
        print(f"\nBrand Matching Analysis:")

        brand_intent_rows = df[df['n_brands'] > 0].copy()

        matches = 0
        for idx, row in brand_intent_rows.iterrows():
            intent_list = parse_intent(row['brand_intent'])
            intent_values = extract_intent_values(intent_list, top_n=3)
            item_value = row.get('brand')

            result = check_attribute_match(item_value, intent_values)
            if result['match']:
                matches += 1

        match_rate = matches / len(brand_intent_rows) * 100
        print(f"  Queries with brand intent: {len(brand_intent_rows):,}")
        print(f"  Brand matches: {matches:,} ({match_rate:.1f}%)")
        print(f"  Brand mismatches: {len(brand_intent_rows) - matches:,} ({100-match_rate:.1f}%)")

        # Rating by brand match
        brand_intent_rows['brand_match'] = brand_intent_rows.apply(
            lambda row: check_attribute_match(
                row.get('brand'),
                extract_intent_values(parse_intent(row['brand_intent']), top_n=3)
            )['match'],
            axis=1
        )

        if 'label' in brand_intent_rows.columns:
            print(f"\n  Average rating by brand match:")
            rating_by_match = brand_intent_rows.groupby('brand_match')['label'].mean()
            for match, rating in rating_by_match.items():
                status = "Match" if match else "Mismatch"
                print(f"    {status:12s}: {rating:.2f}")

    print("\n" + "="*70)
    print("Analysis Complete!")
    print("="*70)
    print("\nNext steps:")
    print("  1. Review QI pair examples above")
    print("  2. Run on full dataset: python analyze_query_item_match.py --sample 10000")
    print("  3. Use insights for LLM-based impact analysis (Skill 3)")

    return 0


if __name__ == '__main__':
    exit(main())
