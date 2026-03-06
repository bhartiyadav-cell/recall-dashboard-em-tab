#!/usr/bin/env python3
"""
Test script for Query Context Enrichment skill.

This demonstrates extraction of structured query intent annotations:
- Product types, brands, colors, gender with scores
- Used for item-query attribute matching analysis

Usage:
    python test_query_context.py
"""

import pandas as pd
import json
from skills.query_context import run, QueryContextInput


def pretty_print_intent(intent_json_str: str, max_items: int = 5) -> str:
    """Pretty print intent JSON."""
    try:
        intent_list = json.loads(intent_json_str)
        if not intent_list:
            return "None"

        items = []
        for item in intent_list[:max_items]:
            value = item.get("value", "")
            score = item.get("score", 0)
            items.append(f"{value} ({score:.2f})")

        result = ", ".join(items)
        if len(intent_list) > max_items:
            result += f" ... (+{len(intent_list) - max_items} more)"

        return result
    except:
        return "Parse error"


def test_with_query_list():
    """Test with a simple list of queries."""
    print("="*60)
    print("Test 1: Enrich Query List with Intent Annotations")
    print("="*60)

    # Sample queries with different attributes
    queries = [
        "red nike shoes for women",
        "black leather sofa",
        "iphone 13 pro max",
        "organic whole milk",
        "baby stroller"
    ]

    print(f"\nEnriching {len(queries)} queries...")

    # Create input config
    input_config = QueryContextInput(
        queries=queries,
        concurrency_limit=10,
        retry_limit=3,
        timeout_seconds=5
    )

    # Run the skill
    result = run(input_config)

    # Display results
    print(f"\n✓ Successfully processed: {result.queries_processed}")
    print(f"✗ Failed: {result.queries_failed}")

    print("\n" + "="*60)
    print("Extracted Query Intent Annotations")
    print("="*60)

    for idx, row in result.enriched_df.iterrows():
        print(f"\nQuery: {row['query']}")
        print(f"  Specificity: {row.get('specificity', 'N/A')}")
        print(f"  Segment: {row.get('segment', 'N/A')}")
        print(f"  Vertical: {row.get('vertical', 'N/A')}")
        print(f"\n  Intent Annotations:")

        # Product types
        pt = pretty_print_intent(row.get('product_type_intent', '[]'))
        if pt != "None":
            print(f"    Product Type: {pt}")

        # Brands
        brand = pretty_print_intent(row.get('brand_intent', '[]'))
        if brand != "None":
            print(f"    Brand:        {brand}")

        # Colors
        color = pretty_print_intent(row.get('color_intent', '[]'))
        if color != "None":
            print(f"    Color:        {color}")

        # Gender
        gender = pretty_print_intent(row.get('gender_intent', '[]'))
        if gender != "None":
            print(f"    Gender:       {gender}")

        # Categories
        category = pretty_print_intent(row.get('category_intent', '[]'))
        if category != "None":
            print(f"    Category:     {category}")

    return result


def test_with_dataframe():
    """Test with DataFrame from downloaded QIP scores."""
    print("\n" + "="*60)
    print("Test 2: Enrich QIP Scores DataFrame")
    print("="*60)

    # Load QIP scores
    qip_scores_path = './temp/downloaded_files/qip_scores.parquet'

    try:
        df = pd.read_parquet(qip_scores_path)
        print(f"\nLoaded {len(df):,} rows from QIP scores")

        # Take a sample for testing
        sample_size = 50
        df_sample = df.head(sample_size)
        print(f"Using sample of {len(df_sample)} rows")

        # Create input config
        input_config = QueryContextInput(
            queries=df_sample,
            concurrency_limit=25,
            retry_limit=3,
            timeout_seconds=5
        )

        # Run the skill
        result = run(input_config)

        # Display results
        print(f"\n✓ Successfully processed: {result.queries_processed} queries")
        print(f"✗ Failed: {result.queries_failed} queries")

        print("\n" + "="*60)
        print("Intent Annotation Statistics")
        print("="*60)

        # Count queries with each intent type
        intent_cols = {
            "Product Type": "n_product_types",
            "Brand": "n_brands",
            "Color": "n_colors",
            "Gender": "n_genders",
            "Category": "n_categories"
        }

        for label, col in intent_cols.items():
            if col in result.enriched_df.columns:
                has_intent = (result.enriched_df[col] > 0).sum()
                pct = has_intent / len(result.enriched_df) * 100
                print(f"  {label:15s}: {has_intent:3d} / {len(result.enriched_df)} queries ({pct:.1f}%)")

        # Show sample enriched data
        print("\n" + "="*60)
        print("Sample Enriched Data")
        print("="*60)

        query_col = 'contextualQuery' if 'contextualQuery' in result.enriched_df.columns else 'query'

        sample_rows = result.enriched_df[result.enriched_df['n_product_types'] > 0].head(5)

        for idx, row in sample_rows.iterrows():
            print(f"\nQuery: {row[query_col]}")
            print(f"  Rating: {row.get('label', 'N/A')}")
            print(f"  Engine: {row.get('engine', 'N/A')}")

            # Show top intent
            if row['n_product_types'] > 0:
                pt = pretty_print_intent(row['product_type_intent'], max_items=3)
                print(f"  Product Types: {pt}")

            if row['n_brands'] > 0:
                brand = pretty_print_intent(row['brand_intent'], max_items=2)
                print(f"  Brands: {brand}")

            if row['n_colors'] > 0:
                color = pretty_print_intent(row['color_intent'], max_items=2)
                print(f"  Colors: {color}")

        # Save enriched data
        output_path = './temp/downloaded_files/qip_scores_with_context_sample.parquet'
        result.enriched_df.to_parquet(output_path, index=False)
        print(f"\n✓ Saved enriched data to: {output_path}")

        return result

    except FileNotFoundError:
        print(f"\n⚠ QIP scores not found at: {qip_scores_path}")
        print("Run test_gcs_download.py first to download files!")
        return None


def main():
    """Run both tests."""
    # Test 1: Simple query list with rich annotations
    result1 = test_with_query_list()

    # Test 2: DataFrame enrichment
    result2 = test_with_dataframe()

    print("\n" + "="*60)
    print("Testing Complete!")
    print("="*60)

    if result1:
        print(f"\n✓ Test 1: Enriched {result1.queries_processed} queries with intent annotations")

    if result2:
        print(f"✓ Test 2: Enriched {result2.queries_processed} queries from DataFrame")
        print("\nNext steps:")
        print("1. Run full enrichment: python enrich_qip_scores.py")
        print("2. Analyze attribute matching: python analyze_query_item_match.py")
        print("3. Use insights to understand L1 ranker impact patterns")

    return 0


if __name__ == '__main__':
    exit(main())
