#!/usr/bin/env python3
"""
Check what files were downloaded and suggest correct paths.
"""

import os
from pathlib import Path

def check_downloads():
    """Check what files are in the download directory."""

    download_dir = './temp/downloaded_files'

    print("="*60)
    print("Checking Downloaded Files")
    print("="*60)

    if not os.path.exists(download_dir):
        print(f"\n❌ Download directory doesn't exist: {download_dir}")
        print("\nRun this first:")
        print("  python test_gcs_download.py")
        return

    # List all files recursively
    all_files = []
    for root, dirs, files in os.walk(download_dir):
        for file in files:
            filepath = os.path.join(root, file)
            size = os.path.getsize(filepath)
            all_files.append((filepath, size))

    if not all_files:
        print(f"\n⚠ No files found in: {download_dir}")
        print("\nRun this first:")
        print("  python test_gcs_download.py")
        return

    print(f"\nFound {len(all_files)} file(s):")
    print()

    # Categorize files
    qip_scores = []
    item_attrs = []
    configs = []
    others = []

    for filepath, size in all_files:
        rel_path = os.path.relpath(filepath, download_dir)
        size_mb = size / (1024 * 1024)

        print(f"  📄 {rel_path}")
        print(f"     Size: {size_mb:.2f} MB")

        filename = os.path.basename(filepath).lower()

        if filename.endswith('.parquet'):
            if 'qip' in filename or 'output' in filename:
                qip_scores.append(filepath)
                print(f"     Type: QIP Scores ✓")
            else:
                others.append((filepath, 'Parquet'))
                print(f"     Type: Parquet file")

        elif filename.endswith('.jsonl'):
            if 'item' in filename or 'attribute' in filename:
                item_attrs.append(filepath)
                print(f"     Type: Item Attributes ✓")
            else:
                others.append((filepath, 'JSONL'))
                print(f"     Type: JSONL file")

        elif filename.endswith('.json'):
            if 'config' in filename or 'preso' in filename:
                configs.append(filepath)
                print(f"     Type: Config ✓")
            else:
                others.append((filepath, 'JSON'))
                print(f"     Type: JSON file")

        elif filename.endswith('.csv'):
            if 'metadata' in filename:
                item_attrs.append(filepath)
                print(f"     Type: Metadata ✓")
            else:
                others.append((filepath, 'CSV'))
                print(f"     Type: CSV file")
        else:
            others.append((filepath, 'Other'))
            print(f"     Type: Unknown")

        print()

    # Summary
    print("="*60)
    print("Summary")
    print("="*60)

    if qip_scores:
        print(f"\n✓ QIP Scores found:")
        for path in qip_scores:
            print(f"  {path}")
    else:
        print(f"\n❌ No QIP scores file found")
        print("  Looking for: *qip*.parquet or output*.parquet")

    if item_attrs:
        print(f"\n✓ Item attributes found:")
        for path in item_attrs:
            print(f"  {path}")
    else:
        print(f"\n⚠ No item attributes file found (optional)")
        print("  Looking for: item_attributes*.jsonl or *metadata*.csv")

    if configs:
        print(f"\n✓ Config file found:")
        for path in configs:
            print(f"  {path}")
    else:
        print(f"\n⚠ No config file found (optional)")
        print("  Will extract from QIP scores instead")

    # Suggest correct paths for load_data_example.py
    print("\n" + "="*60)
    print("Suggested Paths for load_data_example.py")
    print("="*60)

    if qip_scores:
        print(f"\nqip_scores_path = '{qip_scores[0]}'")

    if item_attrs:
        print(f"item_attrs_path = '{item_attrs[0]}'")

    if qip_scores or item_attrs:
        print("\nUpdate load_data_example.py with these paths and run:")
        print("  python load_data_example.py")

if __name__ == '__main__':
    check_downloads()
