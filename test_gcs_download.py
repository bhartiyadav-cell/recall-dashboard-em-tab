#!/usr/bin/env python3
"""
Test script for GCS Download skill.

Usage:
    python test_gcs_download.py
"""

from skills.gcs_download import run, GCSDownloadInput

def main():
    # Test with your actual GCS path
    gs_path = "gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383"

    print("Testing GCS Download Skill")
    print(f"Source: {gs_path}")
    print("Will search recursively through subdirectories (sample-5000, sample-1000, etc.)")
    print()

    try:
        # Create input config with recursive search enabled
        input_config = GCSDownloadInput(
            gs_path=gs_path,
            local_dir='./temp/downloaded_files',
            recursive=True  # Search subdirectories
        )

        # Run the skill
        result = run(input_config)

        # Print results
        print("\n" + "="*60)
        print("DOWNLOAD SUCCESSFUL!")
        print("="*60)
        print(f"\n📦 Downloaded Files:")
        print(f"  • QIP Scores:  {result.qip_scores_path}")
        print(f"  • Config:      {result.config_path or 'Not found'}")
        print(f"  • Metadata:    {result.metadata_path or 'Not found'}")
        print(f"\n📁 Location:     {result.download_dir}")
        print(f"📊 Total files:  {len(result.all_files)}")
        print()

        return 0

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    exit(main())
