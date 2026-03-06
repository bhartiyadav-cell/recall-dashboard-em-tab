#!/usr/bin/env python3
"""
Check if everything is set up correctly for Skill 1: GCS Download
"""

import sys
import os

def check_imports():
    """Check if required packages are installed."""
    print("Checking dependencies...")

    missing = []

    try:
        import gcsfs
        print("  ✓ gcsfs installed")
    except ImportError:
        print("  ✗ gcsfs NOT installed")
        missing.append("gcsfs")

    try:
        import pandas
        print("  ✓ pandas installed")
    except ImportError:
        print("  ✗ pandas NOT installed")
        missing.append("pandas")

    try:
        import pyarrow
        print("  ✓ pyarrow installed")
    except ImportError:
        print("  ✗ pyarrow NOT installed")
        missing.append("pyarrow")

    if missing:
        print(f"\n⚠ Missing packages: {', '.join(missing)}")
        print("\nInstall with:")
        print("  pip install " + " ".join(missing))
        return False

    return True

def check_skill_files():
    """Check if skill files exist."""
    print("\nChecking skill files...")

    required_files = [
        'skills/__init__.py',
        'skills/gcs_download/__init__.py',
        'skills/gcs_download/config.py',
        'skills/gcs_download/main.py',
        'skills/gcs_download/skill.md',
        'test_gcs_download.py'
    ]

    all_exist = True
    for filepath in required_files:
        if os.path.exists(filepath):
            print(f"  ✓ {filepath}")
        else:
            print(f"  ✗ {filepath} MISSING")
            all_exist = False

    return all_exist

def check_gcs_auth():
    """Check if GCS authentication is set up."""
    print("\nChecking GCS authentication...")

    try:
        import gcsfs
        fs = gcsfs.GCSFileSystem()

        # Try to list a known public bucket (won't fail even without auth for public buckets)
        # For private buckets, this will check if auth works
        print("  ℹ Attempting GCS connection...")

        # Try listing the user's bucket
        try:
            files = fs.ls('p0y01cc')
            print(f"  ✓ Successfully connected to GCS (found {len(files)} items)")
            return True
        except Exception as e:
            print(f"  ⚠ Could not access GCS bucket: {e}")
            print("\n  To fix, run one of:")
            print("    1. gcloud auth application-default login")
            print("    2. export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json")
            return False

    except Exception as e:
        print(f"  ✗ Error checking GCS: {e}")
        return False

def check_skill_import():
    """Check if skill can be imported."""
    print("\nChecking skill import...")

    try:
        from skills.gcs_download import run, GCSDownloadInput, GCSDownloadOutput
        print("  ✓ Successfully imported skill")
        print(f"    - GCSDownloadInput: {GCSDownloadInput}")
        print(f"    - GCSDownloadOutput: {GCSDownloadOutput}")
        print(f"    - run function: {run}")
        return True
    except Exception as e:
        print(f"  ✗ Failed to import skill: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("="*60)
    print("Skill 1: GCS Download - Setup Check")
    print("="*60)
    print()

    checks = [
        ("Dependencies", check_imports),
        ("Skill Files", check_skill_files),
        ("Skill Import", check_skill_import),
        ("GCS Authentication", check_gcs_auth)
    ]

    results = {}
    for name, check_func in checks:
        results[name] = check_func()
        print()

    print("="*60)
    print("Summary")
    print("="*60)

    all_passed = True
    for name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}: {name}")
        if not passed:
            all_passed = False

    print()

    if all_passed:
        print("🎉 ALL CHECKS PASSED!")
        print("\nYou're ready to test the skill:")
        print("  python test_gcs_download.py")
        return 0
    else:
        print("⚠ SOME CHECKS FAILED")
        print("\nFix the issues above before running the skill.")
        return 1

if __name__ == '__main__':
    sys.exit(main())
