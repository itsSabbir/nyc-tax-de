# =============================================================================
# Download Script: Fetch NYC Yellow Taxi Parquet files from the TLC website
# =============================================================================
# The NYC Taxi & Limousine Commission (TLC) publishes free trip data monthly
# at https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
#
# This script downloads all 12 months of 2024 yellow taxi data as parquet
# files into the data/raw/ folder. Each file is ~45-55 MB.
#
# TOTAL DOWNLOAD: ~500-600 MB (12 months)
#
# HOW TO RUN:
#   python scripts/download_data.py                    (downloads all 12 months)
#   python scripts/download_data.py --months 1 2 3     (downloads only Jan-Mar)
#   python scripts/download_data.py --year 2023        (downloads a different year)
#
# The script is idempotent — it skips files that already exist locally.
# To re-download a file, delete it from data/raw/ first.
# =============================================================================

import argparse
import os
import sys
from pathlib import Path
from urllib.request import urlretrieve
from urllib.error import HTTPError, URLError

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
# The TLC hosts parquet files on a CloudFront CDN. The URL pattern is:
#   https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet
#
# This pattern has been stable for years, but if it ever changes, update
# BASE_URL and the filename template below.
# ---------------------------------------------------------------------------
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Where to save downloaded files (relative to the project root)
# This matches the path used by the ingest script and .gitignore
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def download_month(year: int, month: int) -> bool:
    """
    Download a single month's parquet file.

    Returns True if the file was downloaded (or already exists), False if
    the download failed. We use urllib instead of requests to avoid adding
    a dependency — this script should work with just the Python standard library.
    """
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"{BASE_URL}/{filename}"
    dest = DATA_DIR / filename

    # Skip if already downloaded (idempotent behavior)
    if dest.exists():
        size_mb = dest.stat().st_size / (1024 * 1024)
        print(f"  SKIP  {filename} (already exists, {size_mb:.1f} MB)")
        return True

    print(f"  GET   {filename} ... ", end="", flush=True)

    try:
        urlretrieve(url, dest)
        size_mb = dest.stat().st_size / (1024 * 1024)
        print(f"OK ({size_mb:.1f} MB)")
        return True
    except HTTPError as e:
        print(f"FAILED (HTTP {e.code})")
        # Month might not be published yet (TLC has a ~2 month lag)
        if e.code == 404:
            print(f"         ^ This month may not be published yet by TLC")
        return False
    except URLError as e:
        print(f"FAILED ({e.reason})")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Download NYC Yellow Taxi parquet files from the TLC website."
    )
    parser.add_argument(
        "--year", type=int, default=2024,
        help="Year to download (default: 2024)"
    )
    parser.add_argument(
        "--months", type=int, nargs="+", default=list(range(1, 13)),
        help="Month numbers to download (default: 1-12). Example: --months 1 2 3"
    )
    args = parser.parse_args()

    # Create the data directory if it doesn't exist
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Downloading {len(args.months)} month(s) of {args.year} yellow taxi data")
    print(f"Destination: {DATA_DIR}")
    print()

    # Track results for a summary at the end
    results = {"downloaded": 0, "skipped": 0, "failed": 0}

    for month in sorted(args.months):
        if month < 1 or month > 12:
            print(f"  SKIP  month {month} (invalid, must be 1-12)")
            continue

        dest = DATA_DIR / f"yellow_tripdata_{args.year}-{month:02d}.parquet"
        already_exists = dest.exists()

        success = download_month(args.year, month)

        if success and already_exists:
            results["skipped"] += 1
        elif success:
            results["downloaded"] += 1
        else:
            results["failed"] += 1

    # Print summary
    print()
    print(f"Done: {results['downloaded']} downloaded, "
          f"{results['skipped']} skipped (already exist), "
          f"{results['failed']} failed")

    if results["failed"] > 0:
        print("\nSome downloads failed. Recent months may not be published yet.")
        print("TLC typically publishes data with a ~2 month delay.")
        sys.exit(1)


if __name__ == "__main__":
    main()
