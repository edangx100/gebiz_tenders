"""
Data fetcher for GeBIZ tender records.

Downloads records from data.gov.sg and caches them locally.
"""

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, Optional

import requests

from pipeline.config import Config


def normalize_record(raw_record: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize a raw GeBIZ record to extract required fields.

    Args:
        raw_record: Raw record from data.gov.sg API

    Returns:
        Normalized record with standardized fields
    """
    return {
        "tender_no": raw_record.get("tender_no", "").strip(),
        "agency": raw_record.get("agency", "").strip(),
        "award_date": raw_record.get("award_date", "").strip(),
        "supplier": raw_record.get("supplier_name", "").strip(),
        "awarded_amt": raw_record.get("awarded_amt", "").strip(),
        "category": raw_record.get("tender_description", "").strip(),
        "tender_description": raw_record.get("tender_description", "").strip(),
        "tender_detail_status": raw_record.get("tender_detail_status", "").strip(),
        "_id": raw_record.get("_id"),
    }


def fetch_gebiz_data(
    limit: Optional[int] = None, force_refetch: bool = False, normalize: bool = True
) -> list[dict[str, Any]]:
    """
    Fetch GeBIZ tender records from CSV or data.gov.sg API.

    Args:
        limit: Maximum number of records to fetch (None for all)
        force_refetch: If True, ignore cache and reload from source
        normalize: If True, normalize records to standardized fields

    Returns:
        List of GeBIZ records (normalized or raw)
    """
    config = Config.load()
    cache_file = config.data.raw_data_dir / "gebiz_raw.json"
    csv_file = config.data.raw_csv_path

    # Check cache
    if not force_refetch and cache_file.exists():
        print(f"Loading cached data from {cache_file}")
        with open(cache_file, "r") as f:
            data: list[dict[str, Any]] = json.load(f)
        if limit:
            data = data[:limit]
        if normalize:
            return [normalize_record(record) for record in data]
        return data

    # Load from CSV if available
    if csv_file.exists():
        print(f"Loading data from {csv_file}")
        records: list[dict[str, Any]] = []
        with open(csv_file, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append(row)
                if limit and len(records) >= limit:
                    break

        print(f"Caching {len(records)} records to {cache_file}")
        with open(cache_file, "w") as f:
            json.dump(records, f, indent=2)

        if normalize:
            return [normalize_record(record) for record in records]
        return records

    # Fetch from API
    print(f"Fetching data from {config.data.source_url}")
    all_records = []
    offset = 0

    while True:
        params: dict[str, str | int] = {
            "resource_id": config.data.resource_id,
            "limit": config.data.batch_size,
            "offset": offset,
        }

        try:
            response = requests.get(config.data.source_url, params=params, timeout=30)
            response.raise_for_status()
            result = response.json()

            if not result.get("success"):
                raise ValueError(f"API returned error: {result}")

            records = result.get("result", {}).get("records", [])
            if not records:
                break

            all_records.extend(records)
            print(f"Fetched {len(all_records)} records so far...")

            if limit and len(all_records) >= limit:
                all_records = all_records[:limit]
                break

            offset += config.data.batch_size

        except Exception as e:
            print(f"Error fetching data: {e}", file=sys.stderr)
            raise

    # Cache the data
    print(f"Caching {len(all_records)} records to {cache_file}")
    with open(cache_file, "w") as f:
        json.dump(all_records, f, indent=2)

    if normalize:
        return [normalize_record(record) for record in all_records]
    return all_records


def main() -> None:
    """CLI entrypoint for data fetcher."""
    parser = argparse.ArgumentParser(description="Fetch GeBIZ tender data")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of records to fetch",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force reload from source even if cache exists",
    )

    args = parser.parse_args()

    try:
        records = fetch_gebiz_data(limit=args.limit, force_refetch=args.force)
        print(f"\nSuccessfully fetched {len(records)} normalized records")
        if records:
            print(f"Sample record keys: {list(records[0].keys())}")
            print(f"\nFirst record:")
            for key, value in records[0].items():
                print(f"  {key}: {value}")
    except Exception as e:
        print(f"Failed to fetch data: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
