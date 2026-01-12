"""
Apply normalization to extracted entity data.

This module reads extracted.jsonl, applies text normalization to keywords
and requirements, and writes normalized output.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from pipeline.config import Config
from pipeline.normalize import normalize_entities_in_extraction


def apply_normalization(
    input_path: Path,
    output_path: Path,
    limit: int | None = None,
    max_keywords: int = 10,
    max_requirements: int = 10
) -> dict[str, Any]:
    """
    Apply normalization to extracted entities.

    Reads extracted.jsonl, normalizes keywords/requirements, and writes
    to a new file.

    Args:
        input_path: Path to extracted.jsonl
        output_path: Path to write normalized output
        limit: Optional limit on number of records to process
        max_keywords: Maximum keywords per tender (default: 10)
        max_requirements: Maximum requirements per tender (default: 10)

    Returns:
        Statistics dict with counts
    """
    stats = {
        "total_processed": 0,
        "keywords_before": 0,
        "keywords_after": 0,
        "requirements_before": 0,
        "requirements_after": 0,
        "money_normalized": 0,
        "dates_normalized": 0
    }

    with open(input_path) as infile, open(output_path, "w") as outfile:
        for i, line in enumerate(infile):
            if limit and i >= limit:
                break

            extraction = json.loads(line)

            # Count before normalization
            stats["keywords_before"] += len(extraction.get("entities", {}).get("Keyword", []))
            stats["requirements_before"] += len(extraction.get("entities", {}).get("Requirement", []))

            # Apply normalization with bloat controls
            normalized = normalize_entities_in_extraction(
                extraction,
                max_keywords=max_keywords,
                max_requirements=max_requirements
            )

            # Count after normalization
            stats["keywords_after"] += len(normalized.get("entities", {}).get("Keyword", []))
            stats["requirements_after"] += len(normalized.get("entities", {}).get("Requirement", []))

            # Count normalized fields
            if "awarded_amt_normalized" in normalized:
                stats["money_normalized"] += 1
            if "award_date_normalized" in normalized:
                stats["dates_normalized"] += 1

            # Write normalized output
            outfile.write(json.dumps(normalized) + "\n")
            stats["total_processed"] += 1

            # Progress feedback
            if stats["total_processed"] % 100 == 0:
                print(f"Processed {stats['total_processed']} records...", file=sys.stderr)

    return stats


def main() -> None:
    """CLI entry point for normalization."""
    parser = argparse.ArgumentParser(description="Apply normalization to extracted entities")
    parser.add_argument("--limit", type=int, help="Limit number of records to process")
    parser.add_argument("--input", type=str, help="Input extracted.jsonl path (default: from config)")
    parser.add_argument("--output", type=str, help="Output path (default: extracted_normalized.jsonl)")
    parser.add_argument("--max-keywords", type=int, default=10,
                        help="Maximum keywords per tender (default: 10)")
    parser.add_argument("--max-requirements", type=int, default=10,
                        help="Maximum requirements per tender (default: 10)")
    args = parser.parse_args()

    # Load config
    config = Config.load()

    # Determine paths
    if args.input:
        input_path = Path(args.input)
    else:
        input_path = config.data.extracted_dir / "extracted.jsonl"

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = config.data.extracted_dir / "extracted_normalized.jsonl"

    # Check input exists
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    # Apply normalization
    print(f"Normalizing {input_path} → {output_path}")
    if args.limit:
        print(f"Limit: {args.limit} records")
    print(f"Bloat controls: max_keywords={args.max_keywords}, max_requirements={args.max_requirements}")

    stats = apply_normalization(
        input_path,
        output_path,
        limit=args.limit,
        max_keywords=args.max_keywords,
        max_requirements=args.max_requirements
    )

    # Print statistics
    print("\n=== Normalization Statistics ===")
    print(f"Total processed: {stats['total_processed']}")
    print(f"Keywords: {stats['keywords_before']} → {stats['keywords_after']} "
          f"({stats['keywords_before'] - stats['keywords_after']} removed)")
    print(f"Requirements: {stats['requirements_before']} → {stats['requirements_after']} "
          f"({stats['requirements_before'] - stats['requirements_after']} removed)")
    print(f"Money fields normalized: {stats['money_normalized']}")
    print(f"Date fields normalized: {stats['dates_normalized']}")


if __name__ == "__main__":
    main()
