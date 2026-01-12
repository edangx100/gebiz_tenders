"""
Chunk builder for GeBIZ tender records.

Converts normalized records into compact "tender card" chunks with stable IDs.
"""

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Optional

from pipeline.config import Config
from pipeline.fetch import fetch_gebiz_data


def generate_chunk_id(record: dict[str, Any]) -> str:
    """
    Generate a stable chunk ID from a record.

    Uses tender_no + award_date as the primary key. If either is missing,
    falls back to a hash of the full record for stability.

    Args:
        record: Normalized GeBIZ record

    Returns:
        Stable chunk ID string
    """
    tender_no = record.get("tender_no", "").strip()
    award_date = record.get("award_date", "").strip()

    if tender_no and award_date:
        # Primary key: tender_no + award_date
        return f"{tender_no}_{award_date}"

    # Fallback: hash of the record for stability
    record_str = json.dumps(record, sort_keys=True)
    hash_digest = hashlib.sha256(record_str.encode()).hexdigest()
    return f"chunk_{hash_digest[:16]}"


def build_chunk_text(record: dict[str, Any]) -> str:
    """
    Build a compact "tender card" text from a normalized record.

    Args:
        record: Normalized GeBIZ record

    Returns:
        Compact tender card text
    """
    lines = []

    # Tender header
    tender_no = record.get("tender_no", "N/A")
    lines.append(f"Tender: {tender_no}")

    # Agency
    agency = record.get("agency", "N/A")
    lines.append(f"Agency: {agency}")

    # Award details
    award_date = record.get("award_date", "N/A")
    supplier = record.get("supplier", "N/A")
    awarded_amt = record.get("awarded_amt", "N/A")
    lines.append(f"Award Date: {award_date}")
    lines.append(f"Awarded To: {supplier}")
    lines.append(f"Amount: {awarded_amt}")

    # Category and description
    category = record.get("category", "N/A")
    lines.append(f"Category: {category}")

    tender_description = record.get("tender_description", "")
    if tender_description and tender_description != category:
        lines.append(f"Description: {tender_description}")

    # Status
    status = record.get("tender_detail_status", "")
    if status:
        lines.append(f"Status: {status}")

    return "\n".join(lines)


def build_chunks(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Convert a list of normalized records into chunks.

    Each chunk contains:
    - chunk_id: stable identifier
    - chunk_text: compact tender card
    - source fields: tender_no, award_date, agency, supplier, awarded_amt, category, etc.

    Args:
        records: List of normalized GeBIZ records

    Returns:
        List of chunk dictionaries
    """
    chunks = []

    for record in records:
        chunk_id = generate_chunk_id(record)
        chunk_text = build_chunk_text(record)

        chunk = {
            "chunk_id": chunk_id,
            "chunk_text": chunk_text,
            # Preserve source fields for traceability
            "tender_no": record.get("tender_no", ""),
            "agency": record.get("agency", ""),
            "award_date": record.get("award_date", ""),
            "supplier": record.get("supplier", ""),
            "awarded_amt": record.get("awarded_amt", ""),
            "category": record.get("category", ""),
            "tender_description": record.get("tender_description", ""),
            "tender_detail_status": record.get("tender_detail_status", ""),
            "_id": record.get("_id"),
        }

        chunks.append(chunk)

    return chunks


def write_chunks_to_jsonl(chunks: list[dict[str, Any]], output_path: Path) -> None:
    """
    Write chunks to a JSONL file.

    Args:
        chunks: List of chunk dictionaries
        output_path: Path to output JSONL file
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        for chunk in chunks:
            json_line = json.dumps(chunk, ensure_ascii=False)
            f.write(json_line + "\n")

    print(f"Wrote {len(chunks)} chunks to {output_path}")


def main() -> None:
    """CLI entrypoint for chunk builder."""
    parser = argparse.ArgumentParser(description="Build tender chunks from GeBIZ data")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of records to process",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output JSONL file path (default: data/chunks/chunks.jsonl)",
    )

    args = parser.parse_args()

    try:
        # Fetch normalized records
        print(f"Fetching GeBIZ data (limit={args.limit})...")
        records = fetch_gebiz_data(limit=args.limit)

        # Build chunks
        print(f"Building chunks from {len(records)} records...")
        chunks = build_chunks(records)

        # Determine output path
        if args.output:
            output_path = Path(args.output)
        else:
            config = Config.load()
            output_path = config.data.chunks_dir / "chunks.jsonl"

        # Write to JSONL
        write_chunks_to_jsonl(chunks, output_path)

        # Show sample
        if chunks:
            print(f"\nSample chunk:")
            print(f"  chunk_id: {chunks[0]['chunk_id']}")
            print(f"  chunk_text preview:\n{chunks[0]['chunk_text'][:200]}...")

        print(f"\nSuccessfully processed {len(chunks)} chunks")

    except Exception as e:
        print(f"Failed to build chunks: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
