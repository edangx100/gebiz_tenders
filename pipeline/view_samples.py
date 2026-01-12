"""
Sample viewer utility for manual spot checks of extracted data.

Allows quick inspection of extraction quality by viewing random samples
or specific chunks from the extracted JSONL file.
"""

import argparse
import json
import random
import sys
from pathlib import Path
from typing import Any

from pipeline.config import Config


def format_entity_display(entities: dict[str, Any]) -> str:
    """Format entities for readable display."""
    if not entities:
        return "  (no entities)"

    lines = []
    for label, entity_list in entities.items():
        if not entity_list:
            continue
        lines.append(f"  {label}:")
        for entity in entity_list[:5]:  # Show max 5 per label
            if isinstance(entity, dict):
                text = entity.get("text", str(entity))
                score = entity.get("score")
                if score is not None:
                    lines.append(f"    - {text} (score={score:.3f})")
                else:
                    lines.append(f"    - {text}")
            else:
                lines.append(f"    - {entity}")
        if len(entity_list) > 5:
            lines.append(f"    ... and {len(entity_list) - 5} more")

    return "\n".join(lines) if lines else "  (empty)"


def format_relation_display(relations: dict[str, Any]) -> str:
    """Format relations for readable display."""
    if not relations:
        return "  (no relations)"

    lines = []
    for rel_type, relation_list in relations.items():
        if not relation_list:
            continue
        lines.append(f"  {rel_type}:")
        for relation in relation_list[:5]:  # Show max 5 per type
            if isinstance(relation, (list, tuple)) and len(relation) >= 2:
                source, target = relation[0], relation[1]
                lines.append(f"    - {source} -> {target}")
            else:
                lines.append(f"    - {relation}")
        if len(relation_list) > 5:
            lines.append(f"    ... and {len(relation_list) - 5} more")

    return "\n".join(lines) if lines else "  (empty)"


def format_quality_flags(quality_flags: dict[str, Any]) -> str:
    """Format quality flags for readable display."""
    if not quality_flags:
        return "  (no quality flags)"

    lines = []
    lines.append(f"  has_entities: {quality_flags.get('has_entities', 'N/A')}")
    lines.append(f"  has_relations: {quality_flags.get('has_relations', 'N/A')}")

    low_conf = quality_flags.get("low_confidence_entities", [])
    if low_conf:
        lines.append(f"  low_confidence_entities ({len(low_conf)}):")
        for item in low_conf[:3]:
            lines.append(
                f"    - {item.get('label')}: '{item.get('text')}' (score={item.get('score', 0):.3f})"
            )

    unknown = quality_flags.get("unknown_labels", [])
    if unknown:
        lines.append(f"  unknown_labels: {unknown}")

    if quality_flags.get("empty_chunk"):
        lines.append("  WARNING: empty chunk")

    return "\n".join(lines)


def display_sample(record: dict[str, Any], index: int | None = None) -> None:
    """Display a single extraction sample."""
    header = f"Sample #{index}" if index is not None else "Sample"
    print(f"\n{'=' * 80}")
    print(f"{header}")
    print(f"{'=' * 80}")
    print(f"Chunk ID: {record.get('chunk_id', 'N/A')}")
    print(f"Tender No: {record.get('tender_no', 'N/A')}")
    print(f"Agency: {record.get('agency', 'N/A')}")
    print(f"Supplier: {record.get('supplier', 'N/A')}")
    print(f"Award Date: {record.get('award_date', 'N/A')}")
    print(f"Amount: {record.get('awarded_amt', 'N/A')}")
    print(f"\nChunk Text:")
    chunk_text = record.get("chunk_text", "")
    # Truncate if too long
    if len(chunk_text) > 500:
        print(f"  {chunk_text[:500]}...")
        print(f"  (truncated, total length: {len(chunk_text)})")
    else:
        print(f"  {chunk_text}")

    print(f"\nEntities:")
    print(format_entity_display(record.get("entities", {})))

    print(f"\nRelations:")
    print(format_relation_display(record.get("relations", {})))

    print(f"\nQuality Flags:")
    print(format_quality_flags(record.get("quality_flags", {})))


def view_samples(
    extracted_path: Path,
    count: int = 3,
    chunk_ids: list[str] | None = None,
    random_seed: int | None = None,
) -> None:
    """
    View extraction samples for manual spot checking.

    Args:
        extracted_path: Path to extracted JSONL file
        count: Number of random samples to show (ignored if chunk_ids provided)
        chunk_ids: Specific chunk IDs to display
        random_seed: Random seed for reproducible sampling
    """
    if not extracted_path.exists():
        print(f"Error: Extracted file not found: {extracted_path}", file=sys.stderr)
        sys.exit(1)

    # Load all records
    records = []
    with open(extracted_path, "r") as f:
        for line_num, line in enumerate(f, start=1):
            try:
                record = json.loads(line)
                records.append(record)
            except json.JSONDecodeError as e:
                print(
                    f"Warning: Invalid JSON on line {line_num}: {e}", file=sys.stderr
                )
                continue

    if not records:
        print("No records found in extracted file.", file=sys.stderr)
        sys.exit(1)

    print(f"Loaded {len(records)} extracted records from {extracted_path}")

    # Show specific chunks or random samples
    if chunk_ids:
        print(f"\nShowing specific chunk IDs: {chunk_ids}")
        chunk_id_set = set(chunk_ids)
        matching_records = [r for r in records if r.get("chunk_id") in chunk_id_set]

        if not matching_records:
            print(f"Warning: No matching chunks found for IDs: {chunk_ids}")
            return

        for idx, record in enumerate(matching_records, start=1):
            display_sample(record, idx)

    else:
        # Random sampling
        if random_seed is not None:
            random.seed(random_seed)

        sample_count = min(count, len(records))
        samples = random.sample(records, sample_count)

        print(f"\nShowing {sample_count} random samples (seed={random_seed}):")
        for idx, record in enumerate(samples, start=1):
            display_sample(record, idx)

    print(f"\n{'=' * 80}")
    print(f"Total records: {len(records)}")


def main() -> None:
    """CLI entrypoint for sample viewer."""
    parser = argparse.ArgumentParser(
        description="View extraction samples for manual spot checks"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Input extracted JSONL file (default: data/extracted/extracted.jsonl)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=3,
        help="Number of random samples to show (default: 3)",
    )
    parser.add_argument(
        "--chunk-ids",
        type=str,
        nargs="+",
        default=None,
        help="Specific chunk IDs to display (overrides --count)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible sampling",
    )

    args = parser.parse_args()

    try:
        # Determine input path
        if args.input:
            input_path = Path(args.input)
        else:
            config = Config.load()
            input_path = config.data.extracted_dir / "extracted.jsonl"

        # View samples
        view_samples(
            input_path,
            count=args.count,
            chunk_ids=args.chunk_ids,
            random_seed=args.seed,
        )

    except Exception as e:
        print(f"Sample viewer failed: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
