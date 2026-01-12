"""
GLiNER2-based entity and relationship extraction for GeBIZ tender chunks.

Applies schema-driven extraction to each chunk and emits JSONL with entities,
relations, and source traceability.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

try:
    from gliner2 import GLiNER2
except ImportError:
    GLiNER2 = None

from pipeline.config import Config
from pipeline.schema import get_schema
from pipeline.normalize import normalize_entities_in_extraction


# Entity descriptions for GLiNER2
ENTITY_DESCRIPTIONS = {
    "Tender": "Government tender reference numbers, procurement IDs, or contract identifiers",
    "Agency": "Government agencies, ministries, statutory boards, or public sector organizations",
    "Supplier": "Companies, vendors, contractors, or service providers awarded tenders",
    "Category": "Product categories, service types, or procurement classifications",
    "Requirement": "Technical requirements, specifications, qualifications, or conditions mentioned in tenders",
    "Keyword": "Key terms, technologies, or important concepts related to the tender",
    "Date": "Dates, deadlines, or time periods mentioned in the tender",
}


# Relationship descriptions for GLiNER2
RELATION_DESCRIPTIONS = {
    "PUBLISHED_BY": "A tender is published or issued by a government agency",
    "AWARDED_TO": "A tender is awarded to a supplier or contractor",
    "IN_CATEGORY": "A tender belongs to a specific category or procurement type",
    "HAS_REQUIREMENT": "A tender specifies or mentions a requirement",
    "HAS_KEYWORD": "A tender is associated with or mentions a key term or technology",
    "HAS_DEADLINE": "A tender has a deadline, closing date, or time period",
}


def load_model(model_name: str = "fastino/gliner2-large-v1") -> Any:
    """
    Load the GLiNER2 model.

    Args:
        model_name: Hugging Face model identifier

    Returns:
        GLiNER2 model instance

    Raises:
        ImportError: If gliner2 is not installed
        RuntimeError: If model loading fails
    """
    if GLiNER2 is None:
        raise ImportError(
            "gliner2 is not installed. Install with: pip install gliner2"
        )

    print(f"Loading GLiNER2 model: {model_name}...")
    try:
        model = GLiNER2.from_pretrained(model_name)
        print(f"Model loaded successfully")
        return model
    except Exception as e:
        raise RuntimeError(f"Failed to load GLiNER2 model: {e}")


def create_structured_relationships(
    chunk_data: dict[str, Any],
) -> dict[str, list[list[str]]]:
    """
    Create reliable relationships from structured source fields.

    Uses deterministic mapping from source data for core relationships:
    - PUBLISHED_BY: tender_no -> agency
    - AWARDED_TO: tender_no -> supplier
    - IN_CATEGORY: tender_no -> category

    Args:
        chunk_data: Chunk dictionary with source fields

    Returns:
        Dictionary mapping relation types to [source, target] pairs
    """
    relations: dict[str, list[list[str]]] = {
        "PUBLISHED_BY": [],
        "AWARDED_TO": [],
        "IN_CATEGORY": [],
    }

    tender_no = chunk_data.get("tender_no", "").strip()
    agency = chunk_data.get("agency", "").strip()
    supplier = chunk_data.get("supplier", "").strip()
    category = chunk_data.get("category", "").strip()

    # PUBLISHED_BY: tender -> agency
    if tender_no and agency:
        relations["PUBLISHED_BY"].append([tender_no, agency])

    # AWARDED_TO: tender -> supplier
    if tender_no and supplier:
        relations["AWARDED_TO"].append([tender_no, supplier])

    # IN_CATEGORY: tender -> category
    if tender_no and category:
        relations["IN_CATEGORY"].append([tender_no, category])

    return relations


def extract_from_chunk(
    model: Any,
    chunk_text: str,
    entity_descriptions: dict[str, str],
    relation_descriptions: dict[str, str],
    confidence_threshold: float = 0.5,
) -> dict[str, Any]:
    """
    Extract entities and relations from a single chunk using GLiNER2.

    Args:
        model: GLiNER2 model instance
        chunk_text: Tender chunk text to extract from
        entity_descriptions: Entity type descriptions for GLiNER2
        relation_descriptions: Relation type descriptions for GLiNER2
        confidence_threshold: Minimum confidence score for logging warnings (default: 0.5)

    Returns:
        Dictionary with 'entities', 'relations', and 'quality_flags' keys
    """
    # Create schema for combined extraction
    schema = (
        model.create_schema()
        .entities(entity_descriptions)
        .relations(relation_descriptions)
    )

    # Extract entities and relations in a single pass
    results = model.extract(chunk_text, schema)

    # Format output
    entities = results.get("entities", {})
    relations = results.get("relation_extraction", {})

    # Quality checks
    quality_flags: dict[str, Any] = {
        "has_entities": bool(entities and any(entities.values())),
        "has_relations": bool(relations and any(relations.values())),
        "low_confidence_entities": [],
        "unknown_labels": [],
    }

    # Check for low-confidence or problematic entities
    for label, entity_list in entities.items():
        if not entity_list:
            continue
        for entity in entity_list:
            # GLiNER2 entities may have confidence scores
            if isinstance(entity, dict) and "score" in entity:
                score = entity.get("score", 1.0)
                if score < confidence_threshold:
                    quality_flags["low_confidence_entities"].append({
                        "label": label,
                        "text": entity.get("text", ""),
                        "score": score,
                    })
            # Check for unknown labels
            if label not in entity_descriptions:
                if label not in quality_flags["unknown_labels"]:
                    quality_flags["unknown_labels"].append(label)

    extracted = {
        "entities": entities,
        "relations": relations,
        "quality_flags": quality_flags,
    }

    return extracted


def extract_from_chunks(
    chunks_path: Path, output_path: Path, model_name: str = "fastino/gliner2-large-v1"
) -> int:
    """
    Extract entities and relations from all chunks and write to JSONL.

    Args:
        chunks_path: Path to chunks JSONL file
        output_path: Path to output extracted JSONL file
        model_name: GLiNER2 model name

    Returns:
        Number of chunks processed
    """
    # Load model once
    model = load_model(model_name)

    # Load schema for validation
    schema = get_schema()

    # Prepare output
    output_path.parent.mkdir(parents=True, exist_ok=True)

    processed_count = 0

    with open(chunks_path, "r") as infile, open(output_path, "w") as outfile:
        for line_num, line in enumerate(infile, start=1):
            try:
                chunk = json.loads(line)
                chunk_id = chunk.get("chunk_id", f"unknown_{line_num}")
                chunk_text = chunk.get("chunk_text", "")

                if not chunk_text:
                    print(f"Warning: Empty chunk_text for chunk_id={chunk_id}", file=sys.stderr)
                    # Write chunk with empty extraction
                    output_record = {
                        "chunk_id": chunk_id,
                        "chunk_text": chunk_text,
                        "entities": {},
                        "relations": {},
                        "quality_flags": {
                            "has_entities": False,
                            "has_relations": False,
                            "low_confidence_entities": [],
                            "unknown_labels": [],
                            "empty_chunk": True,
                        },
                        # Preserve source fields
                        "tender_no": chunk.get("tender_no", ""),
                        "agency": chunk.get("agency", ""),
                        "award_date": chunk.get("award_date", ""),
                        "supplier": chunk.get("supplier", ""),
                        "awarded_amt": chunk.get("awarded_amt", ""),
                        "category": chunk.get("category", ""),
                        "tender_description": chunk.get("tender_description", ""),
                        "tender_detail_status": chunk.get("tender_detail_status", ""),
                        "_id": chunk.get("_id"),
                    }
                    json_line = json.dumps(output_record, ensure_ascii=False)
                    outfile.write(json_line + "\n")
                    processed_count += 1
                    continue

                # Extract entities and relations using GLiNER2
                extracted = extract_from_chunk(
                    model, chunk_text, ENTITY_DESCRIPTIONS, RELATION_DESCRIPTIONS
                )

                # Ensure core entities exist from structured data
                # These are needed for structured relationships to work
                entities = extracted["entities"]
                tender_no = chunk.get("tender_no", "").strip()
                agency = chunk.get("agency", "").strip()
                supplier = chunk.get("supplier", "").strip()
                category = chunk.get("category", "").strip()

                # Add entities from structured data if they don't exist
                if tender_no and tender_no not in entities.get("Tender", []):
                    if "Tender" not in entities:
                        entities["Tender"] = []
                    entities["Tender"].append(tender_no)

                if agency and agency not in entities.get("Agency", []):
                    if "Agency" not in entities:
                        entities["Agency"] = []
                    entities["Agency"].append(agency)

                if supplier and supplier not in entities.get("Supplier", []):
                    if "Supplier" not in entities:
                        entities["Supplier"] = []
                    entities["Supplier"].append(supplier)

                if category and category not in entities.get("Category", []):
                    if "Category" not in entities:
                        entities["Category"] = []
                    entities["Category"].append(category)

                # Create structured relationships from source fields (reliable)
                structured_relations = create_structured_relationships(chunk)

                # Merge relationships: structured (core) + GLiNER2 (enrichment only)
                # Keep only enrichment relationships from GLiNER2
                gliner_relations = extracted["relations"]
                # Normalize HAS_REQUIREMENT to match schema (Tender -> Requirement).
                if gliner_relations.get("HAS_REQUIREMENT"):
                    tender_names_req = set(entities.get("Tender", []))
                    # If GLiNER links a non-Tender source, we anchor it to the current tender_no
                    # instead of dropping the requirement. This preserves the requirement while
                    # aligning the source with the expected Tender label.
                    normalized_requirements: list[list[str]] = []
                    for pair in gliner_relations.get("HAS_REQUIREMENT", []):
                        if not isinstance(pair, list) or len(pair) != 2:
                            continue
                        source_raw, target_raw = pair
                        source = source_raw.get("text", "") if isinstance(source_raw, dict) else source_raw
                        target = target_raw.get("text", "") if isinstance(target_raw, dict) else target_raw
                        source = str(source).strip()
                        target = str(target).strip()
                        if not source or not target:
                            continue
                        # If source isn't a known Tender but tender_no exists, replace it with tender_no.
                        if source not in tender_names_req and tender_no:
                            source = tender_no
                        normalized_requirements.append([source, target])
                    gliner_relations["HAS_REQUIREMENT"] = normalized_requirements
                # Normalize HAS_DEADLINE to match schema (Tender -> Date) and ensure Date entities exist.
                if gliner_relations.get("HAS_DEADLINE"):
                    # Build a normalized set of Tender names so we can re-anchor invalid sources.
                    tender_names_deadline: set[str] = set()
                    for tender_item in entities.get("Tender", []):
                        tender_text = tender_item.get("text", "") if isinstance(tender_item, dict) else tender_item
                        tender_text = str(tender_text).strip()
                        if tender_text:
                            tender_names_deadline.add(tender_text)

                    # Track existing Date entities so we can add missing ones when needed.
                    date_names: set[str] = set()
                    for date_item in entities.get("Date", []):
                        date_text = date_item.get("text", "") if isinstance(date_item, dict) else date_item
                        date_text = str(date_text).strip()
                        if date_text:
                            date_names.add(date_text)

                    normalized_deadlines: list[list[str]] = []
                    for pair in gliner_relations.get("HAS_DEADLINE", []):
                        if not isinstance(pair, list) or len(pair) != 2:
                            continue
                        source_raw, target_raw = pair
                        source = source_raw.get("text", "") if isinstance(source_raw, dict) else source_raw
                        target = target_raw.get("text", "") if isinstance(target_raw, dict) else target_raw
                        source = str(source).strip()
                        target = str(target).strip()
                        if not source or not target:
                            continue
                        # If the source isn't a known Tender, fall back to the structured tender_no.
                        if source not in tender_names_deadline and tender_no:
                            source = tender_no
                        normalized_deadlines.append([source, target])
                        # Ensure the Date entity exists so the relationship can be created.
                        if target not in date_names:
                            if "Date" not in entities:
                                entities["Date"] = []
                            entities["Date"].append(target)
                            date_names.add(target)
                    gliner_relations["HAS_DEADLINE"] = normalized_deadlines
                enrichment_only = ["HAS_REQUIREMENT", "HAS_KEYWORD", "HAS_DEADLINE"]

                merged_relations: dict[str, list[list[str]]] = {}

                # Add all structured relationships (PUBLISHED_BY, AWARDED_TO, IN_CATEGORY)
                for rel_type, pairs in structured_relations.items():
                    merged_relations[rel_type] = pairs

                # Add enrichment relationships from GLiNER2
                for rel_type in enrichment_only:
                    if rel_type in gliner_relations:
                        merged_relations[rel_type] = gliner_relations[rel_type]
                    else:
                        merged_relations[rel_type] = []

                # Update extracted relations with merged version
                extracted["relations"] = merged_relations

                # Log quality issues
                quality_flags = extracted.get("quality_flags", {})
                if quality_flags.get("low_confidence_entities"):
                    low_conf = quality_flags["low_confidence_entities"]
                    print(
                        f"Warning: chunk_id={chunk_id} has {len(low_conf)} low-confidence entities",
                        file=sys.stderr,
                    )
                    for item in low_conf[:3]:  # Log first 3
                        print(
                            f"  - {item['label']}: '{item['text']}' (score={item['score']:.3f})",
                            file=sys.stderr,
                        )

                if quality_flags.get("unknown_labels"):
                    print(
                        f"Warning: chunk_id={chunk_id} has unknown labels: {quality_flags['unknown_labels']}",
                        file=sys.stderr,
                    )

                if not quality_flags.get("has_entities"):
                    print(
                        f"Warning: chunk_id={chunk_id} has no extracted entities",
                        file=sys.stderr,
                    )

                # Build output record with traceability
                output_record = {
                    "chunk_id": chunk_id,
                    "chunk_text": chunk_text,
                    "entities": extracted["entities"],
                    "relations": extracted["relations"],
                    "quality_flags": quality_flags,
                    # Preserve source fields for traceability
                    "tender_no": chunk.get("tender_no", ""),
                    "agency": chunk.get("agency", ""),
                    "award_date": chunk.get("award_date", ""),
                    "supplier": chunk.get("supplier", ""),
                    "awarded_amt": chunk.get("awarded_amt", ""),
                    "category": chunk.get("category", ""),
                    "tender_description": chunk.get("tender_description", ""),
                    "tender_detail_status": chunk.get("tender_detail_status", ""),
                    "_id": chunk.get("_id"),
                }

                # Apply normalization to keywords, requirements, money, and dates
                output_record = normalize_entities_in_extraction(output_record)

                # Write to output JSONL
                json_line = json.dumps(output_record, ensure_ascii=False)
                outfile.write(json_line + "\n")

                processed_count += 1

                if processed_count % 10 == 0:
                    print(f"Processed {processed_count} chunks...")

            except json.JSONDecodeError as e:
                print(f"Warning: Invalid JSON on line {line_num}: {e}", file=sys.stderr)
                continue
            except Exception as e:
                print(f"Error processing chunk {chunk_id}: {e}", file=sys.stderr)
                continue

    print(f"Extraction complete. Processed {processed_count} chunks.")
    return processed_count


def main() -> None:
    """CLI entrypoint for GLiNER2 extraction."""
    parser = argparse.ArgumentParser(
        description="Extract entities and relations from tender chunks using GLiNER2"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Input chunks JSONL file (default: data/chunks/chunks.jsonl)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output extracted JSONL file (default: data/extracted/extracted.jsonl)",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="fastino/gliner2-large-v1",
        help="GLiNER2 model name (default: fastino/gliner2-large-v1)",
    )

    args = parser.parse_args()

    try:
        # Determine paths
        config = Config.load()

        if args.input:
            input_path = Path(args.input)
        else:
            input_path = config.data.chunks_dir / "chunks.jsonl"

        if args.output:
            output_path = Path(args.output)
        else:
            output_path = config.data.extracted_dir / "extracted.jsonl"

        # Check input exists
        if not input_path.exists():
            print(f"Error: Input file not found: {input_path}", file=sys.stderr)
            print("Run 'python -m pipeline.chunk' first to generate chunks.", file=sys.stderr)
            sys.exit(1)

        print(f"Extracting from: {input_path}")
        print(f"Output to: {output_path}")

        # Run extraction
        count = extract_from_chunks(input_path, output_path, model_name=args.model)

        # Show sample
        if count > 0:
            with open(output_path, "r") as f:
                first_line = f.readline()
                sample = json.loads(first_line)
                print(f"\nSample extraction:")
                print(f"  chunk_id: {sample['chunk_id']}")
                print(f"  entities: {list(sample['entities'].keys())}")
                print(f"  relations: {list(sample['relations'].keys())}")

        print(f"\nSuccessfully extracted {count} chunks")

    except Exception as e:
        print(f"Extraction failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
