"""
Main pipeline orchestrator.

Runs all pipeline steps in sequence: fetch -> chunk -> extract -> import.
"""

import argparse
import sys
from typing import Optional


def run_pipeline(limit: Optional[int] = None) -> None:
    """
    Run the complete pipeline.

    Args:
        limit: Maximum number of records to process

    Raises:
        Exception: If any pipeline step fails
    """
    print("=" * 60)
    print("GeBIZ Tender Intelligence Pipeline")
    print("=" * 60)

    # Step 1: Fetch data
    print("\n[1/4] Fetching data...")
    from pipeline.fetch import fetch_gebiz_data

    try:
        records = fetch_gebiz_data(limit=limit)
        print(f"✓ Fetched {len(records)} records")
    except Exception as e:
        print(f"✗ Failed to fetch data: {e}", file=sys.stderr)
        raise

    # Step 2: Build chunks
    print("\n[2/4] Building chunks...")
    from pipeline.chunk import build_chunks, write_chunks_to_jsonl
    from pipeline.config import Config

    try:
        chunks = build_chunks(records)
        config = Config.load()
        output_path = config.data.chunks_dir / "chunks.jsonl"
        write_chunks_to_jsonl(chunks, output_path)
        print(f"✓ Built {len(chunks)} chunks")
    except Exception as e:
        print(f"✗ Failed to build chunks: {e}", file=sys.stderr)
        raise

    # Step 3: Extract entities
    print("\n[3/4] Extracting entities...")
    from pipeline.extract import extract_from_chunks

    try:
        chunks_path = config.data.chunks_dir / "chunks.jsonl"
        extracted_path = config.data.extracted_dir / "extracted.jsonl"
        count = extract_from_chunks(chunks_path, extracted_path)
        print(f"✓ Extracted entities from {count} chunks")
    except ImportError as e:
        print(f"⚠ Skipping extraction: {e}", file=sys.stderr)
        print("  Install gliner2 with: pip install gliner2")
    except Exception as e:
        print(f"✗ Failed to extract entities: {e}", file=sys.stderr)
        raise

    # Step 4: Import to Neo4j
    print("\n[4/4] Importing to Neo4j...")
    from pipeline.import_graph import import_chunks_from_file

    try:
        # Check if Neo4j config is available
        neo4j_config = Config.load(require_neo4j=True)

        if neo4j_config.neo4j is None:
            raise ValueError("Neo4j configuration not loaded")

        extracted_path = config.data.extracted_dir / "extracted.jsonl"

        count = import_chunks_from_file(
            extracted_path=extracted_path,
            neo4j_uri=neo4j_config.neo4j.uri,
            neo4j_username=neo4j_config.neo4j.username,
            neo4j_password=neo4j_config.neo4j.password,
            neo4j_database=neo4j_config.neo4j.database,
            limit=limit,
        )
        print(f"✓ Imported {count} chunks to Neo4j")
    except ValueError as e:
        print(f"⚠ Skipping Neo4j import: {e}", file=sys.stderr)
        print("  Set NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD environment variables")
    except ImportError as e:
        print(f"⚠ Skipping Neo4j import: {e}", file=sys.stderr)
        print("  Install neo4j driver with: pip install neo4j")
    except Exception as e:
        print(f"✗ Failed to import to Neo4j: {e}", file=sys.stderr)
        raise

    print("\n" + "=" * 60)
    print("Pipeline completed successfully!")
    print("=" * 60)


def main() -> None:
    """CLI entrypoint for full pipeline."""
    parser = argparse.ArgumentParser(description="Run GeBIZ pipeline end-to-end")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of records to process",
    )

    args = parser.parse_args()

    try:
        run_pipeline(limit=args.limit)
    except Exception as e:
        print(f"\nPipeline failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
