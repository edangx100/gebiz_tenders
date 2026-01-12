"""
Similar tenders query for GeBIZ tender knowledge graph.

Provides similarity query to find tenders similar to a reference tender
based on shared keywords and requirements.
"""

import argparse
import sys
from pathlib import Path
from typing import Any

try:
    from neo4j import GraphDatabase
    NEO4J_AVAILABLE = True
except ImportError:
    GraphDatabase = None  # type: ignore
    NEO4J_AVAILABLE = False

from pipeline.config import Config


def load_query(query_name: str) -> str:
    """
    Load a Cypher query from the queries directory.

    Args:
        query_name: Name of the query file (without .cql extension)

    Returns:
        Cypher query string

    Raises:
        FileNotFoundError: If query file does not exist
    """
    query_file = Path(__file__).parent / f"{query_name}.cql"
    if not query_file.exists():
        raise FileNotFoundError(f"Query file not found: {query_file}")

    return query_file.read_text()


def find_similar_tenders(
    driver: Any,
    tender_name: str,
    limit: int = 10,
    include_category: bool = False,
    database: str = "neo4j"
) -> list[dict[str, Any]]:
    """
    Find tenders similar to a reference tender based on shared keywords and requirements.

    Args:
        driver: Neo4j driver instance
        tender_name: Name of the reference tender to find similar tenders for
        limit: Maximum number of similar tenders to return (default: 10)
        include_category: Whether to boost similarity score for same category (default: False)
        database: Database name (default: neo4j)

    Returns:
        List of similar tender records with fields:
        - tender_name: Name of the similar tender
        - tender_no: Tender reference number
        - category: Tender category
        - awarded_amt: Awarded amount
        - similarity_score: Overall similarity score
        - overlap_count: Number of shared keywords + requirements
        - shared_keywords: List of shared keyword names
        - shared_requirements: List of shared requirement names
        - same_category: Boolean indicating if category matches
    """
    query = load_query("similar_tenders")

    with driver.session(database=database) as session:
        result = session.run(
            query,
            tender_name=tender_name,
            limit=limit,
            include_category=include_category
        )
        return [dict(record) for record in result]


def format_similar_tenders(results: list[dict[str, Any]]) -> None:
    """
    Format and print similar tenders to stdout.

    Args:
        results: List of similar tender records
    """
    if not results:
        print("No similar tenders found.")
        return

    print(f"\nFound {len(results)} similar tender(s):\n")

    for i, tender in enumerate(results, 1):
        similarity_score = tender.get('similarity_score', 0)
        overlap_count = tender.get('overlap_count', 0)
        same_category = tender.get('same_category', False)

        print(f"{i}. {tender.get('tender_name', 'N/A')} (score: {similarity_score})")
        print(f"   Tender No: {tender.get('tender_no', 'N/A')}")
        print(f"   Category: {tender.get('category', 'N/A')}{' ✓' if same_category else ''}")

        awarded_amt = tender.get('awarded_amt')
        if awarded_amt:
            print(f"   Awarded Amount: ${awarded_amt}")

        print(f"   Overlap: {overlap_count} shared term(s)")

        # Display shared keywords
        shared_keywords = tender.get('shared_keywords', [])
        if shared_keywords:
            # Filter out None values
            shared_keywords = [k for k in shared_keywords if k is not None]
            if shared_keywords:
                display_count = min(len(shared_keywords), 3)
                keywords_str = ', '.join(shared_keywords[:display_count])
                if len(shared_keywords) > display_count:
                    keywords_str += f' (+{len(shared_keywords) - display_count} more)'
                print(f"   Keywords: {keywords_str}")

        # Display shared requirements
        shared_requirements = tender.get('shared_requirements', [])
        if shared_requirements:
            # Filter out None values
            shared_requirements = [r for r in shared_requirements if r is not None]
            if shared_requirements:
                display_count = min(len(shared_requirements), 3)
                reqs_str = ', '.join(shared_requirements[:display_count])
                if len(shared_requirements) > display_count:
                    reqs_str += f' (+{len(shared_requirements) - display_count} more)'
                print(f"   Requirements: {reqs_str}")

        print()


def main() -> None:
    """
    CLI entry point for similar tenders query.

    Find tenders similar to a reference tender.
    """
    if not NEO4J_AVAILABLE:
        raise ImportError(
            "neo4j package not installed. Install with: pip install neo4j"
        )

    parser = argparse.ArgumentParser(
        description="Find tenders similar to a reference tender"
    )

    # Required argument
    parser.add_argument(
        "--tender",
        type=str,
        required=True,
        help="Name of the reference tender to find similar tenders for"
    )

    # Options
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of similar tenders to return (default: 10)"
    )
    parser.add_argument(
        "--include-category",
        action="store_true",
        help="Boost similarity score for tenders in the same category"
    )

    args = parser.parse_args()

    # Load configuration
    config = Config.load(require_neo4j=True)
    if config.neo4j is None:
        raise RuntimeError("Neo4j configuration required but not provided")

    # Create driver
    driver = GraphDatabase.driver(
        config.neo4j.uri,
        auth=(config.neo4j.username, config.neo4j.password)
    )

    try:
        results = find_similar_tenders(
            driver,
            tender_name=args.tender,
            limit=args.limit,
            include_category=args.include_category,
            database=config.neo4j.database
        )
        format_similar_tenders(results)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
