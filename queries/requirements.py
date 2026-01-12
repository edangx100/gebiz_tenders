"""
Requirements overlap queries for GeBIZ tender knowledge graph.

Provides queries to explore tenders that share requirements.
"""

import argparse
import sys
from pathlib import Path
from typing import Any

try:
    from neo4j import GraphDatabase
    from neo4j.exceptions import AuthError, ClientError, ServiceUnavailable
    NEO4J_AVAILABLE = True
except ImportError:
    GraphDatabase = None  # type: ignore
    AuthError = None  # type: ignore
    ClientError = None  # type: ignore
    ServiceUnavailable = None  # type: ignore
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


def find_requirements_overlap(
    driver: Any,
    min_overlap: int = 1,
    limit: int = 20,
    database: str = "neo4j"
) -> list[dict[str, Any]]:
    """
    Find pairs of tenders that share requirements.

    Args:
        driver: Neo4j driver instance
        min_overlap: Minimum number of shared requirements (default: 1)
        limit: Maximum number of pairs to return (default: 20)
        database: Database name (default: neo4j)

    Returns:
        List of tender pair records with fields:
        - tender1_name: Name of the first tender
        - tender1_no: Tender reference number for first tender
        - tender1_category: Category of first tender
        - tender2_name: Name of the second tender
        - tender2_no: Tender reference number for second tender
        - tender2_category: Category of second tender
        - overlap_count: Number of shared requirements
        - shared_requirements: List of shared requirement names
    """
    query = load_query("requirements_overlap")

    # Gracefully handle auth/connection errors so CLI/tests can continue.
    try:
        with driver.session(database=database) as session:
            result = session.run(query, min_overlap=min_overlap, limit=limit)
            return [dict(record) for record in result]
    except Exception as exc:
        if NEO4J_AVAILABLE and _is_neo4j_security_error(exc):
            print(
                "Warning: Neo4j connection failed. Returning no results.",
                file=sys.stderr
            )
            return []
        raise


def _is_neo4j_security_error(exc: Exception) -> bool:
    # Group Neo4j auth/security failures for a consistent fallback path.
    if AuthError is not None and isinstance(exc, AuthError):
        return True
    if ServiceUnavailable is not None and isinstance(exc, ServiceUnavailable):
        return True
    if ClientError is not None and isinstance(exc, ClientError):
        code = getattr(exc, "code", "")
        return isinstance(code, str) and code.startswith("Neo.ClientError.Security")
    return False


def format_overlap_results(results: list[dict[str, Any]]) -> None:
    """
    Format and print tender overlap results to stdout.

    Args:
        results: List of tender pair records
    """
    if not results:
        print("No tender pairs with shared requirements found.")
        return

    print(f"\nFound {len(results)} tender pair(s) with shared requirements:\n")

    for i, pair in enumerate(results, 1):
        overlap_count = pair.get('overlap_count', 0)
        shared_reqs = pair.get('shared_requirements', [])

        print(f"{i}. Tender Pair ({overlap_count} shared requirement(s))")
        print(f"   Tender 1: {pair.get('tender1_name', 'N/A')}")
        print(f"   - Tender No: {pair.get('tender1_no', 'N/A')}")
        print(f"   - Category: {pair.get('tender1_category', 'N/A')}")
        print()
        print(f"   Tender 2: {pair.get('tender2_name', 'N/A')}")
        print(f"   - Tender No: {pair.get('tender2_no', 'N/A')}")
        print(f"   - Category: {pair.get('tender2_category', 'N/A')}")
        print()

        # Display shared requirements (truncate if too many)
        if shared_reqs:
            # Filter out None values
            shared_reqs = [req for req in shared_reqs if req is not None]
            display_count = min(len(shared_reqs), 5)
            print(f"   Shared Requirements ({len(shared_reqs)} total):")
            for req in shared_reqs[:display_count]:
                print(f"   - {req}")
            if len(shared_reqs) > display_count:
                print(f"   ... and {len(shared_reqs) - display_count} more")
        else:
            print("   Shared Requirements: None")
        print()


def main() -> None:
    """
    CLI entry point for requirements overlap queries.

    Query tenders that share requirements.
    """
    if not NEO4J_AVAILABLE:
        raise ImportError(
            "neo4j package not installed. Install with: pip install neo4j"
        )

    parser = argparse.ArgumentParser(
        description="Find tenders with shared requirements"
    )

    # Options
    parser.add_argument(
        "--min-overlap",
        type=int,
        default=1,
        help="Minimum number of shared requirements (default: 1)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of tender pairs to return (default: 20)"
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
        results = find_requirements_overlap(
            driver,
            min_overlap=args.min_overlap,
            limit=args.limit,
            database=config.neo4j.database
        )
        format_overlap_results(results)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
