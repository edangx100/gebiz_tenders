"""
Agency explorer queries for GeBIZ tender knowledge graph.

Provides queries to explore tenders by agency and agency statistics.
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


def get_tenders_by_agency(driver: Any, agency_name: str, database: str = "neo4j") -> list[dict[str, Any]]:
    """
    Get all tenders published by a specific agency.

    Args:
        driver: Neo4j driver instance
        agency_name: Name of the agency to filter by
        database: Database name (default: neo4j)

    Returns:
        List of tender records with fields:
        - tender_name: Name of the tender entity
        - tender_no: Tender reference number
        - award_date: Award date
        - awarded_amt: Awarded amount
        - supplier: Supplier name
        - category: Tender category
    """
    query = load_query("agency_tenders")

    with driver.session(database=database) as session:
        result = session.run(query, agency_name=agency_name)
        return [dict(record) for record in result]


def get_top_agencies(driver: Any, limit: int = 10, database: str = "neo4j") -> list[dict[str, Any]]:
    """
    Get top agencies by number of awards.

    Args:
        driver: Neo4j driver instance
        limit: Maximum number of agencies to return (default: 10)
        database: Database name (default: neo4j)

    Returns:
        List of agency records with fields:
        - agency_name: Name of the agency
        - tender_count: Number of tenders published
    """
    query = load_query("agency_top")

    with driver.session(database=database) as session:
        result = session.run(query, limit=limit)
        return [dict(record) for record in result]


def format_tenders(tenders: list[dict[str, Any]]) -> None:
    """
    Format and print tenders to stdout.

    Args:
        tenders: List of tender records
    """
    if not tenders:
        print("No tenders found.")
        return

    print(f"\nFound {len(tenders)} tender(s):\n")
    for i, tender in enumerate(tenders, 1):
        print(f"{i}. {tender.get('tender_name', 'N/A')}")
        print(f"   Tender No: {tender.get('tender_no', 'N/A')}")
        print(f"   Award Date: {tender.get('award_date', 'N/A')}")
        print(f"   Awarded Amount: ${tender.get('awarded_amt', 'N/A')}")
        print(f"   Supplier: {tender.get('supplier', 'N/A')}")
        print(f"   Category: {tender.get('category', 'N/A')}")
        print()


def format_agencies(agencies: list[dict[str, Any]]) -> None:
    """
    Format and print agencies to stdout.

    Args:
        agencies: List of agency records
    """
    if not agencies:
        print("No agencies found.")
        return

    print(f"\nTop {len(agencies)} agency/agencies by tender count:\n")
    for i, agency in enumerate(agencies, 1):
        print(f"{i}. {agency.get('agency_name', 'N/A')}: {agency.get('tender_count', 0)} tender(s)")


def main() -> None:
    """
    CLI entry point for agency queries.

    Supports two query modes:
    - --top: Show top agencies by tender count
    - --agency: Show tenders for a specific agency
    """
    if not NEO4J_AVAILABLE:
        raise ImportError(
            "neo4j package not installed. Install with: pip install neo4j"
        )

    parser = argparse.ArgumentParser(
        description="Query GeBIZ tenders by agency"
    )

    # Query mode selection (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--top",
        action="store_true",
        help="Show top agencies by number of awards"
    )
    mode_group.add_argument(
        "--agency",
        type=str,
        help="Show tenders for a specific agency"
    )

    # Options
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of results to return (for --top mode, default: 10)"
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
        if args.top:
            # Top agencies query
            agencies = get_top_agencies(driver, limit=args.limit, database=config.neo4j.database)
            format_agencies(agencies)
        elif args.agency:
            # Tenders by agency query
            tenders = get_tenders_by_agency(driver, args.agency, database=config.neo4j.database)
            format_tenders(tenders)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
