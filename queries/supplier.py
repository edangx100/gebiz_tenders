"""
Supplier explorer queries for GeBIZ tender knowledge graph.

Provides queries to explore tenders by supplier and supplier statistics.
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


def get_tenders_by_supplier(driver: Any, supplier_name: str, database: str = "neo4j") -> list[dict[str, Any]]:
    """
    Get all tenders awarded to a specific supplier.

    Args:
        driver: Neo4j driver instance
        supplier_name: Name of the supplier to filter by
        database: Database name (default: neo4j)

    Returns:
        List of tender records with fields:
        - tender_name: Name of the tender entity
        - tender_no: Tender reference number
        - supplier_name: Supplier name
        - award_date: Award date
        - awarded_amt: Awarded amount
        - agency: Agency name
        - category: Tender category
    """
    query = load_query("supplier_tenders")

    with driver.session(database=database) as session:
        result = session.run(query, supplier_name=supplier_name)
        return [dict(record) for record in result]


def get_supplier_suggestions(
    driver: Any,
    supplier_name: str,
    limit: int = 5,
    database: str = "neo4j"
) -> list[str]:
    """
    Suggest supplier names based on a fuzzy (normalized) match.

    Args:
        driver: Neo4j driver instance
        supplier_name: Supplier name input to match
        limit: Maximum number of suggestions to return (default: 5)
        database: Database name (default: neo4j)

    Returns:
        List of suggested supplier names.
    """
    # Use a normalized match to ignore punctuation and spacing differences.
    query = load_query("supplier_suggestions")

    with driver.session(database=database) as session:
        result = session.run(query, supplier_name=supplier_name, limit=limit)
        return [record["supplier_name"] for record in result]


def get_top_suppliers(driver: Any, limit: int = 10, database: str = "neo4j") -> list[dict[str, Any]]:
    """
    Get top suppliers by total awarded amount.

    Args:
        driver: Neo4j driver instance
        limit: Maximum number of suppliers to return (default: 10)
        database: Database name (default: neo4j)

    Returns:
        List of supplier records with fields:
        - supplier_name: Name of the supplier
        - total_amount: Total value of awards
        - tender_count: Number of tenders awarded
    """
    query = load_query("supplier_top")

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
        print(f"   Supplier: {tender.get('supplier_name', 'N/A')}")
        print(f"   Tender No: {tender.get('tender_no', 'N/A')}")
        print(f"   Award Date: {tender.get('award_date', 'N/A')}")
        print(f"   Awarded Amount: ${tender.get('awarded_amt', 'N/A')}")
        print(f"   Agency: {tender.get('agency', 'N/A')}")
        print(f"   Category: {tender.get('category', 'N/A')}")
        print()


def format_suggestions(suggestions: list[str]) -> None:
    """
    Format and print supplier name suggestions to stdout.

    Args:
        suggestions: List of suggested supplier names
    """
    if not suggestions:
        return

    print("Did you mean:")
    for name in suggestions:
        print(f"- {name}")
    print()


def format_suppliers(suppliers: list[dict[str, Any]]) -> None:
    """
    Format and print suppliers to stdout.

    Args:
        suppliers: List of supplier records
    """
    if not suppliers:
        print("No suppliers found.")
        return

    print(f"\nTop {len(suppliers)} supplier(s) by total awarded amount:\n")
    for i, supplier in enumerate(suppliers, 1):
        total_amt = supplier.get('total_amount', 0)
        tender_count = supplier.get('tender_count', 0)
        print(f"{i}. {supplier.get('supplier_name', 'N/A')}")
        print(f"   Total Amount: ${total_amt:,.2f}")
        print(f"   Tender Count: {tender_count}")
        print()


def main() -> None:
    """
    CLI entry point for supplier queries.

    Supports two query modes:
    - --top: Show top suppliers by total awarded amount
    - --supplier: Show tenders for a specific supplier
    """
    if not NEO4J_AVAILABLE:
        raise ImportError(
            "neo4j package not installed. Install with: pip install neo4j"
        )

    parser = argparse.ArgumentParser(
        description="Query GeBIZ tenders by supplier"
    )

    # Query mode selection (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--top",
        action="store_true",
        help="Show top suppliers by total awarded amount"
    )
    mode_group.add_argument(
        "--supplier",
        type=str,
        help="Show tenders for a specific supplier"
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
            # Top suppliers query
            suppliers = get_top_suppliers(driver, limit=args.limit, database=config.neo4j.database)
            format_suppliers(suppliers)
        elif args.supplier:
            # Tenders by supplier query
            tenders = get_tenders_by_supplier(driver, args.supplier, database=config.neo4j.database)
            format_tenders(tenders)
            if not tenders:
                # Fall back to name suggestions when no exact matches are found.
                suggestions = get_supplier_suggestions(
                    driver,
                    args.supplier,
                    database=config.neo4j.database
                )
                format_suggestions(suggestions)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
