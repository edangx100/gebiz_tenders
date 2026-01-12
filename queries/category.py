"""
Category explorer queries for GeBIZ tender knowledge graph.

Provides queries to explore categories with their associated keywords and requirements.
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


def explore_categories(
    driver: Any,
    category_name: str | None = None,
    category_group: str | None = None,
    database: str = "neo4j"
) -> list[dict[str, Any]]:
    """
    Explore categories with their associated keywords and requirements.

    Args:
        driver: Neo4j driver instance
        category_name: Optional category name to filter by (default: all categories)
        category_group: Optional category group to filter by (default: all groups)
        database: Database name (default: neo4j)

    Returns:
        List of category records with fields:
        - category_name: Name of the category
        - tender_count: Number of tenders in this category
        - keywords: List of associated keywords
        - requirements: List of associated requirements
    """
    query = load_query("category_explorer")

    with driver.session(database=database) as session:
        result = session.run(
            query,
            category_name=category_name,
            category_group=category_group,
        )
        return [dict(record) for record in result]


def format_categories(categories: list[dict[str, Any]], max_terms: int = 10) -> None:
    """
    Format and print categories to stdout.

    Args:
        categories: List of category records
        max_terms: Maximum number of keywords/requirements to display per category
    """
    if not categories:
        print("No categories found.")
        return

    print(f"\nFound {len(categories)} category/categories:\n")
    for i, category in enumerate(categories, 1):
        print(f"{i}. {category.get('category_name', 'N/A')}")
        category_group = category.get("category_group")
        if category_group:
            # Display high-level grouping when available.
            print(f"   Category Group: {category_group}")
        print(f"   Tender Count: {category.get('tender_count', 0)}")

        # Display keywords
        keywords = category.get('keywords', [])
        # Filter out None values
        keywords = [k for k in keywords if k is not None]
        if keywords:
            display_keywords = keywords[:max_terms]
            print(f"   Keywords ({len(keywords)} total):")
            for keyword in display_keywords:
                print(f"     - {keyword}")
            if len(keywords) > max_terms:
                print(f"     ... and {len(keywords) - max_terms} more")
        else:
            print("   Keywords: (none)")

        # Display requirements
        requirements = category.get('requirements', [])
        # Filter out None values
        requirements = [r for r in requirements if r is not None]
        if requirements:
            display_reqs = requirements[:max_terms]
            print(f"   Requirements ({len(requirements)} total):")
            for req in display_reqs:
                print(f"     - {req}")
            if len(requirements) > max_terms:
                print(f"     ... and {len(requirements) - max_terms} more")
        else:
            print("   Requirements: (none)")

        print()


def main() -> None:
    """
    CLI entry point for category queries.

    Supports:
    - --all: Show all categories with keywords/requirements
    - --category: Show specific category details
    - --group: Show categories within a category group
    """
    if not NEO4J_AVAILABLE:
        raise ImportError(
            "neo4j package not installed. Install with: pip install neo4j"
        )

    parser = argparse.ArgumentParser(
        description="Explore GeBIZ tender categories"
    )

    # Query mode selection
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--all",
        action="store_true",
        help="Show all categories with keywords and requirements"
    )
    mode_group.add_argument(
        "--category",
        type=str,
        help="Show details for a specific category"
    )
    mode_group.add_argument(
        "--group",
        type=str,
        help="Show categories for a specific category group"
    )

    # Options
    parser.add_argument(
        "--max-terms",
        type=int,
        default=10,
        help="Maximum number of keywords/requirements to display per category (default: 10)"
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
        if args.all:
            # All categories
            categories = explore_categories(driver, database=config.neo4j.database)
            format_categories(categories, max_terms=args.max_terms)
        elif args.category:
            # Specific category
            categories = explore_categories(
                driver,
                category_name=args.category,
                database=config.neo4j.database
            )
            format_categories(categories, max_terms=args.max_terms)
        elif args.group:
            # Specific category group
            categories = explore_categories(
                driver,
                category_group=args.group,
                database=config.neo4j.database
            )
            format_categories(categories, max_terms=args.max_terms)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
