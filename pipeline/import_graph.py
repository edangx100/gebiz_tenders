"""
Neo4j graph import module for GeBIZ tender knowledge graph.

Imports chunks, entities, and relationships into Neo4j with full traceability.
Creates constraints and indexes for efficient querying.
"""

import argparse
import json
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
from pipeline.category_grouping import CategoryGrouper
from pipeline.schema import get_schema


def create_constraints_and_indexes(driver: Any, database: str = "neo4j") -> None:
    """
    Create Neo4j constraints and indexes for the knowledge graph.

    Creates:
    - Uniqueness constraint on Chunk.id
    - Uniqueness constraint on __Entity__.name
    - Index on Chunk.chunk_id for efficient lookups

    Args:
        driver: Neo4j driver instance
        database: Database name (default: neo4j)
    """
    constraints_queries = [
        # Chunk uniqueness constraint
        "CREATE CONSTRAINT chunk_id_unique IF NOT EXISTS FOR (c:Chunk) REQUIRE c.id IS UNIQUE",

        # Entity base label uniqueness constraint on name
        "CREATE CONSTRAINT entity_name_unique IF NOT EXISTS FOR (e:__Entity__) REQUIRE e.name IS UNIQUE",
    ]

    print("Creating constraints and indexes...")

    with driver.session(database=database) as session:
        for query in constraints_queries:
            try:
                session.run(query)
                print(f"  ✓ {query[:60]}...")
            except Exception as e:
                # Constraint may already exist
                print(f"  ! {query[:60]}... (may already exist: {e})")

    print("Constraints and indexes created successfully")


def import_relationships(
    session: Any,
    chunk_id: str,
    relations: dict[str, list[list[str]]],
) -> None:
    """
    Import relationships between entities from extracted relations.

    Creates edges like:
    - (:Tender)-[:PUBLISHED_BY]->(:Agency)
    - (:Tender)-[:AWARDED_TO]->(:Supplier)
    - (:Tender)-[:IN_CATEGORY]->(:Category)
    - (:Tender)-[:HAS_REQUIREMENT]->(:Requirement)
    - (:Tender)-[:HAS_KEYWORD]->(:Keyword)
    - (:Tender)-[:HAS_DEADLINE]->(:Date)

    Args:
        session: Neo4j session
        chunk_id: Source chunk ID for traceability
        relations: Dictionary mapping relation types to list of [source, target] pairs
    """
    schema = get_schema()

    for relation_type, relation_pairs in relations.items():
        if not relation_pairs:
            continue

        # Get relationship definition to find source/target entity types
        rel_def = schema.get_relation_definition(relation_type)
        if rel_def is None:
            print(f"Warning: Unknown relation type {relation_type}, skipping", file=sys.stderr)
            continue

        source_label = rel_def.source_entity
        target_label = rel_def.target_entity

        # Process each [source, target] pair
        for pair in relation_pairs:
            if not isinstance(pair, list) or len(pair) != 2:
                print(f"Warning: Invalid relation pair format: {pair}", file=sys.stderr)
                continue

            source_name, target_name = pair

            # Handle entity names that may be dicts with 'text' key or plain strings
            if isinstance(source_name, dict):
                source_name = source_name.get("text", "")
            if isinstance(target_name, dict):
                target_name = target_name.get("text", "")

            if not source_name or not target_name:
                continue

            source_name = str(source_name).strip()
            target_name = str(target_name).strip()

            if not source_name or not target_name:
                continue

            # Create relationship between entities
            # Using f-string for dynamic labels and relation type
            create_relationship_query = f"""
            MATCH (source:__Entity__:`{source_label}` {{name: $source_name}})
            MATCH (target:__Entity__:`{target_label}` {{name: $target_name}})
            MERGE (source)-[r:`{relation_type}`]->(target)
            RETURN r
            """

            try:
                result = session.run(
                    create_relationship_query,
                    source_name=source_name,
                    target_name=target_name,
                )
                # Check if relationship was created
                if result.single() is None:
                    print(
                        f"Warning: Could not create {relation_type} relationship "
                        f"from '{source_name}' ({source_label}) to '{target_name}' ({target_label}). "
                        f"One or both entities may not exist.",
                        file=sys.stderr
                    )
            except Exception as e:
                print(
                    f"Warning: Failed to create {relation_type} relationship "
                    f"from '{source_name}' to '{target_name}': {e}",
                    file=sys.stderr
                )


def attach_category_group(
    session: Any,
    category_name: str,
    category_group: str,
) -> None:
    """Attach a high-level category group to a Category node."""
    attach_group_query = """
    MERGE (cat:__Entity__:Category {name: $category_name})
    SET cat.category_group = $category_group
    MERGE (grp:CategoryGroup {name: $category_group})
    MERGE (cat)-[:IN_CATEGORY_GROUP]->(grp)
    """
    session.run(
        attach_group_query,
        category_name=category_name,
        category_group=category_group,
    )


def import_chunk(
    session: Any,
    chunk_data: dict[str, Any],
    category_grouper: CategoryGrouper | None = None,
) -> None:
    """
    Import a single chunk with its entities and create relationships.

    Creates:
    - :Chunk node with source fields (id, text, tender_no, agency, award_date, etc.)
    - :__Entity__ nodes with dynamic labels per entity type
    - (:Chunk)-[:MENTIONS]->(:__Entity__) traceability edges
    - Entity-to-entity relationships (PUBLISHED_BY, AWARDED_TO, etc.)

    Args:
        session: Neo4j session
        chunk_data: Chunk dictionary with chunk_id, chunk_text, entities, relations, and source fields
    """
    # Extract core fields
    chunk_id = chunk_data.get("chunk_id", "")
    chunk_text = chunk_data.get("chunk_text", "")

    # Extract source fields for traceability
    source_fields = {
        "tender_no": chunk_data.get("tender_no", ""),
        "agency": chunk_data.get("agency", ""),
        "award_date": chunk_data.get("award_date", ""),
        "supplier": chunk_data.get("supplier", ""),
        "awarded_amt": chunk_data.get("awarded_amt", ""),
        "category": chunk_data.get("category", ""),
        "tender_description": chunk_data.get("tender_description", ""),
        "tender_detail_status": chunk_data.get("tender_detail_status", ""),
    }

    # Extract entities and relations
    entities = chunk_data.get("entities", {})
    relations = chunk_data.get("relations", {})
    quality_flags = chunk_data.get("quality_flags", {})

    category_name = str(source_fields.get("category", "")).strip()
    category_group = chunk_data.get("category_group")
    if not category_group and category_grouper and category_name:
        # LLM classification is optional and only runs when configured.
        category_group = category_grouper.classify(category_name).group

    # Create Chunk node with all source fields
    create_chunk_query = """
    MERGE (c:Chunk {id: $chunk_id})
    SET c.text = $chunk_text,
        c.tender_no = $tender_no,
        c.agency = $agency,
        c.award_date = $award_date,
        c.supplier = $supplier,
        c.awarded_amt = $awarded_amt,
        c.category = $category,
        c.category_group = $category_group,
        c.tender_description = $tender_description,
        c.tender_detail_status = $tender_detail_status,
        c.has_entities = $has_entities,
        c.has_relations = $has_relations
    RETURN c
    """

    session.run(
        create_chunk_query,
        chunk_id=chunk_id,
        chunk_text=chunk_text,
        has_entities=quality_flags.get("has_entities", False),
        has_relations=quality_flags.get("has_relations", False),
        category_group=category_group,
        **source_fields,
    )

    # Create entity nodes with dynamic labels and MENTIONS relationships
    # Process each entity type (Tender, Agency, Supplier, etc.)
    for entity_label, entity_list in entities.items():
        if not entity_list:
            continue

        for entity_item in entity_list:
            # Entity can be a string or dict with text/score
            if isinstance(entity_item, str):
                entity_name = entity_item
            elif isinstance(entity_item, dict):
                entity_name = entity_item.get("text", "")
            else:
                continue

            if not entity_name or not entity_name.strip():
                continue

            entity_name = entity_name.strip()

            # Create entity with base __Entity__ label + dynamic label
            # Using apoc-style dynamic label creation via CALL in transactions
            create_entity_query = f"""
            MATCH (c:Chunk {{id: $chunk_id}})
            MERGE (e:__Entity__ {{name: $entity_name}})
            SET e:`{entity_label}`
            MERGE (c)-[:MENTIONS]->(e)
            RETURN e
            """

            try:
                session.run(
                    create_entity_query,
                    chunk_id=chunk_id,
                    entity_name=entity_name,
                )
            except Exception as e:
                print(f"Warning: Failed to create entity {entity_name} with label {entity_label}: {e}", file=sys.stderr)

    if category_group and category_name:
        # Ensure the structured category title is linked to its group.
        attach_category_group(session, category_name, str(category_group))

    # Import relationships between entities
    if relations:
        import_relationships(session, chunk_id, relations)


def import_chunks_from_file(
    extracted_path: Path,
    neo4j_uri: str,
    neo4j_username: str,
    neo4j_password: str,
    neo4j_database: str = "neo4j",
    limit: int | None = None,
) -> int:
    """
    Import chunks from extracted JSONL file into Neo4j.

    Args:
        extracted_path: Path to extracted JSONL file
        neo4j_uri: Neo4j connection URI
        neo4j_username: Neo4j username
        neo4j_password: Neo4j password
        neo4j_database: Neo4j database name (default: neo4j)
        limit: Optional limit on number of chunks to import

    Returns:
        Number of chunks imported
    """
    if GraphDatabase is None:
        raise ImportError(
            "neo4j driver is not installed. Install with: pip install neo4j"
        )

    if not extracted_path.exists():
        raise FileNotFoundError(f"Extracted data file not found: {extracted_path}")

    print(f"Connecting to Neo4j at {neo4j_uri}...")
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_username, neo4j_password))

    try:
        # Verify connection
        driver.verify_connectivity()
        print("Connected to Neo4j successfully")

        # Create constraints and indexes
        create_constraints_and_indexes(driver, neo4j_database)

        # Initialize category grouping if OpenAI config is available.
        category_grouper = None
        config = Config.load()
        if config.openai is not None:
            try:
                cache_path = config.data.extracted_dir / "category_group_cache.json"
                category_grouper = CategoryGrouper(config.openai, cache_path)
                print("Category grouping enabled (OpenAI).")
            except ImportError as e:
                print(f"Category grouping disabled: {e}", file=sys.stderr)

        # Read and import chunks
        print(f"\nImporting chunks from {extracted_path}...")
        chunk_count = 0

        with extracted_path.open("r") as f:
            with driver.session(database=neo4j_database) as session:
                for line_num, line in enumerate(f, 1):
                    if limit and chunk_count >= limit:
                        break

                    try:
                        chunk_data = json.loads(line)
                        import_chunk(session, chunk_data, category_grouper=category_grouper)
                        chunk_count += 1

                        if chunk_count % 10 == 0:
                            print(f"  Imported {chunk_count} chunks...")

                    except json.JSONDecodeError as e:
                        print(f"Warning: Skipping invalid JSON at line {line_num}: {e}", file=sys.stderr)
                        continue
                    except Exception as e:
                        print(f"Warning: Failed to import chunk at line {line_num}: {e}", file=sys.stderr)
                        continue

        print(f"\n✓ Successfully imported {chunk_count} chunks")

        # Print summary statistics
        with driver.session(database=neo4j_database) as session:
            result = session.run("""
                MATCH (c:Chunk)
                OPTIONAL MATCH (c)-[:MENTIONS]->(e:__Entity__)
                OPTIONAL MATCH (e1:__Entity__)-[r]->(e2:__Entity__)
                WHERE type(r) <> 'MENTIONS'
                RETURN
                    count(DISTINCT c) as chunk_count,
                    count(DISTINCT e) as entity_count,
                    count(DISTINCT r) as relationship_count
            """)
            stats = result.single()
            if stats:
                print(f"\nGraph Statistics:")
                print(f"  Chunks: {stats['chunk_count']}")
                print(f"  Entities: {stats['entity_count']}")
                print(f"  Entity relationships: {stats['relationship_count']}")
                print(f"  MENTIONS relationships: {stats['entity_count']}")  # One per entity

        return chunk_count

    finally:
        driver.close()


def main() -> None:
    """Main entry point for graph import."""
    parser = argparse.ArgumentParser(
        description="Import GeBIZ tender chunks into Neo4j knowledge graph"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of chunks to import (default: all)",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear all existing data before import",
    )

    args = parser.parse_args()

    # Load configuration
    try:
        config = Config.load(require_neo4j=True)
    except ValueError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        print("\nRequired environment variables:", file=sys.stderr)
        print("  NEO4J_URI - Neo4j connection URI (e.g., bolt://localhost:7687)", file=sys.stderr)
        print("  NEO4J_USERNAME - Neo4j username", file=sys.stderr)
        print("  NEO4J_PASSWORD - Neo4j password", file=sys.stderr)
        sys.exit(1)

    # Ensure neo4j config is loaded
    if config.neo4j is None:
        print("Error: Neo4j configuration not loaded", file=sys.stderr)
        sys.exit(1)

    # Default paths
    extracted_path = config.data.extracted_dir / "extracted.jsonl"

    if not extracted_path.exists():
        print(f"Error: No extracted data found at {extracted_path}", file=sys.stderr)
        print("Run extraction first: python -m pipeline.extract", file=sys.stderr)
        sys.exit(1)

    # Clear existing data if requested
    if args.clear:
        print("Clearing existing graph data...")
        if GraphDatabase is None:
            raise ImportError("neo4j driver is not installed. Install with: pip install neo4j")

        driver = GraphDatabase.driver(
            config.neo4j.uri,
            auth=(config.neo4j.username, config.neo4j.password)
        )
        try:
            with driver.session(database=config.neo4j.database) as session:
                session.run("MATCH (n) DETACH DELETE n")
            print("✓ Graph data cleared")
        finally:
            driver.close()

    # Import chunks
    try:
        chunk_count = import_chunks_from_file(
            extracted_path=extracted_path,
            neo4j_uri=config.neo4j.uri,
            neo4j_username=config.neo4j.username,
            neo4j_password=config.neo4j.password,
            neo4j_database=config.neo4j.database,
            limit=args.limit,
        )

        print(f"\n✓ Import completed successfully ({chunk_count} chunks)")

    except Exception as e:
        print(f"Import failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
