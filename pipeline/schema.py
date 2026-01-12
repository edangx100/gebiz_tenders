"""
Extraction schema for GeBIZ tender entity and relationship extraction.
Defines entity types and relationship types for GLiNER2-based extraction.
"""

from dataclasses import dataclass, field
from typing import Literal


# Schema version for tracking changes
SCHEMA_VERSION = "1.0.0"


@dataclass
class EntitySchema:
    """
    Entity types for extraction.

    Each entity type represents a node label in the Neo4j graph.
    """

    # Entity type names
    TENDER: str = "Tender"
    AGENCY: str = "Agency"
    SUPPLIER: str = "Supplier"
    CATEGORY: str = "Category"
    REQUIREMENT: str = "Requirement"
    KEYWORD: str = "Keyword"
    DATE: str = "Date"

    def get_all_types(self) -> list[str]:
        """Return all entity types as a list."""
        return [
            self.TENDER,
            self.AGENCY,
            self.SUPPLIER,
            self.CATEGORY,
            self.REQUIREMENT,
            self.KEYWORD,
            self.DATE,
        ]

    def validate_type(self, entity_type: str) -> bool:
        """Check if an entity type is valid."""
        return entity_type in self.get_all_types()


@dataclass
class RelationSchema:
    """
    Relationship types for extraction.

    Each relationship type represents an edge type in the Neo4j graph.
    """

    # Relationship type names
    PUBLISHED_BY: str = "PUBLISHED_BY"
    AWARDED_TO: str = "AWARDED_TO"
    IN_CATEGORY: str = "IN_CATEGORY"
    HAS_REQUIREMENT: str = "HAS_REQUIREMENT"
    HAS_KEYWORD: str = "HAS_KEYWORD"
    HAS_DEADLINE: str = "HAS_DEADLINE"
    MENTIONS: str = "MENTIONS"  # Chunk -> Entity traceability

    def get_all_types(self) -> list[str]:
        """Return all relationship types as a list."""
        return [
            self.PUBLISHED_BY,
            self.AWARDED_TO,
            self.IN_CATEGORY,
            self.HAS_REQUIREMENT,
            self.HAS_KEYWORD,
            self.HAS_DEADLINE,
            self.MENTIONS,
        ]

    def validate_type(self, relation_type: str) -> bool:
        """Check if a relationship type is valid."""
        return relation_type in self.get_all_types()


@dataclass
class RelationshipDefinition:
    """
    Defines the structure of a relationship between entity types.
    """

    relation_type: str
    source_entity: str
    target_entity: str
    required: bool = True  # Whether this relationship is required or optional


@dataclass
class ExtractionSchema:
    """
    Complete extraction schema with entity and relationship definitions.

    This schema is used by the GLiNER2 extractor to identify and extract
    entities and relationships from tender chunk text.
    """

    version: str = SCHEMA_VERSION
    entities: EntitySchema = field(default_factory=EntitySchema)
    relations: RelationSchema = field(default_factory=RelationSchema)

    # Define expected relationships between entities
    relationship_definitions: list[RelationshipDefinition] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Initialize relationship definitions."""
        if not self.relationship_definitions:
            self.relationship_definitions = [
                RelationshipDefinition(
                    relation_type=self.relations.PUBLISHED_BY,
                    source_entity=self.entities.TENDER,
                    target_entity=self.entities.AGENCY,
                    required=True,
                ),
                RelationshipDefinition(
                    relation_type=self.relations.AWARDED_TO,
                    source_entity=self.entities.TENDER,
                    target_entity=self.entities.SUPPLIER,
                    required=True,
                ),
                RelationshipDefinition(
                    relation_type=self.relations.IN_CATEGORY,
                    source_entity=self.entities.TENDER,
                    target_entity=self.entities.CATEGORY,
                    required=False,
                ),
                RelationshipDefinition(
                    relation_type=self.relations.HAS_REQUIREMENT,
                    source_entity=self.entities.TENDER,
                    target_entity=self.entities.REQUIREMENT,
                    required=False,
                ),
                RelationshipDefinition(
                    relation_type=self.relations.HAS_KEYWORD,
                    source_entity=self.entities.TENDER,
                    target_entity=self.entities.KEYWORD,
                    required=False,
                ),
                RelationshipDefinition(
                    relation_type=self.relations.HAS_DEADLINE,
                    source_entity=self.entities.TENDER,
                    target_entity=self.entities.DATE,
                    required=False,
                ),
            ]

    def validate(self) -> bool:
        """
        Validate the schema integrity.

        Returns:
            True if schema is valid, raises ValueError otherwise
        """
        # Check entity types are unique
        entity_types = self.entities.get_all_types()
        if len(entity_types) != len(set(entity_types)):
            raise ValueError("Duplicate entity types found in schema")

        # Check relation types are unique
        relation_types = self.relations.get_all_types()
        if len(relation_types) != len(set(relation_types)):
            raise ValueError("Duplicate relation types found in schema")

        # Validate relationship definitions
        for rel_def in self.relationship_definitions:
            if not self.relations.validate_type(rel_def.relation_type):
                raise ValueError(f"Invalid relation type: {rel_def.relation_type}")
            if not self.entities.validate_type(rel_def.source_entity):
                raise ValueError(f"Invalid source entity: {rel_def.source_entity}")
            if not self.entities.validate_type(rel_def.target_entity):
                raise ValueError(f"Invalid target entity: {rel_def.target_entity}")

        return True

    def get_entity_labels_for_gliner(self) -> list[str]:
        """
        Get entity labels formatted for GLiNER2 extraction.

        Returns:
            List of entity type labels
        """
        return self.entities.get_all_types()

    def get_relation_definition(self, relation_type: str) -> RelationshipDefinition | None:
        """
        Get the definition for a specific relationship type.

        Args:
            relation_type: The relationship type to look up

        Returns:
            RelationshipDefinition if found, None otherwise
        """
        for rel_def in self.relationship_definitions:
            if rel_def.relation_type == relation_type:
                return rel_def
        return None


# Global schema instance
def get_schema() -> ExtractionSchema:
    """
    Get the global extraction schema instance.

    Returns:
        Validated extraction schema
    """
    schema = ExtractionSchema()
    schema.validate()
    return schema


# Convenience function for getting entity labels
def get_entity_labels() -> list[str]:
    """
    Get all entity labels for extraction.

    Returns:
        List of entity type labels
    """
    return get_schema().get_entity_labels_for_gliner()
