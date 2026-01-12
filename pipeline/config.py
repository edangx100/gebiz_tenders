"""
Configuration management for GeBIZ pipeline.
Handles environment variables, data source, and runtime settings.
"""

import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv


@dataclass
class DataConfig:
    """Data source and path configuration."""
    source_url: str = "https://data.gov.sg/api/action/datastore_search"
    resource_id: str = "d_acde1106003906a75c3fa052592f2fcb"
    batch_size: int = 100
    raw_data_dir: Path = Path("data/raw")
    raw_csv_path: Path = Path("data/GovernmentProcurementviaGeBIZ.csv")
    chunks_dir: Path = Path("data/chunks")
    extracted_dir: Path = Path("data/extracted")


@dataclass
class Neo4jConfig:
    """Neo4j connection configuration."""
    uri: str
    username: str
    password: str
    database: str = "neo4j"

    @classmethod
    def from_env(cls) -> "Neo4jConfig":
        """Load Neo4j configuration from environment variables."""
        uri = os.getenv("NEO4J_URI")
        username = os.getenv("NEO4J_USERNAME")
        password = os.getenv("NEO4J_PASSWORD")
        database = os.getenv("NEO4J_DATABASE", "neo4j")

        if not uri:
            raise ValueError("NEO4J_URI environment variable is required")
        if not username:
            raise ValueError("NEO4J_USERNAME environment variable is required")
        if not password:
            raise ValueError("NEO4J_PASSWORD environment variable is required")

        return cls(uri=uri, username=username, password=password, database=database)


@dataclass
class ModelConfig:
    """GLiNER2 model configuration."""
    model_name: str = "urchade/gliner_multi_pii-v1"
    device: str = "cpu"
    batch_size: int = 8
    threshold: float = 0.3

    @classmethod
    def from_env(cls) -> "ModelConfig":
        """Load model configuration from environment variables."""
        model_name = os.getenv("GLINER_MODEL_NAME", cls.model_name)
        device = os.getenv("GLINER_DEVICE", cls.device)
        batch_size = int(os.getenv("GLINER_BATCH_SIZE", cls.batch_size))
        threshold = float(os.getenv("GLINER_THRESHOLD", cls.threshold))

        return cls(
            model_name=model_name,
            device=device,
            batch_size=batch_size,
            threshold=threshold,
        )


@dataclass
class OpenAIConfig:
    """OpenAI API configuration for LLM-powered classification."""
    api_key: str
    model: str = "gpt-4o-mini"
    temperature: float = 0.0
    max_retries: int = 3
    timeout_seconds: float = 30.0

    @classmethod
    def from_env(cls) -> "OpenAIConfig":
        """Load OpenAI configuration from environment variables."""
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")

        model = os.getenv("OPENAI_MODEL", cls.model)
        temperature = float(os.getenv("OPENAI_TEMPERATURE", cls.temperature))
        max_retries = int(os.getenv("OPENAI_MAX_RETRIES", cls.max_retries))
        timeout_seconds = float(os.getenv("OPENAI_TIMEOUT_SECONDS", cls.timeout_seconds))

        return cls(
            api_key=api_key,
            model=model,
            temperature=temperature,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
        )


@dataclass
class Config:
    """Complete pipeline configuration."""
    data: DataConfig
    neo4j: Optional[Neo4jConfig] = None
    model: Optional[ModelConfig] = None
    openai: Optional[OpenAIConfig] = None

    @classmethod
    def load(
        cls,
        require_neo4j: bool = False,
        require_model: bool = False,
        require_openai: bool = False,
    ) -> "Config":
        """
        Load configuration from environment.

        Args:
            require_neo4j: If True, raise error if Neo4j config is missing
            require_model: If True, raise error if model config is missing
            require_openai: If True, raise error if OpenAI config is missing

        Returns:
            Loaded configuration

        Raises:
            ValueError: If required configuration is missing
        """
        # Load environment variables from .env file
        load_dotenv()

        data = DataConfig()

        # Ensure data directories exist
        data.raw_data_dir.mkdir(parents=True, exist_ok=True)
        data.chunks_dir.mkdir(parents=True, exist_ok=True)
        data.extracted_dir.mkdir(parents=True, exist_ok=True)

        neo4j = None
        if require_neo4j:
            neo4j = Neo4jConfig.from_env()

        model = None
        if require_model:
            model = ModelConfig.from_env()

        openai = None
        if require_openai:
            openai = OpenAIConfig.from_env()
        elif os.getenv("OPENAI_API_KEY"):
            # Auto-load OpenAI config when the API key is present.
            openai = OpenAIConfig.from_env()

        return cls(data=data, neo4j=neo4j, model=model, openai=openai)
