"""Configuration module for the arXiv paper processing application.

This module centralizes all configuration variables following the DRY principle.
It separates concerns by grouping related configurations together.
"""

from airflow.models import Variable
from typing import Optional


class DatabaseConfig:
    """Database configuration settings."""

    DEFAULT_DB_ID = "default_db_postgres"

    @classmethod
    def get_connection_id(cls, conn_id: Optional[str] = None) -> str:
        """Get database connection ID with fallback to default."""
        return conn_id or cls.DEFAULT_DB_ID


class AIConfig:
    """AI API configuration settings."""

    MODEL = "gpt-4o-mini"
    MAX_TOKENS = 500
    TEMPERATURE = 0.7

    @classmethod
    def get_api_key(cls) -> str:
        """Get OPENAI API key from Airflow Variables.

        Returns:
            str: OPENAI API key

        Raises:
            KeyError: If OPENAI_API_KEY variable is not found
        """
        try:
            return Variable.get("OPENAI_API_KEY")
        except Exception as e:
            raise KeyError(f"OPENAI_API_KEY not found in Airflow Variables: {e}")

    @classmethod
    def get_model_config(cls) -> dict:
        """Get complete model configuration.

        Returns:
            dict: Model configuration parameters
        """
        return {
            "model": cls.MODEL,
            "max_tokens": cls.MAX_TOKENS,
            "temperature": cls.TEMPERATURE,
        }


class ArxivConfig:
    """ArXiv API configuration settings."""

    BASE_URL = "http://export.arxiv.org/api/query"
    DEFAULT_MAX_RESULTS = 10
    DEFAULT_START = 0
    SORT_BY = "submittedDate"
    SORT_ORDER = "descending"

    @classmethod
    def get_api_url(
        cls, search_query: str = "all", start: int = None, max_results: int = None
    ) -> str:
        """Build ArXiv API URL with parameters.

        Args:
            search_query: Search query string
            start: Starting index for results
            max_results: Maximum number of results

        Returns:
            str: Complete API URL
        """
        start = start or cls.DEFAULT_START
        max_results = max_results or cls.DEFAULT_MAX_RESULTS

        return (
            f"{cls.BASE_URL}?search_query={search_query}&start={start}"
            f"&max_results={max_results}&sortBy={cls.SORT_BY}&sortOrder={cls.SORT_ORDER}"
        )


class ProcessingConfig:
    """Data processing configuration settings."""

    TRANSLATION_BATCH_SIZE = 15
    ANALYSIS_BATCH_SIZE = 20

    # Research field mapping for arXiv categories
    FIELD_MAPPING = {
        "cs.": "Computer Science",
        "math.": "Mathematics",
        "physics.": "Physics",
        "q-bio.": "Quantitative Biology",
        "stat.": "Statistics",
        "econ.": "Economics",
        "q-fin.": "Quantitative Finance",
    }

    # Default research field
    DEFAULT_RESEARCH_FIELD = "General"

    @classmethod
    def map_research_field(cls, arxiv_category: str) -> str:
        """Map arXiv category to research field.

        Args:
            arxiv_category: ArXiv category string

        Returns:
            str: Mapped research field
        """
        for prefix, field in cls.FIELD_MAPPING.items():
            if arxiv_category.startswith(prefix):
                return field
        return cls.DEFAULT_RESEARCH_FIELD
