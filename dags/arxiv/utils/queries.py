"""Database query functions for arXiv paper processing."""

import pandas as pd
from typing import Dict
from .config import ProcessingConfig, DatabaseConfig
from .db_manager import get_db_manager


def get_papers_for_translation(schema_name: str, limit: int = None) -> pd.DataFrame:
    """Get papers that need translation."""
    limit = limit or ProcessingConfig.TRANSLATION_BATCH_SIZE
    query = f"""
    SELECT id, title, abstract FROM {schema_name}.arxiv_papers 
    WHERE id NOT IN (SELECT paper_id FROM {schema_name}.paper_translations)
    ORDER BY scraped_at DESC
    LIMIT %(limit)s;
    """
    return get_db_manager().read_dataframe_from_db_psycopg2(
        conn_id=DatabaseConfig.get_connection_id(), query=query, params={"limit": limit}
    )


def get_papers_for_analysis(schema_name: str, limit: int = None) -> pd.DataFrame:
    """Get papers that need analysis."""
    limit = limit or ProcessingConfig.ANALYSIS_BATCH_SIZE
    query = f"""
    SELECT id, title, abstract, research_field FROM {schema_name}.arxiv_papers 
    WHERE id NOT IN (SELECT paper_id FROM {schema_name}.paper_analytics)
    ORDER BY scraped_at DESC
    LIMIT %(limit)s;
    """
    return get_db_manager().read_dataframe_from_db_psycopg2(
        conn_id=DatabaseConfig.get_connection_id(), query=query, params={"limit": limit}
    )


def get_total_papers_count(schema_name: str) -> int:
    """Get total number of papers."""
    query = f"SELECT COUNT(*) as count FROM {schema_name}.arxiv_papers;"
    result = get_db_manager().read_dataframe_from_db_psycopg2(
        conn_id=DatabaseConfig.get_connection_id(), query=query, params={}
    )
    return int(result.iloc[0]["count"]) if not result.empty else 0


def get_translated_papers_count(schema_name: str) -> int:
    """Get number of translated papers."""
    query = f"SELECT COUNT(*) as count FROM {schema_name}.paper_translations;"
    result = get_db_manager().read_dataframe_from_db_psycopg2(
        conn_id=DatabaseConfig.get_connection_id(), query=query, params={}
    )
    return int(result.iloc[0]["count"]) if not result.empty else 0


def get_analyzed_papers_count(schema_name: str) -> int:
    """Get number of analyzed papers."""
    query = f"SELECT COUNT(*) as count FROM {schema_name}.paper_analytics;"
    result = get_db_manager().read_dataframe_from_db_psycopg2(
        conn_id=DatabaseConfig.get_connection_id(), query=query, params={}
    )
    return int(result.iloc[0]["count"]) if not result.empty else 0


def get_research_field_distribution(schema_name: str) -> Dict[str, int]:
    """Get distribution of research papers by field."""
    query = f"""
    SELECT research_field, COUNT(*) as count
    FROM {schema_name}.paper_analytics
    GROUP BY research_field
    ORDER BY count DESC;
    """
    result = get_db_manager().read_dataframe_from_db_psycopg2(
        conn_id=DatabaseConfig.get_connection_id(), query=query, params={}
    )
    return (
        dict(zip(result["research_field"], result["count"])) if not result.empty else {}
    )


def get_field_word_count_analytics(schema_name: str) -> Dict[str, Dict[str, float]]:
    """Get average word count per research field category."""
    query = f"""
    SELECT research_field, 
           ROUND(AVG(COALESCE(word_count, 0)), 2) as avg_word_count,
           COUNT(*) as paper_count
    FROM {schema_name}.paper_analytics
    WHERE word_count IS NOT NULL
    GROUP BY research_field
    ORDER BY avg_word_count DESC;
    """
    result = get_db_manager().read_dataframe_from_db_psycopg2(
        conn_id=DatabaseConfig.get_connection_id(), query=query, params={}
    )

    field_stats = {}
    for _, row in result.iterrows():
        # Handle potential None values from database
        avg_word_count = row["avg_word_count"]
        if avg_word_count is None:
            avg_word_count = 0.0
        
        field_stats[row["research_field"]] = {
            "avg_word_count": float(avg_word_count),
            "paper_count": int(row["paper_count"]),
        }

    return field_stats


def get_word_count_analytics(schema_name: str) -> Dict[str, int]:
    """Get word count analytics for all analyzed papers."""
    query = f"""
    SELECT 
        COUNT(*) as total_papers,
        ROUND(AVG(COALESCE(word_count, 0)), 2) as avg_word_count,
        MIN(COALESCE(word_count, 0)) as min_word_count,
        MAX(COALESCE(word_count, 0)) as max_word_count
    FROM {schema_name}.paper_analytics
    WHERE word_count IS NOT NULL;
    """
    result = get_db_manager().read_dataframe_from_db_psycopg2(
        conn_id=DatabaseConfig.get_connection_id(), query=query, params={}
    )
    
    if result.empty:
        return {
            "total_papers": 0,
            "avg_word_count": 0,
            "min_word_count": 0,
            "max_word_count": 0
        }
    
    row = result.iloc[0]
    
    # Handle potential None values from database
    avg_word_count = row["avg_word_count"]
    min_word_count = row["min_word_count"]
    max_word_count = row["max_word_count"]
    
    return {
        "total_papers": int(row["total_papers"]),
        "avg_word_count": float(avg_word_count) if avg_word_count is not None else 0.0,
        "min_word_count": int(min_word_count) if min_word_count is not None else 0,
        "max_word_count": int(max_word_count) if max_word_count is not None else 0
    }
