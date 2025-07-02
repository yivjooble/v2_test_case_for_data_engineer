from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
import re
import json
from typing import List, Dict, Any
import feedparser
import pandas as pd
from datetime import datetime

from utility_hub import logger, call_openai_api

from schedule import schedule_arxiv_paper_processing_v3
from arxiv.utils.db_manager import get_db_manager
from arxiv.utils.config import DatabaseConfig, ArxivConfig, ProcessingConfig, AIConfig
from arxiv.utils.queries import (
    get_papers_for_translation,
    get_papers_for_analysis,
    get_total_papers_count,
    get_translated_papers_count,
    get_analyzed_papers_count,
    get_research_field_distribution,
    get_field_word_count_analytics,
    get_word_count_analytics,
)

DB_SCHEMA_NAME = "arxiv"
model_config = AIConfig.get_model_config()
AI_MODEL = model_config["model"]
AI_MAX_TOKENS = model_config["max_tokens"]
AI_TEMPERATURE = model_config["temperature"]
AI_API_KEY = AIConfig.get_api_key()


@dag(
    dag_id="arxiv_paper_processing_v3",
    description=f"Scrape, translate and analyze arXiv papers.\nDB_SCHEMA_NAME: {DB_SCHEMA_NAME}",
    schedule=schedule_arxiv_paper_processing_v3,
    catchup=False,
)
def arxiv_paper_processing_dag():

    create_schema_task = PostgresOperator(
        task_id="create_schema",
        postgres_conn_id=DatabaseConfig.get_connection_id(),
        sql=f"""create schema if not exists {DB_SCHEMA_NAME};""",
    )

    init_tables_task = PostgresOperator(
        task_id="init_tables",
        postgres_conn_id=DatabaseConfig.get_connection_id(),
        sql=f"""
        CREATE TABLE IF NOT EXISTS {DB_SCHEMA_NAME}.arxiv_papers (
            id SERIAL PRIMARY KEY,
            arxiv_id VARCHAR(50) UNIQUE NOT NULL,
            title TEXT NOT NULL,
            abstract TEXT NOT NULL,
            authors TEXT,
            research_field VARCHAR(255),
            published_date DATE,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Create paper_translations table
        CREATE TABLE IF NOT EXISTS {DB_SCHEMA_NAME}.paper_translations (
            id SERIAL PRIMARY KEY,
            paper_id INTEGER NOT NULL,
            original_text TEXT NOT NULL,
            ukrainian_text TEXT NOT NULL,
            translated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (paper_id) REFERENCES {DB_SCHEMA_NAME}.arxiv_papers(id) ON DELETE CASCADE
        );

        -- Create paper_analytics table
        CREATE TABLE IF NOT EXISTS {DB_SCHEMA_NAME}.paper_analytics (
            id SERIAL PRIMARY KEY,
            paper_id INTEGER NOT NULL,
            research_field VARCHAR(255) NOT NULL,
            word_count INTEGER,
            analysis_date TIMESTAMP,
            analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (paper_id) REFERENCES {DB_SCHEMA_NAME}.arxiv_papers(id) ON DELETE CASCADE
        );

        -- Create analytics_reports table
        CREATE TABLE IF NOT EXISTS {DB_SCHEMA_NAME}.analytics_reports (
            id SERIAL PRIMARY KEY,
            generated_at VARCHAR(255),
            total_statistics TEXT,
            field_distribution TEXT,
            field_word_count_analytics TEXT,
            word_count_analytics TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_arxiv_papers_arxiv_id ON {DB_SCHEMA_NAME}.arxiv_papers(arxiv_id);
        CREATE INDEX IF NOT EXISTS idx_arxiv_papers_research_field ON {DB_SCHEMA_NAME}.arxiv_papers(research_field);
        CREATE INDEX IF NOT EXISTS idx_arxiv_papers_published_date ON {DB_SCHEMA_NAME}.arxiv_papers(published_date);
        CREATE INDEX IF NOT EXISTS idx_arxiv_papers_scraped_at ON {DB_SCHEMA_NAME}.arxiv_papers(scraped_at);
        CREATE INDEX IF NOT EXISTS idx_paper_translations_paper_id ON {DB_SCHEMA_NAME}.paper_translations(paper_id);
        CREATE INDEX IF NOT EXISTS idx_paper_analytics_paper_id ON {DB_SCHEMA_NAME}.paper_analytics(paper_id);
        CREATE INDEX IF NOT EXISTS idx_paper_analytics_research_field ON {DB_SCHEMA_NAME}.paper_analytics(research_field);
        """,
    )

    @task(retries=3, retry_delay=30)
    def scrape_arxiv_papers_api() -> List[Dict[str, Any]]:
        """
        Scrape recent papers from arXiv API using feedparser
        """
        logger.info("Starting arXiv paper scraping using official API")

        try:
            # Parse the Atom feed
            feed = feedparser.parse(ArxivConfig.get_api_url())

            if feed.bozo:
                logger.warning(f"Feed parsing warning: {feed.bozo_exception}")

            papers = []
            for entry in feed.entries:
                try:
                    # Extract arXiv ID from entry URL
                    arxiv_id = entry.id.split("/")[-1]

                    # Normalize whitespace in title and abstract
                    title = re.sub(r"\s+", " ", entry.title).strip()
                    abstract = re.sub(r"\s+", " ", entry.summary).strip()

                    authors = (
                        [author.name for author in entry.authors]
                        if hasattr(entry, "authors")
                        else []
                    )

                    # Map arXiv categories to research fields using ProcessingConfig
                    research_field = ProcessingConfig.DEFAULT_RESEARCH_FIELD
                    if hasattr(entry, "tags"):
                        primary_cat = entry.tags[0].term if entry.tags else ""
                        research_field = ProcessingConfig.map_research_field(
                            primary_cat
                        )

                    published_date = (
                        entry.published[:10]
                        if hasattr(entry, "published")
                        else datetime.now().strftime("%Y-%m-%d")
                    )

                    paper = {
                        "arxiv_id": arxiv_id,
                        "title": title,
                        "abstract": abstract,
                        "authors": ", ".join(authors),
                        "research_field": research_field,
                        "published_date": published_date,
                    }
                    papers.append(paper)

                except Exception as e:
                    logger.error(f"Error processing paper entry: {str(e)}")
                    continue

            logger.info(f"Successfully scraped {len(papers)} papers using arXiv API")
            return papers

        except Exception as e:
            logger.error(f"Error scraping arXiv papers: {str(e)}")
            raise

    @task
    def store_papers(papers: List[Dict[str, Any]]) -> int:
        """
        Store scraped papers in PostgreSQL database
        """
        logger.info("Starting paper storage")

        try:
            df = pd.DataFrame(papers)
            rows_saved = get_db_manager().save_dataframe_to_db_psycopg2(
                conn_id=DatabaseConfig.get_connection_id(),
                df=df,
                schema_name=DB_SCHEMA_NAME,
                table_name="arxiv_papers",
                add_timestamp=True,
                timestamp_column="scraped_at",
            )
            logger.info(f"Successfully stored {rows_saved} papers")
            return rows_saved

        except Exception as e:
            logger.error(f"Error in store_papers: {str(e)}")
            raise

    @task(retries=3, retry_delay=30)
    def translate_papers() -> int:
        """
        Translate papers to Ukrainian using OpenAI API
        """
        logger.info("Starting paper translation")

        try:
            papers_df = get_papers_for_translation(
                schema_name=DB_SCHEMA_NAME,
                limit=ProcessingConfig.TRANSLATION_BATCH_SIZE
            )

            if papers_df.empty:
                logger.info("No papers to translate")
                return 0

            translations_data = []

            for _, row in papers_df.iterrows():
                try:
                    text_to_translate = (
                        f"Title: {row['title']}\n\nAbstract: {row['abstract']}"
                    )

                    system_prompt = "You are a professional translator specializing in academic papers. Translate the following academic paper title and abstract to Ukrainian. Maintain academic terminology and formal tone. Provide only the translation without any additional comments."

                    ukrainian_text = call_openai_api(
                        api_key=AI_API_KEY,
                        content=text_to_translate,
                        system_prompt=system_prompt,
                        model=AI_MODEL,
                        max_tokens=AI_MAX_TOKENS,
                        temperature=AI_TEMPERATURE,
                    )

                    if ukrainian_text is None:
                        logger.error(f"Translation failed for paper {row['id']}")
                        continue

                    translations_data.append(
                        {
                            "paper_id": row["id"],
                            "original_text": text_to_translate,
                            "ukrainian_text": ukrainian_text,
                        }
                    )

                    logger.info(f"Successfully translated paper {row['id']}")

                except Exception as e:
                    logger.error(f"Translation failed for paper {row['id']}: {str(e)}")
                    continue

            if translations_data:
                df = pd.DataFrame(translations_data)
                get_db_manager().save_dataframe_to_db_psycopg2(
                    conn_id=DatabaseConfig.get_connection_id(),
                    df=df,
                    schema_name=DB_SCHEMA_NAME,
                    table_name="paper_translations",
                    add_timestamp=True,
                    timestamp_column="translated_at",
                )

            logger.info(f"Successfully translated {len(translations_data)} papers")
            return len(translations_data)

        except Exception as e:
            logger.error(f"Error in translate_papers: {str(e)}")
            raise

    @task(retries=3, retry_delay=30)
    def analyze_papers() -> int:
        """
        Analyze papers using OpenAI API
        """
        logger.info("Starting paper analysis")

        try:
            papers_df = get_papers_for_analysis(
                schema_name=DB_SCHEMA_NAME,
                limit=ProcessingConfig.ANALYSIS_BATCH_SIZE
            )

            if papers_df.empty:
                logger.info("No papers to analyze")
                return 0

            analytics_data = []

            for _, row in papers_df.iterrows():
                try:
                    text_to_analyze = (
                        f"Title: {row['title']}\n\nAbstract: {row['abstract']}"
                    )
                    
                    # Calculate word count from title and abstract
                    word_count = len(row['title'].split()) + len(row['abstract'].split())

                    # Analysis using OpenAI API for research field classification only
                    system_prompt = "You are an expert academic researcher. Analyze the following paper and classify its research field. Respond with only the research field name (e.g., 'Computer Science', 'Physics', 'Biology', etc.)."

                    analysis_result = call_openai_api(
                        api_key=AI_API_KEY,
                        content=text_to_analyze,
                        system_prompt=system_prompt,
                        model=AI_MODEL,
                        max_tokens=AI_MAX_TOKENS,
                        temperature=AI_TEMPERATURE,
                    )

                    if analysis_result is None:
                        logger.error(f"Analysis failed for paper {row['id']}")
                        continue

                    # Extract research field from response
                    research_field = analysis_result.strip() if analysis_result else ProcessingConfig.DEFAULT_RESEARCH_FIELD
                    
                    # Map research field using ProcessingConfig
                    mapped_field = ProcessingConfig.map_research_field(research_field)

                    analytics_data.append(
                        {
                            "paper_id": row["id"],
                            "research_field": mapped_field,
                            "word_count": word_count,
                            "analysis_date": datetime.now().isoformat(),
                        }
                    )

                    logger.info(f"Successfully analyzed paper {row['id']}")

                except Exception as e:
                    logger.error(f"Analysis failed for paper {row['id']}: {str(e)}")
                    continue

            if analytics_data:
                df = pd.DataFrame(analytics_data)
                get_db_manager().save_dataframe_to_db_psycopg2(
                    conn_id=DatabaseConfig.get_connection_id(),
                    df=df,
                    schema_name=DB_SCHEMA_NAME,
                    table_name="paper_analytics",
                    add_timestamp=True,
                    timestamp_column="analyzed_at",
                )

            logger.info(f"Successfully analyzed {len(analytics_data)} papers")
            return len(analytics_data)

        except Exception as e:
            logger.error(f"Error in analyze_papers: {str(e)}")
            raise

    def _serialize_report_data(report_df: pd.DataFrame) -> pd.DataFrame:
        """Helper function to serialize nested dictionaries to JSON strings."""
        json_columns = [
            "field_distribution", "field_word_count_analytics", "word_count_analytics"
        ]
        
        for col in json_columns:
            if col in report_df.columns:
                report_df[col] = report_df[col].apply(json.dumps)
        
        return report_df

    @task
    def generate_analytics_report() -> Dict[str, Any]:
        logger.info("Generating analytics report")

        try:
            total_papers = get_total_papers_count(DB_SCHEMA_NAME)
            translated_papers = get_translated_papers_count(DB_SCHEMA_NAME)
            analyzed_papers = get_analyzed_papers_count(DB_SCHEMA_NAME)
            field_distribution = get_research_field_distribution(DB_SCHEMA_NAME)
            field_word_count_analytics = get_field_word_count_analytics(DB_SCHEMA_NAME)
            word_count_analytics = get_word_count_analytics(DB_SCHEMA_NAME)

            # Create report data
            report = {
                "generated_at": datetime.now().strftime("%Y-%m-%d"),
                "total_statistics": {
                    "total_papers": total_papers,
                    "translated_papers": translated_papers,
                    "analyzed_papers": analyzed_papers,
                    "translation_coverage": (
                        round((translated_papers / total_papers * 100), 2)
                        if total_papers > 0
                        else 0
                    ),
                    "analysis_coverage": (
                        round((analyzed_papers / total_papers * 100), 2)
                        if total_papers > 0
                        else 0
                    ),
                },
                "field_distribution": field_distribution,
                "field_word_count_analytics": field_word_count_analytics,
                "word_count_analytics": word_count_analytics,
            }

            report_df = pd.DataFrame([report])
            
            # Convert nested dictionaries to JSON strings for storage
            report_df = _serialize_report_data(report_df)

            get_db_manager().save_dataframe_to_db_psycopg2(
                conn_id=DatabaseConfig.get_connection_id(),
                df=report_df,
                schema_name=DB_SCHEMA_NAME,
                table_name="analytics_reports",
                add_timestamp=True,
                timestamp_column="generated_at",
            )

            # Output report summary to logs
            logger.info("=== ANALYTICS REPORT ===")
            logger.info(f"Report Date: {report['generated_at']}")
            logger.info(f"Total Papers: {report['total_statistics']['total_papers']}")
            logger.info(
                f"Translated Papers: {report['total_statistics']['translated_papers']}"
            )
            logger.info(
                f"Analyzed Papers: {report['total_statistics']['analyzed_papers']}"
            )
            logger.info(
                f"Translation Coverage: {report['total_statistics']['translation_coverage']}%"
            )
            logger.info(
                f"Analysis Coverage: {report['total_statistics']['analysis_coverage']}%"
            )
            logger.info("Field Distribution:")
            for field, count in report["field_distribution"].items():
                logger.info(f"  {field}: {count} papers")

            logger.info("Word Count Analytics:")
            wc_stats = report['word_count_analytics']
            logger.info(f"  Total Papers: {wc_stats['total_papers']}, Avg: {wc_stats['avg_word_count']}, Range: {wc_stats['min_word_count']}-{wc_stats['max_word_count']}")
            logger.info("Word Count by Field:")
            for field, stats in report["field_word_count_analytics"].items():
                logger.info(f"  {field}: Avg={stats['avg_word_count']}, Papers={stats['paper_count']}")
            logger.info("========================")

            return report

        except Exception as e:
            logger.error(f"Error generating analytics report: {str(e)}")
            raise

    scraped_papers = scrape_arxiv_papers_api()

    stored_papers = store_papers(scraped_papers)
    translated_papers = translate_papers()
    analyzed_papers = analyze_papers()
    final_report = generate_analytics_report()

    create_schema_task >> init_tables_task >> scraped_papers >> stored_papers
    stored_papers >> [translated_papers, analyzed_papers] >> final_report


dag_instance = arxiv_paper_processing_dag()
