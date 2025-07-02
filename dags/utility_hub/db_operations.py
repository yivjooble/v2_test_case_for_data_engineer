import pandas as pd
from typing import Dict, Optional, Literal
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from airflow.hooks.base import BaseHook
import psycopg2
from psycopg2.extras import execute_values
import json
from datetime import datetime

from utility_hub import logger


class DatabaseConnection:
    """Manages database connections using Airflow connection configuration."""

    def __init__(self, conn_id: Optional[str] = None):
        """Initialize the database connection manager.

        Args:
            conn_id: Database connection ID
        """
        self.conn_id = conn_id
        self._engine = None

    def get_psycopg2_connection(self, conn_id: str = None):
        """Get psycopg2 connection from Airflow connection (Airflow 3.0 compatible).
        
        Args:
            conn_id: Database connection ID (uses self.conn_id if not provided)
            
        Returns:
            psycopg2 connection instance
        """
        target_conn_id = conn_id or self.conn_id
        if not target_conn_id:
            raise ValueError("Connection ID must be provided either in constructor or as parameter")
            
        # Use BaseHook.get_connection() instead of ORM access (Airflow 3.0 compatible)
        connection = BaseHook.get_connection(target_conn_id)

        # Create psycopg2 connection
        conn = psycopg2.connect(
            host=connection.host,
            port=connection.port,
            database=connection.schema,
            user=connection.login,
            password=connection.password
        )
        
        logger.info(f"Created psycopg2 connection for {target_conn_id}")
        return conn

    def get_sqlalchemy_connection(self, conn_id: str = None):
        """Get psycopg2 connection using SQLAlchemy engine approach (Airflow 3.0 compatible).
        
        Args:
            conn_id: Database connection ID (uses self.conn_id if not provided)
            
        Returns:
            psycopg2 connection instance
        """
        target_conn_id = conn_id or self.conn_id
        if not target_conn_id:
            raise ValueError("Connection ID must be provided either in constructor or as parameter")
        
        def _get_engine(target_conn_id: str) -> Engine:
            """Get SQLAlchemy engine from Airflow connection (Airflow 3.0 compatible).

            Returns:
                SQLAlchemy engine instance
            """
            if self._engine is None:
                # Use BaseHook.get_connection() instead of ORM access (Airflow 3.0 compatible)
                connection = BaseHook.get_connection(target_conn_id)

                # Build connection string
                conn_str = (
                    f"postgresql://{connection.login}:{connection.password}"
                    f"@{connection.host}:{connection.port}/{connection.schema}"
                )

                self._engine = create_engine(conn_str)
                logger.info(f"Created database engine for connection {target_conn_id}")

            return self._engine
        
        engine = _get_engine(target_conn_id)
            
        # Get raw connection from SQLAlchemy engine
        raw_conn = engine.raw_connection()
        logger.info(f"Created psycopg2 connection via SQLAlchemy for {target_conn_id}")
        return raw_conn


class PandasDataOperations:
    """Handles pandas DataFrame operations for database interactions."""

    def __init__(self, db_connection: DatabaseConnection = None):
        """Initialize the data operations manager.
        
        Args:
            db_connection: DatabaseConnection instance for connection management
        """
        self.db_connection = db_connection

    def save_dataframe_to_db_sqlalchemy(
        self,
        engine,
        df: pd.DataFrame,
        schema_name: str,
        table_name: str,
        if_exists: Literal["fail", "replace", "append"] = "append",
        index: bool = False,
        add_timestamp: bool = False,
        timestamp_column: str = "created_at",
    ) -> int:
        """Save DataFrame to database using pandas to_sql.

        Args:
            df: DataFrame to save
            table_name: Target table name
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            index: Whether to write DataFrame index as a column
            add_timestamp: Whether to add a timestamp column
            timestamp_column: Name of the timestamp column

        Returns:
            Number of rows inserted
        """
        try:

            # Add timestamp if requested
            if add_timestamp:
                df = df.copy()
                df[timestamp_column] = pd.Timestamp.now()

            # Save DataFrame to database
            df.to_sql(
                schema=schema_name,
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=index,
                method="multi",
            )

            logger.info(f"Successfully saved {len(df)} rows to {table_name}")
            return len(df)

        except Exception as e:
            logger.error(f"Error saving DataFrame to {table_name}: {str(e)}")
            raise

    def read_dataframe_from_db_sqlalchemy(
        self, engine, query: str, params: Optional[Dict] = None
    ) -> pd.DataFrame:
        """Read data from database into DataFrame.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            DataFrame with query results
        """
        try:
            df = pd.read_sql(sql=query, con=engine, params=params)

            logger.info(f"Successfully read {len(df)} rows from database")
            return df

        except Exception as e:
            logger.error(f"Error reading from database: {str(e)}")
            raise

    def save_dataframe_to_db_psycopg2(
        self,
        conn_id: str,
        df: pd.DataFrame,
        schema_name: str,
        table_name: str,
        add_timestamp: bool = False,
        timestamp_column: str = "created_at",
    ) -> int:
        """Save DataFrame to database using psycopg2 (Airflow 3.0 compatible).

        Args:
            conn_id: Database connection ID
            df: DataFrame to save
            schema_name: Target schema name
            table_name: Target table name
            add_timestamp: Whether to add a timestamp column
            timestamp_column: Name of the timestamp column

        Returns:
            Number of rows inserted
        """
        conn = None
        try:
            # Get psycopg2 connection
            if self.db_connection:
                conn = self.db_connection.get_psycopg2_connection(conn_id)
            else:
                # Create temporary DatabaseConnection if not provided
                temp_db_conn = DatabaseConnection()
                conn = temp_db_conn.get_psycopg2_connection(conn_id)
            cursor = conn.cursor()

            # Add timestamp if requested
            if add_timestamp:
                df = df.copy()
                df[timestamp_column] = datetime.now()

            # Create table if it doesn't exist
            columns_def = []
            for col in df.columns:
                # Escape column names to handle special characters
                escaped_col = f'"{col}"'
                if df[col].dtype == 'object':
                    columns_def.append(f"{escaped_col} TEXT")
                elif df[col].dtype in ['int64', 'int32']:
                    columns_def.append(f"{escaped_col} INTEGER")
                elif df[col].dtype in ['float64', 'float32']:
                    columns_def.append(f"{escaped_col} REAL")
                elif 'datetime' in str(df[col].dtype):
                    columns_def.append(f"{escaped_col} TIMESTAMP")
                else:
                    columns_def.append(f"{escaped_col} TEXT")

            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                    id SERIAL PRIMARY KEY,
                    {', '.join(columns_def)}
                )
            """
            
            cursor.execute(create_table_sql)

            # Prepare data for insertion
            columns = list(df.columns)
            values = []
            
            for _, row in df.iterrows():
                row_values = []
                for col in columns:
                    value = row[col]
                    if pd.isna(value):
                        row_values.append(None)
                    elif isinstance(value, datetime):
                        row_values.append(value.isoformat())
                    else:
                        row_values.append(str(value))
                values.append(tuple(row_values))

            # Insert data using execute_values for better performance
            escaped_columns = [f'"{col}"' for col in columns]
            insert_sql = f"""
                INSERT INTO {schema_name}.{table_name} ({', '.join(escaped_columns)})
                VALUES %s
            """
            
            execute_values(cursor, insert_sql, values)
            conn.commit()

            logger.info(f"Successfully saved {len(df)} rows to {schema_name}.{table_name} using psycopg2")
            return len(df)

        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Error saving DataFrame to {schema_name}.{table_name} using psycopg2: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()

    def read_dataframe_from_db_psycopg2(
        self, conn_id: str, query: str, params: Optional[Dict] = None
    ) -> pd.DataFrame:
        """Read data from database into DataFrame using psycopg2 (Airflow 3.0 compatible).

        Args:
            conn_id: Database connection ID
            query: SQL query string
            params: Query parameters

        Returns:
            DataFrame with query results
        """
        conn = None
        try:
            # Get psycopg2 connection
            if self.db_connection:
                conn = self.db_connection.get_psycopg2_connection(conn_id)
            else:
                # Create temporary DatabaseConnection if not provided
                temp_db_conn = DatabaseConnection()
                conn = temp_db_conn.get_psycopg2_connection(conn_id)
            
            # Use pandas read_sql with psycopg2 connection
            df = pd.read_sql(sql=query, con=conn, params=params)

            logger.info(f"Successfully read {len(df)} rows from database using psycopg2")
            return df

        except Exception as e:
            logger.error(f"Error reading from database using psycopg2: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()

