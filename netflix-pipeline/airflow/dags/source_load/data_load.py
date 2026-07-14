"""
source_load/data_load.py
========================
Loads raw Netflix CSVs from S3 into Snowflake's DBT_RAW schema.

Design decisions:
- Single connection per run; truncate + load wrapped in one transaction.
- Credentials sourced exclusively from AWS SSM Parameter Store.
- Structured logging throughout — no bare print() calls.
- Type annotations on all public functions.
- Retries handled by Airflow; this module raises on unrecoverable errors.
"""

import logging
import os
from contextlib import contextmanager
from io import BytesIO
from typing import Generator

import boto3
import pandas as pd
import snowflake.connector as snow
from botocore.exceptions import BotoCoreError, ClientError
from snowflake.connector import SnowflakeConnection
from snowflake.connector.pandas_tools import write_pandas

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
S3_BUCKET = os.environ["S3_BUCKET_NAME"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

_RAW_TABLES = {
    "TITLES_RAW": {
        "s3_key": "raw_files/titles.csv",
        "write_table": "TITLES_RAW",
    },
    "CREDITS_RAW": {
        "s3_key": "raw_files/credits.csv",
        "write_table": "CREDITS_RAW",
    },
}


# ---------------------------------------------------------------------------
# Secrets — fetched once at call time, not at module import
# ---------------------------------------------------------------------------
def _get_ssm_parameter(name: str) -> str:
    """Fetch a SecureString from SSM Parameter Store."""
    ssm = boto3.client("ssm", region_name=AWS_REGION)
    try:
        response = ssm.get_parameter(Name=name, WithDecryption=True)
        return response["Parameter"]["Value"]
    except (BotoCoreError, ClientError) as exc:
        logger.error("Failed to retrieve SSM parameter %s: %s", name, exc)
        raise


def _get_snowflake_credentials() -> dict:
    """Return Snowflake connection kwargs sourced from SSM."""
    return {
        "user": _get_ssm_parameter(os.environ["SSM_SF_USERNAME_PATH"]),
        "password": _get_ssm_parameter(os.environ["SSM_SF_PASSWORD_PATH"]),
        "account": _get_ssm_parameter(os.environ["SSM_SF_ACCOUNT_PATH"]),
        "warehouse": os.environ["SF_WAREHOUSE"],
        "database": os.environ["SF_DATABASE"],
        "schema": os.environ["SF_RAW_SCHEMA"],
        "session_parameters": {
            "QUERY_TAG": "airflow-netflix-pipeline",
            "QUOTED_IDENTIFIERS_IGNORE_CASE": "TRUE",
        },
    }


# ---------------------------------------------------------------------------
# Connection context manager
# ---------------------------------------------------------------------------
@contextmanager
def snowflake_connection() -> Generator[SnowflakeConnection, None, None]:
    """
    Yield a Snowflake connection; commit on clean exit, rollback on exception.
    Always closes the connection, even on error.
    """
    conn = snow.connect(**_get_snowflake_credentials())
    logger.info("Snowflake connection established (account=%s)", os.environ.get("SSM_SF_ACCOUNT_PATH"))
    try:
        yield conn
        conn.commit()
        logger.info("Transaction committed")
    except Exception:
        conn.rollback()
        logger.error("Transaction rolled back due to error")
        raise
    finally:
        conn.close()
        logger.info("Snowflake connection closed")


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------
def _read_csv_from_s3(s3_key: str) -> pd.DataFrame:
    """Download a CSV from S3 and return it as a DataFrame."""
    s3 = boto3.client("s3", region_name=AWS_REGION)
    try:
        logger.info("Reading s3://%s/%s", S3_BUCKET, s3_key)
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        df = pd.read_csv(BytesIO(response["Body"].read()))
        logger.info("Read %d rows from %s", len(df), s3_key)
        return df
    except (BotoCoreError, ClientError) as exc:
        logger.error("Failed to read s3://%s/%s: %s", S3_BUCKET, s3_key, exc)
        raise


# ---------------------------------------------------------------------------
# Load logic
# ---------------------------------------------------------------------------
def _truncate_raw_tables(conn: SnowflakeConnection) -> None:
    """Truncate all raw staging tables inside the open transaction."""
    cursor = conn.cursor()
    for table_name in _RAW_TABLES:
        sql = f"TRUNCATE TABLE IF EXISTS {table_name}"
        logger.info("Executing: %s", sql)
        cursor.execute(sql)
    cursor.close()
    logger.info("All raw tables truncated")


def _load_dataframe(conn: SnowflakeConnection, df: pd.DataFrame, table: str) -> None:
    """Write a DataFrame into a Snowflake table using the bulk copy protocol."""
    # Normalise column names to uppercase for Snowflake consistency
    df.columns = [c.upper() for c in df.columns]

    success, num_chunks, num_rows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=table,
        auto_create_table=True,
        overwrite=False,          # table was already truncated above
        quote_identifiers=False,
    )
    if not success:
        raise RuntimeError(f"write_pandas reported failure for table {table}")

    logger.info("Loaded %d rows into %s (%d chunks)", num_rows, table, num_chunks)


# ---------------------------------------------------------------------------
# Public entry point — called by Airflow PythonOperator
# ---------------------------------------------------------------------------
def run_snowflake_load(env: str = "dev", **context) -> None:
    """
    Full EL step: read CSVs from S3, truncate raw tables, load into Snowflake.

    All operations run inside a single transaction so a mid-load failure
    never leaves tables in a partially-loaded state.

    Args:
        env:     Pipeline environment label (dev/staging/prod). Logged for traceability.
        context: Airflow task context (passed by op_kwargs, unused directly).
    """
    logger.info("Starting Snowflake load [env=%s]", env)

    # Read all source files first — fail fast before touching Snowflake
    dataframes: dict[str, pd.DataFrame] = {}
    for key, cfg in _RAW_TABLES.items():
        dataframes[key] = _read_csv_from_s3(cfg["s3_key"])

    with snowflake_connection() as conn:
        _truncate_raw_tables(conn)

        for table_name, cfg in _RAW_TABLES.items():
            df = dataframes[table_name]
            _load_dataframe(conn, df, cfg["write_table"])

    logger.info("Snowflake load complete [env=%s]", env)
