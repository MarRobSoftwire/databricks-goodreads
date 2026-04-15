import os
import time
import json

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

GOLD_TABLE       = os.environ.get("TABLE_NAME",       "goodreads.gold_pages_per_day")
GOLD_GENRE_TABLE = os.environ.get("GENRE_TABLE_NAME", "goodreads.gold_genre")

_TIMEOUT_SECONDS = 300  # 5 min — enough for a cold warehouse start


def _execute_and_fetch(sdk: WorkspaceClient, statement: str) -> pd.DataFrame:
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]

    response = sdk.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",  # returns inline for warm warehouses
    )
    statement_id = response.statement_id
    state = response.status.state

    if state not in (StatementState.SUCCEEDED, StatementState.PENDING, StatementState.RUNNING):
        raise RuntimeError(f"Query failed immediately: {response.status.error}")

    if state != StatementState.SUCCEEDED:
        deadline = time.time() + _TIMEOUT_SECONDS
        while time.time() < deadline:
            status = sdk.statement_execution.get_statement(statement_id)
            state = status.status.state
            if state == StatementState.SUCCEEDED:
                response = status
                break
            if state in (StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
                raise RuntimeError(f"Query failed: {status.status.error}")
            time.sleep(3)
        else:
            sdk.statement_execution.cancel_execution(statement_id)
            raise TimeoutError("Query timed out after 5 minutes")

    columns = [c.name for c in response.manifest.schema.columns]
    rows = response.result.data_array or []
    return pd.DataFrame(rows, columns=columns)


def load_pages_data(sdk: WorkspaceClient) -> pd.DataFrame:
    df = _execute_and_fetch(
        sdk,
        f"SELECT username, date, est_pages_read, size(books_in_progress) AS books_in_progress, "
        f"books_in_progress AS book_titles FROM {GOLD_TABLE} ORDER BY username, date",
    )
    df["date"] = pd.to_datetime(df["date"])
    df["est_pages_read"] = df["est_pages_read"].astype(float)
    df["books_in_progress"] = df["books_in_progress"].astype(int)
    df["book_titles"] = df["book_titles"].apply(lambda x: json.loads(x) if x else [])
    return df


def load_genre_data(sdk: WorkspaceClient) -> pd.DataFrame:
    df = _execute_and_fetch(
        sdk,
        f"SELECT username, genre, avg_user_rating, total_pages, book_count "
        f"FROM {GOLD_GENRE_TABLE} ORDER BY username, genre",
    )
    df["avg_user_rating"] = df["avg_user_rating"].astype(float)
    df["total_pages"]     = df["total_pages"].astype(int)
    df["book_count"]      = df["book_count"].astype(int)
    return df
