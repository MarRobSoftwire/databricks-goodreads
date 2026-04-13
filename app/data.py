import os
import time
import json

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

GOLD_TABLE = os.environ.get("TABLE_NAME", "goodreads.gold_pages_per_day")


def load_data(sdk: WorkspaceClient) -> pd.DataFrame:
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]

    response = sdk.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=(
            f"SELECT username, date, est_pages_read, size(books_in_progress) AS books_in_progress, "
            f"books_in_progress AS book_titles FROM {GOLD_TABLE} ORDER BY username, date"
        ),
        wait_timeout="0s",
    )
    statement_id = response.statement_id

    deadline = time.time() + 300  # 5 min — enough for a cold warehouse start
    while time.time() < deadline:
        status = sdk.statement_execution.get_statement(statement_id)
        state = status.status.state
        if state == StatementState.SUCCEEDED:
            break
        if state in (StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
            raise RuntimeError(f"Query failed: {status.status.error}")
        time.sleep(3)
    else:
        sdk.statement_execution.cancel_execution(statement_id)
        raise TimeoutError("Query timed out after 5 minutes")

    columns = [c.name for c in status.manifest.schema.columns]
    rows = status.result.data_array or []
    df = pd.DataFrame(rows, columns=columns)
    df["date"] = pd.to_datetime(df["date"])
    df["est_pages_read"] = df["est_pages_read"].astype(float)
    df["books_in_progress"] = df["books_in_progress"].astype(int)
    df["book_titles"] = df["book_titles"].apply(lambda x: json.loads(x) if x else [])
    return df
