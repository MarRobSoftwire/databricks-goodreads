"""
Goodreads — Pages Per Day Dashboard
Databricks App that visualises the gold_pages_per_day table using Dash + Plotly.
"""

import os
import diskcache
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import pandas as pd
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

_sdk = WorkspaceClient()  # auto-configured by the Databricks App runtime

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
GOLD_TABLE = os.environ.get("TABLE_NAME", "goodreads.gold_pages_per_day")

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_data() -> pd.DataFrame:
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]

    response = _sdk.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=f"SELECT date, est_pages_read, size(books_in_progress) AS books_in_progress, books_in_progress AS book_titles FROM {GOLD_TABLE} ORDER BY date",
        wait_timeout="0s",  # return immediately so we can poll with our own timeout
    )
    statement_id = response.statement_id

    deadline = time.time() + 300  # 5 min — enough for a cold warehouse start
    while time.time() < deadline:
        status = _sdk.statement_execution.get_statement(statement_id)
        state = status.status.state
        if state == StatementState.SUCCEEDED:
            break
        if state in (StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
            raise RuntimeError(f"Query failed: {status.status.error}")
        time.sleep(3)
    else:
        _sdk.statement_execution.cancel_execution(statement_id)
        raise TimeoutError("Query timed out after 5 minutes")

    columns = [c.name for c in status.manifest.schema.columns]
    rows = status.result.data_array or []
    df = pd.DataFrame(rows, columns=columns)
    import json
    df["date"] = pd.to_datetime(df["date"])
    df["est_pages_read"] = df["est_pages_read"].astype(float)
    df["books_in_progress"] = df["books_in_progress"].astype(int)
    df["book_titles"] = df["book_titles"].apply(lambda x: json.loads(x) if x else [])
    return df


# ---------------------------------------------------------------------------
# App layout
# ---------------------------------------------------------------------------
cache = diskcache.Cache("./cache")
background_callback_manager = dash.DiskcacheManager(cache)

app = dash.Dash(__name__, background_callback_manager=background_callback_manager)
app.title = "Goodreads - Pages Per Day"

app.layout = html.Div(
    style={"fontFamily": "Arial, sans-serif", "maxWidth": "1200px", "margin": "0 auto", "padding": "24px"},
    children=[
        html.H1("📚 Goodreads - Estimated Pages Read Per Day", style={"marginBottom": "4px"}),
        html.P(
            f"Source: {GOLD_TABLE}",
            style={"color": "#666", "fontSize": "13px", "marginTop": "0"},
        ),

        html.Div(
            style={"display": "flex", "gap": "12px", "marginBottom": "16px", "flexWrap": "wrap"},
            children=[
                html.Div(
                    style={"flex": "1", "minWidth": "180px"},
                    children=[
                        html.Label("Date range", style={"fontWeight": "bold", "fontSize": "13px"}),
                        dcc.DatePickerRange(
                            id="date-range",
                            display_format="YYYY-MM-DD",
                        ),
                    ],
                ),
                html.Div(
                    style={"flex": "1", "minWidth": "200px"},
                    children=[
                        html.Label("Rolling average (days)", style={"fontWeight": "bold", "fontSize": "13px"}),
                        dcc.Slider(
                            id="rolling-window",
                            min=1, max=30, step=1, value=7,
                            marks={1: "1", 7: "7", 14: "14", 30: "30"},
                            tooltip={"placement": "bottom", "always_visible": True},
                        ),
                    ],
                ),
            ],
        ),

        html.Div(
            style={"marginBottom": "16px"},
            children=[
                html.Button(
                    "Re-ingest data",
                    id="reingest-button",
                    n_clicks=0,
                    style={"padding": "6px 16px", "cursor": "pointer"},
                ),
                html.Div(id="job-status", style={"marginTop": "8px", "fontSize": "13px", "color": "#555"}),
            ],
        ),

        html.Div(id="loading-message", style={"fontSize": "13px", "marginBottom": "8px", "color": "#888"}),
        html.Div(id="error-message", style={"fontSize": "13px", "marginBottom": "8px", "color": "#c0392b"}),

        dcc.Graph(id="pages-chart", style={"height": "480px"}),
        dcc.Graph(id="books-chart", style={"height": "300px"}),

        # Hidden store holds the full dataset after initial load
        dcc.Store(id="store-data"),
        dcc.Interval(id="refresh-interval", interval=5 * 60 * 1000, n_intervals=0),  # refresh every 5 min
        # Job triggering
        dcc.Store(id="run-id-store"),
        dcc.Interval(id="job-poll-interval", interval=5000, disabled=True),
    ],
)


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------
@app.callback(
    Output("store-data", "data"),
    Output("date-range", "min_date_allowed"),
    Output("date-range", "max_date_allowed"),
    Output("date-range", "start_date"),
    Output("date-range", "end_date"),
    Output("error-message", "children"),
    Input("refresh-interval", "n_intervals"),
    background=True,
    running=[
        (Output("loading-message", "children"), "⏳ Starting warehouse and loading data…", ""),
    ],
)
def refresh_data(_n):
    try:
        df = load_data()
        min_date = df["date"].min().date().isoformat()
        max_date = df["date"].max().date().isoformat()
        return df.to_json(date_format="iso", orient="split"), min_date, max_date, min_date, max_date, ""
    except Exception as e:
        return None, None, None, None, None, f"Error loading data: {e}"


@app.callback(
    Output("pages-chart", "figure"),
    Output("books-chart", "figure"),
    Input("store-data", "data"),
    Input("date-range", "start_date"),
    Input("date-range", "end_date"),
    Input("rolling-window", "value"),
)
def update_charts(json_data, start_date, end_date, window):
    if json_data is None:
        empty = go.Figure()
        empty.update_layout(template="plotly_white")
        return empty, empty

    df = pd.read_json(json_data, orient="split")
    df["date"] = pd.to_datetime(df["date"])

    if start_date:
        df = df[df["date"] >= start_date]
    if end_date:
        df = df[df["date"] <= end_date]

    df = df.sort_values("date")
    roll_col = df["est_pages_read"].rolling(window, min_periods=1).mean()

    # ── Pages per day chart ─────────────────────────────────────────────────
    book_labels = df["book_titles"].apply(lambda titles: "<br>".join(f"• {t}" for t in titles))

    pages_fig = go.Figure()
    pages_fig.add_trace(
        go.Bar(
            x=df["date"], y=df["est_pages_read"],
            name="Daily pages",
            marker_color="rgba(99, 152, 218, 0.5)",
            customdata=book_labels,
            hovertemplate="<b>%{y:.1f} pages</b><br>%{customdata}<extra></extra>",
        )
    )
    pages_fig.add_trace(
        go.Scatter(
            x=df["date"], y=roll_col,
            name=f"{window}-day avg",
            line=dict(color="#1a56a4", width=2),
            mode="lines",
            hoverinfo="skip",
        )
    )
    pages_fig.update_layout(
        title="Estimated Pages Read Per Day",
        xaxis_title="Date",
        yaxis_title="Pages",
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="closest",
    )

    # ── Books in progress chart ─────────────────────────────────────────────
    books_fig = go.Figure()
    books_fig.add_trace(
        go.Scatter(
            x=df["date"], y=df["books_in_progress"],
            name="Books in progress",
            fill="tozeroy",
            line=dict(color="#e07b39", width=2),
            fillcolor="rgba(224, 123, 57, 0.2)",
            customdata=df["book_titles"].apply(lambda titles: "<br>".join(f"• {t}" for t in titles)),
            hovertemplate="<b>%{y} book(s)</b><br>%{customdata}<extra></extra>",
        )
    )
    books_fig.update_layout(
        title="Books Being Read Concurrently",
        xaxis_title="Date",
        yaxis_title="# books",
        template="plotly_white",
        hovermode="closest",
        yaxis=dict(dtick=1),
    )

    return pages_fig, books_fig


@app.callback(
    Output("run-id-store", "data"),
    Output("job-poll-interval", "disabled", allow_duplicate=True),
    Output("reingest-button", "disabled"),
    Input("reingest-button", "n_clicks"),
    prevent_initial_call=True,
    background=True,
    running=[
        (Output("job-status", "children"), "Submitting job…", ""),
        (Output("reingest-button", "disabled"), True, False),
    ],
)
def trigger_reingest(n_clicks):
    if not n_clicks:
        return dash.no_update, dash.no_update, dash.no_update
    waiter = _sdk.jobs.run_now(job_id=int(os.environ["JOB_ID"]))
    return waiter.run_id, False, False


_TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
_STATE_LABELS = {
    "PENDING": "⏳",
    "RUNNING": "🔄",
    "TERMINATED": "✅",
    "SKIPPED": "⏭",
    "INTERNAL_ERROR": "❌",
    "BLOCKED": "🔒",
    "WAITING_FOR_RETRY": "🔁",
}


@app.callback(
    Output("job-status", "children"),
    Output("job-poll-interval", "disabled"),
    Output("run-id-store", "data", allow_duplicate=True),
    Input("job-poll-interval", "n_intervals"),
    dash.dependencies.State("run-id-store", "data"),
    prevent_initial_call=True,
)
def poll_job_status(_n, run_id):
    if run_id is None:
        return dash.no_update, True, dash.no_update
    try:
        run = _sdk.jobs.get_run(run_id=run_id)
    except Exception as exc:
        return f"Error polling job: {exc}", True, None

    overall = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else "PENDING"
    tasks = run.tasks or []
    task_lines = []
    for t in tasks:
        state = t.state.life_cycle_state.value if t.state and t.state.life_cycle_state else "PENDING"
        icon = _STATE_LABELS.get(state, state)
        task_lines.append(f"{icon} {t.task_key}: {state}")

    if overall in _TERMINAL_STATES:
        result = run.state.result_state.value if run.state and run.state.result_state else ""
        summary = f"Job {result or overall}" + (f" — {run.state.state_message}" if run.state and run.state.state_message else "")
        status_text = summary + (" | " + "  ·  ".join(task_lines) if task_lines else "")
        return status_text, True, None

    status_text = f"Job {overall}" + (" | " + "  ·  ".join(task_lines) if task_lines else "")
    return status_text, False, run_id


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 8050)))
