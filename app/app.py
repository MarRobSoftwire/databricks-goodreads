"""
Goodreads — Pages Per Day Dashboard
Databricks App that visualises the gold_pages_per_day table using Dash + Plotly.
"""

import os

import dash
import diskcache
import pandas as pd
import plotly.graph_objects as go
from dash import dcc, html
from dash.dependencies import Input, Output
from databricks.sdk import WorkspaceClient

from data import GOLD_GENRE_TABLE, GOLD_TABLE, load_pages_data, load_genre_data
from figures import make_books_chart, make_genre_chart, make_pages_chart
from job_status import format_run_status

_sdk = WorkspaceClient()  # auto-configured by the Databricks App runtime

# ---------------------------------------------------------------------------
# App layout
# ---------------------------------------------------------------------------
cache = diskcache.Cache("/tmp/cache")
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
                            disabled=True,
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
                            disabled=True,
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
                dcc.ConfirmDialog(
                    id="reingest-confirm",
                    message="This will trigger a full re-ingest job on Databricks. Continue?",
                ),
                html.Div(id="job-status", style={"marginTop": "8px", "fontSize": "13px", "color": "#555"}),
            ],
        ),

        html.Div(id="loading-message", style={"fontSize": "13px", "marginBottom": "8px", "color": "#888"}),
        html.Div(id="error-message", style={"fontSize": "13px", "marginBottom": "8px", "color": "#c0392b"}),

        dcc.Graph(id="pages-chart", style={"height": "480px"}),
        dcc.Graph(id="books-chart", style={"height": "300px"}),

        html.Hr(style={"margin": "32px 0"}),
        html.H2("Genre Breakdown", style={"marginBottom": "4px"}),
        html.P(
            f"Source: {GOLD_GENRE_TABLE}",
            style={"color": "#666", "fontSize": "13px", "marginTop": "0", "marginBottom": "16px"},
        ),
        html.Div(
            style={"marginBottom": "16px"},
            children=[
                html.Label("Metric", style={"fontWeight": "bold", "fontSize": "13px", "marginRight": "12px"}),
                dcc.RadioItems(
                    id="genre-metric",
                    options=[
                        {"label": "Avg Rating",  "value": "avg_user_rating"},
                        {"label": "Total Pages", "value": "total_pages"},
                        {"label": "Books Read",  "value": "book_count"},
                    ],
                    value="avg_user_rating",
                    inline=True,
                    inputStyle={"marginRight": "4px"},
                    labelStyle={"marginRight": "16px"},
                ),
            ],
        ),
        html.Div(id="genre-loading-message", style={"fontSize": "13px", "marginBottom": "8px", "color": "#888"}),
        html.Div(id="genre-error-message", style={"fontSize": "13px", "color": "#c0392b", "marginBottom": "8px"}),
        dcc.Graph(id="genre-chart", style={"height": "500px"}),

        dcc.Store(id="store-data"),
        dcc.Store(id="store-genre-data"),
        dcc.Interval(id="refresh-interval", interval=5 * 60 * 1000, n_intervals=0),
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
        (Output("date-range", "disabled"), True, False),
        (Output("rolling-window", "disabled"), True, False),
    ],
)
def refresh_data(_n):
    try:
        df = load_pages_data(_sdk)
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
        df = df[df["date"] >= pd.to_datetime(start_date)]
    if end_date:
        df = df[df["date"] <= pd.to_datetime(end_date)]

    df = df.sort_values("date")
    return make_pages_chart(df, window), make_books_chart(df)


@app.callback(
    Output("reingest-confirm", "displayed"),
    Input("reingest-button", "n_clicks"),
    prevent_initial_call=True,
)
def show_reingest_confirm(_n_clicks):
    return True


@app.callback(
    Output("run-id-store", "data"),
    Output("job-poll-interval", "disabled", allow_duplicate=True),
    Output("reingest-button", "disabled"),
    Input("reingest-confirm", "submit_n_clicks"),
    prevent_initial_call=True,
    background=True,
    running=[
        (Output("job-status", "children"), "Submitting job…", ""),
        (Output("reingest-button", "disabled"), True, False),
    ],
)
def trigger_reingest(_submit_n_clicks):
    waiter = _sdk.jobs.run_now(job_id=int(os.environ["JOB_ID"]))
    return waiter.run_id, False, False


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

    status_text, is_terminal = format_run_status(run)
    return status_text, is_terminal, (None if is_terminal else run_id)


@app.callback(
    Output("store-genre-data", "data"),
    Output("genre-error-message", "children"),
    Input("refresh-interval", "n_intervals"),
    background=True,
    running=[
        (Output("genre-loading-message", "children"), "⏳ Loading genre data…", ""),
    ],
)
def refresh_genre_data(_n):
    try:
        df = load_genre_data(_sdk)
        return df.to_json(orient="split"), ""
    except Exception as e:
        return None, f"Error loading genre data: {e}"


@app.callback(
    Output("genre-chart", "figure"),
    Input("store-genre-data", "data"),
    Input("genre-metric", "value"),
)
def update_genre_chart(json_data, metric):
    if json_data is None:
        fig = go.Figure()
        fig.update_layout(template="plotly_white")
        return fig
    df = pd.read_json(json_data, orient="split")
    return make_genre_chart(df, metric)


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 8050)))
