"""
Goodreads — Pages Per Day Dashboard
Databricks App that visualises the gold_pages_per_day table using Dash + Plotly.
"""

import os

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import pandas as pd
from databricks import sql
from databricks.sdk import WorkspaceClient

_sdk = WorkspaceClient()  # auto-configured by the Databricks App runtime

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
GOLD_TABLE = "goodreads.gold_pages_per_day"

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_data() -> pd.DataFrame:
    with sql.connect(
        server_hostname=_sdk.config.host.lstrip("https://"),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=_sdk.config.token,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT date, est_pages_read, books_in_progress "
                f"FROM {GOLD_TABLE} ORDER BY date"
            )
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(rows, columns=columns)
    df["date"] = pd.to_datetime(df["date"])
    df["est_pages_read"] = df["est_pages_read"].astype(float)
    df["books_in_progress"] = df["books_in_progress"].astype(int)
    return df


# ---------------------------------------------------------------------------
# App layout
# ---------------------------------------------------------------------------
app = dash.Dash(__name__)
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

        dcc.Graph(id="pages-chart", style={"height": "480px"}),
        dcc.Graph(id="books-chart", style={"height": "300px"}),

        # Hidden store holds the full dataset after initial load
        dcc.Store(id="store-data"),
        dcc.Interval(id="refresh-interval", interval=5 * 60 * 1000, n_intervals=0),  # refresh every 5 min
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
    Input("refresh-interval", "n_intervals"),
)
def refresh_data(_n):
    df = load_data()
    min_date = df["date"].min().date().isoformat()
    max_date = df["date"].max().date().isoformat()
    return df.to_json(date_format="iso", orient="split"), min_date, max_date, min_date, max_date


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
    pages_fig = go.Figure()
    pages_fig.add_trace(
        go.Bar(
            x=df["date"], y=df["est_pages_read"],
            name="Daily pages",
            marker_color="rgba(99, 152, 218, 0.5)",
        )
    )
    pages_fig.add_trace(
        go.Scatter(
            x=df["date"], y=roll_col,
            name=f"{window}-day avg",
            line=dict(color="#1a56a4", width=2),
            mode="lines",
        )
    )
    pages_fig.update_layout(
        title="Estimated Pages Read Per Day",
        xaxis_title="Date",
        yaxis_title="Pages",
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
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
        )
    )
    books_fig.update_layout(
        title="Books Being Read Concurrently",
        xaxis_title="Date",
        yaxis_title="# books",
        template="plotly_white",
        hovermode="x unified",
        yaxis=dict(dtick=1),
    )

    return pages_fig, books_fig


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 8050)))
