import pandas as pd
import plotly.graph_objects as go

_COLORS = ["#e07b39", "#3d9e6e", "#6398da", "#a855c4", "#e05c5c"]

_METRIC_LABELS = {
    "avg_user_rating": "Avg Rating",
    "total_pages":     "Total Pages",
    "book_count":      "Books Read",
}


def make_genre_chart(df: pd.DataFrame, metric: str) -> go.Figure:
    """Radar chart of per-user genre stats.

    Args:
        df: DataFrame with columns username, genre, avg_user_rating,
            total_pages, book_count.
        metric: one of 'avg_user_rating', 'total_pages', 'book_count'.
    """
    genres = sorted(df["genre"].unique())
    if not genres:
        fig = go.Figure()
        fig.update_layout(template="plotly_white")
        return fig

    # Close the polygon by repeating the first genre
    theta = genres + [genres[0]]

    fig = go.Figure()
    for i, (username, udf) in enumerate(df.groupby("username")):
        color = _COLORS[i % len(_COLORS)]
        r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
        genre_values = dict(zip(udf["genre"], udf[metric].astype(float)))
        values = [genre_values.get(genre, 0) for genre in genres]
        values_closed = values + [values[0]]

        fig.add_trace(
            go.Scatterpolar(
                r=values_closed,
                theta=theta,
                fill="toself",
                name=username,
                line=dict(color=color, width=2),
                fillcolor=f"rgba({r},{g},{b},0.15)",
                hovertemplate=(
                    f"<b>{username}</b><br>"
                    "%{theta}: %{r:.1f}<extra></extra>"
                ),
            )
        )

    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, showticklabels=True),
        ),
        title=f"Genre Breakdown — {_METRIC_LABELS.get(metric, metric)}",
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.08, xanchor="right", x=1),
    )
    return fig
