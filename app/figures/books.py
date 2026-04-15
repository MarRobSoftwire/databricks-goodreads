import pandas as pd
import plotly.graph_objects as go

from .colors import COLORS, color_rgba


def make_books_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    for i, (username, udf) in enumerate(df.groupby("username")):
        color = COLORS[i % len(COLORS)]
        fig.add_trace(
            go.Scatter(
                x=udf["date"], y=udf["books_in_progress"],
                name=username,
                fill="tozeroy",
                line=dict(color=color, width=2),
                fillcolor=color_rgba(color, 0.2),
                customdata=udf["book_titles"].apply(lambda titles: "<br>".join(f"• {t}" for t in titles)),
                hovertemplate=f"<b>{username}</b> — %{{x|%d %b %Y}}<br>%{{y}} book(s)<br>%{{customdata}}<extra></extra>",
            )
        )
    fig.update_layout(
        title="Books Being Read Concurrently",
        xaxis_title="Date",
        yaxis_title="# books",
        template="plotly_white",
        hovermode="closest",
        yaxis=dict(dtick=1),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig
