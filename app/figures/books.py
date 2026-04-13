import pandas as pd
import plotly.graph_objects as go

_COLORS = ["#e07b39", "#3d9e6e", "#6398da", "#a855c4", "#e05c5c"]


def make_books_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    for i, (username, udf) in enumerate(df.groupby("username")):
        color = _COLORS[i % len(_COLORS)]
        r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
        fig.add_trace(
            go.Scatter(
                x=udf["date"], y=udf["books_in_progress"],
                name=username,
                fill="tozeroy",
                line=dict(color=color, width=2),
                fillcolor=f"rgba({r}, {g}, {b}, 0.2)",
                customdata=udf["book_titles"].apply(lambda titles: "<br>".join(f"• {t}" for t in titles)),
                hovertemplate="<b>%{y} book(s)</b><br>%{customdata}<extra></extra>",
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
