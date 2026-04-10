import pandas as pd
import plotly.graph_objects as go


def make_books_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
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
    fig.update_layout(
        title="Books Being Read Concurrently",
        xaxis_title="Date",
        yaxis_title="# books",
        template="plotly_white",
        hovermode="closest",
        yaxis=dict(dtick=1),
    )
    return fig
