import pandas as pd
import plotly.graph_objects as go

from .colors import COLORS


def make_pages_chart(df: pd.DataFrame, window: int) -> go.Figure:
    fig = go.Figure()
    for i, (username, udf) in enumerate(df.groupby("username")):
        color = COLORS[i % len(COLORS)]
        udf = udf.sort_values("date")
        roll_col = udf["est_pages_read"].rolling(window, min_periods=1).mean()
        book_labels = udf["book_titles"].apply(lambda titles: "<br>".join(f"• {t}" for t in titles))
        customdata = list(zip(udf["est_pages_read"], book_labels))
        fig.add_trace(
            go.Scatter(
                x=udf["date"], y=roll_col,
                name=username,
                line=dict(color=color, width=2),
                mode="lines",
                customdata=customdata,
                hovertemplate=(
                    f"<b>{username}</b> — %{{x|%d %b %Y}}<br>"
                    "%{customdata[0]:.1f} pages<br>"
                    "%{customdata[1]}<extra></extra>"
                ),
            )
        )
    fig.update_layout(
        title="Estimated Pages Read Per Day",
        xaxis_title="Date",
        yaxis_title="Pages",
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="closest",
    )
    return fig
