import pandas as pd
import plotly.graph_objects as go


def make_pages_chart(df: pd.DataFrame, window: int) -> go.Figure:
    roll_col = df["est_pages_read"].rolling(window, min_periods=1).mean()
    book_labels = df["book_titles"].apply(lambda titles: "<br>".join(f"• {t}" for t in titles))

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=df["date"], y=df["est_pages_read"],
            name="Daily pages",
            marker_color="rgba(99, 152, 218, 0.5)",
            customdata=book_labels,
            hovertemplate="<b>%{y:.1f} pages</b><br>%{customdata}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df["date"], y=roll_col,
            name=f"{window}-day avg",
            line=dict(color="#1a56a4", width=2),
            mode="lines",
            hoverinfo="skip",
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
