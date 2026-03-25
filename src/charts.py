from __future__ import annotations

from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


COLORS = {
    "bg": "#F5F7FA",
    "panel": "#FFFFFF",
    "text": "#1F2937",
    "muted": "#6B7280",
    "good": "#1B9E77",
    "bad": "#D1495B",
    "neutral": "#4E79A7",
    "target": "#8D99AE",
    "warning": "#F4A261",
}


def _base_layout(fig: go.Figure, title: str, y_title: Optional[str] = None, x_title: Optional[str] = None) -> go.Figure:
    fig.update_layout(
        title=title,
        plot_bgcolor=COLORS["panel"],
        paper_bgcolor=COLORS["panel"],
        font=dict(color=COLORS["text"]),
        margin=dict(l=20, r=20, t=45, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1.0),
    )
    if y_title is not None:
        fig.update_yaxes(title=y_title, gridcolor="#E5E7EB", zeroline=False)
    if x_title is not None:
        fig.update_xaxes(title=x_title, gridcolor="#E5E7EB")
    return fig


def trend_with_target(
    df: pd.DataFrame,
    value_col: str,
    title: str,
    y_title: str,
    target: float = float("nan"),
) -> go.Figure:
    fig = go.Figure()

    if df.empty or "date" not in df.columns or value_col not in df.columns:
        fig = _base_layout(fig, title, y_title=y_title, x_title="Date")
        fig.add_annotation(text="No data available", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    series = df.dropna(subset=["date"]).sort_values("date")
    fig.add_trace(
        go.Scatter(
            x=series["date"],
            y=series[value_col],
            mode="lines+markers",
            name="Actual",
            line=dict(color=COLORS["neutral"], width=2.5),
            marker=dict(size=6),
            customdata=series[[c for c in ["date"] if c in series.columns]],
            hovertemplate="Date: %{x}<br>Value: %{y:.2f}<extra></extra>",
        )
    )

    if pd.notna(target):
        fig.add_trace(
            go.Scatter(
                x=series["date"],
                y=[target] * len(series),
                mode="lines",
                name="Target",
                line=dict(color=COLORS["target"], dash="dash", width=2),
                hovertemplate="Date: %{x}<br>Target: %{y:.2f}<extra></extra>",
            )
        )

        band_low = target * 0.95
        band_high = target * 1.05
        fig.add_trace(
            go.Scatter(
                x=series["date"],
                y=[band_high] * len(series),
                mode="lines",
                line=dict(width=0),
                showlegend=False,
                hoverinfo="skip",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=series["date"],
                y=[band_low] * len(series),
                mode="lines",
                fill="tonexty",
                fillcolor="rgba(141,153,174,0.15)",
                line=dict(width=0),
                name="Target band ±5%",
                hoverinfo="skip",
            )
        )

    return _base_layout(fig, title, y_title=y_title, x_title="Date")


def exceptions_bar(df: pd.DataFrame, category_col: str, value_col: str, title: str) -> go.Figure:
    if df.empty or category_col not in df.columns or value_col not in df.columns:
        fig = go.Figure()
        fig = _base_layout(fig, title)
        fig.add_annotation(text="No exception data", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    fig = px.bar(
        df,
        x=value_col,
        y=category_col,
        orientation="h",
        color=value_col,
        color_continuous_scale=[COLORS["good"], COLORS["warning"], COLORS["bad"]],
        hover_data=[c for c in ["reason"] if c in df.columns],
    )
    fig.update_coloraxes(showscale=False)
    fig.update_traces(hovertemplate="%{y}<br>Value: %{x:.2f}<extra></extra>")
    return _base_layout(fig, title, y_title="", x_title="")


def productivity_scatter(df: pd.DataFrame, title: str) -> go.Figure:
    needed = {"tonnes_per_operating_hour", "availability_pct", "equipment_id"}
    if df.empty or not needed.issubset(df.columns):
        fig = go.Figure()
        fig = _base_layout(fig, title)
        fig.add_annotation(text="No data available", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    fig = px.scatter(
        df,
        x="availability_pct",
        y="tonnes_per_operating_hour",
        hover_name="equipment_id",
        color="area_id" if "area_id" in df.columns else None,
        hover_data=[
            c
            for c in [
                "date",
                "shift",
                "site_id",
                "area_id",
                "equipment_id",
                "tonnes_per_operating_hour",
                "availability_pct",
            ]
            if c in df.columns
        ],
        color_discrete_sequence=[COLORS["neutral"], COLORS["warning"], COLORS["good"], "#76B7B2", "#E15759"],
    )
    fig.update_traces(marker=dict(size=9, opacity=0.75))
    fig.update_xaxes(tickformat=".0%")
    return _base_layout(fig, title, y_title="Tonnes per Operating Hour", x_title="Availability")


def unit_timeline(df: pd.DataFrame, equipment_id: str, title: str) -> go.Figure:
    if df.empty or "equipment_id" not in df.columns:
        fig = go.Figure()
        fig = _base_layout(fig, title)
        fig.add_annotation(text="No unit data available", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    unit_df = df[df["equipment_id"] == equipment_id].copy()
    if unit_df.empty or "date" not in unit_df.columns:
        fig = go.Figure()
        fig = _base_layout(fig, title)
        fig.add_annotation(text="No unit data available", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    unit_df = unit_df.sort_values(["date", "shift"]) if "shift" in unit_df.columns else unit_df.sort_values("date")

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    if "tonnes_per_operating_hour" in unit_df.columns:
        fig.add_trace(
            go.Scatter(
                x=unit_df["date"],
                y=unit_df["tonnes_per_operating_hour"],
                mode="lines+markers",
                name="TPH",
                line=dict(color=COLORS["neutral"], width=2),
                hovertemplate="Date: %{x}<br>TPH: %{y:.2f}<extra></extra>",
            ),
            secondary_y=False,
        )

    for col, label, color in [
        ("down_h", "Down Hours", COLORS["bad"]),
        ("idle_h", "Idle Hours", COLORS["warning"]),
        ("standby_h", "Standby Hours", "#76B7B2"),
    ]:
        if col in unit_df.columns:
            fig.add_trace(
                go.Bar(
                    x=unit_df["date"],
                    y=unit_df[col],
                    name=label,
                    marker_color=color,
                    opacity=0.55,
                    hovertemplate="Date: %{x}<br>" + label + ": %{y:.2f}<extra></extra>",
                ),
                secondary_y=True,
            )

    fig.update_layout(barmode="stack")
    fig = _base_layout(fig, title, x_title="Date")
    fig.update_yaxes(title_text="TPH", secondary_y=False)
    fig.update_yaxes(title_text="Hours", secondary_y=True)
    return fig


def payload_distribution(df: pd.DataFrame, title: str) -> go.Figure:
    if df.empty or "avg_payload_t" not in df.columns:
        fig = go.Figure()
        fig = _base_layout(fig, title)
        fig.add_annotation(text="No payload data", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    clean = df.dropna(subset=["avg_payload_t"])
    if len(clean) >= 25:
        fig = px.histogram(
            clean,
            x="avg_payload_t",
            nbins=25,
            color_discrete_sequence=[COLORS["neutral"]],
            hover_data=[c for c in ["date", "shift", "site_id", "area_id", "equipment_id"] if c in clean.columns],
        )
        fig.update_traces(hovertemplate="Payload: %{x:.2f} t<br>Count: %{y}<extra></extra>")
    else:
        fig = px.box(
            clean,
            y="avg_payload_t",
            points="all",
            color_discrete_sequence=[COLORS["neutral"]],
            hover_data=[c for c in ["date", "shift", "site_id", "area_id", "equipment_id"] if c in clean.columns],
        )
        fig.update_traces(hovertemplate="Payload: %{y:.2f} t<extra></extra>")

    return _base_layout(fig, title, y_title="Payload (t)", x_title="")


def coverage_heatmap(coverage_df: pd.DataFrame, equipment_class: str, title: str) -> go.Figure:
    if coverage_df.empty:
        fig = go.Figure()
        fig = _base_layout(fig, title)
        fig.add_annotation(text="No coverage data", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    subset = coverage_df[coverage_df["equipment_class"] == equipment_class].copy()
    if subset.empty:
        fig = go.Figure()
        fig = _base_layout(fig, title)
        fig.add_annotation(text="No coverage data", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    pivot = subset.pivot_table(index="shift", columns="date", values="records", aggfunc="sum", fill_value=0)
    pivot = pivot.reindex(index=["Day", "Night"], fill_value=0)

    fig = px.imshow(
        pivot,
        aspect="auto",
        color_continuous_scale=[[0.0, "#E5E7EB"], [0.5, COLORS["warning"]], [1.0, COLORS["good"]]],
        labels=dict(x="Date", y="Shift", color="Records"),
    )
    fig.update_traces(hovertemplate="Date: %{x}<br>Shift: %{y}<br>Records: %{z}<extra></extra>")
    return _base_layout(fig, title)
