from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.theme import GROUP_COLORS, TOKENS, rgba


COLORS = TOKENS


def _base_layout(
    fig: go.Figure,
    title: str,
    *,
    height: int = 340,
    show_legend: bool = False,
    legend_y: float = 1.08,
) -> go.Figure:
    fig.update_layout(
        title=dict(text=title, x=0.02, xanchor="left", font=dict(size=16, color=TOKENS["ink"])),
        height=height,
        margin=dict(l=78, r=22, t=60, b=46),
        paper_bgcolor=TOKENS["surface"],
        plot_bgcolor=TOKENS["surface"],
        font=dict(color=TOKENS["ink"], size=12),
        hoverlabel=dict(bgcolor=TOKENS["surface"], bordercolor=TOKENS["border"], font=dict(color=TOKENS["ink"], size=12)),
        showlegend=show_legend,
        legend=dict(orientation="h", x=0.0, xanchor="left", y=legend_y, yanchor="bottom", font=dict(size=11)),
        hovermode="x unified",
    )
    fig.update_xaxes(showgrid=False, zeroline=False, tickfont=dict(size=11), automargin=True)
    fig.update_yaxes(showgrid=True, gridcolor=TOKENS["grid"], zeroline=False, tickfont=dict(size=11), automargin=True)
    return fig


def _empty_figure(title: str, height: int = 320, note: str = "No data available") -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(text=note, x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False, font=dict(size=13, color=TOKENS["muted"]))
    return _base_layout(fig, title, height=height)


def _single_period_figure(title: str, label: str, value: float | None, *, height: int = 250, suffix: str = "") -> go.Figure:
    if value is None or pd.isna(value):
        return _empty_figure(title, height=height)
    fig = go.Figure(
        go.Indicator(
            mode="number",
            value=float(value),
            number={"suffix": suffix, "font": {"size": 36, "color": TOKENS["ink"]}},
            title={"text": label, "font": {"size": 14, "color": TOKENS["muted"]}},
        )
    )
    return _base_layout(fig, title, height=height)


def _ordered_series(df: pd.DataFrame, required_cols: list[str]) -> pd.DataFrame:
    if df.empty or any(col not in df.columns for col in required_cols):
        return pd.DataFrame(columns=required_cols)
    series = df[required_cols].dropna(subset=[required_cols[0]]).copy()
    series[required_cols[0]] = pd.to_datetime(series[required_cols[0]], errors="coerce")
    return series.dropna(subset=[required_cols[0]]).sort_values(required_cols[0])


def _line_or_bar(fig: go.Figure, x: pd.Series, y: pd.Series, *, name: str, color: str, mode: str = "auto") -> None:
    point_count = int(y.dropna().shape[0])
    if mode == "bar" or (mode == "auto" and point_count == 1):
        fig.add_trace(go.Bar(x=x, y=y, name=name, marker_color=rgba(color, 0.82)))
        return
    fig.add_trace(
        go.Scatter(
            x=x,
            y=y,
            name=name,
            mode="lines+markers",
            line=dict(color=color, width=2.6),
            marker=dict(size=6),
        )
    )


def sparkline_figure(df: pd.DataFrame, value_col: str, *, color: str = TOKENS["mine"]) -> go.Figure:
    series = _ordered_series(df, ["date", value_col])
    if series.empty:
        fig = go.Figure()
        fig.update_layout(height=68, margin=dict(l=0, r=0, t=0, b=0), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        fig.update_xaxes(visible=False)
        fig.update_yaxes(visible=False)
        return fig
    fig = go.Figure(
        go.Scatter(
            x=series["date"],
            y=series[value_col],
            mode="lines",
            line=dict(color=color, width=2.2),
            fill="tozeroy",
            fillcolor=rgba(color, 0.10),
            hoverinfo="skip",
        )
    )
    fig.update_layout(height=68, margin=dict(l=0, r=0, t=0, b=0), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    return fig


def mine_production_combo(df: pd.DataFrame, title: str) -> go.Figure:
    frame = _ordered_series(df, ["date", "bcm_moved", "ore_mined_t", "stripping_ratio"])
    if frame.empty:
        return _empty_figure(title, height=360)
    if len(frame) == 1:
        return _single_period_figure(title, "BCM moved", frame["bcm_moved"].iloc[0], height=300)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=frame["date"], y=frame["bcm_moved"], name="BCM moved", marker_color=rgba(TOKENS["mine"], 0.85)), secondary_y=False)
    fig.add_trace(go.Bar(x=frame["date"], y=frame["ore_mined_t"], name="Ore mined (t)", marker_color=rgba(TOKENS["ore"], 0.82)), secondary_y=False)
    if frame["stripping_ratio"].notna().sum() >= 2:
        fig.add_trace(go.Scatter(x=frame["date"], y=frame["stripping_ratio"], name="Stripping ratio", mode="lines+markers", line=dict(color=TOKENS["accent"], width=2.4)), secondary_y=True)
        fig.update_yaxes(title_text="Ratio", secondary_y=True)
    fig.update_layout(barmode="group")
    fig.update_yaxes(title_text="BCM / tonnes", secondary_y=False)
    return _base_layout(fig, title, height=360, show_legend=True)


def plant_performance_combo(df: pd.DataFrame, title: str) -> go.Figure:
    frame = _ordered_series(df, ["date", "feed_tonnes", "throughput_tph", "recovery_pct"])
    if frame.empty:
        return _empty_figure(title, height=360)
    if len(frame) == 1:
        return _single_period_figure(title, "Feed tonnes", frame["feed_tonnes"].iloc[0], height=300)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=frame["date"], y=frame["feed_tonnes"], name="Feed tonnes", marker_color=rgba(TOKENS["plant"], 0.82)), secondary_y=False)
    fig.add_trace(go.Scatter(x=frame["date"], y=frame["throughput_tph"], name="Throughput", mode="lines+markers", line=dict(color=TOKENS["ore"], width=2.4)), secondary_y=True)
    if frame["recovery_pct"].notna().sum() >= 2:
        fig.add_trace(go.Scatter(x=frame["date"], y=frame["recovery_pct"], name="Recovery", mode="lines", line=dict(color=TOKENS["recovery"], width=2.2, dash="dash")), secondary_y=True)
    fig.update_yaxes(title_text="Feed tonnes", secondary_y=False)
    fig.update_yaxes(title_text="Throughput / recovery", secondary_y=True)
    fig.update_yaxes(tickformat=".0%", secondary_y=True)
    return _base_layout(fig, title, height=360, show_legend=True)


def group_metric_chart(df: pd.DataFrame, group_name: str, metric_col: str, title: str, y_title: str) -> go.Figure:
    if df.empty or not {"date", "group_name", metric_col}.issubset(df.columns):
        return _empty_figure(title, height=290)
    frame = df[df["group_name"].astype(str) == group_name].copy()
    frame = _ordered_series(frame, ["date", metric_col])
    if frame.empty:
        return _empty_figure(title, height=290)

    color = GROUP_COLORS.get(group_name, TOKENS["accent"])
    fig = go.Figure()
    if len(frame) == 1:
        fig.add_trace(go.Bar(x=frame["date"], y=frame[metric_col], marker_color=rgba(color, 0.82), name=group_name))
    else:
        fig.add_trace(go.Scatter(x=frame["date"], y=frame[metric_col], mode="lines+markers", line=dict(color=color, width=2.6), marker=dict(size=6), fill="tozeroy", fillcolor=rgba(color, 0.08), name=group_name))
    fig.update_yaxes(range=[0, 1], tickformat=".0%", title=y_title)
    return _base_layout(fig, title, height=290, show_legend=False)


def area_contribution_chart(df: pd.DataFrame, title: str) -> go.Figure:
    if df.empty or not {"area_name", "bcm_moved", "ore_mined_t"}.issubset(df.columns):
        return _empty_figure(title, height=340)
    frame = df.copy().sort_values("bcm_moved", ascending=True)
    fig = go.Figure()
    fig.add_trace(go.Bar(y=frame["area_name"], x=frame["bcm_moved"], name="BCM moved", orientation="h", marker_color=rgba(TOKENS["mine"], 0.82)))
    fig.add_trace(go.Bar(y=frame["area_name"], x=frame["ore_mined_t"], name="Ore mined (t)", orientation="h", marker_color=rgba(TOKENS["ore"], 0.82)))
    fig.update_layout(barmode="group")
    return _base_layout(fig, title, height=340, show_legend=True)


def mine_volume_trend(df: pd.DataFrame, title: str) -> go.Figure:
    if df.empty or not {"date", "waste_bcm", "ore_bcm", "ore_mined_t"}.issubset(df.columns):
        return _empty_figure(title, height=360)
    frame = _ordered_series(df, ["date", "waste_bcm", "ore_bcm", "ore_mined_t"])
    if frame.empty:
        return _empty_figure(title, height=360)
    if len(frame) == 1:
        return _single_period_figure(title, "Ore mined (t)", frame["ore_mined_t"].iloc[0], height=300)

    fig = go.Figure()
    for col, label, color in [
        ("waste_bcm", "Waste BCM", TOKENS["diesel"]),
        ("ore_bcm", "Ore BCM", TOKENS["accent"]),
        ("ore_mined_t", "Ore mined (t)", TOKENS["ore"]),
    ]:
        fig.add_trace(go.Scatter(x=frame["date"], y=frame[col], mode="lines+markers", name=label, line=dict(color=color, width=2.3)))
    return _base_layout(fig, title, height=360, show_legend=True)


def stripping_ratio_trend(df: pd.DataFrame, title: str) -> go.Figure:
    frame = _ordered_series(df, ["date", "stripping_ratio"])
    if frame.empty:
        return _empty_figure(title, height=320)
    if len(frame) == 1:
        return _single_period_figure(title, "Stripping ratio", frame["stripping_ratio"].iloc[0], height=280)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=frame["date"], y=frame["stripping_ratio"], mode="lines+markers", name="Daily", line=dict(color=TOKENS["accent"], width=2.4), marker=dict(size=6)))
    if len(frame) >= 3:
        rolling = frame["stripping_ratio"].rolling(7, min_periods=2).mean()
        fig.add_trace(go.Scatter(x=frame["date"], y=rolling, mode="lines", name="7-day average", line=dict(color=TOKENS["mine"], width=2.1, dash="dash")))
    return _base_layout(fig, title, height=320, show_legend=True)


def diesel_stacked_chart(df: pd.DataFrame, title: str) -> go.Figure:
    if df.empty or not {"date", "group_name", "diesel_l"}.issubset(df.columns):
        return _empty_figure(title, height=340)
    frame = df.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.dropna(subset=["date"]).sort_values("date")
    if frame.empty:
        return _empty_figure(title, height=340)
    if frame["date"].nunique() == 1:
        one_day = frame.groupby("group_name", as_index=False)["diesel_l"].sum(min_count=1)
        fig = go.Figure()
        for row in one_day.itertuples(index=False):
            fig.add_trace(go.Bar(x=[row.group_name], y=[row.diesel_l], marker_color=rgba(GROUP_COLORS.get(row.group_name, TOKENS["diesel"]), 0.85), name=row.group_name))
        return _base_layout(fig, title, height=320, show_legend=False)
    fig = go.Figure()
    for group_name in ["Excavators", "Trucks", "Drills", "Support Units"]:
        part = frame[frame["group_name"] == group_name]
        if part.empty:
            continue
        color = GROUP_COLORS.get(group_name, TOKENS["diesel"])
        fig.add_trace(go.Bar(x=part["date"], y=part["diesel_l"], name=group_name, marker_color=rgba(color, 0.85)))
    fig.update_layout(barmode="stack")
    return _base_layout(fig, title, height=340, show_legend=True)


def rank_bar(df: pd.DataFrame, category_col: str, value_col: str, title: str, *, color: str = TOKENS["mine"]) -> go.Figure:
    if df.empty or category_col not in df.columns or value_col not in df.columns:
        return _empty_figure(title, height=330)
    frame = df.copy().sort_values(value_col, ascending=True, na_position="last").tail(10)
    fig = go.Figure(go.Bar(y=frame[category_col], x=frame[value_col], orientation="h", marker_color=rgba(color, 0.85), hovertemplate="%{y}<br>%{x:,.1f}<extra></extra>"))
    return _base_layout(fig, title, height=max(320, 30 * max(8, len(frame))))


def unit_timeline_chart(df: pd.DataFrame, title: str) -> go.Figure:
    if df.empty or "date" not in df.columns:
        return _empty_figure(title, height=350)
    frame = df.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.dropna(subset=["date"]).sort_values("date")
    if frame.empty:
        return _empty_figure(title, height=350)

    if frame["date"].nunique() == 1:
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        if "availability_pct" in frame.columns:
            fig.add_trace(go.Bar(x=["Availability"], y=[frame["availability_pct"].iloc[0]], marker_color=rgba(TOKENS["mine"], 0.82), name="Availability"), secondary_y=False)
        if "utilization_pct" in frame.columns:
            fig.add_trace(go.Bar(x=["Utilization"], y=[frame["utilization_pct"].iloc[0]], marker_color=rgba(TOKENS["accent"], 0.82), name="Utilization"), secondary_y=False)
        if "diesel_l" in frame.columns:
            fig.add_trace(go.Bar(x=["Diesel"], y=[frame["diesel_l"].iloc[0]], marker_color=rgba(TOKENS["diesel"], 0.72), name="Diesel (L)"), secondary_y=True)
        fig.update_yaxes(range=[0, 1], tickformat=".0%", secondary_y=False)
        fig.update_yaxes(title_text="Diesel (L)", secondary_y=True)
        return _base_layout(fig, title, height=330, show_legend=True)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    if "availability_pct" in frame.columns:
        fig.add_trace(go.Scatter(x=frame["date"], y=frame["availability_pct"], name="Availability", mode="lines+markers", line=dict(color=TOKENS["mine"], width=2.4)), secondary_y=False)
    if "utilization_pct" in frame.columns:
        fig.add_trace(go.Scatter(x=frame["date"], y=frame["utilization_pct"], name="Utilization", mode="lines", line=dict(color=TOKENS["accent"], width=2.0, dash="dash")), secondary_y=False)
    if "diesel_l" in frame.columns:
        fig.add_trace(go.Bar(x=frame["date"], y=frame["diesel_l"], name="Diesel (L)", marker_color=rgba(TOKENS["diesel"], 0.35)), secondary_y=True)
    fig.update_yaxes(range=[0, 1], tickformat=".0%", secondary_y=False)
    fig.update_yaxes(title_text="Diesel (L)", secondary_y=True)
    fig.update_layout(barmode="overlay")
    return _base_layout(fig, title, height=360, show_legend=True)


def unit_heatmap(df: pd.DataFrame, value_col: str, title: str) -> go.Figure:
    if df.empty or not {"equipment_id", "date", value_col}.issubset(df.columns):
        return _empty_figure(title, height=420)
    frame = df.copy()
    frame["date_label"] = pd.to_datetime(frame["date"], errors="coerce").dt.strftime("%d %b")
    pivot = frame.pivot_table(index="equipment_id", columns="date_label", values=value_col, aggfunc="mean")
    if pivot.empty:
        return _empty_figure(title, height=420)
    scale = [[0.0, "#F6EFE2"], [0.5, "#D7BE97"], [1.0, TOKENS["mine"]]] if value_col.endswith("_pct") else "YlOrBr"
    fig = px.imshow(pivot, aspect="auto", color_continuous_scale=scale)
    if value_col.endswith("_pct"):
        fig.update_coloraxes(colorbar_tickformat=".0%")
    return _base_layout(fig, title, height=max(390, 26 * max(10, len(pivot.index))))


def plant_feed_throughput_combo(df: pd.DataFrame, title: str) -> go.Figure:
    frame = _ordered_series(df, ["date", "feed_tonnes", "throughput_tph"])
    if frame.empty:
        return _empty_figure(title, height=350)
    if len(frame) == 1:
        return _single_period_figure(title, "Throughput", frame["throughput_tph"].iloc[0], height=300)
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=frame["date"], y=frame["feed_tonnes"], name="Feed tonnes", marker_color=rgba(TOKENS["plant"], 0.82)), secondary_y=False)
    fig.add_trace(go.Scatter(x=frame["date"], y=frame["throughput_tph"], name="Throughput", mode="lines+markers", line=dict(color=TOKENS["ore"], width=2.4)), secondary_y=True)
    fig.update_yaxes(title_text="Feed tonnes", secondary_y=False)
    fig.update_yaxes(title_text="Throughput (t/h)", secondary_y=True)
    return _base_layout(fig, title, height=350, show_legend=True)


def grade_recovery_scatter(df: pd.DataFrame, title: str) -> go.Figure:
    if df.empty or not {"feed_grade_pct", "recovery_pct"}.issubset(df.columns):
        return _empty_figure(title, height=350)
    frame = df.dropna(subset=["feed_grade_pct", "recovery_pct"]).copy()
    if frame.empty:
        return _empty_figure(title, height=350)
    fig = px.scatter(
        frame,
        x="feed_grade_pct",
        y="recovery_pct",
        size="feed_tonnes" if "feed_tonnes" in frame.columns else None,
        color_discrete_sequence=[TOKENS["plant"]],
        hover_data=[col for col in ["date", "feed_tonnes", "throughput_tph", "metal_produced_t"] if col in frame.columns],
    )
    if len(frame) >= 2:
        x_vals = frame["feed_grade_pct"].astype(float).to_numpy()
        y_vals = frame["recovery_pct"].astype(float).to_numpy()
        slope, intercept = np.polyfit(x_vals, y_vals, 1)
        x_line = np.linspace(x_vals.min(), x_vals.max(), 50)
        y_line = intercept + slope * x_line
        fig.add_trace(go.Scatter(x=x_line, y=y_line, mode="lines", name="Trend line", line=dict(color=TOKENS["metal"], width=2.0, dash="dash")))
    fig.update_xaxes(tickformat=".2%", title="Feed grade")
    fig.update_yaxes(tickformat=".0%", title="Recovery", range=[0, 1])
    return _base_layout(fig, title, height=350, show_legend=True)


def metal_production_trend(df: pd.DataFrame, title: str) -> go.Figure:
    frame = _ordered_series(df, ["date", "metal_produced_t", "recovery_pct"])
    if frame.empty:
        return _empty_figure(title, height=340)
    if len(frame) == 1:
        return _single_period_figure(title, "Metal produced", frame["metal_produced_t"].iloc[0], height=300)
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=frame["date"], y=frame["metal_produced_t"], name="Metal produced", marker_color=rgba(TOKENS["metal"], 0.84)), secondary_y=False)
    if frame["recovery_pct"].notna().sum() >= 2:
        fig.add_trace(go.Scatter(x=frame["date"], y=frame["recovery_pct"], name="Recovery", mode="lines+markers", line=dict(color=TOKENS["recovery"], width=2.2)), secondary_y=True)
        fig.update_yaxes(tickformat=".0%", secondary_y=True)
    fig.update_yaxes(title_text="Metal produced (t)", secondary_y=False)
    return _base_layout(fig, title, height=340, show_legend=True)


def downtime_availability_combo(df: pd.DataFrame, title: str) -> go.Figure:
    frame = _ordered_series(df, ["date", "unplanned_downtime_h", "availability_pct"])
    if frame.empty:
        return _empty_figure(title, height=340)
    if len(frame) == 1:
        return _single_period_figure(title, "Downtime", frame["unplanned_downtime_h"].iloc[0], height=300)
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=frame["date"], y=frame["unplanned_downtime_h"], name="Unplanned downtime", marker_color=rgba(TOKENS["bad"], 0.75)), secondary_y=False)
    fig.add_trace(go.Scatter(x=frame["date"], y=frame["availability_pct"], name="Availability", mode="lines+markers", line=dict(color=TOKENS["good"], width=2.4)), secondary_y=True)
    fig.update_yaxes(title_text="Downtime (h)", secondary_y=False)
    fig.update_yaxes(range=[0, 1], tickformat=".0%", secondary_y=True)
    return _base_layout(fig, title, height=340, show_legend=True)


def coverage_heatmap(df: pd.DataFrame, row_col: str, value_col: str, title: str) -> go.Figure:
    if df.empty or not {row_col, "date", value_col}.issubset(df.columns):
        return _empty_figure(title, height=360)
    frame = df.copy()
    frame["date_label"] = pd.to_datetime(frame["date"], errors="coerce").dt.strftime("%d %b")
    pivot = frame.pivot_table(index=row_col, columns="date_label", values=value_col, aggfunc="sum", fill_value=0)
    if pivot.empty:
        return _empty_figure(title, height=360)
    fig = px.imshow(pivot, aspect="auto", color_continuous_scale=[[0.0, "#F7F1E7"], [0.5, "#D9C4A4"], [1.0, TOKENS["mine"]]])
    return _base_layout(fig, title, height=max(340, 26 * max(8, len(pivot.index))))


def issue_severity_chart(df: pd.DataFrame, title: str) -> go.Figure:
    if df.empty or "severity" not in df.columns:
        return _empty_figure(title, height=280, note="No recorded issues")
    counts = df.groupby("severity", as_index=False).size().rename(columns={"size": "count"})
    order = ["error", "warning", "info"]
    counts["severity"] = pd.Categorical(counts["severity"], categories=order, ordered=True)
    counts = counts.sort_values("severity")
    colors = {"error": TOKENS["bad"], "warning": TOKENS["warn"], "info": TOKENS["accent"]}
    fig = go.Figure(go.Bar(x=counts["severity"], y=counts["count"], marker_color=[rgba(colors.get(sev, TOKENS["accent"]), 0.85) for sev in counts["severity"]]))
    return _base_layout(fig, title, height=280)
