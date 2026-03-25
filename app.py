from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

from src.charts import (
    COLORS,
    coverage_heatmap,
    exceptions_bar,
    payload_distribution,
    productivity_scatter,
    trend_with_target,
    unit_timeline,
)
from src.io_excel import WorkbookData, load_excel_workbook
from src.kpi import (
    FilterState,
    build_kpi_availability_report,
    build_top_area_exceptions,
    build_top_exceptions_excavator,
    build_top_exceptions_truck,
    compute_excavator_kpis,
    compute_overview,
    compute_route_ranking,
    compute_truck_kpis,
    filter_all_facts,
    resolve_target,
    summarize_unit_ranking,
)


DEFAULT_INPUT_PATH = Path("./data/mine_productivity_input.xlsx")
FALLBACK_SAMPLE_PATH = Path("./data/sample_mine_productivity_input.xlsx")


st.set_page_config(
    page_title="Mining Productivity Dashboard",
    page_icon=":bar_chart:",
    layout="wide",
)

st.markdown(
    f"""
<style>
.main {{
    background-color: {COLORS['bg']};
}}
[data-testid="stMetricValue"] {{
    font-size: 1.35rem;
}}
.small-note {{
    color: {COLORS['muted']};
    font-size: 0.85rem;
}}
</style>
""",
    unsafe_allow_html=True,
)


CARD_LABELS = {
    "excavator_tonnes_loaded": "Excavators Tonnes Loaded",
    "excavator_tonnes_per_operating_hour": "Excavator TPH",
    "excavator_availability_pct": "Excavator Availability",
    "truck_tonnes_hauled": "Trucks Tonnes Hauled",
    "truck_tonnes_per_operating_hour": "Truck TPH",
    "truck_availability_pct": "Truck Availability",
    "avg_payload_t": "Average Payload",
    "avg_cycle_time_min": "Average Cycle Time",
    "avg_queue_time_min": "Average Queue Time",
}

CARD_ORDER = [
    "excavator_tonnes_loaded",
    "excavator_tonnes_per_operating_hour",
    "excavator_availability_pct",
    "truck_tonnes_hauled",
    "truck_tonnes_per_operating_hour",
    "truck_availability_pct",
    "avg_payload_t",
    "avg_cycle_time_min",
    "avg_queue_time_min",
]


@st.cache_data(show_spinner=False)
def cached_load_from_path(path_str: str) -> WorkbookData:
    return load_excel_workbook(path_str)


@st.cache_data(show_spinner=False)
def cached_load_from_bytes(content: bytes) -> WorkbookData:
    return load_excel_workbook(content)


@st.cache_data(show_spinner=False)
def cached_compute(
    filtered_exc: pd.DataFrame,
    filtered_trk: pd.DataFrame,
    filtered_route: pd.DataFrame,
    targets: pd.DataFrame,
    site_id: str,
    date_from: Optional[date],
    date_to: Optional[date],
    shift: str,
    area_ids: List[str],
    equipment_ids: List[str],
) -> Dict[str, Any]:
    filters = FilterState(
        site_id=site_id,
        date_from=date_from,
        date_to=date_to,
        shift=shift,
        area_ids=area_ids,
        equipment_ids=equipment_ids,
    )
    overview = compute_overview(filtered_exc, filtered_trk, targets, filters)
    exc_kpi = compute_excavator_kpis(filtered_exc)
    trk_kpi = compute_truck_kpis(filtered_trk)
    route_rank = compute_route_ranking(filtered_route)
    return {
        "overview": overview,
        "exc_kpi": exc_kpi,
        "trk_kpi": trk_kpi,
        "route_rank": route_rank,
    }


def _compact_number(value: float) -> str:
    abs_v = abs(value)
    if abs_v >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs_v >= 1_000:
        return f"{value / 1_000:.1f}k"
    return f"{value:,.1f}"


def format_metric_value(metric: str, value: float) -> str:
    if pd.isna(value):
        return "N/A"
    if metric.endswith("_pct"):
        return f"{value:.1%}"
    if metric.endswith("_min"):
        return f"{value:.1f} min"
    if metric.endswith("_t") or metric.endswith("_hour"):
        return f"{value:,.1f}"
    return _compact_number(value)


def format_metric_delta(metric: str, value: float) -> str:
    if pd.isna(value):
        return "N/A"
    sign = "+" if value >= 0 else ""
    if metric.endswith("_pct"):
        return f"{sign}{value:.1%}"
    if metric.endswith("_min"):
        return f"{sign}{value:.1f} min"
    if metric.endswith("_t") or metric.endswith("_hour"):
        return f"{sign}{value:,.1f}"
    return f"{sign}{_compact_number(value)}"


def render_card(card: Dict[str, Any], container: Any) -> None:
    metric = card["metric"]
    label = CARD_LABELS.get(metric, metric)
    actual = card.get("actual", float("nan"))
    target = card.get("target", float("nan"))
    delta = card.get("delta", float("nan"))

    target_label = format_metric_value(metric, target)
    delta_label = format_metric_delta(metric, delta)

    container.metric(label=label, value=format_metric_value(metric, actual), delta=delta_label)
    container.caption(f"Target: {target_label} | Trend: {card.get('trend_label', 'N/A')}")

    if card.get("status") == "N/A" and card.get("reason"):
        container.markdown(f"<div class='small-note'>Reason: {card['reason']}</div>", unsafe_allow_html=True)


def _get_site_options(sheets: Dict[str, pd.DataFrame]) -> List[str]:
    site_candidates: List[str] = []

    dim_site = sheets.get("dim_site", pd.DataFrame())
    if not dim_site.empty and "site_id" in dim_site.columns:
        site_candidates.extend(dim_site["site_id"].dropna().astype(str).tolist())

    for fact_name in ["fact_shift_excavator", "fact_shift_truck"]:
        fact = sheets.get(fact_name, pd.DataFrame())
        if not fact.empty and "site_id" in fact.columns:
            site_candidates.extend(fact["site_id"].dropna().astype(str).tolist())

    if not site_candidates:
        return ["UNKNOWN_SITE"]

    return sorted(set(site_candidates))


def _site_date_bounds(sheets: Dict[str, pd.DataFrame], site_id: str) -> tuple[Optional[date], Optional[date]]:
    dates: List[date] = []
    for fact_name in ["fact_shift_excavator", "fact_shift_truck"]:
        fact = sheets.get(fact_name, pd.DataFrame())
        if fact.empty or "site_id" not in fact.columns or "date" not in fact.columns:
            continue
        scoped = fact[fact["site_id"] == site_id]
        if scoped.empty:
            continue
        dates.extend(scoped["date"].dropna().tolist())

    if not dates:
        return None, None
    return min(dates), max(dates)


def _safe_dataframe(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=columns)
    missing = [col for col in columns if col not in df.columns]
    out = df.copy()
    for col in missing:
        out[col] = pd.NA
    return out[columns]


def _render_messages(workbook: WorkbookData) -> None:
    for err in workbook.errors:
        st.error(err)
    for warn in workbook.warnings:
        st.warning(warn)


def _build_source_section() -> WorkbookData:
    st.sidebar.header("Data Source")
    source_mode = st.sidebar.radio("Input", ["Default workbook", "Upload workbook"], index=0)

    if source_mode == "Upload workbook":
        upload = st.sidebar.file_uploader("Upload .xlsx", type=["xlsx"])
        if upload is not None:
            return cached_load_from_bytes(upload.getvalue())
        st.sidebar.info("Upload an Excel workbook to replace the default source.")

    if DEFAULT_INPUT_PATH.exists():
        st.sidebar.success(f"Using: {DEFAULT_INPUT_PATH}")
        return cached_load_from_path(str(DEFAULT_INPUT_PATH))

    if FALLBACK_SAMPLE_PATH.exists():
        st.sidebar.warning(
            f"Default path not found ({DEFAULT_INPUT_PATH}). Falling back to sample workbook ({FALLBACK_SAMPLE_PATH})."
        )
        return cached_load_from_path(str(FALLBACK_SAMPLE_PATH))

    st.sidebar.error("No workbook found. Run scripts/make_template.py or upload a file.")
    return WorkbookData(
        sheets={},
        errors=[
            "No input workbook available. Expected ./data/mine_productivity_input.xlsx or ./data/sample_mine_productivity_input.xlsx."
        ],
        warnings=[],
        quality={},
    )


def _build_global_filters(sheets: Dict[str, pd.DataFrame]) -> FilterState:
    st.subheader("Global Filters")

    site_options = _get_site_options(sheets)
    default_site = site_options[0]

    col_site, col_dates, col_shift, col_area, col_equip = st.columns([1.0, 1.5, 0.9, 1.2, 1.4])

    site_id = col_site.selectbox("Site", options=site_options, index=0)
    min_date, max_date = _site_date_bounds(sheets, site_id)

    if min_date is None or max_date is None:
        date_range = col_dates.date_input("Date range", value=())
        date_from, date_to = None, None
    else:
        date_range = col_dates.date_input("Date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
        if isinstance(date_range, tuple) and len(date_range) == 2:
            date_from, date_to = date_range
        else:
            date_from, date_to = min_date, max_date

    shift = col_shift.selectbox("Shift", options=["All", "Day", "Night"], index=0)

    area_options: List[str] = []
    dim_area = sheets.get("dim_area", pd.DataFrame())
    if not dim_area.empty and {"site_id", "area_id"}.issubset(dim_area.columns):
        area_options = sorted(dim_area.loc[dim_area["site_id"] == site_id, "area_id"].dropna().astype(str).unique().tolist())

    selected_areas = col_area.multiselect("Area", options=area_options, default=area_options)

    equipment_options: List[str] = []
    dim_equipment = sheets.get("dim_equipment", pd.DataFrame())
    if not dim_equipment.empty and {"site_id", "equipment_id", "equipment_class"}.issubset(dim_equipment.columns):
        scoped = dim_equipment[
            (dim_equipment["site_id"] == site_id)
            & (dim_equipment["equipment_class"].isin(["excavator", "truck"]))
        ]
        if selected_areas and "area_id" in scoped.columns:
            scoped = scoped[scoped["area_id"].isin(selected_areas)]
        equipment_options = sorted(scoped["equipment_id"].dropna().astype(str).unique().tolist())

    selected_equipment = col_equip.multiselect("Equipment (optional)", options=equipment_options, default=[])

    return FilterState(
        site_id=site_id or default_site,
        date_from=date_from,
        date_to=date_to,
        shift=shift,
        area_ids=selected_areas,
        equipment_ids=selected_equipment,
    )


def render_overview(
    results: Dict[str, Any],
    targets: pd.DataFrame,
    filters: FilterState,
) -> None:
    overview = results["overview"]
    cards = overview["cards"]

    card_map = {card["metric"]: card for card in cards}
    selected_metrics = [metric for metric in CARD_ORDER if metric in card_map]

    # Keep card deck clean with max 8 cards; show remaining driver metric in a compact line.
    shown_metrics = selected_metrics[:8]
    overflow_metrics = selected_metrics[8:]

    st.markdown("### Overview KPI Cards")
    cols = st.columns(4)
    for idx, metric in enumerate(shown_metrics):
        render_card(card_map[metric], cols[idx % 4])

    if overflow_metrics:
        compact_parts = []
        for metric in overflow_metrics:
            card = card_map[metric]
            compact_parts.append(
                f"{CARD_LABELS.get(metric, metric)}: {format_metric_value(metric, card.get('actual', float('nan')))} "
                f"(Target {format_metric_value(metric, card.get('target', float('nan')))}, "
                f"Delta {format_metric_delta(metric, card.get('delta', float('nan')))})"
            )
        st.info(" | ".join(compact_parts))

    st.markdown("### Productivity Trends")
    trend_col1, trend_col2 = st.columns(2)

    exc_daily = overview["exc_daily"]
    trk_daily = overview["trk_daily"]

    exc_target = resolve_target(
        targets,
        filters.site_id,
        "excavator",
        "tonnes_per_operating_hour",
        filters.area_ids,
        filters.date_to,
    )
    trk_target = resolve_target(
        targets,
        filters.site_id,
        "truck",
        "tonnes_per_operating_hour",
        filters.area_ids,
        filters.date_to,
    )

    trend_col1.plotly_chart(
        trend_with_target(
            exc_daily,
            value_col="tonnes_per_operating_hour",
            title="Excavator TPH Trend vs Target",
            y_title="Tonnes/Operating Hour",
            target=exc_target,
        ),
        width="stretch",
    )

    trend_col2.plotly_chart(
        trend_with_target(
            trk_daily,
            value_col="tonnes_per_operating_hour",
            title="Truck TPH Trend vs Target",
            y_title="Tonnes/Operating Hour",
            target=trk_target,
        ),
        width="stretch",
    )

    st.markdown("### Top 5 Problems")
    exc_top = build_top_exceptions_excavator(results["exc_kpi"], top_n=5)
    trk_top = build_top_exceptions_truck(results["trk_kpi"], top_n=5)
    area_top = build_top_area_exceptions(results["trk_kpi"], top_n=5)

    col_a, col_b, col_c = st.columns(3)

    with col_a:
        st.markdown("**Excavators (Worst)**")
        if exc_top.empty:
            st.info("No excavator exception data.")
        else:
            value_col = "tonnes_per_operating_hour" if "tonnes_per_operating_hour" in exc_top.columns else exc_top.columns[1]
            st.plotly_chart(
                exceptions_bar(exc_top, "equipment_id", value_col, "Excavator Exception Ranking"),
                width="stretch",
            )
            st.dataframe(
                _safe_dataframe(exc_top, [c for c in ["equipment_id", "tonnes_per_operating_hour", "availability_pct", "down_h", "reason"] if c in exc_top.columns]),
                width="stretch",
                hide_index=True,
            )

    with col_b:
        st.markdown("**Trucks (Worst)**")
        if trk_top.empty:
            st.info("No truck exception data.")
        else:
            value_col = "tonnes_per_operating_hour" if "tonnes_per_operating_hour" in trk_top.columns else trk_top.columns[1]
            st.plotly_chart(
                exceptions_bar(trk_top, "equipment_id", value_col, "Truck Exception Ranking"),
                width="stretch",
            )
            st.dataframe(
                _safe_dataframe(trk_top, [c for c in ["equipment_id", "tonnes_per_operating_hour", "availability_pct", "cycle_time_min", "queue_time_min", "reason"] if c in trk_top.columns]),
                width="stretch",
                hide_index=True,
            )

    with col_c:
        st.markdown("**Areas (Worst)**")
        if area_top.empty:
            st.info("No area-level exception data.")
        else:
            value_col = "queue_time_min" if "queue_time_min" in area_top.columns else "payload_compliance_pct"
            st.plotly_chart(
                exceptions_bar(area_top, "area_id", value_col, "Area Exception Ranking"),
                width="stretch",
            )
            st.dataframe(
                _safe_dataframe(area_top, [c for c in ["area_id", "queue_time_min", "payload_compliance_pct", "reason"] if c in area_top.columns]),
                width="stretch",
                hide_index=True,
            )


def render_excavators(results: Dict[str, Any], targets: pd.DataFrame, filters: FilterState) -> None:
    st.markdown("### Excavator Productivity")
    exc_kpi = results["exc_kpi"]

    if exc_kpi.empty:
        st.info("No excavator records available for selected filters.")
        return

    trend_df = (
        exc_kpi.groupby("date", as_index=False)["tonnes_per_operating_hour"].mean()
        if {"date", "tonnes_per_operating_hour"}.issubset(exc_kpi.columns)
        else pd.DataFrame(columns=["date", "tonnes_per_operating_hour"])
    )

    target = resolve_target(
        targets,
        filters.site_id,
        "excavator",
        "tonnes_per_operating_hour",
        filters.area_ids,
        filters.date_to,
    )

    st.plotly_chart(
        trend_with_target(
            trend_df,
            value_col="tonnes_per_operating_hour",
            title="Excavator TPH Trend vs Target",
            y_title="Tonnes/Operating Hour",
            target=target,
        ),
        width="stretch",
    )

    rank_table = summarize_unit_ranking(exc_kpi, "excavator")
    table_cols = [
        c
        for c in [
            "equipment_id",
            "tonnes_per_operating_hour",
            "availability_pct",
            "utilization_pct",
            "down_h",
            "cycles_per_hour",
            "avg_cycle_time_s",
            "bucket_fill_factor",
        ]
        if c in rank_table.columns
    ]

    left, right = st.columns([1.2, 1.0])

    with left:
        st.markdown("**Ranking Table (Worst to Best)**")
        st.dataframe(_safe_dataframe(rank_table, table_cols), width="stretch", hide_index=True)

    with right:
        st.markdown("**TPH vs Availability**")
        st.plotly_chart(productivity_scatter(exc_kpi, "Excavator Scatter"), width="stretch")

    units = sorted(exc_kpi["equipment_id"].dropna().astype(str).unique().tolist()) if "equipment_id" in exc_kpi.columns else []
    if units:
        selected_unit = st.selectbox("Per-unit timeline", options=units, index=0, key="exc_unit")
        st.plotly_chart(
            unit_timeline(exc_kpi, selected_unit, f"Excavator {selected_unit} - TPH and Downtime Composition"),
            width="stretch",
        )


def render_trucks(results: Dict[str, Any], targets: pd.DataFrame, filters: FilterState) -> None:
    st.markdown("### Truck Productivity")
    trk_kpi = results["trk_kpi"]

    if trk_kpi.empty:
        st.info("No truck records available for selected filters.")
        return

    trend_df = (
        trk_kpi.groupby("date", as_index=False)["tonnes_per_operating_hour"].mean()
        if {"date", "tonnes_per_operating_hour"}.issubset(trk_kpi.columns)
        else pd.DataFrame(columns=["date", "tonnes_per_operating_hour"])
    )

    target = resolve_target(
        targets,
        filters.site_id,
        "truck",
        "tonnes_per_operating_hour",
        filters.area_ids,
        filters.date_to,
    )

    st.plotly_chart(
        trend_with_target(
            trend_df,
            value_col="tonnes_per_operating_hour",
            title="Truck TPH Trend vs Target",
            y_title="Tonnes/Operating Hour",
            target=target,
        ),
        width="stretch",
    )

    col_1, col_2 = st.columns([1.2, 1.0])

    rank_table = summarize_unit_ranking(trk_kpi, "truck")
    table_cols = [
        c
        for c in [
            "equipment_id",
            "tonnes_per_operating_hour",
            "availability_pct",
            "utilization_pct",
            "avg_payload_t",
            "payload_compliance_pct",
            "cycle_time_min",
            "queue_time_min",
        ]
        if c in rank_table.columns
    ]

    with col_1:
        st.markdown("**Worst Trucks (Ranking)**")
        st.dataframe(_safe_dataframe(rank_table, table_cols), width="stretch", hide_index=True)

    with col_2:
        st.markdown("**Payload Distribution**")
        st.plotly_chart(payload_distribution(trk_kpi, "Truck Payload Distribution"), width="stretch")

    st.markdown("**TPH vs Availability**")
    st.plotly_chart(productivity_scatter(trk_kpi, "Truck Scatter"), width="stretch")

    units = sorted(trk_kpi["equipment_id"].dropna().astype(str).unique().tolist()) if "equipment_id" in trk_kpi.columns else []
    if units:
        selected_unit = st.selectbox("Per-unit timeline", options=units, index=0, key="trk_unit")
        st.plotly_chart(
            unit_timeline(trk_kpi, selected_unit, f"Truck {selected_unit} - TPH and Downtime Composition"),
            width="stretch",
        )

    route_rank = results.get("route_rank", pd.DataFrame())
    if not route_rank.empty:
        st.markdown("**Route Ranking (Optional Sheet)**")
        st.dataframe(route_rank.head(15), width="stretch", hide_index=True)


def render_data_quality(workbook: WorkbookData) -> None:
    st.markdown("### Data Quality & Validation")
    st.markdown(
        "Friendly warnings are shown below; KPI calculations degrade gracefully and display N/A when required columns are missing."
    )

    quality = workbook.quality
    sheets = workbook.sheets

    if workbook.errors:
        st.markdown("**Validation Errors**")
        for msg in workbook.errors:
            st.error(msg)

    if workbook.warnings:
        st.markdown("**Warnings**")
        for msg in workbook.warnings:
            st.warning(msg)

    last_available = quality.get("last_available", pd.DataFrame())
    missing_shifts = quality.get("missing_shifts", pd.DataFrame())
    null_pct = quality.get("null_pct", pd.DataFrame())
    duplicates = quality.get("duplicates", pd.DataFrame())
    anomalies = quality.get("anomalies", pd.DataFrame())
    coverage = quality.get("coverage", pd.DataFrame())

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**Last Available Date by Site**")
        st.dataframe(last_available, width="stretch", hide_index=True)

    with col_b:
        st.markdown("**Missing Shifts by Site/Area**")
        st.dataframe(missing_shifts, width="stretch", hide_index=True)

    st.markdown("**Critical Null % (Required Columns)**")
    st.dataframe(null_pct, width="stretch", hide_index=True)

    col_c, col_d = st.columns(2)
    with col_c:
        st.markdown("**Duplicate Key Checks**")
        st.dataframe(duplicates, width="stretch", hide_index=True)
    with col_d:
        st.markdown("**Anomaly Checks**")
        st.dataframe(anomalies, width="stretch", hide_index=True)

    st.markdown("**Coverage Heatmap (Days vs Shifts)**")
    cov_col1, cov_col2 = st.columns(2)
    with cov_col1:
        st.plotly_chart(
            coverage_heatmap(coverage, "excavator", "Excavator Coverage"),
            width="stretch",
        )
    with cov_col2:
        st.plotly_chart(
            coverage_heatmap(coverage, "truck", "Truck Coverage"),
            width="stretch",
        )

    st.markdown("**KPI Availability and N/A Reasons**")
    st.dataframe(build_kpi_availability_report(sheets), width="stretch", hide_index=True)


def main() -> None:
    st.title("Mining Dispatch Productivity Dashboard")
    st.caption("Focus: Excavators/Shovels and Trucks only. Costs and other domains are intentionally excluded.")

    workbook = _build_source_section()
    _render_messages(workbook)

    sheets = workbook.sheets
    if not sheets:
        st.stop()

    filters = _build_global_filters(sheets)

    filtered = filter_all_facts(sheets, filters)
    targets = sheets.get("targets", pd.DataFrame())

    results = cached_compute(
        filtered_exc=filtered.get("fact_shift_excavator", pd.DataFrame()),
        filtered_trk=filtered.get("fact_shift_truck", pd.DataFrame()),
        filtered_route=filtered.get("fact_shift_truck_route", pd.DataFrame()),
        targets=targets,
        site_id=filters.site_id,
        date_from=filters.date_from,
        date_to=filters.date_to,
        shift=filters.shift,
        area_ids=filters.area_ids or [],
        equipment_ids=filters.equipment_ids or [],
    )

    tabs = st.tabs(["Overview", "Excavators", "Trucks", "Data Quality"])

    with tabs[0]:
        render_overview(results, targets, filters)
    with tabs[1]:
        render_excavators(results, targets, filters)
    with tabs[2]:
        render_trucks(results, targets, filters)
    with tabs[3]:
        render_data_quality(workbook)


if __name__ == "__main__":
    main()
