from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from src.charts import (
    area_contribution_chart,
    coverage_heatmap,
    diesel_stacked_chart,
    downtime_availability_combo,
    grade_recovery_scatter,
    group_metric_chart,
    issue_severity_chart,
    metal_production_trend,
    mine_production_combo,
    mine_volume_trend,
    plant_feed_throughput_combo,
    plant_performance_combo,
    rank_bar,
    sparkline_figure,
    stripping_ratio_trend,
    unit_heatmap,
    unit_timeline_chart,
)
from src.io_excel import WorkbookData, build_field_guide, build_schema_overview, load_excel_workbook
from src.kpi import (
    FilterState,
    MINE_FILTER_GROUP_ORDER,
    build_kpi_availability_report,
    build_unit_heatmap_data,
    build_unit_timeline,
    compute_fleet_page,
    compute_mine_page,
    compute_overview,
    compute_plant_page,
    prepare_site_data,
)
from src.theme import APP_CSS, TOKENS


DEFAULT_INPUT_PATH = Path("./data/mine_productivity_input.xlsx")
FALLBACK_SAMPLE_PATH = Path("./data/sample_mine_productivity_input.xlsx")
TEMPLATE_PATH = Path("./data/mine_productivity_input_template.xlsx")
MINE_CARD_ORDER = ["bcm_moved", "ore_mined_t", "stripping_ratio", "diesel_l"]
PLANT_CARD_ORDER = ["feed_tonnes", "throughput_tph", "recovery_pct", "metal_produced_t"]
AVAILABILITY_GROUP_ORDER = ["Excavators", "Trucks", "Drills", "Ancillary"]


st.set_page_config(page_title="Mining Operations Executive Dashboard", page_icon=":mountain:", layout="wide")
st.markdown(APP_CSS, unsafe_allow_html=True)


METRIC_META = {
    "bcm_moved": {"label": "BCM moved", "kind": "number", "direction": "up"},
    "ore_mined_t": {"label": "Ore mined (t)", "kind": "number", "direction": "up"},
    "stripping_ratio": {"label": "Stripping ratio", "kind": "ratio", "direction": "neutral"},
    "diesel_l": {"label": "Diesel consumption", "kind": "diesel", "direction": "neutral"},
    "feed_tonnes": {"label": "Feed tonnes", "kind": "number", "direction": "up"},
    "throughput_tph": {"label": "Throughput", "kind": "throughput", "direction": "up"},
    "recovery_pct": {"label": "Recovery", "kind": "pct", "direction": "up"},
    "metal_produced_t": {"label": "Metal produced", "kind": "number", "direction": "up"},
    "feed_grade_pct": {"label": "Feed grade", "kind": "pct", "direction": "neutral"},
    "availability_pct": {"label": "Availability", "kind": "pct", "direction": "up"},
    "utilization_pct": {"label": "Utilization", "kind": "pct", "direction": "up"},
    "unplanned_downtime_h": {"label": "Unplanned downtime", "kind": "hours", "direction": "down"},
}

METRIC_COLORS = {
    "bcm_moved": TOKENS["mine"],
    "ore_mined_t": TOKENS["ore"],
    "stripping_ratio": TOKENS["accent"],
    "diesel_l": TOKENS["diesel"],
    "feed_tonnes": TOKENS["plant"],
    "throughput_tph": TOKENS["ore"],
    "recovery_pct": TOKENS["recovery"],
    "metal_produced_t": TOKENS["metal"],
}


@st.cache_data(show_spinner=False)
def cached_load_from_path(path_str: str) -> WorkbookData:
    return load_excel_workbook(path_str)


@st.cache_data(show_spinner=False)
def cached_load_from_bytes(content: bytes) -> WorkbookData:
    return load_excel_workbook(content)


@st.cache_data(show_spinner=False)
def cached_template_bytes(path_str: str) -> bytes:
    return Path(path_str).read_bytes()


@st.cache_data(show_spinner=False)
def cached_schema_overview() -> pd.DataFrame:
    return build_schema_overview()


@st.cache_data(show_spinner=False)
def cached_field_guide() -> pd.DataFrame:
    return build_field_guide()


def _compact_number(value: float) -> str:
    abs_value = abs(value)
    if abs_value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs_value >= 1_000:
        return f"{value / 1_000:.1f}k"
    return f"{value:,.0f}"


def _format_metric(metric: str, value: float) -> str:
    if pd.isna(value):
        return "N/A"
    kind = METRIC_META.get(metric, {}).get("kind", "number")
    if kind == "pct":
        return f"{value:.1%}"
    if kind == "ratio":
        return f"{value:.2f}"
    if kind == "diesel":
        return f"{_compact_number(value)} L"
    if kind == "throughput":
        return f"{value:,.1f} t/h"
    if kind == "hours":
        return f"{value:,.1f} h"
    return _compact_number(value)


def _format_delta(metric: str, delta: float) -> str:
    if pd.isna(delta):
        return "No previous-period comparison"
    sign = "+" if delta >= 0 else ""
    kind = METRIC_META.get(metric, {}).get("kind", "number")
    if kind == "pct":
        return f"{sign}{delta * 100:.1f} pp"
    if kind == "ratio":
        return f"{sign}{delta:.2f}"
    if kind == "diesel":
        return f"{sign}{_compact_number(delta)} L"
    if kind == "throughput":
        return f"{sign}{delta:,.1f} t/h"
    if kind == "hours":
        return f"{sign}{delta:,.1f} h"
    return f"{sign}{_compact_number(delta)}"


def _delta_color(metric: str, delta: float) -> str:
    direction = METRIC_META.get(metric, {}).get("direction", "neutral")
    if pd.isna(delta) or direction == "neutral":
        return TOKENS["muted"]
    if direction == "up":
        return TOKENS["good"] if delta >= 0 else TOKENS["bad"]
    return TOKENS["good"] if delta <= 0 else TOKENS["bad"]


def _delta_chip_class(metric: str, delta: float) -> str:
    direction = METRIC_META.get(metric, {}).get("direction", "neutral")
    if pd.isna(delta) or direction == "neutral":
        return "change-pill-neutral"
    if direction == "up":
        return "change-pill-good" if delta >= 0 else "change-pill-bad"
    return "change-pill-good" if delta <= 0 else "change-pill-bad"


def _format_date_label(value: Optional[date]) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return pd.Timestamp(value).strftime("%b %d, %Y").replace(" 0", " ")


def _range_text(date_from: date, date_to: date) -> str:
    return f"{_format_date_label(date_from)} to {_format_date_label(date_to)}"


def _range_days(date_from: date, date_to: date) -> int:
    return (date_to - date_from).days + 1


def _safe_dataframe(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=columns)
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            out[col] = pd.NA
    return out[columns]


def _default_workbook() -> WorkbookData:
    if DEFAULT_INPUT_PATH.exists():
        return cached_load_from_path(str(DEFAULT_INPUT_PATH))
    if FALLBACK_SAMPLE_PATH.exists():
        return cached_load_from_path(str(FALLBACK_SAMPLE_PATH))
    return WorkbookData(
        sheets={},
        errors=["No workbook found. Generate the template and sample workbook or upload a workbook to continue."],
        warnings=[],
        quality={},
        source_name="",
        schema_version="",
        source_kind="invalid",
    )


def _site_name(metadata: pd.DataFrame) -> str:
    if metadata.empty or "site_name" not in metadata.columns:
        return "Mining Operations"
    return str(metadata["site_name"].iloc[0])


def _plant_name(metadata: pd.DataFrame) -> str:
    if metadata.empty or "plant_name" not in metadata.columns:
        return "Plant"
    return str(metadata["plant_name"].iloc[0])


def _last_refresh(metadata: pd.DataFrame) -> str:
    if metadata.empty or "last_refresh_ts" not in metadata.columns:
        return "Unknown refresh"
    value = pd.to_datetime(metadata["last_refresh_ts"].iloc[0], errors="coerce")
    if pd.isna(value):
        return "Unknown refresh"
    return value.strftime("%d %b %Y %H:%M")


def _date_bounds(site_data: Dict[str, pd.DataFrame]) -> Tuple[Optional[date], Optional[date]]:
    dates: List[date] = []
    for key in ["daily_mine", "daily_plant", "daily_fleet"]:
        df = site_data.get(key, pd.DataFrame())
        if not df.empty and "date" in df.columns:
            dates.extend(pd.to_datetime(df["date"], errors="coerce").dt.date.dropna().tolist())
    if not dates:
        return None, None
    return min(dates), max(dates)


def _health_chip_class(health_summary: pd.DataFrame) -> str:
    if health_summary.empty:
        return "health-chip-warn"
    status = str(health_summary.iloc[0].get("status", "")).lower()
    if "healthy" in status:
        return "health-chip-good"
    if "warning" in status or "usable" in status:
        return "health-chip-warn"
    return "health-chip-bad"


def _render_status_messages(workbook: WorkbookData) -> None:
    for error in workbook.errors:
        st.error(error)
    for warning in workbook.warnings:
        st.warning(warning)


def _apply_range_preset(label: str, min_date: date, max_date: date) -> None:
    if label == "Latest":
        st.session_state["selected_range"] = (max_date, max_date)
        st.rerun()
    if label == "7D":
        start = max(min_date, max_date - pd.Timedelta(days=6).to_pytimedelta())
        st.session_state["selected_range"] = (start, max_date)
        st.rerun()
    if label == "30D":
        start = max(min_date, max_date - pd.Timedelta(days=29).to_pytimedelta())
        st.session_state["selected_range"] = (start, max_date)
        st.rerun()
    if label == "Full":
        st.session_state["selected_range"] = (min_date, max_date)
        st.rerun()


def _render_snapshot_card(card: Dict[str, Any], period_days: int) -> None:
    delta = card.get("delta", float("nan"))
    delta_color = _delta_color(card["metric"], delta)
    sparkline = card.get("sparkline", pd.DataFrame())
    show_sparkline = not sparkline.empty and len(sparkline) >= 2
    st.markdown("<div class='metric-shell'>", unsafe_allow_html=True)
    st.markdown(f"<div class='metric-title'>{card['label']}</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='metric-value'>{_format_metric(card['metric'], card.get('actual', float('nan')))}</div>", unsafe_allow_html=True)
    st.markdown(
        f"<div class='metric-delta' style='color:{delta_color}'>{_format_delta(card['metric'], delta)} vs previous {period_days}-day period</div>",
        unsafe_allow_html=True,
    )
    if card.get("status") == "N/A" and card.get("reason"):
        st.markdown(f"<div class='metric-note'>{card['reason']}</div>", unsafe_allow_html=True)
    else:
        st.markdown(f"<div class='metric-note'>Trend: {card.get('trend_label', 'N/A')}</div>", unsafe_allow_html=True)
    if show_sparkline:
        st.plotly_chart(
            sparkline_figure(sparkline, card["metric"], color=METRIC_COLORS.get(card["metric"], TOKENS["accent"])),
            width="stretch",
            config={"displayModeBar": False},
        )
    st.markdown("</div>", unsafe_allow_html=True)


def _render_snapshot(
    overview: Dict[str, Any],
    site_name: str,
    plant_name: str,
    workbook: WorkbookData,
    date_from: date,
    date_to: date,
    min_date: date,
    max_date: date,
) -> None:
    metadata = workbook.sheets.get("metadata", pd.DataFrame())
    quality_summary = workbook.quality.get("health_summary", pd.DataFrame())
    health_class = _health_chip_class(quality_summary)
    latest_available = workbook.quality.get("last_available", pd.DataFrame())
    latest_label = "Unknown"
    if not latest_available.empty and "last_available_date" in latest_available.columns:
        latest_dt = pd.to_datetime(latest_available["last_available_date"], errors="coerce").max()
        if pd.notna(latest_dt):
            latest_label = latest_dt.strftime("%d %b %Y")

    source_label = "Uploaded workbook" if workbook.source_name == "uploaded_workbook.xlsx" else Path(workbook.source_name).name or "Workbook"
    health_status = "Unknown"
    if not quality_summary.empty:
        health_status = str(quality_summary.iloc[0].get("status", "Unknown"))

    st.markdown("<div class='hero-shell'>", unsafe_allow_html=True)
    title_col, utility_col = st.columns([1.7, 1.0])
    with title_col:
        st.markdown("<div class='hero-title'>Mining Operations Executive Dashboard</div>", unsafe_allow_html=True)
        st.markdown(
            "<div class='hero-subtitle'>A calm, daily operating view across mine, plant, fleet, and data quality for executive review.</div>",
            unsafe_allow_html=True,
        )
        st.markdown("<div class='chip-row'>", unsafe_allow_html=True)
        st.markdown(f"<span class='chip'>Site: {site_name}</span>", unsafe_allow_html=True)
        st.markdown(f"<span class='chip'>Plant: {plant_name}</span>", unsafe_allow_html=True)
        st.markdown(f"<span class='chip'>Latest available: {latest_label}</span>", unsafe_allow_html=True)
        st.markdown(f"<span class='chip {health_class}'>Data quality: {health_status}</span>", unsafe_allow_html=True)
        st.markdown(f"<span class='chip'>Source: {source_label}</span>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    with utility_col:
        uploaded = st.file_uploader("Replace workbook", type=["xlsx"], key="hero_upload")
        if uploaded is not None:
            st.session_state["uploaded_workbook_bytes"] = uploaded.getvalue()
            st.rerun()
        if TEMPLATE_PATH.exists():
            st.download_button(
                "Download official template",
                data=cached_template_bytes(str(TEMPLATE_PATH)),
                file_name=TEMPLATE_PATH.name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                width="stretch",
            )
        st.caption(f"Last import refresh: {_last_refresh(metadata)}")
        st.caption("Use the official template to keep imports compatible and validation calm.")

    if date_from is not None and date_to is not None:
        left, right = st.columns([1.35, 1.0])
        with left:
            preset_cols = st.columns(4)
            preset_labels = ["Latest", "7D", "30D", "Full"]
            for idx, label in enumerate(preset_labels):
                if preset_cols[idx].button(label, key=f"preset_{label}", use_container_width=True):
                    _apply_range_preset(label, min_date, max_date)
            selected = st.date_input(
                "Selected date range",
                value=(date_from, date_to),
                min_value=min_date,
                max_value=max_date,
                key="hero_date_input",
            )
            if isinstance(selected, tuple) and len(selected) == 2:
                current_from, current_to = selected
            else:
                current_from, current_to = date_from, date_to
            st.session_state["selected_range"] = (current_from, current_to)
        with right:
            st.markdown(
                f"<div class='range-banner'><div><div class='range-label'>Selected range</div><div class='range-value'>{_range_text(current_from, current_to)}</div></div><div class='range-days'>{_range_days(current_from, current_to)} days</div></div>",
                unsafe_allow_html=True,
            )
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='readout-shell'>", unsafe_allow_html=True)
    st.markdown("<div class='section-kicker'>Executive readout</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='readout-text'>{overview.get('readout', 'No readout available.')}</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='status-shell'>", unsafe_allow_html=True)
    st.markdown("<div class='section-kicker'>What changed</div>", unsafe_allow_html=True)
    change_strip = overview.get("change_strip", pd.DataFrame())
    if change_strip.empty:
        st.caption("No previous-period deltas are available for the current selection.")
    else:
        for row in change_strip.itertuples(index=False):
            st.markdown(
                f"<span class='change-pill {_delta_chip_class(row.metric, row.delta)}'>{row.label}: {_format_delta(row.metric, row.delta)}</span>",
                unsafe_allow_html=True,
            )
    st.markdown("</div>", unsafe_allow_html=True)

    cards = {card["metric"]: card for card in overview.get("cards", [])}
    st.markdown("<div class='group-heading'>Mine performance</div>", unsafe_allow_html=True)
    mine_cols = st.columns(4)
    for idx, metric in enumerate(MINE_CARD_ORDER):
        if metric in cards:
            with mine_cols[idx]:
                _render_snapshot_card(cards[metric], max(overview.get("period_days", 1), 1))

    st.markdown("<div class='group-heading'>Plant performance</div>", unsafe_allow_html=True)
    plant_cols = st.columns(4)
    for idx, metric in enumerate(PLANT_CARD_ORDER):
        if metric in cards:
            with plant_cols[idx]:
                _render_snapshot_card(cards[metric], max(overview.get("period_days", 1), 1))


def _render_overview(overview: Dict[str, Any]) -> None:
    st.markdown("<div class='subtle-lead'>Start here for the operating picture, then move into Mine, Plant, or Fleet for diagnosis.</div>", unsafe_allow_html=True)
    top_left, top_right = st.columns(2)
    with top_left:
        st.plotly_chart(mine_production_combo(overview.get("mine_daily", pd.DataFrame()), "Movement and ore production"), width="stretch")
    with top_right:
        st.plotly_chart(plant_performance_combo(overview.get("plant_daily", pd.DataFrame()), "Plant operating rhythm"), width="stretch")

    st.markdown("### Fleet availability")
    row_one = st.columns(2)
    row_two = st.columns(2)
    cols = row_one + row_two
    availability = overview.get("availability_groups", pd.DataFrame())
    for idx, group_name in enumerate(AVAILABILITY_GROUP_ORDER):
        with cols[idx]:
            st.plotly_chart(group_metric_chart(availability, group_name, "availability_pct", f"{group_name}", "Availability"), width="stretch")

    st.plotly_chart(area_contribution_chart(overview.get("area_contribution", pd.DataFrame()), "Contribution by cut"), width="stretch")


def _render_mine(site_data: Dict[str, pd.DataFrame], date_from: date, date_to: date) -> None:
    st.markdown("<div class='subtle-lead'>Mine focuses on movement, stripping behaviour, diesel draw, and the unit-level story behind them.</div>", unsafe_allow_html=True)
    fleet_rows = site_data.get("daily_fleet", pd.DataFrame())
    mine_rows = site_data.get("daily_mine", pd.DataFrame())
    area_options = sorted(mine_rows.get("area_name", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
    filter_group_options = [group for group in MINE_FILTER_GROUP_ORDER if group in fleet_rows.get("mine_filter_group", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()]

    filter_cols = st.columns([1.1, 1.0, 1.6])
    selected_areas = filter_cols[0].multiselect("Cuts", options=area_options, default=area_options)
    selected_groups = filter_cols[1].multiselect("Fleet groups", options=filter_group_options, default=filter_group_options)

    equipment_scope = fleet_rows.copy()
    equipment_scope = equipment_scope[(equipment_scope["date"] >= pd.Timestamp(date_from).date()) & (equipment_scope["date"] <= pd.Timestamp(date_to).date())] if not equipment_scope.empty and "date" in equipment_scope.columns else equipment_scope
    if selected_areas and "area_name" in equipment_scope.columns:
        equipment_scope = equipment_scope[equipment_scope["area_name"].isin(selected_areas)]
    if selected_groups and "mine_filter_group" in equipment_scope.columns:
        equipment_scope = equipment_scope[equipment_scope["mine_filter_group"].isin(selected_groups)]
    equipment_options = sorted(equipment_scope.get("equipment_id", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
    selected_units = filter_cols[2].multiselect("Equipment", options=equipment_options, default=[])

    mine_page = compute_mine_page(
        site_data,
        FilterState(date_from=date_from, date_to=date_to),
        area_names=selected_areas,
        equipment_groups=selected_groups,
        equipment_ids=selected_units,
    )

    row_a, row_b = st.columns(2)
    with row_a:
        st.plotly_chart(mine_production_combo(mine_page.get("mine_daily", pd.DataFrame()), "BCM moved, ore mined, and ratio"), width="stretch")
    with row_b:
        st.plotly_chart(mine_volume_trend(mine_page.get("mine_daily", pd.DataFrame()), "Waste, ore BCM, and tonnes"), width="stretch")

    st.plotly_chart(stripping_ratio_trend(mine_page.get("mine_daily", pd.DataFrame()), "Stripping ratio trend"), width="stretch")

    st.markdown("### Fleet utilization")
    util_row_one = st.columns(2)
    util_row_two = st.columns(2)
    util_cols = util_row_one + util_row_two
    utilization_daily = mine_page.get("utilization_daily", pd.DataFrame())
    for idx, group_name in enumerate(AVAILABILITY_GROUP_ORDER):
        with util_cols[idx]:
            st.plotly_chart(group_metric_chart(utilization_daily, group_name, "utilization_pct", group_name, "Utilization"), width="stretch")

    diesel_left, diesel_right = st.columns(2)
    with diesel_left:
        st.plotly_chart(diesel_stacked_chart(mine_page.get("diesel_groups", pd.DataFrame()), "Daily diesel by fleet group"), width="stretch")
    with diesel_right:
        st.plotly_chart(rank_bar(mine_page.get("top_diesel", pd.DataFrame()), "equipment_id", "diesel_l", "Highest diesel units", color=TOKENS["diesel"]), width="stretch")

    st.plotly_chart(rank_bar(mine_page.get("bottom_diesel", pd.DataFrame()), "equipment_id", "diesel_l", "Lowest diesel units", color=TOKENS["plant"]), width="stretch")

    ranking = mine_page.get("unit_ranking", pd.DataFrame())
    ranking_cols = [
        "equipment_id",
        "equipment_class",
        "equipment_subtype",
        "model",
        "area_name",
        "availability_pct",
        "utilization_pct",
        "diesel_l",
        "days_reported",
    ]
    st.markdown("### Unit ranking")
    st.dataframe(_safe_dataframe(ranking, ranking_cols), width="stretch", hide_index=True)

    unit_ids = ranking.get("equipment_id", pd.Series(dtype=str)).dropna().astype(str).tolist()
    if not unit_ids:
        st.info("No unit-level fleet records are available for the current Mine filters.")
        return

    detail_cols = st.columns([1.2, 1.0])
    selected_unit = detail_cols[0].selectbox("Per-unit timeline", options=unit_ids, index=0)
    heat_metric = detail_cols[1].selectbox(
        "Heatmap metric",
        options=["availability_pct", "utilization_pct"],
        format_func=lambda metric: METRIC_META[metric]["label"],
    )
    timeline = build_unit_timeline(mine_page.get("filtered_units", pd.DataFrame()), selected_unit)
    heatmap_df = build_unit_heatmap_data(mine_page.get("filtered_units", pd.DataFrame()), heat_metric)

    timeline_col, heatmap_col = st.columns([1.2, 1.0])
    with timeline_col:
        st.plotly_chart(unit_timeline_chart(timeline, f"{selected_unit} operating profile"), width="stretch")
    with heatmap_col:
        st.plotly_chart(unit_heatmap(heatmap_df, heat_metric, f"Lowest performers by {METRIC_META[heat_metric]['label'].lower()}"), width="stretch")


def _render_plant(site_data: Dict[str, pd.DataFrame], date_from: date, date_to: date) -> None:
    st.markdown("<div class='subtle-lead'>Plant keeps feed, recovery, metal output, and downtime together so the process story stays coherent.</div>", unsafe_allow_html=True)
    plant_page = compute_plant_page(site_data, FilterState(date_from=date_from, date_to=date_to))
    plant_daily = plant_page.get("plant_daily", pd.DataFrame())

    first_row, second_row = st.columns(2), st.columns(2)
    with first_row[0]:
        st.plotly_chart(plant_feed_throughput_combo(plant_daily, "Feed tonnes and throughput"), width="stretch")
    with first_row[1]:
        st.plotly_chart(grade_recovery_scatter(plant_page.get("plant_rows", pd.DataFrame()), "Feed grade versus recovery"), width="stretch")
    with second_row[0]:
        st.plotly_chart(metal_production_trend(plant_daily, "Metal produced and recovery"), width="stretch")
    with second_row[1]:
        st.plotly_chart(downtime_availability_combo(plant_daily, "Downtime and availability"), width="stretch")

    st.markdown("### Daily operating table")
    st.dataframe(
        _safe_dataframe(
            plant_page.get("daily_table", pd.DataFrame()),
            ["date", "feed_tonnes", "feed_grade_pct", "throughput_tph", "recovery_pct", "metal_produced_t", "availability_pct", "unplanned_downtime_h"],
        ),
        width="stretch",
        hide_index=True,
    )


def _render_fleet(site_data: Dict[str, pd.DataFrame], date_from: date, date_to: date) -> None:
    st.markdown("<div class='subtle-lead'>Fleet separates availability from diesel and unit ranking so shortfalls are easier to isolate.</div>", unsafe_allow_html=True)
    fleet_page = compute_fleet_page(site_data, FilterState(date_from=date_from, date_to=date_to))
    availability = fleet_page.get("availability_daily", pd.DataFrame())

    st.markdown("### Fleet availability")
    fleet_row_one = st.columns(2)
    fleet_row_two = st.columns(2)
    fleet_cols = fleet_row_one + fleet_row_two
    for idx, group_name in enumerate(AVAILABILITY_GROUP_ORDER):
        with fleet_cols[idx]:
            st.plotly_chart(group_metric_chart(availability, group_name, "availability_pct", group_name, "Availability"), width="stretch")

    mid_left, mid_right = st.columns(2)
    with mid_left:
        st.plotly_chart(diesel_stacked_chart(fleet_page.get("diesel_groups", pd.DataFrame()), "Diesel mix by fleet grouping"), width="stretch")
    with mid_right:
        ranking = fleet_page.get("unit_ranking", pd.DataFrame())
        st.plotly_chart(rank_bar(ranking.sort_values("availability_pct", ascending=True).head(10), "equipment_id", "availability_pct", "Lowest availability units", color=TOKENS["bad"]), width="stretch")

    ranking = fleet_page.get("unit_ranking", pd.DataFrame())
    st.markdown("### Fleet unit list")
    st.dataframe(
        _safe_dataframe(ranking, ["equipment_id", "equipment_class", "equipment_subtype", "model", "area_name", "availability_pct", "utilization_pct", "diesel_l", "days_reported"]),
        width="stretch",
        hide_index=True,
    )

    unit_ids = ranking.get("equipment_id", pd.Series(dtype=str)).dropna().astype(str).tolist()
    if not unit_ids:
        st.info("No fleet records are available for the selected period.")
        return

    detail_cols = st.columns([1.2, 1.0])
    selected_unit = detail_cols[0].selectbox("Fleet unit detail", options=unit_ids, index=0, key="fleet_unit_select")
    heat_metric = detail_cols[1].selectbox(
        "Fleet heatmap metric",
        options=["availability_pct", "utilization_pct"],
        format_func=lambda metric: METRIC_META[metric]["label"],
        key="fleet_heat_metric",
    )
    timeline = build_unit_timeline(fleet_page.get("fleet_rows", pd.DataFrame()), selected_unit)
    heatmap_df = build_unit_heatmap_data(fleet_page.get("fleet_rows", pd.DataFrame()), heat_metric)

    detail_left, detail_right = st.columns([1.2, 1.0])
    with detail_left:
        st.plotly_chart(unit_timeline_chart(timeline, f"{selected_unit} trend"), width="stretch")
    with detail_right:
        st.plotly_chart(unit_heatmap(heatmap_df, heat_metric, f"Fleet heatmap: {METRIC_META[heat_metric]['label'].lower()}"), width="stretch")


def _render_data_quality(workbook: WorkbookData) -> None:
    st.markdown("<div class='subtle-lead'>Data Quality shows whether the workbook is decision-grade, where it is thin, and what should be fixed next.</div>", unsafe_allow_html=True)
    health_summary = workbook.quality.get("health_summary", pd.DataFrame())
    issues = workbook.quality.get("issues", pd.DataFrame())
    duplicates = workbook.quality.get("duplicates", pd.DataFrame())
    null_pct = workbook.quality.get("null_pct", pd.DataFrame())
    last_available = workbook.quality.get("last_available", pd.DataFrame())
    missing_dates_area = workbook.quality.get("missing_dates_area", pd.DataFrame())
    missing_dates_fleet = workbook.quality.get("missing_dates_fleet", pd.DataFrame())
    coverage_area = workbook.quality.get("coverage_area", pd.DataFrame())
    coverage_fleet = workbook.quality.get("coverage_fleet", pd.DataFrame())

    st.markdown("### Data quality health")
    top_cols = st.columns([0.9, 1.1])
    with top_cols[0]:
        st.dataframe(health_summary, width="stretch", hide_index=True)
    with top_cols[1]:
        st.plotly_chart(issue_severity_chart(issues, "Issue severity"), width="stretch")

    st.markdown("### Issue log")
    if issues.empty:
        st.success("No data quality issues were recorded for this workbook.")
    else:
        st.dataframe(issues, width="stretch", hide_index=True)

    info_left, info_right = st.columns(2)
    with info_left:
        st.markdown("**Last available dates**")
        st.dataframe(last_available, width="stretch", hide_index=True)
        st.markdown("**Duplicate checks**")
        st.dataframe(duplicates, width="stretch", hide_index=True)
    with info_right:
        st.markdown("**Missing dates by cut**")
        st.dataframe(missing_dates_area, width="stretch", hide_index=True)
        st.markdown("**Missing dates by equipment**")
        st.dataframe(missing_dates_fleet.head(25), width="stretch", hide_index=True)

    cov_left, cov_right = st.columns(2)
    with cov_left:
        st.plotly_chart(coverage_heatmap(coverage_area, "area_name", "records", "Coverage by cut"), width="stretch")
    with cov_right:
        st.plotly_chart(coverage_heatmap(coverage_fleet.head(300), "equipment_id", "records", "Coverage by equipment"), width="stretch")

    st.markdown("### Critical null percentages")
    st.dataframe(null_pct, width="stretch", hide_index=True)

    st.markdown("### KPI traceability")
    st.dataframe(build_kpi_availability_report(workbook.sheets), width="stretch", hide_index=True)

    with st.expander("Schema overview", expanded=False):
        st.dataframe(cached_schema_overview(), width="stretch", hide_index=True)
        st.dataframe(cached_field_guide(), width="stretch", hide_index=True)


def main() -> None:
    base_workbook = _default_workbook()
    uploaded_bytes = st.session_state.get("uploaded_workbook_bytes")
    workbook = cached_load_from_bytes(uploaded_bytes) if uploaded_bytes else base_workbook
    _render_status_messages(workbook)

    if not workbook.sheets:
        st.stop()

    site_data = prepare_site_data(workbook.sheets)
    metadata = site_data.get("metadata", pd.DataFrame())
    site_name = _site_name(metadata)
    plant_name = _plant_name(metadata)
    min_date, max_date = _date_bounds(site_data)
    if min_date is None or max_date is None:
        st.error("The workbook does not contain any valid operational dates.")
        st.stop()

    default_range = st.session_state.get("selected_range", (max_date, max_date))
    if not isinstance(default_range, tuple) or len(default_range) != 2:
        default_range = (max_date, max_date)
    default_range = (
        max(min_date, min(default_range[0], max_date)),
        max(min_date, min(default_range[1], max_date)),
    )
    overview_preview = compute_overview(site_data, FilterState(date_from=default_range[0], date_to=default_range[1]), workbook.quality.get("health_summary", pd.DataFrame()))
    _render_snapshot(overview_preview, site_name, plant_name, workbook, default_range[0], default_range[1], min_date, max_date)

    selected_range = st.session_state.get("selected_range", default_range)
    date_from, date_to = selected_range
    overview = compute_overview(site_data, FilterState(date_from=date_from, date_to=date_to), workbook.quality.get("health_summary", pd.DataFrame()))

    tabs = st.tabs(["Overview", "Mine", "Plant", "Fleet", "Data Quality"])
    with tabs[0]:
        _render_overview(overview)
    with tabs[1]:
        _render_mine(site_data, date_from, date_to)
    with tabs[2]:
        _render_plant(site_data, date_from, date_to)
    with tabs[3]:
        _render_fleet(site_data, date_from, date_to)
    with tabs[4]:
        _render_data_quality(workbook)


if __name__ == "__main__":
    main()
