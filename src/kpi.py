from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from src.io_excel import build_field_guide


METRIC_DEFINITIONS: dict[str, dict[str, Any]] = {
    "bcm_moved": {"label": "BCM moved", "kind": "compact", "direction": "up", "domain": "Mine"},
    "ore_mined_t": {"label": "Ore mined (t)", "kind": "compact", "direction": "up", "domain": "Mine"},
    "stripping_ratio": {"label": "Stripping ratio", "kind": "ratio", "direction": "neutral", "domain": "Mine"},
    "diesel_l": {"label": "Diesel consumption", "kind": "liters", "direction": "neutral", "domain": "Mine"},
    "feed_tonnes": {"label": "Feed tonnes", "kind": "compact", "direction": "up", "domain": "Plant"},
    "throughput_tph": {"label": "Throughput", "kind": "rate", "direction": "up", "domain": "Plant"},
    "recovery_pct": {"label": "Recovery", "kind": "pct", "direction": "up", "domain": "Plant"},
    "metal_produced_t": {"label": "Metal produced", "kind": "compact", "direction": "up", "domain": "Plant"},
    "availability_pct": {"label": "Availability", "kind": "pct", "direction": "up", "domain": "Fleet"},
    "utilization_pct": {"label": "Utilization", "kind": "pct", "direction": "up", "domain": "Fleet"},
    "feed_grade_pct": {"label": "Feed grade", "kind": "pct", "direction": "neutral", "domain": "Plant"},
    "unplanned_downtime_h": {"label": "Unplanned downtime", "kind": "hours", "direction": "down", "domain": "Plant"},
}

AVAILABILITY_GROUPS = [
    {"group_name": "Excavators", "equipment_subtypes": ["excavator"]},
    {"group_name": "Trucks", "equipment_subtypes": ["truck_220t", "truck_100t", "truck_60t"]},
    {"group_name": "Drills", "equipment_subtypes": ["drill"]},
    {"group_name": "Ancillary", "equipment_subtypes": ["drill", "dozer", "grader"]},
]

NON_OVERLAP_GROUPS = {
    "excavator": "Excavators",
    "truck_220t": "Trucks",
    "truck_100t": "Trucks",
    "truck_60t": "Trucks",
    "drill": "Drills",
    "dozer": "Support Units",
    "grader": "Support Units",
}

GROUP_ORDER = [group["group_name"] for group in AVAILABILITY_GROUPS]
MINE_FILTER_GROUP_ORDER = ["Excavators", "Trucks", "Drills", "Support Units"]
SNAPSHOT_MINE_METRICS = ["bcm_moved", "ore_mined_t", "stripping_ratio", "diesel_l"]
SNAPSHOT_PLANT_METRICS = ["feed_tonnes", "throughput_tph", "recovery_pct", "metal_produced_t"]


@dataclass(frozen=True)
class FilterState:
    date_from: Optional[date]
    date_to: Optional[date]


def _safe_div(numerator: Any, denominator: Any) -> float:
    try:
        if denominator is None or pd.isna(denominator) or float(denominator) <= 0:
            return float("nan")
        if numerator is None or pd.isna(numerator):
            return float("nan")
        return float(numerator) / float(denominator)
    except (TypeError, ValueError):
        return float("nan")


def _sum_column(df: pd.DataFrame, column: str) -> float:
    if df.empty or column not in df.columns:
        return float("nan")
    value = df[column].sum(min_count=1)
    return float(value) if pd.notna(value) else float("nan")


def _weighted_average(df: pd.DataFrame, value_col: str, weight_col: str) -> float:
    if df.empty or value_col not in df.columns or weight_col not in df.columns:
        return float("nan")
    scoped = df[[value_col, weight_col]].dropna()
    scoped = scoped[scoped[weight_col] > 0]
    if scoped.empty:
        return float("nan")
    return float(np.average(scoped[value_col], weights=scoped[weight_col]))


def _first_mode(series: pd.Series) -> Any:
    cleaned = series.dropna()
    if cleaned.empty:
        return pd.NA
    mode = cleaned.mode()
    return mode.iloc[0] if not mode.empty else cleaned.iloc[0]


def filter_date_range(df: pd.DataFrame, date_from: Optional[date], date_to: Optional[date]) -> pd.DataFrame:
    if df.empty or "date" not in df.columns:
        return df.copy()
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    if date_from is not None:
        out = out[out["date"] >= date_from]
    if date_to is not None:
        out = out[out["date"] <= date_to]
    return out


def prepare_site_data(sheets: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    metadata = sheets.get("metadata", pd.DataFrame()).copy()
    daily_mine = sheets.get("daily_mine", pd.DataFrame()).copy()
    daily_plant = sheets.get("daily_plant", pd.DataFrame()).copy()
    daily_fleet = sheets.get("daily_fleet", pd.DataFrame()).copy()
    lookups = sheets.get("lookups", pd.DataFrame()).copy()

    for frame in [daily_mine, daily_plant, daily_fleet]:
        if not frame.empty and "date" in frame.columns:
            frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date

    if not daily_fleet.empty:
        daily_fleet["availability_group"] = daily_fleet["equipment_subtype"].map(NON_OVERLAP_GROUPS).fillna("Support Units")
        daily_fleet["mine_filter_group"] = daily_fleet["availability_group"]

    return {
        "metadata": metadata,
        "daily_mine": daily_mine,
        "daily_plant": daily_plant,
        "daily_fleet": daily_fleet,
        "lookups": lookups,
    }


def _previous_period(date_from: Optional[date], date_to: Optional[date]) -> tuple[Optional[date], Optional[date], int]:
    if date_from is None or date_to is None:
        return None, None, 0
    days = (date_to - date_from).days + 1
    prev_end = date_from - timedelta(days=1)
    prev_start = prev_end - timedelta(days=days - 1)
    return prev_start, prev_end, days


def aggregate_mine_daily(mine_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["date", "bcm_moved", "waste_bcm", "ore_bcm", "ore_mined_t", "stripping_ratio"]
    if mine_df.empty or "date" not in mine_df.columns:
        return pd.DataFrame(columns=columns)
    grouped = mine_df.groupby("date", as_index=False)[[col for col in ["bcm_moved", "waste_bcm", "ore_bcm", "ore_mined_t"] if col in mine_df.columns]].sum(min_count=1)
    for col in ["bcm_moved", "waste_bcm", "ore_bcm", "ore_mined_t"]:
        if col not in grouped.columns:
            grouped[col] = pd.NA
    grouped["stripping_ratio"] = grouped.apply(lambda row: _safe_div(row["waste_bcm"], row["ore_bcm"]), axis=1)
    return grouped[columns].sort_values("date")


def aggregate_area_contribution(mine_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["area_name", "bcm_moved", "ore_mined_t", "waste_bcm", "ore_bcm", "stripping_ratio"]
    if mine_df.empty or "area_name" not in mine_df.columns:
        return pd.DataFrame(columns=columns)
    grouped = mine_df.groupby("area_name", as_index=False).agg(
        bcm_moved=("bcm_moved", "sum"),
        ore_mined_t=("ore_mined_t", "sum"),
        waste_bcm=("waste_bcm", "sum"),
        ore_bcm=("ore_bcm", "sum"),
    )
    grouped["stripping_ratio"] = grouped.apply(lambda row: _safe_div(row["waste_bcm"], row["ore_bcm"]), axis=1)
    return grouped[columns].sort_values("bcm_moved", ascending=False)


def aggregate_plant_daily(plant_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "date",
        "feed_tonnes",
        "feed_grade_pct",
        "throughput_tph",
        "recovery_pct",
        "metal_produced_t",
        "availability_pct",
        "unplanned_downtime_h",
    ]
    if plant_df.empty or "date" not in plant_df.columns:
        return pd.DataFrame(columns=columns)
    rows: List[Dict[str, Any]] = []
    for current_date, group in plant_df.groupby("date"):
        rows.append(
            {
                "date": current_date,
                "feed_tonnes": _sum_column(group, "feed_tonnes"),
                "feed_grade_pct": _weighted_average(group, "feed_grade_pct", "feed_tonnes"),
                "throughput_tph": group["throughput_tph"].mean() if "throughput_tph" in group.columns else float("nan"),
                "recovery_pct": _weighted_average(group, "recovery_pct", "feed_tonnes"),
                "metal_produced_t": _sum_column(group, "metal_produced_t"),
                "availability_pct": group["availability_pct"].mean() if "availability_pct" in group.columns else float("nan"),
                "unplanned_downtime_h": _sum_column(group, "unplanned_downtime_h"),
            }
        )
    return pd.DataFrame(rows, columns=columns).sort_values("date")


def aggregate_availability_groups(fleet_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["date", "group_name", "availability_pct", "utilization_pct", "equipment_count"]
    if fleet_df.empty or not {"date", "equipment_subtype"}.issubset(fleet_df.columns):
        return pd.DataFrame(columns=columns)
    rows: List[Dict[str, Any]] = []
    for group in AVAILABILITY_GROUPS:
        scoped = fleet_df[fleet_df["equipment_subtypes"].isin(group["equipment_subtypes"])].copy() if "equipment_subtypes" in fleet_df.columns else fleet_df[fleet_df["equipment_subtype"].isin(group["equipment_subtypes"])].copy()
        if scoped.empty:
            continue
        daily = scoped.groupby("date", as_index=False).agg(
            availability_pct=("availability_pct", "mean"),
            utilization_pct=("utilization_pct", "mean"),
            equipment_count=("equipment_id", pd.Series.nunique),
        )
        daily["group_name"] = group["group_name"]
        rows.append(daily[columns])
    if not rows:
        return pd.DataFrame(columns=columns)
    out = pd.concat(rows, ignore_index=True)
    out["group_name"] = pd.Categorical(out["group_name"], categories=GROUP_ORDER, ordered=True)
    return out.sort_values(["group_name", "date"])


def aggregate_diesel_groups(fleet_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["date", "group_name", "diesel_l"]
    if fleet_df.empty or not {"date", "availability_group", "diesel_l"}.issubset(fleet_df.columns):
        return pd.DataFrame(columns=columns)
    return (
        fleet_df.groupby(["date", "availability_group"], as_index=False)["diesel_l"]
        .sum(min_count=1)
        .rename(columns={"availability_group": "group_name"})
        .sort_values(["date", "group_name"])
    )


def summarize_units(fleet_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
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
    if fleet_df.empty or "equipment_id" not in fleet_df.columns:
        return pd.DataFrame(columns=columns)
    rows: List[Dict[str, Any]] = []
    for equipment_id, group in fleet_df.groupby("equipment_id"):
        rows.append(
            {
                "equipment_id": equipment_id,
                "equipment_class": _first_mode(group["equipment_class"]) if "equipment_class" in group.columns else pd.NA,
                "equipment_subtype": _first_mode(group["equipment_subtype"]) if "equipment_subtype" in group.columns else pd.NA,
                "model": _first_mode(group["model"]) if "model" in group.columns else pd.NA,
                "area_name": _first_mode(group["area_name"]) if "area_name" in group.columns else pd.NA,
                "availability_pct": group["availability_pct"].mean() if "availability_pct" in group.columns else float("nan"),
                "utilization_pct": group["utilization_pct"].mean() if "utilization_pct" in group.columns else float("nan"),
                "diesel_l": _sum_column(group, "diesel_l"),
                "days_reported": int(group["date"].nunique()) if "date" in group.columns else 0,
            }
        )
    ranking = pd.DataFrame(rows, columns=columns)
    return ranking.sort_values(["availability_pct", "utilization_pct", "diesel_l"], ascending=[True, True, False], na_position="last")


def build_unit_heatmap_data(fleet_df: pd.DataFrame, metric_name: str, max_units: int = 15) -> pd.DataFrame:
    columns = ["equipment_id", "date", metric_name]
    if fleet_df.empty or metric_name not in fleet_df.columns:
        return pd.DataFrame(columns=columns)
    ranking = summarize_units(fleet_df)
    if ranking.empty or metric_name not in ranking.columns:
        return pd.DataFrame(columns=columns)
    selected_ids = ranking.sort_values(metric_name, ascending=True, na_position="last")["equipment_id"].head(max_units).tolist()
    heatmap_df = fleet_df[fleet_df["equipment_id"].isin(selected_ids)].copy()
    heatmap_df["equipment_id"] = pd.Categorical(heatmap_df["equipment_id"], categories=selected_ids, ordered=True)
    return heatmap_df[columns].sort_values(["equipment_id", "date"])


def build_unit_timeline(fleet_df: pd.DataFrame, equipment_id: str) -> pd.DataFrame:
    columns = ["date", "equipment_id", "availability_pct", "utilization_pct", "diesel_l"]
    if fleet_df.empty or "equipment_id" not in fleet_df.columns:
        return pd.DataFrame(columns=columns)
    scoped = fleet_df[fleet_df["equipment_id"] == equipment_id].copy()
    for column in columns:
        if column not in scoped.columns:
            scoped[column] = pd.NA
    return scoped[columns].sort_values("date")


def build_daily_operating_table(plant_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["date", "feed_tonnes", "feed_grade_pct", "throughput_tph", "recovery_pct", "metal_produced_t", "availability_pct", "unplanned_downtime_h"]
    if plant_df.empty:
        return pd.DataFrame(columns=columns)
    daily = aggregate_plant_daily(plant_df)
    return daily[columns].sort_values("date", ascending=False)


def _trend_label(series_df: pd.DataFrame, value_col: str) -> str:
    if series_df.empty or value_col not in series_df.columns:
        return "N/A"
    clean = series_df[["date", value_col]].dropna().sort_values("date")
    if len(clean) < 2:
        return "Stable"
    change = clean[value_col].iloc[-1] - clean[value_col].iloc[0]
    if pd.isna(change):
        return "N/A"
    if change > 0:
        return "Improving"
    if change < 0:
        return "Softening"
    return "Stable"


def _sparkline_data(series_df: pd.DataFrame, value_col: str, max_points: int = 12) -> pd.DataFrame:
    if series_df.empty or value_col not in series_df.columns:
        return pd.DataFrame(columns=["date", value_col])
    return series_df[["date", value_col]].dropna().sort_values("date").tail(max_points)


def _card(metric: str, actual: float, previous: float, series_df: pd.DataFrame, reason: str = "") -> Dict[str, Any]:
    delta = actual - previous if pd.notna(actual) and pd.notna(previous) else float("nan")
    return {
        "metric": metric,
        "label": METRIC_DEFINITIONS[metric]["label"],
        "actual": actual,
        "previous": previous,
        "delta": delta,
        "trend_label": _trend_label(series_df, metric),
        "sparkline": _sparkline_data(series_df, metric),
        "status": "N/A" if pd.isna(actual) else "OK",
        "reason": reason,
        "direction": METRIC_DEFINITIONS[metric]["direction"],
    }


def _build_overview_cards(
    current_mine: pd.DataFrame,
    previous_mine: pd.DataFrame,
    current_fleet: pd.DataFrame,
    previous_fleet: pd.DataFrame,
    current_plant: pd.DataFrame,
    previous_plant: pd.DataFrame,
    current_mine_daily: pd.DataFrame,
    current_plant_daily: pd.DataFrame,
) -> List[Dict[str, Any]]:
    diesel_daily = (
        current_fleet.groupby("date", as_index=False)["diesel_l"].sum(min_count=1).sort_values("date")
        if not current_fleet.empty and {"date", "diesel_l"}.issubset(current_fleet.columns)
        else pd.DataFrame(columns=["date", "diesel_l"])
    )
    return [
        _card("bcm_moved", _sum_column(current_mine, "bcm_moved"), _sum_column(previous_mine, "bcm_moved"), current_mine_daily, reason="Requires daily_mine.bcm_moved."),
        _card("ore_mined_t", _sum_column(current_mine, "ore_mined_t"), _sum_column(previous_mine, "ore_mined_t"), current_mine_daily, reason="Requires daily_mine.ore_mined_t."),
        _card("stripping_ratio", _safe_div(_sum_column(current_mine, "waste_bcm"), _sum_column(current_mine, "ore_bcm")), _safe_div(_sum_column(previous_mine, "waste_bcm"), _sum_column(previous_mine, "ore_bcm")), current_mine_daily, reason="Requires positive waste_bcm and ore_bcm."),
        _card("diesel_l", _sum_column(current_fleet, "diesel_l"), _sum_column(previous_fleet, "diesel_l"), diesel_daily, reason="Requires daily_fleet.diesel_l."),
        _card("feed_tonnes", _sum_column(current_plant, "feed_tonnes"), _sum_column(previous_plant, "feed_tonnes"), current_plant_daily, reason="Requires daily_plant.feed_tonnes."),
        _card("throughput_tph", current_plant_daily["throughput_tph"].mean() if "throughput_tph" in current_plant_daily.columns else float("nan"), previous_plant["throughput_tph"].mean() if not previous_plant.empty and "throughput_tph" in previous_plant.columns else float("nan"), current_plant_daily, reason="Requires daily_plant.throughput_tph."),
        _card("recovery_pct", _weighted_average(current_plant, "recovery_pct", "feed_tonnes"), _weighted_average(previous_plant, "recovery_pct", "feed_tonnes"), current_plant_daily, reason="Requires daily_plant.recovery_pct and feed_tonnes."),
        _card("metal_produced_t", _sum_column(current_plant, "metal_produced_t"), _sum_column(previous_plant, "metal_produced_t"), current_plant_daily, reason="Requires daily_plant.metal_produced_t."),
    ]


def build_readout(cards: List[Dict[str, Any]], quality_summary: pd.DataFrame, period_days: int) -> str:
    card_map = {card["metric"]: card for card in cards}

    def signal(metric: str) -> float:
        return float(card_map.get(metric, {}).get("delta", float("nan")))

    def phrase(value: float, positive: str, negative: str, neutral: str) -> str:
        if pd.isna(value):
            return neutral
        if value > 0:
            return positive
        if value < 0:
            return negative
        return neutral

    movement = phrase(signal("ore_mined_t"), "ore output improved", "ore output softened", "ore output held broadly steady")
    plant = phrase(signal("recovery_pct"), "recovery strengthened", "recovery eased", "recovery stayed close to the prior period")
    fleet = phrase(signal("diesel_l"), "diesel burn increased", "diesel burn eased", "diesel burn stayed stable")

    trust = "Data quality looks healthy."
    if not quality_summary.empty:
        row = quality_summary.iloc[0]
        if row.get("error_count", 0) > 0:
            trust = "Data quality needs attention before treating every number as decision-grade."
        elif row.get("warning_count", 0) > 0:
            trust = "Data quality has a few warnings, but the operating picture is still usable."

    if period_days <= 1:
        return f"This snapshot shows that {movement}, while {plant} and {fleet}. {trust}"
    return f"Against the previous {period_days}-day period, {movement}, {plant}, and {fleet}. {trust}"


def build_change_strip(cards: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        if pd.isna(card.get("delta")):
            continue
        rows.append(
            {
                "metric": card["metric"],
                "label": card["label"],
                "delta": float(card["delta"]),
                "direction": card.get("direction", "neutral"),
                "magnitude": abs(float(card["delta"])),
            }
        )
    if not rows:
        return pd.DataFrame(columns=["metric", "label", "delta", "direction", "magnitude"])
    out = pd.DataFrame(rows).sort_values("magnitude", ascending=False)
    return out.head(4)


def compute_overview(site_data: Dict[str, pd.DataFrame], filters: FilterState, quality_summary: pd.DataFrame | None = None) -> Dict[str, Any]:
    current_mine = filter_date_range(site_data.get("daily_mine", pd.DataFrame()), filters.date_from, filters.date_to)
    current_fleet = filter_date_range(site_data.get("daily_fleet", pd.DataFrame()), filters.date_from, filters.date_to)
    current_plant = filter_date_range(site_data.get("daily_plant", pd.DataFrame()), filters.date_from, filters.date_to)
    prev_from, prev_to, period_days = _previous_period(filters.date_from, filters.date_to)
    previous_mine = filter_date_range(site_data.get("daily_mine", pd.DataFrame()), prev_from, prev_to)
    previous_fleet = filter_date_range(site_data.get("daily_fleet", pd.DataFrame()), prev_from, prev_to)
    previous_plant = filter_date_range(site_data.get("daily_plant", pd.DataFrame()), prev_from, prev_to)

    current_mine_daily = aggregate_mine_daily(current_mine)
    current_plant_daily = aggregate_plant_daily(current_plant)
    cards = _build_overview_cards(current_mine, previous_mine, current_fleet, previous_fleet, current_plant, previous_plant, current_mine_daily, current_plant_daily)
    availability_groups = aggregate_availability_groups(current_fleet)
    area_contribution = aggregate_area_contribution(current_mine)

    return {
        "cards": cards,
        "mine_daily": current_mine_daily,
        "plant_daily": current_plant_daily,
        "availability_groups": availability_groups,
        "area_contribution": area_contribution,
        "readout": build_readout(cards, quality_summary if quality_summary is not None else pd.DataFrame(), period_days),
        "change_strip": build_change_strip(cards),
        "period_days": period_days,
    }


def compute_mine_page(
    site_data: Dict[str, pd.DataFrame],
    filters: FilterState,
    area_names: Optional[List[str]] = None,
    equipment_groups: Optional[List[str]] = None,
    equipment_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    area_names = list(area_names or [])
    equipment_groups = list(equipment_groups or [])
    equipment_ids = list(equipment_ids or [])

    mine_current = filter_date_range(site_data.get("daily_mine", pd.DataFrame()), filters.date_from, filters.date_to)
    fleet_current = filter_date_range(site_data.get("daily_fleet", pd.DataFrame()), filters.date_from, filters.date_to)

    if area_names and "area_name" in mine_current.columns:
        mine_current = mine_current[mine_current["area_name"].isin(area_names)]
    if area_names and "area_name" in fleet_current.columns:
        fleet_current = fleet_current[fleet_current["area_name"].isin(area_names)]
    if equipment_groups and "mine_filter_group" in fleet_current.columns:
        fleet_current = fleet_current[fleet_current["mine_filter_group"].isin(equipment_groups)]
    if equipment_ids and "equipment_id" in fleet_current.columns:
        fleet_current = fleet_current[fleet_current["equipment_id"].isin(equipment_ids)]

    mine_daily = aggregate_mine_daily(mine_current)
    availability_daily = aggregate_availability_groups(fleet_current)
    diesel_groups = aggregate_diesel_groups(fleet_current)
    unit_ranking = summarize_units(fleet_current)
    top_diesel = unit_ranking.sort_values("diesel_l", ascending=False, na_position="last").head(10) if not unit_ranking.empty else pd.DataFrame()
    bottom_diesel = unit_ranking[unit_ranking["diesel_l"] > 0].sort_values("diesel_l", ascending=True, na_position="last").head(10) if not unit_ranking.empty else pd.DataFrame()

    return {
        "mine_daily": mine_daily,
        "availability_daily": availability_daily,
        "utilization_daily": availability_daily,
        "diesel_groups": diesel_groups,
        "unit_ranking": unit_ranking,
        "top_diesel": top_diesel,
        "bottom_diesel": bottom_diesel,
        "filtered_units": fleet_current,
    }


def compute_plant_page(site_data: Dict[str, pd.DataFrame], filters: FilterState) -> Dict[str, Any]:
    plant_current = filter_date_range(site_data.get("daily_plant", pd.DataFrame()), filters.date_from, filters.date_to)
    plant_daily = aggregate_plant_daily(plant_current)
    return {
        "plant_daily": plant_daily,
        "plant_rows": plant_current,
        "daily_table": build_daily_operating_table(plant_current),
    }


def compute_fleet_page(site_data: Dict[str, pd.DataFrame], filters: FilterState) -> Dict[str, Any]:
    fleet_current = filter_date_range(site_data.get("daily_fleet", pd.DataFrame()), filters.date_from, filters.date_to)
    return {
        "availability_daily": aggregate_availability_groups(fleet_current),
        "diesel_groups": aggregate_diesel_groups(fleet_current),
        "unit_ranking": summarize_units(fleet_current),
        "fleet_rows": fleet_current,
    }


def build_kpi_availability_report(sheets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    field_guide = build_field_guide()
    metric_rows: List[Dict[str, Any]] = []
    mapping = {
        "BCM moved": [("daily_mine", "bcm_moved")],
        "Ore mined (t)": [("daily_mine", "ore_mined_t")],
        "Stripping ratio": [("daily_mine", "waste_bcm"), ("daily_mine", "ore_bcm")],
        "Diesel consumption": [("daily_fleet", "diesel_l")],
        "Feed tonnes": [("daily_plant", "feed_tonnes")],
        "Throughput": [("daily_plant", "throughput_tph")],
        "Recovery": [("daily_plant", "recovery_pct"), ("daily_plant", "feed_tonnes")],
        "Metal produced": [("daily_plant", "metal_produced_t")],
        "Availability": [("daily_fleet", "availability_pct")],
        "Utilization": [("daily_fleet", "utilization_pct")],
        "Feed grade": [("daily_plant", "feed_grade_pct"), ("daily_plant", "feed_tonnes")],
        "Unplanned downtime": [("daily_plant", "unplanned_downtime_h")],
    }
    for metric_name, meta in METRIC_DEFINITIONS.items():
        deps = mapping.get(meta["label"], [])
        missing = []
        for sheet_name, field_name in deps:
            df = sheets.get(sheet_name, pd.DataFrame())
            if field_name not in df.columns:
                missing.append(f"{sheet_name}.{field_name}")
        metric_rows.append(
            {
                "domain": meta["domain"],
                "metric": meta["label"],
                "status": "Available" if not missing else "N/A",
                "reason": "OK" if not missing else f"Missing: {', '.join(missing)}",
            }
        )
    return pd.DataFrame(metric_rows)
