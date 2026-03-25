from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd


@dataclass
class FilterState:
    site_id: str
    date_from: Optional[date]
    date_to: Optional[date]
    shift: str = "All"
    area_ids: Optional[List[str]] = None
    equipment_ids: Optional[List[str]] = None


def _safe_div(numerator: Any, denominator: Any) -> float:
    if denominator is None:
        return float("nan")
    try:
        if float(denominator) <= 0:
            return float("nan")
    except (TypeError, ValueError):
        return float("nan")
    try:
        return float(numerator) / float(denominator)
    except (TypeError, ValueError):
        return float("nan")


def _to_list(values: Optional[Iterable[str]]) -> List[str]:
    if values is None:
        return []
    return [str(v) for v in values if pd.notna(v)]


def filter_fact_dataframe(df: pd.DataFrame, filters: FilterState) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    if "site_id" in out.columns:
        out = out[out["site_id"] == filters.site_id]

    if "date" in out.columns and filters.date_from is not None:
        out = out[out["date"] >= filters.date_from]
    if "date" in out.columns and filters.date_to is not None:
        out = out[out["date"] <= filters.date_to]

    if "shift" in out.columns and filters.shift != "All":
        out = out[out["shift"] == filters.shift]

    area_ids = _to_list(filters.area_ids)
    if "area_id" in out.columns and area_ids:
        out = out[out["area_id"].isin(area_ids)]

    equipment_ids = _to_list(filters.equipment_ids)
    if "equipment_id" in out.columns and equipment_ids:
        out = out[out["equipment_id"].isin(equipment_ids)]

    return out


def filter_all_facts(sheets: Dict[str, pd.DataFrame], filters: FilterState) -> Dict[str, pd.DataFrame]:
    filtered: Dict[str, pd.DataFrame] = {}
    for name in ["fact_shift_excavator", "fact_shift_truck", "fact_shift_truck_route"]:
        filtered[name] = filter_fact_dataframe(sheets.get(name, pd.DataFrame()), filters)
    return filtered


def _apply_target_validity_window(df: pd.DataFrame, as_of_date: Optional[date]) -> pd.DataFrame:
    if df.empty or as_of_date is None:
        return df

    out = df.copy()
    as_of_ts = pd.Timestamp(as_of_date)
    if "effective_from" in out.columns:
        effective_from = pd.to_datetime(out["effective_from"], errors="coerce")
        cond_from = effective_from.isna() | (effective_from <= as_of_ts)
    else:
        cond_from = pd.Series(True, index=out.index)

    if "effective_to" in out.columns:
        effective_to = pd.to_datetime(out["effective_to"], errors="coerce")
        cond_to = effective_to.isna() | (effective_to >= as_of_ts)
    else:
        cond_to = pd.Series(True, index=out.index)

    matched = out[cond_from & cond_to]
    if matched.empty:
        return out
    return matched


def resolve_target(
    targets: pd.DataFrame,
    site_id: str,
    equipment_class: str,
    metric_name: str,
    area_ids: Optional[List[str]] = None,
    as_of_date: Optional[date] = None,
) -> float:
    if targets.empty:
        return float("nan")

    required = {"site_id", "equipment_class", "metric_name", "target"}
    if not required.issubset(targets.columns):
        return float("nan")

    mask = (
        (targets["site_id"] == site_id)
        & (targets["equipment_class"] == equipment_class)
        & (targets["metric_name"] == metric_name)
    )
    scope = targets.loc[mask].copy()

    if scope.empty:
        return float("nan")

    scope = _apply_target_validity_window(scope, as_of_date)

    selected_area_ids = _to_list(area_ids)

    if "area_id" in scope.columns and selected_area_ids:
        area_scope = scope[scope["area_id"].notna()]
        area_match = area_scope[area_scope["area_id"].isin(selected_area_ids)]
        if not area_match.empty:
            return float(area_match["target"].mean())

    if "area_id" in scope.columns:
        site_scope = scope[scope["area_id"].isna()]
        if not site_scope.empty:
            return float(site_scope["target"].mean())

    return float(scope["target"].mean())


def _period_trend(values: pd.DataFrame, value_col: str) -> Dict[str, Any]:
    if values.empty or value_col not in values.columns or "date" not in values.columns:
        return {"delta": float("nan"), "label": "N/A"}

    series = values.dropna(subset=["date", value_col]).sort_values("date")
    if len(series) < 2:
        return {"delta": float("nan"), "label": "N/A"}

    n = len(series)
    window = min(7, max(1, n // 2))
    current = series[value_col].tail(window).mean()
    previous_block = series[value_col].iloc[max(0, n - 2 * window): n - window]
    if previous_block.empty:
        delta = series[value_col].iloc[-1] - series[value_col].iloc[0]
    else:
        delta = current - previous_block.mean()

    if pd.isna(delta):
        label = "N/A"
    elif delta > 0:
        label = "Up"
    elif delta < 0:
        label = "Down"
    else:
        label = "Flat"

    return {"delta": float(delta), "label": label}


def _reference_date(exc_df: pd.DataFrame, trk_df: pd.DataFrame) -> Optional[date]:
    candidates: List[date] = []
    for df in [exc_df, trk_df]:
        if not df.empty and "date" in df.columns:
            max_date = df["date"].dropna().max()
            if pd.notna(max_date):
                candidates.append(max_date)
    if not candidates:
        return None
    return max(candidates)


def _agg_daily_excavator(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "date" not in df.columns:
        return pd.DataFrame(columns=["date", "tonnes_loaded", "availability_pct", "tonnes_per_operating_hour"])

    required = {"tonnes_loaded", "operating_h", "down_h"}
    if not required.issubset(df.columns):
        return pd.DataFrame(columns=["date", "tonnes_loaded", "availability_pct", "tonnes_per_operating_hour"])

    daily = df.groupby("date", as_index=False)[["tonnes_loaded", "operating_h", "down_h"]].sum(min_count=1)
    daily["availability_pct"] = daily.apply(
        lambda r: _safe_div(r["operating_h"], r["operating_h"] + r["down_h"]), axis=1
    )
    daily["tonnes_per_operating_hour"] = daily.apply(
        lambda r: _safe_div(r["tonnes_loaded"], r["operating_h"]), axis=1
    )
    return daily


def _agg_daily_truck(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "date" not in df.columns:
        return pd.DataFrame(columns=["date", "tonnes_hauled", "availability_pct", "tonnes_per_operating_hour", "avg_payload_t", "avg_cycle_time_min", "avg_queue_time_min"])

    required = {"tonnes_hauled", "operating_h", "down_h", "trips"}
    if not required.issubset(df.columns):
        return pd.DataFrame(columns=["date", "tonnes_hauled", "availability_pct", "tonnes_per_operating_hour", "avg_payload_t", "avg_cycle_time_min", "avg_queue_time_min"])

    agg_map: Dict[str, Any] = {
        "tonnes_hauled": "sum",
        "operating_h": "sum",
        "down_h": "sum",
        "trips": "sum",
    }
    for optional in ["cycle_time_min", "queue_time_min"]:
        if optional in df.columns:
            agg_map[optional] = "mean"

    daily = df.groupby("date", as_index=False).agg(agg_map)
    daily["availability_pct"] = daily.apply(
        lambda r: _safe_div(r["operating_h"], r["operating_h"] + r["down_h"]), axis=1
    )
    daily["tonnes_per_operating_hour"] = daily.apply(
        lambda r: _safe_div(r["tonnes_hauled"], r["operating_h"]), axis=1
    )
    daily["avg_payload_t"] = daily.apply(lambda r: _safe_div(r["tonnes_hauled"], r["trips"]), axis=1)

    if "cycle_time_min" in daily.columns:
        daily["avg_cycle_time_min"] = daily["cycle_time_min"]
    else:
        daily["avg_cycle_time_min"] = float("nan")

    if "queue_time_min" in daily.columns:
        daily["avg_queue_time_min"] = daily["queue_time_min"]
    else:
        daily["avg_queue_time_min"] = float("nan")

    return daily


def _metric_card(
    name: str,
    actual: float,
    target: float,
    trend: Dict[str, Any],
    reason: str = "",
) -> Dict[str, Any]:
    if pd.isna(actual):
        return {
            "metric": name,
            "actual": float("nan"),
            "target": target,
            "delta": float("nan"),
            "trend_label": trend.get("label", "N/A"),
            "trend_delta": trend.get("delta", float("nan")),
            "status": "N/A",
            "reason": reason,
        }

    delta = actual - target if not pd.isna(target) else float("nan")
    return {
        "metric": name,
        "actual": float(actual),
        "target": target,
        "delta": delta,
        "trend_label": trend.get("label", "N/A"),
        "trend_delta": trend.get("delta", float("nan")),
        "status": "OK",
        "reason": reason,
    }


def compute_overview(
    filtered_excavator: pd.DataFrame,
    filtered_truck: pd.DataFrame,
    targets: pd.DataFrame,
    filters: FilterState,
) -> Dict[str, Any]:
    ref_date = _reference_date(filtered_excavator, filtered_truck)

    exc_daily = _agg_daily_excavator(filtered_excavator)
    trk_daily = _agg_daily_truck(filtered_truck)

    cards: List[Dict[str, Any]] = []

    # Excavator cards
    exc_tonnes = (
        filtered_excavator["tonnes_loaded"].sum(min_count=1)
        if "tonnes_loaded" in filtered_excavator.columns
        else float("nan")
    )
    cards.append(
        _metric_card(
            "excavator_tonnes_loaded",
            exc_tonnes,
            float("nan"),
            _period_trend(exc_daily.rename(columns={"tonnes_loaded": "value"}), "value"),
            reason="Target not configured for tonnes_loaded in standard contract.",
        )
    )

    exc_tph = exc_daily["tonnes_per_operating_hour"].mean() if not exc_daily.empty else float("nan")
    cards.append(
        _metric_card(
            "excavator_tonnes_per_operating_hour",
            exc_tph,
            resolve_target(
                targets,
                filters.site_id,
                "excavator",
                "tonnes_per_operating_hour",
                filters.area_ids,
                ref_date,
            ),
            _period_trend(exc_daily.rename(columns={"tonnes_per_operating_hour": "value"}), "value"),
            reason="Requires tonnes_loaded and operating_h.",
        )
    )

    exc_avail = exc_daily["availability_pct"].mean() if not exc_daily.empty else float("nan")
    cards.append(
        _metric_card(
            "excavator_availability_pct",
            exc_avail,
            resolve_target(
                targets,
                filters.site_id,
                "excavator",
                "availability_pct",
                filters.area_ids,
                ref_date,
            ),
            _period_trend(exc_daily.rename(columns={"availability_pct": "value"}), "value"),
            reason="Requires operating_h and down_h.",
        )
    )

    # Truck cards
    trk_tonnes = (
        filtered_truck["tonnes_hauled"].sum(min_count=1)
        if "tonnes_hauled" in filtered_truck.columns
        else float("nan")
    )
    cards.append(
        _metric_card(
            "truck_tonnes_hauled",
            trk_tonnes,
            float("nan"),
            _period_trend(trk_daily.rename(columns={"tonnes_hauled": "value"}), "value"),
            reason="Target not configured for tonnes_hauled in standard contract.",
        )
    )

    trk_tph = trk_daily["tonnes_per_operating_hour"].mean() if not trk_daily.empty else float("nan")
    cards.append(
        _metric_card(
            "truck_tonnes_per_operating_hour",
            trk_tph,
            resolve_target(
                targets,
                filters.site_id,
                "truck",
                "tonnes_per_operating_hour",
                filters.area_ids,
                ref_date,
            ),
            _period_trend(trk_daily.rename(columns={"tonnes_per_operating_hour": "value"}), "value"),
            reason="Requires tonnes_hauled and operating_h.",
        )
    )

    trk_avail = trk_daily["availability_pct"].mean() if not trk_daily.empty else float("nan")
    cards.append(
        _metric_card(
            "truck_availability_pct",
            trk_avail,
            resolve_target(
                targets,
                filters.site_id,
                "truck",
                "availability_pct",
                filters.area_ids,
                ref_date,
            ),
            _period_trend(trk_daily.rename(columns={"availability_pct": "value"}), "value"),
            reason="Requires operating_h and down_h.",
        )
    )

    # Driver metrics from truck data
    avg_payload = trk_daily["avg_payload_t"].mean() if "avg_payload_t" in trk_daily.columns else float("nan")
    cards.append(
        _metric_card(
            "avg_payload_t",
            avg_payload,
            resolve_target(
                targets,
                filters.site_id,
                "truck",
                "avg_payload_t",
                filters.area_ids,
                ref_date,
            ),
            _period_trend(trk_daily.rename(columns={"avg_payload_t": "value"}), "value"),
            reason="Requires tonnes_hauled and trips.",
        )
    )

    avg_cycle = trk_daily["avg_cycle_time_min"].mean() if "avg_cycle_time_min" in trk_daily.columns else float("nan")
    cards.append(
        _metric_card(
            "avg_cycle_time_min",
            avg_cycle,
            resolve_target(
                targets,
                filters.site_id,
                "truck",
                "cycle_time_min",
                filters.area_ids,
                ref_date,
            ),
            _period_trend(trk_daily.rename(columns={"avg_cycle_time_min": "value"}), "value"),
            reason="Requires cycle_time_min column.",
        )
    )

    avg_queue = trk_daily["avg_queue_time_min"].mean() if "avg_queue_time_min" in trk_daily.columns else float("nan")
    cards.append(
        _metric_card(
            "avg_queue_time_min",
            avg_queue,
            resolve_target(
                targets,
                filters.site_id,
                "truck",
                "queue_time_min",
                filters.area_ids,
                ref_date,
            ),
            _period_trend(trk_daily.rename(columns={"avg_queue_time_min": "value"}), "value"),
            reason="Requires queue_time_min column.",
        )
    )

    return {
        "cards": cards,
        "exc_daily": exc_daily,
        "trk_daily": trk_daily,
    }


def _build_utilization(df: pd.DataFrame) -> pd.Series:
    if "standby_h" in df.columns:
        denom = df["operating_h"] + df["idle_h"] + df["standby_h"] + df["down_h"]
    else:
        denom = df["operating_h"] + df["idle_h"] + df["down_h"]
    return df["operating_h"] / denom.where(denom > 0)


def compute_excavator_kpis(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    required = {"date", "shift", "site_id", "area_id", "equipment_id"}
    if not required.issubset(df.columns):
        return pd.DataFrame()

    value_cols = ["tonnes_loaded", "operating_h", "down_h", "idle_h"]
    agg_map: Dict[str, Any] = {col: "sum" for col in value_cols if col in df.columns}

    for optional_sum in ["standby_h", "cycles_count", "fuel_l"]:
        if optional_sum in df.columns:
            agg_map[optional_sum] = "sum"

    for optional_avg in ["avg_cycle_time_s", "bucket_fill_factor"]:
        if optional_avg in df.columns:
            agg_map[optional_avg] = "mean"

    grouped = (
        df.groupby(["date", "shift", "site_id", "area_id", "equipment_id"], as_index=False)
        .agg(agg_map)
        .sort_values(["date", "shift", "equipment_id"])
    )

    if {"operating_h", "down_h"}.issubset(grouped.columns):
        grouped["availability_pct"] = grouped["operating_h"] / (
            grouped["operating_h"] + grouped["down_h"]
        ).where((grouped["operating_h"] + grouped["down_h"]) > 0)
    else:
        grouped["availability_pct"] = float("nan")

    if {"operating_h", "down_h", "idle_h"}.issubset(grouped.columns):
        grouped["utilization_pct"] = _build_utilization(grouped)
    else:
        grouped["utilization_pct"] = float("nan")

    if {"tonnes_loaded", "operating_h"}.issubset(grouped.columns):
        grouped["tonnes_per_operating_hour"] = grouped["tonnes_loaded"] / grouped["operating_h"].where(
            grouped["operating_h"] > 0
        )
    else:
        grouped["tonnes_per_operating_hour"] = float("nan")

    if {"cycles_count", "operating_h"}.issubset(grouped.columns):
        grouped["cycles_per_hour"] = grouped["cycles_count"] / grouped["operating_h"].where(grouped["operating_h"] > 0)

    return grouped


def compute_truck_kpis(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    required = {"date", "shift", "site_id", "area_id", "equipment_id"}
    if not required.issubset(df.columns):
        return pd.DataFrame()

    agg_map: Dict[str, Any] = {}
    for col in ["tonnes_hauled", "trips", "operating_h", "down_h", "idle_h", "standby_h", "fuel_l"]:
        if col in df.columns:
            agg_map[col] = "sum"

    for col in [
        "payload_target_t",
        "cycle_time_min",
        "queue_time_min",
        "speed_kmph_avg",
        "distance_km_avg",
    ]:
        if col in df.columns:
            agg_map[col] = "mean"

    grouped = (
        df.groupby(["date", "shift", "site_id", "area_id", "equipment_id"], as_index=False)
        .agg(agg_map)
        .sort_values(["date", "shift", "equipment_id"])
    )

    if {"operating_h", "down_h"}.issubset(grouped.columns):
        grouped["availability_pct"] = grouped["operating_h"] / (
            grouped["operating_h"] + grouped["down_h"]
        ).where((grouped["operating_h"] + grouped["down_h"]) > 0)
    else:
        grouped["availability_pct"] = float("nan")

    if {"operating_h", "down_h", "idle_h"}.issubset(grouped.columns):
        grouped["utilization_pct"] = _build_utilization(grouped)
    else:
        grouped["utilization_pct"] = float("nan")

    if {"tonnes_hauled", "operating_h"}.issubset(grouped.columns):
        grouped["tonnes_per_operating_hour"] = grouped["tonnes_hauled"] / grouped["operating_h"].where(
            grouped["operating_h"] > 0
        )
    else:
        grouped["tonnes_per_operating_hour"] = float("nan")

    if {"tonnes_hauled", "trips"}.issubset(grouped.columns):
        grouped["avg_payload_t"] = grouped["tonnes_hauled"] / grouped["trips"].where(grouped["trips"] > 0)
    else:
        grouped["avg_payload_t"] = float("nan")

    if {"avg_payload_t", "payload_target_t"}.issubset(grouped.columns):
        grouped["payload_compliance_pct"] = grouped["avg_payload_t"] / grouped["payload_target_t"].where(
            grouped["payload_target_t"] > 0
        )
    else:
        grouped["payload_compliance_pct"] = float("nan")

    if {"tonnes_hauled", "distance_km_avg"}.issubset(grouped.columns):
        grouped["tkm"] = grouped["tonnes_hauled"] * grouped["distance_km_avg"]
    else:
        grouped["tkm"] = float("nan")

    if {"fuel_l", "tkm"}.issubset(grouped.columns):
        grouped["l_per_tkm"] = grouped["fuel_l"] / grouped["tkm"].where(grouped["tkm"] > 0)
    else:
        grouped["l_per_tkm"] = float("nan")

    return grouped


def summarize_unit_ranking(kpi_df: pd.DataFrame, equipment_class: str) -> pd.DataFrame:
    if kpi_df.empty or "equipment_id" not in kpi_df.columns:
        return pd.DataFrame()

    agg_map: Dict[str, Any] = {
        "tonnes_per_operating_hour": "mean",
        "availability_pct": "mean",
    }
    for optional in [
        "down_h",
        "queue_time_min",
        "cycle_time_min",
        "avg_payload_t",
        "payload_compliance_pct",
        "tonnes_hauled",
        "tonnes_loaded",
    ]:
        if optional in kpi_df.columns:
            agg_map[optional] = "mean" if optional.endswith("_min") else "sum"

    by_unit = kpi_df.groupby("equipment_id", as_index=False).agg(agg_map)

    if equipment_class == "excavator":
        sort_cols = [col for col in ["tonnes_per_operating_hour", "availability_pct", "down_h"] if col in by_unit.columns]
        ascending = [True, True, False][: len(sort_cols)]
    else:
        sort_cols = [col for col in ["tonnes_per_operating_hour", "queue_time_min", "availability_pct"] if col in by_unit.columns]
        ascending = [True, False, True][: len(sort_cols)]

    if sort_cols:
        by_unit = by_unit.sort_values(sort_cols, ascending=ascending)

    return by_unit


def build_top_exceptions_excavator(kpi_df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    ranking = summarize_unit_ranking(kpi_df, "excavator")
    if ranking.empty:
        return ranking

    out = ranking.head(top_n).copy()

    def _reason(row: pd.Series) -> str:
        reasons: List[str] = []
        if "availability_pct" in row and pd.notna(row["availability_pct"]):
            reasons.append(f"Low availability ({row['availability_pct']:.0%})")
        if "down_h" in row and pd.notna(row["down_h"]):
            reasons.append(f"High down_h ({row['down_h']:.1f}h)")
        return ", ".join(reasons[:2]) if reasons else "Low productivity"

    out["reason"] = out.apply(_reason, axis=1)
    return out


def build_top_exceptions_truck(kpi_df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    ranking = summarize_unit_ranking(kpi_df, "truck")
    if ranking.empty:
        return ranking

    out = ranking.head(top_n).copy()

    def _reason(row: pd.Series) -> str:
        reasons: List[str] = []
        if "availability_pct" in row and pd.notna(row["availability_pct"]):
            reasons.append(f"Low availability ({row['availability_pct']:.0%})")
        if "cycle_time_min" in row and pd.notna(row["cycle_time_min"]):
            reasons.append(f"High cycle ({row['cycle_time_min']:.1f} min)")
        if "queue_time_min" in row and pd.notna(row["queue_time_min"]):
            reasons.append(f"High queue ({row['queue_time_min']:.1f} min)")
        return ", ".join(reasons[:2]) if reasons else "Low productivity"

    out["reason"] = out.apply(_reason, axis=1)
    return out


def build_top_area_exceptions(truck_kpi_df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    if truck_kpi_df.empty or "area_id" not in truck_kpi_df.columns:
        return pd.DataFrame()

    agg_map: Dict[str, Any] = {}
    if "queue_time_min" in truck_kpi_df.columns:
        agg_map["queue_time_min"] = "mean"
    if "payload_compliance_pct" in truck_kpi_df.columns:
        agg_map["payload_compliance_pct"] = "mean"
    if not agg_map:
        return pd.DataFrame()

    by_area = truck_kpi_df.groupby("area_id", as_index=False).agg(agg_map)

    if "queue_time_min" in by_area.columns and by_area["queue_time_min"].notna().any():
        out = by_area.sort_values("queue_time_min", ascending=False).head(top_n).copy()
        out["reason"] = out["queue_time_min"].apply(lambda x: f"High queue time ({x:.1f} min)")
    elif "payload_compliance_pct" in by_area.columns:
        out = by_area.sort_values("payload_compliance_pct", ascending=True).head(top_n).copy()
        out["reason"] = out["payload_compliance_pct"].apply(lambda x: f"Low payload compliance ({x:.0%})")
    else:
        out = pd.DataFrame()

    return out


def compute_route_ranking(route_df: pd.DataFrame) -> pd.DataFrame:
    if route_df.empty:
        return pd.DataFrame()

    required = {"from_area_id", "to_area_id"}
    if not required.issubset(route_df.columns):
        return pd.DataFrame()

    agg_map: Dict[str, Any] = {}
    for col in ["tonnes", "trips"]:
        if col in route_df.columns:
            agg_map[col] = "sum"
    for col in ["distance_km", "cycle_time_min", "queue_time_min"]:
        if col in route_df.columns:
            agg_map[col] = "mean"

    if not agg_map:
        return pd.DataFrame()

    rank = (
        route_df.groupby(["from_area_id", "to_area_id"], as_index=False)
        .agg(agg_map)
        .sort_values(
            [c for c in ["queue_time_min", "cycle_time_min", "tonnes"] if c in agg_map],
            ascending=[False, False, False][: len([c for c in ["queue_time_min", "cycle_time_min", "tonnes"] if c in agg_map])],
        )
    )
    return rank


def build_kpi_availability_report(sheets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    checks = [
        ("excavator", "tonnes_loaded", "fact_shift_excavator", ["tonnes_loaded"]),
        ("excavator", "availability_pct", "fact_shift_excavator", ["operating_h", "down_h"]),
        ("excavator", "utilization_pct", "fact_shift_excavator", ["operating_h", "idle_h", "down_h"]),
        ("excavator", "tonnes_per_operating_hour", "fact_shift_excavator", ["tonnes_loaded", "operating_h"]),
        ("excavator", "cycles_per_hour", "fact_shift_excavator", ["cycles_count", "operating_h"]),
        ("truck", "tonnes_hauled", "fact_shift_truck", ["tonnes_hauled"]),
        ("truck", "availability_pct", "fact_shift_truck", ["operating_h", "down_h"]),
        ("truck", "utilization_pct", "fact_shift_truck", ["operating_h", "idle_h", "down_h"]),
        ("truck", "tonnes_per_operating_hour", "fact_shift_truck", ["tonnes_hauled", "operating_h"]),
        ("truck", "avg_payload_t", "fact_shift_truck", ["tonnes_hauled", "trips"]),
        ("truck", "payload_compliance_pct", "fact_shift_truck", ["tonnes_hauled", "trips", "payload_target_t"]),
        ("truck", "cycle_time_min", "fact_shift_truck", ["cycle_time_min"]),
        ("truck", "queue_time_min", "fact_shift_truck", ["queue_time_min"]),
        ("truck", "tkm", "fact_shift_truck", ["tonnes_hauled", "distance_km_avg"]),
        ("truck", "l_per_tkm", "fact_shift_truck", ["fuel_l", "tonnes_hauled", "distance_km_avg"]),
    ]

    rows: List[Dict[str, Any]] = []
    for equipment_class, metric, sheet_name, required_cols in checks:
        df = sheets.get(sheet_name, pd.DataFrame())
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            status = "N/A"
            reason = f"Missing columns: {', '.join(missing)}"
        else:
            status = "Available"
            reason = "OK"

        rows.append(
            {
                "equipment_class": equipment_class,
                "metric": metric,
                "status": status,
                "reason": reason,
            }
        )

    return pd.DataFrame(rows)
