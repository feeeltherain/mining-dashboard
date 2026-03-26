from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import plotly.io as pio

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
from src.theme import TOKENS


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_PATH = ROOT / "data" / "mine_productivity_input.xlsx"
FALLBACK_SAMPLE_PATH = ROOT / "data" / "sample_mine_productivity_input.xlsx"
TEMPLATE_PATH = ROOT / "data" / "mine_productivity_input_template.xlsx"
SAMPLE_PATH = ROOT / "data" / "sample_mine_productivity_input.xlsx"

MINE_CARD_ORDER = ["bcm_moved", "ore_mined_t", "stripping_ratio", "diesel_l"]
PLANT_CARD_ORDER = ["feed_tonnes", "throughput_tph", "recovery_pct", "metal_produced_t"]
AVAILABILITY_GROUP_ORDER = ["Excavators", "Trucks", "Drills", "Ancillary"]
VALID_HEAT_METRICS = {"availability_pct", "utilization_pct"}
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


def load_dashboard_workbook(content: bytes | None = None) -> WorkbookData:
    if content:
        return load_excel_workbook(content)
    if DEFAULT_INPUT_PATH.exists():
        return load_excel_workbook(str(DEFAULT_INPUT_PATH))
    if FALLBACK_SAMPLE_PATH.exists():
        return load_excel_workbook(str(FALLBACK_SAMPLE_PATH))
    return WorkbookData(
        sheets={},
        errors=["No workbook found. Generate or upload a workbook to continue."],
        warnings=[],
        quality={},
        source_name="",
        schema_version="",
        source_kind="invalid",
    )


def _parse_date(value: str | None) -> Optional[date]:
    if not value:
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _date_bounds(site_data: Dict[str, pd.DataFrame]) -> tuple[Optional[date], Optional[date]]:
    dates: List[date] = []
    for key in ["daily_mine", "daily_plant", "daily_fleet"]:
        df = site_data.get(key, pd.DataFrame())
        if not df.empty and "date" in df.columns:
            dates.extend(pd.to_datetime(df["date"], errors="coerce").dt.date.dropna().tolist())
    if not dates:
        return None, None
    return min(dates), max(dates)


def _clamp_range(
    requested_from: Optional[date],
    requested_to: Optional[date],
    min_date: date,
    max_date: date,
) -> tuple[date, date]:
    start = requested_from or max_date
    end = requested_to or max_date
    start = max(min_date, min(start, max_date))
    end = max(min_date, min(end, max_date))
    if start > end:
        start, end = end, start
    return start, end


def _json_value(value: Any) -> Any:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        if pd.isna(value):
            return None
        return float(value)
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    if pd.isna(value):
        return None
    return value


def _table_payload(df: pd.DataFrame, columns: Optional[List[str]] = None, max_rows: Optional[int] = None) -> Dict[str, Any]:
    if df.empty and not columns:
        return {"columns": [], "rows": []}

    out = df.copy()
    if columns:
        for column in columns:
            if column not in out.columns:
                out[column] = pd.NA
        out = out[columns]
    if max_rows is not None:
        out = out.head(max_rows)

    rows = []
    for record in out.to_dict(orient="records"):
        rows.append({str(key): _json_value(value) for key, value in record.items()})
    return {"columns": list(out.columns), "rows": rows}


def _chart_payload(figure: Any) -> str:
    return pio.to_json(figure, pretty=False)


def _site_name(metadata: pd.DataFrame) -> str:
    if metadata.empty or "site_name" not in metadata.columns:
        return "Mining Operations"
    return str(metadata["site_name"].iloc[0])


def _plant_name(metadata: pd.DataFrame) -> str:
    if metadata.empty or "plant_name" not in metadata.columns:
        return "Plant"
    return str(metadata["plant_name"].iloc[0])


def _last_refresh(metadata: pd.DataFrame) -> Optional[str]:
    if metadata.empty or "last_refresh_ts" not in metadata.columns:
        return None
    value = pd.to_datetime(metadata["last_refresh_ts"].iloc[0], errors="coerce")
    if pd.isna(value):
        return None
    return value.isoformat()


def _health_tone(health_summary: pd.DataFrame) -> str:
    if health_summary.empty:
        return "warn"
    status = str(health_summary.iloc[0].get("status", "")).lower()
    if "healthy" in status:
        return "good"
    if "warning" in status or "usable" in status:
        return "warn"
    return "bad"


def _source_label(workbook: WorkbookData) -> str:
    if workbook.source_name == "uploaded_workbook.xlsx":
        return "Uploaded workbook"
    return Path(workbook.source_name).name or "Workbook"


def _latest_available_date(workbook: WorkbookData) -> Optional[str]:
    latest_available = workbook.quality.get("last_available", pd.DataFrame())
    if latest_available.empty or "last_available_date" not in latest_available.columns:
        return None
    latest_dt = pd.to_datetime(latest_available["last_available_date"], errors="coerce").max()
    if pd.isna(latest_dt):
        return None
    return latest_dt.date().isoformat()


def _valid_selection(requested: Optional[List[str]], options: List[str], *, default_all: bool) -> List[str]:
    chosen = [value for value in (requested or []) if value in options]
    if chosen:
        return chosen
    return options[:] if default_all else []


def _valid_single(requested: Optional[str], options: List[str]) -> Optional[str]:
    if requested and requested in options:
        return requested
    return options[0] if options else None


def _serialize_card(card: Dict[str, Any]) -> Dict[str, Any]:
    sparkline = card.get("sparkline", pd.DataFrame())
    sparkline_chart = None
    if isinstance(sparkline, pd.DataFrame) and not sparkline.empty and len(sparkline) >= 2:
        sparkline_chart = _chart_payload(
            sparkline_figure(sparkline, card["metric"], color=METRIC_COLORS.get(card["metric"], TOKENS["accent"]))
        )

    return {
        "metric": card["metric"],
        "label": card["label"],
        "actual": _json_value(card.get("actual")),
        "previous": _json_value(card.get("previous")),
        "delta": _json_value(card.get("delta")),
        "trend_label": card.get("trend_label", "N/A"),
        "status": card.get("status", "OK"),
        "reason": card.get("reason", ""),
        "direction": card.get("direction", "neutral"),
        "sparkline_chart": sparkline_chart,
    }


def build_dashboard_payload(
    *,
    workbook_content: bytes | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    mine_areas: Optional[List[str]] = None,
    mine_groups: Optional[List[str]] = None,
    mine_equipment: Optional[List[str]] = None,
    mine_selected_unit: Optional[str] = None,
    mine_heat_metric: Optional[str] = None,
    fleet_selected_unit: Optional[str] = None,
    fleet_heat_metric: Optional[str] = None,
) -> Dict[str, Any]:
    workbook = load_dashboard_workbook(workbook_content)
    base_payload: Dict[str, Any] = {
        "ok": True,
        "errors": workbook.errors,
        "warnings": workbook.warnings,
        "meta": {
            "source_name": workbook.source_name,
            "source_label": _source_label(workbook),
            "source_kind": workbook.source_kind,
            "schema_version": workbook.schema_version,
            "template_url": "/api/template",
            "sample_url": "/api/sample",
        },
    }

    if not workbook.sheets:
        base_payload["ok"] = False
        base_payload["message"] = "No workbook data is available."
        return base_payload

    site_data = prepare_site_data(workbook.sheets)
    metadata = site_data.get("metadata", pd.DataFrame())
    min_date, max_date = _date_bounds(site_data)

    base_payload["meta"].update(
        {
            "site_name": _site_name(metadata),
            "plant_name": _plant_name(metadata),
            "last_refresh_ts": _last_refresh(metadata),
            "latest_available_date": _latest_available_date(workbook),
            "health_status": str(workbook.quality.get("health_summary", pd.DataFrame()).iloc[0].get("status", "Unknown"))
            if not workbook.quality.get("health_summary", pd.DataFrame()).empty
            else "Unknown",
            "health_tone": _health_tone(workbook.quality.get("health_summary", pd.DataFrame())),
        }
    )

    if min_date is None or max_date is None:
        base_payload["ok"] = False
        base_payload["message"] = "The workbook does not contain any valid operational dates."
        return base_payload

    selected_from, selected_to = _clamp_range(_parse_date(date_from), _parse_date(date_to), min_date, max_date)
    base_payload["meta"].update(
        {
            "available_range": {"min": min_date.isoformat(), "max": max_date.isoformat()},
            "selected_range": {"from": selected_from.isoformat(), "to": selected_to.isoformat()},
            "range_days": (selected_to - selected_from).days + 1,
        }
    )

    filters = FilterState(date_from=selected_from, date_to=selected_to)
    quality_summary = workbook.quality.get("health_summary", pd.DataFrame())
    overview = compute_overview(site_data, filters, quality_summary)

    mine_rows = site_data.get("daily_mine", pd.DataFrame())
    fleet_rows = site_data.get("daily_fleet", pd.DataFrame())
    area_options = sorted(mine_rows.get("area_name", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
    filter_group_options = [
        group
        for group in MINE_FILTER_GROUP_ORDER
        if group in fleet_rows.get("mine_filter_group", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()
    ]

    selected_areas = _valid_selection(mine_areas, area_options, default_all=True)
    selected_groups = _valid_selection(mine_groups, filter_group_options, default_all=True)

    equipment_scope = fleet_rows.copy()
    if not equipment_scope.empty and "date" in equipment_scope.columns:
        equipment_scope = equipment_scope[
            (equipment_scope["date"] >= selected_from) & (equipment_scope["date"] <= selected_to)
        ]
    if selected_areas and "area_name" in equipment_scope.columns:
        equipment_scope = equipment_scope[equipment_scope["area_name"].isin(selected_areas)]
    if selected_groups and "mine_filter_group" in equipment_scope.columns:
        equipment_scope = equipment_scope[equipment_scope["mine_filter_group"].isin(selected_groups)]
    equipment_options = sorted(
        equipment_scope.get("equipment_id", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()
    )
    selected_equipment = _valid_selection(mine_equipment, equipment_options, default_all=False)

    mine_page = compute_mine_page(
        site_data,
        filters,
        area_names=selected_areas,
        equipment_groups=selected_groups,
        equipment_ids=selected_equipment,
    )
    mine_ranking = mine_page.get("unit_ranking", pd.DataFrame())
    mine_unit_ids = mine_ranking.get("equipment_id", pd.Series(dtype=str)).dropna().astype(str).tolist()
    resolved_mine_heat_metric = mine_heat_metric if mine_heat_metric in VALID_HEAT_METRICS else "availability_pct"
    resolved_mine_unit = _valid_single(mine_selected_unit, mine_unit_ids)
    mine_timeline = (
        build_unit_timeline(mine_page.get("filtered_units", pd.DataFrame()), resolved_mine_unit)
        if resolved_mine_unit
        else pd.DataFrame(columns=["date", "equipment_id", "availability_pct", "utilization_pct", "diesel_l"])
    )
    mine_heatmap = build_unit_heatmap_data(mine_page.get("filtered_units", pd.DataFrame()), resolved_mine_heat_metric)

    plant_page = compute_plant_page(site_data, filters)

    fleet_page = compute_fleet_page(site_data, filters)
    fleet_ranking = fleet_page.get("unit_ranking", pd.DataFrame())
    fleet_unit_ids = fleet_ranking.get("equipment_id", pd.Series(dtype=str)).dropna().astype(str).tolist()
    resolved_fleet_heat_metric = fleet_heat_metric if fleet_heat_metric in VALID_HEAT_METRICS else "availability_pct"
    resolved_fleet_unit = _valid_single(fleet_selected_unit, fleet_unit_ids)
    fleet_timeline = (
        build_unit_timeline(fleet_page.get("fleet_rows", pd.DataFrame()), resolved_fleet_unit)
        if resolved_fleet_unit
        else pd.DataFrame(columns=["date", "equipment_id", "availability_pct", "utilization_pct", "diesel_l"])
    )
    fleet_heatmap = build_unit_heatmap_data(fleet_page.get("fleet_rows", pd.DataFrame()), resolved_fleet_heat_metric)

    base_payload["overview"] = {
        "readout": overview.get("readout", ""),
        "period_days": int(overview.get("period_days", 1) or 1),
        "cards": [_serialize_card(card) for card in overview.get("cards", [])],
        "mine_card_order": MINE_CARD_ORDER,
        "plant_card_order": PLANT_CARD_ORDER,
        "change_strip": _table_payload(overview.get("change_strip", pd.DataFrame())),
        "charts": {
            "mine_production": _chart_payload(
                mine_production_combo(overview.get("mine_daily", pd.DataFrame()), "Movement and ore production")
            ),
            "plant_performance": _chart_payload(
                plant_performance_combo(overview.get("plant_daily", pd.DataFrame()), "Plant operating rhythm")
            ),
            "availability_groups": {
                group_name: _chart_payload(
                    group_metric_chart(
                        overview.get("availability_groups", pd.DataFrame()),
                        group_name,
                        "availability_pct",
                        group_name,
                        "Availability",
                    )
                )
                for group_name in AVAILABILITY_GROUP_ORDER
            },
            "area_contribution": _chart_payload(
                area_contribution_chart(overview.get("area_contribution", pd.DataFrame()), "Contribution by cut")
            ),
        },
    }

    base_payload["mine"] = {
        "filters": {
            "area_options": area_options,
            "group_options": filter_group_options,
            "equipment_options": equipment_options,
            "selected_areas": selected_areas,
            "selected_groups": selected_groups,
            "selected_equipment": selected_equipment,
            "unit_options": mine_unit_ids,
            "selected_unit": resolved_mine_unit,
            "selected_heat_metric": resolved_mine_heat_metric,
        },
        "charts": {
            "mine_production": _chart_payload(
                mine_production_combo(mine_page.get("mine_daily", pd.DataFrame()), "BCM moved, ore mined, and ratio")
            ),
            "mine_volume": _chart_payload(
                mine_volume_trend(mine_page.get("mine_daily", pd.DataFrame()), "Waste, ore BCM, and tonnes")
            ),
            "stripping_ratio": _chart_payload(
                stripping_ratio_trend(mine_page.get("mine_daily", pd.DataFrame()), "Stripping ratio trend")
            ),
            "utilization_groups": {
                group_name: _chart_payload(
                    group_metric_chart(
                        mine_page.get("utilization_daily", pd.DataFrame()),
                        group_name,
                        "utilization_pct",
                        group_name,
                        "Utilization",
                    )
                )
                for group_name in AVAILABILITY_GROUP_ORDER
            },
            "diesel_groups": _chart_payload(
                diesel_stacked_chart(mine_page.get("diesel_groups", pd.DataFrame()), "Daily diesel by fleet group")
            ),
            "top_diesel": _chart_payload(
                rank_bar(
                    mine_page.get("top_diesel", pd.DataFrame()),
                    "equipment_id",
                    "diesel_l",
                    "Highest diesel units",
                    color=TOKENS["diesel"],
                )
            ),
            "bottom_diesel": _chart_payload(
                rank_bar(
                    mine_page.get("bottom_diesel", pd.DataFrame()),
                    "equipment_id",
                    "diesel_l",
                    "Lowest diesel units",
                    color=TOKENS["plant"],
                )
            ),
            "unit_timeline": _chart_payload(
                unit_timeline_chart(
                    mine_timeline,
                    f"{resolved_mine_unit} operating profile" if resolved_mine_unit else "Unit operating profile",
                )
            ),
            "unit_heatmap": _chart_payload(
                unit_heatmap(
                    mine_heatmap,
                    resolved_mine_heat_metric,
                    f"Lowest performers by {resolved_mine_heat_metric.replace('_', ' ')}",
                )
            ),
        },
        "tables": {
            "unit_ranking": _table_payload(
                mine_ranking,
                [
                    "equipment_id",
                    "equipment_class",
                    "equipment_subtype",
                    "model",
                    "area_name",
                    "availability_pct",
                    "utilization_pct",
                    "diesel_l",
                    "days_reported",
                ],
            )
        },
    }

    base_payload["plant"] = {
        "charts": {
            "feed_throughput": _chart_payload(
                plant_feed_throughput_combo(plant_page.get("plant_daily", pd.DataFrame()), "Feed tonnes and throughput")
            ),
            "grade_recovery": _chart_payload(
                grade_recovery_scatter(plant_page.get("plant_rows", pd.DataFrame()), "Feed grade versus recovery")
            ),
            "metal_production": _chart_payload(
                metal_production_trend(plant_page.get("plant_daily", pd.DataFrame()), "Metal produced and recovery")
            ),
            "downtime_availability": _chart_payload(
                downtime_availability_combo(plant_page.get("plant_daily", pd.DataFrame()), "Downtime and availability")
            ),
        },
        "tables": {
            "daily_operating": _table_payload(
                plant_page.get("daily_table", pd.DataFrame()),
                [
                    "date",
                    "feed_tonnes",
                    "feed_grade_pct",
                    "throughput_tph",
                    "recovery_pct",
                    "metal_produced_t",
                    "availability_pct",
                    "unplanned_downtime_h",
                ],
            )
        },
    }

    base_payload["fleet"] = {
        "filters": {
            "unit_options": fleet_unit_ids,
            "selected_unit": resolved_fleet_unit,
            "selected_heat_metric": resolved_fleet_heat_metric,
        },
        "charts": {
            "availability_groups": {
                group_name: _chart_payload(
                    group_metric_chart(
                        fleet_page.get("availability_daily", pd.DataFrame()),
                        group_name,
                        "availability_pct",
                        group_name,
                        "Availability",
                    )
                )
                for group_name in AVAILABILITY_GROUP_ORDER
            },
            "diesel_groups": _chart_payload(
                diesel_stacked_chart(fleet_page.get("diesel_groups", pd.DataFrame()), "Diesel mix by fleet grouping")
            ),
            "lowest_availability": _chart_payload(
                rank_bar(
                    fleet_ranking.sort_values("availability_pct", ascending=True).head(10),
                    "equipment_id",
                    "availability_pct",
                    "Lowest availability units",
                    color=TOKENS["bad"],
                )
            ),
            "unit_timeline": _chart_payload(
                unit_timeline_chart(
                    fleet_timeline,
                    f"{resolved_fleet_unit} trend" if resolved_fleet_unit else "Fleet unit trend",
                )
            ),
            "unit_heatmap": _chart_payload(
                unit_heatmap(
                    fleet_heatmap,
                    resolved_fleet_heat_metric,
                    f"Fleet heatmap: {resolved_fleet_heat_metric.replace('_', ' ')}",
                )
            ),
        },
        "tables": {
            "unit_ranking": _table_payload(
                fleet_ranking,
                [
                    "equipment_id",
                    "equipment_class",
                    "equipment_subtype",
                    "model",
                    "area_name",
                    "availability_pct",
                    "utilization_pct",
                    "diesel_l",
                    "days_reported",
                ],
            )
        },
    }

    base_payload["data_quality"] = {
        "charts": {
            "issue_severity": _chart_payload(
                issue_severity_chart(workbook.quality.get("issues", pd.DataFrame()), "Issue severity")
            ),
            "coverage_area": _chart_payload(
                coverage_heatmap(
                    workbook.quality.get("coverage_area", pd.DataFrame()),
                    "area_name",
                    "records",
                    "Coverage by cut",
                )
            ),
            "coverage_fleet": _chart_payload(
                coverage_heatmap(
                    workbook.quality.get("coverage_fleet", pd.DataFrame()).head(300),
                    "equipment_id",
                    "records",
                    "Coverage by equipment",
                )
            ),
        },
        "tables": {
            "health_summary": _table_payload(workbook.quality.get("health_summary", pd.DataFrame())),
            "issues": _table_payload(workbook.quality.get("issues", pd.DataFrame())),
            "last_available": _table_payload(workbook.quality.get("last_available", pd.DataFrame())),
            "duplicates": _table_payload(workbook.quality.get("duplicates", pd.DataFrame())),
            "missing_dates_area": _table_payload(workbook.quality.get("missing_dates_area", pd.DataFrame())),
            "missing_dates_fleet": _table_payload(workbook.quality.get("missing_dates_fleet", pd.DataFrame()), max_rows=25),
            "null_pct": _table_payload(workbook.quality.get("null_pct", pd.DataFrame())),
            "kpi_traceability": _table_payload(build_kpi_availability_report(workbook.sheets)),
            "schema_overview": _table_payload(build_schema_overview()),
            "field_guide": _table_payload(build_field_guide()),
        },
    }

    return base_payload
