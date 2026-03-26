from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from openpyxl.styles import Font, PatternFill
from openpyxl.worksheet.datavalidation import DataValidation

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.io_excel import (
    AREA_NAMES,
    EQUIPMENT_CLASSES,
    EQUIPMENT_SUBTYPES,
    FLEET_ROSTER,
    PLANT_ID,
    PLANT_NAME,
    SCHEMA_VERSION,
    SITE_ID,
    SITE_NAME,
    TIMEZONE,
    WORKBOOK_SCHEMA,
    build_field_guide,
    build_lookup_frame,
    build_schema_overview,
    template_columns,
)


DATA_DIR = ROOT / "data"
TEMPLATE_PATH = DATA_DIR / "mine_productivity_input_template.xlsx"
SAMPLE_PATH = DATA_DIR / "sample_mine_productivity_input.xlsx"
DEFAULT_PATH = DATA_DIR / "mine_productivity_input.xlsx"


def _equipment_master() -> pd.DataFrame:
    rows: list[dict[str, str | int]] = []
    for idx in range(1, 7):
        rows.append({
            "equipment_id": f"Ex {idx}",
            "equipment_class": "excavator",
            "equipment_subtype": "excavator",
            "model": "Mining Excavator",
            "active_flag": 1,
        })
    for idx in range(1, 25):
        rows.append({
            "equipment_id": f"RDE {idx:02d}",
            "equipment_class": "truck",
            "equipment_subtype": "truck_220t",
            "model": "Hitachi 5500",
            "active_flag": 1,
        })
    for idx in range(1, 28):
        rows.append({
            "equipment_id": f"RD {idx:02d}",
            "equipment_class": "truck",
            "equipment_subtype": "truck_100t",
            "model": "CAT 777",
            "active_flag": 1,
        })
    for idx in range(1, 25):
        rows.append({
            "equipment_id": f"ADT {idx:02d}",
            "equipment_class": "truck",
            "equipment_subtype": "truck_60t",
            "model": "Volvo AH60",
            "active_flag": 1,
        })
    for idx in range(1, 7):
        rows.append({
            "equipment_id": f"DR {idx:02d}",
            "equipment_class": "ancillary",
            "equipment_subtype": "drill",
            "model": "Drill Rig",
            "active_flag": 1,
        })
    for idx in range(1, 4):
        rows.append({
            "equipment_id": f"DZ {idx:02d}",
            "equipment_class": "ancillary",
            "equipment_subtype": "dozer",
            "model": "Dozer",
            "active_flag": 1,
        })
    for idx in range(1, 5):
        rows.append({
            "equipment_id": f"GR {idx:02d}",
            "equipment_class": "ancillary",
            "equipment_subtype": "grader",
            "model": "Grader",
            "active_flag": 1,
        })
    return pd.DataFrame(rows)


def _metadata_frame(last_refresh_ts: datetime | None = None) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "schema_version": SCHEMA_VERSION,
                "site_id": SITE_ID,
                "site_name": SITE_NAME,
                "plant_id": PLANT_ID,
                "plant_name": PLANT_NAME,
                "timezone": TIMEZONE,
                "last_refresh_ts": last_refresh_ts or datetime.now().replace(microsecond=0),
            }
        ],
        columns=template_columns("metadata"),
    )


def _readme_frame() -> pd.DataFrame:
    field_guide = build_field_guide()
    lines = [
        {"section": "Overview", "detail": "Official workbook template for the Mining Operations Executive Dashboard."},
        {"section": "Schema version", "detail": SCHEMA_VERSION},
        {"section": "Operating model", "detail": f"Single site ({SITE_NAME}), single plant ({PLANT_NAME}), three cuts ({', '.join(AREA_NAMES)})."},
        {"section": "Expected sheets", "detail": ", ".join(WORKBOOK_SCHEMA.keys())},
        {"section": "daily_mine grain", "detail": "One row per date per area_name."},
        {"section": "daily_plant grain", "detail": "One row per date for the plant."},
        {"section": "daily_fleet grain", "detail": "One row per date per equipment_id."},
        {"section": "Percent inputs", "detail": "Fields ending in _pct accept either 0-1 or 0-100. The loader normalizes 87 to 0.87."},
        {"section": "Required categories", "detail": f"equipment_class: {', '.join(EQUIPMENT_CLASSES)} | equipment_subtype: {', '.join(EQUIPMENT_SUBTYPES)}"},
        {"section": "Validation", "detail": "Uploads fail fast on missing required sheets, schema mismatch, bad categories, invalid dates, and duplicate keys."},
        {"section": "How to use", "detail": "Use the blank template workbook for data entry. Use the sample workbook as a reference for formatting and expected values."},
    ]
    lines.extend(
        {
            "section": f"Field: {row.field}",
            "detail": f"Sheet={row.sheet} | Required={row.required} | Type={row.type} | Unit={row.unit} | Metrics={row.used_by_metrics or 'n/a'} | {row.description}",
        }
        for row in field_guide.itertuples(index=False)
    )
    return pd.DataFrame(lines)


def _template_frames() -> dict[str, pd.DataFrame]:
    return {
        "README": _readme_frame(),
        "metadata": _metadata_frame(),
        "daily_mine": pd.DataFrame(columns=template_columns("daily_mine")),
        "daily_plant": pd.DataFrame(columns=template_columns("daily_plant")),
        "daily_fleet": pd.DataFrame(columns=template_columns("daily_fleet")),
        "lookups": build_lookup_frame(),
    }


def _sample_daily_mine(days: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    start_date = date.today() - timedelta(days=days - 1)
    area_profile = {
        "Cut 4 - 1": {"waste": 7600, "ore": 2450, "density": 2.18},
        "Cut 4 - 2": {"waste": 5900, "ore": 3200, "density": 2.24},
        "Cut 4 - 3": {"waste": 4700, "ore": 3900, "density": 2.28},
    }
    rows: list[dict[str, object]] = []
    for offset in range(days):
        current_date = start_date + timedelta(days=offset)
        for area_name in AREA_NAMES:
            profile = area_profile[area_name]
            waste_bcm = max(0.0, rng.normal(profile["waste"], 430))
            ore_bcm = max(160.0, rng.normal(profile["ore"], 260))
            bcm_moved = waste_bcm + ore_bcm
            ore_mined_t = max(0.0, ore_bcm * rng.normal(profile["density"], 0.05))
            if rng.random() < 0.025:
                ore_bcm *= 0.55
                ore_mined_t *= 0.55
                bcm_moved = waste_bcm + ore_bcm
            rows.append(
                {
                    "date": current_date,
                    "area_name": area_name,
                    "bcm_moved": round(bcm_moved, 2),
                    "waste_bcm": round(waste_bcm, 2),
                    "ore_bcm": round(ore_bcm, 2),
                    "ore_mined_t": round(ore_mined_t, 2),
                }
            )
    return pd.DataFrame(rows, columns=template_columns("daily_mine"))


def _sample_daily_plant(days: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(126)
    start_date = date.today() - timedelta(days=days - 1)
    rows: list[dict[str, object]] = []
    for offset in range(days):
        current_date = start_date + timedelta(days=offset)
        feed_tonnes = max(0.0, rng.normal(6900, 420))
        feed_grade_pct = float(np.clip(rng.normal(0.0078, 0.0005), 0.0045, 0.0105))
        throughput_tph = max(250.0, rng.normal(610, 22))
        recovery_pct = float(np.clip(rng.normal(0.91, 0.02), 0.76, 0.97))
        availability_pct = float(np.clip(rng.normal(0.93, 0.025), 0.75, 0.99))
        unplanned_downtime_h = max(0.0, rng.normal((1 - availability_pct) * 24, 1.1))
        metal_produced_t = max(0.0, feed_tonnes * feed_grade_pct * recovery_pct)
        if rng.random() < 0.04:
            availability_pct *= 0.86
            unplanned_downtime_h *= 1.55
            throughput_tph *= 0.84
            metal_produced_t *= 0.85
        rows.append(
            {
                "date": current_date,
                "feed_tonnes": round(feed_tonnes, 2),
                "feed_grade_pct": round(feed_grade_pct, 5),
                "throughput_tph": round(throughput_tph, 2),
                "recovery_pct": round(recovery_pct, 4),
                "metal_produced_t": round(metal_produced_t, 3),
                "availability_pct": round(availability_pct, 4),
                "unplanned_downtime_h": round(unplanned_downtime_h, 2),
            }
        )
    return pd.DataFrame(rows, columns=template_columns("daily_plant"))


def _sample_daily_fleet(days: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(84)
    start_date = date.today() - timedelta(days=days - 1)
    equipment = _equipment_master()
    profiles = {
        "excavator": {"availability": 0.89, "utilization": 0.76, "diesel": 2800},
        "truck_220t": {"availability": 0.88, "utilization": 0.78, "diesel": 1850},
        "truck_100t": {"availability": 0.87, "utilization": 0.75, "diesel": 1050},
        "truck_60t": {"availability": 0.86, "utilization": 0.72, "diesel": 680},
        "drill": {"availability": 0.81, "utilization": 0.63, "diesel": 760},
        "dozer": {"availability": 0.84, "utilization": 0.67, "diesel": 1180},
        "grader": {"availability": 0.85, "utilization": 0.66, "diesel": 820},
    }
    rows: list[dict[str, object]] = []
    for offset in range(days):
        current_date = start_date + timedelta(days=offset)
        for unit in equipment.itertuples(index=False):
            profile = profiles[unit.equipment_subtype]
            availability = float(np.clip(rng.normal(profile["availability"], 0.04), 0.55, 0.99))
            utilization = float(np.clip(rng.normal(profile["utilization"], 0.05), 0.28, availability))
            diesel_l = max(0.0, rng.normal(profile["diesel"], profile["diesel"] * 0.12))
            area_name = rng.choice(AREA_NAMES)
            if rng.random() < 0.03:
                availability *= 0.72
                utilization *= 0.62
            if rng.random() < 0.03:
                diesel_l *= 1.18
            rows.append(
                {
                    "date": current_date,
                    "equipment_id": unit.equipment_id,
                    "equipment_class": unit.equipment_class,
                    "equipment_subtype": unit.equipment_subtype,
                    "model": unit.model,
                    "area_name": area_name,
                    "availability_pct": round(availability, 4),
                    "utilization_pct": round(utilization, 4),
                    "diesel_l": round(diesel_l, 2),
                }
            )
    return pd.DataFrame(rows, columns=template_columns("daily_fleet"))


def build_sample_workbook() -> dict[str, pd.DataFrame]:
    refreshed = datetime.now().replace(microsecond=0)
    return {
        "README": _readme_frame(),
        "metadata": _metadata_frame(refreshed),
        "daily_mine": _sample_daily_mine(),
        "daily_plant": _sample_daily_plant(),
        "daily_fleet": _sample_daily_fleet(),
        "lookups": build_lookup_frame(),
    }


def _write_workbook(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=sheet_name, index=False)
        workbook = writer.book
        header_fill = PatternFill(fill_type="solid", fgColor="EEE7DB")
        header_font = Font(bold=True, color="1E2732")
        for sheet_name, frame in sheets.items():
            worksheet = writer.sheets[sheet_name]
            for cell in worksheet[1]:
                cell.fill = header_fill
                cell.font = header_font
            worksheet.freeze_panes = "A2"
            worksheet.auto_filter.ref = worksheet.dimensions
            for idx, column_name in enumerate(frame.columns, start=1):
                width = min(max(len(str(column_name)) + 4, 16), 28)
                worksheet.column_dimensions[chr(64 + idx)].width = width

        lookups_ws = writer.sheets["lookups"]
        template_rows = 5000
        validation_targets = {
            "daily_mine": {"area_name": "A"},
            "daily_fleet": {
                "equipment_id": "B",
                "equipment_class": "C",
                "equipment_subtype": "D",
                "area_name": "F",
            },
        }
        lookup_ranges = {
            "area_name": f"=lookups!$C$2:$C${1 + len(AREA_NAMES)}",
            "equipment_id": f"=lookups!$C${2 + len(AREA_NAMES)}:$C${1 + len(AREA_NAMES) + len(FLEET_ROSTER)}",
            "equipment_class": f"=lookups!$C${2 + len(AREA_NAMES) + len(FLEET_ROSTER)}:$C${1 + len(AREA_NAMES) + len(FLEET_ROSTER) + len(EQUIPMENT_CLASSES)}",
            "equipment_subtype": f"=lookups!$C${2 + len(AREA_NAMES) + len(FLEET_ROSTER) + len(EQUIPMENT_CLASSES)}:$C${1 + len(AREA_NAMES) + len(FLEET_ROSTER) + len(EQUIPMENT_CLASSES) + len(EQUIPMENT_SUBTYPES)}",
        }
        for sheet_name, columns in validation_targets.items():
            ws = writer.sheets[sheet_name]
            for field_name, column_letter in columns.items():
                formula = lookup_ranges[field_name]
                validation = DataValidation(type="list", formula1=formula, allow_blank=False)
                validation.prompt = f"Choose a valid {field_name} value from the lookup list."
                validation.error = f"Use a supported {field_name} value from the official template."
                ws.add_data_validation(validation)
                validation.add(f"{column_letter}2:{column_letter}{template_rows}")


if __name__ == "__main__":
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    template = _template_frames()
    sample = build_sample_workbook()
    _write_workbook(TEMPLATE_PATH, template)
    _write_workbook(SAMPLE_PATH, sample)
    _write_workbook(DEFAULT_PATH, sample)
    print(f"Template workbook written: {TEMPLATE_PATH}")
    print(f"Sample workbook written:   {SAMPLE_PATH}")
    print(f"Default workbook written:  {DEFAULT_PATH}")
