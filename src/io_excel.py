from __future__ import annotations

import io
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Literal

import pandas as pd


SCHEMA_VERSION = "2026.03"
CANONICAL_SHEETS = ["metadata", "daily_mine", "daily_plant", "daily_fleet"]
OPTIONAL_SHEETS = ["README", "lookups"]
LEGACY_SHEETS = {
    "dim_site",
    "dim_area",
    "dim_plant",
    "dim_equipment",
    "fact_daily_mine",
    "fact_daily_fleet_unit",
    "fact_daily_plant",
}

SITE_ID = "S1"
SITE_NAME = "Cut 4 Operations"
PLANT_ID = "P1"
PLANT_NAME = "Cut 4 Process Plant"
TIMEZONE = "America/Denver"
AREA_NAMES = ["Cut 4 - 1", "Cut 4 - 2", "Cut 4 - 3"]
EQUIPMENT_CLASSES = ["excavator", "truck", "ancillary"]
EQUIPMENT_SUBTYPES = ["excavator", "truck_220t", "truck_100t", "truck_60t", "drill", "dozer", "grader"]

FLEET_ROSTER = [
    *(f"Ex {idx}" for idx in range(1, 7)),
    *(f"RDE {idx:02d}" for idx in range(1, 25)),
    *(f"RD {idx:02d}" for idx in range(1, 28)),
    *(f"ADT {idx:02d}" for idx in range(1, 25)),
    *(f"DR {idx:02d}" for idx in range(1, 7)),
    *(f"DZ {idx:02d}" for idx in range(1, 4)),
    *(f"GR {idx:02d}" for idx in range(1, 5)),
]


@dataclass(frozen=True)
class FieldSpec:
    name: str
    dtype: Literal["text", "date", "number", "percent", "datetime"]
    unit: str
    required: bool
    description: str
    grain_role: str = "attribute"
    allowed_values: tuple[str, ...] = ()
    min_value: float | None = None
    max_value: float | None = None
    example: str = ""
    ui_metrics: tuple[str, ...] = ()


@dataclass(frozen=True)
class SheetSpec:
    name: str
    description: str
    grain: str
    required: bool
    fields: tuple[FieldSpec, ...]


@dataclass
class WorkbookData:
    sheets: Dict[str, pd.DataFrame] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    quality: Dict[str, pd.DataFrame] = field(default_factory=dict)
    source_name: str = ""
    schema_version: str = ""
    source_kind: str = "canonical"


WORKBOOK_SCHEMA: dict[str, SheetSpec] = {
    "metadata": SheetSpec(
        name="metadata",
        description="Single-row workbook metadata and schema versioning.",
        grain="one row per workbook",
        required=True,
        fields=(
            FieldSpec("schema_version", "text", "version", True, "Workbook schema version.", example=SCHEMA_VERSION),
            FieldSpec("site_id", "text", "id", True, "Site identifier.", example=SITE_ID),
            FieldSpec("site_name", "text", "name", True, "Display site name.", example=SITE_NAME),
            FieldSpec("plant_id", "text", "id", True, "Plant identifier.", example=PLANT_ID),
            FieldSpec("plant_name", "text", "name", True, "Display plant name.", example=PLANT_NAME),
            FieldSpec("timezone", "text", "tz", False, "IANA timezone for the site.", example=TIMEZONE),
            FieldSpec("last_refresh_ts", "datetime", "timestamp", False, "Timestamp of workbook export or refresh.", example="2026-03-26T06:00:00"),
        ),
    ),
    "daily_mine": SheetSpec(
        name="daily_mine",
        description="Daily mine movement and ore production by cut.",
        grain="date + area_name",
        required=True,
        fields=(
            FieldSpec("date", "date", "date", True, "Calendar date in YYYY-MM-DD format.", grain_role="key", example="2026-03-26"),
            FieldSpec("area_name", "text", "name", True, "Mine cut name.", grain_role="key", allowed_values=tuple(AREA_NAMES), example="Cut 4 - 1"),
            FieldSpec("bcm_moved", "number", "bcm", True, "Total material moved.", min_value=0, example="11250", ui_metrics=("BCM moved",)),
            FieldSpec("waste_bcm", "number", "bcm", True, "Waste movement in bank cubic metres.", min_value=0, example="7600", ui_metrics=("Stripping ratio",)),
            FieldSpec("ore_bcm", "number", "bcm", True, "Ore movement in bank cubic metres.", min_value=0, example="3650", ui_metrics=("Stripping ratio",)),
            FieldSpec("ore_mined_t", "number", "t", True, "Ore mined in tonnes.", min_value=0, example="8230", ui_metrics=("Ore mined (t)",)),
        ),
    ),
    "daily_plant": SheetSpec(
        name="daily_plant",
        description="Daily plant performance for the single processing plant.",
        grain="date",
        required=True,
        fields=(
            FieldSpec("date", "date", "date", True, "Calendar date in YYYY-MM-DD format.", grain_role="key", example="2026-03-26"),
            FieldSpec("feed_tonnes", "number", "t", True, "Plant feed tonnes.", min_value=0, example="6950", ui_metrics=("Feed tonnes",)),
            FieldSpec("feed_grade_pct", "percent", "fraction or %", True, "Feed grade. Accepts 0-1 or 0-100.", min_value=0, max_value=100, example="0.78%", ui_metrics=("Feed grade",)),
            FieldSpec("throughput_tph", "number", "t/h", True, "Plant throughput rate.", min_value=0, example="615", ui_metrics=("Throughput",)),
            FieldSpec("recovery_pct", "percent", "fraction or %", True, "Metallurgical recovery. Accepts 0-1 or 0-100.", min_value=0, max_value=100, example="91%", ui_metrics=("Recovery",)),
            FieldSpec("metal_produced_t", "number", "t", True, "Produced metal in tonnes.", min_value=0, example="49.3", ui_metrics=("Metal produced",)),
            FieldSpec("availability_pct", "percent", "fraction or %", True, "Plant availability. Accepts 0-1 or 0-100.", min_value=0, max_value=100, example="93%", ui_metrics=("Plant availability",)),
            FieldSpec("unplanned_downtime_h", "number", "h", True, "Unplanned downtime hours.", min_value=0, example="1.8", ui_metrics=("Downtime",)),
        ),
    ),
    "daily_fleet": SheetSpec(
        name="daily_fleet",
        description="Daily fleet performance by equipment unit.",
        grain="date + equipment_id",
        required=True,
        fields=(
            FieldSpec("date", "date", "date", True, "Calendar date in YYYY-MM-DD format.", grain_role="key", example="2026-03-26"),
            FieldSpec("equipment_id", "text", "id", True, "Equipment identifier.", grain_role="key", allowed_values=tuple(FLEET_ROSTER), example="Ex 1"),
            FieldSpec("equipment_class", "text", "category", True, "High-level fleet class.", allowed_values=tuple(EQUIPMENT_CLASSES), example="excavator"),
            FieldSpec("equipment_subtype", "text", "category", True, "Subtype for grouping and validation.", allowed_values=tuple(EQUIPMENT_SUBTYPES), example="excavator"),
            FieldSpec("model", "text", "name", False, "Equipment model name.", example="Mining Excavator"),
            FieldSpec("area_name", "text", "name", True, "Assigned operating area.", allowed_values=tuple(AREA_NAMES), example="Cut 4 - 2"),
            FieldSpec("availability_pct", "percent", "fraction or %", True, "Unit availability. Accepts 0-1 or 0-100.", min_value=0, max_value=100, example="88%", ui_metrics=("Fleet availability",)),
            FieldSpec("utilization_pct", "percent", "fraction or %", True, "Unit utilization. Accepts 0-1 or 0-100.", min_value=0, max_value=100, example="73%", ui_metrics=("Fleet utilization",)),
            FieldSpec("diesel_l", "number", "L", True, "Diesel consumption in litres.", min_value=0, example="1850", ui_metrics=("Diesel consumption",)),
        ),
    ),
    "lookups": SheetSpec(
        name="lookups",
        description="Allowed area, equipment, class, and subtype values used by template dropdowns and validation.",
        grain="lookup rows",
        required=False,
        fields=(
            FieldSpec("lookup_type", "text", "category", True, "Lookup bucket such as area_name or equipment_id.", example="area_name"),
            FieldSpec("code", "text", "id", True, "Lookup key or code.", example="CUT4_1"),
            FieldSpec("value", "text", "value", True, "Allowed value as used in the daily sheets.", example="Cut 4 - 1"),
            FieldSpec("label", "text", "label", False, "Friendly display label.", example="Cut 4 - 1"),
            FieldSpec("notes", "text", "text", False, "Optional description or unit note.", example="Mine cut"),
        ),
    ),
}


def build_schema_overview() -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for sheet_name, spec in WORKBOOK_SCHEMA.items():
        rows.append(
            {
                "sheet": sheet_name,
                "required": spec.required,
                "grain": spec.grain,
                "description": spec.description,
                "field_count": len(spec.fields),
            }
        )
    return pd.DataFrame(rows)


def build_field_guide() -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for sheet_name, spec in WORKBOOK_SCHEMA.items():
        for field in spec.fields:
            rows.append(
                {
                    "sheet": sheet_name,
                    "field": field.name,
                    "type": field.dtype,
                    "unit": field.unit,
                    "required": field.required,
                    "grain_role": field.grain_role,
                    "allowed_values": ", ".join(field.allowed_values),
                    "range": _range_label(field.min_value, field.max_value),
                    "used_by_metrics": ", ".join(field.ui_metrics),
                    "description": field.description,
                    "example": field.example,
                }
            )
    return pd.DataFrame(rows)


def template_columns(sheet_name: str) -> list[str]:
    spec = WORKBOOK_SCHEMA[sheet_name]
    return [field.name for field in spec.fields]


def build_lookup_frame() -> pd.DataFrame:
    rows: List[Dict[str, str]] = []
    for area_name in AREA_NAMES:
        rows.append({"lookup_type": "area_name", "code": area_name.upper().replace(" ", "_").replace("-", ""), "value": area_name, "label": area_name, "notes": "Mine cut"})
    for equipment_id in FLEET_ROSTER:
        rows.append({"lookup_type": "equipment_id", "code": equipment_id.replace(" ", "_"), "value": equipment_id, "label": equipment_id, "notes": "Allowed fleet unit"})
    for equipment_class in EQUIPMENT_CLASSES:
        rows.append({"lookup_type": "equipment_class", "code": equipment_class, "value": equipment_class, "label": equipment_class.title(), "notes": "Allowed fleet class"})
    for subtype in EQUIPMENT_SUBTYPES:
        rows.append({"lookup_type": "equipment_subtype", "code": subtype, "value": subtype, "label": subtype, "notes": "Allowed fleet subtype"})
    return pd.DataFrame(rows, columns=template_columns("lookups"))


def _range_label(min_value: float | None, max_value: float | None) -> str:
    if min_value is None and max_value is None:
        return ""
    if min_value is not None and max_value is not None:
        return f"{min_value} to {max_value}"
    if min_value is not None:
        return f">= {min_value}"
    return f"<= {max_value}"


def to_snake_case(value: str) -> str:
    value = value.strip().replace("%", "pct").replace("/", "_")
    value = re.sub(r"[^0-9a-zA-Z]+", "_", value)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_").lower()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = {column: to_snake_case(str(column)) for column in df.columns}
    out = df.rename(columns=renamed).copy()
    for col in out.columns:
        if pd.api.types.is_object_dtype(out[col]):
            out[col] = out[col].astype(str).str.strip()
            out.loc[out[col].isin(["", "nan", "None", "NaT"]), col] = pd.NA
    return out


def _coerce_field_types(df: pd.DataFrame, spec: SheetSpec, warnings: List[str], issues: List[Dict[str, Any]]) -> pd.DataFrame:
    out = df.copy()
    for field in spec.fields:
        if field.name not in out.columns:
            continue
        if field.dtype in {"number", "percent"}:
            before = int(out[field.name].notna().sum())
            out[field.name] = pd.to_numeric(out[field.name], errors="coerce")
            invalid = before - int(out[field.name].notna().sum())
            if invalid > 0:
                warnings.append(f"Sheet '{spec.name}' column '{field.name}' had {invalid} non-numeric values converted to null.")
                issues.append(_issue("warning", spec.name, field.name, invalid, "invalid_type", f"Non-numeric values were converted to null in {field.name}.", "Use numbers only for this field."))
        elif field.dtype == "date":
            before = int(out[field.name].notna().sum())
            out[field.name] = pd.to_datetime(out[field.name], errors="coerce").dt.date
            invalid = before - int(out[field.name].notna().sum())
            if invalid > 0:
                warnings.append(f"Sheet '{spec.name}' column '{field.name}' had {invalid} invalid dates converted to null.")
                issues.append(_issue("warning", spec.name, field.name, invalid, "invalid_date", f"Invalid dates were converted to null in {field.name}.", "Use YYYY-MM-DD dates."))
        elif field.dtype == "datetime":
            before = int(out[field.name].notna().sum())
            out[field.name] = pd.to_datetime(out[field.name], errors="coerce")
            invalid = before - int(out[field.name].notna().sum())
            if invalid > 0:
                warnings.append(f"Sheet '{spec.name}' column '{field.name}' had {invalid} invalid timestamps converted to null.")
                issues.append(_issue("warning", spec.name, field.name, invalid, "invalid_datetime", f"Invalid timestamps were converted to null in {field.name}.", "Use ISO-like timestamps such as 2026-03-26T06:00:00."))
    return out


def _normalize_percent_columns(df: pd.DataFrame, spec: SheetSpec, warnings: List[str], issues: List[Dict[str, Any]]) -> pd.DataFrame:
    out = df.copy()
    for field in spec.fields:
        if field.dtype != "percent" or field.name not in out.columns:
            continue
        series = pd.to_numeric(out[field.name], errors="coerce")
        normalize_mask = series.notna() & (series > 1) & (series <= 100)
        invalid_mask = series.notna() & ((series < 0) | (series > 100))
        normalized_rows = int(normalize_mask.sum())
        invalid_rows = int(invalid_mask.sum())
        if normalized_rows > 0:
            out.loc[normalize_mask, field.name] = series.loc[normalize_mask] / 100
            warnings.append(f"Sheet '{spec.name}' column '{field.name}' normalized {normalized_rows} values from 0-100 to 0-1 scale.")
            issues.append(_issue("warning", spec.name, field.name, normalized_rows, "normalized_percent", f"{field.name} values were normalized from 0-100 into 0-1 scale.", "Keep percentage inputs consistently in either fraction or whole percent form."))
        if invalid_rows > 0:
            out.loc[invalid_mask, field.name] = pd.NA
            warnings.append(f"Sheet '{spec.name}' column '{field.name}' had {invalid_rows} out-of-range percentage values converted to null.")
            issues.append(_issue("error", spec.name, field.name, invalid_rows, "out_of_range_percent", f"{field.name} values outside 0-100 were converted to null.", "Keep percentages between 0-1 or 0-100."))
    return out


def _validate_required_columns(sheet_name: str, df: pd.DataFrame, errors: List[str], issues: List[Dict[str, Any]]) -> None:
    spec = WORKBOOK_SCHEMA[sheet_name]
    missing = [field.name for field in spec.fields if field.required and field.name not in df.columns]
    if missing:
        errors.append(f"Sheet '{sheet_name}' is missing required columns: {', '.join(missing)}.")
        for column in missing:
            issues.append(_issue("error", sheet_name, column, pd.NA, "missing_column", f"Required column '{column}' is missing.", "Add the missing required column to the uploaded workbook."))


def _validate_required_values(sheet_name: str, df: pd.DataFrame, warnings: List[str], issues: List[Dict[str, Any]]) -> None:
    spec = WORKBOOK_SCHEMA[sheet_name]
    for field in spec.fields:
        if not field.required or field.name not in df.columns:
            continue
        missing_rows = int(df[field.name].isna().sum())
        if missing_rows > 0:
            warnings.append(f"Sheet '{sheet_name}' column '{field.name}' has {missing_rows} blank required values.")
            issues.append(_issue("warning", sheet_name, field.name, missing_rows, "missing_required_values", f"Required field '{field.name}' has blank values.", "Populate all required cells for this field."))


def _validate_ranges(sheet_name: str, df: pd.DataFrame, issues: List[Dict[str, Any]]) -> None:
    spec = WORKBOOK_SCHEMA[sheet_name]
    for field in spec.fields:
        if field.name not in df.columns or field.dtype not in {"number", "percent"}:
            continue
        series = pd.to_numeric(df[field.name], errors="coerce")
        if field.min_value is not None:
            low_count = int((series < field.min_value).fillna(False).sum())
            if low_count > 0:
                issues.append(_issue("warning", sheet_name, field.name, low_count, "below_minimum", f"Values in '{field.name}' fell below {field.min_value}.", "Review whether the recorded values or units are correct."))
        if field.max_value is not None and field.dtype != "percent":
            high_count = int((series > field.max_value).fillna(False).sum())
            if high_count > 0:
                issues.append(_issue("warning", sheet_name, field.name, high_count, "above_maximum", f"Values in '{field.name}' exceeded {field.max_value}.", "Review whether the recorded values or units are correct."))


def _validate_allowed_values(sheet_name: str, df: pd.DataFrame, issues: List[Dict[str, Any]], lookups: pd.DataFrame | None) -> None:
    spec = WORKBOOK_SCHEMA[sheet_name]
    lookup_map = _lookup_sets(lookups)
    for field in spec.fields:
        if field.name not in df.columns:
            continue
        allowed_values = set(field.allowed_values)
        if field.name in lookup_map:
            allowed_values = set(lookup_map[field.name])
        if not allowed_values:
            continue
        observed = df[field.name].dropna().astype(str)
        invalid_count = int((~observed.isin(allowed_values)).sum())
        if invalid_count > 0:
            issues.append(_issue("error", sheet_name, field.name, invalid_count, "unexpected_value", f"Unexpected values were found in '{field.name}'.", "Use the values from the template lookups or field guide exactly as provided."))


def _lookup_sets(lookups: pd.DataFrame | None) -> dict[str, list[str]]:
    if lookups is None or lookups.empty or not {"lookup_type", "value"}.issubset(lookups.columns):
        return {}
    result: dict[str, list[str]] = {}
    for lookup_type, group in lookups.groupby("lookup_type"):
        result[str(lookup_type)] = sorted(group["value"].dropna().astype(str).unique().tolist())
    return result


def _issue(
    severity: Literal["error", "warning", "info"],
    sheet: str,
    column: str | None,
    rows_affected: Any,
    issue_type: str,
    message: str,
    recommendation: str,
) -> Dict[str, Any]:
    return {
        "severity": severity,
        "sheet": sheet,
        "column": column or "",
        "rows_affected": rows_affected,
        "issue_type": issue_type,
        "message": message,
        "recommendation": recommendation,
    }


def _parse_excel_source(source: Any) -> tuple[pd.ExcelFile, str]:
    if isinstance(source, str):
        return pd.ExcelFile(source), source
    if isinstance(source, (bytes, bytearray)):
        return pd.ExcelFile(io.BytesIO(source)), "uploaded_workbook.xlsx"
    if hasattr(source, "read"):
        content = source.read()
        return pd.ExcelFile(io.BytesIO(content)), getattr(source, "name", "uploaded_workbook.xlsx")
    raise ValueError("Unsupported Excel source type.")


def _read_all_sheets(excel_file: pd.ExcelFile) -> dict[str, pd.DataFrame]:
    available_sheet_names = {to_snake_case(name): name for name in excel_file.sheet_names}
    out: dict[str, pd.DataFrame] = {}
    for normalized_name, original_name in available_sheet_names.items():
        out[normalized_name] = normalize_columns(pd.read_excel(excel_file, sheet_name=original_name))
    return out


def _canonicalize_legacy(raw_sheets: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    dim_site = raw_sheets.get("dim_site", pd.DataFrame())
    dim_area = raw_sheets.get("dim_area", pd.DataFrame())
    dim_plant = raw_sheets.get("dim_plant", pd.DataFrame())
    dim_equipment = raw_sheets.get("dim_equipment", pd.DataFrame())
    mine = raw_sheets.get("fact_daily_mine", pd.DataFrame())
    fleet = raw_sheets.get("fact_daily_fleet_unit", pd.DataFrame())
    plant = raw_sheets.get("fact_daily_plant", pd.DataFrame())

    site_id = dim_site.get("site_id", pd.Series(dtype=str)).dropna().astype(str).iloc[0] if not dim_site.empty and "site_id" in dim_site.columns else SITE_ID
    site_name = dim_site.get("site_name", pd.Series(dtype=str)).dropna().astype(str).iloc[0] if not dim_site.empty and "site_name" in dim_site.columns else SITE_NAME
    plant_id = dim_plant.get("plant_id", pd.Series(dtype=str)).dropna().astype(str).iloc[0] if not dim_plant.empty and "plant_id" in dim_plant.columns else PLANT_ID
    plant_name = dim_plant.get("plant_name", pd.Series(dtype=str)).dropna().astype(str).iloc[0] if not dim_plant.empty and "plant_name" in dim_plant.columns else PLANT_NAME
    timezone = dim_site.get("timezone", pd.Series(dtype=str)).dropna().astype(str).iloc[0] if not dim_site.empty and "timezone" in dim_site.columns else TIMEZONE

    metadata = pd.DataFrame(
        [
            {
                "schema_version": SCHEMA_VERSION,
                "site_id": site_id,
                "site_name": site_name,
                "plant_id": plant_id,
                "plant_name": plant_name,
                "timezone": timezone,
                "last_refresh_ts": pd.NaT,
            }
        ]
    )

    area_lookup = dim_area[["area_id", "area_name"]].drop_duplicates() if {"area_id", "area_name"}.issubset(dim_area.columns) else pd.DataFrame(columns=["area_id", "area_name"])
    if not mine.empty and "area_id" in mine.columns:
        mine = mine.merge(area_lookup, on="area_id", how="left")
    daily_mine = pd.DataFrame(columns=template_columns("daily_mine"))
    if not mine.empty:
        daily_mine = pd.DataFrame(
            {
                "date": mine.get("date"),
                "area_name": mine.get("area_name").fillna(mine.get("area_id")) if "area_name" in mine.columns else mine.get("area_id"),
                "bcm_moved": mine.get("bcm_moved"),
                "waste_bcm": mine.get("waste_bcm"),
                "ore_bcm": mine.get("ore_bcm"),
                "ore_mined_t": mine.get("ore_mined_t"),
            }
        )

    equipment_lookup = dim_equipment[[col for col in ["equipment_id", "equipment_class", "equipment_subtype", "model"] if col in dim_equipment.columns]].drop_duplicates()
    if not fleet.empty and not equipment_lookup.empty:
        fleet = fleet.merge(equipment_lookup, on="equipment_id", how="left")
    if not fleet.empty and "area_id" in fleet.columns and not area_lookup.empty:
        fleet = fleet.merge(area_lookup, on="area_id", how="left")
    daily_fleet = pd.DataFrame(columns=template_columns("daily_fleet"))
    if not fleet.empty:
        daily_fleet = pd.DataFrame(
            {
                "date": fleet.get("date"),
                "equipment_id": fleet.get("equipment_id"),
                "equipment_class": fleet.get("equipment_class"),
                "equipment_subtype": fleet.get("equipment_subtype"),
                "model": fleet.get("model"),
                "area_name": fleet.get("area_name").fillna(fleet.get("area_id")) if "area_name" in fleet.columns else fleet.get("area_id"),
                "availability_pct": fleet.get("availability_pct"),
                "utilization_pct": fleet.get("utilization_pct"),
                "diesel_l": fleet.get("diesel_l"),
            }
        )

    daily_plant = pd.DataFrame(columns=template_columns("daily_plant"))
    if not plant.empty:
        daily_plant = pd.DataFrame(
            {
                "date": plant.get("date"),
                "feed_tonnes": plant.get("feed_tonnes"),
                "feed_grade_pct": plant.get("feed_grade_pct"),
                "throughput_tph": plant.get("throughput_tph"),
                "recovery_pct": plant.get("recovery_pct"),
                "metal_produced_t": plant.get("metal_produced_t"),
                "availability_pct": plant.get("availability_pct"),
                "unplanned_downtime_h": plant.get("unplanned_downtime_h"),
            }
        )

    return {
        "metadata": metadata,
        "daily_mine": daily_mine,
        "daily_plant": daily_plant,
        "daily_fleet": daily_fleet,
        "lookups": build_lookup_frame(),
    }


def _empty_canonical_workbook() -> dict[str, pd.DataFrame]:
    return {name: pd.DataFrame(columns=template_columns(name)) for name in CANONICAL_SHEETS + ["lookups"]}


def _last_available(sheets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for sheet_name in ["daily_mine", "daily_plant", "daily_fleet"]:
        df = sheets.get(sheet_name, pd.DataFrame())
        if df.empty or "date" not in df.columns:
            continue
        rows.append({"sheet": sheet_name, "last_available_date": pd.to_datetime(df["date"], errors="coerce").max()})
    return pd.DataFrame(rows)


def _duplicates(sheets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    checks = {
        "daily_mine": ["date", "area_name"],
        "daily_plant": ["date"],
        "daily_fleet": ["date", "equipment_id"],
    }
    rows: list[dict[str, Any]] = []
    for sheet_name, key_cols in checks.items():
        df = sheets.get(sheet_name, pd.DataFrame())
        if df.empty or not set(key_cols).issubset(df.columns):
            rows.append({"sheet": sheet_name, "duplicate_rows": pd.NA, "duplicate_keys": pd.NA, "status": "Check unavailable"})
            continue
        dup_mask = df.duplicated(subset=key_cols, keep=False)
        rows.append(
            {
                "sheet": sheet_name,
                "duplicate_rows": int(dup_mask.sum()),
                "duplicate_keys": int(df.loc[dup_mask, key_cols].drop_duplicates().shape[0]),
                "status": "OK" if int(dup_mask.sum()) == 0 else "Has duplicates",
            }
        )
    return pd.DataFrame(rows)


def _null_pct(sheets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for sheet_name in CANONICAL_SHEETS:
        df = sheets.get(sheet_name, pd.DataFrame())
        total = len(df)
        for field in WORKBOOK_SCHEMA[sheet_name].fields:
            if field.name not in df.columns:
                rows.append({"sheet": sheet_name, "column": field.name, "null_pct": 100.0, "column_missing": True, "rows": total})
                continue
            null_pct = 100.0 if total == 0 else float(df[field.name].isna().mean() * 100)
            rows.append({"sheet": sheet_name, "column": field.name, "null_pct": round(null_pct, 2), "column_missing": False, "rows": total})
    return pd.DataFrame(rows)


def _missing_dates(df: pd.DataFrame, label_col: str, label_name: str) -> pd.DataFrame:
    if df.empty or not {"date", label_col}.issubset(df.columns):
        return pd.DataFrame(columns=[label_name, "missing_dates"])
    scoped = df.dropna(subset=["date", label_col]).copy()
    if scoped.empty:
        return pd.DataFrame(columns=[label_name, "missing_dates"])
    rows: list[dict[str, Any]] = []
    expected_dates = set(pd.date_range(scoped["date"].min(), scoped["date"].max(), freq="D").date)
    for label_value, group in scoped.groupby(label_col):
        present_dates = set(pd.to_datetime(group["date"], errors="coerce").dt.date.dropna())
        rows.append({label_name: label_value, "missing_dates": int(len(expected_dates - present_dates))})
    return pd.DataFrame(rows).sort_values("missing_dates", ascending=False)


def _coverage(df: pd.DataFrame, label_col: str, label_name: str) -> pd.DataFrame:
    if df.empty or not {"date", label_col}.issubset(df.columns):
        return pd.DataFrame(columns=[label_name, "date", "records"])
    if label_col == "date":
        covered = df.dropna(subset=["date"]).groupby("date", as_index=False).size()
        covered = covered.rename(columns={"size": "records"})
        covered[label_name] = "Plant"
        return covered[[label_name, "date", "records"]]
    covered = df.dropna(subset=["date", label_col]).groupby([label_col, "date"], as_index=False).size()
    return covered.rename(columns={label_col: label_name, "size": "records"})


def _health_summary(errors: List[str], warnings: List[str], issues: List[Dict[str, Any]]) -> pd.DataFrame:
    issue_df = pd.DataFrame(issues)
    error_count = len(errors) + int((issue_df.get("severity", pd.Series(dtype=str)) == "error").sum())
    warning_count = len(warnings) + int((issue_df.get("severity", pd.Series(dtype=str)) == "warning").sum())
    if error_count > 0:
        status = "Needs attention"
    elif warning_count > 0:
        status = "Usable with warnings"
    else:
        status = "Healthy"
    return pd.DataFrame([{"status": status, "error_count": error_count, "warning_count": warning_count, "issue_count": len(issue_df)}])


def _build_quality_artifacts(sheets: dict[str, pd.DataFrame], errors: List[str], warnings: List[str], issues: List[Dict[str, Any]]) -> dict[str, pd.DataFrame]:
    fleet = sheets.get("daily_fleet", pd.DataFrame())
    return {
        "health_summary": _health_summary(errors, warnings, issues),
        "issues": pd.DataFrame(issues),
        "schema_overview": build_schema_overview(),
        "field_guide": build_field_guide(),
        "last_available": _last_available(sheets),
        "duplicates": _duplicates(sheets),
        "null_pct": _null_pct(sheets),
        "missing_dates_area": _missing_dates(sheets.get("daily_mine", pd.DataFrame()), "area_name", "area_name"),
        "missing_dates_fleet": _missing_dates(fleet, "equipment_id", "equipment_id"),
        "coverage_area": _coverage(sheets.get("daily_mine", pd.DataFrame()), "area_name", "area_name"),
        "coverage_fleet": _coverage(fleet, "equipment_id", "equipment_id"),
        "coverage_plant": _coverage(sheets.get("daily_plant", pd.DataFrame()), "date", "date"),
    }


def _validate_metadata(metadata_df: pd.DataFrame, errors: List[str], issues: List[Dict[str, Any]]) -> str:
    schema_version = ""
    if metadata_df.empty:
        errors.append("Sheet 'metadata' must contain exactly one row.")
        issues.append(_issue("error", "metadata", "", pd.NA, "missing_metadata", "Metadata sheet is empty.", "Populate the metadata sheet using the official template."))
        return schema_version
    if len(metadata_df) > 1:
        issues.append(_issue("warning", "metadata", "", len(metadata_df), "multiple_metadata_rows", "Metadata has more than one row; only the first row will be used.", "Keep metadata to a single row."))
    row = metadata_df.iloc[0]
    schema_version = str(row.get("schema_version") or "")
    if schema_version != SCHEMA_VERSION:
        errors.append(f"Unsupported schema_version '{schema_version or 'blank'}'. Expected '{SCHEMA_VERSION}'.")
        issues.append(_issue("error", "metadata", "schema_version", 1, "schema_version_mismatch", f"Workbook schema version '{schema_version or 'blank'}' is not supported.", f"Download the current template version '{SCHEMA_VERSION}' and reload the workbook."))
    return schema_version


def load_excel_workbook(source: Any) -> WorkbookData:
    errors: List[str] = []
    warnings: List[str] = []
    issues: List[Dict[str, Any]] = []

    try:
        excel_file, source_name = _parse_excel_source(source)
    except Exception as exc:  # noqa: BLE001
        return WorkbookData(sheets=_empty_canonical_workbook(), errors=[f"Unable to read Excel workbook: {exc}"], warnings=[], quality={}, source_name=str(source), source_kind="unknown")

    raw_sheets = _read_all_sheets(excel_file)
    raw_sheet_names = set(raw_sheets)
    if set(CANONICAL_SHEETS).issubset(raw_sheet_names):
        sheets = {name: raw_sheets.get(name, pd.DataFrame(columns=template_columns(name))) for name in CANONICAL_SHEETS + ["lookups"]}
        source_kind = "canonical"
    elif LEGACY_SHEETS.issubset(raw_sheet_names):
        sheets = _canonicalize_legacy(raw_sheets)
        source_kind = "legacy"
        warnings.append("Legacy workbook structure detected. It was normalized into the canonical daily schema.")
        issues.append(_issue("info", "workbook", "", pd.NA, "legacy_normalization", "Legacy daily sheets were normalized into the canonical contract.", "Use the latest official template for future uploads."))
    else:
        missing = [sheet_name for sheet_name in CANONICAL_SHEETS if sheet_name not in raw_sheet_names]
        errors.append(f"Workbook is not compatible with the canonical schema. Missing required sheets: {', '.join(missing)}.")
        for sheet_name in missing:
            issues.append(_issue("error", sheet_name, "", pd.NA, "missing_sheet", f"Required sheet '{sheet_name}' is missing.", "Download the official template and repopulate the workbook."))
        sheets = _empty_canonical_workbook()
        return WorkbookData(sheets=sheets, errors=errors, warnings=warnings, quality=_build_quality_artifacts(sheets, errors, warnings, issues), source_name=source_name, schema_version="", source_kind="invalid")

    for sheet_name in CANONICAL_SHEETS:
        df = sheets.get(sheet_name, pd.DataFrame(columns=template_columns(sheet_name)))
        _validate_required_columns(sheet_name, df, errors, issues)
        df = _coerce_field_types(df, WORKBOOK_SCHEMA[sheet_name], warnings, issues)
        df = _normalize_percent_columns(df, WORKBOOK_SCHEMA[sheet_name], warnings, issues)
        _validate_required_values(sheet_name, df, warnings, issues)
        _validate_ranges(sheet_name, df, issues)
        sheets[sheet_name] = df

    lookups = sheets.get("lookups", pd.DataFrame())
    for sheet_name in ["daily_mine", "daily_fleet"]:
        _validate_allowed_values(sheet_name, sheets.get(sheet_name, pd.DataFrame()), issues, lookups if not lookups.empty else build_lookup_frame())

    schema_version = _validate_metadata(sheets.get("metadata", pd.DataFrame()), errors, issues)

    quality = _build_quality_artifacts(sheets, errors, warnings, issues)
    return WorkbookData(
        sheets=sheets,
        errors=errors,
        warnings=warnings,
        quality=quality,
        source_name=source_name,
        schema_version=schema_version,
        source_kind=source_kind,
    )
