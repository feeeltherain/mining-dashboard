from __future__ import annotations

import io
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List

import pandas as pd


REQUIRED_SHEETS = [
    "dim_site",
    "dim_area",
    "dim_equipment",
    "targets",
    "fact_shift_excavator",
    "fact_shift_truck",
]

OPTIONAL_SHEETS = ["fact_shift_truck_route"]

REQUIRED_COLUMNS: Dict[str, List[str]] = {
    "dim_site": ["site_id", "site_name"],
    "dim_area": ["area_id", "site_id", "area_name"],
    "dim_equipment": ["equipment_id", "site_id", "equipment_class"],
    "targets": [
        "site_id",
        "equipment_class",
        "metric_name",
        "unit",
        "target",
    ],
    "fact_shift_excavator": [
        "date",
        "shift",
        "site_id",
        "area_id",
        "equipment_id",
        "tonnes_loaded",
        "operating_h",
        "down_h",
        "idle_h",
    ],
    "fact_shift_truck": [
        "date",
        "shift",
        "site_id",
        "area_id",
        "equipment_id",
        "tonnes_hauled",
        "trips",
        "operating_h",
        "down_h",
        "idle_h",
    ],
}

NUMERIC_COLUMNS: Dict[str, List[str]] = {
    "dim_equipment": ["capacity_t", "active_flag"],
    "targets": ["target", "min_threshold"],
    "fact_shift_excavator": [
        "tonnes_loaded",
        "operating_h",
        "down_h",
        "idle_h",
        "standby_h",
        "cycles_count",
        "avg_cycle_time_s",
        "bucket_fill_factor",
    ],
    "fact_shift_truck": [
        "tonnes_hauled",
        "trips",
        "operating_h",
        "down_h",
        "idle_h",
        "standby_h",
        "payload_target_t",
        "cycle_time_min",
        "queue_time_min",
        "speed_kmph_avg",
        "distance_km_avg",
        "fuel_l",
    ],
    "fact_shift_truck_route": [
        "tonnes",
        "trips",
        "distance_km",
        "cycle_time_min",
        "queue_time_min",
    ],
}

DATE_COLUMNS: Dict[str, List[str]] = {
    "targets": ["effective_from", "effective_to"],
    "fact_shift_excavator": ["date"],
    "fact_shift_truck": ["date"],
    "fact_shift_truck_route": ["date"],
}

CRITICAL_NULL_COLUMNS: Dict[str, List[str]] = {
    "dim_site": ["site_id", "site_name"],
    "dim_area": ["area_id", "site_id", "area_name"],
    "dim_equipment": ["equipment_id", "site_id", "equipment_class"],
    "targets": ["site_id", "equipment_class", "metric_name", "target"],
    "fact_shift_excavator": [
        "date",
        "shift",
        "site_id",
        "area_id",
        "equipment_id",
        "tonnes_loaded",
        "operating_h",
        "down_h",
        "idle_h",
    ],
    "fact_shift_truck": [
        "date",
        "shift",
        "site_id",
        "area_id",
        "equipment_id",
        "tonnes_hauled",
        "trips",
        "operating_h",
        "down_h",
        "idle_h",
    ],
}


@dataclass
class WorkbookData:
    sheets: Dict[str, pd.DataFrame] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    quality: Dict[str, pd.DataFrame] = field(default_factory=dict)


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


def _normalize_shift(series: pd.Series) -> pd.Series:
    mapping = {
        "D": "Day",
        "DAY": "Day",
        "N": "Night",
        "NIGHT": "Night",
        "SHIFT_D": "Day",
        "SHIFT_N": "Night",
    }
    normalized = series.astype(str).str.strip().str.upper().map(mapping)
    return normalized.fillna(series)


def _coerce_numeric(df: pd.DataFrame, columns: List[str], sheet_name: str, warnings: List[str]) -> pd.DataFrame:
    for col in columns:
        if col not in df.columns:
            continue
        before_non_null = int(df[col].notna().sum())
        df[col] = pd.to_numeric(df[col], errors="coerce")
        after_non_null = int(df[col].notna().sum())
        invalid_count = before_non_null - after_non_null
        if invalid_count > 0:
            warnings.append(
                f"Sheet '{sheet_name}' column '{col}' has {invalid_count} values that are not numeric; converted to null."
            )
    return df


def _coerce_dates(df: pd.DataFrame, columns: List[str], sheet_name: str, warnings: List[str]) -> pd.DataFrame:
    for col in columns:
        if col not in df.columns:
            continue
        before_non_null = int(df[col].notna().sum())
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        after_non_null = int(df[col].notna().sum())
        invalid_count = before_non_null - after_non_null
        if invalid_count > 0:
            warnings.append(
                f"Sheet '{sheet_name}' column '{col}' has {invalid_count} invalid date values; converted to null."
            )
    return df


def _ensure_sheet(df: pd.DataFrame | None, sheet_name: str) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame(columns=REQUIRED_COLUMNS.get(sheet_name, []))
    return df


def _validate_required_columns(sheet_name: str, df: pd.DataFrame, errors: List[str]) -> None:
    required = REQUIRED_COLUMNS.get(sheet_name, [])
    missing = [col for col in required if col not in df.columns]
    if missing:
        errors.append(
            f"Sheet '{sheet_name}' is missing required columns: {', '.join(missing)}. KPIs depending on these columns will show as N/A."
        )


def _validate_equipment_class(df: pd.DataFrame, sheet_name: str, errors: List[str], warnings: List[str]) -> None:
    if "equipment_class" not in df.columns:
        return
    valid = {"excavator", "truck"}
    observed = set(df["equipment_class"].dropna().astype(str).str.lower().unique())
    invalid = sorted(observed - valid)
    if invalid:
        errors.append(
            f"Sheet '{sheet_name}' has unsupported equipment_class values: {', '.join(invalid)}. Only 'excavator' and 'truck' are allowed."
        )
    df["equipment_class"] = df["equipment_class"].astype(str).str.lower().where(df["equipment_class"].notna(), pd.NA)
    blank_count = int(df["equipment_class"].isna().sum())
    if blank_count > 0:
        warnings.append(
            f"Sheet '{sheet_name}' has {blank_count} rows with missing equipment_class."
        )


def _build_last_available_dates(sheets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    for sheet_name, equipment_class in [
        ("fact_shift_excavator", "excavator"),
        ("fact_shift_truck", "truck"),
    ]:
        df = sheets.get(sheet_name, pd.DataFrame())
        if df.empty or "site_id" not in df.columns or "date" not in df.columns:
            continue
        part = df.dropna(subset=["site_id", "date"]).groupby("site_id", as_index=False)["date"].max()
        part["equipment_class"] = equipment_class
        part = part.rename(columns={"date": "last_available_date"})
        rows.append(part)
    if not rows:
        return pd.DataFrame(columns=["site_id", "equipment_class", "last_available_date"])
    return pd.concat(rows, ignore_index=True)


def _build_missing_shifts(sheets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    expected = {"Day", "Night"}
    rows: List[pd.DataFrame] = []
    for sheet_name, equipment_class in [
        ("fact_shift_excavator", "excavator"),
        ("fact_shift_truck", "truck"),
    ]:
        df = sheets.get(sheet_name, pd.DataFrame())
        required = {"site_id", "area_id", "date", "shift"}
        if df.empty or not required.issubset(df.columns):
            continue
        key_cols = ["site_id", "area_id", "date"]
        grp = (
            df.dropna(subset=key_cols)
            .groupby(key_cols)["shift"]
            .agg(lambda x: set(v for v in x if pd.notna(v)))
            .reset_index()
        )
        grp["missing_shift_count"] = grp["shift"].apply(lambda x: max(0, 2 - len(expected.intersection(x))))
        part = grp.groupby(["site_id", "area_id"], as_index=False)["missing_shift_count"].sum()
        part["equipment_class"] = equipment_class
        rows.append(part[["equipment_class", "site_id", "area_id", "missing_shift_count"]])

    if not rows:
        return pd.DataFrame(columns=["equipment_class", "site_id", "area_id", "missing_shift_count"])
    return pd.concat(rows, ignore_index=True)


def _build_null_pct(sheets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for sheet_name, columns in CRITICAL_NULL_COLUMNS.items():
        df = sheets.get(sheet_name, pd.DataFrame())
        total = len(df)
        for col in columns:
            if col not in df.columns:
                rows.append(
                    {
                        "sheet": sheet_name,
                        "column": col,
                        "null_pct": 100.0,
                        "column_missing": True,
                        "rows": total,
                    }
                )
                continue
            if total == 0:
                null_pct = 100.0
            else:
                null_pct = float(df[col].isna().mean() * 100)
            rows.append(
                {
                    "sheet": sheet_name,
                    "column": col,
                    "null_pct": round(null_pct, 2),
                    "column_missing": False,
                    "rows": total,
                }
            )
    return pd.DataFrame(rows)


def _build_duplicates(sheets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    checks = {
        "fact_shift_excavator": ["date", "shift", "site_id", "equipment_id"],
        "fact_shift_truck": ["date", "shift", "site_id", "equipment_id"],
    }
    rows: List[Dict[str, Any]] = []
    for sheet_name, key_cols in checks.items():
        df = sheets.get(sheet_name, pd.DataFrame())
        if df.empty or not set(key_cols).issubset(df.columns):
            rows.append(
                {
                    "sheet": sheet_name,
                    "duplicate_rows": pd.NA,
                    "duplicate_keys": pd.NA,
                    "status": "Check unavailable (missing required key columns)",
                }
            )
            continue
        dup_mask = df.duplicated(subset=key_cols, keep=False)
        duplicate_rows = int(dup_mask.sum())
        duplicate_keys = int(df.loc[dup_mask, key_cols].drop_duplicates().shape[0])
        rows.append(
            {
                "sheet": sheet_name,
                "duplicate_rows": duplicate_rows,
                "duplicate_keys": duplicate_keys,
                "status": "OK" if duplicate_rows == 0 else "Has duplicates",
            }
        )
    return pd.DataFrame(rows)


def _build_coverage(sheets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    for sheet_name, equipment_class in [
        ("fact_shift_excavator", "excavator"),
        ("fact_shift_truck", "truck"),
    ]:
        df = sheets.get(sheet_name, pd.DataFrame())
        if df.empty or "date" not in df.columns or "shift" not in df.columns:
            continue
        part = (
            df.dropna(subset=["date", "shift"])
            .groupby(["date", "shift"], as_index=False)
            .size()
            .rename(columns={"size": "records"})
        )
        part["equipment_class"] = equipment_class
        rows.append(part[["equipment_class", "date", "shift", "records"]])
    if not rows:
        return pd.DataFrame(columns=["equipment_class", "date", "shift", "records"])
    return pd.concat(rows, ignore_index=True)


def _build_anomalies(sheets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    anomalies: List[Dict[str, Any]] = []

    exc = sheets.get("fact_shift_excavator", pd.DataFrame())
    if not exc.empty:
        if {"operating_h", "down_h", "idle_h"}.issubset(exc.columns):
            mask = (exc["operating_h"] <= 0) | (exc["down_h"] < 0) | (exc["idle_h"] < 0)
            anomalies.append(
                {
                    "sheet": "fact_shift_excavator",
                    "check": "non_positive_or_negative_hours",
                    "rows": int(mask.fillna(False).sum()),
                }
            )

    trk = sheets.get("fact_shift_truck", pd.DataFrame())
    if not trk.empty:
        if {"operating_h", "down_h", "idle_h"}.issubset(trk.columns):
            mask = (trk["operating_h"] <= 0) | (trk["down_h"] < 0) | (trk["idle_h"] < 0)
            anomalies.append(
                {
                    "sheet": "fact_shift_truck",
                    "check": "non_positive_or_negative_hours",
                    "rows": int(mask.fillna(False).sum()),
                }
            )
        if {"trips", "tonnes_hauled"}.issubset(trk.columns):
            mask = (trk["trips"] <= 0) & (trk["tonnes_hauled"] > 0)
            anomalies.append(
                {
                    "sheet": "fact_shift_truck",
                    "check": "non_positive_trips_with_positive_tonnage",
                    "rows": int(mask.fillna(False).sum()),
                }
            )

    if not anomalies:
        return pd.DataFrame(columns=["sheet", "check", "rows"])
    return pd.DataFrame(anomalies)


def _build_quality_artifacts(sheets: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    return {
        "last_available": _build_last_available_dates(sheets),
        "missing_shifts": _build_missing_shifts(sheets),
        "null_pct": _build_null_pct(sheets),
        "duplicates": _build_duplicates(sheets),
        "coverage": _build_coverage(sheets),
        "anomalies": _build_anomalies(sheets),
    }


def _parse_excel_source(source: Any) -> pd.ExcelFile:
    if isinstance(source, (str, bytes, bytearray)):
        if isinstance(source, str):
            return pd.ExcelFile(source)
        return pd.ExcelFile(io.BytesIO(source))

    if hasattr(source, "read"):
        content = source.read()
        return pd.ExcelFile(io.BytesIO(content))

    raise ValueError("Unsupported Excel source type.")


def load_excel_workbook(source: Any) -> WorkbookData:
    errors: List[str] = []
    warnings: List[str] = []
    sheets: Dict[str, pd.DataFrame] = {}

    try:
        excel_file = _parse_excel_source(source)
    except Exception as exc:  # noqa: BLE001
        return WorkbookData(
            sheets={name: pd.DataFrame(columns=REQUIRED_COLUMNS.get(name, [])) for name in REQUIRED_SHEETS + OPTIONAL_SHEETS},
            errors=[f"Unable to read Excel workbook: {exc}"],
            warnings=[],
            quality={},
        )

    available_sheet_names = {to_snake_case(name): name for name in excel_file.sheet_names}

    for sheet_name in REQUIRED_SHEETS + OPTIONAL_SHEETS:
        original_name = available_sheet_names.get(sheet_name)
        if original_name is None:
            if sheet_name in REQUIRED_SHEETS:
                errors.append(f"Missing required sheet: '{sheet_name}'.")
            sheets[sheet_name] = pd.DataFrame(columns=REQUIRED_COLUMNS.get(sheet_name, []))
            continue

        df = pd.read_excel(excel_file, sheet_name=original_name)
        df = normalize_columns(df)

        if sheet_name in DATE_COLUMNS:
            df = _coerce_dates(df, DATE_COLUMNS[sheet_name], sheet_name, warnings)

        if sheet_name in NUMERIC_COLUMNS:
            df = _coerce_numeric(df, NUMERIC_COLUMNS[sheet_name], sheet_name, warnings)

        if "shift" in df.columns:
            df["shift"] = _normalize_shift(df["shift"])

        sheets[sheet_name] = df

    for sheet_name, df in sheets.items():
        _validate_required_columns(sheet_name, df, errors)

    if "dim_equipment" in sheets:
        _validate_equipment_class(sheets["dim_equipment"], "dim_equipment", errors, warnings)
    if "targets" in sheets:
        _validate_equipment_class(sheets["targets"], "targets", errors, warnings)

    # Ensure only excavator/truck classes are retained in equipment dimensions and targets.
    valid_classes = {"excavator", "truck"}
    for sheet_name in ["dim_equipment", "targets"]:
        df = sheets.get(sheet_name, pd.DataFrame())
        if "equipment_class" in df.columns and not df.empty:
            invalid_mask = ~df["equipment_class"].isin(valid_classes)
            if invalid_mask.any():
                warnings.append(
                    f"Sheet '{sheet_name}' contains invalid equipment_class rows; they will be ignored in KPI calculations."
                )

    for sheet_name in REQUIRED_SHEETS + OPTIONAL_SHEETS:
        sheets[sheet_name] = _ensure_sheet(sheets.get(sheet_name), sheet_name)

    quality = _build_quality_artifacts(sheets)

    return WorkbookData(sheets=sheets, errors=errors, warnings=warnings, quality=quality)
