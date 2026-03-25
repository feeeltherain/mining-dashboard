from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
TEMPLATE_PATH = DATA_DIR / "mine_productivity_input_template.xlsx"
SAMPLE_PATH = DATA_DIR / "sample_mine_productivity_input.xlsx"
DEFAULT_PATH = DATA_DIR / "mine_productivity_input.xlsx"


def _build_dimensions() -> dict[str, pd.DataFrame]:
    dim_site = pd.DataFrame(
        [
            {"site_id": "S1", "site_name": "North Ridge", "timezone": "America/Denver"},
            {"site_id": "S2", "site_name": "South Basin", "timezone": "America/Denver"},
        ]
    )

    area_rows = [
        {"area_id": "A11", "site_id": "S1", "area_name": "North Cut", "area_type": "Pit"},
        {"area_id": "A12", "site_id": "S1", "area_name": "East Ramp", "area_type": "Ramp"},
        {"area_id": "A13", "site_id": "S1", "area_name": "Crusher Feed", "area_type": "Dump"},
        {"area_id": "A21", "site_id": "S2", "area_name": "Main Pit", "area_type": "Pit"},
        {"area_id": "A22", "site_id": "S2", "area_name": "South Ramp", "area_type": "Ramp"},
        {"area_id": "A23", "site_id": "S2", "area_name": "Stockpile", "area_type": "Dump"},
    ]
    dim_area = pd.DataFrame(area_rows)

    equipment_rows = []
    for site in ["S1", "S2"]:
        for idx in range(1, 5):
            equipment_rows.append(
                {
                    "equipment_id": f"EX-{site}-{idx:02d}",
                    "site_id": site,
                    "equipment_class": "excavator",
                    "model": "CAT 6060",
                    "capacity_t": 42.0,
                    "active_flag": 1,
                }
            )
        for idx in range(1, 11):
            equipment_rows.append(
                {
                    "equipment_id": f"TR-{site}-{idx:02d}",
                    "site_id": site,
                    "equipment_class": "truck",
                    "model": "Komatsu 930E",
                    "capacity_t": 220.0,
                    "active_flag": 1,
                }
            )

    dim_equipment = pd.DataFrame(equipment_rows)
    return {
        "dim_site": dim_site,
        "dim_area": dim_area,
        "dim_equipment": dim_equipment,
    }


def _build_targets(dim_area: pd.DataFrame) -> pd.DataFrame:
    rows = []
    effective_from = date.today() - timedelta(days=90)

    for site in ["S1", "S2"]:
        rows.extend(
            [
                {
                    "site_id": site,
                    "equipment_class": "excavator",
                    "metric_name": "tonnes_per_operating_hour",
                    "unit": "t/op_h",
                    "target": 380.0 if site == "S1" else 365.0,
                    "area_id": pd.NA,
                    "min_threshold": 320.0,
                    "effective_from": effective_from,
                    "effective_to": pd.NA,
                },
                {
                    "site_id": site,
                    "equipment_class": "excavator",
                    "metric_name": "availability_pct",
                    "unit": "ratio",
                    "target": 0.86,
                    "area_id": pd.NA,
                    "min_threshold": 0.80,
                    "effective_from": effective_from,
                    "effective_to": pd.NA,
                },
                {
                    "site_id": site,
                    "equipment_class": "truck",
                    "metric_name": "tonnes_per_operating_hour",
                    "unit": "t/op_h",
                    "target": 155.0 if site == "S1" else 148.0,
                    "area_id": pd.NA,
                    "min_threshold": 130.0,
                    "effective_from": effective_from,
                    "effective_to": pd.NA,
                },
                {
                    "site_id": site,
                    "equipment_class": "truck",
                    "metric_name": "availability_pct",
                    "unit": "ratio",
                    "target": 0.88,
                    "area_id": pd.NA,
                    "min_threshold": 0.82,
                    "effective_from": effective_from,
                    "effective_to": pd.NA,
                },
                {
                    "site_id": site,
                    "equipment_class": "truck",
                    "metric_name": "avg_payload_t",
                    "unit": "t",
                    "target": 195.0,
                    "area_id": pd.NA,
                    "min_threshold": 175.0,
                    "effective_from": effective_from,
                    "effective_to": pd.NA,
                },
                {
                    "site_id": site,
                    "equipment_class": "truck",
                    "metric_name": "cycle_time_min",
                    "unit": "min",
                    "target": 24.0,
                    "area_id": pd.NA,
                    "min_threshold": 30.0,
                    "effective_from": effective_from,
                    "effective_to": pd.NA,
                },
                {
                    "site_id": site,
                    "equipment_class": "truck",
                    "metric_name": "queue_time_min",
                    "unit": "min",
                    "target": 4.8,
                    "area_id": pd.NA,
                    "min_threshold": 8.0,
                    "effective_from": effective_from,
                    "effective_to": pd.NA,
                },
            ]
        )

    # Area-level target override for queue_time_min.
    for row in dim_area.itertuples(index=False):
        rows.append(
            {
                "site_id": row.site_id,
                "equipment_class": "truck",
                "metric_name": "queue_time_min",
                "unit": "min",
                "target": 4.5 if row.area_type == "Ramp" else 5.0,
                "area_id": row.area_id,
                "min_threshold": 8.0,
                "effective_from": effective_from,
                "effective_to": pd.NA,
            }
        )

    return pd.DataFrame(rows)


def _build_fact_excavator(dim_area: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    records = []
    shifts = ["Day", "Night"]
    start_date = date.today() - timedelta(days=34)
    dates = [start_date + timedelta(days=i) for i in range(35)]

    excavators = {
        "S1": [f"EX-S1-{i:02d}" for i in range(1, 5)],
        "S2": [f"EX-S2-{i:02d}" for i in range(1, 5)],
    }
    site_areas = {
        "S1": ["A11", "A12", "A13"],
        "S2": ["A21", "A22", "A23"],
    }

    for site, units in excavators.items():
        for equipment_id in units:
            for d in dates:
                for shift in shifts:
                    area_id = rng.choice(site_areas[site])
                    operating_h = max(0.5, rng.normal(9.2 if shift == "Day" else 8.6, 0.9))
                    down_h = max(0.0, rng.normal(1.2, 0.6))
                    idle_h = max(0.2, rng.normal(1.1, 0.5))
                    standby_h = max(0.0, rng.normal(0.5, 0.3))

                    tph = rng.normal(385 if site == "S1" else 365, 22)
                    tonnes_loaded = max(0.0, operating_h * tph)
                    cycles_count = max(0.0, operating_h * rng.normal(44, 4))
                    avg_cycle_time_s = max(18.0, rng.normal(39.0, 4.5))
                    bucket_fill_factor = float(np.clip(rng.normal(0.9, 0.05), 0.72, 1.0))

                    if rng.random() < 0.012:
                        operating_h = 0.0
                    if rng.random() < 0.01:
                        tonnes_loaded *= 0.2

                    records.append(
                        {
                            "date": d,
                            "shift": shift,
                            "site_id": site,
                            "area_id": area_id,
                            "equipment_id": equipment_id,
                            "tonnes_loaded": round(tonnes_loaded, 2),
                            "operating_h": round(operating_h, 2),
                            "down_h": round(down_h, 2),
                            "idle_h": round(idle_h, 2),
                            "standby_h": round(standby_h, 2),
                            "cycles_count": round(cycles_count, 1),
                            "avg_cycle_time_s": round(avg_cycle_time_s, 1),
                            "bucket_fill_factor": round(bucket_fill_factor, 3),
                        }
                    )

    return pd.DataFrame(records)


def _build_fact_truck() -> pd.DataFrame:
    rng = np.random.default_rng(84)
    records = []
    shifts = ["Day", "Night"]
    start_date = date.today() - timedelta(days=34)
    dates = [start_date + timedelta(days=i) for i in range(35)]

    trucks = {
        "S1": [f"TR-S1-{i:02d}" for i in range(1, 11)],
        "S2": [f"TR-S2-{i:02d}" for i in range(1, 11)],
    }
    site_areas = {
        "S1": ["A11", "A12", "A13"],
        "S2": ["A21", "A22", "A23"],
    }

    for site, units in trucks.items():
        for equipment_id in units:
            for d in dates:
                for shift in shifts:
                    area_id = rng.choice(site_areas[site])
                    operating_h = max(0.3, rng.normal(9.0 if shift == "Day" else 8.4, 1.0))
                    down_h = max(0.0, rng.normal(1.0, 0.6))
                    idle_h = max(0.2, rng.normal(1.2, 0.5))
                    standby_h = max(0.0, rng.normal(0.4, 0.35))

                    trips = max(0, int(rng.normal(30 if shift == "Day" else 27, 4)))
                    avg_payload = rng.normal(194, 12)
                    tonnes_hauled = max(0.0, trips * avg_payload)
                    payload_target_t = 195.0

                    cycle_time_min = max(10.0, rng.normal(24.5, 3.3))
                    queue_time_min = max(0.5, rng.normal(4.9, 1.7))
                    speed_kmph_avg = max(8.0, rng.normal(27, 3.5))
                    distance_km_avg = max(0.6, rng.normal(3.2, 0.5))
                    fuel_l = max(50.0, tonnes_hauled * distance_km_avg * rng.normal(0.018, 0.0025))

                    if rng.random() < 0.015:
                        trips = 0
                    if rng.random() < 0.02:
                        queue_time_min *= 2.0

                    records.append(
                        {
                            "date": d,
                            "shift": shift,
                            "site_id": site,
                            "area_id": area_id,
                            "equipment_id": equipment_id,
                            "tonnes_hauled": round(tonnes_hauled, 2),
                            "trips": int(trips),
                            "operating_h": round(operating_h, 2),
                            "down_h": round(down_h, 2),
                            "idle_h": round(idle_h, 2),
                            "standby_h": round(standby_h, 2),
                            "payload_target_t": payload_target_t,
                            "cycle_time_min": round(cycle_time_min, 2),
                            "queue_time_min": round(queue_time_min, 2),
                            "speed_kmph_avg": round(speed_kmph_avg, 2),
                            "distance_km_avg": round(distance_km_avg, 2),
                            "fuel_l": round(fuel_l, 2),
                        }
                    )

    return pd.DataFrame(records)


def _build_route_fact() -> pd.DataFrame:
    rng = np.random.default_rng(123)
    records = []
    shifts = ["Day", "Night"]
    start_date = date.today() - timedelta(days=34)
    dates = [start_date + timedelta(days=i) for i in range(35)]

    routes = {
        "S1": [("A11", "A13"), ("A12", "A13"), ("A11", "A12")],
        "S2": [("A21", "A23"), ("A22", "A23"), ("A21", "A22")],
    }

    for site, route_pairs in routes.items():
        for d in dates:
            for shift in shifts:
                for from_area, to_area in route_pairs:
                    trips = max(0, int(rng.normal(95, 15)))
                    tonnes = max(0.0, trips * rng.normal(192, 9))
                    distance_km = max(0.5, rng.normal(3.1, 0.4))
                    cycle_time_min = max(10.0, rng.normal(24.2, 2.6))
                    queue_time_min = max(0.3, rng.normal(4.7, 1.5))
                    records.append(
                        {
                            "date": d,
                            "shift": shift,
                            "site_id": site,
                            "from_area_id": from_area,
                            "to_area_id": to_area,
                            "tonnes": round(tonnes, 2),
                            "trips": int(trips),
                            "distance_km": round(distance_km, 2),
                            "cycle_time_min": round(cycle_time_min, 2),
                            "queue_time_min": round(queue_time_min, 2),
                        }
                    )

    return pd.DataFrame(records)


def _build_template_frames() -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {
        "dim_site": pd.DataFrame(columns=["site_id", "site_name", "timezone"]),
        "dim_area": pd.DataFrame(columns=["area_id", "site_id", "area_name", "area_type"]),
        "dim_equipment": pd.DataFrame(
            columns=[
                "equipment_id",
                "site_id",
                "equipment_class",
                "model",
                "capacity_t",
                "active_flag",
            ]
        ),
        "targets": pd.DataFrame(
            columns=[
                "site_id",
                "equipment_class",
                "metric_name",
                "unit",
                "target",
                "area_id",
                "min_threshold",
                "effective_from",
                "effective_to",
            ]
        ),
        "fact_shift_excavator": pd.DataFrame(
            columns=[
                "date",
                "shift",
                "site_id",
                "area_id",
                "equipment_id",
                "tonnes_loaded",
                "operating_h",
                "down_h",
                "idle_h",
                "standby_h",
                "cycles_count",
                "avg_cycle_time_s",
                "bucket_fill_factor",
            ]
        ),
        "fact_shift_truck": pd.DataFrame(
            columns=[
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
                "standby_h",
                "payload_target_t",
                "cycle_time_min",
                "queue_time_min",
                "speed_kmph_avg",
                "distance_km_avg",
                "fuel_l",
            ]
        ),
        "fact_shift_truck_route": pd.DataFrame(
            columns=[
                "date",
                "shift",
                "site_id",
                "from_area_id",
                "to_area_id",
                "tonnes",
                "trips",
                "distance_km",
                "cycle_time_min",
                "queue_time_min",
            ]
        ),
    }
    return frames


def _write_workbook(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=sheet_name, index=False)


def build_sample_workbook() -> dict[str, pd.DataFrame]:
    dims = _build_dimensions()
    targets = _build_targets(dims["dim_area"])
    fact_exc = _build_fact_excavator(dims["dim_area"])
    fact_trk = _build_fact_truck()
    fact_route = _build_route_fact()

    return {
        "dim_site": dims["dim_site"],
        "dim_area": dims["dim_area"],
        "dim_equipment": dims["dim_equipment"],
        "targets": targets,
        "fact_shift_excavator": fact_exc,
        "fact_shift_truck": fact_trk,
        "fact_shift_truck_route": fact_route,
    }


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    template = _build_template_frames()
    sample = build_sample_workbook()

    _write_workbook(TEMPLATE_PATH, template)
    _write_workbook(SAMPLE_PATH, sample)
    _write_workbook(DEFAULT_PATH, sample)

    print(f"Template workbook written: {TEMPLATE_PATH}")
    print(f"Sample workbook written:   {SAMPLE_PATH}")
    print(f"Default workbook written:  {DEFAULT_PATH}")


if __name__ == "__main__":
    main()
