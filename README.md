# Mining Productivity Dashboard (Excavators + Trucks)

Executive-focused Streamlit dashboard for open-pit productivity analysis using a single Excel workbook.

## Scope
- In scope: excavator/shovel and truck productivity KPIs, trend vs target, drilldowns, and data quality checks.
- In scope: unit/shift-level KPIs with global filters (site, date, shift, area, equipment).
- In scope: graceful degradation (`N/A`) when data needed for a KPI is missing.

Out of scope:
- Costs, safety, drilling/blasting, plant/inventory, auth, databases, microservices, and any equipment class outside `excavator` and `truck`.

## Tech Stack
- Python 3.11+
- Streamlit
- pandas
- plotly

## Repository Structure
- `app.py`
- `src/io_excel.py`
- `src/kpi.py`
- `src/charts.py`
- `scripts/make_template.py`
- `requirements.txt`
- `data/mine_productivity_input_template.xlsx` (generated)
- `data/sample_mine_productivity_input.xlsx` (generated)

## Quick Start
1. Create and activate a Python 3.11+ environment.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Generate template + synthetic sample workbook:
   ```bash
   python scripts/make_template.py
   ```
4. Run dashboard:
   ```bash
   streamlit run app.py
   ```

Default load path is `./data/mine_productivity_input.xlsx`.
If it is missing, app falls back to `./data/sample_mine_productivity_input.xlsx`.

## Excel Contract
Workbook name: `mine_productivity_input.xlsx`

### Required sheets
1. `dim_site`
- Required: `site_id`, `site_name`
- Optional: `timezone`

2. `dim_area`
- Required: `area_id`, `site_id`, `area_name`
- Optional: `area_type`

3. `dim_equipment`
- Required: `equipment_id`, `site_id`, `equipment_class` (`excavator` or `truck`)
- Optional: `model`, `capacity_t`, `active_flag`

4. `targets`
- Required: `site_id`, `equipment_class`, `metric_name`, `unit`, `target`
- `metric_name` supported:
  - `tonnes_per_operating_hour`
  - `availability_pct`
  - `avg_payload_t`
  - `cycle_time_min`
  - `queue_time_min`
- Optional: `area_id`, `min_threshold`, `effective_from`, `effective_to`

5. `fact_shift_excavator`
- Required: `date`, `shift`, `site_id`, `area_id`, `equipment_id`, `tonnes_loaded`, `operating_h`, `down_h`, `idle_h`
- Optional: `standby_h`, `cycles_count`, `avg_cycle_time_s`, `bucket_fill_factor`

6. `fact_shift_truck`
- Required: `date`, `shift`, `site_id`, `area_id`, `equipment_id`, `tonnes_hauled`, `trips`, `operating_h`, `down_h`, `idle_h`
- Optional: `standby_h`, `payload_target_t`, `cycle_time_min`, `queue_time_min`, `speed_kmph_avg`, `distance_km_avg`, `fuel_l`

### Optional sheet
7. `fact_shift_truck_route`
- Recommended: `date`, `shift`, `site_id`, `from_area_id`, `to_area_id`, `tonnes`, `trips`, `distance_km`, `cycle_time_min`, `queue_time_min`

## KPI Formulas

### Excavators
- `tonnes_loaded = sum(tonnes_loaded)`
- `availability_pct = operating_h / (operating_h + down_h)`
- `utilization_pct = operating_h / (operating_h + idle_h + standby_h + down_h)` if `standby_h` exists
- `utilization_pct = operating_h / (operating_h + idle_h + down_h)` otherwise
- `tonnes_per_operating_hour = tonnes_loaded / operating_h`
- Optional:
  - `cycles_per_hour = cycles_count / operating_h`
  - `avg_cycle_time_s` (as provided)
  - `bucket_fill_factor` (as provided)

### Trucks
- `tonnes_hauled = sum(tonnes_hauled)`
- `availability_pct = operating_h / (operating_h + down_h)`
- `utilization_pct` follows same logic as excavators
- `tonnes_per_operating_hour = tonnes_hauled / operating_h`
- `avg_payload_t = tonnes_hauled / trips`
- `payload_compliance_pct = avg_payload_t / payload_target_t`
- Optional:
  - `tkm = tonnes_hauled * distance_km_avg`
  - `l_per_tkm = fuel_l / tkm`

If required columns are missing or denominator is non-positive, KPI displays `N/A` and reason appears in **Data Quality** page.

## Target Matching Logic
Target lookup keys:
- `site_id + equipment_class + metric_name`

Rules:
- If area-level targets exist and selected area matches, area target is used.
- Otherwise site-level target is used.
- If `effective_from/effective_to` exist, target valid for selected period end date is preferred.

## Dashboard Pages
- **Overview**: KPI cards, excavator/truck trend vs target, top 5 unit and area exceptions.
- **Excavators**: TPH trend, ranking table, TPH vs availability scatter, per-unit timeline.
- **Trucks**: TPH trend, payload distribution, ranking table, scatter, per-unit timeline, optional route ranking.
- **Data Quality**: last date, missing shifts, null %, duplicates, anomalies, coverage heatmaps, KPI availability report.

## Data Quality Checks (Actionable)
- Missing required sheet/column
- Invalid date/numeric coercion warnings
- Missing shifts count (`Day/Night` expected per date)
- Duplicate key checks:
  - Excavator: `(date, shift, site_id, equipment_id)`
  - Truck: `(date, shift, site_id, equipment_id)`
- Critical null % by column
- Non-positive hours and trips anomalies

## Troubleshooting
- **No workbook found**: run `python scripts/make_template.py` or upload an `.xlsx` in the sidebar.
- **KPI is N/A**: open Data Quality tab and review “KPI Availability and N/A Reasons”.
- **Targets not visible**: verify `targets.metric_name`, `equipment_class`, `site_id`, and optional `area_id` values.
- **Missing trends**: ensure fact sheets contain valid `date` values and filtered records for selected site/date range.
