# Mining Operations Executive Dashboard

Premium single-site executive dashboard for daily mine, plant, fleet, and data-quality review.

## Scope
- Single-site, single-plant operating model
- Daily executive snapshot with previous-period deltas
- Mine, Plant, Fleet, and Data Quality sections
- Canonical workbook contract with strict validation and schema versioning
- Fleet roster aligned to the current MVP:
  - `Ex 1` to `Ex 6`
  - `RDE 01` to `RDE 24`
  - `RD 01` to `RD 27`
  - `ADT 01` to `ADT 24`
  - `DR 01` to `DR 06`
  - `DZ 01` to `DZ 03`
  - `GR 01` to `GR 04`

Out of scope:
- Costs
- Budgets or targets as required inputs
- Shift-level dispatch analytics
- Multi-site UI workflows

## Product Structure
- `Overview`
- `Mine`
- `Plant`
- `Fleet`
- `Data Quality`

The hero area at the top of the app contains:
- fixed site and plant context
- data freshness and import status
- visible date-range selector
- data-quality health badge
- executive readout
- Mine Performance and Plant Performance KPI groups

## Repository Structure
- `/Users/kirill/Desktop/mining dashboard/app.py`
- `/Users/kirill/Desktop/mining dashboard/main.py`
- `/Users/kirill/Desktop/mining dashboard/pyproject.toml`
- `/Users/kirill/Desktop/mining dashboard/src/io_excel.py`
- `/Users/kirill/Desktop/mining dashboard/src/kpi.py`
- `/Users/kirill/Desktop/mining dashboard/src/charts.py`
- `/Users/kirill/Desktop/mining dashboard/src/theme.py`
- `/Users/kirill/Desktop/mining dashboard/scripts/make_template.py`
- `/Users/kirill/Desktop/mining dashboard/requirements.txt`
- `/Users/kirill/Desktop/mining dashboard/data/mine_productivity_input_template.xlsx`
- `/Users/kirill/Desktop/mining dashboard/data/sample_mine_productivity_input.xlsx`
- `/Users/kirill/Desktop/mining dashboard/data/mine_productivity_input.xlsx`

## Run Locally
```bash
cd "/Users/kirill/Desktop/mining dashboard"
/tmp/miniforge3/bin/pip install -r requirements.txt
/tmp/miniforge3/bin/python scripts/make_template.py
/tmp/miniforge3/bin/streamlit run app.py
```

Open [http://localhost:8501](http://localhost:8501).

## Deployment Entrypoints
Direct Python entrypoint:
```bash
cd "/Users/kirill/Desktop/mining dashboard"
/tmp/miniforge3/bin/python main.py
```

Console script after editable install:
```bash
cd "/Users/kirill/Desktop/mining dashboard"
/tmp/miniforge3/bin/pip install -e .
app
```

`main.py` honors:
- `PORT` default `8501`
- `HOST` default `0.0.0.0`

## Canonical Workbook Contract
This application now uses one canonical source of truth across:
- workbook template generation
- parser and import mapping
- validation rules
- metric transformations
- dashboard formatting and traceability

Schema version:
- `2026.03`

Required sheets:
- `README`
- `metadata`
- `daily_mine`
- `daily_plant`
- `daily_fleet`
- `lookups`

### `metadata`
Grain:
- one row per workbook

Columns:
- `schema_version` required
- `site_id` required
- `site_name` required
- `plant_id` required
- `plant_name` required
- `timezone` optional
- `last_refresh_ts` optional

### `daily_mine`
Grain:
- `date + area_name`

Columns:
- `date` required
- `area_name` required
- `bcm_moved` required
- `waste_bcm` required
- `ore_bcm` required
- `ore_mined_t` required

Allowed `area_name` values:
- `Cut 4 - 1`
- `Cut 4 - 2`
- `Cut 4 - 3`

### `daily_plant`
Grain:
- `date`

Columns:
- `date` required
- `feed_tonnes` required
- `feed_grade_pct` required
- `throughput_tph` required
- `recovery_pct` required
- `metal_produced_t` required
- `availability_pct` required
- `unplanned_downtime_h` required

### `daily_fleet`
Grain:
- `date + equipment_id`

Columns:
- `date` required
- `equipment_id` required
- `equipment_class` required
- `equipment_subtype` required
- `model` optional
- `area_name` required
- `availability_pct` required
- `utilization_pct` required
- `diesel_l` required

Allowed `equipment_class` values:
- `excavator`
- `truck`
- `ancillary`

Allowed `equipment_subtype` values:
- `excavator`
- `truck_220t`
- `truck_100t`
- `truck_60t`
- `drill`
- `dozer`
- `grader`

### `lookups`
Reference values used by the template and validation.

Columns:
- `lookup_type`
- `code`
- `value`
- `label`
- `notes`

## Compatibility Guarantees
Every UI metric is traceable to workbook fields or transform rules.

### Mine metrics
- `BCM moved = sum(daily_mine.bcm_moved)`
- `Ore mined (t) = sum(daily_mine.ore_mined_t)`
- `Stripping ratio = sum(daily_mine.waste_bcm) / sum(daily_mine.ore_bcm)`
- `Diesel consumption = sum(daily_fleet.diesel_l)`

### Plant metrics
- `Feed tonnes = sum(daily_plant.feed_tonnes)`
- `Throughput = average(daily_plant.throughput_tph)`
- `Recovery = feed-tonnage-weighted average of daily_plant.recovery_pct`
- `Metal produced = sum(daily_plant.metal_produced_t)`
- `Unplanned downtime = sum(daily_plant.unplanned_downtime_h)`
- `Plant availability = average(daily_plant.availability_pct)`

### Fleet groupings
- `Excavators = equipment_subtype == excavator`
- `Trucks = truck_220t + truck_100t + truck_60t`
- `Drills = equipment_subtype == drill`
- `Ancillary = drill + dozer + grader`

### Delta behavior
Snapshot cards compare the selected date range against the immediately preceding period with the same number of days.

## Percentage Handling
All `_pct` fields accept either:
- fraction form, for example `0.91`
- whole-percent form, for example `91`

Normalization rules:
- `0` to `1` stays unchanged
- `1` to `100` is divided by `100`
- values below `0` or above `100` are invalid and converted to null with a validation issue

## Validation and Data Quality
Uploads fail fast on:
- missing required sheets
- missing required columns
- schema version mismatch
- invalid dates
- duplicate keys
- invalid category values
- out-of-range percentages

The `Data Quality` page shows:
- health summary
- issue severity counts
- issue table with recommendations
- duplicate checks
- missing dates by cut and equipment
- null percentages
- KPI traceability
- schema overview and field guide

## Template and Sample Workbook
Generate both with:
```bash
cd "/Users/kirill/Desktop/mining dashboard"
/tmp/miniforge3/bin/python scripts/make_template.py
```

Outputs:
- `/Users/kirill/Desktop/mining dashboard/data/mine_productivity_input_template.xlsx`
- `/Users/kirill/Desktop/mining dashboard/data/sample_mine_productivity_input.xlsx`
- `/Users/kirill/Desktop/mining dashboard/data/mine_productivity_input.xlsx`

The template is blank and validation-ready.
The sample workbook shows the correct format and realistic example values.

## Notes for Extension
If you add new metrics later, extend the system in this order:
1. update the canonical schema in `/Users/kirill/Desktop/mining dashboard/src/io_excel.py`
2. update workbook generation in `/Users/kirill/Desktop/mining dashboard/scripts/make_template.py`
3. update metric transformations in `/Users/kirill/Desktop/mining dashboard/src/kpi.py`
4. update formatting and visualization in `/Users/kirill/Desktop/mining dashboard/app.py` and `/Users/kirill/Desktop/mining dashboard/src/charts.py`
5. verify the Data Quality page still traces every UI metric back to source fields
