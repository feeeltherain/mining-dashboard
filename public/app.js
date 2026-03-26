const TAB_DEFS = [
    { key: "overview", label: "Overview" },
    { key: "mine", label: "Mine" },
    { key: "plant", label: "Plant" },
    { key: "fleet", label: "Fleet" },
    { key: "data_quality", label: "Data Quality" },
];

const HEAT_METRIC_LABELS = {
    availability_pct: "Availability",
    utilization_pct: "Utilization",
};

const state = {
    activeTab: "overview",
    file: null,
    data: null,
    loading: false,
    requestId: 0,
    networkError: null,
    dateFrom: null,
    dateTo: null,
    mineAreas: [],
    mineGroups: [],
    mineEquipment: [],
    mineSelectedUnit: null,
    mineHeatMetric: "availability_pct",
    fleetSelectedUnit: null,
    fleetHeatMetric: "availability_pct",
};

document.addEventListener("DOMContentLoaded", () => {
    refreshDashboard();
});

async function refreshDashboard() {
    const currentRequest = ++state.requestId;
    state.loading = true;
    renderApp();

    const url = new URL("/api/dashboard", window.location.origin);
    appendIfPresent(url.searchParams, "date_from", state.dateFrom);
    appendIfPresent(url.searchParams, "date_to", state.dateTo);
    appendList(url.searchParams, "mine_areas", state.mineAreas);
    appendList(url.searchParams, "mine_groups", state.mineGroups);
    appendList(url.searchParams, "mine_equipment", state.mineEquipment);
    appendIfPresent(url.searchParams, "mine_selected_unit", state.mineSelectedUnit);
    appendIfPresent(url.searchParams, "mine_heat_metric", state.mineHeatMetric);
    appendIfPresent(url.searchParams, "fleet_selected_unit", state.fleetSelectedUnit);
    appendIfPresent(url.searchParams, "fleet_heat_metric", state.fleetHeatMetric);

    try {
        const response = await fetch(url.toString(), state.file ? {
            method: "POST",
            headers: {
                "Content-Type": "application/octet-stream",
                "X-Workbook-Filename": state.file.name,
            },
            body: state.file,
        } : undefined);

        if (!response.ok) {
            throw new Error(`Request failed with ${response.status}`);
        }

        const payload = await response.json();
        if (currentRequest !== state.requestId) {
            return;
        }

        state.data = payload;
        state.networkError = null;
        syncStateFromPayload(payload);
    } catch (error) {
        if (currentRequest !== state.requestId) {
            return;
        }
        state.networkError = error instanceof Error ? error.message : "Unexpected network error";
    } finally {
        if (currentRequest === state.requestId) {
            state.loading = false;
            renderApp();
        }
    }
}

function syncStateFromPayload(payload) {
    const selectedRange = payload?.meta?.selected_range;
    if (selectedRange) {
        state.dateFrom = selectedRange.from;
        state.dateTo = selectedRange.to;
    }

    const mineFilters = payload?.mine?.filters;
    if (mineFilters) {
        state.mineAreas = mineFilters.selected_areas || [];
        state.mineGroups = mineFilters.selected_groups || [];
        state.mineEquipment = mineFilters.selected_equipment || [];
        state.mineSelectedUnit = mineFilters.selected_unit || null;
        state.mineHeatMetric = mineFilters.selected_heat_metric || "availability_pct";
    }

    const fleetFilters = payload?.fleet?.filters;
    if (fleetFilters) {
        state.fleetSelectedUnit = fleetFilters.selected_unit || null;
        state.fleetHeatMetric = fleetFilters.selected_heat_metric || "availability_pct";
    }
}

function renderApp() {
    renderMessages();
    renderHero();
    renderTabs();
    renderContent();
}

function renderMessages() {
    const host = document.getElementById("messages");
    if (!host) {
        return;
    }

    const banners = [];
    if (state.networkError) {
        banners.push(renderBanner("error", state.networkError));
    }

    if (state.loading) {
        banners.push(renderBanner("info", "Refreshing dashboard..."));
    }

    if (state.data?.message) {
        banners.push(renderBanner(state.data.ok ? "info" : "error", state.data.message));
    }

    (state.data?.errors || []).forEach((message) => banners.push(renderBanner("error", message)));
    (state.data?.warnings || []).forEach((message) => banners.push(renderBanner("warning", message)));

    host.innerHTML = banners.join("");
}

function renderHero() {
    const host = document.getElementById("hero");
    if (!host) {
        return;
    }

    const payload = state.data;
    if (!payload) {
        host.innerHTML = `
            <div class="hero-shell hero-loading">
                <div class="hero-title">Mining Operations Executive Dashboard</div>
                <div class="hero-subtitle">Loading the latest workbook and assembling the operating picture.</div>
            </div>
        `;
        return;
    }

    const meta = payload.meta || {};
    const availableRange = meta.available_range || {};
    const selectedRange = meta.selected_range || {};
    const selectedFile = state.file ? state.file.name : meta.source_label || "Workbook";

    host.innerHTML = `
        <div class="hero-shell">
            <div class="hero-grid">
                <div>
                    <div class="hero-title">Mining Operations Executive Dashboard</div>
                    <div class="hero-subtitle">A calm, daily operating view across mine, plant, fleet, and data quality for executive review.</div>
                    <div class="chip-row">
                        ${renderChip(`Site: ${meta.site_name || "Mining Operations"}`)}
                        ${renderChip(`Plant: ${meta.plant_name || "Plant"}`)}
                        ${renderChip(`Latest available: ${formatDate(meta.latest_available_date) || "Unknown"}`)}
                        ${renderChip(`Data quality: ${meta.health_status || "Unknown"}`, meta.health_tone)}
                        ${renderChip(`Source: ${escapeHtml(selectedFile)}`)}
                    </div>
                </div>
                <div class="utility-stack">
                    <label class="button button-primary" for="workbook-upload">Replace workbook</label>
                    <input id="workbook-upload" type="file" accept=".xlsx" class="visually-hidden">
                    <div class="utility-actions">
                        <a class="button button-secondary" href="${meta.template_url || "/api/template"}">Download official template</a>
                        <a class="button button-secondary" href="${meta.sample_url || "/api/sample"}">Download sample workbook</a>
                    </div>
                    <div class="utility-note">Current workbook: ${escapeHtml(selectedFile)}</div>
                    <div class="utility-note">Last import refresh: ${formatDateTime(meta.last_refresh_ts) || "Unknown refresh"}</div>
                    <div class="utility-note">Use the official template to keep imports compatible and validation calm.</div>
                </div>
            </div>
            <div class="hero-controls">
                <div class="preset-row">
                    ${renderPresetButton("Latest")}
                    ${renderPresetButton("7D")}
                    ${renderPresetButton("30D")}
                    ${renderPresetButton("Full")}
                </div>
                <div class="range-controls">
                    <label class="field">
                        <span>From</span>
                        <input id="date-from" type="date" min="${availableRange.min || ""}" max="${availableRange.max || ""}" value="${selectedRange.from || ""}">
                    </label>
                    <label class="field">
                        <span>To</span>
                        <input id="date-to" type="date" min="${availableRange.min || ""}" max="${availableRange.max || ""}" value="${selectedRange.to || ""}">
                    </label>
                    <div class="range-banner">
                        <div>
                            <div class="range-label">Selected range</div>
                            <div class="range-value">${formatRange(selectedRange.from, selectedRange.to)}</div>
                        </div>
                        <div class="range-days">${meta.range_days || 0} days</div>
                    </div>
                </div>
            </div>
        </div>
    `;

    const uploadInput = document.getElementById("workbook-upload");
    if (uploadInput) {
        uploadInput.addEventListener("change", async (event) => {
            const [file] = event.target.files || [];
            state.file = file || null;
            await refreshDashboard();
        });
    }

    document.querySelectorAll("[data-preset]").forEach((button) => {
        button.addEventListener("click", async () => {
            applyPreset(button.dataset.preset, availableRange.min, availableRange.max);
            await refreshDashboard();
        });
    });

    const dateFromInput = document.getElementById("date-from");
    const dateToInput = document.getElementById("date-to");
    if (dateFromInput) {
        dateFromInput.addEventListener("change", async (event) => {
            state.dateFrom = event.target.value;
            await refreshDashboard();
        });
    }
    if (dateToInput) {
        dateToInput.addEventListener("change", async (event) => {
            state.dateTo = event.target.value;
            await refreshDashboard();
        });
    }
}

function renderTabs() {
    const host = document.getElementById("tabs");
    if (!host) {
        return;
    }

    host.innerHTML = TAB_DEFS.map((tab) => `
        <button class="tab-button ${state.activeTab === tab.key ? "is-active" : ""}" data-tab="${tab.key}">
            ${tab.label}
        </button>
    `).join("");

    host.querySelectorAll("[data-tab]").forEach((button) => {
        button.addEventListener("click", () => {
            state.activeTab = button.dataset.tab;
            renderTabs();
            renderContent();
        });
    });
}

function renderContent() {
    const host = document.getElementById("content");
    if (!host) {
        return;
    }

    if (!state.data) {
        host.innerHTML = `<div class="panel"><div class="section-lead">Loading dashboard contents.</div></div>`;
        return;
    }

    if (!state.data.ok) {
        host.innerHTML = `
            <section class="panel">
                <div class="section-lead">${escapeHtml(state.data.message || "The dashboard could not be rendered from the current workbook.")}</div>
            </section>
        `;
        return;
    }

    switch (state.activeTab) {
        case "mine":
            renderMine(host, state.data.mine);
            return;
        case "plant":
            renderPlant(host, state.data.plant);
            return;
        case "fleet":
            renderFleet(host, state.data.fleet);
            return;
        case "data_quality":
            renderDataQuality(host, state.data.data_quality);
            return;
        default:
            renderOverview(host, state.data.overview);
    }
}

function renderOverview(host, overview) {
    const cardMap = new Map((overview.cards || []).map((card) => [card.metric, card]));

    host.innerHTML = `
        <section class="panel">
            <div class="section-kicker">Executive readout</div>
            <div class="readout-text">${escapeHtml(overview.readout || "No readout available.")}</div>
        </section>
        <section class="panel">
            <div class="section-kicker">What changed</div>
            <div class="pill-row">
                ${(overview.change_strip?.rows || []).map((row) => renderChangePill(row)).join("") || `<div class="utility-note">No previous-period deltas are available for the current selection.</div>`}
            </div>
        </section>
        <section class="metric-group">
            <div class="group-heading">Mine performance</div>
            <div class="metric-grid">
                ${(overview.mine_card_order || []).map((metric) => renderMetricCard(cardMap.get(metric))).join("")}
            </div>
        </section>
        <section class="metric-group">
            <div class="group-heading">Plant performance</div>
            <div class="metric-grid">
                ${(overview.plant_card_order || []).map((metric) => renderMetricCard(cardMap.get(metric))).join("")}
            </div>
        </section>
        <section class="panel">
            <div class="section-lead">Start here for the operating picture, then move into Mine, Plant, or Fleet for diagnosis.</div>
            <div class="chart-grid two-up">
                <div class="chart-card"><div id="overview-mine-production" class="chart-frame"></div></div>
                <div class="chart-card"><div id="overview-plant-performance" class="chart-frame"></div></div>
            </div>
        </section>
        <section class="panel">
            <div class="group-heading">Fleet availability</div>
            <div class="chart-grid quad-up">
                ${["Excavators", "Trucks", "Drills", "Ancillary"].map((group) => `<div class="chart-card"><div id="overview-availability-${slugify(group)}" class="chart-frame"></div></div>`).join("")}
            </div>
        </section>
        <section class="panel">
            <div class="chart-card chart-card-wide"><div id="overview-area-contribution" class="chart-frame"></div></div>
        </section>
    `;

    (overview.cards || []).forEach((card) => {
        if (card.sparkline_chart) {
            renderFigure(`sparkline-${card.metric}`, card.sparkline_chart);
        }
    });
    renderFigure("overview-mine-production", overview.charts?.mine_production);
    renderFigure("overview-plant-performance", overview.charts?.plant_performance);
    ["Excavators", "Trucks", "Drills", "Ancillary"].forEach((group) => {
        renderFigure(`overview-availability-${slugify(group)}`, overview.charts?.availability_groups?.[group]);
    });
    renderFigure("overview-area-contribution", overview.charts?.area_contribution);
}

function renderMine(host, mine) {
    const filters = mine.filters || {};
    const charts = mine.charts || {};
    const tables = mine.tables || {};

    host.innerHTML = `
        <section class="panel">
            <div class="section-lead">Mine focuses on movement, stripping behaviour, diesel draw, and the unit-level story behind them.</div>
            <div class="filter-grid mine-filter-grid">
                ${renderMultiSelectField("mine-areas", "Cuts", filters.area_options || [], filters.selected_areas || [])}
                ${renderMultiSelectField("mine-groups", "Fleet groups", filters.group_options || [], filters.selected_groups || [])}
                ${renderMultiSelectField("mine-equipment", "Equipment", filters.equipment_options || [], filters.selected_equipment || [])}
            </div>
        </section>
        <section class="panel">
            <div class="chart-grid two-up">
                <div class="chart-card"><div id="mine-production" class="chart-frame"></div></div>
                <div class="chart-card"><div id="mine-volume" class="chart-frame"></div></div>
            </div>
            <div class="chart-card chart-card-wide"><div id="mine-stripping-ratio" class="chart-frame"></div></div>
        </section>
        <section class="panel">
            <div class="group-heading">Fleet utilization</div>
            <div class="chart-grid quad-up">
                ${["Excavators", "Trucks", "Drills", "Ancillary"].map((group) => `<div class="chart-card"><div id="mine-utilization-${slugify(group)}" class="chart-frame"></div></div>`).join("")}
            </div>
        </section>
        <section class="panel">
            <div class="chart-grid two-up">
                <div class="chart-card"><div id="mine-diesel-groups" class="chart-frame"></div></div>
                <div class="chart-card"><div id="mine-top-diesel" class="chart-frame"></div></div>
            </div>
            <div class="chart-card chart-card-wide"><div id="mine-bottom-diesel" class="chart-frame"></div></div>
        </section>
        <section class="panel">
            <div class="group-heading">Unit ranking</div>
            ${renderTable(tables.unit_ranking)}
        </section>
        <section class="panel">
            <div class="filter-grid detail-grid">
                ${renderSingleSelectField("mine-selected-unit", "Per-unit timeline", filters.unit_options || [], filters.selected_unit)}
                ${renderSingleSelectField("mine-heat-metric", "Heatmap metric", Object.keys(HEAT_METRIC_LABELS), filters.selected_heat_metric, HEAT_METRIC_LABELS)}
            </div>
            <div class="chart-grid two-up">
                <div class="chart-card"><div id="mine-unit-timeline" class="chart-frame"></div></div>
                <div class="chart-card"><div id="mine-unit-heatmap" class="chart-frame"></div></div>
            </div>
        </section>
    `;

    bindMultiSelect("mine-areas", "mineAreas");
    bindMultiSelect("mine-groups", "mineGroups");
    bindMultiSelect("mine-equipment", "mineEquipment");
    bindSingleSelect("mine-selected-unit", "mineSelectedUnit");
    bindSingleSelect("mine-heat-metric", "mineHeatMetric");

    renderFigure("mine-production", charts.mine_production);
    renderFigure("mine-volume", charts.mine_volume);
    renderFigure("mine-stripping-ratio", charts.stripping_ratio);
    ["Excavators", "Trucks", "Drills", "Ancillary"].forEach((group) => {
        renderFigure(`mine-utilization-${slugify(group)}`, charts.utilization_groups?.[group]);
    });
    renderFigure("mine-diesel-groups", charts.diesel_groups);
    renderFigure("mine-top-diesel", charts.top_diesel);
    renderFigure("mine-bottom-diesel", charts.bottom_diesel);
    renderFigure("mine-unit-timeline", charts.unit_timeline);
    renderFigure("mine-unit-heatmap", charts.unit_heatmap);
}

function renderPlant(host, plant) {
    const charts = plant.charts || {};
    const tables = plant.tables || {};

    host.innerHTML = `
        <section class="panel">
            <div class="section-lead">Plant keeps feed, recovery, metal output, and downtime together so the process story stays coherent.</div>
            <div class="chart-grid two-up">
                <div class="chart-card"><div id="plant-feed-throughput" class="chart-frame"></div></div>
                <div class="chart-card"><div id="plant-grade-recovery" class="chart-frame"></div></div>
                <div class="chart-card"><div id="plant-metal-production" class="chart-frame"></div></div>
                <div class="chart-card"><div id="plant-downtime-availability" class="chart-frame"></div></div>
            </div>
        </section>
        <section class="panel">
            <div class="group-heading">Daily operating table</div>
            ${renderTable(tables.daily_operating)}
        </section>
    `;

    renderFigure("plant-feed-throughput", charts.feed_throughput);
    renderFigure("plant-grade-recovery", charts.grade_recovery);
    renderFigure("plant-metal-production", charts.metal_production);
    renderFigure("plant-downtime-availability", charts.downtime_availability);
}

function renderFleet(host, fleet) {
    const filters = fleet.filters || {};
    const charts = fleet.charts || {};
    const tables = fleet.tables || {};

    host.innerHTML = `
        <section class="panel">
            <div class="section-lead">Fleet separates availability from diesel and unit ranking so shortfalls are easier to isolate.</div>
            <div class="group-heading">Fleet availability</div>
            <div class="chart-grid quad-up">
                ${["Excavators", "Trucks", "Drills", "Ancillary"].map((group) => `<div class="chart-card"><div id="fleet-availability-${slugify(group)}" class="chart-frame"></div></div>`).join("")}
            </div>
        </section>
        <section class="panel">
            <div class="chart-grid two-up">
                <div class="chart-card"><div id="fleet-diesel-groups" class="chart-frame"></div></div>
                <div class="chart-card"><div id="fleet-lowest-availability" class="chart-frame"></div></div>
            </div>
        </section>
        <section class="panel">
            <div class="group-heading">Fleet unit list</div>
            ${renderTable(tables.unit_ranking)}
        </section>
        <section class="panel">
            <div class="filter-grid detail-grid">
                ${renderSingleSelectField("fleet-selected-unit", "Fleet unit detail", filters.unit_options || [], filters.selected_unit)}
                ${renderSingleSelectField("fleet-heat-metric", "Fleet heatmap metric", Object.keys(HEAT_METRIC_LABELS), filters.selected_heat_metric, HEAT_METRIC_LABELS)}
            </div>
            <div class="chart-grid two-up">
                <div class="chart-card"><div id="fleet-unit-timeline" class="chart-frame"></div></div>
                <div class="chart-card"><div id="fleet-unit-heatmap" class="chart-frame"></div></div>
            </div>
        </section>
    `;

    bindSingleSelect("fleet-selected-unit", "fleetSelectedUnit");
    bindSingleSelect("fleet-heat-metric", "fleetHeatMetric");

    ["Excavators", "Trucks", "Drills", "Ancillary"].forEach((group) => {
        renderFigure(`fleet-availability-${slugify(group)}`, charts.availability_groups?.[group]);
    });
    renderFigure("fleet-diesel-groups", charts.diesel_groups);
    renderFigure("fleet-lowest-availability", charts.lowest_availability);
    renderFigure("fleet-unit-timeline", charts.unit_timeline);
    renderFigure("fleet-unit-heatmap", charts.unit_heatmap);
}

function renderDataQuality(host, quality) {
    const charts = quality.charts || {};
    const tables = quality.tables || {};

    host.innerHTML = `
        <section class="panel">
            <div class="section-lead">Data Quality shows whether the workbook is decision-grade, where it is thin, and what should be fixed next.</div>
            <div class="chart-grid two-up">
                <div class="table-card">
                    <div class="group-heading">Data quality health</div>
                    ${renderTable(tables.health_summary)}
                </div>
                <div class="chart-card"><div id="quality-issue-severity" class="chart-frame"></div></div>
            </div>
        </section>
        <section class="panel">
            <div class="group-heading">Issue log</div>
            ${renderTable(tables.issues)}
        </section>
        <section class="panel">
            <div class="chart-grid two-up">
                <div class="table-card">
                    <div class="table-stack">
                        <div>
                            <div class="group-heading">Last available dates</div>
                            ${renderTable(tables.last_available)}
                        </div>
                        <div>
                            <div class="group-heading">Duplicate checks</div>
                            ${renderTable(tables.duplicates)}
                        </div>
                    </div>
                </div>
                <div class="table-card">
                    <div class="table-stack">
                        <div>
                            <div class="group-heading">Missing dates by cut</div>
                            ${renderTable(tables.missing_dates_area)}
                        </div>
                        <div>
                            <div class="group-heading">Missing dates by equipment</div>
                            ${renderTable(tables.missing_dates_fleet)}
                        </div>
                    </div>
                </div>
            </div>
        </section>
        <section class="panel">
            <div class="chart-grid two-up">
                <div class="chart-card"><div id="quality-coverage-area" class="chart-frame"></div></div>
                <div class="chart-card"><div id="quality-coverage-fleet" class="chart-frame"></div></div>
            </div>
        </section>
        <section class="panel">
            <div class="group-heading">Critical null percentages</div>
            ${renderTable(tables.null_pct)}
        </section>
        <section class="panel">
            <div class="group-heading">KPI traceability</div>
            ${renderTable(tables.kpi_traceability)}
        </section>
        <details class="panel details-panel">
            <summary>Schema overview</summary>
            <div class="details-stack">
                <div>
                    <div class="group-heading">Schema overview</div>
                    ${renderTable(tables.schema_overview)}
                </div>
                <div>
                    <div class="group-heading">Field guide</div>
                    ${renderTable(tables.field_guide)}
                </div>
            </div>
        </details>
    `;

    renderFigure("quality-issue-severity", charts.issue_severity);
    renderFigure("quality-coverage-area", charts.coverage_area);
    renderFigure("quality-coverage-fleet", charts.coverage_fleet);
}

function renderMetricCard(card) {
    if (!card) {
        return "";
    }
    const tone = deltaTone(card.direction, card.delta);
    const trendText = card.status === "N/A" && card.reason ? card.reason : `Trend: ${card.trend_label || "N/A"}`;
    return `
        <div class="metric-shell">
            <div class="metric-title">${escapeHtml(card.label)}</div>
            <div class="metric-value">${formatMetricValue(card.metric, card.actual)}</div>
            <div class="metric-delta metric-delta-${tone}">
                ${formatMetricDelta(card.metric, card.delta)} vs previous period
            </div>
            <div class="metric-note">${escapeHtml(trendText)}</div>
            ${card.sparkline_chart ? `<div id="sparkline-${card.metric}" class="sparkline-frame"></div>` : ""}
        </div>
    `;
}

function renderChangePill(row) {
    const tone = deltaTone(row.direction, row.delta);
    return `<span class="change-pill change-pill-${tone}">${escapeHtml(row.label)}: ${formatMetricDelta(row.metric, row.delta)}</span>`;
}

function renderBanner(tone, message) {
    return `<div class="banner banner-${tone}">${escapeHtml(message)}</div>`;
}

function renderChip(label, tone) {
    return `<span class="chip ${tone ? `chip-${tone}` : ""}">${escapeHtml(label)}</span>`;
}

function renderPresetButton(label) {
    return `<button class="button button-secondary" type="button" data-preset="${label}">${label}</button>`;
}

function renderMultiSelectField(id, label, options, selectedValues) {
    const size = Math.min(8, Math.max(3, options.length || 3));
    return `
        <label class="field">
            <span>${label}</span>
            <select id="${id}" multiple size="${size}">
                ${options.map((option) => `<option value="${escapeHtml(option)}" ${selectedValues.includes(option) ? "selected" : ""}>${escapeHtml(option)}</option>`).join("")}
            </select>
        </label>
    `;
}

function renderSingleSelectField(id, label, options, selectedValue, labelMap) {
    return `
        <label class="field">
            <span>${label}</span>
            <select id="${id}">
                ${options.map((option) => `<option value="${escapeHtml(option)}" ${selectedValue === option ? "selected" : ""}>${escapeHtml(labelMap?.[option] || option)}</option>`).join("")}
            </select>
        </label>
    `;
}

function renderTable(table) {
    const columns = table?.columns || [];
    const rows = table?.rows || [];
    if (!columns.length) {
        return `<div class="empty-state">No data available for this view.</div>`;
    }
    if (!rows.length) {
        return `<div class="empty-state">No rows available for this table.</div>`;
    }
    return `
        <div class="table-wrap">
            <table class="data-table">
                <thead>
                    <tr>${columns.map((column) => `<th>${escapeHtml(formatHeader(column))}</th>`).join("")}</tr>
                </thead>
                <tbody>
                    ${rows.map((row) => `
                        <tr>
                            ${columns.map((column) => `<td>${formatCell(column, row[column])}</td>`).join("")}
                        </tr>
                    `).join("")}
                </tbody>
            </table>
        </div>
    `;
}

function renderFigure(id, figureJson) {
    if (!figureJson || !window.Plotly) {
        return;
    }
    const node = document.getElementById(id);
    if (!node) {
        return;
    }
    const figure = typeof figureJson === "string" ? JSON.parse(figureJson) : figureJson;
    Plotly.newPlot(node, figure.data || [], figure.layout || {}, {
        responsive: true,
        displayModeBar: false,
    });
}

function bindMultiSelect(id, stateKey) {
    const element = document.getElementById(id);
    if (!element) {
        return;
    }
    element.addEventListener("change", async () => {
        state[stateKey] = Array.from(element.selectedOptions).map((option) => option.value);
        await refreshDashboard();
    });
}

function bindSingleSelect(id, stateKey) {
    const element = document.getElementById(id);
    if (!element) {
        return;
    }
    element.addEventListener("change", async () => {
        state[stateKey] = element.value || null;
        await refreshDashboard();
    });
}

function applyPreset(preset, minDate, maxDate) {
    if (!minDate || !maxDate) {
        return;
    }
    if (preset === "Latest") {
        state.dateFrom = maxDate;
        state.dateTo = maxDate;
        return;
    }
    if (preset === "7D") {
        state.dateTo = maxDate;
        state.dateFrom = addDays(maxDate, -6, minDate);
        return;
    }
    if (preset === "30D") {
        state.dateTo = maxDate;
        state.dateFrom = addDays(maxDate, -29, minDate);
        return;
    }
    state.dateFrom = minDate;
    state.dateTo = maxDate;
}

function addDays(dateString, offset, minDate) {
    const date = new Date(`${dateString}T00:00:00`);
    date.setDate(date.getDate() + offset);
    const candidate = date.toISOString().slice(0, 10);
    return candidate < minDate ? minDate : candidate;
}

function appendIfPresent(params, key, value) {
    if (value) {
        params.append(key, value);
    }
}

function appendList(params, key, values) {
    (values || []).forEach((value) => {
        if (value) {
            params.append(key, value);
        }
    });
}

function deltaTone(direction, value) {
    if (value === null || value === undefined || Number.isNaN(Number(value)) || direction === "neutral") {
        return "neutral";
    }
    if (direction === "up") {
        return Number(value) >= 0 ? "good" : "bad";
    }
    return Number(value) <= 0 ? "good" : "bad";
}

function formatMetricValue(metric, value) {
    if (value === null || value === undefined) {
        return "N/A";
    }
    const numeric = Number(value);
    if (Number.isNaN(numeric)) {
        return escapeHtml(String(value));
    }
    if (metric.endsWith("_pct")) {
        return `${(numeric * 100).toFixed(1)}%`;
    }
    if (metric === "stripping_ratio") {
        return numeric.toFixed(2);
    }
    if (metric === "diesel_l") {
        return `${compactNumber(numeric)} L`;
    }
    if (metric === "throughput_tph") {
        return `${numeric.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 })} t/h`;
    }
    if (metric === "unplanned_downtime_h") {
        return `${numeric.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 })} h`;
    }
    return compactNumber(numeric);
}

function formatMetricDelta(metric, value) {
    if (value === null || value === undefined) {
        return "No previous-period comparison";
    }
    const numeric = Number(value);
    if (Number.isNaN(numeric)) {
        return "No previous-period comparison";
    }
    const sign = numeric >= 0 ? "+" : "";
    if (metric.endsWith("_pct")) {
        return `${sign}${(numeric * 100).toFixed(1)} pp`;
    }
    if (metric === "stripping_ratio") {
        return `${sign}${numeric.toFixed(2)}`;
    }
    if (metric === "diesel_l") {
        return `${sign}${compactNumber(numeric)} L`;
    }
    if (metric === "throughput_tph") {
        return `${sign}${numeric.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 })} t/h`;
    }
    if (metric === "unplanned_downtime_h") {
        return `${sign}${numeric.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 })} h`;
    }
    return `${sign}${compactNumber(numeric)}`;
}

function compactNumber(value) {
    const absolute = Math.abs(value);
    if (absolute >= 1_000_000) {
        return `${(value / 1_000_000).toFixed(1)}M`;
    }
    if (absolute >= 1_000) {
        return `${(value / 1_000).toFixed(1)}k`;
    }
    return value.toLocaleString(undefined, { maximumFractionDigits: 0 });
}

function formatCell(key, value) {
    if (value === null || value === undefined || value === "") {
        return `<span class="muted-cell">N/A</span>`;
    }
    if (typeof value === "number") {
        if (key.endsWith("_pct")) {
            return `${(value * 100).toFixed(1)}%`;
        }
        if (key === "stripping_ratio") {
            return value.toFixed(2);
        }
        if (key.includes("count") || key.includes("rows") || key.includes("days")) {
            return value.toLocaleString();
        }
        if (Math.abs(value) >= 100 || Number.isInteger(value)) {
            return value.toLocaleString(undefined, { maximumFractionDigits: 0 });
        }
        return value.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 });
    }
    if (typeof value === "boolean") {
        return value ? "Yes" : "No";
    }
    if (isIsoDate(value)) {
        return formatDate(value);
    }
    if (isIsoDateTime(value)) {
        return formatDateTime(value);
    }
    return escapeHtml(String(value));
}

function formatRange(from, to) {
    const fromLabel = formatDate(from) || "N/A";
    const toLabel = formatDate(to) || "N/A";
    return `${fromLabel} to ${toLabel}`;
}

function formatDate(value) {
    if (!value) {
        return "";
    }
    const parsed = new Date(`${value}T00:00:00`);
    if (Number.isNaN(parsed.getTime())) {
        return "";
    }
    return new Intl.DateTimeFormat(undefined, {
        month: "short",
        day: "numeric",
        year: "numeric",
    }).format(parsed);
}

function formatDateTime(value) {
    if (!value) {
        return "";
    }
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
        return "";
    }
    return new Intl.DateTimeFormat(undefined, {
        year: "numeric",
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
    }).format(parsed);
}

function formatHeader(value) {
    return String(value).replace(/_/g, " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

function slugify(value) {
    return String(value).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

function escapeHtml(value) {
    return String(value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
}

function isIsoDate(value) {
    return typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function isIsoDateTime(value) {
    return typeof value === "string" && /^\d{4}-\d{2}-\d{2}T/.test(value);
}
