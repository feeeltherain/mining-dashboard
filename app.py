from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, Request, Response
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from plotly.offline import get_plotlyjs_version

from src.dashboard_api import DEFAULT_INPUT_PATH, SAMPLE_PATH, TEMPLATE_PATH, build_dashboard_payload


ROOT = Path(__file__).resolve().parent
PUBLIC_DIR = ROOT / "public"
TEMPLATE_DIR = ROOT / "templates"
INDEX_TEMPLATE = (TEMPLATE_DIR / "index.html").read_text(encoding="utf-8")
PLOTLY_VERSION = get_plotlyjs_version()

app = FastAPI(
    title="Mining Operations Dashboard",
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
)


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def index() -> HTMLResponse:
    html = INDEX_TEMPLATE.replace("__PLOTLY_VERSION__", PLOTLY_VERSION)
    return HTMLResponse(html)


@app.get("/app.js", include_in_schema=False)
async def app_js() -> FileResponse:
    return FileResponse(PUBLIC_DIR / "app.js", media_type="application/javascript")


@app.get("/styles.css", include_in_schema=False)
async def styles_css() -> FileResponse:
    return FileResponse(PUBLIC_DIR / "styles.css", media_type="text/css")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon() -> Response:
    return Response(status_code=204)


@app.get("/api/health")
async def health() -> JSONResponse:
    return JSONResponse({"ok": True, "default_workbook_present": DEFAULT_INPUT_PATH.exists()})


@app.get("/api/dashboard")
async def dashboard_default(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    mine_areas: Optional[List[str]] = None,
    mine_groups: Optional[List[str]] = None,
    mine_equipment: Optional[List[str]] = None,
    mine_selected_unit: Optional[str] = None,
    mine_heat_metric: Optional[str] = None,
    fleet_selected_unit: Optional[str] = None,
    fleet_heat_metric: Optional[str] = None,
) -> JSONResponse:
    payload = build_dashboard_payload(
        workbook_content=None,
        date_from=date_from,
        date_to=date_to,
        mine_areas=mine_areas,
        mine_groups=mine_groups,
        mine_equipment=mine_equipment,
        mine_selected_unit=mine_selected_unit,
        mine_heat_metric=mine_heat_metric,
        fleet_selected_unit=fleet_selected_unit,
        fleet_heat_metric=fleet_heat_metric,
    )
    return JSONResponse(payload)


@app.post("/api/dashboard")
async def dashboard_uploaded(
    request: Request,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    mine_areas: Optional[List[str]] = None,
    mine_groups: Optional[List[str]] = None,
    mine_equipment: Optional[List[str]] = None,
    mine_selected_unit: Optional[str] = None,
    mine_heat_metric: Optional[str] = None,
    fleet_selected_unit: Optional[str] = None,
    fleet_heat_metric: Optional[str] = None,
) -> JSONResponse:
    body = await request.body()
    payload = build_dashboard_payload(
        workbook_content=body or None,
        date_from=date_from,
        date_to=date_to,
        mine_areas=mine_areas,
        mine_groups=mine_groups,
        mine_equipment=mine_equipment,
        mine_selected_unit=mine_selected_unit,
        mine_heat_metric=mine_heat_metric,
        fleet_selected_unit=fleet_selected_unit,
        fleet_heat_metric=fleet_heat_metric,
    )
    return JSONResponse(payload)


@app.get("/api/template", include_in_schema=False)
async def template_download() -> FileResponse:
    return FileResponse(
        TEMPLATE_PATH,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=TEMPLATE_PATH.name,
    )


@app.get("/api/sample", include_in_schema=False)
async def sample_download() -> FileResponse:
    return FileResponse(
        SAMPLE_PATH,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=SAMPLE_PATH.name,
    )
