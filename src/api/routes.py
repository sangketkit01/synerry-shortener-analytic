import csv
import io
import logging
import httpx
from datetime import datetime, timedelta
from typing import Optional
from fastapi import APIRouter, Depends, Query, BackgroundTasks, Response, HTTPException, Header
from fastapi.responses import StreamingResponse
from sqlalchemy import func, distinct, desc
from sqlalchemy.orm import Session
from src.config.database import get_db
from src.config.settings import settings
from src.pipeline.etl import run_etl_pipeline

logger = logging.getLogger("AnalyticsAPI")
from src.models.star_schema import (
    DimDevice,
    DimBrowser,
    DimPlatform,
    DimReferrer,
    DimCountry,
    DimUrl,
    FactClick,
)

router = APIRouter(prefix="/api/v1/analytics", tags=["analytics"])

def check_scope_authorization(
    url_id: Optional[str],
    short_code: Optional[str],
    user_id: Optional[str],
    x_admin_request: Optional[str],
    x_internal_key: Optional[str],
):
    """
    Prevents leakage of multi-tenant global click metrics.
    Only requests with explicit URL/User scope or authenticated Admin gateway calls are allowed.
    """
    if url_id or short_code or user_id:
        return
    if x_admin_request == "true" and x_internal_key == settings.INTERNAL_PIPELINE_KEY:
        return
    raise HTTPException(
        status_code=400,
        detail="Scope filter required: specify user_id, url_id, or short_code.",
    )

@router.get("/status")
async def get_status():
    return {
        "status": "online",
        "service": "analytics-service",
        "timestamp": datetime.utcnow().isoformat(),
    }

@router.post("/pipeline/trigger")
async def trigger_pipeline():
    """
    On-Demand trigger for ETL pipeline (processes pending raw clicks into Star Schema).
    Returns real-time execution statistics for Admin UI.
    """
    start_time = datetime.utcnow()
    result = await run_etl_pipeline()
    end_time = datetime.utcnow()
    duration_ms = int((end_time - start_time).total_seconds() * 1000)

    if result.get("status") == "error":
        raise HTTPException(status_code=500, detail=result.get("message", "Pipeline execution failed"))

    processed = result.get("processed_events", 0)
    return {
        "success": True,
        "data": {
            "status": "COMPLETED",
            "processed_events": processed,
            "execution_time_ms": duration_ms,
            "timestamp": datetime.utcnow().isoformat(),
            "message": f"Successfully synchronized {processed} click events into Data Warehouse." if processed > 0 else "Data Warehouse is already up to date (0 pending events)."
        }
    }

@router.get("/pipeline/status")
async def get_pipeline_status(db: Session = Depends(get_db)):
    """
    Returns pipeline status: pending raw clicks in queue and total fact clicks.
    """
    pending_count = 0
    backend_url = f"{settings.BACKEND_INTERNAL_URL}/api/v1/internal/clicks/raw?limit=1000"
    headers = {"x-internal-key": settings.INTERNAL_PIPELINE_KEY}
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(backend_url, headers=headers)
            if resp.status_code == 200:
                pending_count = resp.json().get("count", 0)
    except Exception as e:
        logger.warning(f"Failed to fetch pending raw clicks: {e}")

    total_facts = db.query(func.count(FactClick.id)).scalar() or 0
    return {
        "success": True,
        "data": {
            "status": "READY",
            "pending_raw_clicks": pending_count,
            "total_fact_clicks": total_facts,
            "timestamp": datetime.utcnow().isoformat(),
        }
    }

@router.get("/summary")
async def get_summary(
    url_id: Optional[str] = Query(None),
    short_code: Optional[str] = Query(None),
    user_id: Optional[str] = Query(None),
    days: int = Query(7, ge=1, le=90),
    x_admin_request: Optional[str] = Header(None),
    x_internal_key: Optional[str] = Header(None),
    db: Session = Depends(get_db),
):
    """
    High-speed aggregate metrics and daily timeseries from Star Schema.
    Runs on-demand sync of unprocessed clicks first so metrics reflect real-time clicks.
    """
    check_scope_authorization(url_id, short_code, user_id, x_admin_request, x_internal_key)

    # Auto-sync on-demand for real-time consistency
    try:
        await run_etl_pipeline()
    except Exception as e:
        logger.warning(f"On-demand ETL sync before summary skipped: {e}")

    query = db.query(FactClick)

    # Scoping by specific URL or short code
    if url_id:
        if user_id:
            dim_url = db.query(DimUrl).filter(DimUrl.id == url_id, DimUrl.user_id == user_id).first()
            if not dim_url:
                return {"success": True, "data": {"total_clicks": 0, "unique_visitors": 0, "clicks_today": 0, "timeseries": []}}
        query = query.filter(FactClick.url_id == url_id)
    elif short_code:
        dim_query = db.query(DimUrl).filter(DimUrl.short_code == short_code)
        if user_id:
            dim_query = dim_query.filter(DimUrl.user_id == user_id)
        dim_url = dim_query.first()
        if dim_url:
            query = query.filter(FactClick.url_id == dim_url.id)
        else:
            return {
                "success": True,
                "data": {
                    "total_clicks": 0,
                    "unique_visitors": 0,
                    "clicks_today": 0,
                    "timeseries": [],
                },
            }
    elif user_id:
        # Scoped to authenticated user's URLs only (Prevents IDOR / Global Data Leak)
        user_url_ids = [u.id for u in db.query(DimUrl.id).filter(DimUrl.user_id == user_id).all()]
        if not user_url_ids:
            return {
                "success": True,
                "data": {
                    "total_clicks": 0,
                    "unique_visitors": 0,
                    "clicks_today": 0,
                    "timeseries": [],
                },
            }
        query = query.filter(FactClick.url_id.in_(user_url_ids))

    # Total clicks
    total_clicks = query.with_entities(func.count(FactClick.id)).scalar() or 0

    # Unique visitors (distinct masked IP)
    unique_visitors = (
        query.with_entities(func.count(distinct(FactClick.ip_masked))).scalar() or 0
    )

    # Clicks today (UTC)
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    clicks_today = (
        query.filter(FactClick.clicked_at >= today_start)
        .with_entities(func.count(FactClick.id))
        .scalar()
        or 0
    )

    # Timeseries for the past N days
    cutoff_date = datetime.utcnow() - timedelta(days=days)
    timeseries_raw = (
        query.filter(FactClick.clicked_at >= cutoff_date)
        .with_entities(
            func.date(FactClick.clicked_at).label("click_date"),
            func.count(FactClick.id).label("count"),
        )
        .group_by(func.date(FactClick.clicked_at))
        .order_by(func.date(FactClick.clicked_at))
        .all()
    )

    # Build dense date range map
    date_map = {row.click_date.strftime("%Y-%m-%d"): row.count for row in timeseries_raw}
    timeseries = []
    for i in range(days):
        d = (cutoff_date + timedelta(days=i + 1)).strftime("%Y-%m-%d")
        timeseries.append({"date": d, "clicks": date_map.get(d, 0)})

    return {
        "success": True,
        "data": {
            "total_clicks": total_clicks,
            "unique_visitors": unique_visitors,
            "clicks_today": clicks_today,
            "timeseries": timeseries,
        },
    }

@router.get("/breakdown")
async def get_breakdown(
    url_id: Optional[str] = Query(None),
    short_code: Optional[str] = Query(None),
    user_id: Optional[str] = Query(None),
    x_admin_request: Optional[str] = Header(None),
    x_internal_key: Optional[str] = Header(None),
    db: Session = Depends(get_db),
):
    """
    Dimensional breakdowns: Devices, Browsers, Platforms, Referrers, and Countries.
    """
    check_scope_authorization(url_id, short_code, user_id, x_admin_request, x_internal_key)
    base_query = db.query(FactClick)

    if url_id:
        if user_id:
            dim_url = db.query(DimUrl).filter(DimUrl.id == url_id, DimUrl.user_id == user_id).first()
            if not dim_url:
                return {"success": True, "data": {"total_clicks": 0, "devices": [], "browsers": [], "platforms": [], "referrers": [], "countries": []}}
        base_query = base_query.filter(FactClick.url_id == url_id)
    elif short_code:
        dim_query = db.query(DimUrl).filter(DimUrl.short_code == short_code)
        if user_id:
            dim_query = dim_query.filter(DimUrl.user_id == user_id)
        dim_url = dim_query.first()
        if dim_url:
            base_query = base_query.filter(FactClick.url_id == dim_url.id)
        else:
            return {
                "success": True,
                "data": {
                    "total_clicks": 0,
                    "devices": [],
                    "browsers": [],
                    "platforms": [],
                    "referrers": [],
                    "countries": [],
                },
            }
    elif user_id:
        user_url_ids = [u.id for u in db.query(DimUrl.id).filter(DimUrl.user_id == user_id).all()]
        if not user_url_ids:
            return {
                "success": True,
                "data": {
                    "total_clicks": 0,
                    "devices": [],
                    "browsers": [],
                    "platforms": [],
                    "referrers": [],
                    "countries": [],
                },
            }
        base_query = base_query.filter(FactClick.url_id.in_(user_url_ids))

    total_clicks = base_query.with_entities(func.count(FactClick.id)).scalar() or 0

    def calculate_distribution(results):
        items = []
        for name, count in results:
            pct = round((count / total_clicks * 100), 1) if total_clicks > 0 else 0
            items.append({"name": name, "count": count, "percentage": pct})
        return items

    # 1. Devices
    devices_raw = (
        base_query.join(DimDevice, FactClick.device_id == DimDevice.id)
        .with_entities(DimDevice.device_type, func.count(FactClick.id))
        .group_by(DimDevice.device_type)
        .order_by(desc(func.count(FactClick.id)))
        .all()
    )

    # 2. Browsers
    browsers_raw = (
        base_query.join(DimBrowser, FactClick.browser_id == DimBrowser.id)
        .with_entities(DimBrowser.name, func.count(FactClick.id))
        .group_by(DimBrowser.name)
        .order_by(desc(func.count(FactClick.id)))
        .all()
    )

    # 3. Platforms
    platforms_raw = (
        base_query.join(DimPlatform, FactClick.platform_id == DimPlatform.id)
        .with_entities(DimPlatform.name, func.count(FactClick.id))
        .group_by(DimPlatform.name)
        .order_by(desc(func.count(FactClick.id)))
        .all()
    )

    # 4. Referrers
    referrers_raw = (
        base_query.join(DimReferrer, FactClick.referrer_id == DimReferrer.id)
        .with_entities(DimReferrer.source_domain, func.count(FactClick.id))
        .group_by(DimReferrer.source_domain)
        .order_by(desc(func.count(FactClick.id)))
        .all()
    )

    # 5. Countries
    countries_raw = (
        base_query.join(DimCountry, FactClick.country_id == DimCountry.id)
        .with_entities(DimCountry.country_name, func.count(FactClick.id))
        .group_by(DimCountry.country_name)
        .order_by(desc(func.count(FactClick.id)))
        .all()
    )

    return {
        "success": True,
        "data": {
            "total_clicks": total_clicks,
            "devices": calculate_distribution(devices_raw),
            "browsers": calculate_distribution(browsers_raw),
            "platforms": calculate_distribution(platforms_raw),
            "referrers": calculate_distribution(referrers_raw),
            "countries": calculate_distribution(countries_raw),
        },
    }

@router.get("/export")
async def export_clicks_csv(
    url_id: Optional[str] = Query(None),
    short_code: Optional[str] = Query(None),
    user_id: Optional[str] = Query(None),
    x_admin_request: Optional[str] = Header(None),
    x_internal_key: Optional[str] = Header(None),
    db: Session = Depends(get_db),
):
    """
    Exports full fact click analytics to CSV (scoped by user or specific URL)
    """
    check_scope_authorization(url_id, short_code, user_id, x_admin_request, x_internal_key)
    query = (
        db.query(
            FactClick.clicked_at,
            DimUrl.short_code,
            DimUrl.original_url,
            DimDevice.device_type,
            DimBrowser.name.label("browser"),
            DimPlatform.name.label("platform"),
            DimReferrer.source_domain.label("referrer"),
            DimCountry.country_name.label("country"),
            FactClick.ip_masked,
        )
        .join(DimUrl, FactClick.url_id == DimUrl.id)
        .join(DimDevice, FactClick.device_id == DimDevice.id)
        .join(DimBrowser, FactClick.browser_id == DimBrowser.id)
        .join(DimPlatform, FactClick.platform_id == DimPlatform.id)
        .join(DimReferrer, FactClick.referrer_id == DimReferrer.id)
        .join(DimCountry, FactClick.country_id == DimCountry.id)
        .order_by(desc(FactClick.clicked_at))
    )

    if url_id:
        if user_id:
            query = query.filter(DimUrl.user_id == user_id)
        query = query.filter(FactClick.url_id == url_id)
    elif short_code:
        if user_id:
            query = query.filter(DimUrl.user_id == user_id)
        query = query.filter(DimUrl.short_code == short_code)
    elif user_id:
        query = query.filter(DimUrl.user_id == user_id)

    rows = query.limit(5000).all()
    if not rows:
        raise HTTPException(status_code=404, detail="No analytics records found to export.")

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Timestamp (UTC)",
        "Short Code",
        "Original URL",
        "Device",
        "Browser",
        "Platform / OS",
        "Referrer Domain",
        "Country",
        "Masked IP",
    ])

    for row in rows:
        writer.writerow([
            row.clicked_at.strftime("%Y-%m-%d %H:%M:%S"),
            row.short_code,
            row.original_url,
            row.device_type,
            row.browser,
            row.platform,
            row.referrer,
            row.country,
            row.ip_masked,
        ])

    output.seek(0)
    filename = f"synerry_analytics_{short_code or 'export'}_{datetime.utcnow().strftime('%Y%m%d')}.csv"

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )

@router.get("/recent-clicks")
async def get_recent_clicks(
    url_id: Optional[str] = Query(None),
    short_code: Optional[str] = Query(None),
    user_id: Optional[str] = Query(None),
    limit: int = Query(25, ge=1, le=100),
    x_admin_request: Optional[str] = Header(None),
    x_internal_key: Optional[str] = Header(None),
    db: Session = Depends(get_db),
):
    """
    Returns live feed of recent click events from Star Schema (scoped by user or specific URL)
    """
    check_scope_authorization(url_id, short_code, user_id, x_admin_request, x_internal_key)
    query = (
        db.query(
            FactClick.id,
            FactClick.clicked_at,
            DimUrl.short_code,
            DimUrl.original_url,
            DimDevice.device_type,
            DimBrowser.name.label("browser"),
            DimPlatform.name.label("platform"),
            DimReferrer.source_domain.label("referrer"),
            DimCountry.country_name.label("country"),
            FactClick.ip_masked,
        )
        .join(DimUrl, FactClick.url_id == DimUrl.id)
        .join(DimDevice, FactClick.device_id == DimDevice.id)
        .join(DimBrowser, FactClick.browser_id == DimBrowser.id)
        .join(DimPlatform, FactClick.platform_id == DimPlatform.id)
        .join(DimReferrer, FactClick.referrer_id == DimReferrer.id)
        .join(DimCountry, FactClick.country_id == DimCountry.id)
        .order_by(desc(FactClick.clicked_at))
    )

    if url_id:
        if user_id:
            query = query.filter(DimUrl.user_id == user_id)
        query = query.filter(FactClick.url_id == url_id)
    elif short_code:
        if user_id:
            query = query.filter(DimUrl.user_id == user_id)
        query = query.filter(DimUrl.short_code == short_code)
    elif user_id:
        query = query.filter(DimUrl.user_id == user_id)

    rows = query.limit(limit).all()

    clicks = [
        {
            "id": row.id,
            "clicked_at": row.clicked_at.isoformat(),
            "short_code": row.short_code,
            "original_url": row.original_url,
            "device": row.device_type,
            "browser": row.browser,
            "platform": row.platform,
            "referrer": row.referrer,
            "country": row.country,
            "ip_masked": row.ip_masked,
        }
        for row in rows
    ]

    return {"success": True, "data": {"clicks": clicks}}
