from fastapi import APIRouter, HTTPException, BackgroundTasks
from src.pipeline.etl import run_etl_pipeline
from datetime import datetime

router = APIRouter(prefix="/api/v1/analytics", tags=["analytics"])

@router.get("/status")
async def get_status():
    return {
        "status": "online",
        "service": "analytics-service",
        "timestamp": datetime.utcnow().isoformat()
    }

@router.post("/pipeline/trigger")
async def trigger_pipeline(background_tasks: BackgroundTasks):
    """
    On-Demand trigger for ETL pipeline from Admin UI
    """
    background_tasks.add_task(run_etl_pipeline)
    return {
        "status": "triggered",
        "message": "ETL pipeline triggered in background",
        "timestamp": datetime.utcnow().isoformat()
    }

@router.get("/overview")
async def get_overview():
    return {
        "message": "Analytics overview placeholder",
        "metrics": {
            "total_clicks": 0,
            "devices": {},
            "browsers": {}
        }
    }
