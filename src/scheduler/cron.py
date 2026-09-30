import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from src.config.settings import settings
from src.pipeline.etl import run_etl_pipeline

logger = logging.getLogger("Scheduler")
scheduler = AsyncIOScheduler()

def start_scheduler():
    trigger = CronTrigger(hour=settings.CRON_HOUR, minute=settings.CRON_MINUTE)
    scheduler.add_job(
        run_etl_pipeline,
        trigger=trigger,
        id="daily_etl_sync",
        name="Daily ETL Sync at 22:00",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler started. Daily ETL scheduled at %02d:%02d", settings.CRON_HOUR, settings.CRON_MINUTE)

def shutdown_scheduler():
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler shutdown.")
