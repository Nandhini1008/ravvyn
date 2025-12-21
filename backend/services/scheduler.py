"""
Scheduler Service - Background task scheduling for automatic sync
"""

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import os
import logging
from services.sync_service import SyncService
from services.cache import get_cache_service
from core.config import get_settings

logger = logging.getLogger(__name__)

scheduler = None
sync_service = None


def init_scheduler():
    """Initialize and start the scheduler"""
    global scheduler, sync_service
    
    if scheduler is not None:
        return scheduler
    
    # Get settings to check auto sync and interval
    settings = get_settings()
    if not settings.auto_sync_enabled:
        logger.info("Auto sync is disabled")
        return None
    
    # Get sync interval from settings
    sync_interval_minutes = settings.sync_interval_minutes
    
    # Initialize scheduler
    scheduler = AsyncIOScheduler()
    sync_service = SyncService()
    
    # Add job to sync all sheets and docs periodically
    scheduler.add_job(
        sync_all_job,
        trigger=IntervalTrigger(minutes=sync_interval_minutes),
        id='sync_all',
        name='Sync all sheets and docs',
        replace_existing=True
    )
    
    # Add job to cleanup expired cache entries periodically
    cleanup_interval = settings.cache_cleanup_interval
    scheduler.add_job(
        cache_cleanup_job,
        trigger=IntervalTrigger(seconds=cleanup_interval),
        id='cache_cleanup',
        name='Cleanup expired cache entries',
        replace_existing=True
    )
    
    logger.info(f"Scheduler initialized with sync interval: {sync_interval_minutes} minutes, cache cleanup: {cleanup_interval}s")
    return scheduler


async def sync_all_job():
    """Job function to sync all sheets and docs"""
    try:
        logger.info("=" * 70)
        logger.info("🔄 Starting scheduled sync of all sheets and docs")
        logger.info("=" * 70)
        
        stats = await sync_service.sync_all(force=False)
        
        logger.info("=" * 70)
        logger.info("✅ Scheduled sync completed")
        logger.info(f"   Sheets: {stats.get('sheets', {}).get('synced', 0)} synced, {stats.get('sheets', {}).get('skipped', 0)} skipped, {stats.get('sheets', {}).get('errors', 0)} errors")
        logger.info(f"   Docs: {stats.get('docs', {}).get('synced', 0)} synced, {stats.get('docs', {}).get('skipped', 0)} skipped, {stats.get('docs', {}).get('errors', 0)} errors")
        
        if stats.get('total_errors', 0) > 0:
            logger.warning(f"⚠️  Total errors: {stats.get('total_errors', 0)}")
            # Log error details
            for error_type in ['sheets', 'docs']:
                error_details = stats.get(error_type, {}).get('error_details', [])
                if error_details:
                    logger.warning(f"   {error_type.upper()} errors:")
                    for err in error_details[:5]:  # Show first 5 errors
                        logger.warning(f"     - {err.get('sheet_name', err.get('doc_name', 'unknown'))}: {err.get('error', 'unknown error')}")
        
        logger.info("=" * 70)
    except Exception as e:
        logger.error("=" * 70)
        logger.error(f"❌ CRITICAL ERROR in scheduled sync: {str(e)}", exc_info=True)
        logger.error("=" * 70)


async def cache_cleanup_job():
    """Job function to cleanup expired cache entries"""
    try:
        cache = get_cache_service()
        count = cache.cleanup_expired()
        if count > 0:
            logger.debug(f"Cleaned up {count} expired cache entries")
    except Exception as e:
        logger.error(f"Error in cache cleanup job: {str(e)}")


def start_scheduler():
    """Start the scheduler"""
    global scheduler
    if scheduler is None:
        scheduler = init_scheduler()
    
    if scheduler and not scheduler.running:
        scheduler.start()
        logger.info("Scheduler started")
    elif scheduler and scheduler.running:
        logger.info("Scheduler already running")
    else:
        logger.info("Scheduler not initialized (auto sync disabled)")


def stop_scheduler():
    """Stop the scheduler"""
    global scheduler
    if scheduler and scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler stopped")

