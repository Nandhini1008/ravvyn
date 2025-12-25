"""
Sync Service - Automatic synchronization of Google Sheets and Docs to Database
"""

from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any
from sqlalchemy.orm import Session
from services.database import (
    SheetsMetadata, SheetsData, DocsMetadata, DocsContent,
    get_db_context
)
from core.exceptions import DatabaseError, ServiceError
from services.sheets import SheetsService
from services.docs import DocsService
from services.hash_service import HashService
import logging
import asyncio

logger = logging.getLogger(__name__)


def _normalize_datetime(dt: Optional[datetime]) -> Optional[datetime]:
    """
    Normalize datetime to timezone-naive UTC for comparison
    Converts timezone-aware datetimes to naive UTC, leaves naive datetimes as-is
    """
    if dt is None:
        return None
    
    # If timezone-aware, convert to UTC and remove timezone info
    if dt.tzinfo is not None:
        # Convert to UTC and make naive
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    
    # Already naive, assume it's UTC
    return dt


class SyncService:
    def __init__(self):
        self.sheets_service = SheetsService()
        self.docs_service = DocsService()
        self.hash_service = HashService()
    
    async def sync_all_sheets(self, force: bool = False) -> Dict:
        """
        Sync all Google Sheets to database
        force: If True, sync even if already up to date
        """
        stats = {
            'total': 0,
            'synced': 0,
            'skipped': 0,
            'errors': 0,
            'error_details': [],
            'started_at': datetime.utcnow().isoformat()
        }
        
        try:
            logger.info(f"Starting sync_all_sheets (force={force})")
            # Clear cache before syncing to get fresh modified_time data
            try:
                cache_key = self.sheets_service.cache._generate_key('sheets_list')
                self.sheets_service.cache.delete(cache_key)
                logger.debug("Cleared sheets list cache for fresh sync")
            except Exception as e:
                logger.warning(f"Could not clear cache: {str(e)}")
            
            # Get all sheets from Google
            try:
                sheets = await self.sheets_service.list_sheets()
            except Exception as e:
                error_msg = str(e)
                # Check for network connectivity issues
                if any(keyword in error_msg.lower() for keyword in ['unable to find the server', 'connection', 'network', 'dns', 'socket', 'gaierror']):
                    logger.error(f"Network connectivity issue: Cannot reach Google APIs. Error: {error_msg}")
                    logger.info("💡 Please check:")
                    logger.info("   1. Internet connection is active")
                    logger.info("   2. Firewall is not blocking Google APIs")
                    logger.info("   3. DNS resolution is working")
                    logger.info("   4. Proxy settings if behind a corporate firewall")
                    stats['errors'] = 1
                    stats['error_details'].append({
                        'type': 'network_error',
                        'message': 'Cannot reach Google APIs - check internet connection',
                        'details': error_msg
                    })
                    return stats
                else:
                    # Re-raise other errors
                    raise
            
            # Filter to only process the specific sheet ID
            # Only sync the target sheet ID (1ajWB1qm5a_HedC9Bdo4w14RqLmiKhRzjkzzl3iCaLVg)
            target_sheet_id = '1ajWB1qm5a_HedC9Bdo4w14RqLmiKhRzjkzzl3iCaLVg'
            
            # Filter sheets to only include the target sheet
            filtered_sheets = [s for s in sheets if s['id'] == target_sheet_id]
            
            if len(sheets) != len(filtered_sheets):
                logger.info(f"🔒 Filtered sheets: {len(sheets)} total, {len(filtered_sheets)} matching target sheet ID ({target_sheet_id})")
                skipped_sheets = [s for s in sheets if s['id'] != target_sheet_id]
                for skipped in skipped_sheets:
                    logger.info(f"⏭️  Skipping sheet: {skipped['name']} ({skipped['id']}) - not the target sheet")
            
            stats['total'] = len(filtered_sheets)
            logger.info(f"Found {stats['total']} sheets to sync (filtered to target sheet only)")
            
            # Process each sheet with its own database session
            for idx, sheet in enumerate(filtered_sheets, 1):
                try:
                    sheet_id = sheet['id']
                    sheet_name = sheet['name']
                    modified_time = None
                    created_time = None
                    
                    logger.debug(f"[{idx}/{stats['total']}] Processing sheet: {sheet_name} ({sheet_id})")
                    
                    if sheet.get('modifiedTime'):
                        try:
                            modified_time = datetime.fromisoformat(
                                sheet['modifiedTime'].replace('Z', '+00:00')
                            )
                        except (ValueError, AttributeError):
                            try:
                                from dateutil import parser
                                modified_time = parser.parse(sheet['modifiedTime'])
                            except Exception as e:
                                logger.warning(f"Could not parse modifiedTime for sheet {sheet_id}: {str(e)}")
                    
                    if sheet.get('createdTime'):
                        try:
                            created_time = datetime.fromisoformat(
                                sheet['createdTime'].replace('Z', '+00:00')
                            )
                        except (ValueError, AttributeError):
                            try:
                                from dateutil import parser
                                created_time = parser.parse(sheet['createdTime'])
                            except Exception as e:
                                logger.warning(f"Could not parse createdTime for sheet {sheet_id}: {str(e)}")
                    
                    # Check if needs sync and sync with proper session management
                    with get_db_context() as db:
                        # Check if needs sync
                        if not force:
                            existing = db.query(SheetsMetadata).filter(
                                SheetsMetadata.sheet_id == sheet_id
                            ).first()
                            
                            # More aggressive sync: sync if modified_time changed OR if last sync was > 2 minutes ago
                            should_sync = True
                            if existing and existing.modified_time and modified_time:
                                # Normalize both datetimes for comparison
                                existing_modified = _normalize_datetime(existing.modified_time)
                                new_modified = _normalize_datetime(modified_time)
                                
                                # Check if file was modified
                                if existing_modified and new_modified and existing_modified >= new_modified:
                                    # File not modified, but check if we need to sync anyway (for data freshness)
                                    if existing.last_synced:
                                        last_synced_naive = _normalize_datetime(existing.last_synced)
                                        if last_synced_naive:
                                            time_since_sync = datetime.utcnow() - last_synced_naive
                                            # Only skip if file not modified AND synced within last 2 minutes
                                            if time_since_sync < timedelta(minutes=2):
                                                should_sync = False
                            
                            if not should_sync:
                                stats['skipped'] += 1
                                reason = "file not modified"
                                if existing.last_synced:
                                    last_synced_naive = _normalize_datetime(existing.last_synced)
                                    if last_synced_naive:
                                        time_since_sync = datetime.utcnow() - last_synced_naive
                                        if time_since_sync < timedelta(minutes=2):
                                            reason = f"file not modified and synced {time_since_sync.total_seconds()/60:.1f} minutes ago"
                                logger.info(f"⏭️  Skipping {sheet_name} - {reason}")
                                continue
                        
                        # Sync this sheet
                        logger.info(f"Syncing sheet: {sheet_name} ({sheet_id})")
                        await self.sync_sheet(sheet_id, sheet_name, modified_time, created_time, db)
                        stats['synced'] += 1
                        logger.info(f"✅ Successfully synced: {sheet_name}")
                    
                except Exception as e:
                    stats['errors'] += 1
                    error_detail = {
                        'sheet_id': sheet.get('id', 'unknown'),
                        'sheet_name': sheet.get('name', 'unknown'),
                        'error': str(e),
                        'error_type': type(e).__name__
                    }
                    stats['error_details'].append(error_detail)
                    logger.error(f"❌ Error syncing sheet {sheet.get('name', 'unknown')} ({sheet.get('id', 'unknown')}): {str(e)}", exc_info=True)
            
            stats['completed_at'] = datetime.utcnow().isoformat()
            logger.info(f"Sync completed: {stats['synced']} synced, {stats['skipped']} skipped, {stats['errors']} errors out of {stats['total']} total")
            return stats
            
        except Exception as e:
            error_msg = str(e)
            error_type = type(e).__name__
            
            # Check for network connectivity issues
            is_network_error = any(keyword in error_msg.lower() for keyword in [
                'unable to find the server', 'connection', 'network', 'dns', 
                'socket', 'gaierror', 'server not found', 'name resolution'
            ]) or 'gaierror' in error_type.lower() or 'ServerNotFoundError' in error_type
            
            stats['completed_at'] = datetime.utcnow().isoformat()
            stats['fatal_error'] = error_msg
            
            if is_network_error:
                logger.error(f"Network connectivity issue in sync_all_sheets: {error_msg}")
                logger.info("💡 Network connectivity tips:")
                logger.info("   - Check your internet connection")
                logger.info("   - Verify firewall is not blocking Google APIs")
                logger.info("   - Check DNS resolution (try: ping oauth2.googleapis.com)")
                logger.info("   - If behind proxy, configure proxy settings")
                stats['errors'] = 1
                stats['error_details'].append({
                    'type': 'network_error',
                    'message': 'Cannot reach Google APIs - check internet connection',
                    'details': error_msg
                })
                # Don't raise for network errors - allow graceful degradation
                return stats
            else:
                logger.error(f"Fatal error in sync_all_sheets: {error_msg}", exc_info=True)
                raise ServiceError(
                    f"Failed to sync all sheets: {error_msg}",
                    service_name="SyncService"
                )
    
    async def sync_sheet(self, sheet_id: str, sheet_name: str,
                        modified_time: Optional[datetime] = None,
                        created_time: Optional[datetime] = None,
                        db: Optional[Session] = None) -> bool:
        """
        Sync a single sheet (all tabs and rows) to database
        
        Args:
            sheet_id: Google Sheet ID
            sheet_name: Sheet name
            modified_time: Last modified time
            created_time: Creation time
            db: Optional database session (if not provided, creates a new one)
        
        Returns:
            True if sync successful
        
        Raises:
            DatabaseError: If database operation fails
            ServiceError: If sync operation fails
        """
        should_close = False
        db_context = None
        if db is None:
            db_context = get_db_context()
            db = db_context.__enter__()
            should_close = True
        
        try:
            # Update or create metadata
            metadata = db.query(SheetsMetadata).filter(
                SheetsMetadata.sheet_id == sheet_id
            ).first()
            
            # Normalize datetimes before storing (store as naive UTC)
            normalized_modified = _normalize_datetime(modified_time)
            normalized_created = _normalize_datetime(created_time)
            
            if not metadata:
                metadata = SheetsMetadata(
                    sheet_id=sheet_id,
                    sheet_name=sheet_name,
                    created_time=normalized_created,
                    modified_time=normalized_modified,
                    sync_status='syncing'
                )
                db.add(metadata)
            else:
                metadata.sheet_name = sheet_name
                metadata.modified_time = normalized_modified
                metadata.created_time = normalized_created
                metadata.sync_status = 'syncing'
                metadata.error_message = None
            
            db.flush()
            
            # Get all tabs from the sheet
            try:
                spreadsheet = self.sheets_service.sheets_service.spreadsheets().get(
                    spreadsheetId=sheet_id
                ).execute()
                
                sheets_in_spreadsheet = spreadsheet.get('sheets', [])
                
                # Collect all sheet data for hash computation
                all_sheet_data = []
                for sheet_tab in sheets_in_spreadsheet:
                    tab_name = sheet_tab['properties']['title']
                    try:
                        # Read sheet data for hash computation
                        tab_data = await self.sheets_service.read_sheet(sheet_id, tab_name, limit=10000)
                        all_sheet_data.extend(tab_data)
                    except Exception as e:
                        logger.warning(f"Failed to read tab {tab_name} for hashing: {str(e)}")
                        continue
                
                # Compute hashes and detect changes
                hash_changes = None
                try:
                    hash_result = await self.hash_service.process_file_with_change_detection(
                        sheet_id, 'sheet', all_sheet_data
                    )
                    hash_changes = hash_result.get('has_changes', True)  # Default to True if hash computation fails
                    logger.info(f"Hash-based change detection for sheet {sheet_id}: changes={hash_changes}")
                except Exception as e:
                    logger.warning(f"Hash computation failed for sheet {sheet_id}, proceeding with full sync: {str(e)}")
                    hash_changes = True  # Fall back to full sync on hash failure
                
                # Only update database if changes detected or hash computation failed
                if hash_changes:
                    # Delete old data for this sheet
                    db.query(SheetsData).filter(
                        SheetsData.sheet_id == sheet_id
                    ).delete()
                    
                    # Sync each tab
                    for sheet_tab in sheets_in_spreadsheet:
                        tab_name = sheet_tab['properties']['title']
                        await self._sync_sheet_tab(sheet_id, tab_name, db)
                    
                    logger.info(f"Updated database for sheet {sheet_id} due to detected changes")
                else:
                    logger.info(f"Skipped database update for sheet {sheet_id} - no changes detected")
                
                # Update metadata
                metadata.sync_status = 'completed'
                metadata.last_synced = datetime.utcnow()
                db.commit()
                
                return True
                
            except Exception as e:
                metadata.sync_status = 'error'
                metadata.error_message = str(e)
                db.commit()
                logger.error(f"Error syncing sheet {sheet_id}: {str(e)}", exc_info=True)
                raise DatabaseError(
                    f"Failed to sync sheet {sheet_id}: {str(e)}",
                    operation="sync_sheet",
                    details={'sheet_id': sheet_id, 'sheet_name': sheet_name}
                )
        
        except DatabaseError:
            raise
        except Exception as e:
            if should_close:
                try:
                    db.rollback()
                except Exception:
                    pass
            logger.error(f"Error in sync_sheet: {str(e)}", exc_info=True)
            raise ServiceError(
                f"Failed to sync sheet: {str(e)}",
                service_name="SyncService",
                details={'sheet_id': sheet_id, 'sheet_name': sheet_name}
            )
        finally:
            if should_close:
                try:
                    db_context.__exit__(None, None, None)
                except Exception:
                    pass
    
    async def _sync_sheet_tab(self, sheet_id: str, tab_name: str, db: Session):
        """Sync a single tab of a sheet"""
        try:
            # Read all data from the tab (up to 10000 rows)
            values = await self.sheets_service.read_sheet(sheet_id, tab_name, limit=10000)
            
            # Insert rows into database
            for row_index, row_data in enumerate(values):
                sheet_data = SheetsData(
                    sheet_id=sheet_id,
                    tab_name=tab_name,
                    row_index=row_index,
                    row_data=row_data
                )
                db.add(sheet_data)
            
            db.flush()
            
        except Exception as e:
            logger.error(f"Error syncing tab {tab_name} in sheet {sheet_id}: {str(e)}")
            raise
    
    async def sync_all_docs(self, force: bool = False) -> Dict:
        """
        Sync all Google Docs to database
        force: If True, sync even if already up to date
        """
        stats = {
            'total': 0,
            'synced': 0,
            'skipped': 0,
            'errors': 0,
            'error_details': []
        }
        
        try:
            # Get all docs from Google
            docs = await self.docs_service.list_docs()
            stats['total'] = len(docs)
            
            # Process each doc with its own database session
            for doc in docs:
                try:
                    doc_id = doc['id']
                    doc_name = doc['name']
                    modified_time = None
                    created_time = None
                    
                    if doc.get('modifiedTime'):
                        try:
                            modified_time = datetime.fromisoformat(
                                doc['modifiedTime'].replace('Z', '+00:00')
                            )
                        except (ValueError, AttributeError):
                            try:
                                from dateutil import parser
                                modified_time = parser.parse(doc['modifiedTime'])
                            except Exception as e:
                                logger.warning(f"Could not parse modifiedTime for doc {doc_id}: {str(e)}")
                    
                    if doc.get('createdTime'):
                        try:
                            created_time = datetime.fromisoformat(
                                doc['createdTime'].replace('Z', '+00:00')
                            )
                        except (ValueError, AttributeError):
                            try:
                                from dateutil import parser
                                created_time = parser.parse(doc['createdTime'])
                            except Exception as e:
                                logger.warning(f"Could not parse createdTime for doc {doc_id}: {str(e)}")
                    
                    # Check if needs sync and sync with proper session management
                    with get_db_context() as db:
                        # Check if needs sync
                        if not force:
                            existing = db.query(DocsMetadata).filter(
                                DocsMetadata.doc_id == doc_id
                            ).first()
                            
                            if existing and existing.modified_time and modified_time:
                                # Normalize both datetimes for comparison
                                existing_modified = _normalize_datetime(existing.modified_time)
                                new_modified = _normalize_datetime(modified_time)
                                
                                if existing_modified and new_modified and existing_modified >= new_modified:
                                    stats['skipped'] += 1
                                    continue
                        
                        # Sync this doc
                        await self.sync_doc(doc_id, doc_name, modified_time, created_time, db)
                        stats['synced'] += 1
                    
                except Exception as e:
                    stats['errors'] += 1
                    stats['error_details'].append({
                        'doc_id': doc.get('id', 'unknown'),
                        'error': str(e)
                    })
                    logger.error(f"Error syncing doc {doc.get('id')}: {str(e)}", exc_info=True)
            
            return stats
            
        except Exception as e:
            logger.error(f"Error in sync_all_docs: {str(e)}", exc_info=True)
            raise ServiceError(
                f"Failed to sync all docs: {str(e)}",
                service_name="SyncService"
            )
    
    async def sync_doc(self, doc_id: str, doc_name: str,
                      modified_time: Optional[datetime] = None,
                      created_time: Optional[datetime] = None,
                      db: Optional[Session] = None) -> bool:
        """
        Sync a single document to database
        
        Args:
            doc_id: Google Doc ID
            doc_name: Document name
            modified_time: Last modified time
            created_time: Creation time
            db: Optional database session (if not provided, creates a new one)
        
        Returns:
            True if sync successful
        
        Raises:
            DatabaseError: If database operation fails
            ServiceError: If sync operation fails
        """
        should_close = False
        db_context = None
        if db is None:
            db_context = get_db_context()
            db = db_context.__enter__()
            should_close = True
        
        try:
            # Update or create metadata
            metadata = db.query(DocsMetadata).filter(
                DocsMetadata.doc_id == doc_id
            ).first()
            
            # Normalize datetimes before storing (store as naive UTC)
            normalized_modified = _normalize_datetime(modified_time)
            normalized_created = _normalize_datetime(created_time)
            
            if not metadata:
                metadata = DocsMetadata(
                    doc_id=doc_id,
                    doc_name=doc_name,
                    created_time=normalized_created,
                    modified_time=normalized_modified,
                    sync_status='syncing'
                )
                db.add(metadata)
            else:
                metadata.doc_name = doc_name
                metadata.modified_time = normalized_modified
                metadata.created_time = normalized_created
                metadata.sync_status = 'syncing'
                metadata.error_message = None
            
            db.flush()
            
            # Read document content
            try:
                content = await self.docs_service.read_doc(doc_id)
                content_length = len(content)
                
                # Compute hashes and detect changes
                hash_changes = None
                try:
                    hash_result = await self.hash_service.process_file_with_change_detection(
                        doc_id, 'doc', content
                    )
                    hash_changes = hash_result.get('has_changes', True)  # Default to True if hash computation fails
                    logger.info(f"Hash-based change detection for doc {doc_id}: changes={hash_changes}")
                except Exception as e:
                    logger.warning(f"Hash computation failed for doc {doc_id}, proceeding with full sync: {str(e)}")
                    hash_changes = True  # Fall back to full sync on hash failure
                
                # Only update database if changes detected or hash computation failed
                if hash_changes:
                    # Update or create content
                    doc_content = db.query(DocsContent).filter(
                        DocsContent.doc_id == doc_id
                    ).first()
                    
                    if not doc_content:
                        doc_content = DocsContent(
                            doc_id=doc_id,
                            content=content,
                            content_length=content_length
                        )
                        db.add(doc_content)
                    else:
                        doc_content.content = content
                        doc_content.content_length = content_length
                        doc_content.synced_at = datetime.utcnow()
                    
                    logger.info(f"Updated database for doc {doc_id} due to detected changes")
                else:
                    logger.info(f"Skipped database update for doc {doc_id} - no changes detected")
                
                # Update metadata
                metadata.sync_status = 'completed'
                metadata.last_synced = datetime.utcnow()
                db.commit()
                
                return True
                
            except Exception as e:
                metadata.sync_status = 'error'
                metadata.error_message = str(e)
                db.commit()
                logger.error(f"Error syncing doc {doc_id}: {str(e)}", exc_info=True)
                raise DatabaseError(
                    f"Failed to sync doc {doc_id}: {str(e)}",
                    operation="sync_doc",
                    details={'doc_id': doc_id, 'doc_name': doc_name}
                )
        
        except DatabaseError:
            raise
        except Exception as e:
            if should_close:
                try:
                    db.rollback()
                except Exception:
                    pass
            logger.error(f"Error in sync_doc: {str(e)}", exc_info=True)
            raise ServiceError(
                f"Failed to sync doc: {str(e)}",
                service_name="SyncService",
                details={'doc_id': doc_id, 'doc_name': doc_name}
            )
        finally:
            if should_close:
                try:
                    db_context.__exit__(None, None, None)
                except Exception:
                    pass
    
    async def sync_all(self, force: bool = False) -> Dict:
        """Sync both sheets and docs"""
        sheets_stats = await self.sync_all_sheets(force=force)
        docs_stats = await self.sync_all_docs(force=force)
        
        return {
            'sheets': sheets_stats,
            'docs': docs_stats,
            'total_synced': sheets_stats['synced'] + docs_stats['synced'],
            'total_errors': sheets_stats['errors'] + docs_stats['errors']
        }
    
    async def get_sync_statistics_with_hashes(self) -> Dict[str, Any]:
        """
        Get comprehensive sync statistics including hash information.
        
        Returns:
            Dictionary with sync and hash statistics
        """
        try:
            # Get hash service statistics
            hash_stats = await self.hash_service.get_service_statistics()
            
            # Get basic sync statistics from database
            with get_db_context() as db:
                from sqlalchemy import func
                
                # Count sheets and docs
                sheets_count = db.query(func.count(SheetsMetadata.id)).scalar() or 0
                docs_count = db.query(func.count(DocsMetadata.id)).scalar() or 0
                
                # Count recent syncs (last 24 hours)
                recent_cutoff = datetime.utcnow() - timedelta(hours=24)
                recent_sheet_syncs = db.query(func.count(SheetsMetadata.id)).filter(
                    SheetsMetadata.last_synced >= recent_cutoff
                ).scalar() or 0
                recent_doc_syncs = db.query(func.count(DocsMetadata.id)).filter(
                    DocsMetadata.last_synced >= recent_cutoff
                ).scalar() or 0
                
                # Count sync errors
                error_sheets = db.query(func.count(SheetsMetadata.id)).filter(
                    SheetsMetadata.sync_status == 'error'
                ).scalar() or 0
                error_docs = db.query(func.count(DocsMetadata.id)).filter(
                    DocsMetadata.sync_status == 'error'
                ).scalar() or 0
            
            return {
                'sync_statistics': {
                    'total_sheets': sheets_count,
                    'total_docs': docs_count,
                    'recent_sheet_syncs_24h': recent_sheet_syncs,
                    'recent_doc_syncs_24h': recent_doc_syncs,
                    'error_sheets': error_sheets,
                    'error_docs': error_docs
                },
                'hash_statistics': hash_stats,
                'integration_status': 'active'
            }
            
        except Exception as e:
            logger.error(f"Error getting sync statistics with hashes: {str(e)}")
            return {
                'sync_statistics': {},
                'hash_statistics': {},
                'integration_status': 'error',
                'error': str(e)
            }

