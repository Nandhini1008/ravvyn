"""
Hash Storage Service - Database operations for hash persistence
Manages saving, loading, and deleting hash data with proper write locks
"""

import logging
import time
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from services.database import FileHash, HashComputationLog, get_db_context, get_db_write_context
from services.hash_computer import Hash
from datetime import datetime

logger = logging.getLogger(__name__)


class HashStorage:
    """
    Hash storage service for database operations.
    Handles persistence of hash data with retry logic and error handling.
    """
    
    def __init__(self):
        """Initialize hash storage with configuration settings"""
        try:
            from core.config import get_settings
            settings = get_settings()
            
            self.max_retries = settings.hash_max_retries
            self.base_delay = settings.hash_retry_delay_seconds
            self.max_delay = settings.hash_max_retry_delay_seconds
        except Exception as e:
            logger.warning(f"Could not load settings, using defaults: {str(e)}")
            self.max_retries = 3
            self.base_delay = 1.0  # Base delay for exponential backoff
            self.max_delay = 30.0  # Maximum delay between retries
    
    async def save_hashes_incremental(self, file_id: str, file_type: str, hashes: List[Hash], db: Session = None, tab_name: str = None) -> bool:
        """
        Save hashes incrementally - only update what has changed.
        Uses in-memory process lock for write serialization.
        
        Args:
            file_id: File identifier
            file_type: Type of file ('sheet', 'doc', 'pdf')
            hashes: List of Hash objects to save
            db: Optional database session
            
        Returns:
            True if successful, False otherwise
        """
        operation_start = time.time()
        
        try:
            # Use serialized write context with in-memory lock
            with get_db_write_context() as write_db:
                logger.debug(f"🔒 Acquired in-memory write lock for incremental hash save: {file_id}")
                
                # Step 1: Get existing hashes (tab-specific if provided)
                existing_hashes = {}
                query = write_db.query(FileHash).filter(FileHash.file_id == file_id)
                
                # Add tab filter if provided
                if tab_name:
                    query = query.filter(FileHash.tab_name == tab_name)
                    logger.debug(f"🔍 Filtering hashes for tab: {tab_name}")
                else:
                    query = query.filter(FileHash.tab_name.is_(None))
                
                existing_records = query.all()
                
                for record in existing_records:
                    # Ensure we have valid data before creating key
                    if record.hash_type and record.content_index is not None:
                        # Include tab in key for uniqueness
                        key = f"{record.hash_type}_{record.content_index}_{record.tab_name or 'default'}"
                        existing_hashes[key] = record
                    else:
                        logger.warning(f"⚠️  Skipping invalid hash record: ID={record.id}, type={record.hash_type}, index={record.content_index}")
                
                tab_info = f" (tab: {tab_name})" if tab_name else ""
                logger.debug(f"📊 Found {len(existing_hashes)} existing hashes for file {file_id}{tab_info}")
                
                # Step 2: Compare and update only changed hashes
                new_count = 0
                updated_count = 0
                unchanged_count = 0
                
                for hash_obj in hashes:
                    # Include tab in key for uniqueness
                    key = f"{hash_obj.hash_type}_{hash_obj.content_index}_{tab_name or 'default'}"
                    
                    if key in existing_hashes:
                        # Check if hash value has changed
                        existing_record = existing_hashes[key]
                        if existing_record.hash_value != hash_obj.hash_value:
                            # Update existing record
                            existing_record.hash_value = hash_obj.hash_value
                            existing_record.content_metadata = hash_obj.metadata
                            existing_record.updated_at = datetime.utcnow()
                            updated_count += 1
                        else:
                            unchanged_count += 1
                    else:
                        # Add new hash with tab information
                        file_hash = FileHash(
                            file_id=file_id,
                            file_type=file_type,
                            tab_name=tab_name,
                            hash_type=hash_obj.hash_type,
                            hash_value=hash_obj.hash_value,
                            content_index=hash_obj.content_index,
                            content_metadata=hash_obj.metadata
                        )
                        write_db.add(file_hash)
                        new_count += 1
                
                # Transaction is automatically committed by context manager
                logger.debug(f"✅ Committed incremental hash transaction for file {file_id}")
                
                # Log detailed results
                total_processed = new_count + updated_count + unchanged_count
                tab_info = f" (tab: {tab_name})" if tab_name else ""
                logger.info(f"📈 Incremental update for {file_id}{tab_info}: {new_count} new, {updated_count} updated, {unchanged_count} unchanged (total: {total_processed})")
                
                # Log percentage breakdown for clarity
                if total_processed > 0:
                    unchanged_pct = (unchanged_count / total_processed) * 100
                    new_pct = (new_count / total_processed) * 100
                    updated_pct = (updated_count / total_processed) * 100
                    logger.info(f"📊 Breakdown: {unchanged_pct:.1f}% unchanged, {new_pct:.1f}% new, {updated_pct:.1f}% updated")
            
            # Log successful operation (separate read-only session)
            execution_time = int((time.time() - operation_start) * 1000)
            
            try:
                with get_db_context() as log_db:
                    await self._log_operation(file_id, 'incremental_store', 'success', None, execution_time, log_db)
            except Exception as log_error:
                logger.warning(f"Failed to log operation: {str(log_error)}")
            
            logger.info(f"Successfully processed {len(hashes)} hashes incrementally for file {file_id} in {execution_time}ms")
            return True
            
        except Exception as e:
            execution_time = int((time.time() - operation_start) * 1000)
            
            try:
                with get_db_context() as error_log_db:
                    await self._log_operation(file_id, 'incremental_store', 'error', str(e), execution_time, error_log_db)
            except Exception:
                logger.warning(f"Failed to log error operation for {file_id}")
            
            logger.error(f"❌ Error in incremental hash save for file {file_id}: {str(e)}")
            return False

    async def _save_hashes_direct(self, file_id: str, file_type: str, hashes: List[Hash]) -> bool:
        """
        Direct hash save operation (called by queue worker).
        """
        operation_start = time.time()
        
        try:
            # Use a fresh database context for this operation
            with get_db_context() as fresh_db:
                # Get existing hashes
                existing_hashes = {}
                existing_records = fresh_db.query(FileHash).filter(
                    FileHash.file_id == file_id
                ).all()
                
                for record in existing_records:
                    key = f"{record.hash_type}_{record.content_index}"
                    existing_hashes[key] = record
                
                # Compare and update only changed hashes
                new_count = 0
                updated_count = 0
                unchanged_count = 0
                
                for hash_obj in hashes:
                    key = f"{hash_obj.hash_type}_{hash_obj.content_index}"
                    
                    if key in existing_hashes:
                        # Check if hash value has changed
                        existing_record = existing_hashes[key]
                        if existing_record.hash_value != hash_obj.hash_value:
                            # Update existing record
                            existing_record.hash_value = hash_obj.hash_value
                            existing_record.content_metadata = hash_obj.metadata
                            existing_record.updated_at = datetime.utcnow()
                            updated_count += 1
                        else:
                            unchanged_count += 1
                    else:
                        # Add new hash
                        file_hash = FileHash(
                            file_id=file_id,
                            file_type=file_type,
                            hash_type=hash_obj.hash_type,
                            hash_value=hash_obj.hash_value,
                            content_index=hash_obj.content_index,
                            content_metadata=hash_obj.metadata
                        )
                        fresh_db.add(file_hash)
                        new_count += 1
                
                # Commit the transaction
                fresh_db.commit()
                
                execution_time = int((time.time() - operation_start) * 1000)
                
                # Only log if there were actual changes
                if new_count > 0 or updated_count > 0:
                    logger.info(f"📈 Hash update for {file_id}: {new_count} new, {updated_count} updated, {unchanged_count} unchanged ({execution_time}ms)")
                else:
                    logger.debug(f"📊 No changes for {file_id}: {unchanged_count} unchanged ({execution_time}ms)")
                
                return True
                
        except Exception as e:
            logger.error(f"❌ Error in direct hash save for file {file_id}: {str(e)}")
            return False

    async def save_hashes(self, file_id: str, file_type: str, hashes: List[Hash], db: Session = None) -> bool:
        """
        Save hashes to database using incremental updates to prevent repeated processing.
        
        Args:
            file_id: File identifier
            file_type: Type of file ('sheet', 'doc', 'pdf')
            hashes: List of Hash objects to save
            db: Optional database session
            
        Returns:
            True if successful, False otherwise
        """
        # Use incremental updates by default to prevent repeated processing
        return await self.save_hashes_incremental(file_id, file_type, hashes, db)
            

    
    async def save_hashes_with_retry(self, file_id: str, file_type: str, hashes: List[Hash], db: Session = None) -> bool:
        """
        Save hashes with write lock protection (retry logic now handled by write locks).
        
        Args:
            file_id: File identifier
            file_type: Type of file ('sheet', 'doc', 'pdf')
            hashes: List of Hash objects to save
            db: Optional database session
            
        Returns:
            True if successful, False otherwise
        """
        # Write locks now handle the retry logic, so we can directly call save_hashes
        return await self.save_hashes(file_id, file_type, hashes, db)
    
    async def load_hashes(self, file_id: str, db: Session = None, tab_name: str = None) -> List[Hash]:
        """
        Load hashes from database with improved connection handling and tab support.
        
        Args:
            file_id: File identifier
            db: Optional database session
            tab_name: Optional tab name for sheet-specific hashing
            
        Returns:
            List of Hash objects
        """
        import asyncio
        
        # Retry logic for connection pool issues
        max_retries = 3
        base_delay = 0.5
        
        for attempt in range(max_retries):
            try:
                # Use provided session if available, otherwise create new one
                if db:
                    query = db.query(FileHash).filter(FileHash.file_id == file_id)
                    if tab_name:
                        query = query.filter(FileHash.tab_name == tab_name)
                    else:
                        query = query.filter(FileHash.tab_name.is_(None))
                    file_hashes = query.order_by(FileHash.content_index).all()
                else:
                    # Use fresh database session with timeout handling
                    with get_db_context() as fresh_db:
                        query = fresh_db.query(FileHash).filter(FileHash.file_id == file_id)
                        if tab_name:
                            query = query.filter(FileHash.tab_name == tab_name)
                        else:
                            query = query.filter(FileHash.tab_name.is_(None))
                        file_hashes = query.order_by(FileHash.content_index).all()
                
                hashes = []
                for file_hash in file_hashes:
                    hash_obj = Hash(
                        hash_value=file_hash.hash_value,
                        hash_type=file_hash.hash_type,
                        content_index=file_hash.content_index,
                        metadata=file_hash.content_metadata
                    )
                    hashes.append(hash_obj)
                
                tab_info = f" (tab: {tab_name})" if tab_name else ""
                logger.debug(f"Loaded {len(hashes)} hashes for file {file_id}{tab_info}")
                return hashes
                
            except Exception as e:
                error_msg = str(e).lower()
                if ("queuepool" in error_msg or "connection timed out" in error_msg or "timeout" in error_msg) and attempt < max_retries - 1:
                    # Connection pool issue - wait and retry
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Connection pool issue loading hashes for {file_id}, retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error(f"Error loading hashes for file {file_id}: {str(e)}")
                    return []
        
        # All retries exhausted
        logger.error(f"Failed to load hashes for {file_id} after {max_retries} attempts")
        return []
    
    async def delete_hashes(self, file_id: str, db: Session = None) -> bool:
        """
        Delete all hashes for a file with SQLite file lock.
        
        Args:
            file_id: File identifier
            db: Optional database session
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Use SQLite-specific file lock to prevent database locking issues
            with sqlite_write_lock(f"hash_storage_delete_{file_id}"):
                logger.debug(f"🔒 Acquired SQLite write lock for deleting hashes: {file_id}")
                
                # Use a fresh database context for this operation
                with get_db_context() as fresh_db:
                    # Delete all hashes for this file
                    deleted_count = fresh_db.query(FileHash).filter(
                        FileHash.file_id == file_id
                    ).delete()
                    
                    # Commit the deletion
                    fresh_db.commit()
                    logger.debug(f"✅ Committed hash deletion for file {file_id}")
            
            logger.info(f"Successfully deleted {deleted_count} hashes for file {file_id}")
            return True
            
        except OperationalError as e:
            # Handle database lock errors specifically
            logger.error(f"❌ Database lock error deleting hashes for file {file_id}: {str(e)}")
            return False
            
        except Exception as e:
            # Handle other errors
            logger.error(f"❌ Error deleting hashes for file {file_id}: {str(e)}")
            return False
    
    async def cleanup_orphaned_hashes(self, db: Session = None) -> int:
        """
        Clean up orphaned hashes that no longer have corresponding files.
        
        Args:
            db: Optional database session
            
        Returns:
            Number of orphaned hashes cleaned up
        """
        try:
            with get_db_context() as fresh_db:
                # This is a placeholder - in a real implementation, you would
                # check against the actual file metadata tables to find orphans
                # For now, we'll just clean up very old hash computation logs
                
                from datetime import timedelta
                cutoff_date = datetime.utcnow() - timedelta(days=30)
                
                deleted_logs = fresh_db.query(HashComputationLog).filter(
                    HashComputationLog.created_at < cutoff_date
                ).delete()
                
                fresh_db.commit()
                
                logger.info(f"Cleaned up {deleted_logs} old hash computation logs")
                return deleted_logs
                
        except Exception as e:
            logger.error(f"Error cleaning up orphaned hashes: {str(e)}")
            return 0
    
    async def get_hash_statistics(self, db: Session = None) -> Dict[str, Any]:
        """
        Get statistics about stored hashes.
        
        Args:
            db: Optional database session
            
        Returns:
            Dictionary with hash statistics
        """
        try:
            with get_db_context() as fresh_db:
                # Count hashes by type
                from sqlalchemy import func
                from datetime import timedelta
                
                hash_counts = fresh_db.query(
                    FileHash.file_type,
                    FileHash.hash_type,
                    func.count(FileHash.id).label('count')
                ).group_by(FileHash.file_type, FileHash.hash_type).all()
                
                # Count total files with hashes
                total_files = fresh_db.query(func.count(func.distinct(FileHash.file_id))).scalar()
                
                # Count recent operations
                recent_cutoff = datetime.utcnow() - timedelta(hours=24)
                recent_operations = fresh_db.query(func.count(HashComputationLog.id)).filter(
                    HashComputationLog.created_at >= recent_cutoff
                ).scalar()
                
                stats = {
                    'total_files_with_hashes': total_files or 0,
                    'recent_operations_24h': recent_operations or 0,
                    'hash_counts_by_type': {}
                }
                
                for file_type, hash_type, count in hash_counts:
                    if file_type not in stats['hash_counts_by_type']:
                        stats['hash_counts_by_type'][file_type] = {}
                    stats['hash_counts_by_type'][file_type][hash_type] = count
                
                return stats
                
        except Exception as e:
            logger.error(f"Error getting hash statistics: {str(e)}")
            return {}
    
    async def get_file_hash_summary(self, file_id: str) -> Dict[str, Any]:
        """
        Get a summary of hash data for a file to enable efficient querying.
        This method ensures the system can work with existing data in the database.
        
        Args:
            file_id: File identifier
            
        Returns:
            Dictionary with hash summary information
        """
        try:
            with get_db_context() as db:
                # Get basic hash statistics
                hash_count = db.query(FileHash).filter(FileHash.file_id == file_id).count()
                
                if hash_count == 0:
                    return {
                        "file_id": file_id,
                        "has_data": False,
                        "hash_count": 0,
                        "hash_types": [],
                        "content_range": None,
                        "last_updated": None
                    }
                
                # Get hash types and content range
                hash_info = db.query(
                    FileHash.hash_type,
                    FileHash.content_index,
                    FileHash.created_at
                ).filter(FileHash.file_id == file_id).all()
                
                hash_types = list(set(info.hash_type for info in hash_info))
                content_indices = [info.content_index for info in hash_info if info.content_index is not None]
                created_times = [info.created_at for info in hash_info if info.created_at is not None]
                
                return {
                    "file_id": file_id,
                    "has_data": True,
                    "hash_count": hash_count,
                    "hash_types": hash_types,
                    "content_range": {
                        "min_index": min(content_indices) if content_indices else None,
                        "max_index": max(content_indices) if content_indices else None
                    },
                    "last_updated": max(created_times) if created_times else None,
                    "can_answer_queries": True
                }
                
        except Exception as e:
            logger.error(f"Error getting hash summary for file {file_id}: {str(e)}")
            return {
                "file_id": file_id,
                "has_data": False,
                "error": str(e),
                "can_answer_queries": False
            }
    
    async def check_data_availability_for_queries(self, file_id: str) -> bool:
        """
        Check if the system has sufficient data to answer queries about a file.
        
        Args:
            file_id: File identifier
            
        Returns:
            True if data is available for queries, False otherwise
        """
        try:
            summary = await self.get_file_hash_summary(file_id)
            return summary.get("has_data", False) and summary.get("hash_count", 0) > 0
        except Exception as e:
            logger.error(f"Error checking data availability for {file_id}: {str(e)}")
            return False 
    
    async def _log_operation(self, file_id: str, operation: str, status: str, 
                           error_message: str = None, execution_time_ms: int = None, 
                           db: Session = None):
        """
        Log hash operation for debugging and monitoring.
        
        Args:
            file_id: File identifier
            operation: Operation type ('compute', 'compare', 'store')
            status: Operation status ('success', 'error')
            error_message: Error message if status is 'error'
            execution_time_ms: Execution time in milliseconds
            db: Database session
        """
        try:
            log_entry = HashComputationLog(
                file_id=file_id,
                operation=operation,
                status=status,
                error_message=error_message,
                execution_time_ms=execution_time_ms
            )
            db.add(log_entry)
            # Note: Don't commit here, let the caller handle commits
            
        except Exception as e:
            logger.warning(f"Failed to log hash operation: {str(e)}")
            # Don't raise - logging failures shouldn't break the main operation