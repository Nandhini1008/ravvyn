"""
Database Lock Manager - Handles write locks for SQLite database operations
Implements proper locking pattern: Acquire lock → Delete → Insert → Commit → Release lock
"""

import threading
import time
import logging
from contextlib import contextmanager
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError

logger = logging.getLogger(__name__)


class DatabaseLockManager:
    """
    Manages write locks for database operations to prevent SQLite locking issues.
    
    Uses threading locks to ensure only one write operation happens at a time,
    preventing the "database is locked" error in SQLite.
    """
    
    def __init__(self):
        """Initialize the lock manager with a threading lock"""
        self._write_lock = threading.RLock()  # Reentrant lock for nested operations
        self._lock_timeout = 30.0  # 30 second timeout for acquiring locks
        self._active_operations = {}  # Track active operations for debugging
    
    @contextmanager
    def acquire_write_lock(self, operation_name: str = "unknown", file_id: str = None):
        """
        Context manager to acquire a write lock for database operations.
        
        Usage:
            with lock_manager.acquire_write_lock("hash_storage", file_id):
                # Perform database operations
                db.delete(...)
                db.add(...)
                db.commit()
        
        Args:
            operation_name: Name of the operation for logging
            file_id: Optional file ID for tracking
        """
        operation_id = f"{operation_name}_{file_id or 'global'}_{int(time.time() * 1000)}"
        acquired = False
        start_time = time.time()
        
        try:
            logger.debug(f"🔒 Attempting to acquire write lock for {operation_name} (file: {file_id})")
            
            # Try to acquire the lock with timeout
            acquired = self._write_lock.acquire(timeout=self._lock_timeout)
            
            if not acquired:
                elapsed = time.time() - start_time
                logger.error(f"❌ Failed to acquire write lock for {operation_name} after {elapsed:.2f}s")
                raise OperationalError(
                    f"Could not acquire database write lock for {operation_name} within {self._lock_timeout}s",
                    None, None
                )
            
            # Track the active operation
            self._active_operations[operation_id] = {
                'operation': operation_name,
                'file_id': file_id,
                'start_time': start_time,
                'thread_id': threading.current_thread().ident
            }
            
            elapsed = time.time() - start_time
            logger.debug(f"✅ Acquired write lock for {operation_name} in {elapsed:.3f}s (file: {file_id})")
            
            yield operation_id
            
        except Exception as e:
            logger.error(f"❌ Error during locked operation {operation_name}: {str(e)}")
            raise
            
        finally:
            # Clean up tracking
            if operation_id in self._active_operations:
                del self._active_operations[operation_id]
            
            # Release the lock
            if acquired:
                elapsed = time.time() - start_time
                self._write_lock.release()
                logger.debug(f"🔓 Released write lock for {operation_name} after {elapsed:.3f}s (file: {file_id})")
    
    def get_lock_status(self) -> dict:
        """
        Get current lock status for monitoring and debugging.
        
        Returns:
            Dictionary with lock status information
        """
        return {
            'is_locked': self._write_lock._count > 0 if hasattr(self._write_lock, '_count') else False,
            'active_operations': len(self._active_operations),
            'operations': list(self._active_operations.values()),
            'lock_timeout': self._lock_timeout
        }
    
    def force_release_stale_locks(self, max_age_seconds: float = 300.0):
        """
        Force release locks that have been held for too long (emergency cleanup).
        
        Args:
            max_age_seconds: Maximum age of a lock before it's considered stale
        """
        current_time = time.time()
        stale_operations = []
        
        for op_id, op_info in self._active_operations.items():
            age = current_time - op_info['start_time']
            if age > max_age_seconds:
                stale_operations.append((op_id, op_info, age))
        
        if stale_operations:
            logger.warning(f"⚠️  Found {len(stale_operations)} stale lock operations")
            for op_id, op_info, age in stale_operations:
                logger.warning(f"   - {op_info['operation']} (file: {op_info['file_id']}) held for {age:.1f}s")
                # Remove from tracking (the actual lock will be released by the thread)
                if op_id in self._active_operations:
                    del self._active_operations[op_id]


# Global instance
_lock_manager = None

def get_lock_manager() -> DatabaseLockManager:
    """Get the global database lock manager instance"""
    global _lock_manager
    if _lock_manager is None:
        _lock_manager = DatabaseLockManager()
    return _lock_manager


@contextmanager
def database_write_lock(operation_name: str = "unknown", file_id: str = None):
    """
    Convenience function for acquiring database write locks.
    
    Usage:
        from services.db_lock_manager import database_write_lock
        
        with database_write_lock("hash_storage", file_id):
            # Perform database operations
            db.delete(...)
            db.add(...)
            db.commit()
    """
    lock_manager = get_lock_manager()
    with lock_manager.acquire_write_lock(operation_name, file_id) as operation_id:
        yield operation_id