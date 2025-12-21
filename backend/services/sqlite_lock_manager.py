"""
SQLite Lock Manager - File-based locking for SQLite database operations
Implements proper file-based locking to prevent SQLite database lock errors
"""

import os
import time
import logging
from contextlib import contextmanager
from typing import Optional
import tempfile
import platform

# Platform-specific imports
if platform.system() == "Windows":
    import msvcrt
else:
    import fcntl

logger = logging.getLogger(__name__)


class SQLiteLockManager:
    """
    File-based lock manager specifically for SQLite database operations.
    Uses file locking to ensure only one process writes to SQLite at a time.
    """
    
    def __init__(self, lock_dir: Optional[str] = None):
        """Initialize the SQLite lock manager"""
        self.lock_dir = lock_dir or tempfile.gettempdir()
        self.lock_file_path = os.path.join(self.lock_dir, "ravvyn_sqlite.lock")
        self.lock_timeout = 60.0  # 60 second timeout
        
        # Ensure lock directory exists
        os.makedirs(self.lock_dir, exist_ok=True)
    
    def _cleanup_stale_lock(self):
        """Clean up stale lock files from dead processes"""
        try:
            if os.path.exists(self.lock_file_path):
                lock_info = self.get_lock_info()
                if lock_info.get('pid') and lock_info.get('age_seconds', 0) > 300:  # 5 minutes old
                    # Check if the process is still running
                    try:
                        if platform.system() == "Windows":
                            import subprocess
                            result = subprocess.run(['tasklist', '/FI', f'PID eq {lock_info["pid"]}'], 
                                                  capture_output=True, text=True)
                            if "No tasks are running" in result.stdout:
                                # Process is dead, remove stale lock
                                os.remove(self.lock_file_path)
                                logger.info(f"🧹 Cleaned up stale lock file from dead process {lock_info['pid']}")
                        else:
                            # Unix: check if process exists
                            os.kill(int(lock_info['pid']), 0)
                    except (subprocess.SubprocessError, ProcessLookupError, OSError, ValueError):
                        # Process doesn't exist, remove stale lock
                        try:
                            os.remove(self.lock_file_path)
                            logger.info(f"🧹 Cleaned up stale lock file from dead process {lock_info.get('pid', 'unknown')}")
                        except OSError:
                            pass  # File might have been removed by another process
        except Exception as e:
            logger.debug(f"Error cleaning up stale lock: {str(e)}")

    @contextmanager
    def acquire_sqlite_write_lock(self, operation_name: str = "unknown"):
        """
        Context manager to acquire an exclusive file lock for SQLite writes.
        
        This prevents multiple processes from writing to SQLite simultaneously,
        which is the root cause of "database is locked" errors.
        
        Args:
            operation_name: Name of the operation for logging
        """
        lock_file = None
        acquired = False
        start_time = time.time()
        
        try:
            # Clean up any stale locks first
            self._cleanup_stale_lock()
            
            logger.debug(f"🔒 Acquiring SQLite write lock for {operation_name}")
            
            # Try to acquire exclusive lock with timeout
            timeout_end = time.time() + self.lock_timeout
            
            while time.time() < timeout_end:
                try:
                    # Open lock file for writing
                    lock_file = open(self.lock_file_path, 'w')
                    
                    if platform.system() == "Windows":
                        # Windows file locking
                        msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                    else:
                        # Unix file locking
                        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    
                    acquired = True
                    break
                    
                except (IOError, OSError) as e:
                    # Lock is held by another process or file permission issue
                    if lock_file:
                        try:
                            lock_file.close()
                        except:
                            pass
                        lock_file = None
                    
                    # If it's a permission error, try to clean up and retry once
                    if "Permission denied" in str(e) or "Access is denied" in str(e):
                        try:
                            if os.path.exists(self.lock_file_path):
                                os.remove(self.lock_file_path)
                                logger.debug(f"🧹 Removed lock file due to permission error, retrying...")
                                continue
                        except:
                            pass
                    
                    # Wait a bit before retrying
                    time.sleep(0.1)
            
            if not acquired:
                elapsed = time.time() - start_time
                raise TimeoutError(f"Could not acquire SQLite write lock for {operation_name} within {self.lock_timeout}s")
            
            elapsed = time.time() - start_time
            logger.debug(f"✅ Acquired SQLite write lock for {operation_name} in {elapsed:.3f}s")
            
            # Write process info to lock file for debugging
            lock_file.write(f"{operation_name}:{os.getpid()}:{time.time()}\n")
            lock_file.flush()
            
            yield
            
        except Exception as e:
            logger.error(f"❌ Error during SQLite locked operation {operation_name}: {str(e)}")
            raise
            
        finally:
            # Release the lock
            if acquired and lock_file:
                try:
                    if platform.system() == "Windows":
                        # Windows file unlocking
                        msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        # Unix file unlocking
                        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                    elapsed = time.time() - start_time
                    logger.debug(f"🔓 Released SQLite write lock for {operation_name} after {elapsed:.3f}s")
                except Exception as e:
                    logger.warning(f"Error releasing SQLite lock: {str(e)}")
            
            # Close and clean up lock file
            if lock_file:
                try:
                    lock_file.close()
                except Exception:
                    pass
            
            # Remove lock file after successful operation
            if acquired:
                try:
                    # Small delay to ensure file handle is fully released
                    time.sleep(0.01)
                    if os.path.exists(self.lock_file_path):
                        os.remove(self.lock_file_path)
                except Exception as e:
                    # On Windows, sometimes the file is still locked briefly
                    # Try once more after a short delay
                    try:
                        time.sleep(0.1)
                        if os.path.exists(self.lock_file_path):
                            os.remove(self.lock_file_path)
                    except Exception:
                        logger.debug(f"Could not remove lock file: {str(e)}")  # Don't log as warning since it's not critical
    
    def is_locked(self) -> bool:
        """Check if the SQLite write lock is currently held"""
        try:
            with open(self.lock_file_path, 'w') as test_file:
                if platform.system() == "Windows":
                    # Windows file locking test
                    msvcrt.locking(test_file.fileno(), msvcrt.LK_NBLCK, 1)
                    msvcrt.locking(test_file.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    # Unix file locking test
                    fcntl.flock(test_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    fcntl.flock(test_file.fileno(), fcntl.LOCK_UN)
                return False
        except (IOError, OSError, FileNotFoundError):
            return True
    
    def get_lock_info(self) -> dict:
        """Get information about the current lock"""
        try:
            if os.path.exists(self.lock_file_path):
                with open(self.lock_file_path, 'r') as f:
                    content = f.read().strip()
                    if content:
                        parts = content.split(':')
                        if len(parts) >= 3:
                            return {
                                'is_locked': self.is_locked(),
                                'operation': parts[0],
                                'pid': parts[1],
                                'timestamp': float(parts[2]),
                                'age_seconds': time.time() - float(parts[2])
                            }
            
            return {
                'is_locked': self.is_locked(),
                'operation': None,
                'pid': None,
                'timestamp': None,
                'age_seconds': None
            }
        except Exception as e:
            logger.warning(f"Error getting lock info: {str(e)}")
            return {'is_locked': False, 'error': str(e)}


# Global instance
_sqlite_lock_manager = None

def get_sqlite_lock_manager() -> SQLiteLockManager:
    """Get the global SQLite lock manager instance"""
    global _sqlite_lock_manager
    if _sqlite_lock_manager is None:
        _sqlite_lock_manager = SQLiteLockManager()
    return _sqlite_lock_manager


@contextmanager
def sqlite_write_lock(operation_name: str = "unknown"):
    """
    Convenience function for acquiring SQLite write locks.
    
    Usage:
        from services.sqlite_lock_manager import sqlite_write_lock
        
        with sqlite_write_lock("hash_storage"):
            # Perform SQLite operations
            db.delete(...)
            db.add(...)
            db.commit()
    """
    lock_manager = get_sqlite_lock_manager()
    with lock_manager.acquire_sqlite_write_lock(operation_name):
        yield