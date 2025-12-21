"""
Database Operation Queue - Serializes database operations to prevent SQLite locks
Implements a queue-based system for database writes to eliminate lock conflicts
"""

import asyncio
import logging
import time
from typing import Callable, Any, Dict, Optional
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class OperationType(Enum):
    """Types of database operations"""
    HASH_SAVE = "hash_save"
    HASH_DELETE = "hash_delete"
    METADATA_UPDATE = "metadata_update"
    LOG_OPERATION = "log_operation"


@dataclass
class DatabaseOperation:
    """Represents a database operation to be queued"""
    operation_type: OperationType
    operation_func: Callable
    args: tuple
    kwargs: dict
    priority: int = 0  # Higher priority operations go first
    created_at: float = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()


class DatabaseOperationQueue:
    """
    Queue-based database operation manager.
    Ensures only one database write operation happens at a time.
    """
    
    def __init__(self):
        """Initialize the database operation queue"""
        self.queue = asyncio.PriorityQueue()
        self.is_processing = False
        self.worker_task = None
        self.stats = {
            'total_operations': 0,
            'successful_operations': 0,
            'failed_operations': 0,
            'queue_size': 0,
            'average_wait_time': 0.0
        }
    
    async def start_worker(self):
        """Start the background worker that processes queued operations"""
        if self.worker_task is None or self.worker_task.done():
            self.worker_task = asyncio.create_task(self._process_queue())
            logger.info("🚀 Database operation queue worker started")
    
    async def stop_worker(self):
        """Stop the background worker"""
        if self.worker_task and not self.worker_task.done():
            self.worker_task.cancel()
            try:
                await self.worker_task
            except asyncio.CancelledError:
                pass
            logger.info("🛑 Database operation queue worker stopped")
    
    async def enqueue_operation(self, operation: DatabaseOperation) -> Any:
        """
        Enqueue a database operation for processing.
        
        Args:
            operation: DatabaseOperation to enqueue
            
        Returns:
            Result of the operation (when completed)
        """
        # Create a future to return the result
        result_future = asyncio.Future()
        
        # Add result future to the operation
        operation.result_future = result_future
        
        # Add to queue (priority queue uses negative priority for max-heap behavior)
        await self.queue.put((-operation.priority, operation.created_at, operation))
        
        self.stats['queue_size'] = self.queue.qsize()
        logger.debug(f"📥 Enqueued {operation.operation_type.value} operation (queue size: {self.queue.qsize()})")
        
        # Start worker if not running
        await self.start_worker()
        
        # Wait for result
        return await result_future
    
    async def _process_queue(self):
        """Background worker that processes queued operations"""
        logger.info("🔄 Database operation queue worker started processing")
        
        while True:
            try:
                # Get next operation from queue
                _, created_at, operation = await self.queue.get()
                
                self.stats['queue_size'] = self.queue.qsize()
                self.stats['total_operations'] += 1
                
                # Calculate wait time
                wait_time = time.time() - created_at
                self.stats['average_wait_time'] = (
                    (self.stats['average_wait_time'] * (self.stats['total_operations'] - 1) + wait_time) 
                    / self.stats['total_operations']
                )
                
                logger.debug(f"🔄 Processing {operation.operation_type.value} operation (waited {wait_time:.3f}s)")
                
                try:
                    # Execute the operation
                    result = await operation.operation_func(*operation.args, **operation.kwargs)
                    
                    # Set the result
                    if not operation.result_future.done():
                        operation.result_future.set_result(result)
                    
                    self.stats['successful_operations'] += 1
                    logger.debug(f"✅ Completed {operation.operation_type.value} operation")
                    
                except Exception as e:
                    # Set the exception
                    if not operation.result_future.done():
                        operation.result_future.set_exception(e)
                    
                    self.stats['failed_operations'] += 1
                    logger.error(f"❌ Failed {operation.operation_type.value} operation: {str(e)}")
                
                # Mark task as done
                self.queue.task_done()
                
            except asyncio.CancelledError:
                logger.info("🛑 Database operation queue worker cancelled")
                break
            except Exception as e:
                logger.error(f"❌ Error in database operation queue worker: {str(e)}")
                await asyncio.sleep(1)  # Brief pause before continuing
    
    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        return {
            **self.stats,
            'is_processing': self.is_processing,
            'worker_running': self.worker_task is not None and not self.worker_task.done(),
            'current_queue_size': self.queue.qsize()
        }
    
    async def wait_for_empty_queue(self, timeout: float = 30.0):
        """Wait for the queue to be empty"""
        start_time = time.time()
        
        while self.queue.qsize() > 0:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Queue did not empty within {timeout} seconds")
            await asyncio.sleep(0.1)


# Global instance
_db_operation_queue = None

def get_db_operation_queue() -> DatabaseOperationQueue:
    """Get the global database operation queue instance"""
    global _db_operation_queue
    if _db_operation_queue is None:
        _db_operation_queue = DatabaseOperationQueue()
    return _db_operation_queue


async def queue_db_operation(operation_type: OperationType, operation_func: Callable, 
                           *args, priority: int = 0, **kwargs) -> Any:
    """
    Convenience function to queue a database operation.
    
    Args:
        operation_type: Type of operation
        operation_func: Function to execute
        *args: Arguments for the function
        priority: Operation priority (higher = more important)
        **kwargs: Keyword arguments for the function
        
    Returns:
        Result of the operation
    """
    queue = get_db_operation_queue()
    operation = DatabaseOperation(
        operation_type=operation_type,
        operation_func=operation_func,
        args=args,
        kwargs=kwargs,
        priority=priority
    )
    return await queue.enqueue_operation(operation)