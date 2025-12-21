"""
Content Processor Service - Batch processing and workflow management
Handles large-scale content processing with progress tracking and status reporting
"""

import logging
import asyncio
import time
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
from enum import Enum
from sqlalchemy.orm import Session

from services.hash_service import HashService
from services.content_retrieval import ContentRetrievalService
from services.database import get_db_context

logger = logging.getLogger(__name__)


class ProcessingStatus(Enum):
    """Processing status enumeration"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ProcessingJob:
    """Represents a content processing job"""
    job_id: str
    file_id: str
    file_type: str
    operation: str  # 'hash', 'detect_changes', 'cleanup'
    status: ProcessingStatus
    created_at: float
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    progress: float = 0.0
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class BatchProcessingResult:
    """Result of batch processing operation"""
    total_jobs: int
    completed_jobs: int
    failed_jobs: int
    cancelled_jobs: int
    total_time_seconds: float
    results: List[Dict[str, Any]]
    errors: List[Dict[str, Any]]


class ContentProcessor:
    """
    Content processing service for batch operations and workflows.
    Handles large-scale processing with progress tracking and status reporting.
    """
    
    def __init__(self):
        """Initialize content processor"""
        self.hash_service = HashService()
        self.content_retrieval = ContentRetrievalService()
        
        # Job management
        self.active_jobs: Dict[str, ProcessingJob] = {}
        self.job_counter = 0
        
        # Load configuration
        try:
            from core.config import get_settings
            settings = get_settings()
            
            self.max_concurrent_jobs = settings.processing_max_concurrent_jobs
            self.job_timeout_seconds = settings.processing_job_timeout_seconds
            self.batch_size = settings.processing_batch_size
            self.cleanup_interval = settings.processing_cleanup_interval_seconds
        except Exception as e:
            logger.warning(f"Could not load settings, using defaults: {str(e)}")
            self.max_concurrent_jobs = 5
            self.job_timeout_seconds = 300  # 5 minutes
            self.batch_size = 100
            self.cleanup_interval = 300
        
        logger.info("Content processor initialized")
    
    async def process_new_content(self, file_id: str, file_type: str, content: Any, **kwargs) -> Dict[str, Any]:
        """
        Process new content: compute hashes and store them.
        
        Args:
            file_id: File identifier
            file_type: Type of file ('sheet', 'doc', 'pdf')
            content: File content
            **kwargs: Additional processing options
            
        Returns:
            Dictionary with processing results
        """
        try:
            job_id = self._generate_job_id()
            job = ProcessingJob(
                job_id=job_id,
                file_id=file_id,
                file_type=file_type,
                operation='new_content',
                status=ProcessingStatus.PENDING,
                created_at=time.time(),
                metadata=kwargs
            )
            
            self.active_jobs[job_id] = job
            
            try:
                job.status = ProcessingStatus.RUNNING
                job.started_at = time.time()
                job.progress = 0.1
                
                # Compute hashes for new content
                hash_result = await self.hash_service.compute_file_hash(file_id, file_type, content)
                job.progress = 0.5
                
                if not hash_result.success:
                    raise Exception(f"Hash computation failed: {hash_result.error_message}")
                
                # Store hashes
                stored = await self.hash_service.store_hashes(file_id, file_type, hash_result.hashes)
                job.progress = 0.9
                
                if not stored:
                    raise Exception("Failed to store hashes")
                
                # Complete job
                job.status = ProcessingStatus.COMPLETED
                job.completed_at = time.time()
                job.progress = 1.0
                job.result = {
                    'hash_count': len(hash_result.hashes),
                    'computation_time_ms': hash_result.computation_time_ms,
                    'total_size': hash_result.total_size,
                    'stored': stored
                }
                
                logger.info(f"Successfully processed new content for {file_type} {file_id}")
                
                return {
                    'success': True,
                    'job_id': job_id,
                    'result': job.result
                }
                
            except Exception as e:
                job.status = ProcessingStatus.FAILED
                job.completed_at = time.time()
                job.error = str(e)
                raise
                
        except Exception as e:
            logger.error(f"Error processing new content for {file_type} {file_id}: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'job_id': job_id if 'job_id' in locals() else None
            }
        finally:
            # Clean up job after some time
            if 'job_id' in locals():
                asyncio.create_task(self._cleanup_job_after_delay(job_id, 300))  # 5 minutes
    
    async def process_content_deletion(self, file_id: str) -> Dict[str, Any]:
        """
        Process content deletion: remove associated hashes.
        
        Args:
            file_id: File identifier
            
        Returns:
            Dictionary with deletion results
        """
        try:
            job_id = self._generate_job_id()
            job = ProcessingJob(
                job_id=job_id,
                file_id=file_id,
                file_type='unknown',
                operation='deletion',
                status=ProcessingStatus.PENDING,
                created_at=time.time()
            )
            
            self.active_jobs[job_id] = job
            
            try:
                job.status = ProcessingStatus.RUNNING
                job.started_at = time.time()
                job.progress = 0.1
                
                # Delete hashes
                deleted = await self.hash_service.hash_storage.delete_hashes(file_id)
                job.progress = 0.9
                
                # Complete job
                job.status = ProcessingStatus.COMPLETED
                job.completed_at = time.time()
                job.progress = 1.0
                job.result = {
                    'deleted': deleted
                }
                
                logger.info(f"Successfully processed deletion for file {file_id}")
                
                return {
                    'success': True,
                    'job_id': job_id,
                    'result': job.result
                }
                
            except Exception as e:
                job.status = ProcessingStatus.FAILED
                job.completed_at = time.time()
                job.error = str(e)
                raise
                
        except Exception as e:
            logger.error(f"Error processing deletion for file {file_id}: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'job_id': job_id if 'job_id' in locals() else None
            }
        finally:
            # Clean up job after some time
            if 'job_id' in locals():
                asyncio.create_task(self._cleanup_job_after_delay(job_id, 300))
    
    async def batch_process_files(self, file_list: List[Dict[str, Any]], 
                                operation: str = 'hash_and_detect',
                                progress_callback: Optional[Callable] = None) -> BatchProcessingResult:
        """
        Process multiple files in batches.
        
        Args:
            file_list: List of file dictionaries with 'file_id', 'file_type', etc.
            operation: Operation to perform ('hash_and_detect', 'hash_only', 'detect_only')
            progress_callback: Optional callback for progress updates
            
        Returns:
            BatchProcessingResult with batch processing results
        """
        start_time = time.time()
        total_files = len(file_list)
        completed = 0
        failed = 0
        cancelled = 0
        results = []
        errors = []
        
        try:
            logger.info(f"Starting batch processing of {total_files} files with operation '{operation}'")
            
            # Process files in batches
            for i in range(0, total_files, self.batch_size):
                batch = file_list[i:i + self.batch_size]
                batch_results = await self._process_file_batch(batch, operation)
                
                # Aggregate results
                for result in batch_results:
                    if result['success']:
                        completed += 1
                        results.append(result)
                    else:
                        failed += 1
                        errors.append(result)
                
                # Update progress
                current_progress = (i + len(batch)) / total_files
                if progress_callback:
                    try:
                        await progress_callback(current_progress, completed, failed)
                    except Exception as e:
                        logger.warning(f"Progress callback failed: {str(e)}")
                
                logger.info(f"Batch {i//self.batch_size + 1} completed: {len(batch)} files processed")
            
            total_time = time.time() - start_time
            
            result = BatchProcessingResult(
                total_jobs=total_files,
                completed_jobs=completed,
                failed_jobs=failed,
                cancelled_jobs=cancelled,
                total_time_seconds=total_time,
                results=results,
                errors=errors
            )
            
            logger.info(f"Batch processing completed: {completed} succeeded, {failed} failed in {total_time:.2f}s")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in batch processing: {str(e)}")
            total_time = time.time() - start_time
            
            return BatchProcessingResult(
                total_jobs=total_files,
                completed_jobs=completed,
                failed_jobs=failed,
                cancelled_jobs=cancelled,
                total_time_seconds=total_time,
                results=results,
                errors=errors + [{'error': str(e), 'batch_error': True}]
            )
    
    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get status of a processing job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Dictionary with job status or None if not found
        """
        job = self.active_jobs.get(job_id)
        if not job:
            return None
        
        return {
            'job_id': job.job_id,
            'file_id': job.file_id,
            'file_type': job.file_type,
            'operation': job.operation,
            'status': job.status.value,
            'progress': job.progress,
            'created_at': job.created_at,
            'started_at': job.started_at,
            'completed_at': job.completed_at,
            'result': job.result,
            'error': job.error,
            'metadata': job.metadata
        }
    
    async def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a processing job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            True if cancelled, False if not found or already completed
        """
        job = self.active_jobs.get(job_id)
        if not job:
            return False
        
        if job.status in [ProcessingStatus.COMPLETED, ProcessingStatus.FAILED, ProcessingStatus.CANCELLED]:
            return False
        
        job.status = ProcessingStatus.CANCELLED
        job.completed_at = time.time()
        
        logger.info(f"Job {job_id} cancelled")
        return True
    
    async def get_processing_statistics(self) -> Dict[str, Any]:
        """
        Get processing statistics.
        
        Returns:
            Dictionary with processing statistics
        """
        try:
            active_count = len([j for j in self.active_jobs.values() if j.status == ProcessingStatus.RUNNING])
            pending_count = len([j for j in self.active_jobs.values() if j.status == ProcessingStatus.PENDING])
            completed_count = len([j for j in self.active_jobs.values() if j.status == ProcessingStatus.COMPLETED])
            failed_count = len([j for j in self.active_jobs.values() if j.status == ProcessingStatus.FAILED])
            
            return {
                'service_status': 'active',
                'configuration': {
                    'max_concurrent_jobs': self.max_concurrent_jobs,
                    'job_timeout_seconds': self.job_timeout_seconds,
                    'batch_size': self.batch_size
                },
                'job_statistics': {
                    'total_jobs': len(self.active_jobs),
                    'active_jobs': active_count,
                    'pending_jobs': pending_count,
                    'completed_jobs': completed_count,
                    'failed_jobs': failed_count
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting processing statistics: {str(e)}")
            return {
                'service_status': 'error',
                'error': str(e)
            }
    
    async def _process_file_batch(self, batch: List[Dict[str, Any]], operation: str) -> List[Dict[str, Any]]:
        """
        Process a batch of files concurrently.
        
        Args:
            batch: List of file dictionaries
            operation: Operation to perform
            
        Returns:
            List of processing results
        """
        semaphore = asyncio.Semaphore(self.max_concurrent_jobs)
        
        async def process_single_file(file_info):
            async with semaphore:
                try:
                    file_id = file_info['file_id']
                    file_type = file_info['file_type']
                    
                    if operation == 'hash_and_detect':
                        result = await self.hash_service.compute_hash_from_source(file_id, file_type)
                    elif operation == 'hash_only':
                        # Retrieve content and compute hash
                        content_result = await self.content_retrieval.retrieve_content_by_type(
                            file_id, file_type
                        )
                        if content_result['success']:
                            hash_result = await self.hash_service.compute_file_hash(
                                file_id, file_type, content_result['content']
                            )
                            result = {
                                'success': hash_result.success,
                                'file_id': file_id,
                                'file_type': file_type,
                                'hash_count': len(hash_result.hashes) if hash_result.success else 0,
                                'error': hash_result.error_message if not hash_result.success else None
                            }
                        else:
                            result = {
                                'success': False,
                                'file_id': file_id,
                                'file_type': file_type,
                                'error': content_result.get('error', 'Content retrieval failed')
                            }
                    else:
                        result = {
                            'success': False,
                            'file_id': file_id,
                            'file_type': file_type,
                            'error': f'Unsupported operation: {operation}'
                        }
                    
                    return result
                    
                except Exception as e:
                    return {
                        'success': False,
                        'file_id': file_info.get('file_id', 'unknown'),
                        'file_type': file_info.get('file_type', 'unknown'),
                        'error': str(e)
                    }
        
        # Process all files in the batch concurrently
        tasks = [process_single_file(file_info) for file_info in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle any exceptions that occurred
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    'success': False,
                    'file_id': batch[i].get('file_id', 'unknown'),
                    'file_type': batch[i].get('file_type', 'unknown'),
                    'error': str(result)
                })
            else:
                processed_results.append(result)
        
        return processed_results
    
    def _generate_job_id(self) -> str:
        """Generate a unique job ID"""
        self.job_counter += 1
        return f"job_{int(time.time())}_{self.job_counter}"
    
    async def _cleanup_job_after_delay(self, job_id: str, delay_seconds: int):
        """Clean up job after specified delay"""
        await asyncio.sleep(delay_seconds)
        if job_id in self.active_jobs:
            del self.active_jobs[job_id]
            logger.debug(f"Cleaned up job {job_id}")