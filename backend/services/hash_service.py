"""
Hash Service - Main orchestrator for file hashing operations
Coordinates hashing operations across different file types with error handling
"""

import logging
import time
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
from sqlalchemy.orm import Session

from services.hash_computer import HashComputer, Hash
from services.hash_storage import HashStorage
from services.hash_validator import HashValidator, ChangeDetectionResult
from services.content_retrieval import ContentRetrievalService
from services.database import get_db_context

logger = logging.getLogger(__name__)


@dataclass
class HashResult:
    """Result of hash computation operation"""
    file_id: str
    file_type: str
    hashes: List[Hash]
    computation_time_ms: int
    total_size: int
    success: bool
    error_message: Optional[str] = None


class HashService:
    """
    Main hash service orchestrator.
    Coordinates hashing operations across different file types.
    """
    
    def __init__(self):
        """Initialize hash service with component services"""
        self.hash_computer = HashComputer()
        self.hash_storage = HashStorage()
        self.hash_validator = HashValidator()
        self.content_retrieval = ContentRetrievalService()
        
        # Initialize monitoring (avoid circular import)
        self.monitoring = None
        
        # Load configuration
        try:
            from core.config import get_settings
            settings = get_settings()
            
            self.enabled = settings.hash_enabled
            self.pdf_large_threshold = settings.hash_pdf_threshold_mb * 1024 * 1024
            self.max_content_size = settings.hash_max_content_size_mb * 1024 * 1024
        except Exception as e:
            logger.warning(f"Could not load settings, using defaults: {str(e)}")
            self.enabled = True
            self.pdf_large_threshold = 100 * 1024 * 1024  # 100MB threshold for PDFs
            self.max_content_size = 500 * 1024 * 1024  # 500MB max content size
        
        logger.info(f"Hash service initialized (enabled={self.enabled})")
    
    def set_monitoring(self, monitoring):
        """Set monitoring service (called after initialization to avoid circular imports)"""
        self.monitoring = monitoring
    
    async def compute_file_hash(self, file_id: str, file_type: str, content: Any) -> HashResult:
        """
        Compute hash for a file based on its type.
        
        Args:
            file_id: File identifier
            file_type: Type of file ('sheet', 'doc', 'pdf')
            content: File content (format depends on file_type)
            
        Returns:
            HashResult with computation results
        """
        start_time = time.time()
        
        try:
            # Validate input
            if not file_id or not file_type:
                raise ValueError("file_id and file_type are required")
            
            if file_type not in ['sheet', 'doc', 'pdf']:
                raise ValueError(f"Unsupported file type: {file_type}")
            
            # Route to appropriate hash computation method
            if file_type == 'sheet':
                hashes, total_size = await self._compute_sheet_hashes(content)
            elif file_type == 'doc':
                hashes, total_size = await self._compute_doc_hashes(content)
            elif file_type == 'pdf':
                hashes, total_size = await self._compute_pdf_hashes(content)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            
            # Validate computed hashes
            for hash_obj in hashes:
                if not self.hash_validator.validate_hash_object(hash_obj):
                    raise ValueError(f"Invalid hash computed: {hash_obj.hash_value}")
            
            computation_time = int((time.time() - start_time) * 1000)
            
            result = HashResult(
                file_id=file_id,
                file_type=file_type,
                hashes=hashes,
                computation_time_ms=computation_time,
                total_size=total_size,
                success=True
            )
            
            # Record monitoring metrics
            if self.monitoring:
                self.monitoring.record_hash_computation(
                    file_id, file_type, computation_time, len(hashes), True
                )
            
            logger.info(f"Successfully computed {len(hashes)} hashes for {file_type} file {file_id} "
                       f"in {computation_time}ms")
            
            return result
            
        except Exception as e:
            computation_time = int((time.time() - start_time) * 1000)
            error_msg = str(e)
            
            # Record monitoring metrics for error
            if self.monitoring:
                self.monitoring.record_hash_computation(
                    file_id, file_type, computation_time, 0, False, error_msg
                )
            
            logger.error(f"Error computing hash for {file_type} file {file_id}: {error_msg}")
            
            return HashResult(
                file_id=file_id,
                file_type=file_type,
                hashes=[],
                computation_time_ms=computation_time,
                total_size=0,
                success=False,
                error_message=error_msg
            )
    
    async def compare_hashes(self, file_id: str, new_hashes: List[Hash], tab_name: str = None) -> ChangeDetectionResult:
        """
        Compare new hashes with stored hashes to detect changes.
        
        Args:
            file_id: File identifier
            new_hashes: New hash set to compare
            
        Returns:
            ChangeDetectionResult with detected changes
        """
        try:
            # Load existing hashes from storage (tab-specific)
            with get_db_context() as db:
                old_hashes = await self.hash_storage.load_hashes(file_id, db, tab_name=tab_name)
            
            # Compare hash sets
            change_set = self.hash_validator.compare_hash_sets(old_hashes, new_hashes)
            
            # Create comprehensive result
            result = self.hash_validator.create_change_detection_result(file_id, change_set)
            
            # Record monitoring metrics
            if self.monitoring:
                # Determine file type from stored hashes if available
                file_type = 'unknown'
                if old_hashes:
                    # Try to get file type from database
                    try:
                        with get_db_context() as db:
                            from services.database import FileHash
                            file_hash = db.query(FileHash).filter(FileHash.file_id == file_id).first()
                            if file_hash:
                                file_type = file_hash.file_type
                    except Exception:
                        pass
                
                self.monitoring.record_change_detection(
                    file_id, file_type, result.has_changes, 
                    result.change_summary.get('total_changes', 0)
                )
            
            logger.info(f"Change detection for file {file_id}: {result.change_summary}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error comparing hashes for file {file_id}: {str(e)}")
            
            # Return empty result on error
            return ChangeDetectionResult(
                file_id=file_id,
                has_changes=False,
                added_items=[],
                modified_items=[],
                deleted_items=[],
                unchanged_count=0,
                change_summary={'added': 0, 'modified': 0, 'deleted': 0, 'unchanged': 0, 'total_changes': 0}
            )
    
    async def store_hashes(self, file_id: str, file_type: str, hashes: List[Hash], tab_name: str = None) -> bool:
        """
        Store hashes in database with tab support.
        
        Args:
            file_id: File identifier
            file_type: Type of file ('sheet', 'doc', 'pdf')
            hashes: List of Hash objects to store
            tab_name: Optional tab name for sheet-specific storage
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Validate hashes before storing
            for hash_obj in hashes:
                if not self.hash_validator.validate_hash_object(hash_obj):
                    logger.warning(f"Invalid hash object detected for file {file_id}: {hash_obj.hash_value}")
                    return False
            
            # Store with incremental updates to prevent repeated processing (tab-specific)
            success = await self.hash_storage.save_hashes_incremental(file_id, file_type, hashes, tab_name=tab_name)
            
            if success:
                tab_info = f" (tab: {tab_name})" if tab_name else ""
                logger.info(f"Successfully stored {len(hashes)} hashes for file {file_id}{tab_info}")
            else:
                tab_info = f" (tab: {tab_name})" if tab_name else ""
                logger.error(f"Failed to store hashes for file {file_id}{tab_info}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error storing hashes for file {file_id}: {str(e)}")
            return False
    
    async def get_stored_hashes(self, file_id: str, tab_name: str = None) -> List[Hash]:
        """
        Get stored hashes for a file with optional tab filtering.
        
        Args:
            file_id: File identifier
            tab_name: Optional tab name for sheet-specific hashes
            
        Returns:
            List of Hash objects
        """
        try:
            with get_db_context() as db:
                hashes = await self.hash_storage.load_hashes(file_id, db, tab_name=tab_name)
            
            tab_info = f" (tab: {tab_name})" if tab_name else ""
            logger.debug(f"Retrieved {len(hashes)} stored hashes for file {file_id}{tab_info}")
            return hashes
            
        except Exception as e:
            tab_info = f" (tab: {tab_name})" if tab_name else ""
            logger.error(f"Error retrieving stored hashes for file {file_id}{tab_info}: {str(e)}")
            return []
    
    async def process_file_with_change_detection(self, file_id: str, file_type: str, content: Any, tab_name: str = None) -> Dict[str, Any]:
        """
        Complete file processing: compute hashes, detect changes, and store results.
        
        Args:
            file_id: File identifier
            file_type: Type of file ('sheet', 'doc', 'pdf')
            content: File content
            tab_name: Optional tab name for sheet-specific processing
            
        Returns:
            Dictionary with processing results
        """
        try:
            # Compute new hashes
            hash_result = await self.compute_file_hash(file_id, file_type, content)
            
            if not hash_result.success:
                return {
                    'success': False,
                    'error': hash_result.error_message,
                    'file_id': file_id,
                    'file_type': file_type
                }
            
            # Detect changes (tab-specific)
            change_result = await self.compare_hashes(file_id, hash_result.hashes, tab_name=tab_name)
            
            # Store new hashes if there are changes or if no previous hashes exist
            stored = False
            if change_result.has_changes or change_result.unchanged_count == 0:
                stored = await self.store_hashes(file_id, file_type, hash_result.hashes, tab_name=tab_name)
            
            return {
                'success': True,
                'file_id': file_id,
                'file_type': file_type,
                'hash_computation': {
                    'hash_count': len(hash_result.hashes),
                    'computation_time_ms': hash_result.computation_time_ms,
                    'total_size': hash_result.total_size
                },
                'change_detection': change_result.change_summary,
                'has_changes': change_result.has_changes,
                'hashes_stored': stored
            }
            
        except Exception as e:
            logger.error(f"Error processing file {file_id}: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'file_id': file_id,
                'file_type': file_type
            }
    
    async def _compute_sheet_hashes(self, content: List[List[Any]]) -> tuple[List[Hash], int]:
        """
        Compute hashes for spreadsheet content.
        
        Args:
            content: List of rows, each row is a list of cell values
            
        Returns:
            Tuple of (hashes, total_size)
        """
        try:
            if not isinstance(content, list):
                raise ValueError("Sheet content must be a list of rows")
            
            # Compute row hashes
            hashes = self.hash_computer.compute_row_hashes(content)
            
            # Calculate total size (approximate)
            total_size = sum(len(str(cell)) for row in content for cell in row)
            
            return hashes, total_size
            
        except Exception as e:
            logger.error(f"Error computing sheet hashes: {str(e)}")
            raise
    
    async def _compute_doc_hashes(self, content: str) -> tuple[List[Hash], int]:
        """
        Compute hashes for document content.
        
        Args:
            content: Document content as string
            
        Returns:
            Tuple of (hashes, total_size)
        """
        try:
            if not isinstance(content, str):
                raise ValueError("Document content must be a string")
            
            # Check content size
            if len(content) > self.max_content_size:
                raise ValueError(f"Content too large: {len(content)} bytes (max: {self.max_content_size})")
            
            # Compute block hashes
            hashes = self.hash_computer.compute_block_hashes(content)
            
            total_size = len(content)
            
            return hashes, total_size
            
        except Exception as e:
            logger.error(f"Error computing document hashes: {str(e)}")
            raise
    
    async def _compute_pdf_hashes(self, content: bytes) -> tuple[List[Hash], int]:
        """
        Compute hashes for PDF content with size-based strategy selection.
        
        Args:
            content: PDF content as bytes
            
        Returns:
            Tuple of (hashes, total_size)
        """
        try:
            if not isinstance(content, bytes):
                raise ValueError("PDF content must be bytes")
            
            # Check content size
            if len(content) > self.max_content_size:
                raise ValueError(f"Content too large: {len(content)} bytes (max: {self.max_content_size})")
            
            # Determine strategy based on size
            use_blocks = len(content) >= self.pdf_large_threshold
            
            # Compute hashes
            hashes = self.hash_computer.compute_binary_hashes(content, use_blocks)
            
            total_size = len(content)
            
            logger.info(f"PDF hashing strategy: {'block-wise' if use_blocks else 'whole-file'} "
                       f"for {total_size} bytes")
            
            return hashes, total_size
            
        except Exception as e:
            logger.error(f"Error computing PDF hashes: {str(e)}")
            raise
    
    async def get_service_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive service statistics.
        
        Returns:
            Dictionary with service statistics
        """
        try:
            with get_db_context() as db:
                hash_stats = await self.hash_storage.get_hash_statistics(db)
            
            stats = {
                'service_status': 'active',
                'configuration': {
                    'pdf_large_threshold_mb': self.pdf_large_threshold / (1024 * 1024),
                    'max_content_size_mb': self.max_content_size / (1024 * 1024),
                    'default_block_size_kb': self.hash_computer.default_block_size / 1024,
                    'pdf_block_size_mb': self.hash_computer.pdf_block_size / (1024 * 1024)
                },
                'storage_statistics': hash_stats
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting service statistics: {str(e)}")
            return {
                'service_status': 'error',
                'error': str(e)
            }
    
    async def compute_hash_from_source(self, file_id: str, file_type: str, **kwargs) -> Dict[str, Any]:
        """
        Compute hash by retrieving content from source.
        
        Args:
            file_id: File identifier
            file_type: Type of file ('sheet', 'doc', 'pdf')
            **kwargs: Additional arguments for content retrieval
            
        Returns:
            Dictionary with hash computation results
        """
        try:
            # Retrieve content from source
            content_result = await self.content_retrieval.retrieve_content_by_type(
                file_id, file_type, **kwargs
            )
            
            if not content_result['success']:
                return {
                    'success': False,
                    'error': f"Content retrieval failed: {content_result.get('error', 'Unknown error')}",
                    'file_id': file_id,
                    'file_type': file_type
                }
            
            # Process with change detection (pass tab_name if available)
            tab_name = kwargs.get('tab_name')
            result = await self.process_file_with_change_detection(
                file_id, file_type, content_result['content'], tab_name=tab_name
            )
            
            # Add content metadata to result
            result['content_metadata'] = content_result.get('metadata', {})
            
            return result
            
        except Exception as e:
            logger.error(f"Error computing hash from source for {file_type} {file_id}: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'file_id': file_id,
                'file_type': file_type
            }
    
    async def cleanup_orphaned_data(self) -> Dict[str, Any]:
        """
        Clean up orphaned hash data.
        
        Returns:
            Dictionary with cleanup results
        """
        try:
            with get_db_context() as db:
                cleaned_count = await self.hash_storage.cleanup_orphaned_hashes(db)
            
            result = {
                'success': True,
                'cleaned_items': cleaned_count,
                'message': f"Cleaned up {cleaned_count} orphaned items"
            }
            
            logger.info(f"Cleanup completed: {cleaned_count} items removed")
            return result
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'cleaned_items': 0
            }