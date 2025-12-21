"""
Content Retrieval Service - API integration for content retrieval
Integrates with existing services and adds PDF content retrieval capabilities
"""

import logging
import time
import asyncio
from typing import Any, Dict, Optional, Union
from services.sheets import SheetsService
from services.docs import DocsService

logger = logging.getLogger(__name__)


class ContentRetrievalService:
    """
    Content retrieval service with API integration and retry logic.
    Handles content retrieval from various sources with fault tolerance.
    """
    
    def __init__(self):
        """Initialize content retrieval service"""
        self.sheets_service = SheetsService()
        self.docs_service = DocsService()
        
        # Load retry configuration from hash service settings
        try:
            from core.config import get_settings
            settings = get_settings()
            
            self.max_retries = settings.hash_max_retries
            self.base_delay = settings.hash_retry_delay_seconds
            self.max_delay = settings.hash_max_retry_delay_seconds
        except Exception as e:
            logger.warning(f"Could not load settings, using defaults: {str(e)}")
            self.max_retries = 3
            self.base_delay = 1.0
            self.max_delay = 30.0
        
        logger.info("Content retrieval service initialized")
    
    async def retrieve_sheet_content(self, sheet_id: str, tab_name: str = None) -> Dict[str, Any]:
        """
        Retrieve content from Google Sheets with retry logic.
        
        Args:
            sheet_id: Google Sheet ID
            tab_name: Optional tab name (retrieves all tabs if None)
            
        Returns:
            Dictionary with content and metadata
        """
        try:
            if tab_name:
                # Retrieve specific tab
                content = await self._retry_operation(
                    self.sheets_service.read_sheet,
                    sheet_id, tab_name
                )
                
                return {
                    'success': True,
                    'content': content,
                    'content_type': 'sheet_tab',
                    'metadata': {
                        'sheet_id': sheet_id,
                        'tab_name': tab_name,
                        'row_count': len(content),
                        'total_cells': sum(len(row) for row in content)
                    }
                }
            else:
                # Retrieve all tabs
                spreadsheet = await self._retry_operation(
                    self._get_spreadsheet_info,
                    sheet_id
                )
                
                all_content = []
                tabs_info = []
                
                for sheet_tab in spreadsheet.get('sheets', []):
                    tab_name = sheet_tab['properties']['title']
                    try:
                        tab_content = await self._retry_operation(
                            self.sheets_service.read_sheet,
                            sheet_id, tab_name
                        )
                        all_content.extend(tab_content)
                        tabs_info.append({
                            'name': tab_name,
                            'row_count': len(tab_content)
                        })
                    except Exception as e:
                        logger.warning(f"Failed to retrieve tab {tab_name}: {str(e)}")
                        tabs_info.append({
                            'name': tab_name,
                            'error': str(e)
                        })
                
                return {
                    'success': True,
                    'content': all_content,
                    'content_type': 'sheet_full',
                    'metadata': {
                        'sheet_id': sheet_id,
                        'tabs': tabs_info,
                        'total_rows': len(all_content),
                        'total_cells': sum(len(row) for row in all_content)
                    }
                }
                
        except Exception as e:
            logger.error(f"Error retrieving sheet content {sheet_id}: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'content_type': 'sheet',
                'metadata': {'sheet_id': sheet_id}
            }
    
    async def retrieve_doc_content(self, doc_id: str) -> Dict[str, Any]:
        """
        Retrieve content from Google Docs with retry logic.
        
        Args:
            doc_id: Google Doc ID
            
        Returns:
            Dictionary with content and metadata
        """
        try:
            content = await self._retry_operation(
                self.docs_service.read_doc,
                doc_id
            )
            
            return {
                'success': True,
                'content': content,
                'content_type': 'doc',
                'metadata': {
                    'doc_id': doc_id,
                    'content_length': len(content),
                    'word_count': len(content.split()) if content else 0
                }
            }
            
        except Exception as e:
            logger.error(f"Error retrieving doc content {doc_id}: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'content_type': 'doc',
                'metadata': {'doc_id': doc_id}
            }
    
    async def retrieve_pdf_content(self, pdf_source: Union[str, bytes], source_type: str = 'url') -> Dict[str, Any]:
        """
        Retrieve PDF content from various sources.
        
        Args:
            pdf_source: PDF source (URL, file path, or bytes)
            source_type: Type of source ('url', 'file', 'bytes')
            
        Returns:
            Dictionary with content and metadata
        """
        try:
            if source_type == 'bytes':
                if not isinstance(pdf_source, bytes):
                    raise ValueError("PDF source must be bytes when source_type is 'bytes'")
                content = pdf_source
                
            elif source_type == 'url':
                content = await self._retrieve_pdf_from_url(pdf_source)
                
            elif source_type == 'file':
                content = await self._retrieve_pdf_from_file(pdf_source)
                
            else:
                raise ValueError(f"Unsupported source_type: {source_type}")
            
            return {
                'success': True,
                'content': content,
                'content_type': 'pdf',
                'metadata': {
                    'source': str(pdf_source)[:100] + '...' if len(str(pdf_source)) > 100 else str(pdf_source),
                    'source_type': source_type,
                    'size_bytes': len(content),
                    'size_mb': round(len(content) / (1024 * 1024), 2)
                }
            }
            
        except Exception as e:
            logger.error(f"Error retrieving PDF content from {source_type}: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'content_type': 'pdf',
                'metadata': {
                    'source': str(pdf_source)[:100] + '...' if len(str(pdf_source)) > 100 else str(pdf_source),
                    'source_type': source_type
                }
            }
    
    async def retrieve_content_by_type(self, file_id: str, file_type: str, **kwargs) -> Dict[str, Any]:
        """
        Retrieve content based on file type.
        
        Args:
            file_id: File identifier
            file_type: Type of file ('sheet', 'doc', 'pdf')
            **kwargs: Additional arguments specific to file type
            
        Returns:
            Dictionary with content and metadata
        """
        try:
            if file_type == 'sheet':
                tab_name = kwargs.get('tab_name')
                return await self.retrieve_sheet_content(file_id, tab_name)
                
            elif file_type == 'doc':
                return await self.retrieve_doc_content(file_id)
                
            elif file_type == 'pdf':
                source_type = kwargs.get('source_type', 'url')
                return await self.retrieve_pdf_content(file_id, source_type)
                
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
                
        except Exception as e:
            logger.error(f"Error retrieving content for {file_type} {file_id}: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'content_type': file_type,
                'metadata': {'file_id': file_id}
            }
    
    async def _retry_operation(self, operation, *args, **kwargs):
        """
        Execute operation with exponential backoff retry logic.
        
        Args:
            operation: Async function to execute
            *args: Arguments for the operation
            **kwargs: Keyword arguments for the operation
            
        Returns:
            Result of the operation
        """
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                return await operation(*args, **kwargs)
                
            except Exception as e:
                last_exception = e
                logger.warning(f"Operation failed on attempt {attempt + 1}/{self.max_retries}: {str(e)}")
                
                if attempt < self.max_retries - 1:
                    # Calculate exponential backoff delay
                    delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                    logger.info(f"Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
        
        # All retries failed
        logger.error(f"Operation failed after {self.max_retries} attempts")
        raise last_exception
    
    async def _get_spreadsheet_info(self, sheet_id: str) -> Dict[str, Any]:
        """
        Get spreadsheet information including tabs.
        
        Args:
            sheet_id: Google Sheet ID
            
        Returns:
            Spreadsheet information
        """
        return self.sheets_service.sheets_service.spreadsheets().get(
            spreadsheetId=sheet_id
        ).execute()
    
    async def _retrieve_pdf_from_url(self, url: str) -> bytes:
        """
        Retrieve PDF content from URL.
        
        Args:
            url: PDF URL
            
        Returns:
            PDF content as bytes
        """
        try:
            import aiohttp
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        content = await response.read()
                        
                        # Validate it's a PDF
                        if not content.startswith(b'%PDF'):
                            raise ValueError("Retrieved content is not a valid PDF")
                        
                        return content
                    else:
                        raise Exception(f"HTTP {response.status}: Failed to retrieve PDF from URL")
                        
        except ImportError:
            logger.error("aiohttp not available for URL retrieval")
            raise Exception("aiohttp library required for URL-based PDF retrieval")
        except Exception as e:
            logger.error(f"Error retrieving PDF from URL {url}: {str(e)}")
            raise
    
    async def _retrieve_pdf_from_file(self, file_path: str) -> bytes:
        """
        Retrieve PDF content from file system.
        
        Args:
            file_path: Path to PDF file
            
        Returns:
            PDF content as bytes
        """
        try:
            import aiofiles
            
            async with aiofiles.open(file_path, 'rb') as f:
                content = await f.read()
                
                # Validate it's a PDF
                if not content.startswith(b'%PDF'):
                    raise ValueError("File is not a valid PDF")
                
                return content
                
        except ImportError:
            # Fallback to synchronous file reading
            try:
                with open(file_path, 'rb') as f:
                    content = f.read()
                    
                    # Validate it's a PDF
                    if not content.startswith(b'%PDF'):
                        raise ValueError("File is not a valid PDF")
                    
                    return content
            except Exception as e:
                logger.error(f"Error reading PDF file {file_path}: {str(e)}")
                raise
        except Exception as e:
            logger.error(f"Error retrieving PDF from file {file_path}: {str(e)}")
            raise
    
    async def get_retrieval_statistics(self) -> Dict[str, Any]:
        """
        Get content retrieval statistics.
        
        Returns:
            Dictionary with retrieval statistics
        """
        try:
            return {
                'service_status': 'active',
                'supported_types': ['sheet', 'doc', 'pdf'],
                'retry_configuration': {
                    'max_retries': self.max_retries,
                    'base_delay': self.base_delay,
                    'max_delay': self.max_delay
                },
                'integrations': {
                    'sheets_service': 'active',
                    'docs_service': 'active',
                    'pdf_support': 'active'
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting retrieval statistics: {str(e)}")
            return {
                'service_status': 'error',
                'error': str(e)
            }