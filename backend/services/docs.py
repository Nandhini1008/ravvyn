"""
Google Docs Service
Handles all Google Docs operations with proper error handling and retry logic
"""

from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from typing import List, Dict, Optional
import os
import json
import logging
import asyncio
from pathlib import Path

from core.exceptions import ExternalAPIError, ValidationError, ServiceError, NotFoundError
from services.cache import get_cache_service
from core.config import get_settings

logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds


class DocsService:
    """Service for interacting with Google Docs API"""
    
    def __init__(self):
        """Initialize Google Docs API client"""
        try:
            creds = self._get_credentials()
            self.docs_service = build('docs', 'v1', credentials=creds)
            self.drive_service = build('drive', 'v3', credentials=creds)
            self.cache = get_cache_service()
            settings = get_settings()
            self.cache_ttl = settings.cache_docs_ttl
        except Exception as e:
            logger.error(f"Failed to initialize DocsService: {str(e)}")
            raise ServiceError(
                f"Failed to initialize Google Docs service: {str(e)}",
                service_name="DocsService"
            )
    
    def _get_credentials(self):
        """Get Google credentials from various sources"""
        # Option 1: Service Account (recommended for server)
        creds_path = Path('credentials/service-account.json')
        if creds_path.exists():
            return service_account.Credentials.from_service_account_file(
                str(creds_path),
                scopes=[
                    'https://www.googleapis.com/auth/documents',
                    'https://www.googleapis.com/auth/drive.readonly'
                ]
            )
        
        # Option 2: OAuth2 credentials from environment
        creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
        if creds_json:
            try:
                creds_data = json.loads(creds_json)
                return Credentials.from_authorized_user_info(creds_data)
            except json.JSONDecodeError as e:
                raise ValidationError(
                    f"Invalid GOOGLE_CREDENTIALS_JSON format: {str(e)}",
                    field="GOOGLE_CREDENTIALS_JSON"
                )
        
        # Option 3: Check GOOGLE_APPLICATION_CREDENTIALS
        app_creds = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if app_creds and Path(app_creds).exists():
            return service_account.Credentials.from_service_account_file(
                app_creds,
                scopes=[
                    'https://www.googleapis.com/auth/documents',
                    'https://www.googleapis.com/auth/drive.readonly'
                ]
            )
        
        raise ServiceError(
            "No Google credentials found. Set up service account or OAuth2.",
            service_name="DocsService"
        )
    
    async def _retry_request(self, func, *args, **kwargs):
        """Retry a request with exponential backoff"""
        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except HttpError as e:
                last_error = e
                status_code = e.resp.status if hasattr(e.resp, 'status') else None
                
                # Don't retry on 4xx errors (client errors)
                if status_code and 400 <= status_code < 500:
                    raise ExternalAPIError(
                        f"Google Docs API client error: {str(e)}",
                        api_name="Google Docs",
                        status_code=status_code,
                        details={'error': str(e), 'attempt': attempt + 1}
                    )
                
                # Retry on 5xx errors or network issues
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAY * (2 ** attempt)
                    logger.warning(
                        f"Google Docs API request failed (attempt {attempt + 1}/{MAX_RETRIES}), "
                        f"retrying in {delay}s: {str(e)}"
                    )
                    await asyncio.sleep(delay)
                else:
                    raise ExternalAPIError(
                        f"Google Docs API request failed after {MAX_RETRIES} attempts: {str(e)}",
                        api_name="Google Docs",
                        status_code=status_code,
                        details={'error': str(e), 'attempts': MAX_RETRIES}
                    )
            except Exception as e:
                last_error = e
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAY * (2 ** attempt)
                    logger.warning(
                        f"Unexpected error in Google Docs API request (attempt {attempt + 1}/{MAX_RETRIES}), "
                        f"retrying in {delay}s: {str(e)}"
                    )
                    await asyncio.sleep(delay)
                else:
                    raise ServiceError(
                        f"Unexpected error in Google Docs API after {MAX_RETRIES} attempts: {str(e)}",
                        service_name="DocsService",
                        details={'error': str(e), 'attempts': MAX_RETRIES}
                    )
        
        # Should never reach here, but just in case
        raise ServiceError(
            f"Failed to complete Google Docs API request: {str(last_error)}",
            service_name="DocsService"
        )
    
    async def list_docs(self) -> List[Dict]:
        """
        List all Google Docs
        
        Returns:
            List of document dictionaries with id, name, createdTime, modifiedTime
        
        Raises:
            ExternalAPIError: If Google API call fails
            ServiceError: If service operation fails
        """
        try:
            # Try cache first
            cache_key = self.cache._generate_key('docs_list')
            cached_docs = self.cache.get(cache_key)
            if cached_docs is not None:
                logger.info(f"Cache hit for docs list: {len(cached_docs)} docs")
                return cached_docs
            
            def _list():
                return self.drive_service.files().list(
                    q="mimeType='application/vnd.google-apps.document' and trashed=false",
                    pageSize=100,
                    fields="files(id, name, createdTime, modifiedTime)"
                ).execute()
            
            results = await self._retry_request(_list)
            docs = results.get('files', [])
            logger.info(f"Successfully listed {len(docs)} documents")
            
            # Cache the result
            self.cache.set(cache_key, docs, self.cache_ttl)
            logger.debug(f"Cached docs list: {cache_key}")
            
            return docs
        except (ExternalAPIError, ServiceError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error listing docs: {str(e)}")
            raise ServiceError(
                f"Failed to list docs: {str(e)}",
                service_name="DocsService"
            )
    
    async def read_doc(self, doc_id: str) -> str:
        """
        Read content from a Google Doc
        
        Args:
            doc_id: ID of the Google Doc
        
        Returns:
            Full text content of the document
        
        Raises:
            ValidationError: If doc_id is invalid
            NotFoundError: If document is not found
            ExternalAPIError: If Google API call fails
        """
        if not doc_id or not isinstance(doc_id, str):
            raise ValidationError("doc_id is required and must be a string", field="doc_id")
        
        try:
            # Try cache first
            cache_key = self.cache._generate_key('doc_read', doc_id=doc_id)
            cached_content = self.cache.get(cache_key)
            if cached_content is not None:
                logger.info(f"Cache hit for doc read: {doc_id}")
                return cached_content
            
            def _read():
                return self.docs_service.documents().get(documentId=doc_id).execute()
            
            doc = await self._retry_request(_read)
            
            # Extract text content from document structure
            content = []
            body = doc.get('body', {})
            elements = body.get('content', [])
            
            for element in elements:
                if 'paragraph' in element:
                    paragraph = element['paragraph']
                    for text_run in paragraph.get('elements', []):
                        if 'textRun' in text_run:
                            text_content = text_run['textRun'].get('content', '')
                            content.append(text_content)
            
            text = ''.join(content)
            logger.info(f"Successfully read document {doc_id} ({len(text)} characters)")
            
            # Cache the result
            self.cache.set(cache_key, text, self.cache_ttl)
            logger.debug(f"Cached doc content: {cache_key}")
            
            return text
        except HttpError as e:
            if e.resp.status == 404:
                raise NotFoundError("document", doc_id)
            raise
        except (ExternalAPIError, ValidationError, NotFoundError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error reading doc {doc_id}: {str(e)}")
            raise ServiceError(
                f"Failed to read doc: {str(e)}",
                service_name="DocsService",
                details={'doc_id': doc_id}
            )
    
    async def create_doc(self, doc_name: str) -> Dict:
        """
        Create a new Google Doc
        
        Args:
            doc_name: Name for the new document
        
        Returns:
            Dictionary with id, name, and url of the created document
        
        Raises:
            ValidationError: If doc_name is invalid
            ExternalAPIError: If Google API call fails
        """
        if not doc_name or not isinstance(doc_name, str):
            raise ValidationError("doc_name is required and must be a string", field="doc_name")
        
        if len(doc_name.strip()) == 0:
            raise ValidationError("doc_name cannot be empty", field="doc_name")
        
        try:
            def _create():
                return self.docs_service.documents().create(body={'title': doc_name}).execute()
            
            doc = await self._retry_request(_create)
            doc_id = doc.get('documentId')
            doc_info = {
                'id': doc_id,
                'name': doc_name,
                'url': f"https://docs.google.com/document/d/{doc_id}/edit"
            }
            logger.info(f"Successfully created document: {doc_name} (ID: {doc_id})")
            return doc_info
        except (ExternalAPIError, ValidationError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating doc: {str(e)}")
            raise ServiceError(
                f"Failed to create doc: {str(e)}",
                service_name="DocsService",
                details={'doc_name': doc_name}
            )
    
    async def update_doc(self, doc_id: str, content: str, insert_index: Optional[int] = None) -> Dict:
        """
        Update content in a Google Doc
        
        Args:
            doc_id: ID of the Google Doc
            content: Content to insert/update
            insert_index: Optional index to insert at (if None, appends to end)
        
        Returns:
            Result dictionary from Google API
        
        Raises:
            ValidationError: If inputs are invalid
            NotFoundError: If document is not found
            ExternalAPIError: If Google API call fails
        """
        if not doc_id or not isinstance(doc_id, str):
            raise ValidationError("doc_id is required and must be a string", field="doc_id")
        
        if not content or not isinstance(content, str):
            raise ValidationError("content is required and must be a string", field="content")
        
        try:
            # Get current document to find insertion point
            doc = await self._retry_request(
                lambda: self.docs_service.documents().get(documentId=doc_id).execute()
            )
            
            # Find insertion index
            if insert_index is None:
                # Append to end
                body_content = doc.get('body', {}).get('content', [])
                if body_content:
                    # Find the end index
                    last_element = body_content[-1]
                    if 'endIndex' in last_element:
                        insert_index = last_element['endIndex'] - 1
                    else:
                        insert_index = 1
                else:
                    insert_index = 1
            
            def _update():
                requests = [{
                    'insertText': {
                        'location': {
                            'index': insert_index
                        },
                        'text': content
                    }
                }]
                
                return self.docs_service.documents().batchUpdate(
                    documentId=doc_id,
                    body={'requests': requests}
                ).execute()
            
            result = await self._retry_request(_update)
            logger.info(f"Successfully updated doc {doc_id} at index {insert_index}")
            
            # Invalidate cache for this doc
            self._invalidate_doc_cache(doc_id)
            
            return result
        except HttpError as e:
            if e.resp.status == 404:
                raise NotFoundError("document", doc_id)
            raise
        except (ExternalAPIError, ValidationError, NotFoundError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error updating doc {doc_id}: {str(e)}")
            raise ServiceError(
                f"Failed to update doc: {str(e)}",
                service_name="DocsService",
                details={'doc_id': doc_id}
            )
    
    async def delete_doc_content(self, doc_id: str, start_index: int, end_index: int) -> Dict:
        """
        Delete content from a Google Doc
        
        Args:
            doc_id: ID of the Google Doc
            start_index: Start character index
            end_index: End character index
        
        Returns:
            Result dictionary from Google API
        """
        if not doc_id or not isinstance(doc_id, str):
            raise ValidationError("doc_id is required and must be a string", field="doc_id")
        
        if start_index < 0 or end_index <= start_index:
            raise ValidationError("Invalid index range", field="indices")
        
        try:
            def _delete():
                requests = [{
                    'deleteContentRange': {
                        'range': {
                            'startIndex': start_index,
                            'endIndex': end_index
                        }
                    }
                }]
                
                return self.docs_service.documents().batchUpdate(
                    documentId=doc_id,
                    body={'requests': requests}
                ).execute()
            
            result = await self._retry_request(_delete)
            logger.info(f"Successfully deleted content from doc {doc_id} (indices {start_index}-{end_index})")
            
            # Invalidate cache for this doc
            self._invalidate_doc_cache(doc_id)
            
            return result
        except HttpError as e:
            if e.resp.status == 404:
                raise NotFoundError("document", doc_id)
            raise
        except (ExternalAPIError, ValidationError, NotFoundError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error deleting doc content: {str(e)}")
            raise ServiceError(
                f"Failed to delete doc content: {str(e)}",
                service_name="DocsService",
                details={'doc_id': doc_id, 'start_index': start_index, 'end_index': end_index}
            )
    
    async def replace_doc_content(self, doc_id: str, search_text: str, replace_text: str) -> Dict:
        """
        Replace text in a Google Doc
        
        Args:
            doc_id: ID of the Google Doc
            search_text: Text to search for
            replace_text: Text to replace with
        
        Returns:
            Result dictionary from Google API
        """
        if not doc_id or not isinstance(doc_id, str):
            raise ValidationError("doc_id is required and must be a string", field="doc_id")
        
        if not search_text:
            raise ValidationError("search_text cannot be empty", field="search_text")
        
        try:
            def _replace():
                requests = [{
                    'replaceAllText': {
                        'containsText': {
                            'text': search_text,
                            'matchCase': False
                        },
                        'replaceText': replace_text
                    }
                }]
                
                return self.docs_service.documents().batchUpdate(
                    documentId=doc_id,
                    body={'requests': requests}
                ).execute()
            
            result = await self._retry_request(_replace)
            logger.info(f"Successfully replaced text in doc {doc_id}")
            
            # Invalidate cache for this doc
            self._invalidate_doc_cache(doc_id)
            
            return result
        except HttpError as e:
            if e.resp.status == 404:
                raise NotFoundError("document", doc_id)
            raise
        except (ExternalAPIError, ValidationError, NotFoundError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error replacing doc content: {str(e)}")
            raise ServiceError(
                f"Failed to replace doc content: {str(e)}",
                service_name="DocsService",
                details={'doc_id': doc_id}
            )
    
    def _invalidate_doc_cache(self, doc_id: str):
        """
        Invalidate cache entries for a document.
        
        Args:
            doc_id: Document ID
        """
        try:
            # Invalidate doc read cache
            pattern = f"doc_read:{doc_id}"
            count = self.cache.invalidate(pattern)
            if count > 0:
                logger.debug(f"Invalidated {count} cache entries for doc {doc_id}")
            
            # Also invalidate docs list cache
            self.cache.delete(self.cache._generate_key('docs_list'))
        except Exception as e:
            logger.warning(f"Error invalidating doc cache: {str(e)}")
