"""
Google Sheets Service
Handles all Google Sheets operations with proper error handling and retry logic
"""

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.errors import HttpError
from typing import List, Dict, Optional
import os
import json
import logging
import asyncio
from pathlib import Path

from core.exceptions import ExternalAPIError, ValidationError, ServiceError
from services.cache import get_cache_service
from services.rate_limiter import get_rate_limiter
from core.config import get_settings

logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds


class SheetsService:
    """Service for interacting with Google Sheets API"""
    
    def __init__(self):
        """Initialize Google Sheets API client"""
        try:
            creds = self._get_credentials()
            self.sheets_service = build('sheets', 'v4', credentials=creds)
            self.drive_service = build('drive', 'v3', credentials=creds)
            self.cache = get_cache_service()
            self.rate_limiter = get_rate_limiter()
            settings = get_settings()
            self.cache_ttl = settings.cache_sheets_ttl
        except Exception as e:
            logger.error(f"Failed to initialize SheetsService: {str(e)}")
            raise ServiceError(
                f"Failed to initialize Google Sheets service: {str(e)}",
                service_name="SheetsService"
            )
    
    def _get_credentials(self):
        """Get Google credentials from various sources"""
        # Option 1: Service Account (recommended for server)
        creds_path = Path('credentials/service-account.json')
        if creds_path.exists():
            return service_account.Credentials.from_service_account_file(
                str(creds_path),
                scopes=[
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'  # Full drive access for creating sheets
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
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'  # Full drive access for creating sheets
                ]
            )
        
        raise ServiceError(
            "No Google credentials found. Set up service account or OAuth2.",
            service_name="SheetsService"
        )
    
    async def _retry_request(self, func, *args, **kwargs):
        """Retry a request with exponential backoff and rate limiting"""
        # Apply rate limiting before making the request
        operation_name = kwargs.get('operation_name', func.__name__ if hasattr(func, '__name__') else 'api_call')
        await self.rate_limiter.acquire(operation_name)
        
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
                        f"Google Sheets API client error: {str(e)}",
                        api_name="Google Sheets",
                        status_code=status_code,
                        details={'error': str(e), 'attempt': attempt + 1}
                    )
                
                # Retry on 5xx errors or network issues
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAY * (2 ** attempt)
                    logger.warning(
                        f"Google Sheets API request failed (attempt {attempt + 1}/{MAX_RETRIES}), "
                        f"retrying in {delay}s: {str(e)}"
                    )
                    await asyncio.sleep(delay)
                else:
                    raise ExternalAPIError(
                        f"Google Sheets API request failed after {MAX_RETRIES} attempts: {str(e)}",
                        api_name="Google Sheets",
                        status_code=status_code,
                        details={'error': str(e), 'attempts': MAX_RETRIES}
                    )
            except Exception as e:
                last_error = e
                error_msg = str(e)
                error_type = type(e).__name__
                
                # Check for network connectivity issues
                is_network_error = any(keyword in error_msg.lower() for keyword in [
                    'unable to find the server', 'connection', 'network', 'dns', 
                    'socket', 'gaierror', 'server not found', 'name resolution'
                ]) or 'gaierror' in error_type.lower() or 'ServerNotFoundError' in error_type
                
                if is_network_error:
                    # For network errors, provide helpful message and don't retry as much
                    if attempt < MAX_RETRIES - 1:
                        delay = RETRY_DELAY * (2 ** attempt)
                        logger.warning(
                            f"Network connectivity issue (attempt {attempt + 1}/{MAX_RETRIES}), "
                            f"retrying in {delay}s: {error_msg}"
                        )
                        await asyncio.sleep(delay)
                    else:
                        # Provide helpful error message for network issues
                        raise ServiceError(
                            f"Network connectivity issue: Cannot reach Google APIs. "
                            f"Please check your internet connection and firewall settings. "
                            f"Error: {error_msg}",
                            service_name="SheetsService",
                            details={
                                'error': error_msg,
                                'error_type': 'network_error',
                                'attempts': MAX_RETRIES,
                                'suggestion': 'Check internet connection, firewall, DNS, and proxy settings'
                            }
                        )
                else:
                    # For other errors, retry normally
                    if attempt < MAX_RETRIES - 1:
                        delay = RETRY_DELAY * (2 ** attempt)
                        logger.warning(
                            f"Unexpected error in Google Sheets API request (attempt {attempt + 1}/{MAX_RETRIES}), "
                            f"retrying in {delay}s: {error_msg}"
                        )
                        await asyncio.sleep(delay)
                    else:
                        raise ServiceError(
                            f"Unexpected error in Google Sheets API after {MAX_RETRIES} attempts: {error_msg}",
                            service_name="SheetsService",
                            details={'error': error_msg, 'attempts': MAX_RETRIES}
                        )
        
        # Should never reach here, but just in case
        raise ServiceError(
            f"Failed to complete Google Sheets API request: {str(last_error)}",
            service_name="SheetsService"
        )
    
    async def list_sheets(self) -> List[Dict]:
        """
        List all Google Sheets
        
        Returns:
            List of sheet dictionaries with id, name, createdTime, modifiedTime
        
        Raises:
            ExternalAPIError: If Google API call fails
            ServiceError: If service operation fails
        """
        try:
            # Try cache first
            cache_key = self.cache._generate_key('sheets_list')
            cached_sheets = self.cache.get(cache_key)
            if cached_sheets is not None:
                logger.info(f"Cache hit for sheets list: {len(cached_sheets)} sheets")
                return cached_sheets
            
            def _list():
                return self.drive_service.files().list(
                    q="mimeType='application/vnd.google-apps.spreadsheet' and trashed=false",
                    pageSize=100,
                    fields="files(id, name, createdTime, modifiedTime)"
                ).execute()
            
            results = await self._retry_request(_list)
            sheets = results.get('files', [])
            logger.info(f"Successfully listed {len(sheets)} sheets")
            
            # Cache the result
            self.cache.set(cache_key, sheets, self.cache_ttl)
            logger.debug(f"Cached sheets list: {cache_key}")
            
            return sheets
        except (ExternalAPIError, ServiceError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error listing sheets: {str(e)}")
            raise ServiceError(
                f"Failed to list sheets: {str(e)}",
                service_name="SheetsService"
            )
    
    async def read_sheet(self, sheet_id: str, tab_name: str, limit: int = None) -> List[List]:
        """
        Read data from a Google Sheet
        
        Args:
            sheet_id: ID of the Google Sheet
            tab_name: Name of the tab/sheet to read
            limit: Maximum number of rows to read (None = read all data)
        
        Returns:
            List of rows, where each row is a list of cell values
        
        Raises:
            ValidationError: If sheet_id or tab_name is invalid
            NotFoundError: If sheet or tab is not found
            ExternalAPIError: If Google API call fails
        """
        if not sheet_id or not isinstance(sheet_id, str):
            raise ValidationError("sheet_id is required and must be a string", field="sheet_id")
        
        if not tab_name or not isinstance(tab_name, str):
            raise ValidationError("tab_name is required and must be a string", field="tab_name")
        
        if limit is not None and (limit < 1 or limit > 50000):
            raise ValidationError("limit must be between 1 and 50000", field="limit")
        
        try:
            # Try cache first
            cache_key = self.cache._generate_key(
                'sheet_read',
                sheet_id=sheet_id,
                tab_name=tab_name,
                limit=limit or 'all'
            )
            cached_data = self.cache.get(cache_key)
            if cached_data is not None:
                logger.info(f"Cache hit for sheet read: {sheet_id}/{tab_name}")
                return cached_data
            
            def _read():
                # If no limit specified, read all data from the tab
                if limit is None:
                    range_spec = f"{tab_name}"  # This reads all data in the tab
                else:
                    range_spec = f"{tab_name}!A1:ZZ{limit}"
                
                return self.sheets_service.spreadsheets().values().get(
                    spreadsheetId=sheet_id,
                    range=range_spec
                ).execute()
            
            result = await self._retry_request(_read)
            values = result.get('values', [])
            logger.info(f"Successfully read {len(values)} rows from sheet {sheet_id}, tab {tab_name}")
            
            # Cache the result (with shorter TTL for large datasets)
            cache_ttl = self.cache_ttl if limit and limit <= 1000 else 300  # 5 minutes for large datasets
            self.cache.set(cache_key, values, cache_ttl)
            logger.debug(f"Cached sheet data: {cache_key}")
            
            return values
        except HttpError as e:
            if e.resp.status == 404:
                from core.exceptions import NotFoundError
                raise NotFoundError("sheet", sheet_id)
            raise
        except (ExternalAPIError, ValidationError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error reading sheet {sheet_id}: {str(e)}")
            raise ServiceError(
                f"Failed to read sheet: {str(e)}",
                service_name="SheetsService",
                details={'sheet_id': sheet_id, 'tab_name': tab_name}
            )
    
    async def write_sheet(self, sheet_id: str, tab_name: str, data: List[List]) -> Dict:
        """
        Write data to a Google Sheet (append rows)
        
        Args:
            sheet_id: ID of the Google Sheet
            tab_name: Name of the tab/sheet to write to
            data: List of rows, where each row is a list of cell values
        
        Returns:
            Result dictionary from Google API
        
        Raises:
            ValidationError: If inputs are invalid
            NotFoundError: If sheet or tab is not found
            ExternalAPIError: If Google API call fails
        """
        if not sheet_id or not isinstance(sheet_id, str):
            raise ValidationError("sheet_id is required and must be a string", field="sheet_id")
        
        if not tab_name or not isinstance(tab_name, str):
            raise ValidationError("tab_name is required and must be a string", field="tab_name")
        
        if not data or not isinstance(data, list):
            raise ValidationError("data is required and must be a list", field="data")
        
        if not all(isinstance(row, list) for row in data):
            raise ValidationError("data must be a list of lists", field="data")
        
        try:
            def _write():
                body = {'values': data}
                return self.sheets_service.spreadsheets().values().append(
                    spreadsheetId=sheet_id,
                    range=f"{tab_name}!A1",
                    valueInputOption='USER_ENTERED',
                    body=body
                ).execute()
            
            result = await self._retry_request(_write)
            logger.info(f"Successfully wrote {len(data)} rows to sheet {sheet_id}, tab {tab_name}")
            
            # Invalidate cache for this sheet
            self._invalidate_sheet_cache(sheet_id, tab_name)
            
            return result
        except HttpError as e:
            if e.resp.status == 404:
                from core.exceptions import NotFoundError
                raise NotFoundError("sheet", sheet_id)
            raise
        except (ExternalAPIError, ValidationError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error writing to sheet {sheet_id}: {str(e)}")
            raise ServiceError(
                f"Failed to write to sheet: {str(e)}",
                service_name="SheetsService",
                details={'sheet_id': sheet_id, 'tab_name': tab_name, 'rows': len(data)}
            )
    
    async def create_sheet(self, sheet_name: str) -> Dict:
        """
        Create a new Google Sheet
        
        Args:
            sheet_name: Name for the new sheet
        
        Returns:
            Dictionary with id, url, and name of the created sheet
        
        Raises:
            ValidationError: If sheet_name is invalid
            ExternalAPIError: If Google API call fails
        """
        if not sheet_name or not isinstance(sheet_name, str):
            raise ValidationError("sheet_name is required and must be a string", field="sheet_name")
        
        if len(sheet_name.strip()) == 0:
            raise ValidationError("sheet_name cannot be empty", field="sheet_name")
        
        try:
            def _create():
                spreadsheet = {
                    'properties': {
                        'title': sheet_name
                    }
                }
                return self.sheets_service.spreadsheets().create(
                    body=spreadsheet,
                    fields='spreadsheetId,spreadsheetUrl'
                ).execute()
            
            result = await self._retry_request(_create)
            sheet_info = {
                'id': result.get('spreadsheetId'),
                'url': result.get('spreadsheetUrl'),
                'name': sheet_name
            }
            logger.info(f"Successfully created sheet: {sheet_name} (ID: {sheet_info['id']})")
            return sheet_info
        except (ExternalAPIError, ValidationError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating sheet: {str(e)}")
            raise ServiceError(
                f"Failed to create sheet: {str(e)}",
                service_name="SheetsService",
                details={'sheet_name': sheet_name}
            )
    
    async def update_cell(self, sheet_id: str, tab_name: str, row: int, col: int, value: str) -> Dict:
        """
        Update a specific cell in a Google Sheet
        
        Args:
            sheet_id: ID of the Google Sheet
            tab_name: Name of the tab/sheet
            row: Row index (1-based)
            col: Column index (1-based, A=1, B=2, etc.)
            value: New cell value
        
        Returns:
            Result dictionary from Google API
        
        Raises:
            ValidationError: If inputs are invalid
            NotFoundError: If sheet or tab is not found
            ExternalAPIError: If Google API call fails
        """
        if not sheet_id or not isinstance(sheet_id, str):
            raise ValidationError("sheet_id is required and must be a string", field="sheet_id")
        
        if not tab_name or not isinstance(tab_name, str):
            raise ValidationError("tab_name is required and must be a string", field="tab_name")
        
        if row < 1:
            raise ValidationError("row must be >= 1", field="row")
        
        if col < 1:
            raise ValidationError("col must be >= 1", field="col")
        
        try:
            # Convert column number to letter (A, B, C, etc.)
            col_letter = self._number_to_column_letter(col)
            range_name = f"{tab_name}!{col_letter}{row}"
            
            def _update():
                body = {'values': [[value]]}
                return self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=sheet_id,
                    range=range_name,
                    valueInputOption='USER_ENTERED',
                    body=body
                ).execute()
            
            result = await self._retry_request(_update)
            logger.info(f"Successfully updated cell {range_name} in sheet {sheet_id}")
            
            # Invalidate cache for this sheet
            self._invalidate_sheet_cache(sheet_id, tab_name)
            
            return result
        except HttpError as e:
            if e.resp.status == 404:
                raise NotFoundError("sheet", sheet_id)
            raise
        except (ExternalAPIError, ValidationError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error updating cell: {str(e)}")
            raise ServiceError(
                f"Failed to update cell: {str(e)}",
                service_name="SheetsService",
                details={'sheet_id': sheet_id, 'tab_name': tab_name, 'row': row, 'col': col}
            )
    
    async def update_range(self, sheet_id: str, tab_name: str, start_row: int, start_col: int,
                          end_row: int, end_col: int, values: List[List[str]]) -> Dict:
        """
        Update a range of cells in a Google Sheet
        
        Args:
            sheet_id: ID of the Google Sheet
            tab_name: Name of the tab/sheet
            start_row: Start row index (1-based)
            start_col: Start column index (1-based)
            end_row: End row index (1-based)
            end_col: End column index (1-based)
            values: 2D list of values to write
        
        Returns:
            Result dictionary from Google API
        """
        if not sheet_id or not isinstance(sheet_id, str):
            raise ValidationError("sheet_id is required and must be a string", field="sheet_id")
        
        if not tab_name or not isinstance(tab_name, str):
            raise ValidationError("tab_name is required and must be a string", field="tab_name")
        
        if not values or not isinstance(values, list):
            raise ValidationError("values is required and must be a list of lists", field="values")
        
        try:
            start_col_letter = self._number_to_column_letter(start_col)
            end_col_letter = self._number_to_column_letter(end_col)
            range_name = f"{tab_name}!{start_col_letter}{start_row}:{end_col_letter}{end_row}"
            
            def _update():
                body = {'values': values}
                return self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=sheet_id,
                    range=range_name,
                    valueInputOption='USER_ENTERED',
                    body=body
                ).execute()
            
            result = await self._retry_request(_update)
            logger.info(f"Successfully updated range {range_name} in sheet {sheet_id}")
            
            # Invalidate cache for this sheet
            self._invalidate_sheet_cache(sheet_id, tab_name)
            
            return result
        except HttpError as e:
            if e.resp.status == 404:
                raise NotFoundError("sheet", sheet_id)
            raise
        except (ExternalAPIError, ValidationError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error updating range: {str(e)}")
            raise ServiceError(
                f"Failed to update range: {str(e)}",
                service_name="SheetsService",
                details={'sheet_id': sheet_id, 'tab_name': tab_name}
            )
    
    async def delete_rows(self, sheet_id: str, tab_name: str, start_row: int, end_row: int) -> Dict:
        """
        Delete rows from a Google Sheet
        
        Args:
            sheet_id: ID of the Google Sheet
            tab_name: Name of the tab/sheet
            start_row: Start row index (1-based)
            end_row: End row index (1-based)
        
        Returns:
            Result dictionary from Google API
        """
        if not sheet_id or not isinstance(sheet_id, str):
            raise ValidationError("sheet_id is required and must be a string", field="sheet_id")
        
        if not tab_name or not isinstance(tab_name, str):
            raise ValidationError("tab_name is required and must be a string", field="tab_name")
        
        if start_row < 1 or end_row < start_row:
            raise ValidationError("Invalid row range", field="rows")
        
        try:
            # Get sheet ID for the tab
            spreadsheet = self.sheets_service.spreadsheets().get(
                spreadsheetId=sheet_id
            ).execute()
            
            sheet_id_for_tab = None
            for sheet in spreadsheet.get('sheets', []):
                if sheet['properties']['title'] == tab_name:
                    sheet_id_for_tab = sheet['properties']['sheetId']
                    break
            
            if not sheet_id_for_tab:
                raise NotFoundError("tab", tab_name)
            
            def _delete():
                requests = [{
                    'deleteDimension': {
                        'range': {
                            'sheetId': sheet_id_for_tab,
                            'dimension': 'ROWS',
                            'startIndex': start_row - 1,  # Convert to 0-based
                            'endIndex': end_row
                        }
                    }
                }]
                
                body = {'requests': requests}
                return self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=sheet_id,
                    body=body
                ).execute()
            
            result = await self._retry_request(_delete)
            logger.info(f"Successfully deleted rows {start_row}-{end_row} from sheet {sheet_id}, tab {tab_name}")
            
            # Invalidate cache for this sheet
            self._invalidate_sheet_cache(sheet_id, tab_name)
            
            return result
        except HttpError as e:
            if e.resp.status == 404:
                raise NotFoundError("sheet", sheet_id)
            raise
        except (ExternalAPIError, ValidationError, NotFoundError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error deleting rows: {str(e)}")
            raise ServiceError(
                f"Failed to delete rows: {str(e)}",
                service_name="SheetsService",
                details={'sheet_id': sheet_id, 'tab_name': tab_name, 'start_row': start_row, 'end_row': end_row}
            )
    
    async def delete_columns(self, sheet_id: str, tab_name: str, start_col: int, end_col: int) -> Dict:
        """
        Delete columns from a Google Sheet
        
        Args:
            sheet_id: ID of the Google Sheet
            tab_name: Name of the tab/sheet
            start_col: Start column index (1-based, A=1)
            end_col: End column index (1-based)
        
        Returns:
            Result dictionary from Google API
        """
        if not sheet_id or not isinstance(sheet_id, str):
            raise ValidationError("sheet_id is required and must be a string", field="sheet_id")
        
        if not tab_name or not isinstance(tab_name, str):
            raise ValidationError("tab_name is required and must be a string", field="tab_name")
        
        if start_col < 1 or end_col < start_col:
            raise ValidationError("Invalid column range", field="columns")
        
        try:
            # Get sheet ID for the tab
            spreadsheet = self.sheets_service.spreadsheets().get(
                spreadsheetId=sheet_id
            ).execute()
            
            sheet_id_for_tab = None
            for sheet in spreadsheet.get('sheets', []):
                if sheet['properties']['title'] == tab_name:
                    sheet_id_for_tab = sheet['properties']['sheetId']
                    break
            
            if not sheet_id_for_tab:
                raise NotFoundError("tab", tab_name)
            
            def _delete():
                requests = [{
                    'deleteDimension': {
                        'range': {
                            'sheetId': sheet_id_for_tab,
                            'dimension': 'COLUMNS',
                            'startIndex': start_col - 1,  # Convert to 0-based
                            'endIndex': end_col
                        }
                    }
                }]
                
                body = {'requests': requests}
                return self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=sheet_id,
                    body=body
                ).execute()
            
            result = await self._retry_request(_delete)
            logger.info(f"Successfully deleted columns {start_col}-{end_col} from sheet {sheet_id}, tab {tab_name}")
            
            # Invalidate cache for this sheet
            self._invalidate_sheet_cache(sheet_id, tab_name)
            
            return result
        except HttpError as e:
            if e.resp.status == 404:
                raise NotFoundError("sheet", sheet_id)
            raise
        except (ExternalAPIError, ValidationError, NotFoundError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error deleting columns: {str(e)}")
            raise ServiceError(
                f"Failed to delete columns: {str(e)}",
                service_name="SheetsService",
                details={'sheet_id': sheet_id, 'tab_name': tab_name, 'start_col': start_col, 'end_col': end_col}
            )
    
    async def insert_rows(self, sheet_id: str, tab_name: str, row_index: int, num_rows: int = 1) -> Dict:
        """
        Insert rows into a Google Sheet
        
        Args:
            sheet_id: ID of the Google Sheet
            tab_name: Name of the tab/sheet
            row_index: Row index where to insert (1-based)
            num_rows: Number of rows to insert
        
        Returns:
            Result dictionary from Google API
        """
        if not sheet_id or not isinstance(sheet_id, str):
            raise ValidationError("sheet_id is required and must be a string", field="sheet_id")
        
        if not tab_name or not isinstance(tab_name, str):
            raise ValidationError("tab_name is required and must be a string", field="tab_name")
        
        if row_index < 1:
            raise ValidationError("row_index must be >= 1", field="row_index")
        
        if num_rows < 1:
            raise ValidationError("num_rows must be >= 1", field="num_rows")
        
        try:
            # Get sheet ID for the tab
            spreadsheet = self.sheets_service.spreadsheets().get(
                spreadsheetId=sheet_id
            ).execute()
            
            sheet_id_for_tab = None
            for sheet in spreadsheet.get('sheets', []):
                if sheet['properties']['title'] == tab_name:
                    sheet_id_for_tab = sheet['properties']['sheetId']
                    break
            
            if not sheet_id_for_tab:
                raise NotFoundError("tab", tab_name)
            
            def _insert():
                requests = [{
                    'insertDimension': {
                        'range': {
                            'sheetId': sheet_id_for_tab,
                            'dimension': 'ROWS',
                            'startIndex': row_index - 1,  # Convert to 0-based
                            'endIndex': row_index - 1 + num_rows
                        }
                    }
                }]
                
                body = {'requests': requests}
                return self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=sheet_id,
                    body=body
                ).execute()
            
            result = await self._retry_request(_insert)
            logger.info(f"Successfully inserted {num_rows} row(s) at index {row_index} in sheet {sheet_id}, tab {tab_name}")
            
            # Invalidate cache for this sheet
            self._invalidate_sheet_cache(sheet_id, tab_name)
            
            return result
        except HttpError as e:
            if e.resp.status == 404:
                raise NotFoundError("sheet", sheet_id)
            raise
        except (ExternalAPIError, ValidationError, NotFoundError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error inserting rows: {str(e)}")
            raise ServiceError(
                f"Failed to insert rows: {str(e)}",
                service_name="SheetsService",
                details={'sheet_id': sheet_id, 'tab_name': tab_name, 'row_index': row_index, 'num_rows': num_rows}
            )
    
    def _number_to_column_letter(self, col_num: int) -> str:
        """Convert column number (1-based) to letter (A, B, C, etc.)"""
        result = ""
        while col_num > 0:
            col_num -= 1
            result = chr(65 + (col_num % 26)) + result
            col_num //= 26
        return result
    
    async def create_tab(self, sheet_id: str, tab_name: str) -> Dict:
        """
        Create a new tab in a Google Sheet
        
        Args:
            sheet_id: ID of the Google Sheet
            tab_name: Name for the new tab
        
        Returns:
            Result dictionary from Google API
        
        Raises:
            ValidationError: If inputs are invalid
            NotFoundError: If sheet is not found
            ExternalAPIError: If Google API call fails
        """
        if not sheet_id or not isinstance(sheet_id, str):
            raise ValidationError("sheet_id is required and must be a string", field="sheet_id")
        
        if not tab_name or not isinstance(tab_name, str):
            raise ValidationError("tab_name is required and must be a string", field="tab_name")
        
        if len(tab_name.strip()) == 0:
            raise ValidationError("tab_name cannot be empty", field="tab_name")
        
        try:
            def _create_tab():
                body = {
                    'requests': [{
                        'addSheet': {
                            'properties': {
                                'title': tab_name
                            }
                        }
                    }]
                }
                return self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=sheet_id,
                    body=body
                ).execute()
            
            result = await self._retry_request(_create_tab)
            logger.info(f"Successfully created tab '{tab_name}' in sheet {sheet_id}")
            
            # Invalidate cache for this sheet
            self._invalidate_sheet_cache(sheet_id)
            
            return result
        except HttpError as e:
            if e.resp.status == 404:
                raise NotFoundError("sheet", sheet_id)
            raise
        except (ExternalAPIError, ValidationError, NotFoundError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating tab: {str(e)}")
            raise ServiceError(
                f"Failed to create tab: {str(e)}",
                service_name="SheetsService",
                details={'sheet_id': sheet_id, 'tab_name': tab_name}
            )
    
    def _invalidate_sheet_cache(self, sheet_id: str, tab_name: Optional[str] = None):
        """
        Invalidate cache entries for a sheet.
        
        Args:
            sheet_id: Sheet ID
            tab_name: Optional tab name (if None, invalidates all tabs)
        """
        try:
            # Invalidate sheet read cache
            if tab_name:
                # Invalidate specific tab
                pattern = f"sheet_read:{sheet_id}:{tab_name}"
            else:
                # Invalidate all tabs for this sheet
                pattern = f"sheet_read:{sheet_id}"
            
            count = self.cache.invalidate(pattern)
            if count > 0:
                logger.debug(f"Invalidated {count} cache entries for sheet {sheet_id}")
            
            # Also invalidate sheets list cache
            self.cache.delete(self.cache._generate_key('sheets_list'))
        except Exception as e:
            logger.warning(f"Error invalidating sheet cache: {str(e)}")
