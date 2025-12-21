"""
Export Service - Generate Google Sheets/Docs from AI responses
Converts structured data and AI responses into Google Sheets or Docs
"""

import json
import re
from typing import Dict, List, Optional, Any
import logging

from services.sheets import SheetsService
from services.docs import DocsService
from core.exceptions import ValidationError, ServiceError

logger = logging.getLogger(__name__)


class ExportService:
    """Service for exporting data to Google Sheets/Docs"""
    
    def __init__(self):
        self.sheets_service = SheetsService()
        self.docs_service = DocsService()
    
    async def export_to_sheet(
        self,
        data: Any,
        sheet_name: str,
        tab_name: str = "Sheet1"
    ) -> Dict:
        """
        Export data to a Google Sheet
        
        Args:
            data: Data to export (list of lists, list of dicts, or string)
            sheet_name: Name for the new sheet
            tab_name: Name for the tab
        
        Returns:
            Dictionary with sheet information
        
        Raises:
            ValidationError: If data format is invalid
            ServiceError: If export fails
        """
        try:
            # Convert data to list of lists format
            rows = self._convert_to_rows(data)
            
            if not rows:
                raise ValidationError("No data to export", field="data")
            
            # Create new sheet
            sheet = await self.sheets_service.create_sheet(sheet_name)
            sheet_id = sheet['id']
            
            # Write data to the sheet
            await self.sheets_service.write_sheet(sheet_id, tab_name, rows)
            
            logger.info(f"Successfully exported {len(rows)} rows to sheet {sheet_id}")
            
            return {
                'sheet_id': sheet_id,
                'sheet_name': sheet_name,
                'tab_name': tab_name,
                'rows_exported': len(rows),
                'url': sheet.get('url')
            }
        except (ValidationError, ServiceError):
            raise
        except Exception as e:
            logger.error(f"Error exporting to sheet: {str(e)}")
            raise ServiceError(
                f"Failed to export to sheet: {str(e)}",
                service_name="ExportService"
            )
    
    async def export_to_existing_sheet(
        self,
        sheet_id: str,
        tab_name: str,
        data: Any,
        append: bool = False
    ) -> Dict:
        """
        Export data to an existing Google Sheet
        
        Args:
            sheet_id: ID of existing sheet
            tab_name: Name of the tab
            data: Data to export
            append: If True, append to existing data; if False, overwrite
        
        Returns:
            Dictionary with export information
        """
        try:
            rows = self._convert_to_rows(data)
            
            if not rows:
                raise ValidationError("No data to export", field="data")
            
            if append:
                # Append data
                await self.sheets_service.write_sheet(sheet_id, tab_name, rows)
            else:
                # Overwrite - clear existing and write new
                # For now, just write (will append). For true overwrite, would need to clear first
                await self.sheets_service.write_sheet(sheet_id, tab_name, rows)
            
            logger.info(f"Successfully exported {len(rows)} rows to existing sheet {sheet_id}")
            
            return {
                'sheet_id': sheet_id,
                'tab_name': tab_name,
                'rows_exported': len(rows),
                'append': append
            }
        except (ValidationError, ServiceError):
            raise
        except Exception as e:
            logger.error(f"Error exporting to existing sheet: {str(e)}")
            raise ServiceError(
                f"Failed to export to existing sheet: {str(e)}",
                service_name="ExportService"
            )
    
    async def export_to_doc(
        self,
        content: str,
        doc_name: str
    ) -> Dict:
        """
        Export content to a Google Doc
        
        Args:
            content: Content to export (text or structured data)
            doc_name: Name for the new document
        
        Returns:
            Dictionary with document information
        
        Raises:
            ValidationError: If content is invalid
            ServiceError: If export fails
        """
        try:
            if not content or not isinstance(content, str):
                raise ValidationError("content is required and must be a string", field="content")
            
            # Format content if it's structured data
            formatted_content = self._format_content_for_doc(content)
            
            # Create new doc
            doc = await self.docs_service.create_doc(doc_name)
            doc_id = doc['id']
            
            # Write content to doc
            await self.docs_service.update_doc(doc_id, formatted_content, insert_index=1)
            
            logger.info(f"Successfully exported content to doc {doc_id}")
            
            return {
                'doc_id': doc_id,
                'doc_name': doc_name,
                'content_length': len(formatted_content),
                'url': doc.get('url')
            }
        except (ValidationError, ServiceError):
            raise
        except Exception as e:
            logger.error(f"Error exporting to doc: {str(e)}")
            raise ServiceError(
                f"Failed to export to doc: {str(e)}",
                service_name="ExportService"
            )
    
    async def export_to_existing_doc(
        self,
        doc_id: str,
        content: str,
        append: bool = True
    ) -> Dict:
        """
        Export content to an existing Google Doc
        
        Args:
            doc_id: ID of existing document
            content: Content to export
            append: If True, append to end; if False, replace all content
        
        Returns:
            Dictionary with export information
        """
        try:
            if not content or not isinstance(content, str):
                raise ValidationError("content is required and must be a string", field="content")
            
            formatted_content = self._format_content_for_doc(content)
            
            if append:
                # Append to end
                await self.docs_service.update_doc(doc_id, "\n\n" + formatted_content)
            else:
                # Replace all content - would need to read current length and delete
                # For now, just append
                await self.docs_service.update_doc(doc_id, formatted_content)
            
            logger.info(f"Successfully exported content to existing doc {doc_id}")
            
            return {
                'doc_id': doc_id,
                'content_length': len(formatted_content),
                'append': append
            }
        except (ValidationError, ServiceError):
            raise
        except Exception as e:
            logger.error(f"Error exporting to existing doc: {str(e)}")
            raise ServiceError(
                f"Failed to export to existing doc: {str(e)}",
                service_name="ExportService"
            )
    
    def _convert_to_rows(self, data: Any) -> List[List[str]]:
        """
        Convert various data formats to list of lists for sheets
        
        Args:
            data: Data in various formats
        
        Returns:
            List of lists (rows) for sheet export
        """
        rows = []
        
        if isinstance(data, str):
            # Try to parse as JSON
            try:
                parsed = json.loads(data)
                return self._convert_to_rows(parsed)
            except json.JSONDecodeError:
                # Treat as plain text - split by lines
                lines = data.split('\n')
                return [[line] for line in lines if line.strip()]
        
        elif isinstance(data, list):
            if not data:
                return []
            
            # Check if it's a list of lists
            if all(isinstance(item, list) for item in data):
                return [[str(cell) for cell in row] for row in data]
            
            # Check if it's a list of dicts
            elif all(isinstance(item, dict) for item in data):
                if not data:
                    return []
                
                # Use keys as headers
                headers = list(data[0].keys())
                rows.append([str(h) for h in headers])
                
                # Add data rows
                for item in data:
                    row = [str(item.get(key, '')) for key in headers]
                    rows.append(row)
                
                return rows
            
            # List of strings or other types
            else:
                return [[str(item)] for item in data]
        
        elif isinstance(data, dict):
            # Convert dict to rows (key-value pairs)
            rows.append(['Key', 'Value'])
            for key, value in data.items():
                rows.append([str(key), str(value)])
            return rows
        
        else:
            # Convert to string
            return [[str(data)]]
    
    def _format_content_for_doc(self, content: str) -> str:
        """
        Format content for document export
        
        Args:
            content: Raw content
        
        Returns:
            Formatted content string
        """
        # Try to parse as JSON and format nicely
        try:
            parsed = json.loads(content)
            if isinstance(parsed, dict):
                # Format as key-value pairs
                lines = []
                for key, value in parsed.items():
                    lines.append(f"{key}: {value}")
                return "\n".join(lines)
            elif isinstance(parsed, list):
                # Format as list
                return "\n".join(str(item) for item in parsed)
            else:
                return str(parsed)
        except json.JSONDecodeError:
            # Return as-is if not JSON
            return content
    
    async def export_chat_to_sheet(
        self,
        conversation_id: str,
        sheet_name: Optional[str] = None
    ) -> Dict:
        """
        Export chat conversation to a Google Sheet
        
        Args:
            conversation_id: Conversation ID
            sheet_name: Optional name for the sheet
        
        Returns:
            Dictionary with sheet information
        """
        try:
            from services.db_queries import get_conversation_history
            
            # Get conversation history
            history = get_conversation_history(conversation_id, limit=1000)
            
            if not history:
                raise ValidationError("No conversation history found", field="conversation_id")
            
            # Convert to rows
            rows = [['Timestamp', 'Role', 'Message']]
            for chat in history:
                created_at = chat.get('created_at', '')
                rows.append(['User', chat.get('message', '')])
                rows.append(['Assistant', chat.get('response', '')])
            
            # Create sheet
            if not sheet_name:
                sheet_name = f"Chat Export - {conversation_id[:8]}"
            
            return await self.export_to_sheet(rows, sheet_name)
        except (ValidationError, ServiceError):
            raise
        except Exception as e:
            logger.error(f"Error exporting chat to sheet: {str(e)}")
            raise ServiceError(
                f"Failed to export chat to sheet: {str(e)}",
                service_name="ExportService"
            )
    
    async def export_chat_to_doc(
        self,
        conversation_id: str,
        doc_name: Optional[str] = None
    ) -> Dict:
        """
        Export chat conversation to a Google Doc
        
        Args:
            conversation_id: Conversation ID
            doc_name: Optional name for the document
        
        Returns:
            Dictionary with document information
        """
        try:
            from services.db_queries import get_conversation_history
            
            # Get conversation history
            history = get_conversation_history(conversation_id, limit=1000)
            
            if not history:
                raise ValidationError("No conversation history found", field="conversation_id")
            
            # Format as document
            lines = []
            if not doc_name:
                doc_name = f"Chat Export - {conversation_id[:8]}"
            
            lines.append(f"# {doc_name}\n")
            lines.append(f"Conversation ID: {conversation_id}\n\n")
            
            for i, chat in enumerate(history, 1):
                lines.append(f"## Message {i}\n")
                lines.append(f"**User:** {chat.get('message', '')}\n")
                lines.append(f"**Assistant:** {chat.get('response', '')}\n\n")
            
            content = "\n".join(lines)
            
            return await self.export_to_doc(content, doc_name)
        except (ValidationError, ServiceError):
            raise
        except Exception as e:
            logger.error(f"Error exporting chat to doc: {str(e)}")
            raise ServiceError(
                f"Failed to export chat to doc: {str(e)}",
                service_name="ExportService"
            )

