"""
Database Query Helpers
Helper functions for querying sheets, docs, and user context
"""

from sqlalchemy.orm import Session
from sqlalchemy import or_, and_, func, desc, String, cast
from datetime import datetime, date
from typing import List, Dict, Optional, Tuple
from services.database import (
    SheetsMetadata, SheetsData, DocsMetadata, DocsContent,
    ChatHistory, UserContext, ConversationContext, get_db_context
)
from core.exceptions import DatabaseError, NotFoundError
import re
import logging

logger = logging.getLogger(__name__)


def get_all_sheets(limit: int = 100, db: Optional[Session] = None) -> List[Dict]:
    """
    Get all sheets from database
    
    Args:
        limit: Maximum number of results
        db: Optional database session (if not provided, creates a new one)
    
    Returns:
        List of sheet metadata dictionaries
    """
    if db is not None:
        return _get_all_sheets_impl(limit, db)
    else:
        with get_db_context() as db:
            return _get_all_sheets_impl(limit, db)


def _get_all_sheets_impl(limit: int, db: Session) -> List[Dict]:
    """Internal implementation of get_all_sheets"""
    try:
        sheets = db.query(SheetsMetadata).filter(
            SheetsMetadata.sync_status == 'completed'
        ).order_by(desc(SheetsMetadata.modified_time)).limit(limit).all()
        
        result = [{
            'sheet_id': sheet.sheet_id,
            'sheet_name': sheet.sheet_name,
            'modified_time': sheet.modified_time.isoformat() if sheet.modified_time else None,
            'last_synced': sheet.last_synced.isoformat() if sheet.last_synced else None
        } for sheet in sheets]
        
        return result
    except Exception as e:
        logger.error(f"Error getting all sheets: {str(e)}")
        raise DatabaseError(f"Failed to get all sheets: {str(e)}", operation="get_all_sheets")


def find_relevant_sheets(query_text: str, limit: int = 5, db: Optional[Session] = None) -> List[Dict]:
    """
    Find relevant sheets based on query text (keyword matching)
    Returns list of sheet metadata dicts
    
    Args:
        query_text: Search query text
        limit: Maximum number of results
        db: Optional database session (if not provided, creates a new one)
    
    Returns:
        List of sheet metadata dictionaries
    """
    if db is not None:
        # Use provided session
        return _find_relevant_sheets_impl(query_text, limit, db)
    else:
        # Create new session context
        with get_db_context() as db:
            return _find_relevant_sheets_impl(query_text, limit, db)


def _find_relevant_sheets_impl(query_text: str, limit: int, db: Session) -> List[Dict]:
    """Internal implementation of find_relevant_sheets"""
    try:
        query_lower = query_text.lower()
        keywords = re.findall(r'\b\w+\b', query_lower)
        
        # Search in sheet names
        query = db.query(SheetsMetadata).filter(
            SheetsMetadata.sync_status == 'completed'
        )
        
        sheets = []
        if keywords:
            # First try to find sheets matching by name
            conditions = []
            for keyword in keywords:
                conditions.append(SheetsMetadata.sheet_name.ilike(f'%{keyword}%'))
            name_match_query = query.filter(or_(*conditions))
            sheets = name_match_query.order_by(desc(SheetsMetadata.modified_time)).limit(limit).all()
        
        # If no sheets match by name, return all sheets
        # (The tab-level relevance scoring will find the right data)
        if not sheets:
            sheets = query.order_by(desc(SheetsMetadata.modified_time)).limit(limit).all()
        
        result = [{
            'sheet_id': sheet.sheet_id,
            'sheet_name': sheet.sheet_name,
            'modified_time': sheet.modified_time.isoformat() if sheet.modified_time else None,
            'last_synced': sheet.last_synced.isoformat() if sheet.last_synced else None
        } for sheet in sheets]
        
        return result
    except Exception as e:
        logger.error(f"Error finding relevant sheets: {str(e)}")
        raise DatabaseError(f"Failed to find relevant sheets: {str(e)}", operation="find_relevant_sheets")


def find_relevant_docs(query_text: str, limit: int = 3, db: Optional[Session] = None) -> List[Dict]:
    """
    Find relevant docs based on query text (keyword matching)
    Returns list of doc metadata dicts
    
    Args:
        query_text: Search query text
        limit: Maximum number of results
        db: Optional database session (if not provided, creates a new one)
    
    Returns:
        List of doc metadata dictionaries
    """
    if db is not None:
        return _find_relevant_docs_impl(query_text, limit, db)
    else:
        with get_db_context() as db:
            return _find_relevant_docs_impl(query_text, limit, db)


def _find_relevant_docs_impl(query_text: str, limit: int, db: Session) -> List[Dict]:
    """Internal implementation of find_relevant_docs"""
    try:
        query_lower = query_text.lower()
        keywords = re.findall(r'\b\w+\b', query_lower)
        
        query = db.query(DocsMetadata).filter(
            DocsMetadata.sync_status == 'completed'
        )
        
        if keywords:
            conditions = []
            for keyword in keywords:
                conditions.append(DocsMetadata.doc_name.ilike(f'%{keyword}%'))
            query = query.filter(or_(*conditions))
        
        docs = query.order_by(desc(DocsMetadata.modified_time)).limit(limit).all()
        
        result = [{
            'doc_id': doc.doc_id,
            'doc_name': doc.doc_name,
            'modified_time': doc.modified_time.isoformat() if doc.modified_time else None,
            'last_synced': doc.last_synced.isoformat() if doc.last_synced else None
        } for doc in docs]
        
        return result
    except Exception as e:
        logger.error(f"Error finding relevant docs: {str(e)}")
        raise DatabaseError(f"Failed to find relevant docs: {str(e)}", operation="find_relevant_docs")


def get_sheet_data(sheet_id: str, tab_name: Optional[str] = None, 
                   filters: Optional[Dict] = None, limit: int = 1000,
                   db: Optional[Session] = None) -> List[Dict]:
    """
    Get sheet data with optional filters
    
    Args:
        sheet_id: ID of the sheet
        tab_name: Optional tab name to filter by
        filters: Optional dict with keys like {'date': '2025-01-20', 'column_name': 'value'}
        limit: Maximum number of rows to return (None = unlimited)
        db: Optional database session (if not provided, creates a new one)
    
    Returns:
        List of row dictionaries
    """
    if db is not None:
        return _get_sheet_data_impl(sheet_id, tab_name, filters, limit, db)
    else:
        with get_db_context() as db:
            return _get_sheet_data_impl(sheet_id, tab_name, filters, limit, db)


def search_sheet_data_by_date(sheet_id: str, tab_name: str, date_value: str,
                              db: Optional[Session] = None) -> List[Dict]:
    """
    Search for rows containing a specific date - UNLIMITED search in database
    This is much more efficient than loading all rows and filtering in memory
    
    Args:
        sheet_id: ID of the sheet
        tab_name: Tab name to search in
        date_value: Date to search for (e.g., "27.11.2025", "27/11/2025")
        db: Optional database session
    
    Returns:
        List of matching row dictionaries
    """
    if db is not None:
        return _search_sheet_data_by_date_impl(sheet_id, tab_name, date_value, db)
    else:
        with get_db_context() as db:
            return _search_sheet_data_by_date_impl(sheet_id, tab_name, date_value, db)


def search_sheet_data_by_date_range(sheet_id: str, tab_name: str, 
                                     month: Optional[int] = None,
                                     year: Optional[int] = None,
                                     start_date: Optional[str] = None,
                                     end_date: Optional[str] = None,
                                     db: Optional[Session] = None) -> List[Dict]:
    """
    Search for rows within a date range - UNLIMITED search in database
    Supports month/year ranges (e.g., November 2025) or specific date ranges
    
    Args:
        sheet_id: ID of the sheet
        tab_name: Tab name to search in
        month: Month number (1-12), e.g., 11 for November
        year: Year number, e.g., 2025
        start_date: Start date string (e.g., "01.11.2025")
        end_date: End date string (e.g., "30.11.2025")
        db: Optional database session
    
    Returns:
        List of matching row dictionaries
    """
    if db is not None:
        return _search_sheet_data_by_date_range_impl(sheet_id, tab_name, month, year, start_date, end_date, db)
    else:
        with get_db_context() as db:
            return _search_sheet_data_by_date_range_impl(sheet_id, tab_name, month, year, start_date, end_date, db)


def _search_sheet_data_by_date_range_impl(sheet_id: str, tab_name: str, 
                                          month: Optional[int] = None,
                                          year: Optional[int] = None,
                                          start_date: Optional[str] = None,
                                          end_date: Optional[str] = None,
                                          db: Session = None) -> List[Dict]:
    """
    Internal implementation - searches database for date ranges
    Supports month/year ranges (e.g., December 2025) or specific date ranges
    """
    try:
        import json
        
        # Get all rows for this sheet and tab
        query = db.query(SheetsData).filter(
            SheetsData.sheet_id == sheet_id,
            SheetsData.tab_name == tab_name
        )
        
        all_rows = query.order_by(SheetsData.row_index).all()
        
        result = []
        
        for row in all_rows:
            if not row.row_data:
                continue
                
            row_str = ' '.join(str(cell) for cell in row.row_data)
            
            # If searching by month/year (e.g., December 2025)
            if month and year:
                # Generate date patterns for all days in the month
                # Format: D/MM/YYYY or DD/MM/YYYY (e.g., 1/12/2025, 15/12/2025)
                from calendar import monthrange
                days_in_month = monthrange(year, month)[1]
                
                # Build patterns to search for
                patterns = []
                for day in range(1, days_in_month + 1):
                    # Try different formats: D/MM/YYYY, DD/MM/YYYY, D.MM.YYYY, DD.MM.YYYY
                    patterns.append(f"{day}/{month}/{year}")
                    patterns.append(f"{day}.{month}.{year}")
                    patterns.append(f"{day}-{month}-{year}")
                    if day < 10:
                        patterns.append(f"0{day}/{month}/{year}")
                        patterns.append(f"0{day}.{month}.{year}")
                        patterns.append(f"0{day}-{month}-{year}")
                
                # Also try with 2-digit year
                for day in range(1, days_in_month + 1):
                    short_year = str(year)[-2:]
                    patterns.append(f"{day}/{month}/{short_year}")
                    patterns.append(f"{day}.{month}.{short_year}")
                    if day < 10:
                        patterns.append(f"0{day}/{month}/{short_year}")
                        patterns.append(f"0{day}.{month}.{short_year}")
                
                # Check if any pattern matches
                if any(pattern in row_str for pattern in patterns):
                    result.append({
                        'row_index': row.row_index,
                        'data': row.row_data,
                        'tab_name': row.tab_name
                    })
            
            # If searching by specific date range
            elif start_date and end_date:
                # This would require parsing dates, which is more complex
                # For now, use simple string matching
                date_formats_start = [
                    start_date,
                    start_date.replace('/', '.'),
                    start_date.replace('.', '/'),
                ]
                date_formats_end = [
                    end_date,
                    end_date.replace('/', '.'),
                    end_date.replace('.', '/'),
                ]
                
                # Check if row contains any date in the range
                # This is a simplified check - full implementation would parse dates
                if any(df in row_str for df in date_formats_start) or any(df in row_str for df in date_formats_end):
                    result.append({
                        'row_index': row.row_index,
                        'data': row.row_data,
                        'tab_name': row.tab_name
                    })
        
        logger.info(f"Found {len(result)} rows matching date range (month={month}, year={year}) in {tab_name} (searched ALL rows)")
        return result
        
    except Exception as e:
        logger.error(f"Error searching sheet data by date range: {str(e)}", exc_info=True)
        return []


def _search_sheet_data_by_date_impl(sheet_id: str, tab_name: str, date_value: str, db: Session) -> List[Dict]:
    """
    Internal implementation - searches database directly using SQL LIKE queries
    This is UNLIMITED - searches ALL rows in the database efficiently
    """
    try:
        import json
        
        # Generate all possible date format variations
        date_formats = [
            date_value,
            date_value.replace('/', '.'),
            date_value.replace('.', '/'),
            date_value.replace('-', '.'),
            date_value.replace('-', '/'),
        ]
        
        # Remove duplicates
        date_formats = list(set(date_formats))
        
        # SQLite stores JSON as TEXT, so we can use LIKE on the JSON string
        # Build conditions for each date format
        conditions = []
        for date_fmt in date_formats:
            # SQLite JSON columns are stored as TEXT, so we can search directly
            # Use cast to ensure we're searching as string
            conditions.append(
                cast(SheetsData.row_data, String).like(f'%{date_fmt}%')
            )
        
        # Query ALL rows matching any date format - NO LIMIT
        if not conditions:
            return []
            
        rows = db.query(SheetsData).filter(
            SheetsData.sheet_id == sheet_id,
            SheetsData.tab_name == tab_name,
            or_(*conditions)
        ).order_by(SheetsData.row_index).all()
        
        # Convert to list of dicts
        result = []
        for row in rows:
            row_dict = {
                'row_index': row.row_index,
                'data': row.row_data,
                'tab_name': row.tab_name
            }
            result.append(row_dict)
        
        logger.info(f"Found {len(result)} rows matching date {date_value} in {tab_name} (searched ALL rows)")
        return result
        
    except Exception as e:
        logger.error(f"Error searching sheet data by date: {str(e)}", exc_info=True)
        # Fallback: get all rows and filter in Python (less efficient but works)
        logger.warning(f"Falling back to Python-based search")
        try:
            all_rows = db.query(SheetsData).filter(
                SheetsData.sheet_id == sheet_id,
                SheetsData.tab_name == tab_name
            ).order_by(SheetsData.row_index).all()
            
            result = []
            for row in all_rows:
                row_str = ' '.join(str(cell) for cell in row.row_data)
                if any(df in row_str for df in date_formats):
                    result.append({
                        'row_index': row.row_index,
                        'data': row.row_data,
                        'tab_name': row.tab_name
                    })
            logger.info(f"Found {len(result)} rows matching date {date_value} (fallback method)")
            return result
        except Exception as e2:
            logger.error(f"Fallback search also failed: {str(e2)}")
            return []


def _get_sheet_data_impl(sheet_id: str, tab_name: Optional[str], 
                         filters: Optional[Dict], limit: int, db: Session) -> List[Dict]:
    """Internal implementation of get_sheet_data"""
    try:
        query = db.query(SheetsData).filter(
            SheetsData.sheet_id == sheet_id
        )
        
        if tab_name:
            query = query.filter(SheetsData.tab_name == tab_name)
        
        # CRITICAL FIX: Get header row + RECENT data (not old data from beginning)
        # Step 1: Get header row (row_index = 0)
        header_query = query.filter(SheetsData.row_index == 0)
        header_row = header_query.first()
        
        # Step 2: Get recent data (last N rows, excluding header)
        # Order by row_index DESC to get the most recent rows
        data_query = query.filter(SheetsData.row_index > 0).order_by(SheetsData.row_index.desc()).limit(limit - 1)
        recent_rows = data_query.all()
        
        # Combine: header first, then recent rows in chronological order
        rows = []
        if header_row:
            rows.append(header_row)
        # Reverse recent_rows to get chronological order (oldest to newest of the recent data)
        rows.extend(reversed(recent_rows))
        
        # Convert to list of dicts
        result = []
        for row in rows:
            row_dict = {
                'row_index': row.row_index,
                'data': row.row_data,
                'tab_name': row.tab_name
            }
            result.append(row_dict)
        
        # Apply filters if provided
        if filters and result:
            filtered_result = []
            for row in result:
                if _matches_filters(row['data'], filters):
                    filtered_result.append(row)
            return filtered_result
        
        return result
    except Exception as e:
        logger.error(f"Error getting sheet data: {str(e)}")
        raise DatabaseError(f"Failed to get sheet data: {str(e)}", operation="get_sheet_data")


def _matches_filters(row_data: List, filters: Dict) -> bool:
    """Check if a row matches the given filters"""
    if not row_data:
        return False
    
    # Simple filter matching - check if any cell contains filter values
    row_str = ' '.join(str(cell) for cell in row_data).lower()
    
    for key, value in filters.items():
        value_str = str(value).lower()
        # For date matching, try multiple formats
        if key == 'date' or 'date' in key.lower():
            # Try different date formats: 27.11.2025, 27/11/2025, 27-11-2025, etc.
            date_formats = [
                value_str,
                value_str.replace('/', '.'),
                value_str.replace('.', '/'),
                value_str.replace('-', '.'),
                value_str.replace('-', '/'),
            ]
            if not any(df in row_str for df in date_formats):
                return False
        else:
            if value_str not in row_str:
                return False
    
    return True


def get_doc_content(doc_id: str, db: Optional[Session] = None) -> Optional[str]:
    """
    Get full content of a document
    
    Args:
        doc_id: ID of the document
        db: Optional database session (if not provided, creates a new one)
    
    Returns:
        Document content as string, or None if not found
    """
    if db is not None:
        return _get_doc_content_impl(doc_id, db)
    else:
        with get_db_context() as db:
            return _get_doc_content_impl(doc_id, db)


def _get_doc_content_impl(doc_id: str, db: Session) -> Optional[str]:
    """Internal implementation of get_doc_content"""
    try:
        doc_content = db.query(DocsContent).filter(
            DocsContent.doc_id == doc_id
        ).first()
        
        if doc_content:
            return doc_content.content
        return None
    except Exception as e:
        logger.error(f"Error getting doc content: {str(e)}")
        raise DatabaseError(f"Failed to get doc content: {str(e)}", operation="get_doc_content")


def get_user_context(user_id: str, db: Optional[Session] = None) -> Dict:
    """
    Get user's last used sheets/docs context
    
    Args:
        user_id: User identifier
        db: Optional database session (if not provided, creates a new one)
    
    Returns:
        Dictionary with user context information
    """
    if db is not None:
        return _get_user_context_impl(user_id, db)
    else:
        with get_db_context() as db:
            return _get_user_context_impl(user_id, db)


def _get_user_context_impl(user_id: str, db: Session) -> Dict:
    """Internal implementation of get_user_context"""
    try:
        context = db.query(UserContext).filter(
            UserContext.user_id == user_id
        ).first()
        
        if context:
            return {
                'last_sheet_id': context.last_sheet_id,
                'last_sheet_name': context.last_sheet_name,
                'last_tab_name': context.last_tab_name,
                'last_doc_id': context.last_doc_id,
                'last_doc_name': context.last_doc_name,
                'updated_at': context.updated_at.isoformat() if context.updated_at else None
            }
        return {}
    except Exception as e:
        logger.error(f"Error getting user context: {str(e)}")
        raise DatabaseError(f"Failed to get user context: {str(e)}", operation="get_user_context")


def update_user_context(user_id: str, sheet_id: Optional[str] = None,
                       sheet_name: Optional[str] = None, tab_name: Optional[str] = None,
                       doc_id: Optional[str] = None, doc_name: Optional[str] = None,
                       db: Optional[Session] = None):
    """
    Update user context with last used sheet/doc
    
    Args:
        user_id: User identifier
        sheet_id: Optional sheet ID
        sheet_name: Optional sheet name
        tab_name: Optional tab name
        doc_id: Optional document ID
        doc_name: Optional document name
        db: Optional database session (if not provided, creates a new one)
    """
    if db is not None:
        _update_user_context_impl(user_id, sheet_id, sheet_name, tab_name, doc_id, doc_name, db)
    else:
        with get_db_context() as db:
            _update_user_context_impl(user_id, sheet_id, sheet_name, tab_name, doc_id, doc_name, db)


def _update_user_context_impl(user_id: str, sheet_id: Optional[str],
                              sheet_name: Optional[str], tab_name: Optional[str],
                              doc_id: Optional[str], doc_name: Optional[str], db: Session):
    """Internal implementation of update_user_context"""
    try:
        context = db.query(UserContext).filter(
            UserContext.user_id == user_id
        ).first()
        
        if not context:
            context = UserContext(user_id=user_id)
            db.add(context)
        
        if sheet_id:
            context.last_sheet_id = sheet_id
        if sheet_name:
            context.last_sheet_name = sheet_name
        if tab_name:
            context.last_tab_name = tab_name
        if doc_id:
            context.last_doc_id = doc_id
        if doc_name:
            context.last_doc_name = doc_name
        
        context.updated_at = datetime.utcnow()
    except Exception as e:
        logger.error(f"Error updating user context: {str(e)}")
        raise DatabaseError(f"Failed to update user context: {str(e)}", operation="update_user_context")


def get_tab_metadata(sheet_id: str, tab_name: str, db: Optional[Session] = None) -> Dict:
    """
    Get metadata for a specific tab including date range
    Returns: dict with tab_name, row_count, date_range (min/max dates found)
    """
    if db is not None:
        return _get_tab_metadata_impl(sheet_id, tab_name, db)
    else:
        with get_db_context() as db:
            return _get_tab_metadata_impl(sheet_id, tab_name, db)


def _get_tab_metadata_impl(sheet_id: str, tab_name: str, db: Session) -> Dict:
    """Internal implementation of get_tab_metadata"""
    try:
        # Get row count
        row_count = db.query(func.count(SheetsData.id)).filter(
            SheetsData.sheet_id == sheet_id,
            SheetsData.tab_name == tab_name
        ).scalar() or 0
        
        # Try to find date range by looking at first column
        date_range = {"min_date": None, "max_date": None}
        
        # Get all rows (excluding header row 0) ordered by row_index
        rows = db.query(SheetsData).filter(
            SheetsData.sheet_id == sheet_id,
            SheetsData.tab_name == tab_name,
            SheetsData.row_index > 0
        ).order_by(SheetsData.row_index).all()
        
        dates = []
        for row in rows:
            if row.row_data and len(row.row_data) > 0:
                first_col = str(row.row_data[0]).strip()
                # Check if it looks like a date (contains . and numbers)
                if '.' in first_col and any(char.isdigit() for char in first_col):
                    # Try to parse various date formats
                    if '2025' in first_col or '2024' in first_col:
                        dates.append(first_col)
        
        if dates:
            date_range["min_date"] = dates[0] if dates else None
            date_range["max_date"] = dates[-1] if dates else None
        
        return {
            "tab_name": tab_name,
            "row_count": row_count,
            "date_range": date_range
        }
    except Exception as e:
        logger.warning(f"Error getting tab metadata for {tab_name}: {str(e)}")
        return {
            "tab_name": tab_name,
            "row_count": 0,
            "date_range": {"min_date": None, "max_date": None}
        }


def get_sheet_tabs(sheet_id: str, db: Optional[Session] = None) -> List[str]:
    """
    Get list of all tabs in a sheet
    
    Args:
        sheet_id: ID of the sheet
        db: Optional database session (if not provided, creates a new one)
    
    Returns:
        List of tab names
    """
    if db is not None:
        return _get_sheet_tabs_impl(sheet_id, db)
    else:
        with get_db_context() as db:
            return _get_sheet_tabs_impl(sheet_id, db)


def _get_sheet_tabs_impl(sheet_id: str, db: Session) -> List[str]:
    """Internal implementation of get_sheet_tabs"""
    try:
        tabs = db.query(SheetsData.tab_name).filter(
            SheetsData.sheet_id == sheet_id
        ).distinct().all()
        
        return [tab[0] for tab in tabs]
    except Exception as e:
        logger.error(f"Error getting sheet tabs: {str(e)}")
        raise DatabaseError(f"Failed to get sheet tabs: {str(e)}", operation="get_sheet_tabs")


def save_chat_history(user_id: str, message: str, response: str,
                     query_type: Optional[str] = None,
                     context_used: Optional[Dict] = None,
                     sheet_id: Optional[str] = None,
                     doc_id: Optional[str] = None,
                     conversation_id: Optional[str] = None,
                     db: Optional[Session] = None):
    """
    Save chat history to database
    
    Args:
        user_id: User identifier
        message: User message
        response: AI response
        query_type: Type of query (optional)
        context_used: Context information (optional)
        sheet_id: Sheet ID if used (optional)
        doc_id: Document ID if used (optional)
        conversation_id: Conversation ID for grouping (optional)
        db: Optional database session (if not provided, creates a new one)
    """
    if db is not None:
        _save_chat_history_impl(user_id, message, response, query_type, context_used, sheet_id, doc_id, conversation_id, db)
    else:
        with get_db_context() as db:
            _save_chat_history_impl(user_id, message, response, query_type, context_used, sheet_id, doc_id, conversation_id, db)


def _save_chat_history_impl(user_id: str, message: str, response: str,
                            query_type: Optional[str], context_used: Optional[Dict],
                            sheet_id: Optional[str], doc_id: Optional[str],
                            conversation_id: Optional[str], db: Session):
    """Internal implementation of save_chat_history"""
    try:
        chat = ChatHistory(
            user_id=user_id,
            conversation_id=conversation_id,
            message=message,
            response=response,
            query_type=query_type,
            context_used=context_used,
            sheet_id=sheet_id,
            doc_id=doc_id
        )
        db.add(chat)
    except Exception as e:
        logger.error(f"Error saving chat history: {str(e)}")
        raise DatabaseError(f"Failed to save chat history: {str(e)}", operation="save_chat_history")


def get_recent_chat_history(user_id: str, limit: int = 10, db: Optional[Session] = None) -> List[Dict]:
    """
    Get recent chat history for a user
    
    Args:
        user_id: User identifier
        limit: Maximum number of chat entries to return
        db: Optional database session (if not provided, creates a new one)
    
    Returns:
        List of chat history dictionaries
    """
    if db is not None:
        return _get_recent_chat_history_impl(user_id, limit, db)
    else:
        with get_db_context() as db:
            return _get_recent_chat_history_impl(user_id, limit, db)


def _get_recent_chat_history_impl(user_id: str, limit: int, db: Session) -> List[Dict]:
    """Internal implementation of get_recent_chat_history"""
    try:
        chats = db.query(ChatHistory).filter(
            ChatHistory.user_id == user_id
        ).order_by(desc(ChatHistory.created_at)).limit(limit).all()
        
        return [{
            'message': chat.message,
            'response': chat.response,
            'query_type': chat.query_type,
            'created_at': chat.created_at.isoformat() if chat.created_at else None
        } for chat in chats]
    except Exception as e:
        logger.error(f"Error getting recent chat history: {str(e)}")
        raise DatabaseError(f"Failed to get recent chat history: {str(e)}", operation="get_recent_chat_history")


def get_or_create_conversation_id(user_id: str, db: Optional[Session] = None) -> str:
    """
    Get or create a conversation ID for the user
    
    Args:
        user_id: User identifier
        db: Optional database session
    
    Returns:
        Conversation ID string
    """
    if db is not None:
        return _get_or_create_conversation_id_impl(user_id, db)
    else:
        with get_db_context() as db:
            return _get_or_create_conversation_id_impl(user_id, db)


def _get_or_create_conversation_id_impl(user_id: str, db: Session) -> str:
    """Internal implementation"""
    import uuid
    try:
        # Get the most recent conversation for this user
        recent_chat = db.query(ChatHistory).filter(
            ChatHistory.user_id == user_id
        ).order_by(desc(ChatHistory.created_at)).first()
        
        if recent_chat and recent_chat.conversation_id:
            # Check if conversation is recent (within last hour)
            from datetime import timedelta
            if recent_chat.created_at and (datetime.utcnow() - recent_chat.created_at) < timedelta(hours=1):
                return recent_chat.conversation_id
        
        # Create new conversation ID
        return str(uuid.uuid4())
    except Exception as e:
        logger.error(f"Error getting conversation ID: {str(e)}")
        # Return new ID on error
        return str(uuid.uuid4())


def get_conversation_context(conversation_id: str, user_id: str, db: Optional[Session] = None) -> Dict:
    """
    Get conversation context
    
    Args:
        conversation_id: Conversation identifier
        user_id: User identifier
        db: Optional database session
    
    Returns:
        Dictionary with conversation context
    """
    if db is not None:
        return _get_conversation_context_impl(conversation_id, user_id, db)
    else:
        with get_db_context() as db:
            return _get_conversation_context_impl(conversation_id, user_id, db)


def _get_conversation_context_impl(conversation_id: str, user_id: str, db: Session) -> Dict:
    """Internal implementation"""
    try:
        context = db.query(ConversationContext).filter(
            ConversationContext.conversation_id == conversation_id,
            ConversationContext.user_id == user_id
        ).first()
        
        if context:
            return {
                'conversation_id': context.conversation_id,
                'active_sheet_id': context.active_sheet_id,
                'active_doc_id': context.active_doc_id,
                'active_filters': context.active_filters or {},
                'recent_operations': context.recent_operations or [],
                'context_summary': context.context_summary,
                'updated_at': context.updated_at.isoformat() if context.updated_at else None
            }
        return {}
    except Exception as e:
        logger.error(f"Error getting conversation context: {str(e)}")
        return {}


def update_conversation_context(
    conversation_id: str,
    user_id: str,
    active_sheet_id: Optional[str] = None,
    active_doc_id: Optional[str] = None,
    active_filters: Optional[Dict] = None,
    recent_operations: Optional[List] = None,
    context_summary: Optional[str] = None,
    db: Optional[Session] = None
):
    """
    Update conversation context
    
    Args:
        conversation_id: Conversation identifier
        user_id: User identifier
        active_sheet_id: Active sheet ID
        active_doc_id: Active document ID
        active_filters: Active filters
        recent_operations: Recent operations list
        context_summary: Context summary
        db: Optional database session
    """
    if db is not None:
        _update_conversation_context_impl(
            conversation_id, user_id, active_sheet_id, active_doc_id,
            active_filters, recent_operations, context_summary, db
        )
    else:
        with get_db_context() as db:
            _update_conversation_context_impl(
                conversation_id, user_id, active_sheet_id, active_doc_id,
                active_filters, recent_operations, context_summary, db
            )


def _update_conversation_context_impl(
    conversation_id: str,
    user_id: str,
    active_sheet_id: Optional[str],
    active_doc_id: Optional[str],
    active_filters: Optional[Dict],
    recent_operations: Optional[List],
    context_summary: Optional[str],
    db: Session
):
    """Internal implementation"""
    try:
        context = db.query(ConversationContext).filter(
            ConversationContext.conversation_id == conversation_id,
            ConversationContext.user_id == user_id
        ).first()
        
        if not context:
            context = ConversationContext(
                conversation_id=conversation_id,
                user_id=user_id
            )
            db.add(context)
        
        if active_sheet_id is not None:
            context.active_sheet_id = active_sheet_id
        if active_doc_id is not None:
            context.active_doc_id = active_doc_id
        if active_filters is not None:
            context.active_filters = active_filters
        if recent_operations is not None:
            # Keep only last 10 operations
            context.recent_operations = recent_operations[-10:]
        if context_summary is not None:
            context.context_summary = context_summary
        
        context.updated_at = datetime.utcnow()
    except Exception as e:
        logger.error(f"Error updating conversation context: {str(e)}")
        raise DatabaseError(
            f"Failed to update conversation context: {str(e)}",
            operation="update_conversation_context"
        )


def get_conversation_history(conversation_id: str, limit: int = 20, db: Optional[Session] = None) -> List[Dict]:
    """
    Get chat history for a specific conversation
    
    Args:
        conversation_id: Conversation identifier
        limit: Maximum number of messages
        db: Optional database session
    
    Returns:
        List of chat messages
    """
    if db is not None:
        return _get_conversation_history_impl(conversation_id, limit, db)
    else:
        with get_db_context() as db:
            return _get_conversation_history_impl(conversation_id, limit, db)


def _get_conversation_history_impl(conversation_id: str, limit: int, db: Session) -> List[Dict]:
    """Internal implementation"""
    try:
        chats = db.query(ChatHistory).filter(
            ChatHistory.conversation_id == conversation_id
        ).order_by(ChatHistory.created_at).limit(limit).all()
        
        return [{
            'message': chat.message,
            'response': chat.response,
            'query_type': chat.query_type,
            'created_at': chat.created_at.isoformat() if chat.created_at else None
        } for chat in chats]
    except Exception as e:
        logger.error(f"Error getting conversation history: {str(e)}")
        raise DatabaseError(
            f"Failed to get conversation history: {str(e)}",
            operation="get_conversation_history"
        )

