"""
Query Parser Service
Parses natural language queries to identify operations on sheets/docs
"""

import re
from typing import Dict, Optional, List, Tuple
from datetime import datetime, date, timedelta
import logging

logger = logging.getLogger(__name__)


class QueryParser:
    """Parse natural language queries to identify operations"""
    
    # Patterns for different operations
    UPDATE_PATTERNS = [
        r'update\s+(?:cell|row|column|value)',
        r'change\s+(?:cell|row|column|value)',
        r'set\s+(?:cell|row|column|value)',
        r'edit\s+(?:cell|row|column|value)',
        r'modify\s+(?:cell|row|column|value)',
        r'replace\s+(?:cell|row|column|value)',
    ]
    
    DELETE_PATTERNS = [
        r'delete\s+(?:row|rows|column|columns|cell|cells)',
        r'remove\s+(?:row|rows|column|columns|cell|cells)',
        r'clear\s+(?:row|rows|column|columns|cell|cells)',
    ]
    
    INSERT_PATTERNS = [
        r'insert\s+(?:row|rows)',
        r'add\s+(?:row|rows)',
        r'create\s+(?:row|rows)',
    ]
    
    READ_PATTERNS = [
        r'read\s+(?:cell|row|column|data|value)',
        r'get\s+(?:cell|row|column|data|value)',
        r'show\s+(?:cell|row|column|data|value)',
        r'find\s+(?:cell|row|column|data|value)',
        r'what\s+is',
        r'what\s+are',
    ]
    
    def parse_sheet_query(self, query: str) -> Dict:
        """
        Parse a natural language query about sheets
        
        Returns:
            Dictionary with operation type and parsed parameters
        """
        query_lower = query.lower().strip()
        
        # Detect operation type
        operation = self._detect_operation(query_lower)
        
        result = {
            'operation': operation,
            'query': query,
            'sheet_id': None,
            'tab_name': None,
            'row': None,
            'column': None,
            'value': None,
            'filters': {}
        }
        
        if operation == 'update':
            result.update(self._parse_update_query(query_lower))
        elif operation == 'delete':
            result.update(self._parse_delete_query(query_lower))
        elif operation == 'insert':
            result.update(self._parse_insert_query(query_lower))
        elif operation == 'read':
            result.update(self._parse_read_query(query_lower))
        
        return result
    
    def _detect_operation(self, query: str) -> str:
        """Detect the type of operation from query"""
        if any(re.search(pattern, query) for pattern in self.UPDATE_PATTERNS):
            return 'update'
        elif any(re.search(pattern, query) for pattern in self.DELETE_PATTERNS):
            return 'delete'
        elif any(re.search(pattern, query) for pattern in self.INSERT_PATTERNS):
            return 'insert'
        elif any(re.search(pattern, query) for pattern in self.READ_PATTERNS):
            return 'read'
        else:
            return 'query'  # General query
    
    def _parse_update_query(self, query: str) -> Dict:
        """Parse update query to extract parameters"""
        result = {}
        
        # Extract row number
        row_match = re.search(r'row\s+(\d+)', query)
        if row_match:
            result['row'] = int(row_match.group(1))
        
        # Extract column (letter or number)
        col_match = re.search(r'column\s+([A-Z]+|\d+)', query, re.IGNORECASE)
        if col_match:
            col = col_match.group(1)
            if col.isdigit():
                result['column'] = int(col)
            else:
                result['column'] = self._column_letter_to_number(col.upper())
        
        # Extract cell reference (e.g., A1, B5)
        cell_match = re.search(r'([A-Z]+)(\d+)', query, re.IGNORECASE)
        if cell_match:
            col_letter = cell_match.group(1).upper()
            row_num = int(cell_match.group(2))
            result['column'] = self._column_letter_to_number(col_letter)
            result['row'] = row_num
        
        # Extract value to set
        value_match = re.search(r'(?:to|with|as)\s+["\']?([^"\']+)["\']?', query)
        if not value_match:
            value_match = re.search(r'=\s*["\']?([^"\']+)["\']?', query)
        if value_match:
            result['value'] = value_match.group(1).strip()
        
        return result
    
    def _parse_delete_query(self, query: str) -> Dict:
        """Parse delete query to extract parameters"""
        result = {}
        
        # Extract row range
        row_match = re.search(r'row(?:s)?\s+(\d+)(?:\s+to\s+(\d+))?', query)
        if row_match:
            result['start_row'] = int(row_match.group(1))
            result['end_row'] = int(row_match.group(2)) if row_match.group(2) else result['start_row']
        
        # Extract column range
        col_match = re.search(r'column(?:s)?\s+([A-Z]+|\d+)(?:\s+to\s+([A-Z]+|\d+))?', query, re.IGNORECASE)
        if col_match:
            start_col = col_match.group(1)
            end_col = col_match.group(2) if col_match.group(2) else start_col
            
            if start_col.isdigit():
                result['start_column'] = int(start_col)
            else:
                result['start_column'] = self._column_letter_to_number(start_col.upper())
            
            if end_col.isdigit():
                result['end_column'] = int(end_col)
            else:
                result['end_column'] = self._column_letter_to_number(end_col.upper())
        
        return result
    
    def _parse_insert_query(self, query: str) -> Dict:
        """Parse insert query to extract parameters"""
        result = {}
        
        # Extract row index
        row_match = re.search(r'(?:at|before|after)\s+row\s+(\d+)', query)
        if row_match:
            result['row_index'] = int(row_match.group(1))
        
        # Extract number of rows
        num_match = re.search(r'(\d+)\s+row(?:s)?', query)
        if num_match:
            result['num_rows'] = int(num_match.group(1))
        else:
            result['num_rows'] = 1
        
        return result
    
    def _parse_read_query(self, query: str) -> Dict:
        """Parse read query to extract parameters"""
        result = {}
        
        # Extract row number
        row_match = re.search(r'row\s+(\d+)', query)
        if row_match:
            result['row'] = int(row_match.group(1))
        
        # Extract column
        col_match = re.search(r'column\s+([A-Z]+|\d+)', query, re.IGNORECASE)
        if col_match:
            col = col_match.group(1)
            if col.isdigit():
                result['column'] = int(col)
            else:
                result['column'] = self._column_letter_to_number(col.upper())
        
        # Extract cell reference
        cell_match = re.search(r'([A-Z]+)(\d+)', query, re.IGNORECASE)
        if cell_match:
            col_letter = cell_match.group(1).upper()
            row_num = int(cell_match.group(2))
            result['column'] = self._column_letter_to_number(col_letter)
            result['row'] = row_num
        
        # Extract date filters
        date_filters = self._extract_date_filters(query)
        if date_filters:
            result['filters'] = date_filters
        
        return result
    
    def _extract_date_filters(self, query: str) -> Optional[Dict]:
        """Extract date filters from query"""
        today = date.today()
        filters = {}
        
        if 'today' in query:
            filters['date'] = today.isoformat()
        elif 'yesterday' in query:
            filters['date'] = (today - timedelta(days=1)).isoformat()
        elif 'this week' in query:
            days_since_monday = today.weekday()
            week_start = today - timedelta(days=days_since_monday)
            filters['date'] = week_start.isoformat()
        elif 'last week' in query:
            days_since_monday = today.weekday()
            last_week_start = today - timedelta(days=days_since_monday + 7)
            filters['date'] = last_week_start.isoformat()
        
        return filters if filters else None
    
    def _column_letter_to_number(self, col_letter: str) -> int:
        """Convert column letter (A, B, C) to number (1, 2, 3)"""
        result = 0
        for char in col_letter:
            result = result * 26 + (ord(char) - ord('A') + 1)
        return result

