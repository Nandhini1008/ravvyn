"""
Smart Field Mapper Service - Automatically detects headers and maps fields dynamically
Works with any sheet structure by analyzing the actual data
"""

import logging
import re
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


class SmartFieldMapper:
    """
    Automatically detects and maps fields based on actual sheet data structure
    """
    
    def __init__(self):
        """Initialize smart field mapper"""
        
        # Common field name patterns for normalization
        self.field_patterns = {
            'DATE': [r'date', r'dt', r'day'],
            'TIME': [r'time', r'tm', r'hour'],
            'AMOUNT': [r'amount', r'cost', r'price', r'total', r'sum'],
            'LEVEL': [r'level', r'lvl', r'tank.*level', r'feed.*tank'],
            'FEED': [r'feed', r'supply'],
            'TANK': [r'tank', r'reservoir'],
            'PRESSURE': [r'pressure', r'press'],
            'TEMPERATURE': [r'temp', r'temperature'],
            'TDS': [r'tds', r'total.*dissolved.*solids'],
            'FLOW': [r'flow', r'rate'],
            'PERMEATE': [r'permeate', r'product'],
            'REJECT': [r'reject', r'waste', r'brine'],
            'RECOVERY': [r'recovery', r'efficiency'],
            'RUN_TIME': [r'run.*time', r'runtime', r'duration'],
            'START_TIME': [r'start.*time', r'begin'],
            'END_TIME': [r'end.*time', r'finish'],
            'REMARKS': [r'remarks', r'notes', r'comments'],
            'STATUS': [r'status', r'state', r'condition']
        }
        
        # Field aliases for search
        self.field_aliases = {
            'ro1&2 feed tank level': ['RO_1_2_FEED_TANK_LEVEL', 'RO1&2_FEED_TANK_LEVEL', 'FEED_TANK_LEVEL'],
            'ro feed tank level': ['RO_1_2_FEED_TANK_LEVEL', 'RO1&2_FEED_TANK_LEVEL', 'FEED_TANK_LEVEL'],
            'feed tank level': ['FEED_TANK_LEVEL', 'UF_FEED_TANK_LEVEL', 'RO_1_2_FEED_TANK_LEVEL'],
            'tank level': ['TANK_LEVEL', 'FEED_TANK_LEVEL', 'UF_FEED_TANK_LEVEL'],
            'amount': ['AMOUNT', 'TOTAL_COST', 'TOTAL_AMOUNT', 'UNIT_COST', 'UNIT_PRICE'],
            'cost': ['COST', 'TOTAL_COST', 'UNIT_COST', 'PRICE'],
            'level': ['LEVEL', 'TANK_LEVEL', 'FEED_TANK_LEVEL'],
            'time': ['TIME', 'START_TIME', 'END_TIME'],
            'date': ['DATE'],
            'tds': ['TDS', 'TDS_VALUE', 'FEED_TDS', 'PERMEATE_TDS']
        }
    
    def detect_header_row(self, rows_data: List[List[Any]]) -> Tuple[Optional[int], Optional[List[str]]]:
        """
        Detect which row contains headers
        
        Args:
            rows_data: List of row data
            
        Returns:
            Tuple of (header_row_index, header_list) or (None, None)
        """
        if not rows_data:
            return None, None
        
        # Check first few rows for headers
        for i, row in enumerate(rows_data[:3]):
            if not row:
                continue
            
            # Count text vs numeric values
            text_count = 0
            numeric_count = 0
            empty_count = 0
            
            for cell in row:
                if not cell or str(cell).strip() == '':
                    empty_count += 1
                elif self._is_numeric(cell):
                    numeric_count += 1
                else:
                    text_count += 1
            
            # Header row should have more text than numbers and fewer empty cells
            total_cells = len(row)
            if total_cells > 0:
                text_ratio = text_count / total_cells
                empty_ratio = empty_count / total_cells
                
                # Likely header if mostly text and not too many empty cells
                if text_ratio > 0.5 and empty_ratio < 0.7:
                    headers = [self._normalize_header(str(cell)) for cell in row]
                    logger.info(f"Detected header row at index {i}: {headers[:5]}...")
                    return i, headers
        
        return None, None
    
    def _normalize_header(self, header: str) -> str:
        """Normalize header text to standard field name"""
        if not header:
            return ""
        
        # Clean up the header
        normalized = str(header).strip().upper()
        normalized = re.sub(r'[^\w\s&]', '', normalized)  # Remove special chars except &
        normalized = re.sub(r'\s+', '_', normalized)  # Replace spaces with underscores
        
        # Handle special cases
        if 'RO' in normalized and 'FEED' in normalized and 'TANK' in normalized:
            if '1' in normalized and '2' in normalized:
                return 'RO_1_2_FEED_TANK_LEVEL'
            elif '3' in normalized:
                return 'RO_3_FEED_TANK_LEVEL'
            else:
                return 'RO_FEED_TANK_LEVEL'
        
        return normalized
    
    def _is_numeric(self, value: Any) -> bool:
        """Check if a value is numeric"""
        if value is None:
            return False
        
        try:
            str_val = str(value).strip()
            if not str_val:
                return False
            
            # Remove common formatting
            clean_val = str_val.replace(',', '').replace('$', '').replace('%', '')
            
            # Check for time format (HH:MM)
            if ':' in clean_val and len(clean_val.split(':')) == 2:
                parts = clean_val.split(':')
                return all(part.isdigit() for part in parts)
            
            # Check for date format (DD.MM.YY or similar)
            if '.' in clean_val or '/' in clean_val or '-' in clean_val:
                # Could be date or decimal number
                try:
                    float(clean_val)
                    return True
                except ValueError:
                    return False
            
            float(clean_val)
            return True
        except (ValueError, TypeError):
            return False
    
    def auto_map_fields(self, tab_name: str, rows_data: List[List[Any]]) -> Dict[str, Any]:
        """
        Automatically detect and map fields for any tab
        
        Args:
            tab_name: Name of the tab
            rows_data: Raw row data
            
        Returns:
            Dictionary with mapping information
        """
        if not rows_data:
            return {
                'header_row': None,
                'headers': [],
                'field_mapping': {},
                'data_rows': []
            }
        
        # Detect header row
        header_row_idx, headers = self.detect_header_row(rows_data)
        
        # Create field mapping
        field_mapping = {}
        if headers:
            for i, header in enumerate(headers):
                if header and header.strip():
                    field_mapping[i] = header
        else:
            # Fallback: create generic column names
            max_cols = max(len(row) for row in rows_data if row)
            for i in range(max_cols):
                field_mapping[i] = f'COLUMN_{i}'
        
        # Identify data rows (skip header row)
        data_start_idx = (header_row_idx + 1) if header_row_idx is not None else 0
        data_rows = rows_data[data_start_idx:] if data_start_idx < len(rows_data) else []
        
        # Filter out empty rows
        data_rows = [row for row in data_rows if row and any(cell for cell in row)]
        
        logger.info(f"Auto-mapped {tab_name}: {len(headers or [])} headers, {len(data_rows)} data rows")
        
        return {
            'header_row': header_row_idx,
            'headers': headers or [],
            'field_mapping': field_mapping,
            'data_rows': data_rows,
            'total_rows': len(rows_data)
        }
    
    def map_row_to_fields(self, tab_name: str, row_data: List[Any], 
                         field_mapping: Dict[int, str] = None) -> Dict[str, Any]:
        """
        Map a single row to field names
        
        Args:
            tab_name: Name of the tab
            row_data: Raw row data
            field_mapping: Optional pre-computed field mapping
            
        Returns:
            Dictionary mapping field names to values
        """
        if not row_data:
            return {}
        
        # Use provided mapping or auto-detect
        if field_mapping is None:
            mapping_info = self.auto_map_fields(tab_name, [row_data])
            field_mapping = mapping_info['field_mapping']
        
        mapped_data = {}
        for i, value in enumerate(row_data):
            if i in field_mapping and value is not None and str(value).strip():
                field_name = field_mapping[i]
                mapped_data[field_name] = value
        
        return mapped_data
    
    def get_field_value(self, mapped_data: Dict[str, Any], field_query: str) -> Optional[Any]:
        """
        Get field value by name or alias with fuzzy matching
        
        Args:
            mapped_data: Mapped field data
            field_query: Field name or alias to search for
            
        Returns:
            Field value if found, None otherwise
        """
        field_query_lower = field_query.lower().strip()
        
        # Direct field name match
        for field_name, value in mapped_data.items():
            if field_name.lower() == field_query_lower:
                return value
        
        # Check aliases
        if field_query_lower in self.field_aliases:
            alias_fields = self.field_aliases[field_query_lower]
            for alias_field in alias_fields:
                for field_name, value in mapped_data.items():
                    if field_name.upper() == alias_field.upper():
                        return value
        
        # Fuzzy matching - partial match
        for field_name, value in mapped_data.items():
            field_name_lower = field_name.lower()
            
            # Check if query is contained in field name or vice versa
            if (field_query_lower in field_name_lower or 
                field_name_lower in field_query_lower or
                self._fuzzy_match(field_query_lower, field_name_lower)):
                return value
        
        return None
    
    def _fuzzy_match(self, query: str, field_name: str) -> bool:
        """Check if query fuzzy matches field name"""
        # Remove common words and check key terms
        query_words = set(re.findall(r'\w+', query.lower()))
        field_words = set(re.findall(r'\w+', field_name.lower()))
        
        # Remove common words
        common_words = {'the', 'and', 'or', 'of', 'in', 'on', 'at', 'to', 'for'}
        query_words -= common_words
        field_words -= common_words
        
        if not query_words or not field_words:
            return False
        
        # Check if significant overlap
        overlap = len(query_words & field_words)
        return overlap >= min(len(query_words), len(field_words)) * 0.5
    
    def find_latest_data_row(self, rows_data: List[List[Any]], 
                           field_mapping: Dict[int, str] = None) -> Optional[Dict[str, Any]]:
        """
        Find the most recent data entry (not header row)
        
        Args:
            rows_data: List of row data
            field_mapping: Optional field mapping
            
        Returns:
            Latest data row with field mapping or None
        """
        if not rows_data:
            return None
        
        # Auto-detect structure if no mapping provided
        if field_mapping is None:
            mapping_info = self.auto_map_fields("", rows_data)
            field_mapping = mapping_info['field_mapping']
            data_rows = mapping_info['data_rows']
        else:
            # Skip first row if it looks like headers
            header_idx, _ = self.detect_header_row(rows_data)
            start_idx = (header_idx + 1) if header_idx is not None else 0
            data_rows = rows_data[start_idx:]
        
        # Find the most recent non-empty data row
        for row in reversed(data_rows):  # Start from the end (most recent)
            if row and any(cell for cell in row):  # Non-empty row
                mapped_row = self.map_row_to_fields("", row, field_mapping)
                if mapped_row:  # Has actual data
                    return {
                        'fields': mapped_row,
                        'raw_data': row,
                        'field_mapping': field_mapping
                    }
        
        return None
    
    def search_by_field_value(self, rows_data: List[List[Any]], field_name: str, 
                            search_value: str, field_mapping: Dict[int, str] = None) -> List[Dict[str, Any]]:
        """
        Search for rows containing a specific field value
        
        Args:
            rows_data: List of row data
            field_name: Field name to search in
            search_value: Value to search for
            field_mapping: Optional field mapping
            
        Returns:
            List of matching rows with field mappings
        """
        if not rows_data:
            return []
        
        # Auto-detect structure if no mapping provided
        if field_mapping is None:
            mapping_info = self.auto_map_fields("", rows_data)
            field_mapping = mapping_info['field_mapping']
            data_rows = mapping_info['data_rows']
        else:
            # Skip header row
            header_idx, _ = self.detect_header_row(rows_data)
            start_idx = (header_idx + 1) if header_idx is not None else 0
            data_rows = rows_data[start_idx:]
        
        matching_rows = []
        search_value_lower = search_value.lower()
        
        for i, row in enumerate(data_rows):
            if not row:
                continue
            
            mapped_data = self.map_row_to_fields("", row, field_mapping)
            field_value = self.get_field_value(mapped_data, field_name)
            
            if field_value and search_value_lower in str(field_value).lower():
                matching_rows.append({
                    "row_number": i + 1,
                    "fields": mapped_data,
                    "raw_data": row,
                    "matched_field": field_name,
                    "matched_value": field_value
                })
        
        return matching_rows


# Global smart field mapper instance
_smart_field_mapper = None

def get_smart_field_mapper() -> SmartFieldMapper:
    """Get the global smart field mapper instance"""
    global _smart_field_mapper
    if _smart_field_mapper is None:
        _smart_field_mapper = SmartFieldMapper()
    return _smart_field_mapper