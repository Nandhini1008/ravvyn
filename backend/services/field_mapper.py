"""
Field Mapper Service - Maps column positions to meaningful field names
Provides proper field mapping for RO DETAILS and other tabs
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class FieldMapper:
    """
    Maps raw column data to meaningful field names based on tab structure
    """
    
    def __init__(self):
        """Initialize field mappings for different tabs"""
        
        # Field mappings for each tab - maps column index to field name
        self.tab_field_mappings = {
            'RO DETAILS': {
                0: 'DATE',
                1: 'TIME', 
                2: 'FILTER_FEED',
                3: 'UF_FEED_TANK_LEVEL',
                4: 'START_TIME',
                5: 'END_TIME', 
                6: 'RUN_TIME',
                7: 'RO_1_2_FEED_TANK_LEVEL',
                8: 'RO_1_PERMEATE',
                9: 'RO_2_PERMEATE',
                10: 'TOTAL_PERMEATE',
                11: 'RO_1_REJECT',
                12: 'RO_2_REJECT',
                13: 'TOTAL_REJECT',
                14: 'RECOVERY_PERCENTAGE',
                15: 'FEED_TDS',
                16: 'PERMEATE_TDS',
                17: 'REJECT_TDS',
                18: 'FEED_PRESSURE',
                19: 'PERMEATE_PRESSURE',
                20: 'REJECT_PRESSURE'
            },
            'costing': {
                0: 'DATE',
                1: 'ITEM',
                2: 'QUANTITY',
                3: 'UNIT_COST',
                4: 'TOTAL_COST',
                5: 'SUPPLIER',
                6: 'CATEGORY'
            },
            'CONDENSATE HOUR DETAILS': {
                0: 'DATE',
                1: 'TIME',
                2: 'CONDENSATE_FLOW',
                3: 'TEMPERATURE',
                4: 'PRESSURE',
                5: 'QUALITY_PARAMETER'
            },
            'RUNNING HRS': {
                0: 'DATE',
                1: 'EQUIPMENT',
                2: 'START_TIME',
                3: 'END_TIME',
                4: 'RUNNING_HOURS',
                5: 'STATUS'
            },
            'TANK DIPPING': {
                0: 'DATE',
                1: 'TANK_NAME',
                2: 'LEVEL_MM',
                3: 'VOLUME_LITERS',
                4: 'PERCENTAGE_FULL'
            },
            'EVENING REPORT': {
                0: 'DATE',
                1: 'SHIFT',
                2: 'OPERATOR',
                3: 'PRODUCTION',
                4: 'ISSUES',
                5: 'REMARKS'
            },
            'purchase details': {
                0: 'DATE',
                1: 'ITEM',
                2: 'VENDOR',
                3: 'QUANTITY',
                4: 'UNIT_PRICE',
                5: 'TOTAL_AMOUNT',
                6: 'STATUS'
            },
            'TDS DETAILS': {
                0: 'DATE',
                1: 'TIME',
                2: 'LOCATION',
                3: 'TDS_VALUE',
                4: 'TEMPERATURE',
                5: 'PH_VALUE'
            },
            'MEETING DETAILS': {
                0: 'DATE',
                1: 'TIME',
                2: 'ATTENDEES',
                3: 'AGENDA',
                4: 'DECISIONS',
                5: 'ACTION_ITEMS'
            },
            'Maintance work details': {
                0: 'DATE',
                1: 'EQUIPMENT',
                2: 'WORK_TYPE',
                3: 'DESCRIPTION',
                4: 'TECHNICIAN',
                5: 'STATUS',
                6: 'COST'
            }
        }
        
        # Common field aliases for search
        self.field_aliases = {
            'ro1&2 feed tank level': 'RO_1_2_FEED_TANK_LEVEL',
            'ro 1&2 feed tank level': 'RO_1_2_FEED_TANK_LEVEL',
            'ro feed tank level': 'RO_1_2_FEED_TANK_LEVEL',
            'feed tank level': 'RO_1_2_FEED_TANK_LEVEL',
            'amount': ['TOTAL_COST', 'TOTAL_AMOUNT', 'UNIT_COST', 'UNIT_PRICE'],
            'cost': ['TOTAL_COST', 'TOTAL_AMOUNT', 'UNIT_COST', 'UNIT_PRICE'],
            'price': ['UNIT_PRICE', 'TOTAL_AMOUNT'],
            'level': ['UF_FEED_TANK_LEVEL', 'RO_1_2_FEED_TANK_LEVEL', 'LEVEL_MM'],
            'time': ['TIME', 'START_TIME', 'END_TIME'],
            'date': ['DATE'],
            'tds': ['TDS_VALUE', 'FEED_TDS', 'PERMEATE_TDS', 'REJECT_TDS']
        }
    
    def map_row_to_fields(self, tab_name: str, row_data: List[Any]) -> Dict[str, Any]:
        """
        Map a raw row to field names
        
        Args:
            tab_name: Name of the tab
            row_data: Raw row data as list
            
        Returns:
            Dictionary mapping field names to values
        """
        if tab_name not in self.tab_field_mappings:
            # Return generic mapping for unknown tabs
            return {f'COLUMN_{i}': value for i, value in enumerate(row_data) if value}
        
        field_mapping = self.tab_field_mappings[tab_name]
        mapped_data = {}
        
        for i, value in enumerate(row_data):
            if i in field_mapping and value is not None and str(value).strip():
                field_name = field_mapping[i]
                mapped_data[field_name] = value
        
        return mapped_data
    
    def get_field_value(self, mapped_data: Dict[str, Any], field_query: str) -> Optional[Any]:
        """
        Get field value by name or alias
        
        Args:
            mapped_data: Mapped field data
            field_query: Field name or alias to search for
            
        Returns:
            Field value if found, None otherwise
        """
        field_query_lower = field_query.lower()
        
        # Direct field name match
        for field_name, value in mapped_data.items():
            if field_name.lower() == field_query_lower:
                return value
        
        # Check aliases
        if field_query_lower in self.field_aliases:
            alias_fields = self.field_aliases[field_query_lower]
            if isinstance(alias_fields, str):
                alias_fields = [alias_fields]
            
            for alias_field in alias_fields:
                if alias_field in mapped_data:
                    return mapped_data[alias_field]
        
        # Partial match
        for field_name, value in mapped_data.items():
            if field_query_lower in field_name.lower() or field_name.lower() in field_query_lower:
                return value
        
        return None
    
    def format_data_for_display(self, tab_name: str, rows_data: List[List[Any]], 
                               limit: int = 10) -> Dict[str, Any]:
        """
        Format multiple rows for user-friendly display
        
        Args:
            tab_name: Name of the tab
            rows_data: List of raw row data
            limit: Maximum number of rows to format
            
        Returns:
            Formatted data with field names and values
        """
        if not rows_data:
            return {
                "tab_name": tab_name,
                "message": "No data found",
                "rows": []
            }
        
        formatted_rows = []
        headers = None
        
        # Get headers from first row if it looks like headers
        first_row = rows_data[0] if rows_data else []
        if first_row and all(isinstance(val, str) and val.isupper() for val in first_row[:5] if val):
            headers = first_row
            data_rows = rows_data[1:limit+1]  # Skip header row
        else:
            data_rows = rows_data[:limit]
        
        for i, row_data in enumerate(data_rows):
            if headers:
                # Use actual headers from sheet
                mapped_data = {}
                for j, value in enumerate(row_data):
                    if j < len(headers) and headers[j] and value is not None and str(value).strip():
                        mapped_data[headers[j]] = value
            else:
                # Use predefined field mapping
                mapped_data = self.map_row_to_fields(tab_name, row_data)
            
            if mapped_data:  # Only include rows with data
                formatted_rows.append({
                    "row_number": i + 1,
                    "fields": mapped_data,
                    "raw_data": row_data  # Keep raw data for reference
                })
        
        return {
            "tab_name": tab_name,
            "total_rows": len(formatted_rows),
            "headers": headers or list(self.tab_field_mappings.get(tab_name, {}).values()),
            "rows": formatted_rows
        }
    
    def search_by_field(self, tab_name: str, rows_data: List[List[Any]], 
                       field_name: str, search_value: str) -> List[Dict[str, Any]]:
        """
        Search for rows containing a specific field value
        
        Args:
            tab_name: Name of the tab
            rows_data: List of raw row data
            field_name: Field name to search in
            search_value: Value to search for
            
        Returns:
            List of matching rows with field mappings
        """
        matching_rows = []
        
        for i, row_data in enumerate(rows_data):
            mapped_data = self.map_row_to_fields(tab_name, row_data)
            field_value = self.get_field_value(mapped_data, field_name)
            
            if field_value and search_value.lower() in str(field_value).lower():
                matching_rows.append({
                    "row_number": i + 1,
                    "fields": mapped_data,
                    "matched_field": field_name,
                    "matched_value": field_value
                })
        
        return matching_rows
    
    def get_available_fields(self, tab_name: str) -> List[str]:
        """Get list of available fields for a tab"""
        if tab_name in self.tab_field_mappings:
            return list(self.tab_field_mappings[tab_name].values())
        return []


# Global field mapper instance
_field_mapper = None

def get_field_mapper() -> FieldMapper:
    """Get the global field mapper instance"""
    global _field_mapper
    if _field_mapper is None:
        _field_mapper = FieldMapper()
    return _field_mapper