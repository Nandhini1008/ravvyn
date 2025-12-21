"""
Data Retrieval Service - Efficiently retrieve and format data for LLM processing
Handles the JSON array data structure stored in sheets_content table
"""

import json
import logging
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text
from services.database import get_db_context, SheetsData, SheetsMetadata
from services.smart_field_mapper import get_smart_field_mapper
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class DataRetrievalService:
    """
    Service to retrieve and format sheet data for LLM processing.
    Handles the JSON array format stored in the database.
    """
    
    def __init__(self):
        """Initialize the data retrieval service"""
        self.default_sheet_id = "1MtjJyKiDR7COszXxZF-wYR-UF5LcwYSpTfr_Aa_PEt8"
        self.field_mapper = get_smart_field_mapper()
    
    async def get_sheet_data_for_llm(self, sheet_id: str = None, tab_name: str = None, 
                                   limit: int = 1000) -> Dict[str, Any]:
        """
        Retrieve sheet data formatted for LLM processing.
        
        Args:
            sheet_id: Sheet ID (defaults to your sheet)
            tab_name: Specific tab name (optional)
            limit: Maximum number of rows to retrieve
            
        Returns:
            Dictionary with formatted data for LLM
        """
        sheet_id = sheet_id or self.default_sheet_id
        
        try:
            with get_db_context() as db:
                # Get sheet metadata
                sheet_meta = db.query(SheetsMetadata).filter(
                    SheetsMetadata.sheet_id == sheet_id
                ).first()
                
                if not sheet_meta:
                    return {
                        "success": False,
                        "error": f"Sheet {sheet_id} not found in database",
                        "data": []
                    }
                
                # Build query
                query = db.query(SheetsData).filter(
                    SheetsData.sheet_id == sheet_id
                )
                
                if tab_name:
                    query = query.filter(SheetsData.tab_name == tab_name)
                
                # Get recent data first
                query = query.order_by(SheetsData.synced_at.desc())
                
                if limit:
                    query = query.limit(limit)
                
                rows = query.all()
                
                # Group rows by tab for smart processing
                tabs_data = {}
                for row in rows:
                    if row.tab_name not in tabs_data:
                        tabs_data[row.tab_name] = []
                    tabs_data[row.tab_name].append(row)
                
                # Format data for LLM with smart field mapping
                formatted_data = []
                tabs_summary = {}
                
                for tab_name, tab_rows in tabs_data.items():
                    try:
                        # Parse all row data for this tab
                        tab_raw_data = []
                        for row in tab_rows:
                            row_data = json.loads(row.row_data) if isinstance(row.row_data, str) else row.row_data
                            tab_raw_data.append(row_data)
                        
                        # Auto-detect field mapping for this tab
                        mapping_info = self.field_mapper.auto_map_fields(tab_name, tab_raw_data)
                        field_mapping = mapping_info['field_mapping']
                        
                        # Process each row with the detected mapping
                        for i, row in enumerate(tab_rows):
                            try:
                                row_data = json.loads(row.row_data) if isinstance(row.row_data, str) else row.row_data
                                
                                # Skip header rows (row_index 0 is often headers)
                                is_header_row = (row.row_index == 0 and 
                                               mapping_info.get('header_row') == 0)
                                
                                # Map raw data to field names
                                mapped_fields = self.field_mapper.map_row_to_fields(
                                    tab_name, row_data, field_mapping
                                ) if not is_header_row else {}
                                
                                # Create formatted row
                                formatted_row = {
                                    "tab_name": row.tab_name,
                                    "row_index": row.row_index,
                                    "data": row_data,
                                    "fields": mapped_fields,
                                    "is_header": is_header_row,
                                    "synced_at": row.synced_at.isoformat() if row.synced_at else None,
                                    "non_empty_values": [val for val in row_data if val and str(val).strip()],
                                    "data_summary": self._summarize_row_data(row_data)
                                }
                                
                                formatted_data.append(formatted_row)
                                
                            except Exception as e:
                                logger.warning(f"Error processing row {row.id}: {str(e)}")
                                continue
                        
                        # Track tab summary
                        data_rows = [r for r in formatted_data if r["tab_name"] == tab_name and not r.get("is_header")]
                        tabs_summary[tab_name] = {
                            "row_count": len(data_rows),
                            "total_rows": len(tab_rows),
                            "headers": mapping_info.get('headers', []),
                            "field_mapping": field_mapping,
                            "sample_data": [r["fields"] for r in data_rows[:3] if r["fields"]],
                            "last_updated": max(row.synced_at for row in tab_rows if row.synced_at)
                        }
                        
                    except Exception as e:
                        logger.error(f"Error processing tab {tab_name}: {str(e)}")
                        continue
                
                return {
                    "success": True,
                    "sheet_info": {
                        "sheet_id": sheet_id,
                        "sheet_name": sheet_meta.sheet_name,
                        "last_synced": sheet_meta.last_synced.isoformat() if sheet_meta.last_synced else None,
                        "sync_status": sheet_meta.sync_status
                    },
                    "data_summary": {
                        "total_rows_retrieved": len(formatted_data),
                        "tabs_found": list(tabs_summary.keys()),
                        "tabs_summary": tabs_summary
                    },
                    "data": formatted_data,
                    "llm_context": self._create_llm_context(formatted_data, tabs_summary)
                }
                
        except Exception as e:
            logger.error(f"Error retrieving sheet data: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "data": []
            }
    
    def _summarize_row_data(self, row_data: List[Any]) -> Dict[str, Any]:
        """Create a summary of row data for easier LLM processing"""
        non_empty = [val for val in row_data if val and str(val).strip()]
        
        # Try to identify data types
        numbers = []
        times = []
        text_values = []
        
        for val in non_empty:
            val_str = str(val).strip()
            if val_str:
                # Check if it's a number
                try:
                    num = float(val_str)
                    numbers.append(num)
                except ValueError:
                    # Check if it's a time format
                    if ":" in val_str and len(val_str.split(":")) == 2:
                        times.append(val_str)
                    else:
                        text_values.append(val_str)
        
        return {
            "total_columns": len(row_data),
            "non_empty_columns": len(non_empty),
            "numbers": numbers,
            "times": times,
            "text_values": text_values,
            "has_data": len(non_empty) > 0
        }
    
    def _create_llm_context(self, data: List[Dict], tabs_summary: Dict) -> str:
        """Create a context string for LLM processing"""
        if not data:
            return "No data available."
        
        context_parts = []
        
        # Sheet overview
        context_parts.append("=== SHEET DATA OVERVIEW ===")
        context_parts.append(f"Total rows: {len(data)}")
        context_parts.append(f"Tabs available: {', '.join(tabs_summary.keys())}")
        
        # Tab summaries
        for tab_name, summary in tabs_summary.items():
            context_parts.append(f"\n--- {tab_name} ---")
            context_parts.append(f"Rows: {summary['row_count']}")
            if summary['sample_data']:
                context_parts.append("Sample data:")
                for i, sample in enumerate(summary['sample_data'][:2]):  # Show 2 samples
                    non_empty_sample = [val for val in sample if val and str(val).strip()]
                    if non_empty_sample:
                        context_parts.append(f"  Row {i+1}: {non_empty_sample}")
        
        # Recent data highlights
        context_parts.append("\n=== RECENT DATA HIGHLIGHTS ===")
        recent_data = data[:10]  # First 10 rows (most recent)
        
        for row in recent_data:
            if row["data_summary"]["has_data"]:
                summary = row["data_summary"]
                highlights = []
                
                if summary["numbers"]:
                    highlights.append(f"Numbers: {summary['numbers'][:3]}")  # First 3 numbers
                if summary["times"]:
                    highlights.append(f"Times: {summary['times'][:2]}")  # First 2 times
                if summary["text_values"]:
                    highlights.append(f"Text: {summary['text_values'][:2]}")  # First 2 text values
                
                if highlights:
                    context_parts.append(f"{row['tab_name']} Row {row['row_index']}: {', '.join(highlights)}")
        
        return "\n".join(context_parts)
    
    async def search_data_by_criteria(self, sheet_id: str = None, search_terms: List[str] = None,
                                    tab_names: List[str] = None, date_range: Tuple[datetime, datetime] = None) -> Dict[str, Any]:
        """
        Search data based on specific criteria.
        
        Args:
            sheet_id: Sheet ID to search in
            search_terms: Terms to search for in the data
            tab_names: Specific tabs to search in
            date_range: Date range for synced_at field
            
        Returns:
            Filtered data matching criteria
        """
        sheet_id = sheet_id or self.default_sheet_id
        
        try:
            with get_db_context() as db:
                query = db.query(SheetsData).filter(
                    SheetsData.sheet_id == sheet_id
                )
                
                # Filter by tab names
                if tab_names:
                    query = query.filter(SheetsData.tab_name.in_(tab_names))
                
                # Filter by date range
                if date_range:
                    start_date, end_date = date_range
                    query = query.filter(
                        SheetsData.synced_at >= start_date,
                        SheetsData.synced_at <= end_date
                    )
                
                rows = query.all()
                
                # Filter by search terms if provided
                if search_terms:
                    filtered_rows = []
                    for row in rows:
                        try:
                            row_data = json.loads(row.row_data) if isinstance(row.row_data, str) else row.row_data
                            row_text = " ".join([str(val) for val in row_data if val]).lower()
                            
                            # Check if any search term is found
                            if any(term.lower() in row_text for term in search_terms):
                                filtered_rows.append(row)
                        except Exception:
                            continue
                    rows = filtered_rows
                
                # Format results
                results = []
                # Group by tab for smart processing
                tabs_data = {}
                for row in rows:
                    if row.tab_name not in tabs_data:
                        tabs_data[row.tab_name] = []
                    tabs_data[row.tab_name].append(row)
                
                # Process each tab with smart field mapping
                for tab_name, tab_rows in tabs_data.items():
                    try:
                        # Get all row data for field mapping detection
                        tab_raw_data = []
                        for row in tab_rows:
                            row_data = json.loads(row.row_data) if isinstance(row.row_data, str) else row.row_data
                            tab_raw_data.append(row_data)
                        
                        # Auto-detect field mapping
                        mapping_info = self.field_mapper.auto_map_fields(tab_name, tab_raw_data)
                        field_mapping = mapping_info['field_mapping']
                        
                        # Process each row
                        for row in tab_rows:
                            try:
                                row_data = json.loads(row.row_data) if isinstance(row.row_data, str) else row.row_data
                                
                                # Skip header rows
                                is_header_row = (row.row_index == 0 and 
                                               mapping_info.get('header_row') == 0)
                                
                                if not is_header_row:
                                    mapped_fields = self.field_mapper.map_row_to_fields(
                                        tab_name, row_data, field_mapping
                                    )
                                    
                                    results.append({
                                        "tab_name": row.tab_name,
                                        "row_index": row.row_index,
                                        "data": row_data,
                                        "fields": mapped_fields,
                                        "synced_at": row.synced_at.isoformat() if row.synced_at else None,
                                        "match_summary": self._summarize_row_data(row_data)
                                    })
                            except Exception as e:
                                logger.warning(f"Error processing row {row.id}: {str(e)}")
                                continue
                    except Exception as e:
                        logger.error(f"Error processing tab {tab_name}: {str(e)}")
                        continue
                
                return {
                    "success": True,
                    "search_criteria": {
                        "search_terms": search_terms,
                        "tab_names": tab_names,
                        "date_range": [d.isoformat() for d in date_range] if date_range else None
                    },
                    "results_count": len(results),
                    "results": results
                }
                
        except Exception as e:
            logger.error(f"Error searching data: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "results": []
            }
    
    async def get_formatted_data_by_date(self, date_str: str, tab_name: str = None, 
                                       sheet_id: str = None) -> Dict[str, Any]:
        """
        Get properly formatted data for a specific date with field names
        
        Args:
            date_str: Date string to search for (e.g., "26.6.25", "4.11.25")
            tab_name: Specific tab to search in (optional)
            sheet_id: Sheet ID (optional, uses default)
            
        Returns:
            Formatted data with proper field names
        """
        sheet_id = sheet_id or self.default_sheet_id
        
        try:
            with get_db_context() as db:
                query = db.query(SheetsData).filter(
                    SheetsData.sheet_id == sheet_id
                )
                
                if tab_name:
                    query = query.filter(SheetsData.tab_name == tab_name)
                
                rows = query.all()
                
                # Search for date in row data
                matching_rows = []
                for row in rows:
                    try:
                        row_data = json.loads(row.row_data) if isinstance(row.row_data, str) else row.row_data
                        row_text = " ".join([str(val) for val in row_data if val]).lower()
                        
                        # Check if date appears in the row
                        if date_str.lower() in row_text:
                            mapped_fields = self.field_mapper.map_row_to_fields(row.tab_name, row_data)
                            
                            matching_rows.append({
                                "tab_name": row.tab_name,
                                "row_index": row.row_index,
                                "fields": mapped_fields,
                                "raw_data": row_data,
                                "synced_at": row.synced_at.isoformat() if row.synced_at else None
                            })
                    except Exception as e:
                        logger.warning(f"Error processing row {row.id}: {str(e)}")
                        continue
                
                return {
                    "success": True,
                    "date_searched": date_str,
                    "tab_name": tab_name,
                    "results_count": len(matching_rows),
                    "results": matching_rows
                }
                
        except Exception as e:
            logger.error(f"Error getting formatted data by date: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "results": []
            }
    
    async def get_field_value_by_date(self, date_str: str, field_name: str, 
                                    tab_name: str = None, sheet_id: str = None) -> Dict[str, Any]:
        """
        Get specific field value for a date (e.g., RO1&2 feed tank level on 26.6.25)
        
        Args:
            date_str: Date string to search for
            field_name: Field name or alias to retrieve
            tab_name: Specific tab to search in (optional)
            sheet_id: Sheet ID (optional, uses default)
            
        Returns:
            Field value and context
        """
        data_result = await self.get_formatted_data_by_date(date_str, tab_name, sheet_id)
        
        if not data_result["success"] or not data_result["results"]:
            return {
                "success": False,
                "message": f"No data found for date {date_str}",
                "field_name": field_name,
                "date": date_str
            }
        
        # Search for the field in matching rows
        field_values = []
        for row in data_result["results"]:
            field_value = self.field_mapper.get_field_value(row["fields"], field_name)
            if field_value is not None:
                field_values.append({
                    "tab_name": row["tab_name"],
                    "field_name": field_name,
                    "value": field_value,
                    "row_index": row["row_index"],
                    "all_fields": row["fields"]
                })
        
        if field_values:
            return {
                "success": True,
                "date": date_str,
                "field_name": field_name,
                "values_found": len(field_values),
                "values": field_values
            }
        else:
            available_fields = []
            for row in data_result["results"]:
                available_fields.extend(list(row["fields"].keys()))
            
            return {
                "success": False,
                "message": f"Field '{field_name}' not found for date {date_str}",
                "date": date_str,
                "field_name": field_name,
                "available_fields": list(set(available_fields)),
                "suggestion": f"Try one of these fields: {', '.join(list(set(available_fields))[:5])}"
            }
    
    async def get_latest_data(self, sheet_id: str = None, tab_name: str = None) -> Dict[str, Any]:
        """
        Get the most recent actual data entry (not header rows)
        
        Args:
            sheet_id: Sheet ID (optional, uses default)
            tab_name: Specific tab name (optional)
            
        Returns:
            Latest data with proper field mapping
        """
        sheet_id = sheet_id or self.default_sheet_id
        
        try:
            with get_db_context() as db:
                query = db.query(SheetsData).filter(
                    SheetsData.sheet_id == sheet_id
                )
                
                if tab_name:
                    query = query.filter(SheetsData.tab_name == tab_name)
                
                # Order by synced_at descending to get most recent
                rows = query.order_by(SheetsData.synced_at.desc()).all()
                
                if not rows:
                    return {
                        "success": False,
                        "message": "No data found",
                        "latest_data": None
                    }
                
                # Group by tab and find latest data for each
                tabs_data = {}
                for row in rows:
                    if row.tab_name not in tabs_data:
                        tabs_data[row.tab_name] = []
                    tabs_data[row.tab_name].append(row)
                
                latest_data = {}
                
                for tab_name, tab_rows in tabs_data.items():
                    try:
                        # Parse all row data for smart mapping
                        tab_raw_data = []
                        for row in tab_rows:
                            row_data = json.loads(row.row_data) if isinstance(row.row_data, str) else row.row_data
                            tab_raw_data.append(row_data)
                        
                        # Find latest actual data row (not header)
                        latest_row_data = self.field_mapper.find_latest_data_row(tab_raw_data)
                        
                        if latest_row_data:
                            # Find the corresponding database row
                            for row in tab_rows:
                                row_data = json.loads(row.row_data) if isinstance(row.row_data, str) else row.row_data
                                if row_data == latest_row_data['raw_data']:
                                    latest_data[tab_name] = {
                                        "tab_name": tab_name,
                                        "row_index": row.row_index,
                                        "fields": latest_row_data['fields'],
                                        "raw_data": latest_row_data['raw_data'],
                                        "synced_at": row.synced_at.isoformat() if row.synced_at else None
                                    }
                                    break
                    except Exception as e:
                        logger.warning(f"Error processing latest data for tab {tab_name}: {str(e)}")
                        continue
                
                return {
                    "success": True,
                    "latest_data": latest_data,
                    "tabs_found": list(latest_data.keys())
                }
                
        except Exception as e:
            logger.error(f"Error getting latest data: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "latest_data": None
            }


# Global instance
_data_retrieval_service = None

def get_data_retrieval_service() -> DataRetrievalService:
    """Get the global data retrieval service instance"""
    global _data_retrieval_service
    if _data_retrieval_service is None:
        _data_retrieval_service = DataRetrievalService()
    return _data_retrieval_service