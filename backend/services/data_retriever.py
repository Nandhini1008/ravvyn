"""
Data Retriever Service - Retrieves and processes data from database for LLM responses
Handles JSON array data structure and provides structured output for AI processing
"""

import json
import logging
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text
from services.database import get_db_context, SheetsData, SheetsMetadata
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class DataRetriever:
    """
    Service to retrieve and process data from the database for LLM responses.
    Handles the JSON array data structure and provides meaningful context.
    """
    
    def __init__(self):
        """Initialize the data retriever"""
        self.max_rows_per_query = 1000  # Limit to prevent overwhelming LLM
        self.default_sheet_id = "1MtjJyKiDR7COszXxZF-wYR-UF5LcwYSpTfr_Aa_PEt8"
    
    async def get_sheet_data_for_llm(self, sheet_id: str = None, tab_name: str = None, 
                                   limit: int = None, query_context: str = None) -> Dict[str, Any]:
        """
        Retrieve sheet data formatted for LLM processing.
        
        Args:
            sheet_id: Sheet ID (defaults to your sheet)
            tab_name: Specific tab name to filter
            limit: Maximum number of rows to return
            query_context: Context about what the user is asking for
            
        Returns:
            Dictionary with structured data for LLM
        """
        sheet_id = sheet_id or self.default_sheet_id
        limit = limit or self.max_rows_per_query
        
        try:
            with get_db_context() as db:
                # Get sheet metadata
                sheet_meta = db.query(SheetsMetadata).filter(
                    SheetsMetadata.sheet_id == sheet_id
                ).first()
                
                if not sheet_meta:
                    return {
                        "success": False,
                        "error": "Sheet not found in database",
                        "suggestion": "Please sync the sheet first"
                    }
                
                # Build query based on filters
                query = db.query(SheetsData).filter(
                    SheetsData.sheet_id == sheet_id
                )
                
                if tab_name:
                    query = query.filter(SheetsData.tab_name == tab_name)
                
                # Order by row_index for logical sequence
                query = query.order_by(SheetsData.row_index)
                
                if limit:
                    query = query.limit(limit)
                
                rows = query.all()
                
                if not rows:
                    return {
                        "success": False,
                        "error": "No data found",
                        "sheet_info": {
                            "sheet_name": sheet_meta.sheet_name,
                            "sheet_id": sheet_id,
                            "last_synced": sheet_meta.last_synced.isoformat() if sheet_meta.last_synced else None
                        }
                    }
                
                # Process and structure the data
                structured_data = await self._process_rows_for_llm(rows, sheet_meta, query_context)
                
                return {
                    "success": True,
                    "sheet_info": {
                        "sheet_name": sheet_meta.sheet_name,
                        "sheet_id": sheet_id,
                        "last_synced": sheet_meta.last_synced.isoformat() if sheet_meta.last_synced else None,
                        "sync_status": sheet_meta.sync_status
                    },
                    "data": structured_data,
                    "summary": {
                        "total_rows": len(rows),
                        "tabs_included": list(set(row.tab_name for row in rows)),
                        "data_range": f"Rows {min(row.row_index for row in rows)} to {max(row.row_index for row in rows)}" if rows else "No data"
                    }
                }
                
        except Exception as e:
            logger.error(f"Error retrieving sheet data: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "suggestion": "Please check database connection and try again"
            }
    
    async def _process_rows_for_llm(self, rows: List[SheetsData], sheet_meta: SheetsMetadata, 
                                  query_context: str = None) -> Dict[str, Any]:
        """
        Process raw database rows into structured data for LLM.
        
        Args:
            rows: Raw database rows
            sheet_meta: Sheet metadata
            query_context: Context about the query
            
        Returns:
            Structured data for LLM processing
        """
        try:
            # Group data by tab
            tabs_data = {}
            
            for row in rows:
                tab_name = row.tab_name
                
                if tab_name not in tabs_data:
                    tabs_data[tab_name] = {
                        "tab_name": tab_name,
                        "rows": [],
                        "headers": None,
                        "data_types": [],
                        "summary": {}
                    }
                
                # Parse JSON data
                try:
                    row_data = json.loads(row.row_data) if isinstance(row.row_data, str) else row.row_data
                    
                    # Clean empty strings and normalize data
                    cleaned_data = []
                    for cell in row_data:
                        if cell == "":
                            cleaned_data.append(None)
                        else:
                            # Try to convert to number if possible
                            try:
                                if '.' in str(cell):
                                    cleaned_data.append(float(cell))
                                else:
                                    cleaned_data.append(int(cell))
                            except (ValueError, TypeError):
                                cleaned_data.append(str(cell))
                    
                    tabs_data[tab_name]["rows"].append({
                        "row_index": row.row_index,
                        "data": cleaned_data,
                        "synced_at": row.synced_at.isoformat() if row.synced_at else None
                    })
                    
                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning(f"Error parsing row data for row {row.row_index}: {str(e)}")
                    continue
            
            # Process each tab's data
            for tab_name, tab_data in tabs_data.items():
                tab_data["summary"] = await self._generate_tab_summary(tab_data["rows"], tab_name)
                
                # Detect headers (usually first row with non-empty values)
                if tab_data["rows"]:
                    first_row = tab_data["rows"][0]
                    if any(cell is not None for cell in first_row["data"]):
                        tab_data["headers"] = first_row["data"]
                        # Remove header row from data
                        tab_data["rows"] = tab_data["rows"][1:]
            
            return {
                "tabs": tabs_data,
                "processing_info": {
                    "processed_at": datetime.now().isoformat(),
                    "query_context": query_context,
                    "data_format": "JSON arrays converted to structured format"
                }
            }
            
        except Exception as e:
            logger.error(f"Error processing rows for LLM: {str(e)}")
            return {
                "error": str(e),
                "raw_row_count": len(rows)
            }
    
    async def _generate_tab_summary(self, rows: List[Dict], tab_name: str) -> Dict[str, Any]:
        """
        Generate a summary of the tab data for LLM context.
        
        Args:
            rows: Processed row data
            tab_name: Name of the tab
            
        Returns:
            Summary information
        """
        if not rows:
            return {"row_count": 0, "has_data": False}
        
        # Analyze data patterns
        total_rows = len(rows)
        non_empty_rows = sum(1 for row in rows if any(cell is not None for cell in row["data"]))
        
        # Find common data patterns
        column_count = max(len(row["data"]) for row in rows) if rows else 0
        
        # Detect data types per column
        column_types = []
        for col_idx in range(column_count):
            col_values = [row["data"][col_idx] for row in rows if col_idx < len(row["data"]) and row["data"][col_idx] is not None]
            
            if not col_values:
                column_types.append("empty")
            elif all(isinstance(v, (int, float)) for v in col_values):
                column_types.append("numeric")
            elif any(":" in str(v) for v in col_values):
                column_types.append("time")
            else:
                column_types.append("text")
        
        # Recent data check
        recent_rows = [row for row in rows if row.get("synced_at")]
        latest_sync = max(row["synced_at"] for row in recent_rows) if recent_rows else None
        
        return {
            "row_count": total_rows,
            "non_empty_rows": non_empty_rows,
            "column_count": column_count,
            "column_types": column_types,
            "has_data": non_empty_rows > 0,
            "latest_sync": latest_sync,
            "data_density": round(non_empty_rows / total_rows * 100, 1) if total_rows > 0 else 0
        }
    
    async def get_specific_data_for_query(self, user_query: str, sheet_id: str = None) -> Dict[str, Any]:
        """
        Get specific data based on user query context.
        
        Args:
            user_query: User's question/request
            sheet_id: Sheet ID to search in
            
        Returns:
            Relevant data for the query
        """
        sheet_id = sheet_id or self.default_sheet_id
        
        # Analyze query for keywords
        query_lower = user_query.lower()
        
        # Determine relevant tabs based on query
        relevant_tabs = []
        if any(word in query_lower for word in ["ro", "reverse osmosis", "water"]):
            relevant_tabs.append("RO DETAILS")
        if any(word in query_lower for word in ["cost", "price", "money", "expense"]):
            relevant_tabs.append("costing")
        if any(word in query_lower for word in ["condensate", "hour"]):
            relevant_tabs.append("CONDENSATE HOUR DETAILS")
        if any(word in query_lower for word in ["running", "hours", "operation"]):
            relevant_tabs.append("RUNNING HRS")
        if any(word in query_lower for word in ["tank", "dipping", "level"]):
            relevant_tabs.append("TANK DIPPING")
        if any(word in query_lower for word in ["evening", "report"]):
            relevant_tabs.append("EVENING REPORT")
        if any(word in query_lower for word in ["maintenance", "work"]):
            relevant_tabs.append("Maintance work details")
        if any(word in query_lower for word in ["purchase", "buy"]):
            relevant_tabs.append("purchase details")
        if any(word in query_lower for word in ["tds", "dissolved solids"]):
            relevant_tabs.append("TDS DETAILS")
        if any(word in query_lower for word in ["meeting"]):
            relevant_tabs.append("MEETING DETAILS")
        
        # If no specific tabs identified, get recent data from main tabs
        if not relevant_tabs:
            relevant_tabs = ["RO DETAILS", "EVENING REPORT", "costing"]
        
        # Get data for relevant tabs
        all_data = {"tabs": {}, "query_analysis": {"user_query": user_query, "relevant_tabs": relevant_tabs}}
        
        for tab_name in relevant_tabs:
            tab_data = await self.get_sheet_data_for_llm(
                sheet_id=sheet_id, 
                tab_name=tab_name, 
                limit=100,  # Smaller limit for specific queries
                query_context=user_query
            )
            
            if tab_data.get("success") and tab_data.get("data", {}).get("tabs", {}).get(tab_name):
                all_data["tabs"][tab_name] = tab_data["data"]["tabs"][tab_name]
        
        return all_data
    
    async def format_data_for_llm_context(self, data: Dict[str, Any], max_context_length: int = 4000) -> str:
        """
        Format retrieved data into a context string for LLM.
        
        Args:
            data: Retrieved data dictionary
            max_context_length: Maximum length of context string
            
        Returns:
            Formatted context string
        """
        if not data.get("success", True) or not data.get("tabs"):
            return "No relevant data found in the database."
        
        context_parts = []
        
        # Add sheet info
        if "sheet_info" in data:
            sheet_info = data["sheet_info"]
            context_parts.append(f"=== SHEET: {sheet_info.get('sheet_name', 'Unknown')} ===")
            if sheet_info.get("last_synced"):
                context_parts.append(f"Last synced: {sheet_info['last_synced']}")
        
        # Add tab data
        for tab_name, tab_data in data["tabs"].items():
            context_parts.append(f"\n--- {tab_name} ---")
            
            summary = tab_data.get("summary", {})
            if summary.get("has_data"):
                context_parts.append(f"Rows: {summary.get('row_count', 0)}, Columns: {summary.get('column_count', 0)}")
                
                # Add headers if available
                if tab_data.get("headers"):
                    headers = [str(h) for h in tab_data["headers"] if h is not None]
                    if headers:
                        context_parts.append(f"Headers: {', '.join(headers)}")
                
                # Add sample data (first few rows)
                rows = tab_data.get("rows", [])
                if rows:
                    context_parts.append("Recent data:")
                    for i, row in enumerate(rows[:5]):  # First 5 rows
                        row_data = [str(cell) if cell is not None else "" for cell in row["data"]]
                        context_parts.append(f"  Row {row['row_index']}: {', '.join(row_data)}")
                        
                        # Check context length
                        current_context = "\n".join(context_parts)
                        if len(current_context) > max_context_length:
                            context_parts.append("  ... (data truncated)")
                            break
            else:
                context_parts.append("No data available")
        
        return "\n".join(context_parts)


# Global instance
_data_retriever = None

def get_data_retriever() -> DataRetriever:
    """Get the global data retriever instance"""
    global _data_retriever
    if _data_retriever is None:
        _data_retriever = DataRetriever()
    return _data_retriever