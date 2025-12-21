"""
Query Results Exporter - Exports database query results to Google Sheets
Automatically creates formatted sheets with retrieved data for better visualization
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import json
from services.sheets import SheetsService

logger = logging.getLogger(__name__)

# Target Google Sheet ID for query results (separate sheet for exports)
QUERY_RESULTS_SHEET_ID = "1o309wUobSEur2jx_T3UV0Rm942CWwhbx77SILtQhbh0"


class QueryResultsExporter:
    """
    Exports query results to Google Sheets for better data visualization
    Creates formatted sheets with headers, data, and metadata
    """
    
    def __init__(self):
        """Initialize the exporter with sheets service"""
        try:
            self.sheets_service = SheetsService()
            self.target_sheet_id = QUERY_RESULTS_SHEET_ID
            logger.info(f"✅ Query Results Exporter initialized with sheet: {self.target_sheet_id}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Query Results Exporter: {str(e)}")
            self.sheets_service = None
    
    async def export_query_results(self, query: str, raw_data: Dict[str, Any], 
                                 formatted_response: str) -> Dict[str, Any]:
        """
        Export query results to Google Sheets
        
        Args:
            query: Original user query
            raw_data: Raw data retrieved from database
            formatted_response: Formatted response text
            
        Returns:
            Export result with success status and details
        """
        if not self.sheets_service:
            return {
                "success": False,
                "error": "Sheets service not available",
                "message": "Cannot export to Google Sheets"
            }
        
        try:
            logger.info(f"📊 Exporting query results to Google Sheets")
            logger.info(f"   Query: '{query}'")
            logger.info(f"   Target Sheet: {self.target_sheet_id}")
            
            # Generate tab name based on query and timestamp
            tab_name = self._generate_tab_name(query)
            
            # Prepare data for export
            export_data = self._prepare_export_data(query, raw_data, formatted_response)
            
            if not export_data:
                return {
                    "success": False,
                    "error": "No data to export",
                    "message": "Query returned no exportable data"
                }
            
            # Create or update the sheet tab
            result = await self._write_to_sheet(tab_name, export_data)
            
            if result["success"]:
                logger.info(f"✅ Successfully exported query results to sheet tab: {tab_name}")
                return {
                    "success": True,
                    "sheet_id": self.target_sheet_id,
                    "tab_name": tab_name,
                    "rows_exported": len(export_data) - 1,  # Exclude header
                    "sheet_url": f"https://docs.google.com/spreadsheets/d/{self.target_sheet_id}",
                    "message": f"Query results exported to '{tab_name}' tab"
                }
            else:
                return result
                
        except Exception as e:
            logger.error(f"❌ Error exporting query results: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "message": "Failed to export query results to Google Sheets"
            }
    
    def _generate_tab_name(self, query: str) -> str:
        """Generate a meaningful tab name from the query"""
        # Clean and shorten the query for tab name
        clean_query = query.lower().strip()
        
        # Extract key terms
        key_terms = []
        if "12-12" in clean_query or "12.12" in clean_query or "december" in clean_query:
            key_terms.append("Dec12")
        if "amount" in clean_query:
            key_terms.append("Amount")
        if "ro details" in clean_query or "ro1" in clean_query:
            key_terms.append("RO")
        if "tank" in clean_query:
            key_terms.append("Tank")
        if "latest" in clean_query:
            key_terms.append("Latest")
        if "costing" in clean_query:
            key_terms.append("Cost")
        
        # Build tab name
        if key_terms:
            base_name = "_".join(key_terms[:3])  # Max 3 terms
        else:
            base_name = "Query"
        
        # Add timestamp
        timestamp = datetime.now().strftime("%m%d_%H%M")
        tab_name = f"{base_name}_{timestamp}"
        
        # Ensure tab name is valid (max 100 chars, no special chars)
        tab_name = tab_name.replace(" ", "_").replace("/", "_").replace("\\", "_")
        return tab_name[:100]
    
    def _prepare_export_data(self, query: str, raw_data: Dict[str, Any], 
                           formatted_response: str) -> List[List[Any]]:
        """Prepare data for export to Google Sheets with improved formatting"""
        export_data = []
        
        try:
            # Add header with query information in key-value format
            export_data.append(["QUERY RESULTS EXPORT"])
            export_data.append([])
            export_data.append(["Query:", query])
            export_data.append(["Export Date:", datetime.now().strftime('%Y-%m-%d')])
            export_data.append(["Export Time:", datetime.now().strftime('%H:%M:%S')])
            export_data.append(["Sheet ID:", self.target_sheet_id])
            export_data.append([])
            export_data.append(["=" * 80])
            export_data.append([])
            
            # Add formatted response summary with better structure
            export_data.append(["RESPONSE SUMMARY"])
            export_data.append([])
            response_lines = formatted_response.split('\n')[:8]  # First 8 lines
            for line in response_lines:
                if line.strip():
                    # Clean up markdown formatting for better readability
                    clean_line = line.strip().replace('**', '').replace('*', '•')
                    export_data.append([clean_line])
            export_data.append([])
            export_data.append(["=" * 80])
            export_data.append([])
            
            # Process different types of raw data
            if raw_data.get("values"):
                export_data.extend(self._format_values_data(raw_data["values"]))
            
            elif raw_data.get("results"):
                if raw_data.get("tab_groups"):
                    export_data.extend(self._format_tab_groups_data(raw_data["tab_groups"]))
                else:
                    export_data.extend(self._format_results_data(raw_data["results"]))
            
            elif raw_data.get("latest_data"):
                export_data.extend(self._format_latest_data(raw_data["latest_data"]))
            
            elif raw_data.get("etp_table"):
                # Handle ETP table data (specialized format)
                export_data.extend(raw_data["etp_table"])
            
            else:
                export_data.append(["No structured data available for export"])
            
            return export_data
            
        except Exception as e:
            logger.error(f"Error preparing export data: {str(e)}")
            return [
                ["ERROR PREPARING DATA"],
                [f"Query: {query}"],
                [f"Error: {str(e)}"],
                [f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"]
            ]
    
    def _format_values_data(self, values: List[Dict[str, Any]]) -> List[List[Any]]:
        """Format values data as Excel-like table with column headers"""
        data = []
        
        # Header
        data.append(["FIELD VALUES DATA"])
        data.append([])
        
        if not values:
            data.append(["No data available"])
            return data
        
        # Collect all unique fields from row_data
        all_fields = set()
        for value_info in values:
            if value_info.get('row_data'):
                all_fields.update(value_info['row_data'].keys())
        
        if all_fields:
            # Create table with all fields as columns
            sorted_fields = sorted(list(all_fields))
            
            # Table header row
            header_row = ["#", "Source", "Row"] + sorted_fields
            data.append(header_row)
            
            # Add data rows
            for i, value_info in enumerate(values[:100], 1):  # Limit to 100 rows
                tab_name = value_info.get('tab_name', 'Unknown')
                row_index = value_info.get('row_index', 'N/A')
                row_data = value_info.get('row_data', {})
                
                # Build row with all field values
                row = [i, tab_name, str(row_index)]
                for field in sorted_fields:
                    field_value = row_data.get(field, '')
                    row.append(str(field_value) if field_value else '')
                
                data.append(row)
        else:
            # Fallback to simple table if no row_data
            data.append(["#", "Field Name", "Value", "Source", "Row"])
            
            for i, value_info in enumerate(values[:100], 1):
                field_name = value_info.get('field_name', 'Unknown')
                value = value_info.get('value', 'N/A')
                tab_name = value_info.get('tab_name', 'Unknown')
                row_index = value_info.get('row_index', 'N/A')
                
                data.append([i, field_name, str(value), tab_name, str(row_index)])
        
        data.append([])  # Empty row for spacing
        return data
    
    def _format_results_data(self, results: List[Dict[str, Any]]) -> List[List[Any]]:
        """Format search results data as Excel-like table"""
        data = []
        
        # Header
        data.append(["SEARCH RESULTS DATA"])
        data.append([])
        
        if not results:
            data.append(["No results available"])
            return data
        
        # Simple results table
        data.append(["#", "Field Name", "Value", "Source", "Row"])
        
        for i, result in enumerate(results[:100], 1):  # Limit to 100 rows
            field_name = result.get('field_name', 'Unknown')
            value = result.get('value', 'N/A')
            tab_name = result.get('tab_name', 'Unknown')
            row_index = result.get('row_index', 'N/A')
            
            data.append([i, field_name, str(value), tab_name, str(row_index)])
        
        data.append([])  # Empty row for spacing
        return data
    
    def _format_tab_groups_data(self, tab_groups: Dict[str, List[Dict[str, Any]]]) -> List[List[Any]]:
        """Format tab groups data as Excel-like tables by source"""
        data = []
        
        # Header
        data.append(["DATA BY SOURCE TABS"])
        data.append([])
        
        for tab_idx, (tab_name, tab_results) in enumerate(list(tab_groups.items())[:10], 1):
            # Tab section header
            data.append([f"TAB {tab_idx}: {tab_name} ({len(tab_results)} entries)"])
            data.append([])
            
            if not tab_results:
                data.append(["No data available"])
                data.append([])
                continue
            
            # Collect all unique fields from this tab
            all_fields = set()
            for result in tab_results:
                if result.get('row_data'):
                    all_fields.update(result['row_data'].keys())
            
            if all_fields:
                # Create table with all fields as columns
                sorted_fields = sorted(list(all_fields))
                
                # Table header
                header_row = ["Row"] + sorted_fields
                data.append(header_row)
                
                # Add data rows
                for result in tab_results[:50]:  # Limit to 50 rows per tab
                    row_index = result.get('row_index', 'N/A')
                    row_data = result.get('row_data', {})
                    
                    # Build row with all field values
                    row = [str(row_index)]
                    for field in sorted_fields:
                        field_value = row_data.get(field, '')
                        row.append(str(field_value) if field_value else '')
                    
                    data.append(row)
                
                if len(tab_results) > 50:
                    data.append([f"... and {len(tab_results) - 50} more rows"])
            else:
                data.append(["No detailed data available"])
            
            data.append([])  # Empty row between tabs
        
        return data
    
    def _format_latest_data(self, latest_data: Dict[str, Dict[str, Any]]) -> List[List[Any]]:
        """Format latest data as Excel-like table"""
        data = []
        
        # Header
        data.append(["LATEST DATA BY TAB"])
        data.append([])
        
        if not latest_data:
            data.append(["No latest data available"])
            return data
        
        # Collect all unique fields across all tabs
        all_fields = set()
        for tab_data in latest_data.values():
            fields = tab_data.get("fields", {})
            all_fields.update(fields.keys())
        
        if all_fields:
            # Create table with all fields as columns
            sorted_fields = sorted(list(all_fields))
            
            # Table header
            header_row = ["Source Tab"] + sorted_fields
            data.append(header_row)
            
            # Add data rows
            for tab_name, tab_data in latest_data.items():
                fields = tab_data.get("fields", {})
                
                # Build row with all field values
                row = [tab_name]
                for field in sorted_fields:
                    field_value = fields.get(field, '')
                    row.append(str(field_value) if field_value else '')
                
                data.append(row)
        else:
            data.append(["No field data available"])
        
        data.append([])  # Empty row for spacing
        return data
    
    async def _write_to_sheet(self, tab_name: str, data: List[List[Any]]) -> Dict[str, Any]:
        """Write data to Google Sheets"""
        try:
            # Use the default "Sheet1" tab and append data with clear separators
            default_tab = "Sheet1"
            
            # Add a separator and timestamp to distinguish this query from others
            separator_data = [
                [],  # Empty row for spacing
                [f"=" * 80],  # Visual separator
                [f"QUERY EXPORT: {tab_name}"],
                [f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"],
                [f"=" * 80],
                []  # Empty row for spacing
            ]
            
            # Combine separator with actual data
            full_data = separator_data + data
            
            # Write data to the default sheet tab
            result = await self.sheets_service.write_sheet(
                sheet_id=self.target_sheet_id,
                tab_name=default_tab,
                data=full_data
            )
            
            logger.info(f"✅ Successfully wrote {len(full_data)} rows to sheet tab: {default_tab}")
            
            return {
                "success": True,
                "result": result,
                "rows_written": len(full_data),
                "tab_used": default_tab
            }
            
        except Exception as e:
            logger.error(f"❌ Error writing to sheet: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "message": "Failed to write data to Google Sheets"
            }


# Global instance
_query_results_exporter = None

def get_query_results_exporter() -> QueryResultsExporter:
    """Get the global query results exporter instance"""
    global _query_results_exporter
    if _query_results_exporter is None:
        _query_results_exporter = QueryResultsExporter()
    return _query_results_exporter