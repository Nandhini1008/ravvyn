"""
ETP Tank Capacity and Storage Details Processor
Specialized processor for ETP tank queries that generates structured tables with calculations
"""

import logging
import re
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from services.database import get_db_context
from services.query_results_exporter import get_query_results_exporter

logger = logging.getLogger(__name__)


class ETPTankProcessor:
    """
    Specialized processor for ETP Tank Capacity and Storage Details queries
    Creates structured tables with calculations and exports to Google Sheets
    """
    
    def __init__(self):
        """Initialize ETP Tank Processor"""
        self.tank_names = [
            "ETP Inlet Tank",
            "Filter Feed Tank", 
            "UF Feed Tank",
            "RO 1 & 2 Feed Tank",
            "RO 3 Feed Tank",
            "Salzberg Feed Tank"
        ]
        
        # Common field patterns for ETP data
        self.capacity_patterns = [
            r'actual\s*capacity',
            r'capacity',
            r'tank\s*capacity',
            r'max\s*capacity'
        ]
        
        self.storage_patterns = [
            r'storage.*5:?00',
            r'storage.*17:?00',
            r'level.*5:?00',
            r'level.*17:?00',
            r'@\s*5:?00',
            r'at\s*5:?00'
        ]
    
    def is_etp_query(self, query: str) -> bool:
        """Check if query is asking for ETP tank capacity and storage details"""
        query_lower = query.lower()
        
        etp_indicators = [
            'etp tank capacity',
            'etp tank storage',
            'tank capacity and storage',
            'etp capacity',
            'tank details',
            'storage details'
        ]
        
        return any(indicator in query_lower for indicator in etp_indicators)
    
    def extract_date_from_query(self, query: str) -> Optional[str]:
        """Extract date from the query"""
        # Common date patterns
        date_patterns = [
            r'\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}',  # DD.MM.YYYY, DD-MM-YYYY, DD/MM/YYYY
            r'\d{4}[.\-/]\d{1,2}[.\-/]\d{1,2}',    # YYYY.MM.DD, YYYY-MM-DD, YYYY/MM/DD
            r'\d{1,2}\s+(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{4}',
            r'(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+\d{4}'
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                return match.group(0)
        
        return None
    
    async def process_etp_query(self, query: str, sheet_id: str = None) -> Dict[str, Any]:
        """
        Process ETP tank capacity and storage query
        
        Args:
            query: User query about ETP tank details
            sheet_id: Optional sheet ID
            
        Returns:
            Structured ETP tank data with calculations
        """
        logger.info(f"🏭 Processing ETP tank query: {query}")
        
        try:
            # Extract date from query
            query_date = self.extract_date_from_query(query)
            if not query_date:
                return {
                    "success": False,
                    "error": "No date found in query. Please specify a date for ETP tank details.",
                    "message": "Example: 'ETP tank capacity and storage details for 25.10.2025'"
                }
            
            logger.info(f"📅 Extracted date: {query_date}")
            
            # Search for ETP tank data for the specified date
            etp_data = await self._search_etp_data_for_date(query_date, sheet_id)
            
            if not etp_data:
                return {
                    "success": False,
                    "error": f"No ETP tank data found for {query_date}",
                    "message": "Please check if data exists for this date in the database."
                }
            
            # Process and structure the ETP data
            structured_data = self._structure_etp_data(etp_data, query_date)
            
            # Calculate totals and balances
            calculated_data = self._calculate_etp_totals(structured_data)
            
            # Generate formatted response
            response = self._generate_etp_response(calculated_data, query_date)
            
            # Export disabled - User can export on demand via "View in Sheets" button
            # await self._export_etp_table_to_sheets(calculated_data, query_date, query)
            
            return {
                "success": True,
                "query": query,
                "date": query_date,
                "answer": response,
                "etp_data": calculated_data,
                "raw_data": {"etp_table": calculated_data},  # Store for on-demand export
                "data_found": len(calculated_data.get("tanks", [])),
                "processing_method": "etp_specialized"
            }
            
        except Exception as e:
            logger.error(f"Error processing ETP query: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "message": "Failed to process ETP tank query"
            }
    
    async def _search_etp_data_for_date(self, date_str: str, sheet_id: str = None) -> List[Dict[str, Any]]:
        """Search for ETP tank data for specific date"""
        try:
            with get_db_context() as db:
                # Generate date variations for better matching
                date_variations = self._generate_date_variations(date_str)
                
                # Build search query for ETP data
                search_params = {'sheet_id': sheet_id or "1MtjJyKiDR7COszXxZF-wYR-UF5LcwYSpTfr_Aa_PEt8"}
                
                # Create OR conditions for date matching
                date_conditions = []
                for i, date_var in enumerate(date_variations):
                    param_name = f'date_{i}'
                    date_conditions.append(f'row_data LIKE :{param_name}')
                    search_params[param_name] = f'%{date_var}%'
                
                # Search for ETP-related data
                sql_query = f"""
                    SELECT tab_name, row_index, row_data 
                    FROM sheets_data 
                    WHERE sheet_id = :sheet_id 
                    AND row_index > 0
                    AND ({' OR '.join(date_conditions)})
                    AND (
                        LOWER(row_data) LIKE '%etp%' OR
                        LOWER(row_data) LIKE '%tank%' OR
                        LOWER(row_data) LIKE '%capacity%' OR
                        LOWER(row_data) LIKE '%storage%' OR
                        LOWER(row_data) LIKE '%inlet%' OR
                        LOWER(row_data) LIKE '%filter%' OR
                        LOWER(row_data) LIKE '%feed%' OR
                        LOWER(row_data) LIKE '%salzberg%'
                    )
                    ORDER BY tab_name, row_index
                    LIMIT 200
                """
                
                logger.info(f"🔍 Searching ETP data for date: {date_str}")
                
                result = db.execute(sql_query, search_params)
                matching_rows = result.fetchall()
                
                logger.info(f"✅ Found {len(matching_rows)} ETP-related rows for {date_str}")
                
                # Process results
                etp_data = []
                for row_tuple in matching_rows:
                    try:
                        tab_name = row_tuple[0]
                        row_index = row_tuple[1]
                        row_data_str = row_tuple[2]
                        
                        # Parse row data
                        import json
                        row_data = json.loads(row_data_str) if isinstance(row_data_str, str) else row_data_str
                        
                        if not row_data or not isinstance(row_data, list):
                            continue
                        
                        etp_data.append({
                            "tab_name": tab_name,
                            "row_index": row_index,
                            "row_data": row_data,
                            "date_found": date_str
                        })
                        
                    except Exception as e:
                        logger.warning(f"Error processing ETP row: {e}")
                        continue
                
                return etp_data
                
        except Exception as e:
            logger.error(f"Error searching ETP data: {str(e)}")
            return []
    
    def _generate_date_variations(self, date_str: str) -> List[str]:
        """Generate multiple variations of a date string for better matching"""
        variations = [date_str]
        
        # Try to parse and generate variations
        import re
        
        # Match patterns like DD.MM.YYYY, DD-MM-YYYY, DD/MM/YYYY
        date_match = re.match(r'(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{2,4})', date_str)
        if date_match:
            day, month, year = date_match.groups()
            
            # Ensure 2-digit day/month
            day = day.zfill(2)
            month = month.zfill(2)
            
            # Generate variations
            variations.extend([
                f"{day}.{month}.{year}",
                f"{day}/{month}/{year}",
                f"{day}-{month}-{year}",
                f"{int(day)}.{int(month)}.{year}",
                f"{int(day)}/{int(month)}/{year}",
                f"{int(day)}-{int(month)}-{year}",
                f"{day}.{month}.{year[-2:]}",
                f"{day}/{month}/{year[-2:]}",
                f"{day}-{month}-{year[-2:]}",
            ])
        
        return list(set(variations))
    
    def _structure_etp_data(self, etp_data: List[Dict[str, Any]], date_str: str) -> Dict[str, Any]:
        """Structure ETP data into tank-specific information"""
        structured = {
            "date": date_str,
            "tanks": {},
            "raw_data": etp_data
        }
        
        # Process each row to extract tank information
        for data_row in etp_data:
            row_data = data_row["row_data"]
            tab_name = data_row["tab_name"]
            
            # Try to identify tank information from the row
            tank_info = self._extract_tank_info_from_row(row_data, tab_name)
            
            if tank_info:
                tank_name = tank_info["tank_name"]
                if tank_name not in structured["tanks"]:
                    structured["tanks"][tank_name] = {
                        "actual_capacity": None,
                        "storage_5pm": None,
                        "balance": None,
                        "source_rows": []
                    }
                
                # Update tank information
                if tank_info.get("actual_capacity"):
                    structured["tanks"][tank_name]["actual_capacity"] = tank_info["actual_capacity"]
                
                if tank_info.get("storage_5pm"):
                    structured["tanks"][tank_name]["storage_5pm"] = tank_info["storage_5pm"]
                
                structured["tanks"][tank_name]["source_rows"].append({
                    "tab": tab_name,
                    "row": data_row["row_index"],
                    "data": row_data
                })
        
        return structured
    
    def _extract_tank_info_from_row(self, row_data: List[Any], tab_name: str) -> Optional[Dict[str, Any]]:
        """Extract tank information from a row of data"""
        if not row_data:
            return None
        
        # Convert row to string for analysis
        row_text = ' '.join(str(cell).lower() for cell in row_data if cell)
        
        # Identify tank name
        tank_name = None
        for tank in self.tank_names:
            if any(word in row_text for word in tank.lower().split()):
                tank_name = tank
                break
        
        if not tank_name:
            # Try to identify from common patterns
            if 'inlet' in row_text and 'etp' in row_text:
                tank_name = "ETP Inlet Tank"
            elif 'filter' in row_text and 'feed' in row_text:
                tank_name = "Filter Feed Tank"
            elif 'uf' in row_text and 'feed' in row_text:
                tank_name = "UF Feed Tank"
            elif 'ro' in row_text and ('1' in row_text or '2' in row_text) and 'feed' in row_text:
                tank_name = "RO 1 & 2 Feed Tank"
            elif 'ro' in row_text and '3' in row_text and 'feed' in row_text:
                tank_name = "RO 3 Feed Tank"
            elif 'salzberg' in row_text:
                tank_name = "Salzberg Feed Tank"
        
        if not tank_name:
            return None
        
        # Extract numerical values
        tank_info = {"tank_name": tank_name}
        
        # Look for capacity and storage values
        for i, cell in enumerate(row_data):
            if cell and str(cell).replace('.', '').replace(',', '').isdigit():
                value = float(str(cell).replace(',', ''))
                
                # Determine if this is capacity or storage based on context
                context = ' '.join(str(row_data[max(0, i-2):i+3]).lower() for cell in row_data[max(0, i-2):i+3] if cell)
                
                if any(pattern in context for pattern in ['capacity', 'max', 'total']):
                    tank_info["actual_capacity"] = value
                elif any(pattern in context for pattern in ['storage', '5:00', '17:00', '@', 'level']):
                    tank_info["storage_5pm"] = value
        
        return tank_info if len(tank_info) > 1 else None
    
    def _calculate_etp_totals(self, structured_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate totals and balances for ETP tank data"""
        tanks = structured_data["tanks"]
        
        # Calculate balance for each tank and prepare for totals
        total_capacity = 0
        total_storage = 0
        total_balance = 0
        
        for tank_name, tank_data in tanks.items():
            capacity = tank_data.get("actual_capacity", 0) or 0
            storage = tank_data.get("storage_5pm", 0) or 0
            
            # Calculate balance
            balance = capacity - storage
            tank_data["balance"] = balance
            
            # Add to totals
            total_capacity += capacity
            total_storage += storage
            total_balance += balance
        
        # Add totals to structured data
        structured_data["totals"] = {
            "total_capacity": total_capacity,
            "total_storage": total_storage,
            "total_balance": total_balance
        }
        
        return structured_data
    
    def _generate_etp_response(self, calculated_data: Dict[str, Any], date_str: str) -> str:
        """Generate clean, conversational response for ETP tank data (no table in UI)"""
        tanks = calculated_data["tanks"]
        totals = calculated_data["totals"]
        
        # Count tanks with data
        tanks_with_data = len([t for t in tanks.values() if t.get("actual_capacity") or t.get("storage_5pm")])
        
        if tanks_with_data == 0:
            return f"No ETP tank data found for {date_str}."
        
        # Generate clean, conversational response
        response_parts = [
            f"**ETP Tank Details for {date_str}:**",
            f"",
            f"Found data for {tanks_with_data} ETP tanks with the following totals:",
            f"",
            f"• **Total Actual Capacity:** {totals['total_capacity']:.0f} KL",
            f"• **Total Storage @5:00pm:** {totals['total_storage']:.0f} KL", 
            f"• **Total Available Balance:** {totals['total_balance']:.0f} KL",
            f"",
            f"**Tank Summary:**"
        ]
        
        # Add brief summary of each tank
        for tank_name, tank_data in tanks.items():
            capacity = tank_data.get("actual_capacity", 0) or 0
            storage = tank_data.get("storage_5pm", 0) or 0
            balance = tank_data.get("balance", 0) or 0
            
            if capacity > 0 or storage > 0:
                response_parts.append(
                    f"• **{tank_name}:** {balance:.0f} KL available ({capacity:.0f} KL capacity, {storage:.0f} KL stored)"
                )
        
        response_parts.extend([
            f"",
            f"📊 **Detailed table exported to Google Sheets for analysis**"
        ])
        
        return "\n".join(response_parts)
    
    async def _export_etp_table_to_sheets(self, calculated_data: Dict[str, Any], date_str: str, query: str):
        """Export ETP table to Google Sheets in structured format"""
        try:
            exporter = get_query_results_exporter()
            
            # Prepare data for export
            export_data = []
            
            # Header
            export_data.extend([
                ["ETP Tank Capacity and Storage Details"],
                [f"Date: {date_str}"],
                [],
                ["Tank Particulars", "Actual Capacity", "Storage @5:00pm", "Balance"],
                ["", "Qty. in KL", "Qty. in KL", "Qty. in KL"]
            ])
            
            # Tank data rows
            tanks = calculated_data["tanks"]
            for tank_name, tank_data in tanks.items():
                capacity = tank_data.get("actual_capacity", 0) or 0
                storage = tank_data.get("storage_5pm", 0) or 0
                balance = tank_data.get("balance", 0) or 0
                
                export_data.append([tank_name, capacity, storage, balance])
            
            # Totals row
            totals = calculated_data["totals"]
            export_data.append([
                "Total",
                totals["total_capacity"],
                totals["total_storage"], 
                totals["total_balance"]
            ])
            
            # Export to sheets
            await exporter.export_query_results(
                query=query,
                raw_data={"etp_table": export_data},
                formatted_response=self._generate_etp_response(calculated_data, date_str)
            )
            
            logger.info(f"✅ ETP table exported to Google Sheets for {date_str}")
            
        except Exception as e:
            logger.error(f"Error exporting ETP table: {str(e)}")


# Global instance
_etp_tank_processor = None

def get_etp_tank_processor() -> ETPTankProcessor:
    """Get the global ETP tank processor instance"""
    global _etp_tank_processor
    if _etp_tank_processor is None:
        _etp_tank_processor = ETPTankProcessor()
    return _etp_tank_processor