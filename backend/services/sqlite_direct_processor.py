"""
SQLite Direct Processor
Bypasses AI/LLM entirely and queries SQLite database directly
Solves quota issues and ensures precise data retrieval
"""

import logging
import json
import re
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, date
from services.database import get_db_context
from services.query_analyzer import get_query_analyzer

logger = logging.getLogger(__name__)


class SQLiteDirectProcessor:
    """
    Direct SQLite processor that bypasses AI/LLM completely
    Uses pure database queries for data retrieval
    No API quotas, no external dependencies
    """
    
    def __init__(self):
        """Initialize the SQLite direct processor"""
        self.query_analyzer = get_query_analyzer()
        
        # Common field mappings for better search
        self.field_mappings = {
            'salt': ['salt', 'saltz', 'nacl', 'sodium'],
            'water': ['water', 'h2o', 'aqua'],
            'tank': ['tank', 'vessel', 'container'],
            'level': ['level', 'height', 'depth'],
            'pressure': ['pressure', 'press', 'psi', 'bar'],
            'temperature': ['temp', 'temperature', 'heat'],
            'flow': ['flow', 'rate', 'throughput'],
            'ro': ['ro', 'reverse osmosis', 'osmosis'],
            'feed': ['feed', 'supply', 'input'],
            'amount': ['amount', 'quantity', 'volume', 'kg', 'liter', 'litre']
        }
    
    async def process_direct_query(self, query: str, sheet_id: str = None) -> Dict[str, Any]:
        """
        Process query using direct SQLite database access
        No AI/LLM involved - pure database operations
        
        Args:
            query: Natural language query
            sheet_id: Optional sheet ID for filtering
            
        Returns:
            Direct database results with formatted response
        """
        try:
            logger.info(f"🗄️  SQLITE DIRECT PROCESSOR - Processing: '{query}'")
            
            # Step 1: Analyze query structure (no AI - pure pattern matching)
            analysis = self.query_analyzer.analyze_query(query)
            logger.info(f"📋 Query Analysis: Intent={analysis['intent']}")
            
            # Step 2: Extract search terms and date filters
            search_terms = self._extract_search_terms(query)
            date_filters = self._extract_date_filters(query)
            
            logger.info(f"🔍 Search Terms: {search_terms}")
            logger.info(f"📅 Date Filters: {date_filters}")
            
            # Step 3: Build and execute SQLite query
            sql_result = await self._execute_direct_sql(search_terms, date_filters, sheet_id)
            
            if not sql_result['success']:
                return self._create_error_response(query, sql_result['error'])
            
            # Step 4: Format response without AI
            formatted_response = self._format_direct_response(query, sql_result['data'], search_terms)
            
            logger.info(f"✅ Direct SQLite processing complete: {len(sql_result['data'])} results")
            
            return {
                "success": True,
                "query": query,
                "answer": formatted_response,
                "raw_data": {"results": sql_result['data']},
                "data_found": len(sql_result['data']),
                "confidence": 0.9,
                "processing_method": "sqlite_direct",
                "sql_query": sql_result.get('sql_query', ''),
                "search_terms": search_terms,
                "date_filters": date_filters
            }
            
        except Exception as e:
            logger.error(f"❌ Error in SQLite direct processing: {str(e)}")
            return self._create_error_response(query, str(e))
    
    def _extract_search_terms(self, query: str) -> List[str]:
        """Extract search terms from query using pattern matching"""
        query_lower = query.lower()
        search_terms = []
        
        # Extract specific terms mentioned in query
        words = re.findall(r'\b\w+\b', query_lower)
        
        # Map words to field categories
        for word in words:
            for category, synonyms in self.field_mappings.items():
                if word in synonyms:
                    search_terms.append(category)
                    search_terms.extend(synonyms[:3])  # Add top synonyms
                    break
            else:
                # Add the word itself if it's meaningful
                if len(word) > 2 and word not in ['the', 'and', 'for', 'from', 'all', 'data', 'give', 'show', 'get']:
                    search_terms.append(word)
        
        # Remove duplicates while preserving order
        unique_terms = []
        for term in search_terms:
            if term not in unique_terms:
                unique_terms.append(term)
        
        return unique_terms[:10]  # Limit to top 10 terms
    
    def _extract_date_filters(self, query: str) -> Dict[str, Any]:
        """Extract date filters from query"""
        query_lower = query.lower()
        current_date = datetime.now().date()
        
        date_filters = {}
        
        # Check for month names
        months = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4,
            'may': 5, 'june': 6, 'july': 7, 'august': 8,
            'september': 9, 'october': 10, 'november': 11, 'december': 12,
            'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
            'jun': 6, 'jul': 7, 'aug': 8, 'sep': 9,
            'oct': 10, 'nov': 11, 'dec': 12
        }
        
        for month_name, month_num in months.items():
            if month_name in query_lower:
                # Default to current year
                year = current_date.year
                
                # Check if year is mentioned
                year_match = re.search(r'\b(20\d{2})\b', query)
                if year_match:
                    year = int(year_match.group(1))
                
                # Calculate month start and end dates
                start_date = date(year, month_num, 1)
                
                # Get last day of month
                if month_num == 12:
                    end_date = date(year + 1, 1, 1) - timedelta(days=1)
                else:
                    end_date = date(year, month_num + 1, 1) - timedelta(days=1)
                
                date_filters = {
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat(),
                    'type': 'month',
                    'description': f"{month_name.title()} {year}"
                }
                break
        
        # Check for relative dates
        if 'last 7 days' in query_lower or 'past week' in query_lower:
            start_date = current_date - timedelta(days=7)
            date_filters = {
                'start_date': start_date.isoformat(),
                'end_date': current_date.isoformat(),
                'type': 'last_7_days',
                'description': 'Last 7 days'
            }
        elif 'today' in query_lower:
            date_filters = {
                'start_date': current_date.isoformat(),
                'end_date': current_date.isoformat(),
                'type': 'today',
                'description': 'Today'
            }
        elif 'yesterday' in query_lower:
            yesterday = current_date - timedelta(days=1)
            date_filters = {
                'start_date': yesterday.isoformat(),
                'end_date': yesterday.isoformat(),
                'type': 'yesterday',
                'description': 'Yesterday'
            }
        
        return date_filters
    
    async def _execute_direct_sql(self, search_terms: List[str], date_filters: Dict[str, Any], 
                                sheet_id: str = None) -> Dict[str, Any]:
        """Execute direct SQL query against SQLite database"""
        try:
            with get_db_context() as db:
                # Build SQL query
                sql_parts = []
                params = {}
                
                # Base query
                sql_parts.append("""
                    SELECT 
                        sd.sheet_id,
                        sd.tab_name,
                        sd.row_index,
                        sd.row_data,
                        sd.synced_at,
                        sm.sheet_name
                    FROM sheets_data sd
                    LEFT JOIN sheets_metadata sm ON sd.sheet_id = sm.sheet_id
                    WHERE sd.row_index > 0
                """)
                
                # Add sheet filter if provided
                if sheet_id:
                    sql_parts.append("AND sd.sheet_id = :sheet_id")
                    params['sheet_id'] = sheet_id
                
                # Add search term filters
                if search_terms:
                    search_conditions = []
                    for i, term in enumerate(search_terms[:5]):  # Limit to 5 terms
                        param_name = f'search_term_{i}'
                        search_conditions.append(f"LOWER(sd.row_data) LIKE :{param_name}")
                        params[param_name] = f'%{term.lower()}%'
                    
                    if search_conditions:
                        sql_parts.append(f"AND ({' OR '.join(search_conditions)})")
                
                # Add date filters
                if date_filters.get('start_date') and date_filters.get('end_date'):
                    sql_parts.append("AND DATE(sd.synced_at) BETWEEN :start_date AND :end_date")
                    params['start_date'] = date_filters['start_date']
                    params['end_date'] = date_filters['end_date']
                
                # Add ordering and limit
                sql_parts.append("ORDER BY sd.synced_at DESC, sd.sheet_id, sd.tab_name, sd.row_index")
                sql_parts.append("LIMIT 1000")  # Reasonable limit
                
                # Combine SQL parts
                sql_query = " ".join(sql_parts)
                
                logger.info(f"🗄️  Executing SQL: {sql_query}")
                logger.info(f"📊 Parameters: {params}")
                
                # Execute query using SQLAlchemy text() for raw SQL
                from sqlalchemy import text
                result = db.execute(text(sql_query), params)
                rows = result.fetchall()
                
                logger.info(f"📊 SQL returned {len(rows)} rows")
                
                # Process results
                processed_data = []
                for row in rows:
                    try:
                        # Parse row data JSON
                        row_data_str = row[3]  # row_data column
                        if isinstance(row_data_str, str):
                            row_data = json.loads(row_data_str)
                        else:
                            row_data = row_data_str
                        
                        if isinstance(row_data, list):
                            # Convert list to dict with column indices
                            row_dict = {}
                            for i, value in enumerate(row_data):
                                if value and str(value).strip():
                                    row_dict[f"col_{i}"] = str(value).strip()
                            
                            processed_data.append({
                                "sheet_id": row[0],
                                "sheet_name": row[5] or "Unknown Sheet",
                                "tab_name": row[1],
                                "row_index": row[2],
                                "row_data": row_dict,
                                "created_at": row[4]
                            })
                    
                    except Exception as e:
                        logger.warning(f"Error processing row: {str(e)}")
                        continue
                
                return {
                    "success": True,
                    "data": processed_data,
                    "sql_query": sql_query,
                    "row_count": len(processed_data)
                }
                
        except Exception as e:
            logger.error(f"❌ SQL execution error: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "data": []
            }
    
    def _format_direct_response(self, query: str, data: List[Dict[str, Any]], 
                              search_terms: List[str]) -> str:
        """Format response without using AI/LLM"""
        if not data:
            return f"No data found in SQLite database for query: '{query}'"
        
        # Build response
        response_parts = []
        
        # Header
        response_parts.append(f"✅ Found {len(data)} entries in SQLite database for '{query}'")
        response_parts.append("")
        
        # Group by sheet and tab
        grouped_data = {}
        for item in data:
            sheet_key = f"{item['sheet_name']} - {item['tab_name']}"
            if sheet_key not in grouped_data:
                grouped_data[sheet_key] = []
            grouped_data[sheet_key].append(item)
        
        # Show results by group
        for sheet_tab, items in list(grouped_data.items())[:5]:  # Limit to 5 groups
            response_parts.append(f"📋 **{sheet_tab}** ({len(items)} entries):")
            
            for i, item in enumerate(items[:10], 1):  # Limit to 10 items per group
                row_data = item['row_data']
                
                # Find relevant data based on search terms
                relevant_data = []
                for col_key, col_value in row_data.items():
                    if any(term.lower() in col_value.lower() for term in search_terms):
                        relevant_data.append(f"{col_key}: {col_value}")
                
                if relevant_data:
                    response_parts.append(f"  {i}. {', '.join(relevant_data[:3])}")
                else:
                    # Show first few non-empty columns
                    first_cols = list(row_data.items())[:3]
                    col_display = [f"{k}: {v}" for k, v in first_cols]
                    response_parts.append(f"  {i}. {', '.join(col_display)}")
            
            if len(items) > 10:
                response_parts.append(f"  ... and {len(items) - 10} more entries")
            
            response_parts.append("")
        
        if len(grouped_data) > 5:
            response_parts.append(f"... and {len(grouped_data) - 5} more groups")
        
        # Add summary
        response_parts.append(f"📊 **Summary**: Retrieved {len(data)} records from SQLite database")
        response_parts.append(f"🔍 **Search terms**: {', '.join(search_terms[:5])}")
        
        return "\n".join(response_parts)
    
    def _create_error_response(self, query: str, error: str) -> Dict[str, Any]:
        """Create error response"""
        return {
            "success": False,
            "query": query,
            "error": error,
            "answer": f"Error processing query '{query}': {error}",
            "raw_data": {"results": []},
            "data_found": 0,
            "confidence": 0.0,
            "processing_method": "sqlite_direct_error"
        }


# Global instance
_sqlite_direct_processor = None

def get_sqlite_direct_processor() -> SQLiteDirectProcessor:
    """Get the global SQLite direct processor instance"""
    global _sqlite_direct_processor
    if _sqlite_direct_processor is None:
        _sqlite_direct_processor = SQLiteDirectProcessor()
    return _sqlite_direct_processor