"""
Intelligent Query Processor
Integrates the Query Analyzer with the existing universal query system
Provides enhanced query understanding and SQL generation capabilities
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from services.query_analyzer import get_query_analyzer
from services.universal_query_processor import get_universal_query_processor
from services.database import get_db_context

logger = logging.getLogger(__name__)


class IntelligentQueryProcessor:
    """
    Enhanced query processor that combines query analysis with universal processing
    Handles complex daily report questions with intelligent SQL generation
    """
    
    def __init__(self):
        """Initialize the intelligent query processor"""
        self.query_analyzer = get_query_analyzer()
        self.universal_processor = get_universal_query_processor()
        
        # Database schema information (can be dynamically loaded)
        self.schema_info = {
            "table_name": "sheets_data",
            "columns": [
                "id", "sheet_id", "tab_name", "row_index", "row_data", 
                "created_at", "updated_at", "hash_value"
            ]
        }
    
    async def process_intelligent_query(self, query: str, sheet_id: str = None) -> Dict[str, Any]:
        """
        Process query using intelligent analysis and SQL generation
        
        Args:
            query: Natural language query
            sheet_id: Optional sheet ID for context
            
        Returns:
            Enhanced query result with analysis and data
        """
        try:
            logger.info(f"🧠 Processing intelligent query: '{query}'")
            
            # Step 1: Analyze the query using the Query Analyzer
            analysis = self.query_analyzer.analyze_query(query, self.schema_info)
            
            logger.info(f"📋 Query Analysis:")
            logger.info(f"   Intent: {analysis['intent']}")
            logger.info(f"   SQL Possible: {analysis['sql_possible']}")
            logger.info(f"   Time Range: {analysis['time_range']}")
            logger.info(f"   Metrics: {analysis['metrics']}")
            
            # Step 2: Determine processing approach
            if analysis['sql_possible'] and analysis['sql_query']:
                # Use direct SQL approach for structured queries
                result = await self._process_with_sql(query, analysis, sheet_id)
            else:
                # Fallback to universal processor for complex/unstructured queries
                logger.info("🔄 Falling back to universal processor")
                result = await self.universal_processor.process_query(query, sheet_id)
                
                # Enhance with analysis information
                result['query_analysis'] = analysis
            
            # Step 3: Add intelligence metadata
            result['processing_method'] = 'intelligent_sql' if analysis['sql_possible'] else 'universal_fallback'
            result['query_intent'] = analysis['intent']
            result['time_range_detected'] = analysis['time_range']
            
            logger.info(f"✅ Intelligent processing complete: {result['processing_method']}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error in intelligent query processing: {str(e)}")
            
            # Ultimate fallback to universal processor
            try:
                result = await self.universal_processor.process_query(query, sheet_id)
                result['processing_method'] = 'fallback_universal'
                result['error'] = str(e)
                return result
            except Exception as fallback_error:
                return {
                    "success": False,
                    "query": query,
                    "error": f"Both intelligent and universal processing failed: {str(e)}, {str(fallback_error)}",
                    "answer": "I'm sorry, I couldn't process your query due to technical issues. Please try rephrasing your question.",
                    "processing_method": "failed"
                }
    
    async def _process_with_sql(self, query: str, analysis: Dict[str, Any], sheet_id: str = None) -> Dict[str, Any]:
        """
        Process query using direct SQL execution based on analysis
        """
        try:
            sql_query = analysis['sql_query']
            intent = analysis['intent']
            
            logger.info(f"🗄️  Executing SQL query: {sql_query}")
            
            # Execute the SQL query
            with get_db_context() as db:
                # Modify SQL query to work with our schema
                modified_sql = self._adapt_sql_to_schema(sql_query, sheet_id)
                logger.info(f"🔧 Adapted SQL: {modified_sql}")
                
                result = db.execute(modified_sql)
                rows = result.fetchall()
                
                logger.info(f"📊 SQL returned {len(rows)} rows")
            
            # Format the results based on intent
            formatted_data = self._format_sql_results(rows, intent, analysis)
            
            # Generate natural language response
            response_text = self._generate_response_from_sql_results(
                query, intent, formatted_data, analysis
            )
            
            return {
                "success": True,
                "query": query,
                "answer": response_text,
                "raw_data": formatted_data,
                "sql_query": modified_sql,
                "query_analysis": analysis,
                "data_found": len(rows),
                "confidence": 0.9  # High confidence for SQL-based results
            }
            
        except Exception as e:
            logger.error(f"❌ Error executing SQL query: {str(e)}")
            raise
    
    def _adapt_sql_to_schema(self, sql_query: str, sheet_id: str = None) -> str:
        """
        Adapt the generated SQL query to work with our actual database schema
        """
        # Replace generic table name with our actual table
        adapted_sql = sql_query.replace("daily_reports", "sheets_data")
        
        # Add sheet_id filter if provided
        if sheet_id:
            if "WHERE" in adapted_sql.upper():
                adapted_sql += f" AND sheet_id = '{sheet_id}'"
            else:
                adapted_sql += f" WHERE sheet_id = '{sheet_id}'"
        
        # Replace generic column names with our schema
        column_mappings = {
            "created_at": "created_at",
            "job_name": "tab_name",
            "status": "row_data",  # Will need JSON extraction
            "duration": "row_data",  # Will need JSON extraction
            "error_count": "COUNT(*)",
            "success_count": "COUNT(*)",
            "processed_count": "COUNT(*)"
        }
        
        for generic_col, actual_col in column_mappings.items():
            adapted_sql = adapted_sql.replace(generic_col, actual_col)
        
        return adapted_sql
    
    def _format_sql_results(self, rows: List, intent: str, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format SQL results based on the query intent
        """
        if not rows:
            return {"results": [], "summary": "No data found"}
        
        formatted_data = {
            "results": [],
            "summary": "",
            "total_rows": len(rows),
            "intent": intent
        }
        
        if intent == "COUNT":
            # Handle count results
            if len(rows) == 1 and len(rows[0]) == 1:
                count = rows[0][0]
                formatted_data["results"] = [{"count": count}]
                formatted_data["summary"] = f"Total count: {count}"
            else:
                # Multiple counts (e.g., grouped by date)
                for row in rows:
                    if len(row) >= 2:
                        formatted_data["results"].append({
                            "count": row[0],
                            "date": row[1] if len(row) > 1 else None
                        })
        
        elif intent in ["SUMMARY", "DETAILS", "STATUS"]:
            # Handle detailed results
            for row in rows:
                row_dict = {}
                for i, value in enumerate(row):
                    row_dict[f"column_{i}"] = value
                formatted_data["results"].append(row_dict)
            
            formatted_data["summary"] = f"Retrieved {len(rows)} records"
        
        elif intent == "ERROR":
            # Handle error-specific results
            error_count = len(rows)
            formatted_data["results"] = [{"error_count": error_count}]
            formatted_data["summary"] = f"Found {error_count} error records"
        
        elif intent == "TREND":
            # Handle trend results (usually time-series data)
            for row in rows:
                trend_point = {
                    "date": row[1] if len(row) > 1 else None,
                    "value": row[0]
                }
                formatted_data["results"].append(trend_point)
            
            formatted_data["summary"] = f"Trend data with {len(rows)} data points"
        
        return formatted_data
    
    def _generate_response_from_sql_results(self, query: str, intent: str, 
                                          formatted_data: Dict[str, Any], 
                                          analysis: Dict[str, Any]) -> str:
        """
        Generate natural language response from SQL results
        """
        results = formatted_data["results"]
        total_rows = formatted_data["total_rows"]
        
        if not results:
            return f"No data found for your query: '{query}'"
        
        # Generate response based on intent
        if intent == "COUNT":
            if len(results) == 1:
                count = results[0]["count"]
                time_desc = self._get_time_description(analysis["time_range"])
                return f"Found {count} records{time_desc}."
            else:
                # Multiple counts (grouped)
                response = f"Count results for '{query}':\n"
                for result in results[:10]:  # Limit to 10 for readability
                    count = result["count"]
                    date_str = result.get("date", "Unknown date")
                    response += f"• {date_str}: {count} records\n"
                
                if len(results) > 10:
                    response += f"... and {len(results) - 10} more entries"
                
                return response.strip()
        
        elif intent == "STATUS":
            time_desc = self._get_time_description(analysis["time_range"])
            return f"Status information{time_desc}: Found {total_rows} status records. " + \
                   f"Use 'show details' to see specific status information."
        
        elif intent == "SUMMARY":
            time_desc = self._get_time_description(analysis["time_range"])
            return f"Summary report{time_desc}: Retrieved {total_rows} records. " + \
                   f"The data includes information from your sheets and can be exported for detailed analysis."
        
        elif intent == "DETAILS":
            time_desc = self._get_time_description(analysis["time_range"])
            return f"Detailed data{time_desc}: Found {total_rows} records. " + \
                   f"The results have been processed and are available for review."
        
        elif intent == "ERROR":
            error_count = results[0].get("error_count", total_rows)
            time_desc = self._get_time_description(analysis["time_range"])
            return f"Error analysis{time_desc}: Found {error_count} error-related records. " + \
                   f"Review the data for specific error details."
        
        elif intent == "TREND":
            time_desc = self._get_time_description(analysis["time_range"])
            return f"Trend analysis{time_desc}: Generated {len(results)} data points. " + \
                   f"The trend data shows patterns over the specified time period."
        
        else:
            return f"Query processed successfully: Found {total_rows} records for '{query}'"
    
    def _get_time_description(self, time_range: Dict[str, Optional[str]]) -> str:
        """
        Generate human-readable time description
        """
        start = time_range.get("start")
        end = time_range.get("end")
        
        if not start and not end:
            return ""
        
        if start == end:
            return f" for {start}"
        elif start and end:
            return f" from {start} to {end}"
        elif start:
            return f" from {start}"
        elif end:
            return f" until {end}"
        
        return ""


# Global instance
_intelligent_query_processor = None

def get_intelligent_query_processor() -> IntelligentQueryProcessor:
    """Get the global intelligent query processor instance"""
    global _intelligent_query_processor
    if _intelligent_query_processor is None:
        _intelligent_query_processor = IntelligentQueryProcessor()
    return _intelligent_query_processor