"""
Precision Query Processor
Integrates UNIVERSAL DATABASE SEARCHER with existing query system
Ensures ZERO TOLERANCE for over-retrieval and under-filtering
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from services.universal_database_searcher import get_universal_database_searcher, SearchRequest
from services.query_analyzer import get_query_analyzer
from services.database import get_db_context

logger = logging.getLogger(__name__)


class PrecisionQueryProcessor:
    """
    Precision Query Processor that ensures exact data retrieval
    Combines query analysis with universal database searching
    ZERO TOLERANCE for over-retrieval or under-filtering
    """
    
    def __init__(self):
        """Initialize the precision query processor"""
        self.query_analyzer = get_query_analyzer()
        self.database_searcher = get_universal_database_searcher()
    
    async def process_precision_query(self, query: str, sheet_id: str = None) -> Dict[str, Any]:
        """
        Process query with precision requirements
        
        Args:
            query: Natural language query
            sheet_id: Optional sheet ID for context
            
        Returns:
            Precise query result with exact data only
        """
        try:
            logger.info(f"🎯 PRECISION QUERY PROCESSOR - Processing: '{query}'")
            
            # Step 1: Analyze query intent and structure
            analysis = self.query_analyzer.analyze_query(query)
            logger.info(f"📋 Query Analysis: Intent={analysis['intent']}, SQL Possible={analysis['sql_possible']}")
            
            # Step 2: Convert to SearchRequest format
            search_request = self._convert_to_search_request(query, analysis, sheet_id)
            
            if not search_request:
                return self._fail_response("Cannot convert query to precise search request", query)
            
            # Step 3: Execute precision database search
            search_result = self.database_searcher.search_database(search_request)
            
            if not search_result['success']:
                return self._fail_response(search_result['error'], query)
            
            # Step 4: Execute the generated SQL
            execution_result = await self._execute_precision_sql(search_result)
            
            if not execution_result['success']:
                return self._fail_response(execution_result['error'], query)
            
            # Step 5: Format precise response
            response = self._format_precision_response(query, search_result, execution_result)
            
            logger.info(f"✅ Precision query completed: {len(execution_result['data'])} exact results")
            
            return {
                "success": True,
                "query": query,
                "answer": response['formatted_answer'],
                "raw_data": execution_result['data'],
                "sql_query": search_result['sql_query'],
                "request_type": search_result['request_type'],
                "columns_selected": search_result['columns_selected'],
                "filters_applied": search_result['filters_applied'],
                "data_found": len(execution_result['data']),
                "confidence": 0.95,  # High confidence for precision queries
                "processing_method": "precision_database_search"
            }
            
        except Exception as e:
            logger.error(f"❌ Error in precision query processing: {str(e)}")
            return self._fail_response(f"Processing error: {str(e)}", query)
    
    def _convert_to_search_request(self, query: str, analysis: Dict[str, Any], 
                                 sheet_id: str = None) -> Optional[SearchRequest]:
        """
        Convert query analysis to SearchRequest format
        """
        try:
            # Extract entity from query
            entity = self._extract_entity(query, analysis)
            
            # Extract field from query
            field = self._extract_field(query, analysis)
            
            # Extract conditions
            conditions = {}
            
            # Add status conditions based on query
            query_lower = query.lower()
            if 'failed' in query_lower or 'error' in query_lower:
                conditions['status'] = 'FAILED'
            elif 'success' in query_lower or 'completed' in query_lower:
                conditions['status'] = 'SUCCESS'
            
            # Add sheet_id if provided
            if sheet_id:
                conditions['sheet_id'] = sheet_id
            
            # Extract time range
            time_range = analysis.get('time_range', {"start": None, "end": None})
            
            # Determine aggregation
            aggregation = None
            if analysis['intent'] in ['COUNT']:
                aggregation = 'count'
            
            return SearchRequest(
                intent=analysis['intent'].lower(),
                entity=entity,
                field=field,
                conditions=conditions,
                time_range=time_range,
                aggregation=aggregation,
                original_query=query
            )
            
        except Exception as e:
            logger.error(f"Error converting to search request: {str(e)}")
            return None
    
    def _extract_entity(self, query: str, analysis: Dict[str, Any]) -> Optional[str]:
        """Extract entity from query"""
        import re
        
        # Look for specific entity patterns
        entity_patterns = [
            r'(?:for|of)\s+([a-zA-Z0-9_\-\.]+)',
            r'job\s+([a-zA-Z0-9_\-]+)',
            r'file\s+([a-zA-Z0-9_\-\.]+)',
            r'sheet\s+([a-zA-Z0-9_\-]+)',
            r'tab\s+([a-zA-Z0-9_\-\s]+)'
        ]
        
        for pattern in entity_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return None
    
    def _extract_field(self, query: str, analysis: Dict[str, Any]) -> Optional[str]:
        """Extract field from query"""
        import re
        
        # Look for field patterns
        field_patterns = [
            r'what\s+is\s+(?:the\s+)?(\w+)',
            r'(?:show|get)\s+(?:the\s+)?(\w+)',
            r'(\w+)\s+(?:for|of)'
        ]
        
        for pattern in field_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                field = match.group(1).strip()
                # Validate field name
                if field not in ['what', 'is', 'the', 'show', 'get', 'for', 'of']:
                    return field
        
        return None
    
    async def _execute_precision_sql(self, search_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the precision SQL query
        """
        try:
            sql_query = search_result['sql_query']
            logger.info(f"🗄️  Executing precision SQL: {sql_query}")
            
            with get_db_context() as db:
                result = db.execute(sql_query)
                rows = result.fetchall()
                
                # Convert rows to list of dictionaries
                columns = search_result['columns_selected']
                data = []
                
                for row in rows:
                    row_dict = {}
                    for i, column in enumerate(columns):
                        if i < len(row):
                            row_dict[column] = row[i]
                    data.append(row_dict)
                
                logger.info(f"📊 SQL execution successful: {len(data)} rows returned")
                
                return {
                    "success": True,
                    "data": data,
                    "row_count": len(data)
                }
                
        except Exception as e:
            logger.error(f"❌ SQL execution error: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "data": []
            }
    
    def _format_precision_response(self, query: str, search_result: Dict[str, Any], 
                                 execution_result: Dict[str, Any]) -> Dict[str, str]:
        """
        Format precise response based on request type
        """
        data = execution_result['data']
        request_type = search_result['request_type']
        columns = search_result['columns_selected']
        
        if not data:
            return {
                "formatted_answer": f"No data found for your precise query: '{query}'"
            }
        
        # Format based on request type
        if request_type == "EXACT_FIELD_LOOKUP":
            # Single field lookup
            if len(data) == 1 and len(columns) == 1:
                field_name = columns[0]
                value = data[0][field_name]
                return {
                    "formatted_answer": f"The {field_name} is: {value}"
                }
            else:
                # Multiple results for field lookup
                field_name = columns[0]
                values = [str(row[field_name]) for row in data[:10]]
                answer = f"Found {len(data)} values for {field_name}:\n"
                for i, value in enumerate(values, 1):
                    answer += f"{i}. {value}\n"
                if len(data) > 10:
                    answer += f"... and {len(data) - 10} more results"
                return {"formatted_answer": answer.strip()}
        
        elif request_type == "ENTITY_SPECIFIC_RECORD":
            # Entity status or details
            if len(data) == 1:
                row = data[0]
                answer = "Entity details:\n"
                for column, value in row.items():
                    answer += f"• {column}: {value}\n"
                return {"formatted_answer": answer.strip()}
            else:
                answer = f"Found {len(data)} records:\n"
                for i, row in enumerate(data[:5], 1):
                    answer += f"{i}. "
                    for column, value in row.items():
                        answer += f"{column}: {value}, "
                    answer = answer.rstrip(", ") + "\n"
                if len(data) > 5:
                    answer += f"... and {len(data) - 5} more records"
                return {"formatted_answer": answer.strip()}
        
        elif request_type == "AGGREGATED_SPECIFIC":
            # Count or aggregation
            if len(data) == 1 and 'COUNT(*)' in data[0]:
                count = data[0]['COUNT(*)']
                return {
                    "formatted_answer": f"Count result: {count}"
                }
            else:
                return {
                    "formatted_answer": f"Aggregation result: {data}"
                }
        
        elif request_type == "CONDITIONAL_EXTRACTION":
            # Conditional data extraction
            answer = f"Found {len(data)} records matching conditions:\n"
            for i, row in enumerate(data[:10], 1):
                answer += f"{i}. "
                for column, value in row.items():
                    answer += f"{column}: {value}, "
                answer = answer.rstrip(", ") + "\n"
            if len(data) > 10:
                answer += f"... and {len(data) - 10} more records"
            return {"formatted_answer": answer.strip()}
        
        elif request_type == "RANGE_SCOPED_DETAILS":
            # Range scoped details
            answer = f"Found {len(data)} records in specified range:\n"
            for i, row in enumerate(data[:15], 1):
                answer += f"{i}. "
                for column, value in row.items():
                    answer += f"{column}: {value}, "
                answer = answer.rstrip(", ") + "\n"
            if len(data) > 15:
                answer += f"... and {len(data) - 15} more records"
            return {"formatted_answer": answer.strip()}
        
        else:
            # Default formatting
            return {
                "formatted_answer": f"Retrieved {len(data)} precise results for your query."
            }
    
    def _fail_response(self, error: str, query: str) -> Dict[str, Any]:
        """Generate failure response"""
        return {
            "success": False,
            "query": query,
            "error": error,
            "answer": f"I couldn't process your query with the required precision: {error}",
            "raw_data": [],
            "sql_query": None,
            "data_found": 0,
            "confidence": 0.0,
            "processing_method": "precision_failed"
        }


# Global instance
_precision_query_processor = None

def get_precision_query_processor() -> PrecisionQueryProcessor:
    """Get the global precision query processor instance"""
    global _precision_query_processor
    if _precision_query_processor is None:
        _precision_query_processor = PrecisionQueryProcessor()
    return _precision_query_processor