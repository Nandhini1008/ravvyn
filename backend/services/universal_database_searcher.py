"""
UNIVERSAL DATABASE SEARCHER Agent
Retrieves EXACT data requested by user from relational database
ZERO TOLERANCE for over-retrieval or under-filtering
Precision is mandatory. Over-retrieval is a failure.
"""

import re
import json
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, date
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SearchRequest:
    """Structured query intent input"""
    intent: str
    entity: Optional[str]
    field: Optional[str]
    conditions: Dict[str, Any]
    time_range: Dict[str, Optional[str]]
    aggregation: Optional[str]
    original_query: str


class UniversalDatabaseSearcher:
    """
    UNIVERSAL DATABASE SEARCHER agent for precise data retrieval
    Follows strict 8-step process with zero tolerance for over-retrieval
    """
    
    def __init__(self):
        """Initialize the universal database searcher"""
        
        # Database schema mapping (should be loaded dynamically)
        self.schema_mapping = {
            "sheets_data": {
                "columns": ["id", "sheet_id", "tab_name", "row_index", "row_data", "created_at", "updated_at"],
                "entity_columns": ["sheet_id", "tab_name"],
                "time_columns": ["created_at", "updated_at"],
                "data_column": "row_data"
            },
            "job_reports": {
                "columns": ["id", "job_name", "status", "error_message", "run_date", "duration"],
                "entity_columns": ["job_name"],
                "time_columns": ["run_date"],
                "status_column": "status"
            },
            "file_reports": {
                "columns": ["id", "file_name", "status", "size", "processed_date"],
                "entity_columns": ["file_name"],
                "time_columns": ["processed_date"],
                "status_column": "status"
            }
        }
        
        # Request granularity patterns
        self.granularity_patterns = {
            'EXACT_FIELD_LOOKUP': [
                r'what\s+is\s+(?:the\s+)?(\w+)\s+(?:for|of)\s+(.+)',
                r'(?:show|get)\s+(?:the\s+)?(\w+)\s+(?:for|of)\s+(.+)',
                r'(\w+)\s+(?:for|of)\s+(.+)'
            ],
            'ENTITY_SPECIFIC_RECORD': [
                r'(?:status|details|info)\s+(?:of|for)\s+(.+)',
                r'(?:show|get)\s+(.+)\s+(?:status|details|info)',
                r'(.+)\s+(?:status|details|record)'
            ],
            'CONDITIONAL_EXTRACTION': [
                r'(?:failed|error|success)\s+(.+)\s+(?:for|on|in)\s+(.+)',
                r'(.+)\s+(?:where|with|that)\s+(.+)',
                r'(.+)\s+(?:on|in|during)\s+(.+)'
            ],
            'AGGREGATED_SPECIFIC': [
                r'(?:count|total|sum)\s+(?:of\s+)?(.+)\s+(?:for|of)\s+(.+)',
                r'(?:how\s+many|number\s+of)\s+(.+)\s+(?:for|of)\s+(.+)'
            ],
            'RANGE_SCOPED_DETAILS': [
                r'(?:all|list)\s+(.+)\s+(?:between|from|in)\s+(.+)',
                r'(.+)\s+(?:in\s+last|during|between)\s+(.+)'
            ]
        }
    
    def search_database(self, request: SearchRequest) -> Dict[str, Any]:
        """
        Main entry point for precise database search
        Follows strict 8-step process
        
        Args:
            request: Structured query intent
            
        Returns:
            Precise search result or failure
        """
        try:
            logger.info(f"🔍 UNIVERSAL DATABASE SEARCHER - Processing: '{request.original_query}'")
            
            # STEP 1: Request Granularity Classification
            granularity = self._classify_granularity(request)
            logger.info(f"📋 STEP 1 - Granularity: {granularity}")
            
            if granularity == "FAIL":
                return self._fail_response("Request scope too broad or unclear")
            
            # STEP 2: Column Selection (Zero Tolerance)
            columns = self._select_columns(request, granularity)
            logger.info(f"📊 STEP 2 - Columns: {columns}")
            
            if not columns:
                return self._fail_response("Cannot determine required columns")
            
            # STEP 3: Entity & Value Matching
            entity_filters = self._match_entities(request)
            logger.info(f"🎯 STEP 3 - Entity Filters: {entity_filters}")
            
            # STEP 4: Time & Condition Enforcement
            time_filters = self._enforce_time_conditions(request)
            condition_filters = self._enforce_conditions(request)
            logger.info(f"⏰ STEP 4 - Time: {time_filters}, Conditions: {condition_filters}")
            
            # STEP 5: Aggregation Control
            aggregation = self._control_aggregation(request, granularity)
            logger.info(f"📈 STEP 5 - Aggregation: {aggregation}")
            
            # STEP 6: Multi-Table Safety
            table = self._select_table(request)
            logger.info(f"🗄️  STEP 6 - Table: {table}")
            
            if not table:
                return self._fail_response("Cannot determine target table")
            
            # STEP 7: Result Scope Validation
            validation_result = self._validate_scope(columns, entity_filters, time_filters, condition_filters)
            logger.info(f"✅ STEP 7 - Validation: {validation_result}")
            
            if not validation_result["valid"]:
                return self._fail_response(f"Scope validation failed: {validation_result['reason']}")
            
            # STEP 8: Generate Final SQL
            sql_result = self._generate_sql(
                table, columns, entity_filters, time_filters, 
                condition_filters, aggregation, granularity
            )
            
            logger.info(f"🗄️  STEP 8 - SQL Generated: {sql_result['sql_query']}")
            
            return {
                "success": True,
                "request_type": granularity,
                "table_used": table,
                "columns_selected": columns,
                "filters_applied": {**entity_filters, **time_filters, **condition_filters},
                "group_by": aggregation.get("group_by", []) if aggregation else [],
                "limit": aggregation.get("limit") if aggregation else None,
                "sql_query": sql_result["sql_query"]
            }
            
        except Exception as e:
            logger.error(f"❌ Database searcher error: {str(e)}")
            return self._fail_response(f"Processing error: {str(e)}")
    
    def _classify_granularity(self, request: SearchRequest) -> str:
        """
        STEP 1: Classify request into ONE granularity category
        """
        query = request.original_query.lower()
        
        # Check each granularity pattern
        for granularity, patterns in self.granularity_patterns.items():
            for pattern in patterns:
                if re.search(pattern, query, re.IGNORECASE):
                    logger.info(f"📋 Matched pattern '{pattern}' for {granularity}")
                    return granularity
        
        # Special cases for broad requests that should FAIL
        broad_indicators = [
            'all details', 'everything', 'all data', 'show all', 'give me all',
            'complete information', 'full report', 'entire dataset'
        ]
        
        if any(indicator in query for indicator in broad_indicators):
            logger.warning(f"❌ Broad request detected: {query}")
            return "FAIL"
        
        # Default to most specific if unclear
        if request.field and request.entity:
            return "EXACT_FIELD_LOOKUP"
        elif request.entity:
            return "ENTITY_SPECIFIC_RECORD"
        else:
            return "FAIL"
    
    def _select_columns(self, request: SearchRequest, granularity: str) -> List[str]:
        """
        STEP 2: Select ONLY explicitly requested columns (Zero Tolerance)
        """
        columns = []
        
        if granularity == "EXACT_FIELD_LOOKUP":
            # User asks for specific field - return ONLY that field
            if request.field:
                columns = [request.field]
            else:
                # Extract field from query
                field_match = re.search(r'what\s+is\s+(?:the\s+)?(\w+)', request.original_query, re.IGNORECASE)
                if field_match:
                    columns = [field_match.group(1)]
        
        elif granularity == "ENTITY_SPECIFIC_RECORD":
            # User asks about entity - return minimal identifying columns
            if 'status' in request.original_query.lower():
                columns = ['status']
            elif 'details' in request.original_query.lower():
                columns = ['status', 'error_message']  # Minimal details
            else:
                columns = ['status']  # Default minimal
        
        elif granularity == "CONDITIONAL_EXTRACTION":
            # User asks for data under conditions - return requested fields only
            if request.field:
                columns = [request.field]
            else:
                # Infer from conditions
                if 'error' in request.original_query.lower():
                    columns = ['error_message', 'status']
                elif 'failed' in request.original_query.lower():
                    columns = ['status', 'error_message']
                else:
                    columns = ['status']
        
        elif granularity == "AGGREGATED_SPECIFIC":
            # User asks for aggregation - return COUNT(*) or specific aggregate
            columns = ['COUNT(*)']
        
        elif granularity == "RANGE_SCOPED_DETAILS":
            # User asks for range data - return minimal columns
            columns = ['status', 'created_at']
        
        # Validate columns exist in schema
        validated_columns = []
        for col in columns:
            if col == 'COUNT(*)' or self._column_exists(col):
                validated_columns.append(col)
            else:
                logger.warning(f"⚠️  Column '{col}' not found in schema")
        
        return validated_columns
    
    def _match_entities(self, request: SearchRequest) -> Dict[str, str]:
        """
        STEP 3: Apply STRICT equality filters for specific values
        """
        filters = {}
        
        if request.entity:
            # Determine entity column based on entity type
            if self._looks_like_job_name(request.entity):
                filters['job_name'] = request.entity
            elif self._looks_like_file_name(request.entity):
                filters['file_name'] = request.entity
            elif self._looks_like_sheet_id(request.entity):
                filters['sheet_id'] = request.entity
            elif self._looks_like_tab_name(request.entity):
                filters['tab_name'] = request.entity
            else:
                # Try to infer from context
                query_lower = request.original_query.lower()
                if 'job' in query_lower:
                    filters['job_name'] = request.entity
                elif 'file' in query_lower:
                    filters['file_name'] = request.entity
                elif 'sheet' in query_lower:
                    filters['sheet_id'] = request.entity
                elif 'tab' in query_lower:
                    filters['tab_name'] = request.entity
        
        return filters
    
    def _enforce_time_conditions(self, request: SearchRequest) -> Dict[str, str]:
        """
        STEP 4: Apply ALL time constraints (mandatory if present)
        """
        filters = {}
        
        if request.time_range.get('start') and request.time_range.get('end'):
            if request.time_range['start'] == request.time_range['end']:
                # Single date
                filters['DATE(created_at)'] = f"= '{request.time_range['start']}'"
            else:
                # Date range
                filters['DATE(created_at)'] = f"BETWEEN '{request.time_range['start']}' AND '{request.time_range['end']}'"
        elif request.time_range.get('start'):
            filters['DATE(created_at)'] = f">= '{request.time_range['start']}'"
        elif request.time_range.get('end'):
            filters['DATE(created_at)'] = f"<= '{request.time_range['end']}'"
        
        # Handle relative dates
        query_lower = request.original_query.lower()
        if 'today' in query_lower:
            filters['DATE(created_at)'] = "= CURRENT_DATE"
        elif 'yesterday' in query_lower:
            filters['DATE(created_at)'] = "= DATE('now', '-1 day')"
        
        return filters
    
    def _enforce_conditions(self, request: SearchRequest) -> Dict[str, str]:
        """
        STEP 4: Apply ALL condition constraints
        """
        filters = {}
        
        # Status conditions
        query_lower = request.original_query.lower()
        if 'failed' in query_lower or 'error' in query_lower:
            filters['status'] = 'FAILED'
        elif 'success' in query_lower or 'completed' in query_lower:
            filters['status'] = 'SUCCESS'
        elif 'running' in query_lower:
            filters['status'] = 'RUNNING'
        elif 'pending' in query_lower:
            filters['status'] = 'PENDING'
        
        # Add explicit conditions from request
        for key, value in request.conditions.items():
            filters[key] = value
        
        return filters
    
    def _control_aggregation(self, request: SearchRequest, granularity: str) -> Optional[Dict[str, Any]]:
        """
        STEP 5: Control aggregation with strict rules
        """
        if granularity != "AGGREGATED_SPECIFIC":
            return None
        
        aggregation = {}
        
        # Determine aggregation type
        query_lower = request.original_query.lower()
        if 'count' in query_lower or 'how many' in query_lower:
            aggregation['type'] = 'COUNT'
        elif 'sum' in query_lower or 'total' in query_lower:
            aggregation['type'] = 'SUM'
        else:
            aggregation['type'] = 'COUNT'  # Default
        
        # Determine GROUP BY - only entity explicitly requested
        if request.entity and 'for' in query_lower:
            # "count of errors for job A" - GROUP BY job_name
            if self._looks_like_job_name(request.entity):
                aggregation['group_by'] = ['job_name']
            elif self._looks_like_file_name(request.entity):
                aggregation['group_by'] = ['file_name']
        else:
            # No grouping for specific entity queries
            aggregation['group_by'] = []
        
        return aggregation
    
    def _select_table(self, request: SearchRequest) -> Optional[str]:
        """
        STEP 6: Select single required table (no joins unless explicit)
        """
        query_lower = request.original_query.lower()
        
        # Determine table based on context
        if any(word in query_lower for word in ['job', 'pipeline', 'task']):
            return 'job_reports'
        elif any(word in query_lower for word in ['file', 'document']):
            return 'file_reports'
        elif any(word in query_lower for word in ['sheet', 'tab', 'data']):
            return 'sheets_data'
        
        # Default to most common table
        return 'sheets_data'
    
    def _validate_scope(self, columns: List[str], entity_filters: Dict[str, str], 
                       time_filters: Dict[str, str], condition_filters: Dict[str, str]) -> Dict[str, Any]:
        """
        STEP 7: Validate result scope before finalizing
        """
        # Check if query is too broad
        if not entity_filters and not time_filters and not condition_filters:
            return {
                "valid": False,
                "reason": "No filters applied - would return entire table"
            }
        
        # Check if columns are justified
        if not columns:
            return {
                "valid": False,
                "reason": "No columns selected"
            }
        
        # Check for over-fetching
        if len(columns) > 5 and 'COUNT(*)' not in columns:
            return {
                "valid": False,
                "reason": "Too many columns selected - potential over-fetching"
            }
        
        return {"valid": True, "reason": "Scope validation passed"}
    
    def _generate_sql(self, table: str, columns: List[str], entity_filters: Dict[str, str],
                     time_filters: Dict[str, str], condition_filters: Dict[str, str],
                     aggregation: Optional[Dict[str, Any]], granularity: str) -> Dict[str, str]:
        """
        STEP 8: Generate final SQL query
        """
        # Build SELECT clause
        if aggregation and aggregation['type'] == 'COUNT':
            select_clause = "SELECT COUNT(*)"
        else:
            select_clause = f"SELECT {', '.join(columns)}"
        
        # Build FROM clause
        from_clause = f"FROM {table}"
        
        # Build WHERE clause
        where_conditions = []
        
        # Add entity filters
        for column, value in entity_filters.items():
            where_conditions.append(f"{column} = '{value}'")
        
        # Add time filters
        for column, condition in time_filters.items():
            where_conditions.append(f"{column} {condition}")
        
        # Add condition filters
        for column, value in condition_filters.items():
            where_conditions.append(f"{column} = '{value}'")
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Build GROUP BY clause
        group_by_clause = ""
        if aggregation and aggregation.get('group_by'):
            group_by_clause = f"GROUP BY {', '.join(aggregation['group_by'])}"
        
        # Build ORDER BY clause (minimal)
        order_by_clause = ""
        if granularity == "RANGE_SCOPED_DETAILS":
            order_by_clause = "ORDER BY created_at DESC"
        
        # Build LIMIT clause (conservative)
        limit_clause = ""
        if granularity in ["ENTITY_SPECIFIC_RECORD", "CONDITIONAL_EXTRACTION"]:
            limit_clause = "LIMIT 100"  # Prevent runaway queries
        
        # Combine all clauses
        sql_parts = [select_clause, from_clause, where_clause, group_by_clause, order_by_clause, limit_clause]
        sql_query = " ".join(part for part in sql_parts if part)
        
        return {"sql_query": sql_query}
    
    def _fail_response(self, reason: str) -> Dict[str, Any]:
        """Generate failure response"""
        return {
            "success": False,
            "error": reason,
            "request_type": "FAILED",
            "table_used": None,
            "columns_selected": [],
            "filters_applied": {},
            "group_by": [],
            "limit": None,
            "sql_query": None
        }
    
    # Helper methods
    def _column_exists(self, column: str) -> bool:
        """Check if column exists in any table schema"""
        for table_schema in self.schema_mapping.values():
            if column in table_schema["columns"]:
                return True
        return False
    
    def _looks_like_job_name(self, entity: str) -> bool:
        """Check if entity looks like a job name"""
        job_indicators = ['job', 'pipeline', 'task', 'process']
        return any(indicator in entity.lower() for indicator in job_indicators)
    
    def _looks_like_file_name(self, entity: str) -> bool:
        """Check if entity looks like a file name"""
        return '.' in entity or 'file' in entity.lower()
    
    def _looks_like_sheet_id(self, entity: str) -> bool:
        """Check if entity looks like a sheet ID"""
        return len(entity) > 20 and any(c.isalnum() for c in entity)
    
    def _looks_like_tab_name(self, entity: str) -> bool:
        """Check if entity looks like a tab name"""
        tab_indicators = ['tab', 'sheet', 'worksheet']
        return any(indicator in entity.lower() for indicator in tab_indicators)


# Global instance
_universal_database_searcher = None

def get_universal_database_searcher() -> UniversalDatabaseSearcher:
    """Get the global universal database searcher instance"""
    global _universal_database_searcher
    if _universal_database_searcher is None:
        _universal_database_searcher = UniversalDatabaseSearcher()
    return _universal_database_searcher