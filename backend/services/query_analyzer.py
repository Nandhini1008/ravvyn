"""
Query Understanding and SQL Planning Agent
Specialized in interpreting DAILY REPORT questions over SQLite database
Handles intent classification, time normalization, and SQL query generation
"""

import re
import json
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta, date
from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta
import calendar

logger = logging.getLogger(__name__)


class QueryAnalyzer:
    """
    Expert Query Understanding and SQL Planning agent for DAILY REPORT questions
    Follows strict 6-step process for safe SQL generation
    """
    
    def __init__(self):
        """Initialize the query analyzer with patterns and mappings"""
        
        # Intent classification patterns
        self.intent_patterns = {
            'COUNT': [
                r'\b(how many|count|total|number of|sum)\b',
                r'\b(totals?|numbers?)\b'
            ],
            'STATUS': [
                r'\b(status|health|completion|success|failure|failed|completed)\b',
                r'\b(running|pending|active|inactive)\b'
            ],
            'SUMMARY': [
                r'\b(summary|report|overview|daily report|monthly report)\b',
                r'\b(give me.*report|show.*summary)\b'
            ],
            'DETAILS': [
                r'\b(details?|show|list|display|get)\b',
                r'\b(row|column|record|entry)\b'
            ],
            'ERROR': [
                r'\b(error|failure|issue|problem|bug|exception)\b',
                r'\b(failed|broken|crash)\b'
            ],
            'TREND': [
                r'\b(compare|comparison|trend|vs|versus|against)\b',
                r'\b(increase|decrease|change|difference)\b'
            ],
            'TIME_BASED': [
                r'\b(today|yesterday|last|this|current|recent)\b',
                r'\b(date|time|when|during|between|from.*to)\b'
            ]
        }
        
        # Time expression patterns
        self.time_patterns = {
            'relative_dates': {
                r'\btoday\b': 0,
                r'\byesterday\b': -1,
                r'\blast run\b': 'latest',
                r'\brecent\b': -1,
                r'\blast week\b': -7
            },
            'explicit_dates': [
                r'\b(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})\b',
                r'\b(\d{4})-(\d{1,2})-(\d{1,2})\b',
                r'\b(\d{1,2})/(\d{1,2})/(\d{4})\b',
                r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})\b'
            ],
            'months': [
                r'\b(January|February|March|April|May|June|July|August|September|October|November|December)\b',
                r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})\b',
                r'\blast month\b',
                r'\bthis month\b'
            ],
            'date_ranges': [
                r'\bfrom\s+(.+?)\s+to\s+(.+?)\b',
                r'\bbetween\s+(.+?)\s+and\s+(.+?)\b',
                r'(.+?)\s*[–-]\s*(.+?)'
            ],
            'years': [
                r'\b(\d{4})\b',
                r'\bQ([1-4])\s+(\d{4})\b',
                r'\blast year\b'
            ]
        }
        
        # Metric mappings
        self.metric_mappings = {
            'count': ['count', 'total', 'number', 'sum'],
            'duration': ['duration', 'time', 'elapsed'],
            'error': ['errors', 'failures', 'issues', 'problems'],
            'success': ['success', 'completed', 'successful'],
            'processed': ['processed', 'ingested', 'handled']
        }
        
        # Month name mappings
        self.month_names = {
            'january': 1, 'jan': 1,
            'february': 2, 'feb': 2,
            'march': 3, 'mar': 3,
            'april': 4, 'apr': 4,
            'may': 5,
            'june': 6, 'jun': 6,
            'july': 7, 'jul': 7,
            'august': 8, 'aug': 8,
            'september': 9, 'sep': 9,
            'october': 10, 'oct': 10,
            'november': 11, 'nov': 11,
            'december': 12, 'dec': 12
        }
    
    def analyze_query(self, query: str, schema_info: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Main entry point for query analysis
        Follows the strict 6-step process
        
        Args:
            query: User's natural language query
            schema_info: Database schema information
            
        Returns:
            Structured analysis result in JSON format
        """
        try:
            logger.info(f"🔍 Analyzing query: '{query}'")
            
            # STEP 1: Intent Classification
            intent = self._classify_intent(query)
            logger.info(f"📋 Intent classified as: {intent}")
            
            # STEP 2: Time Extraction & Normalization
            time_range = self._extract_and_normalize_time(query)
            logger.info(f"⏰ Time range: {time_range}")
            
            # STEP 3: Metric & Entity Extraction
            metrics, entities, conditions = self._extract_metrics_and_entities(query)
            logger.info(f"📊 Metrics: {metrics}, Entities: {entities}, Conditions: {conditions}")
            
            # STEP 4: Schema Mapping
            mapped_columns, unmapped_terms = self._map_to_schema(metrics, entities, schema_info)
            logger.info(f"🗄️  Mapped columns: {mapped_columns}, Unmapped: {unmapped_terms}")
            
            # STEP 5: Query Feasibility Check
            sql_possible, reason = self._check_feasibility(intent, time_range, mapped_columns, schema_info)
            logger.info(f"✅ SQL possible: {sql_possible}, Reason: {reason}")
            
            # STEP 6: Generate Result
            result = self._generate_result(
                intent, time_range, metrics, conditions, 
                mapped_columns, sql_possible, reason, schema_info
            )
            
            logger.info(f"🎯 Analysis complete: {result['intent']}, SQL possible: {result['sql_possible']}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error analyzing query: {str(e)}")
            return {
                "intent": "UNKNOWN",
                "time_range": {"start": None, "end": None},
                "metrics": [],
                "filters": {},
                "group_by": [],
                "order_by": None,
                "limit": None,
                "sql_possible": False,
                "reason_if_not_possible": f"Analysis error: {str(e)}",
                "sql_query": None
            }
    
    def _classify_intent(self, query: str) -> str:
        """
        STEP 1: Classify the question into exactly one intent
        """
        query_lower = query.lower()
        intent_scores = {}
        
        # Score each intent based on pattern matches
        for intent, patterns in self.intent_patterns.items():
            score = 0
            for pattern in patterns:
                matches = len(re.findall(pattern, query_lower))
                score += matches
            
            if score > 0:
                intent_scores[intent] = score
        
        # Handle special cases
        if not intent_scores:
            return "UNKNOWN"
        
        # If multiple intents, choose the primary one
        if len(intent_scores) > 1:
            # Priority order for conflicting intents
            priority_order = ['COUNT', 'ERROR', 'STATUS', 'SUMMARY', 'DETAILS', 'TREND', 'TIME_BASED']
            
            for intent in priority_order:
                if intent in intent_scores:
                    return intent
        
        # Return the highest scoring intent
        return max(intent_scores, key=intent_scores.get)
    
    def _extract_and_normalize_time(self, query: str) -> Dict[str, Optional[str]]:
        """
        STEP 2: Extract and normalize time expressions
        """
        query_lower = query.lower()
        current_date = datetime.now().date()
        current_year = current_date.year
        
        # Check for relative dates
        for pattern, offset in self.time_patterns['relative_dates'].items():
            if re.search(pattern, query_lower):
                if offset == 'latest':
                    return {"start": None, "end": None}  # Will be handled by SQL
                else:
                    target_date = current_date + timedelta(days=offset)
                    return {
                        "start": target_date.isoformat(),
                        "end": target_date.isoformat()
                    }
        
        # Check for explicit dates
        for pattern in self.time_patterns['explicit_dates']:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                try:
                    date_str = match.group(0)
                    parsed_date = parse_date(date_str, default=datetime(current_year, 1, 1))
                    return {
                        "start": parsed_date.date().isoformat(),
                        "end": parsed_date.date().isoformat()
                    }
                except:
                    continue
        
        # Check for months
        for pattern in self.time_patterns['months']:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                month_str = match.group(0).lower()
                
                if 'last month' in month_str:
                    last_month = current_date.replace(day=1) - timedelta(days=1)
                    start_date = last_month.replace(day=1)
                    end_date = last_month
                elif 'this month' in month_str:
                    start_date = current_date.replace(day=1)
                    end_date = current_date
                else:
                    # Extract month name and year
                    for month_name, month_num in self.month_names.items():
                        if month_name in month_str:
                            year_match = re.search(r'\b(\d{4})\b', month_str)
                            year = int(year_match.group(1)) if year_match else current_year
                            
                            start_date = date(year, month_num, 1)
                            last_day = calendar.monthrange(year, month_num)[1]
                            end_date = date(year, month_num, last_day)
                            break
                    else:
                        continue
                
                return {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat()
                }
        
        # Check for date ranges
        for pattern in self.time_patterns['date_ranges']:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                try:
                    start_str = match.group(1).strip()
                    end_str = match.group(2).strip()
                    
                    start_date = parse_date(start_str, default=datetime(current_year, 1, 1)).date()
                    end_date = parse_date(end_str, default=datetime(current_year, 1, 1)).date()
                    
                    return {
                        "start": start_date.isoformat(),
                        "end": end_date.isoformat()
                    }
                except:
                    continue
        
        # Check for years
        for pattern in self.time_patterns['years']:
            match = re.search(pattern, query)
            if match:
                if 'last year' in query_lower:
                    year = current_year - 1
                elif match.group(0).startswith('Q'):
                    # Quarter handling
                    quarter = int(match.group(1))
                    year = int(match.group(2))
                    start_month = (quarter - 1) * 3 + 1
                    end_month = quarter * 3
                    
                    start_date = date(year, start_month, 1)
                    last_day = calendar.monthrange(year, end_month)[1]
                    end_date = date(year, end_month, last_day)
                    
                    return {
                        "start": start_date.isoformat(),
                        "end": end_date.isoformat()
                    }
                else:
                    year = int(match.group(0))
                
                return {
                    "start": date(year, 1, 1).isoformat(),
                    "end": date(year, 12, 31).isoformat()
                }
        
        # Default: assume current date if no time mentioned
        return {
            "start": current_date.isoformat(),
            "end": current_date.isoformat()
        }
    
    def _extract_metrics_and_entities(self, query: str) -> Tuple[List[str], List[str], Dict[str, str]]:
        """
        STEP 3: Extract and normalize metrics, entities, and conditions
        """
        query_lower = query.lower()
        
        # Extract metrics
        metrics = []
        for metric, synonyms in self.metric_mappings.items():
            for synonym in synonyms:
                if synonym in query_lower:
                    metrics.append(metric)
                    break
        
        # Extract entities (common database entities)
        entities = []
        entity_patterns = [
            r'\b(jobs?|pipelines?|tasks?|files?|records?|users?|services?)\b',
            r'\b(processes?|operations?|transactions?|requests?)\b'
        ]
        
        for pattern in entity_patterns:
            matches = re.findall(pattern, query_lower)
            entities.extend(matches)
        
        # Extract conditions
        conditions = {}
        condition_patterns = {
            'success': r'\b(success|successful|completed)\b',
            'failed': r'\b(failed|failure|error)\b',
            'pending': r'\b(pending|waiting|queued)\b',
            'running': r'\b(running|active|in progress)\b'
        }
        
        for condition, pattern in condition_patterns.items():
            if re.search(pattern, query_lower):
                conditions['status'] = condition
        
        return metrics, entities, conditions
    
    def _map_to_schema(self, metrics: List[str], entities: List[str], 
                      schema_info: Dict[str, Any] = None) -> Tuple[Dict[str, str], List[str]]:
        """
        STEP 4: Map extracted terms to known SQLite columns
        """
        mapped_columns = {}
        unmapped_terms = []
        
        if not schema_info:
            # Default schema mapping for common patterns
            default_mappings = {
                'count': 'COUNT(*)',
                'duration': 'duration',
                'error': 'error_count',
                'success': 'success_count',
                'processed': 'processed_count',
                'jobs': 'job_name',
                'tasks': 'task_name',
                'files': 'file_name',
                'records': 'record_id'
            }
            
            for term in metrics + entities:
                if term in default_mappings:
                    mapped_columns[term] = default_mappings[term]
                else:
                    unmapped_terms.append(term)
        else:
            # Use actual schema information
            available_columns = schema_info.get('columns', [])
            
            for term in metrics + entities:
                # Try exact match first
                if term in available_columns:
                    mapped_columns[term] = term
                else:
                    # Try fuzzy matching
                    for col in available_columns:
                        if term in col.lower() or col.lower() in term:
                            mapped_columns[term] = col
                            break
                    else:
                        unmapped_terms.append(term)
        
        return mapped_columns, unmapped_terms
    
    def _check_feasibility(self, intent: str, time_range: Dict[str, Optional[str]], 
                          mapped_columns: Dict[str, str], 
                          schema_info: Dict[str, Any] = None) -> Tuple[bool, str]:
        """
        STEP 5: Validate query feasibility
        """
        # Check if we have unmapped critical terms
        if not mapped_columns and intent not in ['SUMMARY', 'UNKNOWN']:
            return False, "No metrics or entities could be mapped to database schema"
        
        # Check for time column existence
        if time_range['start'] or time_range['end']:
            time_columns = ['created_at', 'run_date', 'timestamp', 'date', 'time']
            
            if schema_info:
                available_columns = [col.lower() for col in schema_info.get('columns', [])]
                has_time_column = any(tc in available_columns for tc in time_columns)
            else:
                has_time_column = True  # Assume time column exists
            
            if not has_time_column:
                return False, "No time column found in schema for date filtering"
        
        # Check intent-specific requirements
        if intent == 'COUNT' and not any('count' in col.lower() for col in mapped_columns.values()):
            # COUNT intent should have countable metrics
            pass  # Can use COUNT(*) as fallback
        
        if intent == 'UNKNOWN':
            return False, "Query intent is unclear or unsafe"
        
        return True, "Query is feasible"
    
    def _generate_result(self, intent: str, time_range: Dict[str, Optional[str]], 
                        metrics: List[str], conditions: Dict[str, str],
                        mapped_columns: Dict[str, str], sql_possible: bool, 
                        reason: str, schema_info: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        STEP 6: Generate the final result in strict JSON format
        """
        result = {
            "intent": intent,
            "time_range": time_range,
            "metrics": metrics,
            "filters": conditions,
            "group_by": [],
            "order_by": None,
            "limit": None,
            "sql_possible": sql_possible,
            "reason_if_not_possible": reason if not sql_possible else None,
            "sql_query": None
        }
        
        # Generate SQL query if possible
        if sql_possible:
            sql_query = self._generate_sql_query(
                intent, time_range, metrics, conditions, mapped_columns, schema_info
            )
            result["sql_query"] = sql_query
            
            # Set additional query parameters based on intent
            if intent == 'COUNT':
                result["group_by"] = ["date"] if time_range['start'] != time_range['end'] else []
            elif intent == 'TREND':
                result["group_by"] = ["date"]
                result["order_by"] = "date"
            elif intent == 'DETAILS':
                result["limit"] = 100  # Reasonable limit for details
                result["order_by"] = "created_at DESC"
        
        return result
    
    def _generate_sql_query(self, intent: str, time_range: Dict[str, Optional[str]], 
                           metrics: List[str], conditions: Dict[str, str],
                           mapped_columns: Dict[str, str], 
                           schema_info: Dict[str, Any] = None) -> str:
        """
        Generate SQL query based on analysis results
        """
        # Default table name
        table_name = schema_info.get('table_name', 'daily_reports') if schema_info else 'daily_reports'
        
        # Build SELECT clause
        if intent == 'COUNT':
            select_clause = "SELECT COUNT(*) as total_count"
            if time_range['start'] != time_range['end']:
                select_clause += ", DATE(created_at) as date"
        elif intent == 'SUMMARY':
            select_clause = "SELECT *"
        else:
            # Select mapped columns
            if mapped_columns:
                columns = list(mapped_columns.values())
                select_clause = f"SELECT {', '.join(columns)}"
            else:
                select_clause = "SELECT *"
        
        # Build FROM clause
        from_clause = f"FROM {table_name}"
        
        # Build WHERE clause
        where_conditions = []
        
        # Add time filtering
        if time_range['start'] and time_range['end']:
            if time_range['start'] == time_range['end']:
                where_conditions.append(f"DATE(created_at) = '{time_range['start']}'")
            else:
                where_conditions.append(f"DATE(created_at) BETWEEN '{time_range['start']}' AND '{time_range['end']}'")
        
        # Add condition filters
        for column, value in conditions.items():
            where_conditions.append(f"{column} = '{value}'")
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Build GROUP BY clause
        group_by_clause = ""
        if intent == 'COUNT' and time_range['start'] != time_range['end']:
            group_by_clause = "GROUP BY DATE(created_at)"
        elif intent == 'TREND':
            group_by_clause = "GROUP BY DATE(created_at)"
        
        # Build ORDER BY clause
        order_by_clause = ""
        if intent == 'TREND':
            order_by_clause = "ORDER BY DATE(created_at)"
        elif intent == 'DETAILS':
            order_by_clause = "ORDER BY created_at DESC"
        
        # Build LIMIT clause
        limit_clause = ""
        if intent == 'DETAILS':
            limit_clause = "LIMIT 100"
        
        # Combine all clauses
        sql_parts = [select_clause, from_clause, where_clause, group_by_clause, order_by_clause, limit_clause]
        sql_query = " ".join(part for part in sql_parts if part)
        
        return sql_query


# Global instance
_query_analyzer = None

def get_query_analyzer() -> QueryAnalyzer:
    """Get the global query analyzer instance"""
    global _query_analyzer
    if _query_analyzer is None:
        _query_analyzer = QueryAnalyzer()
    return _query_analyzer