"""
Expert Database Reasoning Agent
Answers user questions ONLY using database data with strict validation.
"""

import re
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple, List
import json
import pytz

class DatabaseReasoningAgent:
    def __init__(self, db_path: str = "ravvyn.db"):
        self.db_path = db_path
        self.current_date = datetime.now(pytz.timezone('Asia/Kolkata')).date()
        self.timezone = "Asia/Kolkata"
        
    def classify_question_type(self, question: str) -> str:
        """
        STEP 1: Classify the Question Type
        Returns: exact_date_query, relative_time_query, month_query, range_query, non_time_query
        """
        question_lower = question.lower()
        
        # Check for exact dates (YYYY-MM-DD format or similar)
        if re.search(r'\d{4}-\d{1,2}-\d{1,2}|\d{1,2}[/-]\d{1,2}[/-]\d{4}', question):
            return "exact_date_query"
        
        # Check for relative time queries
        relative_patterns = [
            r'last \d+ days?', r'past \d+ days?', r'this week', r'last week',
            r'this month', r'last month', r'today', r'yesterday'
        ]
        if any(re.search(pattern, question_lower) for pattern in relative_patterns):
            return "relative_time_query"
        
        # Check for month names
        months = ['january', 'february', 'march', 'april', 'may', 'june',
                 'july', 'august', 'september', 'october', 'november', 'december',
                 'jan', 'feb', 'mar', 'apr', 'may', 'jun',
                 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
        if any(month in question_lower for month in months):
            return "month_query"
        
        # Check for range queries
        range_patterns = [r'from .+ to .+', r'between .+ and .+']
        if any(re.search(pattern, question_lower) for pattern in range_patterns):
            return "range_query"
        
        return "non_time_query"
    
    def resolve_time_to_date_range(self, question: str, question_type: str) -> Dict[str, Any]:
        """
        STEP 2: Resolve Time into Exact Date Range (MANDATORY)
        Returns time resolution JSON
        """
        question_lower = question.lower()
        
        if question_type == "exact_date_query":
            # Extract specific date
            date_match = re.search(r'(\d{4}-\d{1,2}-\d{1,2})', question)
            if date_match:
                date_str = date_match.group(1)
                return {
                    "time_type": "exact",
                    "start_date": date_str,
                    "end_date": date_str,
                    "assumed_year": 2025,
                    "timezone": self.timezone,
                    "confidence": "high"
                }
        
        elif question_type == "relative_time_query":
            return self._resolve_relative_time(question_lower)
        
        elif question_type == "month_query":
            return self._resolve_month_query(question_lower)
        
        elif question_type == "range_query":
            return self._resolve_range_query(question_lower)
        
        elif question_type == "non_time_query":
            return {
                "time_type": "none",
                "start_date": None,
                "end_date": None,
                "assumed_year": 2025,
                "timezone": self.timezone,
                "confidence": "high"
            }
        
        return {
            "time_type": "unknown",
            "start_date": None,
            "end_date": None,
            "assumed_year": 2025,
            "timezone": self.timezone,
            "confidence": "low"
        }
    
    def _resolve_relative_time(self, question_lower: str) -> Dict[str, Any]:
        """Resolve relative time references"""
        current_date = self.current_date
        
        # Last N days
        last_days_match = re.search(r'last (\d+) days?', question_lower)
        if last_days_match:
            n_days = int(last_days_match.group(1))
            end_date = current_date
            start_date = current_date - timedelta(days=n_days - 1)
            return {
                "time_type": "relative",
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "assumed_year": 2025,
                "timezone": self.timezone,
                "confidence": "high"
            }
        
        # Last week
        if "last week" in question_lower:
            end_date = current_date - timedelta(days=current_date.weekday() + 1)  # Last Sunday
            start_date = end_date - timedelta(days=6)  # Previous Monday
            return {
                "time_type": "relative",
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "assumed_year": 2025,
                "timezone": self.timezone,
                "confidence": "high"
            }
        
        # This month
        if "this month" in question_lower:
            start_date = current_date.replace(day=1)
            return {
                "time_type": "relative",
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": current_date.strftime("%Y-%m-%d"),
                "assumed_year": 2025,
                "timezone": self.timezone,
                "confidence": "high"
            }
        
        # Last month
        if "last month" in question_lower:
            first_day_current = current_date.replace(day=1)
            last_day_previous = first_day_current - timedelta(days=1)
            first_day_previous = last_day_previous.replace(day=1)
            return {
                "time_type": "relative",
                "start_date": first_day_previous.strftime("%Y-%m-%d"),
                "end_date": last_day_previous.strftime("%Y-%m-%d"),
                "assumed_year": 2025,
                "timezone": self.timezone,
                "confidence": "high"
            }
        
        return {
            "time_type": "relative",
            "start_date": None,
            "end_date": None,
            "assumed_year": 2025,
            "timezone": self.timezone,
            "confidence": "low"
        }
    
    def _resolve_month_query(self, question_lower: str) -> Dict[str, Any]:
        """Resolve month name references"""
        month_map = {
            'january': 1, 'jan': 1, 'february': 2, 'feb': 2, 'march': 3, 'mar': 3,
            'april': 4, 'apr': 4, 'may': 5, 'june': 6, 'jun': 6,
            'july': 7, 'jul': 7, 'august': 8, 'aug': 8, 'september': 9, 'sep': 9,
            'october': 10, 'oct': 10, 'november': 11, 'nov': 11, 'december': 12, 'dec': 12
        }
        
        # Find month
        month_num = None
        for month_name, num in month_map.items():
            if month_name in question_lower:
                month_num = num
                break
        
        if month_num is None:
            return {
                "time_type": "month",
                "start_date": None,
                "end_date": None,
                "assumed_year": 2025,
                "timezone": self.timezone,
                "confidence": "low"
            }
        
        # Check for year
        year_match = re.search(r'\b(20\d{2})\b', question_lower)
        year = int(year_match.group(1)) if year_match else 2025
        
        # Calculate month range
        start_date = datetime(year, month_num, 1).date()
        if month_num == 12:
            end_date = datetime(year + 1, 1, 1).date() - timedelta(days=1)
        else:
            end_date = datetime(year, month_num + 1, 1).date() - timedelta(days=1)
        
        return {
            "time_type": "month",
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "assumed_year": year,
            "timezone": self.timezone,
            "confidence": "high"
        }
    
    def _resolve_range_query(self, question_lower: str) -> Dict[str, Any]:
        """Resolve range queries (from X to Y)"""
        # This is a simplified implementation
        # In practice, you'd need more sophisticated date parsing
        return {
            "time_type": "range",
            "start_date": None,
            "end_date": None,
            "assumed_year": 2025,
            "timezone": self.timezone,
            "confidence": "low"
        }
    
    async def get_available_tabs(self) -> List[str]:
        """Get list of available tabs for parallel processing"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=5.0)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT tab_name 
                FROM sheets_data 
                WHERE tab_name IS NOT NULL 
                ORDER BY tab_name
            """)
            tabs = [row[0] for row in cursor.fetchall()]
            conn.close()
            return tabs
        except:
            return ['RO DETAILS', 'COSTING', 'Default']  # Fallback to common tabs

    def generate_optimized_sql(self, question: str, time_resolution: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        STEP 4: Generate OPTIMIZED SQL queries for parallel execution across sub-sheets
        Returns list of queries that can be executed in parallel
        """
        question_lower = question.lower()
        queries = []
        
        # Count queries for sheets data - parallel by tab
        if "count" in question_lower or "how many" in question_lower:
            if "sheet" in question_lower or "record" in question_lower:
                if time_resolution["start_date"] and time_resolution["end_date"]:
                    # Generate parallel queries for each major tab
                    major_tabs = ['RO DETAILS', 'COSTING', 'TANK LEVEL', 'PRESSURE', 'FLOW RATE']
                    
                    for tab in major_tabs:
                        queries.append({
                            "name": f"count_tab_{tab.lower().replace(' ', '_')}",
                            "sql": f"""
                            SELECT COUNT(*) AS total_count, '{tab}' AS tab_name, 'tab_specific' AS source
                            FROM sheets_data sd
                            INNER JOIN sheets_metadata sm ON sd.sheet_id = sm.sheet_id
                            WHERE sd.tab_name = '{tab}'
                            AND sm.modified_time >= '{time_resolution["start_date"]} 00:00:00' 
                            AND sm.modified_time <= '{time_resolution["end_date"]} 23:59:59'
                            """
                        })
                    
                    # General count query (fallback)
                    queries.append({
                        "name": "count_general",
                        "sql": f"""
                        SELECT COUNT(*) AS total_count, 'all_tabs' AS tab_name, 'general' AS source
                        FROM sheets_data sd
                        INNER JOIN sheets_metadata sm ON sd.sheet_id = sm.sheet_id
                        WHERE sm.modified_time >= '{time_resolution["start_date"]} 00:00:00' 
                        AND sm.modified_time <= '{time_resolution["end_date"]} 23:59:59'
                        """
                    })
                else:
                    # Fast count without date filter - still parallel by tab
                    major_tabs = ['RO DETAILS', 'COSTING', 'TANK LEVEL']
                    for tab in major_tabs:
                        queries.append({
                            "name": f"total_tab_{tab.lower().replace(' ', '_')}",
                            "sql": f"SELECT COUNT(*) AS total_count, '{tab}' AS tab_name FROM sheets_data WHERE tab_name = '{tab}'"
                        })
        
        # Chat history queries - optimized
        elif "chat" in question_lower or "message" in question_lower or "conversation" in question_lower:
            if time_resolution["start_date"] and time_resolution["end_date"]:
                queries.append({
                    "name": "chat_count",
                    "sql": f"""
                    SELECT COUNT(*) AS total_messages, 'chat_history' AS source
                    FROM chat_history 
                    WHERE created_at >= '{time_resolution["start_date"]} 00:00:00' 
                    AND created_at <= '{time_resolution["end_date"]} 23:59:59'
                    """
                })
            else:
                queries.append({
                    "name": "total_chat",
                    "sql": "SELECT COUNT(*) AS total_messages, 'chat_history' AS source FROM chat_history"
                })
        
        # Task queries - optimized
        elif "task" in question_lower:
            if time_resolution["start_date"] and time_resolution["end_date"]:
                queries.append({
                    "name": "task_stats",
                    "sql": f"""
                    SELECT 
                        COUNT(*) AS total_tasks, 
                        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_tasks,
                        'tasks' AS source
                    FROM tasks 
                    WHERE created_at >= '{time_resolution["start_date"]} 00:00:00' 
                    AND created_at <= '{time_resolution["end_date"]} 23:59:59'
                    """
                })
            else:
                queries.append({
                    "name": "all_tasks",
                    "sql": """
                    SELECT 
                        COUNT(*) AS total_tasks, 
                        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_tasks,
                        'tasks' AS source
                    FROM tasks
                    """
                })
        
        # Sync status queries - fast lookup
        elif "sync" in question_lower and "status" in question_lower:
            queries.append({
                "name": "sync_status",
                "sql": """
                SELECT sync_status, COUNT(*) as count, 'sheets_metadata' AS source
                FROM sheets_metadata 
                GROUP BY sync_status
                """
            })
        
        # Data activity queries - parallel by ALL available tabs
        elif "activity" in question_lower or "data" in question_lower or "plan" in question_lower:
            if time_resolution["start_date"] and time_resolution["end_date"]:
                # Query ALL major tabs in parallel for comprehensive search
                all_tabs = [
                    'RO DETAILS', 'COSTING', 'TANK LEVEL', 'PRESSURE', 'FLOW RATE', 
                    'TEMPERATURE', 'MAINTENANCE', 'OPERATIONS', 'DAILY REPORT', 
                    'WEEKLY PLAN', 'MONTHLY PLAN', 'Default'
                ]
                
                for tab in all_tabs:
                    queries.append({
                        "name": f"activity_{tab.lower().replace(' ', '_').replace('/', '_')}",
                        "sql": f"""
                        SELECT 
                            COUNT(*) as total_rows,
                            MAX(sd.row_index) as max_row,
                            '{tab}' as tab_name,
                            'tab_activity' as source
                        FROM sheets_data sd
                        WHERE sd.tab_name = '{tab}'
                        AND sd.synced_at >= '{time_resolution["start_date"]} 00:00:00' 
                        AND sd.synced_at <= '{time_resolution["end_date"]} 23:59:59'
                        HAVING COUNT(*) > 0
                        """
                    })
                
                # Search for plan-related data specifically
                if "plan" in question_lower:
                    queries.append({
                        "name": "plan_search",
                        "sql": f"""
                        SELECT 
                            COUNT(*) as total_rows,
                            sd.tab_name,
                            'plan_data' as source
                        FROM sheets_data sd
                        WHERE (sd.tab_name LIKE '%PLAN%' OR sd.row_data LIKE '%plan%' OR sd.row_data LIKE '%Plan%')
                        AND sd.synced_at >= '{time_resolution["start_date"]} 00:00:00' 
                        AND sd.synced_at <= '{time_resolution["end_date"]} 23:59:59'
                        GROUP BY sd.tab_name
                        HAVING COUNT(*) > 0
                        """
                    })
            else:
                # Search all tabs without date filter
                all_tabs = ['RO DETAILS', 'COSTING', 'TANK LEVEL', 'PRESSURE', 'FLOW RATE']
                for tab in all_tabs:
                    queries.append({
                        "name": f"total_activity_{tab.lower().replace(' ', '_')}",
                        "sql": f"""
                        SELECT 
                            COUNT(*) as total_rows,
                            '{tab}' as tab_name
                        FROM sheets_data 
                        WHERE tab_name = '{tab}'
                        """
                    })
        
        # Total queries (non-time specific) - fast counts
        elif "total" in question_lower:
            if "sheet" in question_lower or "record" in question_lower:
                queries.append({
                    "name": "total_sheets",
                    "sql": "SELECT COUNT(*) AS total_count, 'sheets_data' AS source FROM sheets_data"
                })
            elif "task" in question_lower:
                queries.append({
                    "name": "total_tasks",
                    "sql": "SELECT COUNT(*) AS total_tasks, 'tasks' AS source FROM tasks"
                })
        
        return queries
        
        # Sheets metadata queries
        if "sheet" in question_lower and ("name" in question_lower or "title" in question_lower):
            if time_resolution["start_date"] and time_resolution["end_date"]:
                return f"""
                SELECT sheet_name, sheet_id, modified_time 
                FROM sheets_metadata 
                WHERE DATE(modified_time) BETWEEN '{time_resolution["start_date"]}' AND '{time_resolution["end_date"]}'
                """
            else:
                return "SELECT sheet_name, sheet_id, modified_time FROM sheets_metadata"
        
        # Chat history queries
        if "chat" in question_lower or "message" in question_lower or "conversation" in question_lower:
            if time_resolution["start_date"] and time_resolution["end_date"]:
                return f"""
                SELECT COUNT(*) AS total_messages 
                FROM chat_history 
                WHERE DATE(created_at) BETWEEN '{time_resolution["start_date"]}' AND '{time_resolution["end_date"]}'
                """
            else:
                return "SELECT COUNT(*) AS total_messages FROM chat_history"
        
        # Task queries
        if "task" in question_lower:
            if time_resolution["start_date"] and time_resolution["end_date"]:
                return f"""
                SELECT COUNT(*) AS total_tasks, 
                       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_tasks
                FROM tasks 
                WHERE DATE(created_at) BETWEEN '{time_resolution["start_date"]}' AND '{time_resolution["end_date"]}'
                """
            else:
                return """
                SELECT COUNT(*) AS total_tasks, 
                       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_tasks
                FROM tasks
                """
        
        # Reminder queries
        if "reminder" in question_lower:
            if time_resolution["start_date"] and time_resolution["end_date"]:
                return f"""
                SELECT COUNT(*) AS total_reminders 
                FROM reminders 
                WHERE DATE(created_at) BETWEEN '{time_resolution["start_date"]}' AND '{time_resolution["end_date"]}'
                """
            else:
                return "SELECT COUNT(*) AS total_reminders FROM reminders"
        
        # Sync status queries
        if "sync" in question_lower and "status" in question_lower:
            return """
            SELECT sync_status, COUNT(*) as count 
            FROM sheets_metadata 
            GROUP BY sync_status
            """
        
        # General data activity
        if "activity" in question_lower or "data" in question_lower:
            if time_resolution["start_date"] and time_resolution["end_date"]:
                return f"""
                SELECT 
                    COUNT(DISTINCT sd.sheet_id) as active_sheets,
                    COUNT(*) as total_rows
                FROM sheets_data sd
                JOIN sheets_metadata sm ON sd.sheet_id = sm.sheet_id
                WHERE DATE(sd.synced_at) BETWEEN '{time_resolution["start_date"]}' AND '{time_resolution["end_date"]}'
                """
            else:
                return """
                SELECT 
                    COUNT(DISTINCT sheet_id) as active_sheets,
                    COUNT(*) as total_rows
                FROM sheets_data
                """
        
        # Total queries (non-time specific)
        if "total" in question_lower:
            if "sheet" in question_lower or "record" in question_lower:
                return "SELECT COUNT(*) AS total_count FROM sheets_data"
            elif "task" in question_lower:
                return "SELECT COUNT(*) AS total_tasks FROM tasks"
            elif "reminder" in question_lower:
                return "SELECT COUNT(*) AS total_reminders FROM reminders"
        
        return None
    
    async def execute_query(self, sql: str) -> Tuple[bool, List[Dict[str, Any]]]:
        """Execute SQL query asynchronously with optimization"""
        import asyncio
        import concurrent.futures
        
        def _execute_sync(sql_query: str):
            """Synchronous database execution in thread pool with optimization"""
            try:
                conn = sqlite3.connect(self.db_path, timeout=30.0)  # Increased timeout
                conn.row_factory = sqlite3.Row
                
                # Aggressive query optimization
                conn.execute("PRAGMA query_only = ON")  # Read-only mode
                conn.execute("PRAGMA cache_size = -128000")  # 128MB cache
                conn.execute("PRAGMA temp_store = MEMORY")  # Use memory for temp storage
                conn.execute("PRAGMA mmap_size = 268435456")  # 256MB memory mapping
                conn.execute("PRAGMA synchronous = OFF")  # Faster for read-only
                conn.execute("PRAGMA journal_mode = OFF")  # No journaling for reads
                
                cursor = conn.cursor()
                cursor.execute(sql_query)
                rows = cursor.fetchall()
                
                results = [dict(row) for row in rows]
                conn.close()
                
                return True, results
            except Exception as e:
                # Only log actual errors, not timeouts
                if "timeout" not in str(e).lower():
                    print(f"⚠️  Database query error: {e}")
                return False, []
        
        try:
            # Execute in thread pool to avoid blocking
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_execute_sync, sql)
                success, results = await asyncio.wrap_future(future)
                return success, results
        except Exception as e:
            return False, []
    
    def validate_query_results(self, results: List[Dict[str, Any]], 
                             time_resolution: Dict[str, Any]) -> bool:
        """
        STEP 5: Validate Query Results
        """
        if not results:
            return False
        
        # Check if we have meaningful data
        first_result = results[0]
        for value in first_result.values():
            if value is not None and value != 0:
                return True
        
        return False
    
    def construct_answer(self, results: List[Dict[str, Any]], 
                        question: str, time_resolution: Dict[str, Any]) -> str:
        """
        STEP 6: Answer Construction Rules
        Answer ONLY using returned DB values
        """
        if not results:
            return "No data available for the resolved time range."
        
        result = results[0]
        question_lower = question.lower()
        
        # Time context formatting
        time_context = ""
        if time_resolution["start_date"] and time_resolution["end_date"]:
            if time_resolution["start_date"] == time_resolution["end_date"]:
                time_context = f" on {time_resolution['start_date']}"
            else:
                time_context = f" from {time_resolution['start_date']} to {time_resolution['end_date']}"
        
        # Format answer based on query type
        if "count" in question_lower and ("sheet" in question_lower or "record" in question_lower):
            count = result.get('total_count', 0)
            return f"Total sheet records{time_context}: {count}"
        
        elif "count" in question_lower and ("message" in question_lower or "chat" in question_lower):
            count = result.get('total_messages', 0)
            return f"Total chat messages{time_context}: {count}"
        
        elif "task" in question_lower:
            total = result.get('total_tasks', 0)
            completed = result.get('completed_tasks', 0)
            return f"Total tasks{time_context}: {total}, Completed: {completed}"
        
        elif "reminder" in question_lower:
            count = result.get('total_reminders', 0)
            return f"Total reminders{time_context}: {count}"
        
        elif "sync" in question_lower and "status" in question_lower:
            # Handle multiple results for sync status
            if len(results) > 1:
                status_summary = []
                for row in results:
                    status_summary.append(f"{row.get('sync_status', 'unknown')}: {row.get('count', 0)}")
                return f"Sync status distribution: {', '.join(status_summary)}"
            else:
                return f"Sync status: {result.get('sync_status', 'unknown')} ({result.get('count', 0)} items)"
        
        elif "activity" in question_lower or "data" in question_lower:
            sheets = result.get('active_sheets', 0)
            rows = result.get('total_rows', 0)
            return f"Data activity{time_context}: {sheets} active sheets, {rows} total rows"
        
        elif "sheet" in question_lower and "name" in question_lower:
            # Handle sheet listing
            if len(results) > 1:
                sheet_names = [row.get('sheet_name', 'Unknown') for row in results[:5]]  # Limit to 5
                return f"Sheets{time_context}: {', '.join(sheet_names)}" + (f" (and {len(results)-5} more)" if len(results) > 5 else "")
            else:
                return f"Sheet: {result.get('sheet_name', 'Unknown')}"
        
        # Generic count
        elif "count" in question_lower:
            count = result.get('total_count', 0)
            return f"Total count{time_context}: {count}"
        
        return str(result)
    
    async def execute_parallel_queries(self, queries: List[Dict[str, str]]) -> Dict[str, Any]:
        """Execute multiple queries in parallel for faster results - only return matching data"""
        import asyncio
        
        if not queries:
            return {}
        
        # Execute all queries in parallel
        tasks = []
        for query_info in queries:
            task = asyncio.create_task(
                self.execute_query(query_info["sql"]),
                name=query_info["name"]
            )
            tasks.append((query_info["name"], task))
        
        # Wait for all queries to complete with extended timeout
        results = {}
        matching_results = {}
        
        try:
            completed_tasks = await asyncio.wait_for(
                asyncio.gather(*[task for _, task in tasks], return_exceptions=True),
                timeout=60.0  # 60 second timeout for complex queries
            )
            
            # Process results - only keep results with actual data
            for i, (name, _) in enumerate(tasks):
                if i < len(completed_tasks) and not isinstance(completed_tasks[i], Exception):
                    success, data = completed_tasks[i]
                    if success and data:
                        result_data = data[0] if data else {}
                        
                        # Only include results that have meaningful data
                        has_data = False
                        for key, value in result_data.items():
                            if key != 'source' and key != 'tab_name' and value and value != 0:
                                has_data = True
                                break
                        
                        if has_data:
                            matching_results[name] = result_data
                            # Only log successful matches, not all queries
                            if 'total_count' in result_data and result_data['total_count'] > 0:
                                print(f"✅ Found {result_data['total_count']} records in {result_data.get('tab_name', name)}")
                            elif 'total_rows' in result_data and result_data['total_rows'] > 0:
                                print(f"✅ Found {result_data['total_rows']} rows in {result_data.get('tab_name', name)}")
                    
        except asyncio.TimeoutError:
            print("⚠️  Some queries timed out, returning partial results")
            # Handle timeout - return partial results from completed tasks
            for name, task in tasks:
                if task.done() and not task.exception():
                    try:
                        success, data = task.result()
                        if success and data:
                            result_data = data[0] if data else {}
                            # Only include if has meaningful data
                            if any(v for k, v in result_data.items() if k not in ['source', 'tab_name'] and v and v != 0):
                                matching_results[name] = result_data
                    except:
                        pass  # Skip failed tasks silently
        
        # Only return results that actually have data
        return matching_results
    
    def aggregate_parallel_results(self, results: Dict[str, Any], question: str, time_resolution: Dict[str, Any]) -> str:
        """Aggregate results from parallel queries into a coherent answer - focus on matching data"""
        if not results:
            return "No data available for the resolved time range."
        
        question_lower = question.lower()
        
        # Time context formatting
        time_context = ""
        if time_resolution["start_date"] and time_resolution["end_date"]:
            if time_resolution["start_date"] == time_resolution["end_date"]:
                time_context = f" on {time_resolution['start_date']}"
            else:
                time_context = f" from {time_resolution['start_date']} to {time_resolution['end_date']}"
        
        # Plan-specific queries
        if "plan" in question_lower:
            plan_results = []
            total_plan_rows = 0
            
            for name, data in results.items():
                if "error" not in data and ("plan" in name or data.get("source") == "plan_data"):
                    rows = data.get("total_rows", 0)
                    tab_name = data.get("tab_name", "Unknown")
                    if rows > 0:
                        plan_results.append(f"{tab_name}: {rows} entries")
                        total_plan_rows += rows
            
            if plan_results:
                answer = f"Plan data{time_context}: {total_plan_rows} total entries found"
                answer += f"\nFound in: {', '.join(plan_results[:5])}"  # Show top 5 tabs
                return answer
        
        # Count queries - show breakdown by tab
        if "count" in question_lower or "how many" in question_lower:
            tab_counts = []
            total_count = 0
            
            for name, data in results.items():
                if "error" not in data and "total_count" in data:
                    count = data.get("total_count", 0)
                    tab_name = data.get("tab_name", name.replace("count_tab_", "").replace("_", " ").title())
                    
                    if count > 0:
                        tab_counts.append({"tab": tab_name, "count": count})
                        total_count += count
            
            if tab_counts:
                # Sort by count descending
                tab_counts.sort(key=lambda x: x["count"], reverse=True)
                
                answer = f"Sheet records{time_context}: {total_count} total records"
                
                # Show breakdown of top tabs with data
                if len(tab_counts) > 1:
                    top_tabs = [f"{tc['tab']}: {tc['count']}" for tc in tab_counts[:5]]
                    answer += f"\nBreakdown: {', '.join(top_tabs)}"
                
                return answer
        
        # Activity queries - show only tabs with data
        elif "activity" in question_lower or "data" in question_lower:
            active_tabs = []
            total_rows = 0
            
            for name, data in results.items():
                if "error" not in data and "total_rows" in data:
                    rows = data.get("total_rows", 0)
                    tab_name = data.get("tab_name", name.replace("activity_", "").replace("_", " ").title())
                    
                    if rows > 0:
                        active_tabs.append({"tab": tab_name, "rows": rows})
                        total_rows += rows
            
            if active_tabs:
                # Sort by activity level
                active_tabs.sort(key=lambda x: x["rows"], reverse=True)
                
                answer = f"Data activity{time_context}: {total_rows} total rows across {len(active_tabs)} active tabs"
                
                # Show most active tabs
                if len(active_tabs) > 1:
                    top_active = [f"{at['tab']}: {at['rows']} rows" for at in active_tabs[:5]]
                    answer += f"\nMost active: {', '.join(top_active)}"
                
                return answer
        
        # Task queries
        elif "task" in question_lower:
            for name, data in results.items():
                if "error" not in data and "total_tasks" in data:
                    total = data.get("total_tasks", 0)
                    completed = data.get("completed_tasks", 0)
                    return f"Total tasks{time_context}: {total}, Completed: {completed}"
        
        # Sync status
        elif "sync" in question_lower:
            for name, data in results.items():
                if "error" not in data and "sync_status" in data:
                    status = data.get("sync_status", "unknown")
                    count = data.get("count", 0)
                    return f"Sync status: {status} ({count} items)"
        
        # Chat queries
        elif "chat" in question_lower or "message" in question_lower:
            for name, data in results.items():
                if "error" not in data and "total_messages" in data:
                    count = data.get("total_messages", 0)
                    return f"Total chat messages{time_context}: {count}"
        
        # Fallback - return first valid result
        for name, data in results.items():
            if "error" not in data:
                return f"Query result: {data}"
        
        return "No data available for the resolved time range."
    
    async def answer_question(self, question: str) -> str:
        """
        Main method that follows all 7 steps strictly with async optimization
        """
        try:
            # STEP 1: Classify Question Type
            question_type = self.classify_question_type(question)
            
            # STEP 2: Resolve Time into Exact Date Range
            time_resolution = self.resolve_time_to_date_range(question, question_type)
            
            # STEP 3: Output Time Resolution JSON (internal validation)
            if time_resolution["confidence"] == "low":
                return "Unable to answer accurately due to ambiguous or unavailable time data."
            
            # STEP 4: Generate OPTIMIZED SQL queries for parallel execution
            queries = self.generate_optimized_sql(question, time_resolution)
            if not queries:
                return "Unable to answer accurately due to ambiguous or unavailable time data."
            
            # Execute queries in parallel
            results = await self.execute_parallel_queries(queries)
            if not results:
                return "Unable to answer accurately due to ambiguous or unavailable time data."
            
            # STEP 5: Validate Query Results (check if any query returned data)
            has_valid_data = any(
                "error" not in data and any(v for v in data.values() if v not in [None, 0, ""])
                for data in results.values()
            )
            
            if not has_valid_data:
                return "No data available for the resolved time range."
            
            # STEP 6: Answer Construction from parallel results
            return self.aggregate_parallel_results(results, question, time_resolution)
            
        except Exception as e:
            return "Unable to answer accurately due to ambiguous or unavailable time data."


# Example usage and testing
if __name__ == "__main__":
    agent = DatabaseReasoningAgent()
    
    # Test questions
    test_questions = [
        "What was the revenue last 7 days?",
        "How much revenue in December?",
        "What was the total revenue on 2025-12-12?",
        "Count of records this month"
    ]
    
    for question in test_questions:
        print(f"\nQuestion: {question}")
        answer = agent.answer_question(question)
        print(f"Answer: {answer}")