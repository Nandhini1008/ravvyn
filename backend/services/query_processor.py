"""
Query Processor - Converts natural language queries into data searches and provides direct answers
Handles questions like "what is the amount on December 12th?" and returns specific data
"""

import re
import json
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from services.database import get_db_context
from services.data_retrieval import get_data_retrieval_service

logger = logging.getLogger(__name__)


class QueryProcessor:
    """
    Processes natural language queries and extracts specific data from sheets.
    Converts questions into structured searches and provides direct answers.
    """
    
    def __init__(self):
        """Initialize the query processor"""
        self.default_sheet_id = "1ajWB1qm5a_HedC9Bdo4w14RqLmiKhRzjkzzl3iCaLVg"
        self.data_service = get_data_retrieval_service()
        
        # Tab name mappings for common terms
        self.tab_mappings = {
            'ro details': 'RO DETAILS',
            'ro': 'RO DETAILS',
            'costing': 'costing',
            'cost': 'costing',
            'condensate': 'CONDENSATE HOUR DETAILS',
            'running': 'RUNNING HRS',
            'running hours': 'RUNNING HRS',
            'tank': 'TANK DIPPING',
            'tank dipping': 'TANK DIPPING',
            'ber': 'BER',
            'sludge': 'SLUDGE DRYING BED HUMIDITY',
            'humidity': 'SLUDGE DRYING BED HUMIDITY',
            'pre treatment': 'PRE TREATMENT',
            'pretreatment': 'PRE TREATMENT',
            'maintenance': 'Maintance work details',
            'evening': 'EVENING REPORT',
            'evening report': 'EVENING REPORT',
            'purchase': 'purchase details',
            'tds': 'TDS DETAILS',
            'meeting': 'MEETING DETAILS'
        }
        
        # Tab detection patterns
        self.tab_patterns = [
            r'\b(ro\s+details?|ro)\b',
            r'\b(costing?|cost)\b',
            r'\b(condensate)\b',
            r'\b(running\s+(?:hours?|hrs?))\b',
            r'\b(tank\s+dipping|tank)\b',
            r'\b(ber)\b',
            r'\b(sludge|humidity)\b',
            r'\b(pre\s*treatment)\b',
            r'\b(maintenance|maintance)\b',
            r'\b(evening\s+report|evening)\b',
            r'\b(purchase)\b',
            r'\b(tds)\b',
            r'\b(meeting)\b'
        ]
        
        # Common query patterns
        self.query_patterns = {
            'amount_by_date': [
                r'what.*amount.*(?:on|for|in)\s*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})',
                r'amount.*(?:on|for|in)\s*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})',
                r'how much.*(?:on|for|in)\s*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})'
            ],
            'data_by_date': [
                r'(?:show|get|find).*data.*(?:on|for|from)\s*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})',
                r'data.*(?:on|for|from)\s*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})',
                r'(?:what|show).*(?:on|for)\s*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})'
            ],
            'field_by_date': [
                r'what.*(?:is|are).*(?:level|tank|feed|amount|value).*(?:on|for)\s*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})',
                r'(?:level|tank|feed).*(?:on|for)\s*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})',
                r'ro.*(?:feed|tank|level).*(?:on|for)\s*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})',
                r'what.*(?:ro|feed|tank|level).*(?:on|for)\s*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})'
            ],
            'data_by_time': [
                r'what.*(?:at|around)\s*(\d{1,2}:\d{2})',
                r'data.*(?:at|around)\s*(\d{1,2}:\d{2})',
                r'value.*(?:at|around)\s*(\d{1,2}:\d{2})'
            ],
            'latest_data': [
                r'latest.*(?:data|amount|value)',
                r'recent.*(?:data|amount|value)',
                r'current.*(?:data|amount|value)',
                r'show.*latest.*data'
            ],
            'total_amount': [
                r'total.*amount',
                r'sum.*amount',
                r'all.*amount'
            ],
            'specific_tab': [
                r'(?:from|in)\s+(ro\s*details|costing|condensate|running|tank|evening|purchase|tds|meeting)',
                r'(ro\s*details|costing|condensate|running|tank|evening|purchase|tds|meeting).*(?:data|amount|value)'
            ]
        }
    
    def detect_tab_name(self, query: str) -> Optional[str]:
        """
        Detect tab name from query text
        
        Args:
            query: Natural language query
            
        Returns:
            Tab name if detected, None otherwise
        """
        query_lower = query.lower()
        
        # Check direct mappings first
        for term, tab_name in self.tab_mappings.items():
            if term in query_lower:
                logger.debug(f"Detected tab '{tab_name}' from term '{term}' in query")
                return tab_name
        
        # Check patterns
        for pattern in self.tab_patterns:
            match = re.search(pattern, query_lower, re.IGNORECASE)
            if match:
                matched_term = match.group(1).strip()
                # Map to actual tab name
                for term, tab_name in self.tab_mappings.items():
                    if term in matched_term or matched_term in term:
                        logger.debug(f"Detected tab '{tab_name}' from pattern match '{matched_term}'")
                        return tab_name
        
        return None
    
    async def process_query(self, query: str) -> Dict[str, Any]:
        """
        Process a natural language query and return structured answer.
        
        Args:
            query: Natural language query from user
            
        Returns:
            Dictionary with answer and supporting data
        """
        query_lower = query.lower().strip()
        logger.info(f"🔍 Processing query: {query}")
        
        try:
            # Detect tab name from query
            detected_tab = self.detect_tab_name(query)
            
            # Analyze query type and extract parameters
            query_analysis = self._analyze_query(query_lower)
            query_analysis["tab_name"] = detected_tab
            
            # Get relevant data based on query type (tab-specific)
            data_results = await self._fetch_relevant_data(query_analysis)
            
            # Generate direct answer
            answer = await self._generate_answer(query, query_analysis, data_results)
            
            return {
                "success": True,
                "query": query,
                "query_type": query_analysis["type"],
                "answer": answer["text"],
                "data_found": len(data_results.get("results", [])),
                "supporting_data": data_results.get("results", [])[:5],  # Top 5 results
                "confidence": answer["confidence"],
                "suggestions": answer.get("suggestions", [])
            }
            
        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            return {
                "success": False,
                "query": query,
                "error": str(e),
                "answer": "I'm sorry, I couldn't process your query. Please try rephrasing it.",
                "suggestions": [
                    "Try asking: 'What is the amount on December 12th?'",
                    "Or: 'Show me data from RO DETAILS'",
                    "Or: 'What is the latest amount?'"
                ]
            }
    
    def _analyze_query(self, query: str) -> Dict[str, Any]:
        """Analyze query to determine type and extract parameters"""
        analysis = {
            "type": "general",
            "parameters": {},
            "confidence": 0.5
        }
        
        # Check for amount by date queries
        for pattern in self.query_patterns['amount_by_date']:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                analysis["type"] = "amount_by_date"
                analysis["parameters"]["date_text"] = match.group(1)
                analysis["confidence"] = 0.9
                break
        
        # Check for data by date queries
        if analysis["type"] == "general":
            for pattern in self.query_patterns['data_by_date']:
                match = re.search(pattern, query, re.IGNORECASE)
                if match:
                    analysis["type"] = "data_by_date"
                    analysis["parameters"]["date_text"] = match.group(1)
                    analysis["confidence"] = 0.9
                    break
        
        # Check for field by date queries
        if analysis["type"] == "general":
            for pattern in self.query_patterns['field_by_date']:
                match = re.search(pattern, query, re.IGNORECASE)
                if match:
                    analysis["type"] = "field_by_date"
                    analysis["parameters"]["date_text"] = match.group(1)
                    analysis["confidence"] = 0.9
                    break
        
        # Check for data by time queries
        if analysis["type"] == "general":
            for pattern in self.query_patterns['data_by_time']:
                match = re.search(pattern, query, re.IGNORECASE)
                if match:
                    analysis["type"] = "data_by_time"
                    analysis["parameters"]["time"] = match.group(1)
                    analysis["confidence"] = 0.8
                    break
        
        # Check for latest data queries
        if analysis["type"] == "general":
            for pattern in self.query_patterns['latest_data']:
                if re.search(pattern, query, re.IGNORECASE):
                    analysis["type"] = "latest_data"
                    analysis["confidence"] = 0.8
                    break
        
        # Check for total amount queries
        if analysis["type"] == "general":
            for pattern in self.query_patterns['total_amount']:
                if re.search(pattern, query, re.IGNORECASE):
                    analysis["type"] = "total_amount"
                    analysis["confidence"] = 0.8
                    break
        
        # Check for specific tab queries
        for pattern in self.query_patterns['specific_tab']:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                tab_text = match.group(1).lower().strip()
                mapped_tab = self.tab_mappings.get(tab_text, tab_text.upper())
                analysis["parameters"]["tab_name"] = mapped_tab
                analysis["confidence"] += 0.2
                break
        
        # Extract date information
        date_match = re.search(r'(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})', query)
        if date_match:
            analysis["parameters"]["date_text"] = date_match.group(1)
        
        # Extract time information
        time_match = re.search(r'(\d{1,2}:\d{2})', query)
        if time_match:
            analysis["parameters"]["time"] = time_match.group(1)
        
        # Extract number references
        number_matches = re.findall(r'\b(\d+(?:\.\d+)?)\b', query)
        if number_matches:
            analysis["parameters"]["numbers"] = [float(n) for n in number_matches]
        
        logger.info(f"📊 Query analysis: {analysis}")
        return analysis
    
    async def _fetch_relevant_data(self, query_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch data relevant to the query analysis"""
        try:
            search_criteria = {
                "sheet_id": self.default_sheet_id,
                "search_terms": [],
                "tab_names": [],
                "limit": 100
            }
            
            # Add tab filter if detected or specified
            if query_analysis.get("tab_name"):
                search_criteria["tab_names"] = [query_analysis["tab_name"]]
            elif "tab_name" in query_analysis["parameters"]:
                search_criteria["tab_names"] = [query_analysis["parameters"]["tab_name"]]
            
            # Add search terms based on query type
            if query_analysis["type"] in ["amount_by_date", "data_by_date", "field_by_date"]:
                # Search for date-related data
                date_text = query_analysis["parameters"].get("date_text", "")
                search_criteria["search_terms"].append(date_text)
            
            elif query_analysis["type"] == "data_by_time":
                # Search for time-related data
                time_text = query_analysis["parameters"].get("time", "")
                search_criteria["search_terms"].append(time_text)
            
            elif query_analysis["type"] == "latest_data":
                # Get recent data (no specific search terms needed)
                pass
            
            # Add number search terms if present
            if "numbers" in query_analysis["parameters"]:
                for num in query_analysis["parameters"]["numbers"]:
                    search_criteria["search_terms"].append(str(int(num)))
            
            # Fetch data
            if search_criteria["search_terms"] or search_criteria["tab_names"]:
                result = await self.data_service.search_data_by_criteria(
                    sheet_id=search_criteria["sheet_id"],
                    search_terms=search_criteria["search_terms"] if search_criteria["search_terms"] else None,
                    tab_names=search_criteria["tab_names"] if search_criteria["tab_names"] else None
                )
            else:
                # Get general data
                result = await self.data_service.get_sheet_data_for_llm(
                    sheet_id=search_criteria["sheet_id"],
                    limit=search_criteria["limit"]
                )
                if result["success"]:
                    result = {
                        "success": True,
                        "results": result["data"],
                        "results_count": len(result["data"])
                    }
            
            return result
            
        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}")
            return {"success": False, "error": str(e), "results": []}
    
    async def _generate_answer(self, original_query: str, query_analysis: Dict[str, Any], 
                             data_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a direct answer based on query analysis and data"""
        
        if not data_results.get("success") or not data_results.get("results"):
            return {
                "text": "I couldn't find any data matching your query. Please check if the data exists or try rephrasing your question.",
                "confidence": 0.1,
                "suggestions": [
                    "Try asking about a specific tab: 'Show me RO DETAILS data'",
                    "Or ask for recent data: 'What is the latest amount?'"
                ]
            }
        
        results = data_results["results"]
        query_type = query_analysis["type"]
        
        # Generate specific answers based on query type
        if query_type == "amount_by_date":
            return self._answer_amount_by_date(original_query, query_analysis, results)
        
        elif query_type == "data_by_date":
            return self._answer_data_by_date(original_query, query_analysis, results)
        
        elif query_type == "field_by_date":
            return self._answer_field_by_date(original_query, query_analysis, results)
        
        elif query_type == "data_by_time":
            return self._answer_data_by_time(original_query, query_analysis, results)
        
        elif query_type == "latest_data":
            return self._answer_latest_data(original_query, results)
        
        elif query_type == "total_amount":
            return self._answer_total_amount(original_query, results)
        
        else:
            return self._answer_general_query(original_query, results)
    
    def _answer_amount_by_date(self, query: str, analysis: Dict, results: List[Dict]) -> Dict[str, Any]:
        """Generate answer for amount by date queries"""
        date_text = analysis["parameters"].get("date_text", "")
        
        # Find relevant amounts using field mapping
        amounts = []
        relevant_data = []
        
        for result in results:
            fields = result.get("fields", {})
            tab_name = result.get("tab_name", "Unknown")
            
            # Look for amount-related fields
            amount_fields = ['TOTAL_COST', 'TOTAL_AMOUNT', 'UNIT_COST', 'UNIT_PRICE']
            for field_name in amount_fields:
                if field_name in fields:
                    try:
                        amount = float(str(fields[field_name]).replace(",", ""))
                        if amount > 0:
                            amounts.append(amount)
                            relevant_data.append({
                                "tab": tab_name,
                                "field": field_name,
                                "amount": amount,
                                "date": fields.get("DATE", ""),
                                "row_index": result.get("row_index", 0)
                            })
                    except (ValueError, TypeError):
                        continue
            
            # Also check raw data for numeric values if no field mapping found amounts
            if not amounts:
                data = result.get("data", [])
                for i, value in enumerate(data):
                    if value and str(value).replace(".", "").replace(",", "").isdigit():
                        try:
                            amount = float(str(value).replace(",", ""))
                            if amount > 0:
                                amounts.append(amount)
                                relevant_data.append({
                                    "tab": tab_name,
                                    "field": f"COLUMN_{i}",
                                    "amount": amount,
                                    "row_data": data,
                                    "row_index": result.get("row_index", 0)
                                })
                        except ValueError:
                            continue
        
        if amounts:
            if len(amounts) == 1:
                data_item = relevant_data[0]
                answer = f"For {date_text}, I found an amount of {amounts[0]:,.2f}"
                if data_item.get("field") != f"COLUMN_0":
                    answer += f" ({data_item['field']})"
                answer += f" in {data_item['tab']}."
            else:
                total = sum(amounts)
                answer = f"For {date_text}, I found {len(amounts)} amounts totaling {total:,.2f}:\n"
                for i, (amount, data_item) in enumerate(zip(amounts[:5], relevant_data[:5])):
                    answer += f"• {data_item['tab']}: {amount:,.2f}"
                    if data_item.get("field") and not data_item["field"].startswith("COLUMN_"):
                        answer += f" ({data_item['field']})"
                    answer += "\n"
                if len(amounts) > 5:
                    answer += f"... and {len(amounts) - 5} more"
            
            return {
                "text": answer.strip(),
                "confidence": 0.8,
                "data_details": relevant_data[:3]
            }
        else:
            return {
                "text": f"I couldn't find any specific amounts for {date_text}. The data might be in a different format or date.",
                "confidence": 0.3,
                "suggestions": [
                    "Try asking for a different date format",
                    "Or ask: 'Show me all amounts from RO DETAILS'"
                ]
            }
    
    def _answer_data_by_date(self, query: str, analysis: Dict, results: List[Dict]) -> Dict[str, Any]:
        """Generate answer for data by date queries"""
        date_text = analysis["parameters"].get("date_text", "")
        
        if not results:
            return {
                "text": f"I couldn't find any data for {date_text}.",
                "confidence": 0.3,
                "suggestions": [
                    "Try a different date format",
                    "Or ask: 'Show me latest data from RO DETAILS'"
                ]
            }
        
        # Group results by tab
        tabs_data = {}
        for result in results:
            tab_name = result.get("tab_name", "Unknown")
            if tab_name not in tabs_data:
                tabs_data[tab_name] = []
            tabs_data[tab_name].append(result)
        
        answer = f"I found {len(results)} rows of data for {date_text} across {len(tabs_data)} tabs:\n"
        
        for tab_name, tab_results in list(tabs_data.items())[:3]:  # Show top 3 tabs
            answer += f"\n• **{tab_name}** ({len(tab_results)} rows):\n"
            
            # Show sample data from this tab
            for i, result in enumerate(tab_results[:2]):  # Show 2 samples per tab
                fields = result.get("fields", {})
                if fields:
                    field_items = []
                    for field, value in list(fields.items())[:4]:
                        if value and str(value).strip():
                            field_items.append(f"{field}: {value}")
                    if field_items:
                        answer += f"  Row {result.get('row_index', i+1)}: {', '.join(field_items)}\n"
        
        return {
            "text": answer.strip(),
            "confidence": 0.8,
            "data_details": results[:5]
        }
    
    def _answer_field_by_date(self, query: str, analysis: Dict, results: List[Dict]) -> Dict[str, Any]:
        """Generate answer for field by date queries (e.g., 'what is the ro1&2 feed tank level on 26.6.25')"""
        date_text = analysis["parameters"].get("date_text", "")
        
        # Try to detect what field they're asking about
        query_lower = query.lower()
        field_keywords = {
            'ro1&2 feed tank level': 'RO_1_2_FEED_TANK_LEVEL',
            'ro feed tank level': 'RO_1_2_FEED_TANK_LEVEL',
            'feed tank level': 'RO_1_2_FEED_TANK_LEVEL',
            'tank level': 'RO_1_2_FEED_TANK_LEVEL',
            'uf feed tank level': 'UF_FEED_TANK_LEVEL',
            'amount': ['TOTAL_COST', 'TOTAL_AMOUNT', 'UNIT_COST', 'UNIT_PRICE'],
            'cost': ['TOTAL_COST', 'TOTAL_AMOUNT'],
            'tds': ['TDS_VALUE', 'FEED_TDS', 'PERMEATE_TDS', 'REJECT_TDS'],
            'pressure': ['FEED_PRESSURE', 'PERMEATE_PRESSURE', 'REJECT_PRESSURE'],
            'permeate': ['RO_1_PERMEATE', 'RO_2_PERMEATE', 'TOTAL_PERMEATE']
        }
        
        target_fields = []
        for keyword, field_names in field_keywords.items():
            if keyword in query_lower:
                if isinstance(field_names, list):
                    target_fields.extend(field_names)
                else:
                    target_fields.append(field_names)
                break
        
        if not target_fields:
            # Fallback: look for any numeric fields
            target_fields = ['RO_1_2_FEED_TANK_LEVEL', 'UF_FEED_TANK_LEVEL', 'TOTAL_COST', 'TOTAL_AMOUNT']
        
        # Search for the field values
        found_values = []
        for result in results:
            fields = result.get("fields", {})
            tab_name = result.get("tab_name", "Unknown")
            
            for field_name in target_fields:
                if field_name in fields and fields[field_name]:
                    found_values.append({
                        "tab": tab_name,
                        "field": field_name,
                        "value": fields[field_name],
                        "date": fields.get("DATE", date_text),
                        "time": fields.get("TIME", ""),
                        "row_index": result.get("row_index", 0)
                    })
        
        if found_values:
            if len(found_values) == 1:
                val = found_values[0]
                answer = f"For {date_text}, the {val['field'].replace('_', ' ').title()} is {val['value']}"
                if val['time']:
                    answer += f" at {val['time']}"
                answer += f" (from {val['tab']})."
            else:
                answer = f"For {date_text}, I found {len(found_values)} values:\n"
                for val in found_values[:5]:
                    answer += f"• {val['tab']}: {val['field'].replace('_', ' ').title()} = {val['value']}"
                    if val['time']:
                        answer += f" at {val['time']}"
                    answer += "\n"
            
            return {
                "text": answer.strip(),
                "confidence": 0.9,
                "data_details": found_values[:3]
            }
        else:
            # Show available fields for that date
            available_fields = set()
            for result in results:
                fields = result.get("fields", {})
                available_fields.update(fields.keys())
            
            return {
                "text": f"I couldn't find the specific field you're looking for on {date_text}.",
                "confidence": 0.4,
                "suggestions": [
                    f"Available fields for {date_text}: {', '.join(list(available_fields)[:5])}",
                    "Try asking: 'Show me all data for 26.6.25'"
                ]
            }
    
    def _answer_data_by_time(self, query: str, analysis: Dict, results: List[Dict]) -> Dict[str, Any]:
        """Generate answer for data by time queries"""
        time_text = analysis["parameters"].get("time", "")
        
        time_data = []
        for result in results:
            fields = result.get("fields", {})
            data = result.get("data", [])
            
            # Check if this row contains the time in fields or raw data
            time_found = False
            if "TIME" in fields and time_text in str(fields["TIME"]):
                time_found = True
            elif any(time_text in str(val) for val in data):
                time_found = True
            
            if time_found:
                time_data.append({
                    "tab": result.get("tab_name", "Unknown"),
                    "fields": fields,
                    "data": [val for val in data if val and str(val).strip()],
                    "row_index": result.get("row_index", 0)
                })
        
        if time_data:
            answer = f"At {time_text}, I found data in {len(time_data)} row(s):\n"
            for i, item in enumerate(time_data[:3]):  # Show top 3
                answer += f"• {item['tab']}: "
                if item["fields"]:
                    # Show field data if available
                    field_summary = []
                    for field, value in list(item["fields"].items())[:4]:
                        if value:
                            field_summary.append(f"{field}: {value}")
                    answer += ", ".join(field_summary)
                else:
                    # Fallback to raw data
                    non_empty = item["data"]
                    answer += ", ".join([str(v) for v in non_empty[:5]])
                answer += "\n"
            
            return {
                "text": answer.strip(),
                "confidence": 0.8,
                "data_details": time_data[:3]
            }
        else:
            return {
                "text": f"I couldn't find any data specifically for {time_text}.",
                "confidence": 0.3,
                "suggestions": [
                    "Try a different time format like '11:00'",
                    "Or ask for data from a specific tab"
                ]
            }
    
    def _answer_latest_data(self, query: str, results: List[Dict]) -> Dict[str, Any]:
        """Generate answer for latest data queries"""
        if not results:
            return {
                "text": "No recent data available.",
                "confidence": 0.2
            }
        
        # Filter out header rows and find actual latest data
        data_rows = [r for r in results if not r.get("is_header", False) and r.get("fields")]
        
        if not data_rows:
            return {
                "text": "No recent data entries found (only headers available).",
                "confidence": 0.3
            }
        
        # Group by tab and get latest from each
        tabs_latest = {}
        for row in data_rows:
            tab_name = row.get('tab_name', 'Unknown')
            if tab_name not in tabs_latest:
                tabs_latest[tab_name] = row
            else:
                # Compare by row_index or synced_at to find more recent
                current_latest = tabs_latest[tab_name]
                if (row.get('row_index', 0) > current_latest.get('row_index', 0) or
                    row.get('synced_at', '') > current_latest.get('synced_at', '')):
                    tabs_latest[tab_name] = row
        
        if len(tabs_latest) == 1:
            # Single tab - show detailed data
            tab_name, latest = list(tabs_latest.items())[0]
            fields = latest.get("fields", {})
            
            answer = f"The latest data from {tab_name}:\n"
            
            if fields:
                field_items = []
                for field, value in list(fields.items())[:8]:
                    if value and str(value).strip():
                        field_items.append(f"{field}: {value}")
                
                if field_items:
                    answer += f"• {', '.join(field_items)}"
                else:
                    answer += "• No data values found in latest entry"
            else:
                answer += "• No field mapping available"
        else:
            # Multiple tabs - show summary
            answer = f"Latest data from {len(tabs_latest)} tabs:\n"
            
            for tab_name, latest in list(tabs_latest.items())[:3]:
                fields = latest.get("fields", {})
                answer += f"\n• **{tab_name}**:\n"
                
                if fields:
                    field_items = []
                    for field, value in list(fields.items())[:4]:
                        if value and str(value).strip():
                            field_items.append(f"{field}: {value}")
                    
                    if field_items:
                        answer += f"  {', '.join(field_items)}"
                    else:
                        answer += "  No data values in latest entry"
        
        return {
            "text": answer.strip(),
            "confidence": 0.9,
            "data_details": list(tabs_latest.values())[:3]
        }
    
    def _answer_total_amount(self, query: str, results: List[Dict]) -> Dict[str, Any]:
        """Generate answer for total amount queries"""
        all_amounts = []
        tabs_with_amounts = {}
        
        for result in results:
            fields = result.get("fields", {})
            data = result.get("data", [])
            tab_name = result.get("tab_name", "Unknown")
            
            # Check field data first
            amount_fields = ['TOTAL_COST', 'TOTAL_AMOUNT', 'UNIT_COST', 'UNIT_PRICE']
            for field_name in amount_fields:
                if field_name in fields:
                    try:
                        amount = float(str(fields[field_name]).replace(",", ""))
                        if amount > 0:
                            all_amounts.append(amount)
                            if tab_name not in tabs_with_amounts:
                                tabs_with_amounts[tab_name] = []
                            tabs_with_amounts[tab_name].append(amount)
                    except (ValueError, TypeError):
                        continue
            
            # Also check raw data
            for value in data:
                if value and str(value).replace(".", "").replace(",", "").isdigit():
                    try:
                        amount = float(str(value).replace(",", ""))
                        if amount > 0:
                            all_amounts.append(amount)
                            if tab_name not in tabs_with_amounts:
                                tabs_with_amounts[tab_name] = []
                            tabs_with_amounts[tab_name].append(amount)
                    except ValueError:
                        continue
        
        if all_amounts:
            total = sum(all_amounts)
            answer = f"Total amount across all data: {total:,.2f}\n"
            answer += f"Found {len(all_amounts)} amounts across {len(tabs_with_amounts)} tabs.\n"
            
            # Show breakdown by tab
            for tab, amounts in list(tabs_with_amounts.items())[:3]:
                tab_total = sum(amounts)
                answer += f"• {tab}: {tab_total:,.2f} ({len(amounts)} entries)\n"
            
            return {
                "text": answer.strip(),
                "confidence": 0.8,
                "breakdown": tabs_with_amounts
            }
        else:
            return {
                "text": "I couldn't find any numeric amounts in the available data.",
                "confidence": 0.3,
                "suggestions": [
                    "Try asking for data from a specific tab",
                    "Or ask: 'Show me RO DETAILS amounts'"
                ]
            }
    
    def _answer_general_query(self, query: str, results: List[Dict]) -> Dict[str, Any]:
        """Generate answer for general queries"""
        if not results:
            return {
                "text": "I found some data but couldn't determine what specific information you're looking for.",
                "confidence": 0.4
            }
        
        # Provide a summary of available data
        tabs = set(result.get("tab_name", "Unknown") for result in results)
        total_rows = len(results)
        
        answer = f"I found {total_rows} rows of data across {len(tabs)} tabs:\n"
        
        # Show sample from each tab
        tab_samples = {}
        for result in results:
            tab = result.get("tab_name", "Unknown")
            if tab not in tab_samples:
                fields = result.get("fields", {})
                if fields:
                    # Show field data if available
                    field_items = []
                    for field, value in list(fields.items())[:3]:
                        if value:
                            field_items.append(f"{field}: {value}")
                    if field_items:
                        tab_samples[tab] = field_items
                else:
                    # Fallback to raw data
                    data = result.get("data", [])
                    non_empty = [val for val in data if val and str(val).strip()]
                    if non_empty:
                        tab_samples[tab] = [str(v) for v in non_empty[:3]]
        
        for tab, sample in list(tab_samples.items())[:3]:
            answer += f"• {tab}: {', '.join(sample)}\n"
        
        return {
            "text": answer.strip(),
            "confidence": 0.6,
            "suggestions": [
                "Try asking more specifically: 'What is the amount on December 12th?'",
                "Or: 'Show me latest data from RO DETAILS'"
            ]
        }


# Global instance
_query_processor = None

def get_query_processor() -> QueryProcessor:
    """Get the global query processor instance"""
    global _query_processor
    if _query_processor is None:
        _query_processor = QueryProcessor()
    return _query_processor