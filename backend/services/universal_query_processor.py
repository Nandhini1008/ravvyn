"""
Universal Query Processor - Works with any sheet structure using X-Y-Z coordinates
Provides intelligent query processing with normalization, database search, and AI responses
"""

import re
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from services.universal_data_service import get_universal_data_service
from services.universal_query_normalizer import get_universal_query_normalizer
from services.universal_db_searcher import get_universal_database_searcher
from services.etp_tank_processor import get_etp_tank_processor

logger = logging.getLogger(__name__)


class UniversalQueryProcessor:
    """
    Universal query processor that works with any sheet structure
    Uses coordinate-based analysis and semantic understanding
    """
    
    def __init__(self):
        """Initialize universal query processor"""
        self.data_service = get_universal_data_service()
        self.query_normalizer = get_universal_query_normalizer()
        self.database_searcher = get_universal_database_searcher()
        self.etp_processor = get_etp_tank_processor()
        self.ai_service = None  # Will be initialized lazily to avoid circular imports
        
        # Universal query patterns (not sheet-specific)
        self.query_patterns = {
            'field_value_by_criteria': [
                r'what\s+is\s+(?:the\s+)?(.+?)\s+(?:on|for|in|at)\s+(.+)',
                r'(?:show|get|find)\s+(?:me\s+)?(.+?)\s+(?:on|for|from)\s+(.+)',
                r'(.+?)\s+(?:on|for|in)\s+(.+)',
            ],
            'latest_data': [
                r'(?:show|get|find)\s+(?:me\s+)?(?:the\s+)?latest\s+(?:data|information|entry)',
                r'(?:what\s+is\s+)?(?:the\s+)?(?:most\s+)?recent\s+(?:data|entry|information)',
                r'latest\s+(?:from\s+)?(.+)',
            ],
            'field_search': [
                r'(?:show|get|find)\s+(?:me\s+)?(?:all\s+)?(.+?)(?:\s+data|\s+values?)?$',
                r'what\s+(?:are\s+)?(?:the\s+)?(.+?)(?:\s+values?)?$',
                r'list\s+(?:all\s+)?(.+)',
            ],
            'data_by_date': [
                r'(?:provide|show|get|find)\s+(?:me\s+)?(?:all\s+)?(?:the\s+)?(?:data|datas|information|entries)\s+(?:entered\s+)?(?:on|for|from|in|at)\s+(?:the\s+)?(.+)',
                r'(?:all|everything)\s+(?:data|datas|entries|records)\s+(?:on|for|from|in|at)\s+(.+)',
                r'(?:what\s+happened\s+)?(?:on|in|at)\s+(.+)',
                r'data\s+(?:for|from|on)\s+(.+)',
            ],
            'coordinate_query': [
                r'(?:cell|value)\s+(?:at\s+)?(?:position\s+)?(?:row\s+)?(\d+)(?:\s*,\s*|\s+)(?:column\s+)?(\d+)',
                r'(?:x|column)\s*=?\s*(\d+)(?:\s*,\s*|\s+and\s+)(?:y|row)\s*=?\s*(\d+)',
            ],
            'summary_query': [
                r'(?:show|get|describe)\s+(?:me\s+)?(?:the\s+)?(?:sheet|structure|summary)',
                r'what\s+(?:fields|columns|data)\s+(?:are\s+)?(?:available|present)',
                r'analyze\s+(?:this\s+)?(?:sheet|data)',
            ]
        }
        
        # Semantic field categories for better matching
        self.field_semantics = {
            'level': ['level', 'height', 'depth', 'amount', 'quantity'],
            'time': ['time', 'hour', 'minute', 'timestamp', 'when'],
            'date': ['date', 'day', 'month', 'year', 'when'],
            'temperature': ['temp', 'temperature', 'heat', 'thermal'],
            'pressure': ['pressure', 'press', 'force', 'psi', 'bar'],
            'flow': ['flow', 'rate', 'speed', 'velocity'],
            'cost': ['cost', 'price', 'amount', 'money', 'expense'],
            'status': ['status', 'state', 'condition', 'active', 'running'],
            'tank': ['tank', 'container', 'vessel', 'reservoir'],
            'feed': ['feed', 'supply', 'input', 'source'],
        }
    
    def _get_ai_service(self):
        """Get AI service instance (lazy initialization to avoid circular imports)"""
        if self.ai_service is None:
            try:
                from services.ai import AIService
                self.ai_service = AIService()
            except Exception as e:
                logger.warning(f"Could not initialize AI service: {str(e)}")
                self.ai_service = None
        return self.ai_service
    
    def _generate_sqlite_response(self, query: str, raw_data: Dict[str, Any]) -> str:
        """
        Generate direct response from SQLite data without AI
        
        Args:
            query: Original user query
            raw_data: Raw data retrieved from the system
            
        Returns:
            Direct response with actual data values
        """
        try:
            # Handle different types of data
            if raw_data.get("values"):
                values = raw_data["values"]
                if len(values) == 1:
                    # Single result
                    value_info = values[0]
                    field_name = value_info.get('field_name', 'Unknown field')
                    value = value_info.get('value', 'No value')
                    tab_name = value_info.get('tab_name', 'Unknown tab')
                    row_index = value_info.get('row_index', 'N/A')
                    
                    response = f"The {field_name} is {value} (from {tab_name}, row {row_index})"
                    
                    # Add context if available
                    if value_info.get("row_data"):
                        context_fields = []
                        for fname, fvalue in list(value_info["row_data"].items())[:3]:
                            if fvalue and str(fvalue).strip() and fname != field_name:
                                context_fields.append(f"{fname}: {fvalue}")
                        if context_fields:
                            response += f". Context: {', '.join(context_fields)}"
                    
                    return response
                
                else:
                    # Multiple results
                    response = f"Found {len(values)} results:\n"
                    for i, value_info in enumerate(values[:10], 1):
                        field_name = value_info.get('field_name', 'Unknown field')
                        value = value_info.get('value', 'No value')
                        tab_name = value_info.get('tab_name', 'Unknown tab')
                        row_index = value_info.get('row_index', 'N/A')
                        
                        response += f"{i}. {field_name}: {value} (from {tab_name}, row {row_index})\n"
                        
                        # Add key context
                        if value_info.get("row_data"):
                            context_fields = []
                            for fname, fvalue in list(value_info["row_data"].items())[:2]:
                                if fvalue and str(fvalue).strip() and fname != field_name:
                                    context_fields.append(f"{fname}: {fvalue}")
                            if context_fields:
                                response += f"   Context: {', '.join(context_fields)}\n"
                    
                    if len(values) > 10:
                        response += f"... and {len(values) - 10} more results"
                    
                    return response.strip()
            
            elif raw_data.get("latest_data"):
                latest_data = raw_data["latest_data"]
                response = "Latest data:\n"
                
                for tab_name, tab_data in latest_data.items():
                    response += f"\nFrom {tab_name}:\n"
                    fields = tab_data.get("fields", {})
                    
                    for field, value in fields.items():
                        if value and str(value).strip():
                            response += f"  {field}: {value}\n"
                
                return response.strip()
            
            elif raw_data.get("results"):
                results = raw_data["results"]
                
                # Check if this is a date query with tab groups
                if raw_data.get("tab_groups"):
                    response = f"Data entries for the requested date ({len(results)} total):\n\n"
                    
                    tab_groups = raw_data["tab_groups"]
                    for tab_name, tab_results in list(tab_groups.items())[:5]:
                        response += f"📋 {tab_name} ({len(tab_results)} entries):\n"
                        
                        for i, result in enumerate(tab_results[:5], 1):
                            row_index = result.get('row_index', 'N/A')
                            row_data = result.get('row_data', {})
                            
                            response += f"  {i}. Row {row_index}: "
                            
                            # Show key data fields
                            data_items = []
                            for field, value in list(row_data.items())[:6]:
                                if value and str(value).strip():
                                    data_items.append(f"{field}: {value}")
                            
                            if data_items:
                                response += ", ".join(data_items)
                            else:
                                response += "No data"
                            
                            response += "\n"
                        
                        if len(tab_results) > 5:
                            response += f"  ... and {len(tab_results) - 5} more entries\n"
                        
                        response += "\n"
                    
                    if len(tab_groups) > 5:
                        response += f"... and {len(tab_groups) - 5} more tabs"
                    
                    return response.strip()
                
                else:
                    # Regular search results
                    response = f"Search results ({len(results)} found):\n"
                    
                    for i, result in enumerate(results[:10], 1):
                        field_name = result.get('field_name', 'Unknown')
                        value = result.get('value', 'No value')
                        tab_name = result.get('tab_name', 'Unknown tab')
                        row_index = result.get('row_index', 'N/A')
                        
                        response += f"{i}. {field_name}: {value} (from {tab_name}, row {row_index})\n"
                    
                    if len(results) > 10:
                        response += f"... and {len(results) - 10} more results"
                    
                    return response.strip()
            
            else:
                return raw_data.get("answer", "No specific data found.")
                
        except Exception as e:
            logger.error(f"Error generating SQLite response: {str(e)}")
            return raw_data.get("answer", "Error processing data.")
    
    async def _generate_natural_response(self, query: str, raw_data: Dict[str, Any]) -> str:
        """
        CRITICAL: AI/LLM is ONLY used for formatting responses, NOT for data retrieval
        All data comes from pure database operations (SQLite) before this step
        
        Uses enhanced response formatter to ensure data is always displayed when found
        
        Args:
            query: Original user query
            raw_data: Raw data ALREADY retrieved from database (NO AI involved in retrieval)
            
        Returns:
            Natural language formatted response (AI for formatting only)
        """
        try:
            # 🔍 DETAILED DATABASE DATA LOGGING FOR CROSS-CHECKING
            logger.info("=" * 80)
            logger.info(f"🔍 DATABASE DATA RETRIEVED FOR QUERY: '{query}'")
            logger.info("=" * 80)
            
            # Log raw data structure
            data_count = 0
            if raw_data.get("values"):
                data_count += len(raw_data["values"])
                logger.info(f"📊 VALUES FOUND: {len(raw_data['values'])} items")
                for i, value_info in enumerate(raw_data["values"][:10], 1):
                    logger.info(f"  {i}. Field: {value_info.get('field_name', 'Unknown')}")
                    logger.info(f"     Value: {value_info.get('value', 'No value')}")
                    logger.info(f"     Tab: {value_info.get('tab_name', 'Unknown')}")
                    logger.info(f"     Row: {value_info.get('row_index', 'N/A')}")
                    if value_info.get('row_data'):
                        logger.info(f"     Context: {dict(list(value_info['row_data'].items())[:5])}")
                    logger.info("")
                
                if len(raw_data["values"]) > 10:
                    logger.info(f"     ... and {len(raw_data['values']) - 10} more values")
            
            if raw_data.get("results"):
                data_count += len(raw_data["results"])
                logger.info(f"📊 RESULTS FOUND: {len(raw_data['results'])} items")
                
                if raw_data.get("tab_groups"):
                    logger.info("📋 GROUPED BY TABS:")
                    for tab_name, tab_results in list(raw_data["tab_groups"].items())[:5]:
                        logger.info(f"  📁 {tab_name}: {len(tab_results)} entries")
                        for i, result in enumerate(tab_results[:3], 1):
                            row_data = result.get('row_data', {})
                            logger.info(f"    Row {result.get('row_index', 'N/A')}: {dict(list(row_data.items())[:6])}")
                else:
                    for i, result in enumerate(raw_data["results"][:10], 1):
                        logger.info(f"  {i}. Field: {result.get('field_name', 'Unknown')}")
                        logger.info(f"     Value: {result.get('value', 'No value')}")
                        logger.info(f"     Tab: {result.get('tab_name', 'Unknown')}")
                        logger.info(f"     Row: {result.get('row_index', 'N/A')}")
                        logger.info("")
            
            if raw_data.get("latest_data"):
                data_count += len(raw_data["latest_data"])
                logger.info(f"📊 LATEST DATA FOUND: {len(raw_data['latest_data'])} tabs")
                for tab_name, tab_data in raw_data["latest_data"].items():
                    fields = tab_data.get("fields", {})
                    logger.info(f"  📁 {tab_name}:")
                    for field, value in list(fields.items())[:8]:
                        if value and str(value).strip():
                            logger.info(f"    {field}: {value}")
                    logger.info("")
            
            if data_count == 0:
                logger.info("❌ NO DATA FOUND IN DATABASE")
                logger.info(f"Raw data keys: {list(raw_data.keys())}")
                logger.info(f"Raw data content: {raw_data}")
            else:
                logger.info(f"✅ TOTAL DATA ITEMS FOUND: {data_count}")
            
            logger.info("=" * 80)
            
            # Use enhanced response formatter to handle LLM formatting issues
            from services.enhanced_response_formatter import get_enhanced_response_formatter
            
            formatter = get_enhanced_response_formatter()
            formatted_response = await formatter.format_response(query, raw_data)
            
            # Log formatting method for monitoring
            logger.info(f"🎯 RESPONSE FORMATTING RESULT:")
            logger.info(f"   Method: {formatted_response.formatting_method}")
            logger.info(f"   Data Count: {formatted_response.data_count}")
            logger.info(f"   Fallback Used: {formatted_response.fallback_used}")
            logger.info(f"   Validation Passed: {formatted_response.validation_passed}")
            logger.info(f"   Processing Time: {formatted_response.processing_time_ms}ms")
            
            if formatted_response.warnings:
                logger.warning(f"⚠️  Formatting warnings: {formatted_response.warnings}")
            
            # Log final response preview
            response_preview = formatted_response.response_text[:300] + "..." if len(formatted_response.response_text) > 300 else formatted_response.response_text
            logger.info(f"📝 FINAL RESPONSE PREVIEW:\n{response_preview}")
            logger.info("=" * 80)
            
            # 📊 EXPORT DISABLED - User can export on demand via "View in Sheets" button
            # Raw data is stored in the response for on-demand export
            
            return formatted_response.response_text
            
        except Exception as e:
            logger.error(f"Enhanced formatting failed: {str(e)}, falling back to direct summary")
            # Ultimate fallback to direct data summary
            return self._generate_direct_data_summary(query, raw_data)
    
    async def _try_ai_formatting(self, query: str, raw_data: Dict[str, Any], ai_service) -> Optional[str]:
        """Try to format response using AI with timeout and error handling"""
        try:
            # Build context for AI
            context_parts = []
            context_parts.append(f"User asked: '{query}'")
            
            # Add data found with comprehensive details
            if raw_data.get("values"):
                values = raw_data["values"]
                context_parts.append(f"\nFOUND DATA ({len(values)} results):")
                for i, value_info in enumerate(values[:10], 1):
                    field_name = value_info.get('field_name', 'Unknown field')
                    value = value_info.get('value', 'No value')
                    tab_name = value_info.get('tab_name', 'Unknown tab')
                    row_index = value_info.get('row_index', 'N/A')
                    
                    context_parts.append(f"{i}. {field_name}: {value} (from {tab_name}, row {row_index})")
                    
                    # Add row context
                    if value_info.get("row_data"):
                        context_fields = []
                        for fname, fvalue in list(value_info["row_data"].items())[:4]:
                            if fvalue and str(fvalue).strip() and fname != field_name:
                                context_fields.append(f"{fname}: {fvalue}")
                        if context_fields:
                            context_parts.append(f"   Context: {', '.join(context_fields)}")
            
            elif raw_data.get("results"):
                results = raw_data["results"]
                
                # Check if this is a date query with tab groups
                if raw_data.get("tab_groups"):
                    context_parts.append(f"\nDATE QUERY RESULTS ({len(results)} total entries):")
                    tab_groups = raw_data["tab_groups"]
                    
                    for tab_name, tab_results in list(tab_groups.items())[:5]:
                        context_parts.append(f"\n📋 {tab_name} ({len(tab_results)} entries):")
                        
                        for i, result in enumerate(tab_results[:3], 1):
                            row_index = result.get('row_index', 'N/A')
                            row_data = result.get('row_data', {})
                            
                            data_items = []
                            for field, value in list(row_data.items())[:6]:
                                if value and str(value).strip():
                                    data_items.append(f"{field}: {value}")
                            
                            context_parts.append(f"  Row {row_index}: {', '.join(data_items) if data_items else 'No data'}")
                else:
                    context_parts.append(f"\nSEARCH RESULTS ({len(results)} found):")
                    for i, result in enumerate(results[:10], 1):
                        field_name = result.get('field_name', 'Unknown')
                        value = result.get('value', 'No value')
                        tab_name = result.get('tab_name', 'Unknown tab')
                        row_index = result.get('row_index', 'N/A')
                        
                        context_parts.append(f"{i}. {field_name}: {value} (from {tab_name}, row {row_index})")
            
            elif raw_data.get("latest_data"):
                latest_data = raw_data["latest_data"]
                context_parts.append(f"\nLATEST DATA FOUND:")
                
                for tab_name, tab_data in latest_data.items():
                    context_parts.append(f"From {tab_name}:")
                    fields = tab_data.get("fields", {})
                    
                    for field, value in fields.items():
                        if value and str(value).strip():
                            context_parts.append(f"  {field}: {value}")
            
            # Create AI prompt for better formatting
            ai_prompt = f"""The user asked: '{query}'

Here is the data I found. Please format this into a clear, helpful response for the user.

{chr(10).join(context_parts)}

INSTRUCTIONS:
- Format the response in a clear, organized way
- Use bullet points or tables if helpful
- Be conversational but informative
- Show the actual data values clearly
- Group related information together
- Keep it concise but complete"""
            
            # Get AI response with timeout
            import asyncio
            ai_response = await asyncio.wait_for(
                ai_service._retry_ai_request(
                    ai_service._make_chat_request,
                    [{"role": "user", "content": ai_prompt}]
                ),
                timeout=10.0  # 10 second timeout
            )
            
            if ai_response and ai_response.strip():
                logger.info("✅ AI formatting successful")
                return ai_response.strip()
            else:
                logger.warning("AI returned empty response")
                return None
                
        except asyncio.TimeoutError:
            logger.warning("AI formatting timed out")
            return None
        except Exception as e:
            logger.error(f"AI formatting error: {str(e)}")
            return None
            # Build context for AI
            context_parts = []
            
            # Add query context
            context_parts.append(f"User asked: '{query}'")
            
            # Add data found - Include more comprehensive data
            if raw_data.get("values"):
                context_parts.append(f"\nFOUND DATA ({len(raw_data['values'])} results):")
                for i, value_info in enumerate(raw_data["values"][:10], 1):  # Show more results
                    field_name = value_info.get('field_name', 'Unknown field')
                    value = value_info.get('value', 'No value')
                    tab_name = value_info.get('tab_name', 'Unknown tab')
                    coordinates = value_info.get('coordinates', {})
                    
                    context_parts.append(f"{i}. {field_name}: {value} (from {tab_name})")
                    
                    # Add row context with more details
                    if value_info.get("row_data"):
                        row_data = value_info["row_data"]
                        context_fields = []
                        for fname, fvalue in list(row_data.items())[:6]:  # Show more context
                            if fvalue and str(fvalue).strip() and fname != field_name:
                                context_fields.append(f"{fname}: {fvalue}")
                        if context_fields:
                            context_parts.append(f"   Row context: {', '.join(context_fields)}")
                    
                    if coordinates:
                        context_parts.append(f"   Location: Row {coordinates.get('y', 'N/A')}, Column {coordinates.get('x', 'N/A')}")
                
                if len(raw_data["values"]) > 10:
                    context_parts.append(f"... and {len(raw_data['values']) - 10} more results")
            
            # Handle when we're showing fallback data (no exact criteria match)
            if raw_data.get("is_fallback"):
                context_parts.append(f"\nNote: No exact match for the specific criteria, showing available data for this field:")
            
            elif raw_data.get("latest_data"):
                context_parts.append(f"\nLATEST DATA FOUND:")
                for tab_name, tab_data in raw_data["latest_data"].items():
                    context_parts.append(f"From {tab_name}:")
                    fields = tab_data.get("fields", {})
                    coordinates = tab_data.get("coordinates", {})
                    
                    # Show all non-empty fields
                    for field, value in fields.items():
                        if value and str(value).strip():
                            context_parts.append(f"  {field}: {value}")
                    
                    if coordinates:
                        context_parts.append(f"  Location: Row {coordinates.get('y', 'N/A')}")
            
            elif raw_data.get("results"):
                context_parts.append(f"\nSEARCH RESULTS ({len(raw_data['results'])} found):")
                for i, result in enumerate(raw_data["results"][:10], 1):  # Show more results
                    field_name = result.get('field_name', 'Unknown')
                    value = result.get('value', 'No value')
                    tab_name = result.get('tab_name', 'Unknown tab')
                    row_index = result.get('row_index', 'N/A')
                    
                    context_parts.append(f"{i}. {field_name}: {value} (from {tab_name}, row {row_index})")
                    
                    # Add row context if available
                    if result.get("row_data"):
                        row_context = []
                        for fname, fvalue in list(result["row_data"].items())[:4]:
                            if fvalue and str(fvalue).strip() and fname != field_name:
                                row_context.append(f"{fname}: {fvalue}")
                        if row_context:
                            context_parts.append(f"   Context: {', '.join(row_context)}")
                
                if len(raw_data["results"]) > 10:
                    context_parts.append(f"... and {len(raw_data['results']) - 10} more results")
            
            # Create AI prompt - ONLY FOR FORMATTING (data already retrieved from database)
            if raw_data.get("values") or raw_data.get("latest_data") or raw_data.get("results") or raw_data.get("fallback_values"):
                data_count = (len(raw_data.get("results", [])) + 
                             len(raw_data.get("values", [])) + 
                             len(raw_data.get("fallback_values", [])))
                
                # For date queries with many results, use direct summary to avoid AI confusion
                if data_count > 100 and any(date_word in query.lower() for date_word in ['12-12', '12.12', '12/12', 'december']):
                    logger.info(f"Using direct summary for date query with {data_count} results to avoid AI confusion")
                    return self._generate_direct_data_summary(query, raw_data)
                
                ai_prompt = f"""CRITICAL: DATA WAS FOUND! You are formatting a SUCCESS response.

The user asked: '{query}'

✅ SUCCESS: The database search found {data_count} matching entries.

Here is the actual data that was retrieved from the database:

{chr(10).join(context_parts)}

FORMATTING INSTRUCTIONS:
- Start with "I found data for your query!"
- Show the specific values that were found
- List the actual data entries with their values
- Include dates, sources, and context
- Be positive and informative - DATA WAS FOUND!
- Format like: "Here's what I found for [date/query]: [list the actual values]"
- Do NOT say "no data found" - DATA EXISTS!"""
            else:
                ai_prompt = f"""IMPORTANT: You are ONLY formatting a response. The database search was already completed.

The user asked: '{query}'

The database search found no matching data. Format a helpful response.

{chr(10).join(context_parts)}

FORMATTING INSTRUCTIONS:
- Format a brief, helpful "no data found" response
- Explain what was searched for
- Keep it conversational and supportive
- Do NOT attempt to retrieve data - only format the response"""
            
            # Get AI response
            ai_response = await ai_service._retry_ai_request(
                ai_service._make_chat_request,
                [{"role": "user", "content": ai_prompt}]
            )
            
            # Check if AI response incorrectly says "no data" when data exists
            if ai_response and data_count > 0:
                ai_lower = ai_response.lower()
                negative_phrases = [
                    'no data', 'not find', 'couldn\'t find', 'no entries', 'no specific',
                    'couldn\'t find any', 'no entries matching', 'none of the entries',
                    'nothing for', 'no information', 'not contain any entries',
                    'couldn\'t find specific', 'no entries for', 'not find any'
                ]
                
                if any(phrase in ai_lower for phrase in negative_phrases):
                    logger.warning(f"AI incorrectly said 'no data' when {data_count} results exist. Using direct summary.")
                    return self._generate_direct_data_summary(query, raw_data)
            
            return ai_response.strip() if ai_response else self._generate_direct_data_summary(query, raw_data)
            
        except Exception as e:
            logger.error(f"Error generating natural response with AI: {str(e)}")
            # Fallback to direct data summary
            return self._generate_direct_data_summary(query, raw_data)
    
    def _generate_direct_data_summary(self, query: str, raw_data: Dict[str, Any]) -> str:
        """Generate direct data summary when AI fails or gets confused"""
        try:
            # Check what type of data we have
            if raw_data.get("results"):
                results = raw_data["results"]
                if not results:
                    return f"No data found for '{query}'."
                
                response = f"✅ Found {len(results)} entries for '{query}':\n\n"
                
                # Group by tab for better organization
                by_tab = {}
                for result in results[:20]:  # Limit to first 20 for readability
                    tab = result.get('tab_name', 'Unknown')
                    if tab not in by_tab:
                        by_tab[tab] = []
                    by_tab[tab].append(result)
                
                for tab_name, tab_results in by_tab.items():
                    response += f"📋 **{tab_name}** ({len(tab_results)} entries):\n"
                    
                    for i, result in enumerate(tab_results[:5], 1):
                        field_name = result.get('field_name', 'Data')
                        value = result.get('value', 'N/A')
                        row_data = result.get('row_data', {})
                        
                        response += f"  {i}. {field_name}: {value}\n"
                        
                        # Add context from row data
                        context_items = []
                        for fname, fvalue in list(row_data.items())[:4]:
                            if fvalue and str(fvalue).strip() and fname != field_name:
                                context_items.append(f"{fname}: {fvalue}")
                        
                        if context_items:
                            response += f"     Context: {', '.join(context_items)}\n"
                    
                    if len(tab_results) > 5:
                        response += f"     ... and {len(tab_results) - 5} more entries\n"
                    response += "\n"
                
                if len(results) > 20:
                    response += f"... and {len(results) - 20} more entries total."
                
                return response.strip()
            
            elif raw_data.get("values"):
                values = raw_data["values"]
                if not values:
                    return f"No data found for '{query}'."
                
                response = f"✅ Found {len(values)} values for '{query}':\n\n"
                
                for i, value_info in enumerate(values[:10], 1):
                    field_name = value_info.get('field_name', 'Field')
                    value = value_info.get('value', 'N/A')
                    tab_name = value_info.get('tab_name', 'Unknown')
                    
                    response += f"{i}. {field_name}: {value} (from {tab_name})\n"
                    
                    # Add row context
                    if value_info.get('row_data'):
                        context_items = []
                        for fname, fvalue in list(value_info['row_data'].items())[:3]:
                            if fvalue and str(fvalue).strip() and fname != field_name:
                                context_items.append(f"{fname}: {fvalue}")
                        
                        if context_items:
                            response += f"   Context: {', '.join(context_items)}\n"
                
                if len(values) > 10:
                    response += f"... and {len(values) - 10} more values."
                
                return response.strip()
            
            else:
                return raw_data.get("answer", f"Found data for '{query}' but couldn't format it properly.")
                
        except Exception as e:
            logger.error(f"Error in direct data summary: {str(e)}")
            return f"Found data for '{query}' but encountered formatting error."
    
    def _is_date_query_with_results(self, query: str, raw_data: Dict[str, Any]) -> bool:
        """Check if this is a date query with results that should use direct summary"""
        query_lower = query.lower()
        
        # Check if query contains date patterns
        date_indicators = [
            '12-12', '12.12', '12/12', 'december 12', 'dec 12',
            'for 12', 'on 12', 'data for', 'information on'
        ]
        
        has_date_query = any(indicator in query_lower for indicator in date_indicators)
        
        # Check if we have results
        has_results = (raw_data.get("results") or raw_data.get("values") or 
                      raw_data.get("fallback_values") or raw_data.get("latest_data"))
        
        return has_date_query and has_results
    
    async def process_query(self, query: str, sheet_id: str = None, 
                          tab_name: str = None) -> Dict[str, Any]:
        """
        Process any query using comprehensive universal approach
        DATA RETRIEVAL: Pure database operations only (NO AI/LLM)
        AI/LLM: Only for final response formatting
        
        Args:
            query: Natural language query
            sheet_id: Optional sheet ID (for compatibility, but searches all sheets)
            tab_name: Optional tab name (for compatibility, but searches all tabs)
            
        Returns:
            Query result with answer and supporting data
        """
        logger.info(f"🔍 Processing universal query: {query}")
        
        try:
            # Step 0: Check if this is an ETP tank capacity query (specialized processing)
            if self.etp_processor.is_etp_query(query):
                logger.info(f"🏭 Detected ETP tank query, using specialized processor")
                etp_result = await self.etp_processor.process_etp_query(query, sheet_id)
                
                if etp_result["success"]:
                    return {
                        "success": True,
                        "query": query,
                        "query_type": "etp_tank_details",
                        "answer": etp_result["answer"],
                        "raw_data": etp_result["etp_data"],
                        "confidence": 0.95,  # High confidence for specialized processing
                        "data_found": etp_result["data_found"],
                        "processing_method": "etp_specialized",
                        "date": etp_result.get("date"),
                        "suggestions": []
                    }
                else:
                    # Fall back to regular processing if ETP processing fails
                    logger.warning(f"ETP processing failed: {etp_result.get('error')}, falling back to regular processing")
            
            # Step 1: Normalize the query to standardized format (NO AI - pure pattern matching)
            normalized_query = self.query_normalizer.normalize_query(query)
            logger.info(f"📋 Normalized: type={normalized_query.query_type}, scope={normalized_query.scope}")
            
            # Step 2: Search entire database using X-Y-Z coordinates (NO AI - pure SQL/database)
            search_results = await self.database_searcher.search_database(normalized_query)
            
            if not search_results["success"]:
                return {
                    "success": False,
                    "query": query,
                    "error": search_results.get("error", "Database search failed"),
                    "answer": "I couldn't search the database. Please try again.",
                    "suggestions": ["Check if sheets are synced", "Try a different query"]
                }
            
            # Step 3: Process results based on scope (NO AI - pure data processing)
            if normalized_query.scope == 'single_value':
                result = await self._process_single_value_results(search_results, normalized_query)
            elif normalized_query.scope == 'multiple_values':
                result = await self._process_multiple_values_results(search_results, normalized_query)
            else:  # all_related
                result = await self._process_all_related_results(search_results, normalized_query)
            
            # Step 4: ONLY NOW use AI for formatting the response (NOT for data retrieval)
            natural_answer = await self._generate_natural_response(query, result)
            
            return {
                "success": True,
                "query": query,
                "query_type": normalized_query.query_type,
                "answer": natural_answer,  # AI-formatted response
                "raw_data": result,  # Pure database results
                "confidence": normalized_query.confidence,
                "data_found": len(search_results.get("results", [])),
                "supporting_data": result.get("supporting_data", []),
                "suggestions": result.get("suggestions", []),
                "search_metadata": search_results.get("search_metadata", {}),
                "sheets_searched": search_results.get("sheets_searched", 0)
            }
            
        except Exception as e:
            logger.error(f"Error processing universal query: {str(e)}")
            return {
                "success": False,
                "query": query,
                "error": str(e),
                "answer": "I'm sorry, I couldn't process your query. Please try rephrasing it.",
                "suggestions": [
                    "Try: 'What is the [field name] on [date]?'",
                    "Or: 'Show me latest data'",
                    "Or: 'Get data for [date]'"
                ]
            }
    
    def _analyze_query(self, query: str) -> Dict[str, Any]:
        """Analyze query to determine type and extract parameters"""
        analysis = {
            "type": "general",
            "parameters": {},
            "confidence": 0.5,
            "extracted_entities": {}
        }
        
        # Check for date patterns first (higher priority)
        date_keywords = ['on', 'for', 'in', 'at', 'during']
        has_date_context = any(keyword in query.lower() for keyword in date_keywords)
        
        # Extract entities first to help with classification
        analysis["extracted_entities"] = self._extract_entities(query)
        has_dates = bool(analysis["extracted_entities"]["dates"])
        
        # Priority order for query types (date queries first)
        priority_order = ['data_by_date', 'coordinate_query', 'summary_query', 'latest_data', 'field_search', 'field_value_by_criteria']
        
        # Check each query pattern in priority order
        for query_type in priority_order:
            if query_type not in self.query_patterns:
                continue
                
            patterns = self.query_patterns[query_type]
            for pattern in patterns:
                match = re.search(pattern, query, re.IGNORECASE)
                if match:
                    # Special handling for date queries
                    if query_type == "data_by_date" and (has_dates or has_date_context):
                        analysis["type"] = query_type
                        analysis["confidence"] = 0.9
                        analysis["parameters"]["date"] = match.group(1).strip()
                        break
                    elif query_type == "data_by_date":
                        # Skip if no date context
                        continue
                    else:
                        analysis["type"] = query_type
                        analysis["confidence"] = 0.8
                        
                        # Extract parameters based on query type
                        if query_type == "field_value_by_criteria":
                            analysis["parameters"]["field"] = match.group(1).strip()
                            analysis["parameters"]["criteria"] = match.group(2).strip()
                        elif query_type == "latest_data":
                            if match.groups():
                                analysis["parameters"]["source"] = match.group(1).strip()
                        elif query_type == "field_search":
                            analysis["parameters"]["field"] = match.group(1).strip()
                        elif query_type == "coordinate_query":
                            analysis["parameters"]["x"] = int(match.group(1))
                            analysis["parameters"]["y"] = int(match.group(2))
                        
                        break
            
            if analysis["type"] != "general":
                break
        
        # Extract additional entities
        analysis["extracted_entities"] = self._extract_entities(query)
        
        # Enhance field matching with semantics
        if "field" in analysis["parameters"]:
            analysis["parameters"]["semantic_field"] = self._enhance_field_semantics(
                analysis["parameters"]["field"]
            )
        
        logger.info(f"📊 Universal query analysis: {analysis}")
        return analysis
    
    def _extract_entities(self, query: str) -> Dict[str, Any]:
        """Extract entities like dates, numbers, field names"""
        entities = {
            "dates": [],
            "numbers": [],
            "field_hints": []
        }
        
        # Extract dates
        date_patterns = [
            r'\d{1,2}[./\-]\d{1,2}[./\-]\d{2,4}',
            r'\d{4}[./\-]\d{1,2}[./\-]\d{1,2}',
        ]
        
        for pattern in date_patterns:
            matches = re.findall(pattern, query)
            entities["dates"].extend(matches)
        
        # Extract numbers
        number_pattern = r'\b\d+(?:\.\d+)?\b'
        entities["numbers"] = [float(n) for n in re.findall(number_pattern, query)]
        
        # Extract field hints using semantic categories
        for category, keywords in self.field_semantics.items():
            for keyword in keywords:
                if keyword in query.lower():
                    entities["field_hints"].append(category)
        
        return entities
    
    def _enhance_field_semantics(self, field_query: str) -> List[str]:
        """Enhance field query with semantic understanding"""
        enhanced_fields = [field_query]
        field_lower = field_query.lower()
        
        # Add semantic variations
        for category, keywords in self.field_semantics.items():
            if any(keyword in field_lower for keyword in keywords):
                enhanced_fields.append(category)
                enhanced_fields.extend(keywords)
        
        return list(set(enhanced_fields))
    
    async def _handle_field_value_query(self, analysis: Dict[str, Any], 
                                      sheet_id: str, tab_name: str) -> Dict[str, Any]:
        """Handle field value queries (e.g., 'what is the level on 26.6.25')"""
        field_query = analysis["parameters"]["field"]
        criteria_text = analysis["parameters"]["criteria"]
        
        # Parse criteria
        criteria = {"search_text": criteria_text}
        
        # Check if criteria contains a date
        if analysis["extracted_entities"]["dates"]:
            criteria["date"] = analysis["extracted_entities"]["dates"][0]
        
        # Get field value
        result = await self.data_service.get_field_value(
            field_query, criteria, sheet_id, tab_name
        )
        
        # If no results with criteria, try without criteria and use that data directly
        if result["success"] and not result["values"] and criteria:
            logger.info(f"No data found with criteria {criteria}, getting all available data for field...")
            fallback_result = await self.data_service.get_field_value(
                field_query, {}, sheet_id, tab_name
            )
            if fallback_result["success"] and fallback_result["values"]:
                # Use fallback data as main data - just show what we have
                result["values"] = fallback_result["values"][:10]  # Show more data
                result["is_fallback"] = True  # Mark as fallback for AI context
        
        if result["success"] and result["values"]:
            values = result["values"]
            
            if len(values) == 1:
                value_info = values[0]
                answer = f"The {field_query} is {value_info['value']}"
                
                # Add context if available
                if value_info.get("row_data"):
                    context_fields = []
                    for fname, fvalue in list(value_info["row_data"].items())[:3]:
                        if fvalue and fname.lower() != field_query.lower():
                            context_fields.append(f"{fname}: {fvalue}")
                    
                    if context_fields:
                        answer += f" (Context: {', '.join(context_fields)})"
                
                answer += f" from {value_info['tab_name']}."
            else:
                answer = f"Found {len(values)} values for {field_query}:\n"
                for i, value_info in enumerate(values[:5]):
                    answer += f"• {value_info['tab_name']}: {value_info['value']}\n"
                
                if len(values) > 5:
                    answer += f"... and {len(values) - 5} more"
            
            return {
                "answer": answer.strip(),
                "confidence": 0.9,
                "data_count": len(values),
                "supporting_data": values[:3],
                "values": values  # Add raw values for AI processing
            }
        else:
            return {
                "answer": f"No data found for '{field_query}'.",
                "confidence": 0.3,
                "values": [],
                "suggestions": []  # Remove suggestions to keep it simple
            }
    
    async def _handle_latest_data_query(self, analysis: Dict[str, Any], 
                                      sheet_id: str, tab_name: str) -> Dict[str, Any]:
        """Handle latest data queries"""
        result = await self.data_service.get_latest_data(sheet_id, tab_name)
        
        if result["success"] and result["latest_data"]:
            latest_data = result["latest_data"]
            
            if len(latest_data) == 1:
                # Single tab
                tab_name, tab_data = list(latest_data.items())[0]
                fields = tab_data["fields"]
                
                answer = f"Latest data from {tab_name}:\n"
                
                # Show non-empty fields
                field_items = []
                for field, value in fields.items():
                    if value and str(value).strip():
                        field_items.append(f"{field}: {value}")
                
                if field_items:
                    answer += "• " + ", ".join(field_items[:8])
                    if len(field_items) > 8:
                        answer += f" (and {len(field_items) - 8} more fields)"
                else:
                    answer += "• No data values found in latest entry"
            else:
                # Multiple tabs
                answer = f"Latest data from {len(latest_data)} tabs:\n"
                
                for tab_name, tab_data in list(latest_data.items())[:3]:
                    fields = tab_data["fields"]
                    non_empty_fields = [f"{k}: {v}" for k, v in fields.items() 
                                      if v and str(v).strip()]
                    
                    answer += f"\n• **{tab_name}**: "
                    if non_empty_fields:
                        answer += ", ".join(non_empty_fields[:4])
                    else:
                        answer += "No data values"
            
            return {
                "answer": answer.strip(),
                "confidence": 0.9,
                "data_count": len(latest_data),
                "supporting_data": list(latest_data.values())[:3],
                "latest_data": latest_data  # Add raw data for AI processing
            }
        else:
            return {
                "answer": "No recent data found.",
                "confidence": 0.3,
                "latest_data": {},
                "suggestions": []
            }
    
    async def _handle_field_search_query(self, analysis: Dict[str, Any], 
                                       sheet_id: str, tab_name: str) -> Dict[str, Any]:
        """Handle field search queries"""
        field_query = analysis["parameters"]["field"]
        
        # Use search functionality
        result = await self.data_service.search_data(field_query, sheet_id, tab_name, limit=50)
        
        if result["success"] and result["results"]:
            results = result["results"]
            
            # Group by field name
            field_groups = {}
            for res in results:
                field_name = res["field_name"]
                if field_name not in field_groups:
                    field_groups[field_name] = []
                field_groups[field_name].append(res)
            
            answer = f"Found {result['total_matches']} matches for '{field_query}':\n"
            
            for field_name, field_results in list(field_groups.items())[:3]:
                answer += f"\n• **{field_name}** ({len(field_results)} values):\n"
                
                for res in field_results[:3]:
                    answer += f"  - {res['value']} (from {res['tab_name']})\n"
                
                if len(field_results) > 3:
                    answer += f"  ... and {len(field_results) - 3} more\n"
            
            return {
                "answer": answer.strip(),
                "confidence": 0.8,
                "data_count": result["total_matches"],
                "supporting_data": results[:5],
                "results": results  # Add raw results for AI processing
            }
        else:
            return {
                "answer": f"No data found for '{field_query}'.",
                "confidence": 0.3,
                "results": [],
                "suggestions": []
            }
    
    async def _handle_date_query(self, analysis: Dict[str, Any], 
                               sheet_id: str, tab_name: str) -> Dict[str, Any]:
        """Handle date-based queries using direct SQL search"""
        date_text = analysis["parameters"]["date"]
        
        # Use direct SQL search for better date matching
        try:
            from services.database import get_db_context
            
            all_results = []
            
            with get_db_context() as db:
                # Create flexible date patterns
                date_patterns = [
                    f'%"{date_text}"%',  # JSON string match
                    f'%{date_text}%',    # General substring
                ]
                
                # Handle different date formats
                if '/' in date_text:
                    alt_date = date_text.replace('/', '.')
                    date_patterns.extend([f'%"{alt_date}"%', f'%{alt_date}%'])
                elif '.' in date_text:
                    alt_date = date_text.replace('.', '/')
                    date_patterns.extend([f'%"{alt_date}"%', f'%{alt_date}%'])
                elif '-' in date_text:
                    alt_date1 = date_text.replace('-', '.')
                    alt_date2 = date_text.replace('-', '/')
                    date_patterns.extend([f'%{alt_date1}%', f'%{alt_date2}%'])
                
                # Build OR conditions
                or_conditions = []
                search_params = {'sheet_id': sheet_id or self.data_service.default_sheet_id}
                
                for i, pattern in enumerate(date_patterns):
                    param_name = f'pattern_{i}'
                    or_conditions.append(f'row_data LIKE :{param_name}')
                    search_params[param_name] = pattern
                
                # Build query - search all tabs if no specific tab
                if tab_name:
                    search_params['tab_name'] = tab_name
                    tab_condition = "AND tab_name = :tab_name"
                else:
                    tab_condition = ""
                
                sql_query = f"""
                    SELECT tab_name, row_index, row_data FROM sheets_data 
                    WHERE sheet_id = :sheet_id 
                    {tab_condition}
                    AND row_index > 0
                    AND ({' OR '.join(or_conditions)})
                    ORDER BY tab_name, row_index
                    LIMIT 100
                """
                
                logger.info(f"🔍 Date query SQL: {sql_query}")
                logger.info(f"🔍 Searching for date: {date_text}")
                
                result = db.execute(sql_query, search_params)
                matching_rows = result.fetchall()
                
                logger.info(f"✅ Found {len(matching_rows)} rows for date: {date_text}")
                
                # Process results
                for row_tuple in matching_rows:
                    try:
                        tab_name_db = row_tuple[0]
                        row_index = row_tuple[1]
                        row_data_str = row_tuple[2]
                        
                        # Parse row data
                        import json
                        row_data = json.loads(row_data_str) if isinstance(row_data_str, str) else row_data_str
                        
                        if not row_data or not isinstance(row_data, list):
                            continue
                        
                        # Build result with all fields
                        result_item = {
                            "tab_name": tab_name_db,
                            "row_index": row_index,
                            "row_data": {},
                            "date_found": date_text
                        }
                        
                        # Map data to field names (simplified)
                        for i, value in enumerate(row_data):
                            if value and str(value).strip():
                                result_item["row_data"][f"Column_{i}"] = value
                        
                        all_results.append(result_item)
                        
                    except Exception as e:
                        logger.warning(f"Error processing date query result: {e}")
                        continue
            
            if all_results:
                # Group by tab
                tab_groups = {}
                for result_item in all_results:
                    tab_name_key = result_item["tab_name"]
                    if tab_name_key not in tab_groups:
                        tab_groups[tab_name_key] = []
                    tab_groups[tab_name_key].append(result_item)
                
                return {
                    "answer": f"Found {len(all_results)} entries for {date_text}",
                    "confidence": 0.9,
                    "data_count": len(all_results),
                    "supporting_data": all_results[:10],
                    "results": all_results,
                    "tab_groups": tab_groups
                }
            else:
                return {
                    "answer": f"No data found for {date_text}.",
                    "confidence": 0.3,
                    "results": [],
                    "suggestions": []
                }
                
        except Exception as e:
            logger.error(f"Error in date query: {str(e)}")
            # Fallback to regular search
            result = await self.data_service.search_data(date_text, sheet_id, tab_name)
            
            if result["success"] and result["results"]:
                return {
                    "answer": f"Found {result['total_matches']} results for {date_text}",
                    "confidence": 0.7,
                    "data_count": result["total_matches"],
                    "supporting_data": result["results"][:5],
                    "results": result["results"]
                }
            else:
                return {
                    "answer": f"No data found for {date_text}.",
                    "confidence": 0.3,
                    "results": [],
                    "suggestions": []
                }
    
    async def _handle_coordinate_query(self, analysis: Dict[str, Any], 
                                     sheet_id: str, tab_name: str) -> Dict[str, Any]:
        """Handle coordinate-based queries"""
        x = analysis["parameters"]["x"]
        y = analysis["parameters"]["y"]
        
        result = await self.data_service.get_data_by_coordinates(x, y, sheet_id, tab_name)
        
        if result["success"]:
            answer = f"Cell at coordinates ({x}, {y}): {result['value']}"
            
            if result["field_name"]:
                answer += f" (Field: {result['field_name']})"
            
            answer += f" from {result['tab_name']}"
            
            if result["context"]["is_header"]:
                answer += " (Header row)"
            
            return {
                "answer": answer,
                "confidence": 0.9,
                "data_count": 1,
                "supporting_data": [result]
            }
        else:
            return {
                "answer": f"No data found at coordinates ({x}, {y}).",
                "confidence": 0.3
            }
    
    async def _handle_summary_query(self, analysis: Dict[str, Any], 
                                  sheet_id: str, tab_name: str) -> Dict[str, Any]:
        """Handle summary/structure queries"""
        result = await self.data_service.get_sheet_summary(sheet_id)
        
        if result["success"]:
            summary = result["tabs_summary"]
            
            answer = f"Sheet Summary ({len(summary)} tabs):\n"
            
            for tab_name, tab_info in list(summary.items())[:5]:
                answer += f"\n• **{tab_name}**:\n"
                answer += f"  - {tab_info['field_count']} fields: {', '.join(tab_info['fields'][:5])}"
                if len(tab_info['fields']) > 5:
                    answer += f" (and {len(tab_info['fields']) - 5} more)"
                answer += f"\n  - {tab_info['dimensions']['rows']} rows"
                
                if tab_info.get('sample_queries'):
                    answer += f"\n  - Sample queries: {', '.join(tab_info['sample_queries'][:2])}"
            
            return {
                "answer": answer.strip(),
                "confidence": 0.9,
                "data_count": len(summary),
                "supporting_data": [summary]
            }
        else:
            return {
                "answer": "Could not analyze sheet structure.",
                "confidence": 0.3
            }
    
    async def _handle_general_query(self, analysis: Dict[str, Any], 
                                  sheet_id: str, tab_name: str) -> Dict[str, Any]:
        """Handle general queries with fallback search"""
        query_text = " ".join(analysis.get("parameters", {}).values())
        
        if not query_text:
            query_text = "data"  # Fallback
        
        # Use general search
        result = await self.data_service.search_data(query_text, sheet_id, tab_name, limit=20)
        
        if result["success"] and result["results"]:
            answer = f"Found {result['total_matches']} results. Here are the top matches:\n"
            
            for i, res in enumerate(result["results"][:5], 1):
                answer += f"{i}. {res['field_name']}: {res['value']} (from {res['tab_name']})\n"
            
            return {
                "answer": answer.strip(),
                "confidence": 0.6,
                "data_count": result["total_matches"],
                "supporting_data": result["results"][:3],
                "results": result["results"],  # Add raw results for AI processing
                "suggestions": [
                    "Try being more specific about what you're looking for",
                    "Use 'show me latest data' to see available fields"
                ]
            }
        else:
            return {
                "answer": "No specific data found for your query.",
                "confidence": 0.3,
                "results": [],
                "suggestions": []
            }


    async def _process_single_value_results(self, search_results: Dict[str, Any], 
                                          normalized_query) -> Dict[str, Any]:
        """Process results for single value queries"""
        results = search_results.get("results", [])
        
        if not results:
            return {
                "answer": f"No data found for your query.",
                "confidence": 0.3,
                "supporting_data": [],
                "suggestions": [
                    "Try a different field name or date",
                    "Check if the data exists in your sheets",
                    "Use 'show me latest data' to see available fields"
                ]
            }
        
        # For single value, return the best match
        best_result = results[0]
        
        return {
            "answer": f"Found: {best_result['value']}",
            "confidence": 0.9,
            "supporting_data": results[:3],
            "values": results,  # For AI processing
            "data_count": len(results)
        }
    
    async def _process_multiple_values_results(self, search_results: Dict[str, Any], 
                                             normalized_query) -> Dict[str, Any]:
        """Process results for multiple values queries"""
        results = search_results.get("results", [])
        
        if not results:
            return {
                "answer": f"No matching data found.",
                "confidence": 0.3,
                "supporting_data": [],
                "suggestions": [
                    "Try broader search terms",
                    "Check field names in your sheets",
                    "Use 'show me all data' for overview"
                ]
            }
        
        # Group results by field/sheet for better organization
        grouped_results = {}
        for result in results:
            key = f"{result['sheet_name']}_{result['field_name']}"
            if key not in grouped_results:
                grouped_results[key] = []
            grouped_results[key].append(result)
        
        return {
            "answer": f"Found {len(results)} matching values across {len(grouped_results)} fields",
            "confidence": 0.8,
            "supporting_data": results[:10],
            "results": results,  # For AI processing
            "grouped_results": grouped_results,
            "data_count": len(results)
        }
    
    async def _process_all_related_results(self, search_results: Dict[str, Any], 
                                         normalized_query) -> Dict[str, Any]:
        """Process results for all related data queries"""
        results = search_results.get("results", [])
        
        if not results:
            return {
                "answer": f"No related data found.",
                "confidence": 0.3,
                "supporting_data": [],
                "suggestions": [
                    "Try different keywords",
                    "Check what data is available in your sheets"
                ]
            }
        
        # Organize by sheets and fields
        sheet_summary = {}
        for result in results:
            sheet_name = result['sheet_name']
            if sheet_name not in sheet_summary:
                sheet_summary[sheet_name] = {
                    'fields': set(),
                    'sample_data': [],
                    'total_values': 0
                }
            
            sheet_summary[sheet_name]['fields'].add(result['field_name'])
            if len(sheet_summary[sheet_name]['sample_data']) < 5:
                sheet_summary[sheet_name]['sample_data'].append(result)
            sheet_summary[sheet_name]['total_values'] += 1
        
        return {
            "answer": f"Found related data across {len(sheet_summary)} sheets",
            "confidence": 0.7,
            "supporting_data": results[:15],
            "results": results,  # For AI processing
            "sheet_summary": sheet_summary,
            "data_count": len(results)
        }


# Global instance
_universal_query_processor = None

def get_universal_query_processor() -> UniversalQueryProcessor:
    """Get the global universal query processor instance"""
    global _universal_query_processor
    if _universal_query_processor is None:
        _universal_query_processor = UniversalQueryProcessor()
    return _universal_query_processor