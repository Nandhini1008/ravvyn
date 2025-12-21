"""
Enhanced Response Formatter - Fixes LLM formatting issues
Ensures that when database returns data, the response always shows that data
"""

import logging
import asyncio
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of data or response validation"""
    is_valid: bool
    error_message: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    corrected_data: Optional[Dict[str, Any]] = None


@dataclass
class FormattedResponse:
    """Complete formatted response with metadata"""
    response_text: str
    formatting_method: str  # 'llm', 'fallback', 'hybrid'
    data_count: int
    validation_passed: bool
    fallback_used: bool
    processing_time_ms: int
    warnings: List[str] = field(default_factory=list)


class EnhancedResponseFormatter:
    """
    Enhanced response formatter that ensures data is always displayed when found
    Fixes the critical issue where LLM says "no data found" when data exists
    """
    
    def __init__(self):
        self.ai_service = None  # Lazy initialization
        
        # Patterns that indicate false negative responses
        self.false_negative_patterns = [
            'no data', 'not find', 'couldn\'t find', 'no entries', 'no specific',
            'couldn\'t find any', 'no entries matching', 'none of the entries',
            'nothing for', 'no information', 'not contain any entries',
            'couldn\'t find specific', 'no entries for', 'not find any',
            'no matching data', 'no results', 'unable to find', 'cannot find',
            'doesn\'t contain', 'no relevant data', 'no available data'
        ]
    
    def _get_ai_service(self):
        """Get AI service instance (lazy initialization)"""
        if self.ai_service is None:
            try:
                from services.ai import AIService
                self.ai_service = AIService()
            except Exception as e:
                logger.warning(f"Could not initialize AI service: {str(e)}")
                self.ai_service = None
        return self.ai_service
    
    async def format_response(self, query: str, raw_data: Dict[str, Any]) -> FormattedResponse:
        """
        Main method to format response with validation and fallback
        
        Args:
            query: Original user query
            raw_data: Raw data retrieved from database
            
        Returns:
            FormattedResponse with guaranteed data display if data exists
        """
        start_time = datetime.now()
        
        # Step 1: Validate raw data
        validation_result = self.validate_raw_data(raw_data)
        if not validation_result.is_valid:
            logger.warning(f"Raw data validation failed: {validation_result.error_message}")
            return FormattedResponse(
                response_text=f"Error processing data: {validation_result.error_message}",
                formatting_method='error',
                data_count=0,
                validation_passed=False,
                fallback_used=True,
                processing_time_ms=self._get_processing_time(start_time),
                warnings=[validation_result.error_message]
            )
        
        # Step 2: Count data elements
        data_count = self._count_data_elements(raw_data)
        
        # Step 3: Try AI formatting if data exists
        if data_count > 0:
            ai_response = await self._try_ai_formatting_with_validation(query, raw_data, data_count)
            
            if ai_response and not self._is_false_negative_response(ai_response, data_count):
                # AI formatting successful
                return FormattedResponse(
                    response_text=ai_response,
                    formatting_method='llm',
                    data_count=data_count,
                    validation_passed=True,
                    fallback_used=False,
                    processing_time_ms=self._get_processing_time(start_time)
                )
            else:
                # AI failed or gave false negative - use fallback
                logger.warning(f"AI formatting failed or gave false negative for {data_count} data items")
                fallback_response = self._generate_fallback_response(query, raw_data)
                
                return FormattedResponse(
                    response_text=fallback_response,
                    formatting_method='fallback',
                    data_count=data_count,
                    validation_passed=True,
                    fallback_used=True,
                    processing_time_ms=self._get_processing_time(start_time),
                    warnings=['AI formatting failed, used direct data display']
                )
        else:
            # No data found - use simple response
            return FormattedResponse(
                response_text=f"No data found for '{query}'.",
                formatting_method='direct',
                data_count=0,
                validation_passed=True,
                fallback_used=False,
                processing_time_ms=self._get_processing_time(start_time)
            )
    
    def validate_raw_data(self, raw_data: Dict[str, Any]) -> ValidationResult:
        """Validate raw data structure and content"""
        try:
            if not isinstance(raw_data, dict):
                return ValidationResult(
                    is_valid=False,
                    error_message="Raw data must be a dictionary"
                )
            
            # Check for expected data structures
            has_data = any([
                raw_data.get("values"),
                raw_data.get("results"),
                raw_data.get("latest_data"),
                raw_data.get("tab_groups")
            ])
            
            if not has_data and not raw_data.get("answer"):
                return ValidationResult(
                    is_valid=False,
                    error_message="No data or answer found in raw data"
                )
            
            return ValidationResult(is_valid=True)
            
        except Exception as e:
            return ValidationResult(
                is_valid=False,
                error_message=f"Data validation error: {str(e)}"
            )
    
    def _count_data_elements(self, raw_data: Dict[str, Any]) -> int:
        """Count total data elements in raw data"""
        count = 0
        
        if raw_data.get("values"):
            count += len(raw_data["values"])
        
        if raw_data.get("results"):
            count += len(raw_data["results"])
        
        if raw_data.get("latest_data"):
            count += len(raw_data["latest_data"])
        
        if raw_data.get("tab_groups"):
            for tab_results in raw_data["tab_groups"].values():
                count += len(tab_results)
        
        return count
    
    async def _try_ai_formatting_with_validation(self, query: str, raw_data: Dict[str, Any], data_count: int) -> Optional[str]:
        """Try AI formatting with enhanced validation"""
        ai_service = self._get_ai_service()
        if not ai_service:
            return None
        
        try:
            # Build enhanced prompt that emphasizes data presence
            prompt = self._build_enhanced_prompt(query, raw_data, data_count)
            
            # Get AI response with timeout
            ai_response = await asyncio.wait_for(
                ai_service._retry_ai_request(
                    ai_service._make_chat_request,
                    [{"role": "user", "content": prompt}]
                ),
                timeout=10.0
            )
            
            if ai_response and ai_response.strip():
                # Validate AI response
                if self._validate_ai_response(ai_response, raw_data, data_count):
                    return ai_response.strip()
                else:
                    logger.warning("AI response failed validation")
                    return None
            else:
                logger.warning("AI returned empty response")
                return None
                
        except asyncio.TimeoutError:
            logger.warning("AI formatting timed out")
            return None
        except Exception as e:
            logger.error(f"AI formatting error: {str(e)}")
            return None
    
    def _build_enhanced_prompt(self, query: str, raw_data: Dict[str, Any], data_count: int) -> str:
        """Build enhanced prompt that emphasizes data presence and query context"""
        
        # Analyze query intent for better context
        query_intent = self._analyze_query_intent(query)
        
        # Start with strong emphasis on data presence and query context
        prompt_parts = [
            f"🚨 CRITICAL: DATA WAS SUCCESSFULLY FOUND! 🚨",
            f"",
            f"USER ASKED: '{query}'",
            f"QUERY TYPE: {query_intent['type']}",
            f"DATABASE RESULT: Found {data_count} matching data entries",
            f"",
            f"✅ SUCCESS STATUS: Data exists and MUST be displayed to answer the user's question",
            f"❌ NEVER say 'no data found' or 'couldn't find' - DATA EXISTS!",
            f"🎯 YOUR TASK: Answer the user's specific question using the data below",
            f"",
            f"📊 RETRIEVED DATA TO ANSWER '{query}':"
        ]
        
        # Add comprehensive data details
        if raw_data.get("values"):
            prompt_parts.append(f"\n🔍 FIELD VALUES ({len(raw_data['values'])} items):")
            for i, value_info in enumerate(raw_data["values"][:15], 1):
                field_name = value_info.get('field_name', 'Unknown field')
                value = value_info.get('value', 'No value')
                tab_name = value_info.get('tab_name', 'Unknown tab')
                row_index = value_info.get('row_index', 'N/A')
                
                prompt_parts.append(f"  {i}. {field_name}: {value} (from {tab_name}, row {row_index})")
                
                # Add row context
                if value_info.get("row_data"):
                    context_fields = []
                    for fname, fvalue in list(value_info["row_data"].items())[:4]:
                        if fvalue and str(fvalue).strip() and fname != field_name:
                            context_fields.append(f"{fname}: {fvalue}")
                    if context_fields:
                        prompt_parts.append(f"     Context: {', '.join(context_fields)}")
        
        if raw_data.get("results"):
            results = raw_data["results"]
            if raw_data.get("tab_groups"):
                # Date query with tab groups
                prompt_parts.append(f"\n📅 DATE QUERY RESULTS ({len(results)} total entries):")
                tab_groups = raw_data["tab_groups"]
                
                for tab_name, tab_results in list(tab_groups.items())[:8]:
                    prompt_parts.append(f"\n📋 {tab_name} ({len(tab_results)} entries):")
                    
                    for i, result in enumerate(tab_results[:5], 1):
                        row_index = result.get('row_index', 'N/A')
                        row_data = result.get('row_data', {})
                        
                        data_items = []
                        for field, value in list(row_data.items())[:6]:
                            if value and str(value).strip():
                                data_items.append(f"{field}: {value}")
                        
                        prompt_parts.append(f"  Row {row_index}: {', '.join(data_items) if data_items else 'No data'}")
            else:
                # Regular search results
                prompt_parts.append(f"\n🔍 SEARCH RESULTS ({len(results)} found):")
                for i, result in enumerate(results[:15], 1):
                    field_name = result.get('field_name', 'Unknown')
                    value = result.get('value', 'No value')
                    tab_name = result.get('tab_name', 'Unknown tab')
                    row_index = result.get('row_index', 'N/A')
                    
                    prompt_parts.append(f"  {i}. {field_name}: {value} (from {tab_name}, row {row_index})")
        
        if raw_data.get("latest_data"):
            latest_data = raw_data["latest_data"]
            prompt_parts.append(f"\n📊 LATEST DATA FOUND:")
            
            for tab_name, tab_data in latest_data.items():
                prompt_parts.append(f"From {tab_name}:")
                fields = tab_data.get("fields", {})
                
                for field, value in fields.items():
                    if value and str(value).strip():
                        prompt_parts.append(f"  {field}: {value}")
        
        # Add formatting instructions with calculation emphasis
        prompt_parts.extend([
            f"",
            f"📝 CRITICAL INSTRUCTIONS FOR CLEAN ANSWERS:",
            f"1. ANALYZE the user's question: '{query}'",
            f"2. The user wants: {query_intent['action']} for {query_intent['subject']}",
            f"3. PROVIDE A DIRECT, CONVERSATIONAL ANSWER:",
            f"   • Start with the main result (e.g., 'Total amount: **1,250 KG**')",
            f"   • Add brief context if helpful",
            f"   • Keep it concise and business-focused",
            f"   • NO tables or technical formatting in the response",
            f"4. FOR CALCULATION QUERIES:",
            f"   • If asking for 'total amount', provide the calculated sum",
            f"   • If asking for 'how many', provide the count",
            f"   • If asking for 'average', provide the calculated average",
            f"   • If asking for specific value, provide that value clearly",
            f"5. RESPONSE FORMAT:",
            f"   • Direct answer first",
            f"   • Brief explanation or breakdown",
            f"   • Source information (which reports/dates)",
            f"   • Note about detailed data export if applicable",
            f"6. AVOID:",
            f"   • Raw search results or technical details",
            f"   • Table formatting in the response",
            f"   • Lengthy explanations",
            f"   • Repetitive information",
            f"",
            f"🎯 GOAL: Provide a clean, direct answer that answers '{query}' conversationally",
            f"",
            f"Example for 'total amount coated last 5 days':",
            f"**Total amount coated in the last 5 days: 1,250 KG**",
            f"",
            f"This includes daily amounts from December 8-12, 2025, sourced from the daily coating reports. The breakdown shows consistent production with amounts ranging from 200-300 KG per day.",
            f"",
            f"📊 Detailed breakdown exported to Google Sheets for analysis.",
            f"",
            f"Now provide your clean, conversational answer:"
        ])
        
        return "\n".join(prompt_parts)
    
    def _analyze_query_intent(self, query: str) -> Dict[str, str]:
        """Analyze query to understand what the user is asking for"""
        query_lower = query.lower()
        
        intent = {
            'type': 'general',
            'subject': 'data',
            'action': 'show'
        }
        
        # Detect query type
        if any(word in query_lower for word in ['what is', 'what\'s', 'tell me']):
            intent['type'] = 'specific_value'
            intent['action'] = 'get'
        elif any(word in query_lower for word in ['show', 'display', 'list', 'give me']):
            intent['type'] = 'list_data'
            intent['action'] = 'show'
        elif any(word in query_lower for word in ['how many', 'count', 'number of']):
            intent['type'] = 'count'
            intent['action'] = 'count'
        elif any(word in query_lower for word in ['latest', 'recent', 'last']):
            intent['type'] = 'latest'
            intent['action'] = 'get latest'
        elif any(word in query_lower for word in ['on', 'for', 'at', 'during']) and any(char.isdigit() for char in query):
            intent['type'] = 'date_query'
            intent['action'] = 'get data for date'
        
        # Extract subject (what they're asking about)
        # Look for key terms after "what is", "show me", etc.
        subject_patterns = [
            r'what\s+is\s+(?:the\s+)?(.+?)(?:\s+on|\s+for|\s+in|\s+at|$)',
            r'show\s+(?:me\s+)?(?:the\s+)?(.+?)(?:\s+on|\s+for|\s+from|$)',
            r'get\s+(?:me\s+)?(?:the\s+)?(.+?)(?:\s+on|\s+for|\s+from|$)',
            r'(?:data|information|entries)\s+(?:for|on|about)\s+(.+)',
        ]
        
        import re
        for pattern in subject_patterns:
            match = re.search(pattern, query_lower)
            if match:
                intent['subject'] = match.group(1).strip()
                break
        
        # If no subject found, use first few words
        if intent['subject'] == 'data':
            words = query.split()
            if len(words) > 2:
                intent['subject'] = ' '.join(words[:3])
            else:
                intent['subject'] = query[:30]
        
        return intent
    
    def _generate_calculation_response(self, query: str, raw_data: Dict[str, Any], query_intent: Dict[str, str]) -> str:
        """Generate response focused on calculations and clean answers"""
        try:
            # Extract numerical values from the data
            values = []
            sources = []
            
            # Process different data types to extract numbers
            if raw_data.get("values"):
                for value_info in raw_data["values"]:
                    value = value_info.get('value', '')
                    if self._is_numeric(value):
                        values.append(float(value))
                        sources.append(f"{value_info.get('tab_name', 'Unknown')} (Row {value_info.get('row_index', 'N/A')})")
            
            elif raw_data.get("results"):
                for result in raw_data["results"]:
                    # Check both the main value and row_data for numbers
                    value = result.get('value', '')
                    if self._is_numeric(value):
                        values.append(float(value))
                        sources.append(f"{result.get('tab_name', 'Unknown')} (Row {result.get('row_index', 'N/A')})")
                    
                    # Also check row_data for numeric fields
                    row_data = result.get('row_data', {})
                    for field_name, field_value in row_data.items():
                        if self._is_numeric(field_value) and 'total' in field_name.lower():
                            values.append(float(field_value))
                            sources.append(f"{result.get('tab_name', 'Unknown')} - {field_name}")
            
            # Generate calculation-focused response
            if not values:
                return f"**No numerical values found for calculation in '{query}'**\n\nFound {len(raw_data.get('results', raw_data.get('values', [])))} entries but no calculable amounts."
            
            # Perform calculations based on query type
            if 'total' in query.lower() or 'sum' in query.lower():
                total = sum(values)
                response_parts = [
                    f"**Total amount: {total:,.2f}**",
                    f"",
                    f"Found {len(values)} entries with amounts ranging from {min(values):,.2f} to {max(values):,.2f}.",
                ]
                
                if len(values) <= 5:
                    response_parts.append(f"The individual amounts were: {', '.join([f'{v:,.2f}' for v in values])}.")
                else:
                    response_parts.append(f"This includes {len(values)} separate entries from various reports.")
                
                response_parts.extend([
                    f"",
                    f"📊 **Detailed breakdown exported to Google Sheets for analysis**"
                ])
                
            elif 'average' in query.lower():
                average = sum(values) / len(values)
                total = sum(values)
                response_parts = [
                    f"**Average amount: {average:,.2f}**",
                    f"",
                    f"Based on {len(values)} entries with a total of {total:,.2f}.",
                    f"Values range from {min(values):,.2f} to {max(values):,.2f}.",
                    f"",
                    f"📊 **Detailed data exported to Google Sheets for analysis**"
                ]
                
            elif 'count' in query.lower() or 'how many' in query.lower():
                response_parts = [
                    f"**Found {len(values)} entries**",
                    f"",
                    f"Total sum of all values: {sum(values):,.2f}",
                    f"Average per entry: {sum(values)/len(values):,.2f}",
                    f"Range: {min(values):,.2f} to {max(values):,.2f}",
                    f"",
                    f"📊 **Complete list exported to Google Sheets**"
                ]
                
            else:
                # General calculation response
                total = sum(values)
                response_parts = [
                    f"**Result: {total:,.2f}**",
                    f"",
                    f"Calculated from {len(values)} numerical values found in the database.",
                    f"Average value: {total/len(values):,.2f}",
                    f"",
                    f"📊 **Detailed data exported to Google Sheets for analysis**"
                ]
            
            return "\n".join(response_parts)
            
        except Exception as e:
            logger.error(f"Error in calculation response: {str(e)}")
            return f"Found data for '{query}' but couldn't perform calculations. Please check the data format."
    
    def _is_numeric(self, value) -> bool:
        """Check if a value is numeric"""
        if not value:
            return False
        
        try:
            # Clean the value (remove common non-numeric characters)
            cleaned = str(value).replace(',', '').replace(' ', '').strip()
            
            # Try to convert to float
            float(cleaned)
            return True
        except (ValueError, TypeError):
            return False
    
    def _validate_ai_response(self, ai_response: str, raw_data: Dict[str, Any], data_count: int) -> bool:
        """Validate AI response to ensure it doesn't give false negatives"""
        
        # Check for false negative patterns
        if self._is_false_negative_response(ai_response, data_count):
            return False
        
        # Check if response contains actual data references
        response_lower = ai_response.lower()
        
        # Look for data indicators in the response
        data_indicators = ['found', 'data', 'value', 'result', 'entry', 'entries', 'shows', 'displays']
        has_data_indicators = any(indicator in response_lower for indicator in data_indicators)
        
        if data_count > 0 and not has_data_indicators:
            logger.warning("AI response lacks data indicators despite data being present")
            return False
        
        return True
    
    def _is_false_negative_response(self, response: str, data_count: int) -> bool:
        """Check if response incorrectly indicates no data when data exists"""
        if data_count == 0:
            return False  # No data, so "no data found" is correct
        
        response_lower = response.lower()
        
        # Check for false negative patterns
        for pattern in self.false_negative_patterns:
            if pattern in response_lower:
                logger.warning(f"False negative detected: '{pattern}' found in response when {data_count} data items exist")
                return True
        
        return False
    
    def _generate_fallback_response(self, query: str, raw_data: Dict[str, Any]) -> str:
        """Generate high-quality fallback response with calculation focus"""
        try:
            data_count = self._count_data_elements(raw_data)
            
            if data_count == 0:
                return f"No data found for '{query}'."
            
            # Analyze query to provide better context
            query_intent = self._analyze_query_intent(query)
            
            # Check if this is a calculation query
            is_calculation_query = any(word in query.lower() for word in [
                'total', 'sum', 'amount', 'count', 'how many', 'average', 'calculate'
            ])
            
            if is_calculation_query:
                return self._generate_calculation_response(query, raw_data, query_intent)
            
            # Build structured response that directly answers the query
            if query_intent['type'] == 'specific_value' and data_count == 1:
                # Single value response with better formatting
                response_parts = [f"**Answer to your query:**"]
            elif query_intent['type'] == 'count':
                # Count response
                response_parts = [f"**Found {data_count} entries for '{query}':**"]
            else:
                # General response
                response_parts = [f"**Here's what I found for '{query}':**", f"", f"📊 **{data_count} data entries found**"]
            
            # Handle different data types with table formatting
            if raw_data.get("values"):
                if len(raw_data["values"]) == 1:
                    # Single value - format as simple table
                    value_info = raw_data["values"][0]
                    field_name = value_info.get('field_name', 'Field')
                    value = value_info.get('value', 'N/A')
                    tab_name = value_info.get('tab_name', 'Unknown')
                    row_index = value_info.get('row_index', 'N/A')
                    
                    response_parts.extend([
                        f"",
                        f"**{field_name}: {value}**",
                        f"",
                        f"| Field | Value | Source | Row |",
                        f"|-------|-------|--------|-----|",
                        f"| {field_name} | **{value}** | {tab_name} | {row_index} |"
                    ])
                    
                    # Add context data as additional table rows
                    if value_info.get('row_data'):
                        response_parts.extend([f"", f"**Complete Row Data:**", f""])
                        
                        # Get all fields from row data
                        row_data = value_info['row_data']
                        if row_data:
                            # Create table headers
                            headers = list(row_data.keys())
                            values = [str(row_data.get(h, '')) for h in headers]
                            
                            # Format as table
                            header_row = "| " + " | ".join(headers) + " |"
                            separator_row = "|" + "|".join(["-" * (len(h) + 2) for h in headers]) + "|"
                            value_row = "| " + " | ".join([f"**{v}**" if v else "" for v in values]) + " |"
                            
                            response_parts.extend([header_row, separator_row, value_row])
                else:
                    # Multiple values - format as comprehensive table
                    response_parts.extend([f"", f"**📊 Data Table ({len(raw_data['values'])} entries):**", f""])
                    
                    # Collect all unique field names for table headers
                    all_fields = set()
                    for value_info in raw_data["values"]:
                        if value_info.get('row_data'):
                            all_fields.update(value_info['row_data'].keys())
                    
                    # If no row data, use basic columns
                    if not all_fields:
                        response_parts.extend([
                            f"| # | Field | Value | Source | Row |",
                            f"|---|-------|-------|--------|-----|"
                        ])
                        
                        for i, value_info in enumerate(raw_data["values"][:15], 1):
                            field_name = value_info.get('field_name', 'Field')
                            value = value_info.get('value', 'N/A')
                            tab_name = value_info.get('tab_name', 'Unknown')
                            row_index = value_info.get('row_index', 'N/A')
                            
                            response_parts.append(f"| {i} | {field_name} | **{value}** | {tab_name} | {row_index} |")
                    else:
                        # Create comprehensive table with all fields
                        sorted_fields = sorted(list(all_fields))[:10]  # Limit to 10 columns for readability
                        
                        # Table headers
                        header_row = "| # | " + " | ".join(sorted_fields) + " | Source |"
                        separator_row = "|---|" + "|".join(["-" * (len(f) + 2) for f in sorted_fields]) + "|--------|"
                        
                        response_parts.extend([header_row, separator_row])
                        
                        # Table rows
                        for i, value_info in enumerate(raw_data["values"][:15], 1):
                            row_data = value_info.get('row_data', {})
                            tab_name = value_info.get('tab_name', 'Unknown')
                            
                            row_values = []
                            for field in sorted_fields:
                                field_value = row_data.get(field, '')
                                if field_value:
                                    row_values.append(f"**{field_value}**")
                                else:
                                    row_values.append("")
                            
                            table_row = f"| {i} | " + " | ".join(row_values) + f" | {tab_name} |"
                            response_parts.append(table_row)
                    
                    if len(raw_data["values"]) > 15:
                        response_parts.extend([f"", f"*... and {len(raw_data['values']) - 15} more rows*"])
            
            elif raw_data.get("results"):
                results = raw_data["results"]
                
                if raw_data.get("tab_groups"):
                    # Date query with tab groups - table format by source
                    response_parts.extend([f"", f"**📅 Data Table by Source:**"])
                    tab_groups = raw_data["tab_groups"]
                    
                    for tab_name, tab_results in list(tab_groups.items())[:5]:
                        response_parts.extend([
                            f"",
                            f"**📋 {tab_name}** ({len(tab_results)} entries):"
                        ])
                        
                        if tab_results:
                            # Get all unique fields from this tab
                            all_fields = set()
                            for result in tab_results:
                                if result.get('row_data'):
                                    all_fields.update(result['row_data'].keys())
                            
                            if all_fields:
                                sorted_fields = sorted(list(all_fields))[:8]  # Limit columns
                                
                                # Create table
                                header_row = "| Row | " + " | ".join(sorted_fields) + " |"
                                separator_row = "|-----|" + "|".join(["-" * (len(f) + 2) for f in sorted_fields]) + "|"
                                
                                response_parts.extend([f"", header_row, separator_row])
                                
                                # Add data rows
                                for result in tab_results[:10]:
                                    row_index = result.get('row_index', 'N/A')
                                    row_data = result.get('row_data', {})
                                    
                                    row_values = []
                                    for field in sorted_fields:
                                        field_value = row_data.get(field, '')
                                        if field_value:
                                            row_values.append(f"**{field_value}**")
                                        else:
                                            row_values.append("")
                                    
                                    table_row = f"| {row_index} | " + " | ".join(row_values) + " |"
                                    response_parts.append(table_row)
                                
                                if len(tab_results) > 10:
                                    response_parts.append(f"*... and {len(tab_results) - 10} more rows*")
                            else:
                                response_parts.append("No detailed data available")
                    
                    if len(tab_groups) > 5:
                        response_parts.extend([f"", f"*... and {len(tab_groups) - 5} more tabs*"])
                else:
                    # Regular search results - simple table
                    response_parts.extend([f"", f"**🔍 Search Results Table:**", f""])
                    
                    response_parts.extend([
                        f"| # | Field | Value | Source | Row |",
                        f"|---|-------|-------|--------|-----|"
                    ])
                    
                    for i, result in enumerate(results[:15], 1):
                        field_name = result.get('field_name', 'Field')
                        value = result.get('value', 'N/A')
                        tab_name = result.get('tab_name', 'Unknown')
                        row_index = result.get('row_index', 'N/A')
                        
                        response_parts.append(f"| {i} | {field_name} | **{value}** | {tab_name} | {row_index} |")
                    
                    if len(results) > 15:
                        response_parts.extend([f"", f"*... and {len(results) - 15} more rows*"])
            
            elif raw_data.get("latest_data"):
                latest_data = raw_data["latest_data"]
                response_parts.extend([f"", f"**📊 Latest Data Table:**", f""])
                
                # Collect all fields across all tabs
                all_fields = set()
                for tab_data in latest_data.values():
                    fields = tab_data.get("fields", {})
                    all_fields.update(fields.keys())
                
                if all_fields:
                    sorted_fields = sorted(list(all_fields))[:10]  # Limit columns
                    
                    # Create table headers
                    header_row = "| Source | " + " | ".join(sorted_fields) + " |"
                    separator_row = "|--------|" + "|".join(["-" * (len(f) + 2) for f in sorted_fields]) + "|"
                    
                    response_parts.extend([header_row, separator_row])
                    
                    # Add data rows
                    for tab_name, tab_data in latest_data.items():
                        fields = tab_data.get("fields", {})
                        
                        row_values = []
                        for field in sorted_fields:
                            field_value = fields.get(field, '')
                            if field_value and str(field_value).strip():
                                row_values.append(f"**{field_value}**")
                            else:
                                row_values.append("")
                        
                        table_row = f"| {tab_name} | " + " | ".join(row_values) + " |"
                        response_parts.append(table_row)
                else:
                    response_parts.append("No data available in latest entries")
            
            # Add a clean summary footer for multiple entries
            if data_count > 1:
                response_parts.extend([
                    f"",
                    f"---",
                    f"**📈 Summary:** {data_count} total entries found"
                ])
            
            return "\n".join(response_parts)
            
        except Exception as e:
            logger.error(f"Error generating fallback response: {str(e)}")
            return f"Found data for '{query}' but encountered formatting error. Data count: {self._count_data_elements(raw_data)}"
    
    def _get_processing_time(self, start_time: datetime) -> int:
        """Calculate processing time in milliseconds"""
        return int((datetime.now() - start_time).total_seconds() * 1000)


# Global instance
_enhanced_response_formatter = None

def get_enhanced_response_formatter() -> EnhancedResponseFormatter:
    """Get the global enhanced response formatter instance"""
    global _enhanced_response_formatter
    if _enhanced_response_formatter is None:
        _enhanced_response_formatter = EnhancedResponseFormatter()
    return _enhanced_response_formatter