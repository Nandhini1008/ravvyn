"""
Universal Query Normalizer - Converts any query to standardized format for data retrieval
Works with any sheet structure dynamically without hardcoded values
"""

import re
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, date, timedelta
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class NormalizedQuery:
    """Standardized query structure"""
    query_type: str  # 'specific_value', 'general_search', 'latest_data', 'summary'
    field_patterns: List[str]  # Field name patterns to search for
    criteria: Dict[str, Any]  # Search criteria (dates, values, etc.)
    scope: str  # 'single_value', 'multiple_values', 'all_related'
    original_query: str
    confidence: float


class UniversalQueryNormalizer:
    """
    Universal query normalizer that works with any sheet structure
    Converts natural language to standardized search parameters
    """
    
    def __init__(self):
        """Initialize the normalizer with universal patterns"""
        
        # Universal field semantic categories (expandable for any domain)
        self.semantic_categories = {
            'quantity': ['amount', 'total', 'sum', 'count', 'quantity', 'number', 'value'],
            'level': ['level', 'height', 'depth', 'percentage', '%', 'rate'],
            'time': ['time', 'hour', 'minute', 'timestamp', 'when', 'at'],
            'date': ['date', 'day', 'month', 'year', 'today', 'yesterday', 'week'],
            'temperature': ['temp', 'temperature', 'heat', 'thermal', 'degree'],
            'pressure': ['pressure', 'press', 'force', 'psi', 'bar', 'pascal'],
            'flow': ['flow', 'rate', 'speed', 'velocity', 'throughput'],
            'cost': ['cost', 'price', 'expense', 'money', 'rupees', '₹', '$'],
            'status': ['status', 'state', 'condition', 'active', 'running', 'on', 'off'],
            'location': ['tank', 'container', 'vessel', 'reservoir', 'plant', 'unit'],
            'process': ['feed', 'supply', 'input', 'output', 'source', 'destination'],
            'quality': ['quality', 'grade', 'standard', 'specification', 'purity'],
            'maintenance': ['maintenance', 'repair', 'service', 'check', 'inspection'],
            'operation': ['operation', 'running', 'working', 'functioning', 'performance']
        }
        
        # Universal query patterns (not domain-specific)
        self.query_patterns = {
            'specific_value': [
                r'what\s+is\s+(?:the\s+)?(.+?)\s+(?:on|for|in|at|of)\s+(.+)',
                r'(?:show|get|find)\s+(?:me\s+)?(.+?)\s+(?:on|for|from|in)\s+(.+)',
                r'(.+?)\s+(?:on|for|in|at|of)\s+(.+)',
                r'value\s+of\s+(.+?)\s+(?:on|for|in)\s+(.+)',
            ],
            'general_search': [
                r'(?:show|get|find|search)\s+(?:me\s+)?(?:all\s+)?(.+?)(?:\s+data|\s+values?|\s+information)?$',
                r'what\s+(?:are\s+)?(?:the\s+)?(.+?)(?:\s+values?|\s+data)?$',
                r'list\s+(?:all\s+)?(.+)',
                r'tell\s+me\s+about\s+(.+)',
                r'(.+?)\s+(?:data|information|details)$',
            ],
            'latest_data': [
                r'(?:show|get|find)\s+(?:me\s+)?(?:the\s+)?latest\s+(.+)',
                r'(?:what\s+is\s+)?(?:the\s+)?(?:most\s+)?recent\s+(.+)',
                r'current\s+(.+)',
                r'today\'?s\s+(.+)',
                r'latest\s+(.+)',
            ],
            'summary': [
                r'(?:show|get|describe|summarize)\s+(?:me\s+)?(?:the\s+)?(?:sheet|data|everything|all)',
                r'what\s+(?:fields|columns|data)\s+(?:are\s+)?(?:available|present)',
                r'analyze\s+(?:this\s+)?(?:sheet|data)',
                r'overview\s+of\s+(.+)',
                r'summary\s+of\s+(.+)',
            ]
        }
        
        # Enhanced date/time patterns for extraction
        # Prioritize dd.mm.yyyy format (day.month.year)
        self.date_patterns = [
            r'\b(\d{1,2}\.\d{1,2}\.\d{2,4})\b',  # PRIMARY: dd.mm.yyyy or dd.mm.yy (e.g., 25.10.2025, 25.10.25)
            r'\b(\d{1,2}[./\-]\d{1,2}[./\-]\d{2,4})\b',  # Alternative formats: 1.9.25, 01/09/2025, 12-12-2025
            r'\b(\d{4}[./\-]\d{1,2}[./\-]\d{1,2})\b',    # ISO format: 2025.09.01, 2025-12-12
            r'\b(\d{1,2})\s*[./\-]\s*(\d{1,2})\s*[./\-]\s*(\d{2,4})\b',  # Flexible spacing
            r'\b(today|yesterday|tomorrow)\b',
            r'\b(this|last|next)\s+(week|month|year)\b',
            r'\b(\d{1,2})\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s+(\d{2,4})\b',
            r'\b(december|november|october|september|august|july|june|may|april|march|february|january)\s+(\d{1,2}),?\s+(\d{2,4})\b',
        ]
        
        # Relative date patterns for range queries
        self.relative_date_patterns = [
            r'\blast\s+(\d+)\s+days?\b',      # last 7 days, last 30 days
            r'\blast\s+(\d+)\s+weeks?\b',     # last 2 weeks
            r'\blast\s+(\d+)\s+months?\b',    # last 3 months
            r'\blast\s+week\b',               # last week
            r'\blast\s+month\b',              # last month
            r'\bpast\s+(\d+)\s+days?\b',      # past 7 days
            r'\brecent\s+(\d+)\s+days?\b',    # recent 7 days
        ]
        
        # Time patterns
        self.time_patterns = [
            r'\b(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(am|pm)?\b',
            r'\b(\d{1,2})\s*(am|pm)\b',
        ]
        
        # Number patterns
        self.number_patterns = [
            r'\b(\d+(?:\.\d+)?)\s*(%|percent)\b',
            r'\b(\d+(?:,\d{3})*(?:\.\d+)?)\b',
            r'[₹$€£¥]\s*(\d+(?:,\d{3})*(?:\.\d+)?)',
        ]
    
    def normalize_query(self, query: str) -> NormalizedQuery:
        """
        PURE PATTERN MATCHING - NO AI/LLM INVOLVED
        Normalize any query using regex patterns and semantic categories
        
        Args:
            query: Natural language query
            
        Returns:
            NormalizedQuery object with standardized parameters (created by pattern matching)
        """
        query_lower = query.lower().strip()
        logger.info(f"🔄 Normalizing query: {query}")
        
        # Step 1: Determine query type and extract basic components
        query_type, extracted_components = self._classify_query(query_lower)
        
        # Step 2: Extract field patterns using semantic analysis
        field_patterns = self._extract_field_patterns(query_lower, extracted_components)
        
        # Step 3: Extract criteria (dates, numbers, conditions)
        criteria = self._extract_criteria(query_lower)
        
        # Step 4: Determine scope based on query type and patterns
        scope = self._determine_scope(query_type, field_patterns, criteria)
        
        # Step 5: Calculate confidence based on pattern matches
        confidence = self._calculate_confidence(query_type, field_patterns, criteria)
        
        normalized = NormalizedQuery(
            query_type=query_type,
            field_patterns=field_patterns,
            criteria=criteria,
            scope=scope,
            original_query=query,
            confidence=confidence
        )
        
        logger.info(f"✅ Normalized query: type={query_type}, patterns={field_patterns[:3]}, scope={scope}")
        return normalized
    
    def _classify_query(self, query: str) -> Tuple[str, Dict[str, Any]]:
        """Classify query type and extract basic components"""
        
        for query_type, patterns in self.query_patterns.items():
            for pattern in patterns:
                match = re.search(pattern, query, re.IGNORECASE)
                if match:
                    components = {
                        'matched_pattern': pattern,
                        'groups': match.groups() if match.groups() else [],
                        'full_match': match.group(0)
                    }
                    return query_type, components
        
        # Default to general search if no pattern matches
        return 'general_search', {'groups': [query], 'matched_pattern': 'fallback'}
    
    def _extract_field_patterns(self, query: str, components: Dict[str, Any]) -> List[str]:
        """Extract field patterns using semantic analysis"""
        field_patterns = []
        
        # Start with extracted groups from pattern matching
        for group in components.get('groups', []):
            if group and isinstance(group, str):
                field_patterns.append(group.strip())
        
        # Add semantic field patterns
        for category, keywords in self.semantic_categories.items():
            for keyword in keywords:
                if keyword in query:
                    field_patterns.append(keyword)
                    field_patterns.append(category)  # Add category as pattern too
        
        # Extract potential field names (words that might be field names)
        # Look for technical terms, abbreviations, compound words
        technical_patterns = [
            r'\b[a-z]+\d+(?:&\d+)?\b',  # ro1&2, tank1, unit2
            r'\b[a-z]+_[a-z]+\b',       # field_name, data_value
            r'\b[a-z]+\s+[a-z]+\s+[a-z]+\b',  # three word combinations
        ]
        
        for pattern in technical_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            field_patterns.extend(matches)
        
        # Remove duplicates and empty strings
        field_patterns = list(set([p for p in field_patterns if p and p.strip()]))
        
        return field_patterns
    
    def _extract_criteria(self, query: str) -> Dict[str, Any]:
        """Extract search criteria from query"""
        criteria = {}
        
        # Extract dates (both specific and relative)
        dates = []
        
        # First check for relative date patterns
        relative_dates = self._extract_relative_dates(query)
        if relative_dates:
            dates.extend(relative_dates)
            criteria['is_date_range'] = True
            criteria['date_range_type'] = 'relative'
        
        # Then check for specific date patterns
        # Prioritize dd.mm.yyyy format extraction
        for pattern in self.date_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            if matches:
                if isinstance(matches[0], tuple):
                    # For tuple matches, reconstruct date in dd.mm.yyyy format
                    for match in matches:
                        if len(match) == 3:
                            day, month, year = match
                            # Normalize to dd.mm.yyyy format
                            day = day.zfill(2)
                            month = month.zfill(2)
                            normalized_date = f"{day}.{month}.{year}"
                            dates.append(normalized_date)
                        else:
                            dates.append(' '.join(match))
                else:
                    # For string matches, normalize to dd.mm.yyyy if possible
                    for match in matches:
                        # Check if it's already in dd.mm.yyyy format
                        if re.match(r'\d{1,2}\.\d{1,2}\.\d{2,4}', match):
                            dates.append(match)
                        else:
                            # Try to normalize other formats to dd.mm.yyyy
                            date_match = re.match(r'(\d{1,2})[./\-](\d{1,2})[./\-](\d{2,4})', match)
                            if date_match:
                                day, month, year = date_match.groups()
                                day = day.zfill(2)
                                month = month.zfill(2)
                                normalized_date = f"{day}.{month}.{year}"
                                dates.append(normalized_date)
                            else:
                                dates.append(match)
        
        if dates:
            # Remove duplicates while preserving order (dd.mm.yyyy format first)
            unique_dates = []
            seen = set()
            for date_val in dates:
                if date_val.lower() not in seen:
                    seen.add(date_val.lower())
                    unique_dates.append(date_val)
            criteria['dates'] = unique_dates
            criteria['primary_date'] = unique_dates[0]  # First date (prioritized format)
        
        # Extract times
        times = []
        for pattern in self.time_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            times.extend(matches)
        
        if times:
            criteria['times'] = times
        
        # Extract numbers
        numbers = []
        for pattern in self.number_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            if matches:
                if isinstance(matches[0], tuple):
                    numbers.extend([match[0] for match in matches if match[0]])
                else:
                    numbers.extend(matches)
        
        if numbers:
            criteria['numbers'] = numbers
        
        # Extract comparison operators
        comparisons = re.findall(r'\b(greater|less|more|above|below|equal|than|over|under)\b', query)
        if comparisons:
            criteria['comparisons'] = comparisons
        
        # Extract range indicators
        ranges = re.findall(r'\b(between|from|to|range|span)\b', query)
        if ranges:
            criteria['ranges'] = ranges
        
        # Extract status/condition words
        conditions = re.findall(r'\b(active|inactive|running|stopped|on|off|high|low|normal|abnormal)\b', query)
        if conditions:
            criteria['conditions'] = conditions
        
        return criteria
    
    def _extract_relative_dates(self, query: str) -> List[str]:
        """Extract and convert relative date expressions to actual date ranges"""
        relative_dates = []
        current_date = datetime.now().date()
        
        logger.info(f"🔍 Extracting relative dates from: '{query}'")
        logger.info(f"📅 Current date: {current_date}")
        
        # Check each relative date pattern
        for pattern in self.relative_date_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                logger.info(f"✅ Found relative date pattern: {pattern} -> {match.group(0)}")
                
                if 'last' in match.group(0).lower() and len(match.groups()) > 0:
                    # Extract number of days/weeks/months
                    num = int(match.group(1))
                    
                    if 'days' in match.group(0).lower():
                        # Calculate date range for last N days
                        start_date = current_date - timedelta(days=num)
                        end_date = current_date
                        
                        logger.info(f"📅 Last {num} days: {start_date} to {end_date}")
                        
                        # Generate all dates in the range
                        # Prioritize dd.mm.yyyy format for all dates
                        date_range = []
                        current = start_date
                        while current <= end_date:
                            # Add multiple date formats - dd.mm.yyyy format first (prioritized)
                            date_range.extend([
                                current.strftime('%d.%m.%Y'),    # PRIMARY: 16.12.2025 (dd.mm.yyyy)
                                current.strftime('%d.%m.%y'),     # 16.12.25 (dd.mm.yy)
                                current.strftime('%d/%m/%Y'),     # 16/12/2025 (alternative)
                                current.strftime('%d/%m/%y'),     # 16/12/25
                                current.strftime('%d-%m-%Y'),     # 16-12-2025 (alternative)
                                current.strftime('%d-%m-%y'),     # 16-12-25
                                current.strftime('%Y-%m-%d'),     # 2025-12-16 (ISO format for compatibility)
                            ])
                            current += timedelta(days=1)
                        
                        relative_dates.extend(date_range)
                        logger.info(f"📅 Generated {len(date_range)} date variations for last {num} days")
                        
                    elif 'weeks' in match.group(0).lower():
                        # Calculate date range for last N weeks
                        start_date = current_date - timedelta(weeks=num)
                        end_date = current_date
                        
                        # Generate date range
                        date_range = []
                        current = start_date
                        while current <= end_date:
                            date_range.extend([
                                current.strftime('%d.%m.%Y'),
                                current.strftime('%d.%m.%y'),
                                current.strftime('%d/%m/%Y'),
                                current.strftime('%d/%m/%y'),
                            ])
                            current += timedelta(days=1)
                        
                        relative_dates.extend(date_range)
                        logger.info(f"📅 Generated date range for last {num} weeks")
                        
                elif 'last week' in match.group(0).lower():
                    # Last week (7 days)
                    start_date = current_date - timedelta(days=7)
                    end_date = current_date
                    
                    date_range = []
                    current = start_date
                    while current <= end_date:
                        date_range.extend([
                            current.strftime('%d.%m.%Y'),
                            current.strftime('%d.%m.%y'),
                            current.strftime('%d/%m/%Y'),
                            current.strftime('%d/%m/%y'),
                        ])
                        current += timedelta(days=1)
                    
                    relative_dates.extend(date_range)
                    logger.info(f"📅 Generated date range for last week")
                
                elif 'last month' in match.group(0).lower():
                    # Last month
                    first_day_current_month = current_date.replace(day=1)
                    last_day_previous_month = first_day_current_month - timedelta(days=1)
                    first_day_previous_month = last_day_previous_month.replace(day=1)
                    
                    date_range = []
                    current = first_day_previous_month
                    while current <= last_day_previous_month:
                        date_range.extend([
                            current.strftime('%d.%m.%Y'),
                            current.strftime('%d.%m.%y'),
                            current.strftime('%d/%m/%Y'),
                            current.strftime('%d/%m/%y'),
                        ])
                        current += timedelta(days=1)
                    
                    relative_dates.extend(date_range)
                    logger.info(f"📅 Generated date range for last month")
                
                break  # Only process first match
        
        if relative_dates:
            logger.info(f"✅ Total relative dates generated: {len(relative_dates)}")
            logger.info(f"📅 Sample dates: {relative_dates[:10]}")
        else:
            logger.info(f"❌ No relative date patterns found in query")
        
        return relative_dates
    
    def _determine_scope(self, query_type: str, field_patterns: List[str], criteria: Dict[str, Any]) -> str:
        """Determine the scope of data retrieval needed"""
        
        if query_type == 'specific_value':
            if criteria.get('dates') or criteria.get('times'):
                return 'single_value'  # Looking for specific value at specific time
            else:
                return 'multiple_values'  # Looking for field values across time
        
        elif query_type == 'general_search':
            if len(field_patterns) == 1 and not criteria:
                return 'all_related'  # Show all data related to one field
            else:
                return 'multiple_values'  # Show matching values
        
        elif query_type == 'latest_data':
            return 'single_value'  # Latest/current values
        
        elif query_type == 'summary':
            return 'all_related'  # Overview of all data
        
        return 'multiple_values'  # Default
    
    def _calculate_confidence(self, query_type: str, field_patterns: List[str], criteria: Dict[str, Any]) -> float:
        """Calculate confidence score for the normalization"""
        confidence = 0.5  # Base confidence
        
        # Boost confidence based on query type match
        if query_type != 'general_search':
            confidence += 0.2
        
        # Boost confidence based on field patterns found
        if field_patterns:
            confidence += min(0.2, len(field_patterns) * 0.05)
        
        # Boost confidence based on criteria found
        if criteria:
            confidence += min(0.2, len(criteria) * 0.05)
        
        # Boost confidence for specific patterns
        if criteria.get('dates'):
            confidence += 0.1
        
        if criteria.get('numbers'):
            confidence += 0.1
        
        return min(1.0, confidence)
    
    def expand_field_patterns(self, patterns: List[str]) -> List[str]:
        """
        Expand field patterns with variations and synonyms
        This helps match fields even if they're named differently
        """
        expanded = set(patterns)
        
        for pattern in patterns:
            pattern_lower = pattern.lower()
            
            # Add variations with common separators
            variations = [
                pattern_lower.replace(' ', '_'),
                pattern_lower.replace(' ', '-'),
                pattern_lower.replace(' ', ''),
                pattern_lower.replace('_', ' '),
                pattern_lower.replace('-', ' '),
            ]
            expanded.update(variations)
            
            # Add abbreviations and expansions
            abbreviations = {
                'temp': 'temperature',
                'press': 'pressure',
                'qty': 'quantity',
                'amt': 'amount',
                'val': 'value',
                'lvl': 'level',
                'pct': 'percent',
                'hr': 'hour',
                'min': 'minute',
                'sec': 'second',
            }
            
            for abbr, full in abbreviations.items():
                if abbr in pattern_lower:
                    expanded.add(pattern_lower.replace(abbr, full))
                if full in pattern_lower:
                    expanded.add(pattern_lower.replace(full, abbr))
        
        return list(expanded)
    
    def get_search_strategy(self, normalized_query: NormalizedQuery) -> Dict[str, Any]:
        """
        Get the optimal search strategy for the normalized query
        """
        strategy = {
            'search_type': normalized_query.scope,
            'field_patterns': self.expand_field_patterns(normalized_query.field_patterns),
            'criteria': normalized_query.criteria,
            'priority_order': [],
            'fallback_options': []
        }
        
        # Determine priority order for field matching
        if normalized_query.query_type == 'specific_value':
            strategy['priority_order'] = ['exact_match', 'partial_match', 'semantic_match']
        elif normalized_query.query_type == 'general_search':
            strategy['priority_order'] = ['semantic_match', 'partial_match', 'exact_match']
        else:
            strategy['priority_order'] = ['partial_match', 'semantic_match', 'exact_match']
        
        # Add fallback options
        if normalized_query.criteria.get('dates'):
            strategy['fallback_options'].append('search_without_date')
        
        if len(normalized_query.field_patterns) > 1:
            strategy['fallback_options'].append('search_individual_patterns')
        
        strategy['fallback_options'].append('broad_search')
        
        return strategy


# Global instance
_universal_query_normalizer = None

def get_universal_query_normalizer() -> UniversalQueryNormalizer:
    """Get the global universal query normalizer instance"""
    global _universal_query_normalizer
    if _universal_query_normalizer is None:
        _universal_query_normalizer = UniversalQueryNormalizer()
    return _universal_query_normalizer