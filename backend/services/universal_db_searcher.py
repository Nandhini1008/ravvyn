"""
Universal Database Searcher - Searches entire database using X-Y-Z coordinates
Works with any sheet structure dynamically without hardcoded values
"""

import json
import logging
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy.orm import Session
from services.database import get_db_context, SheetsData, SheetsMetadata
from services.universal_query_normalizer import NormalizedQuery
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SearchResult:
    """Universal search result with X-Y-Z coordinates"""
    sheet_id: str
    sheet_name: str
    tab_name: str
    field_name: str
    value: Any
    coordinates: Dict[str, int]  # x, y, z (z = tab index)
    row_data: Dict[str, Any]
    row_index: int
    match_score: float
    context: Dict[str, Any]


class UniversalDatabaseSearcher:
    """
    Universal database searcher that works with any sheet structure
    Uses X-Y-Z coordinate system for precise data location
    """
    
    def __init__(self, debug_logging: bool = False):
        """Initialize the universal searcher"""
        self.cache = {}  # Simple cache for sheet metadata
        self.debug_logging = debug_logging  # Control verbose logging
    
    async def search_database(self, normalized_query: NormalizedQuery) -> Dict[str, Any]:
        """
        PURE DATABASE SEARCH - NO AI/LLM INVOLVED
        Search entire SQLite database using X-Y-Z coordinates and pattern matching
        
        Args:
            normalized_query: Normalized query object (created by pattern matching, not AI)
            
        Returns:
            Comprehensive search results with X-Y-Z coordinates (pure database data)
        """
        logger.info(f"🔍 Searching database for: {normalized_query.query_type} - {normalized_query.field_patterns[:3]}")
        if normalized_query.criteria.get('dates'):
            logger.info(f"📅 Date range: {len(normalized_query.criteria['dates'])} dates")
        
        try:
            # Step 1: Get all available sheets and tabs dynamically
            sheet_metadata = await self._get_all_sheet_metadata()
            
            if not sheet_metadata:
                return {
                    "success": False,
                    "error": "No sheets found in database",
                    "results": []
                }
            
            # Step 2: Search based on query scope
            if normalized_query.scope == 'single_value':
                results = await self._search_single_value(normalized_query, sheet_metadata)
            elif normalized_query.scope == 'multiple_values':
                results = await self._search_multiple_values(normalized_query, sheet_metadata)
            else:  # all_related
                results = await self._search_all_related(normalized_query, sheet_metadata)
            
            # Step 3: Sort and rank results
            ranked_results = self._rank_results(results, normalized_query)
            
            # Step 4: Build comprehensive response
            logger.info(f"✅ Search completed: {len(ranked_results)} results found across {len(sheet_metadata)} sheets")
            
            return {
                "success": True,
                "query_type": normalized_query.query_type,
                "scope": normalized_query.scope,
                "total_results": len(ranked_results),
                "results": ranked_results,
                "sheets_searched": len(sheet_metadata),
                "search_metadata": {
                    "field_patterns": normalized_query.field_patterns,
                    "criteria": normalized_query.criteria,
                    "confidence": normalized_query.confidence
                }
            }
            
        except Exception as e:
            logger.error(f"Error in database search: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "results": []
            }
    
    async def _get_all_sheet_metadata(self) -> List[Dict[str, Any]]:
        """Get metadata for all sheets in database dynamically"""
        try:
            with get_db_context() as db:
                # Get all sheet metadata
                sheets = db.query(SheetsMetadata).filter(
                    SheetsMetadata.sync_status == 'completed'
                ).all()
                
                metadata = []
                for sheet in sheets:
                    # Get all tabs for this sheet
                    tabs = db.query(SheetsData.tab_name).filter(
                        SheetsData.sheet_id == sheet.sheet_id
                    ).distinct().all()
                    
                    tab_names = [tab[0] for tab in tabs]
                    
                    metadata.append({
                        'sheet_id': sheet.sheet_id,
                        'sheet_name': sheet.sheet_name,
                        'tabs': tab_names,
                        'last_synced': sheet.last_synced,
                        'total_tabs': len(tab_names)
                    })
                
                logger.info(f"📊 Found {len(metadata)} sheets with {sum(m['total_tabs'] for m in metadata)} tabs")
                return metadata
                
        except Exception as e:
            logger.error(f"Error getting sheet metadata: {str(e)}")
            return []
    
    async def _search_single_value(self, query: NormalizedQuery, sheet_metadata: List[Dict]) -> List[SearchResult]:
        """Search for single specific value"""
        results = []
        
        try:
            with get_db_context() as db:
                # Process all sheets and tabs concurrently
                import asyncio
                
                async def process_sheet_tab(sheet_meta, z, tab_name):
                    """Process a single sheet tab"""
                    sheet_id = sheet_meta['sheet_id']
                    sheet_name = sheet_meta['sheet_name']
                    
                    # Get field structure for this tab
                    field_structure = await self._analyze_tab_structure(db, sheet_id, tab_name)
                    
                    # Find matching fields
                    matching_fields = self._find_matching_fields(field_structure, query.field_patterns)
                    
                    tab_results = []
                    for field_info in matching_fields:
                        # Search for values with criteria
                        field_results = await self._search_field_with_criteria(
                            db, sheet_id, sheet_name, tab_name, z, field_info, query.criteria
                        )
                        tab_results.extend(field_results)
                        
                        # For single value, return first good match
                        if field_results and query.scope == 'single_value':
                            break
                    
                    return tab_results
                
                # Create tasks for all sheet-tab combinations
                tasks = []
                for sheet_meta in sheet_metadata:
                    for z, tab_name in enumerate(sheet_meta['tabs']):
                        task = process_sheet_tab(sheet_meta, z, tab_name)
                        tasks.append(task)
                
                # Process all tabs concurrently
                if tasks:
                    tab_results_list = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Collect results, filtering out exceptions
                    for tab_results in tab_results_list:
                        if isinstance(tab_results, list):
                            results.extend(tab_results)
                        elif isinstance(tab_results, Exception):
                            logger.warning(f"Error processing tab: {tab_results}")
                
        except Exception as e:
            logger.error(f"Error in single value search: {str(e)}")
        
        return results
    
    async def _search_multiple_values(self, query: NormalizedQuery, sheet_metadata: List[Dict]) -> List[SearchResult]:
        """Search for multiple values across database"""
        results = []
        
        try:
            with get_db_context() as db:
                # Process all sheets and tabs concurrently
                import asyncio
                
                async def process_sheet_tab(sheet_meta, z, tab_name):
                    """Process a single sheet tab"""
                    sheet_id = sheet_meta['sheet_id']
                    sheet_name = sheet_meta['sheet_name']
                    
                    # Get field structure for this tab
                    field_structure = await self._analyze_tab_structure(db, sheet_id, tab_name)
                    
                    # Find matching fields
                    matching_fields = self._find_matching_fields(field_structure, query.field_patterns)
                    
                    tab_results = []
                    for field_info in matching_fields:
                        # Search for all matching values
                        field_results = await self._search_field_with_criteria(
                            db, sheet_id, sheet_name, tab_name, z, field_info, query.criteria
                        )
                        tab_results.extend(field_results)
                    
                    return tab_results
                
                # Create tasks for all sheet-tab combinations
                tasks = []
                for sheet_meta in sheet_metadata:
                    for z, tab_name in enumerate(sheet_meta['tabs']):
                        task = process_sheet_tab(sheet_meta, z, tab_name)
                        tasks.append(task)
                
                # Process all tabs concurrently
                if tasks:
                    tab_results_list = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Collect results, filtering out exceptions
                    for tab_results in tab_results_list:
                        if isinstance(tab_results, list):
                            results.extend(tab_results)
                        elif isinstance(tab_results, Exception):
                            logger.warning(f"Error processing tab: {tab_results}")
                
        except Exception as e:
            logger.error(f"Error in multiple values search: {str(e)}")
        
        return results
    
    async def _search_all_related(self, query: NormalizedQuery, sheet_metadata: List[Dict]) -> List[SearchResult]:
        """Search for all data related to query patterns"""
        results = []
        
        try:
            with get_db_context() as db:
                # Process all sheets and tabs concurrently
                import asyncio
                
                async def process_sheet_tab(sheet_meta, z, tab_name):
                    """Process a single sheet tab"""
                    sheet_id = sheet_meta['sheet_id']
                    sheet_name = sheet_meta['sheet_name']
                    
                    # Get field structure for this tab
                    field_structure = await self._analyze_tab_structure(db, sheet_id, tab_name)
                    
                    # Find matching fields (more lenient for all_related)
                    matching_fields = self._find_matching_fields(field_structure, query.field_patterns, lenient=True)
                    
                    tab_results = []
                    for field_info in matching_fields:
                        # For queries with date criteria, search ALL data, not just samples
                        if query.criteria.get('dates'):
                            field_results = await self._search_field_with_criteria(
                                db, sheet_id, sheet_name, tab_name, z, field_info, query.criteria
                            )
                        else:
                            # For general queries, get sample data
                            field_results = await self._get_field_sample_data(
                                db, sheet_id, sheet_name, tab_name, z, field_info, limit=20
                            )
                        tab_results.extend(field_results)
                    
                    return tab_results
                
                # Create tasks for all sheet-tab combinations
                tasks = []
                for sheet_meta in sheet_metadata:
                    for z, tab_name in enumerate(sheet_meta['tabs']):
                        task = process_sheet_tab(sheet_meta, z, tab_name)
                        tasks.append(task)
                
                # Process all tabs concurrently
                if tasks:
                    tab_results_list = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Collect results, filtering out exceptions
                    for tab_results in tab_results_list:
                        if isinstance(tab_results, list):
                            results.extend(tab_results)
                        elif isinstance(tab_results, Exception):
                            logger.warning(f"Error processing tab: {tab_results}")
                
        except Exception as e:
            logger.error(f"Error in all related search: {str(e)}")
        
        return results
    
    async def _analyze_tab_structure(self, db: Session, sheet_id: str, tab_name: str) -> Dict[str, Any]:
        """Analyze tab structure to get field information with X-Y coordinates"""
        cache_key = f"{sheet_id}_{tab_name}_structure"
        
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            # Get first few rows to determine structure
            rows = db.query(SheetsData).filter(
                SheetsData.sheet_id == sheet_id,
                SheetsData.tab_name == tab_name
            ).order_by(SheetsData.row_index).limit(10).all()
            
            if not rows:
                return {'fields': [], 'headers': []}
            
            # Parse first row as headers
            first_row = rows[0]
            headers = json.loads(first_row.row_data) if isinstance(first_row.row_data, str) else first_row.row_data
            
            if not headers or not isinstance(headers, list):
                return {'fields': [], 'headers': []}
            
            # Build field structure with coordinates
            fields = []
            for x, header in enumerate(headers):
                if header and str(header).strip():
                    field_info = {
                        'name': str(header).strip(),
                        'x': x,
                        'header_y': first_row.row_index,
                        'data_type': self._detect_field_type(db, sheet_id, tab_name, x),
                        'sample_values': self._get_field_samples(db, sheet_id, tab_name, x)
                    }
                    fields.append(field_info)
            
            structure = {
                'fields': fields,
                'headers': headers,
                'total_rows': db.query(SheetsData).filter(
                    SheetsData.sheet_id == sheet_id,
                    SheetsData.tab_name == tab_name
                ).count()
            }
            
            # Cache the structure
            self.cache[cache_key] = structure
            return structure
            
        except Exception as e:
            logger.error(f"Error analyzing tab structure: {str(e)}")
            return {'fields': [], 'headers': []}
    
    def _find_matching_fields(self, field_structure: Dict[str, Any], patterns: List[str], lenient: bool = False) -> List[Dict[str, Any]]:
        """Find fields that match the query patterns"""
        matching_fields = []
        fields = field_structure.get('fields', [])
        
        for field in fields:
            field_name = field['name'].lower()
            match_score = 0
            
            for pattern in patterns:
                pattern_lower = pattern.lower()
                
                # Exact match
                if pattern_lower == field_name:
                    match_score += 10
                
                # Partial match
                elif pattern_lower in field_name or field_name in pattern_lower:
                    match_score += 5
                
                # Word match
                elif any(word in field_name for word in pattern_lower.split()):
                    match_score += 3
                
                # Fuzzy match for lenient search
                elif lenient and self._fuzzy_match(pattern_lower, field_name):
                    match_score += 2
            
            if match_score > 0:
                field['match_score'] = match_score
                matching_fields.append(field)
        
        # Sort by match score
        matching_fields.sort(key=lambda x: x['match_score'], reverse=True)
        return matching_fields
    
    def _fuzzy_match(self, pattern: str, field_name: str) -> bool:
        """Simple fuzzy matching for field names"""
        # Remove common separators and compare
        pattern_clean = pattern.replace('_', '').replace('-', '').replace(' ', '')
        field_clean = field_name.replace('_', '').replace('-', '').replace(' ', '')
        
        # Check if significant portion matches
        if len(pattern_clean) >= 3 and len(field_clean) >= 3:
            common_chars = sum(1 for a, b in zip(pattern_clean, field_clean) if a == b)
            return common_chars / max(len(pattern_clean), len(field_clean)) > 0.6
        
        return False
    
    async def _search_field_with_criteria(self, db: Session, sheet_id: str, sheet_name: str, 
                                        tab_name: str, z: int, field_info: Dict[str, Any], 
                                        criteria: Dict[str, Any]) -> List[SearchResult]:
        """Search specific field with criteria"""
        results = []
        
        try:
            # Get all rows for this tab
            rows = db.query(SheetsData).filter(
                SheetsData.sheet_id == sheet_id,
                SheetsData.tab_name == tab_name
            ).order_by(SheetsData.row_index).all()
            
            field_x = field_info['x']
            field_name = field_info['name']
            
            for row in rows:
                try:
                    row_data = json.loads(row.row_data) if isinstance(row.row_data, str) else row.row_data
                    
                    if not row_data or not isinstance(row_data, list) or field_x >= len(row_data):
                        continue
                    
                    field_value = row_data[field_x]
                    
                    # Check if row matches criteria
                    if self._matches_criteria(row_data, criteria):
                        # Build complete row data with field names
                        complete_row_data = self._build_complete_row_data(row_data, field_info, db, sheet_id, tab_name)
                        
                        result = SearchResult(
                            sheet_id=sheet_id,
                            sheet_name=sheet_name,
                            tab_name=tab_name,
                            field_name=field_name,
                            value=field_value,
                            coordinates={'x': field_x, 'y': row.row_index, 'z': z},
                            row_data=complete_row_data,
                            row_index=row.row_index,
                            match_score=field_info.get('match_score', 1.0),
                            context={
                                'data_type': field_info.get('data_type', 'unknown'),
                                'has_criteria_match': bool(criteria),
                                'row_has_data': any(v for v in row_data if v and str(v).strip())
                            }
                        )
                        results.append(result)
                
                except (json.JSONDecodeError, TypeError, IndexError) as e:
                    logger.warning(f"Error processing row {row.row_index}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error searching field with criteria: {str(e)}")
        
        return results
    
    async def _get_field_sample_data(self, db: Session, sheet_id: str, sheet_name: str, 
                                   tab_name: str, z: int, field_info: Dict[str, Any], 
                                   limit: int = 20) -> List[SearchResult]:
        """Get sample data from a field"""
        results = []
        
        try:
            # Get recent rows with data
            rows = db.query(SheetsData).filter(
                SheetsData.sheet_id == sheet_id,
                SheetsData.tab_name == tab_name
            ).order_by(SheetsData.row_index.desc()).limit(limit * 2).all()  # Get more to filter
            
            field_x = field_info['x']
            field_name = field_info['name']
            count = 0
            
            for row in rows:
                if count >= limit:
                    break
                
                try:
                    row_data = json.loads(row.row_data) if isinstance(row.row_data, str) else row.row_data
                    
                    if not row_data or not isinstance(row_data, list) or field_x >= len(row_data):
                        continue
                    
                    field_value = row_data[field_x]
                    
                    # Only include rows with actual data
                    if field_value and str(field_value).strip():
                        complete_row_data = self._build_complete_row_data(row_data, field_info, db, sheet_id, tab_name)
                        
                        result = SearchResult(
                            sheet_id=sheet_id,
                            sheet_name=sheet_name,
                            tab_name=tab_name,
                            field_name=field_name,
                            value=field_value,
                            coordinates={'x': field_x, 'y': row.row_index, 'z': z},
                            row_data=complete_row_data,
                            row_index=row.row_index,
                            match_score=field_info.get('match_score', 1.0),
                            context={
                                'data_type': field_info.get('data_type', 'unknown'),
                                'is_sample': True,
                                'row_has_data': True
                            }
                        )
                        results.append(result)
                        count += 1
                
                except (json.JSONDecodeError, TypeError, IndexError):
                    continue
            
        except Exception as e:
            logger.error(f"Error getting field sample data: {str(e)}")
        
        return results
    
    def _matches_criteria(self, row_data: List[Any], criteria: Dict[str, Any]) -> bool:
        """Check if row matches search criteria with enhanced date matching"""
        if not criteria:
            return True
        
        row_text = ' '.join(str(cell).lower() for cell in row_data if cell)
        
        # 🔍 CRITICAL FIX: Proper date filtering logic for relative date ranges
        if criteria.get('dates'):
            # For relative date ranges (like "last 7 days"), we have many date variations
            # We need to check if ANY of the dates in the row match ANY of our target dates
            date_found = False
            
            for date_val in criteria['dates']:
                if self._matches_date_in_row(str(date_val), row_data):
                    date_found = True
                    break  # Found a match, no need to check more
            
            if not date_found:
                # If we have date criteria but no match, return False
                return False
        
        # Check number criteria
        if criteria.get('numbers'):
            for num_val in criteria['numbers']:
                if str(num_val) in row_text:
                    return True
            return False  # If number criteria exists but no match
        
        # Check condition criteria
        if criteria.get('conditions'):
            for condition in criteria['conditions']:
                if condition.lower() in row_text:
                    return True
            return False  # If condition criteria exists but no match
        
        # If no specific criteria matched but criteria exist, do broad search
        if criteria and not (criteria.get('dates') or criteria.get('numbers') or criteria.get('conditions')):
            return True
        
        return True  # Return True if no criteria
    
    def _matches_date_in_row(self, target_date: str, row_data: List[Any]) -> bool:
        """Enhanced date matching that handles multiple date formats"""
        if not target_date or not row_data:
            return False
        
        # Convert target date to multiple possible formats
        date_variations = self._generate_date_variations(target_date)
        
        # Check each cell in the row
        for i, cell in enumerate(row_data):
            if not cell:
                continue
                
            cell_str = str(cell).strip()
            if not cell_str:
                continue
            
            # 🔍 ENHANCED DATE MATCHING with minimal logging
            # Check for exact matches with any date variation
            for date_var in date_variations:
                if date_var in cell_str.lower():
                    return True
            
            # Check if cell contains date components
            if self._contains_date_components(target_date, cell_str):
                return True
        
        # Only log when no match is found for debugging (reduced logging)
        return False
    
    def _generate_date_variations(self, date_str: str) -> List[str]:
        """Generate multiple variations of a date string, prioritizing dd.mm.yyyy format"""
        variations = []
        
        # Try to parse the date and generate variations
        import re
        
        # Match patterns like 12-12-2025, 12.12.2025, 12/12/2025, 2025-12-12
        date_match = re.match(r'(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{2,4})', date_str)
        if date_match:
            day, month, year = date_match.groups()
            
            # Ensure 2-digit day/month
            day = day.zfill(2)
            month = month.zfill(2)
            
            # 🔍 ENHANCED DATE VARIATIONS - Prioritize dd.mm.yyyy format
            variations = [
                # PRIMARY FORMAT: dd.mm.yyyy (prioritized)
                f"{day}.{month}.{year}",
                f"{day}.{month}.{year[-2:]}",  # dd.mm.yy
                f"{int(day)}.{int(month)}.{year}",  # d.m.yyyy (no leading zeros)
                f"{int(day)}.{int(month)}.{year[-2:]}",  # d.m.yy
                
                # Alternative formats (for compatibility)
                f"{day}/{month}/{year}",
                f"{day}-{month}-{year}",
                f"{day}/{month}/{year[-2:]}",
                f"{day}-{month}-{year[-2:]}",
                f"{int(day)}/{int(month)}/{year}",
                f"{int(day)}-{int(month)}-{year}",
                f"{int(day)}/{int(month)}/{year[-2:]}",
                f"{int(day)}-{int(month)}-{year[-2:]}",
                
                # Reverse order (month-day) for compatibility
                f"{month}.{day}.{year}",
                f"{month}/{day}/{year}",
                f"{month}-{day}-{year}",
                f"{int(month)}.{int(day)}.{year}",
                f"{int(month)}/{int(day)}/{year}",
                f"{int(month)}-{int(day)}-{year}",
                
                # Just day and month
                f"{day}.{month}",
                f"{day}/{month}",
                f"{day}-{month}",
                f"{int(day)}.{int(month)}",
                f"{int(day)}/{int(month)}",
                f"{int(day)}-{int(month)}",
            ]
            
            # Add original if not already in list
            if date_str.lower() not in [v.lower() for v in variations]:
                variations.insert(0, date_str)
            else:
                variations.insert(0, date_str.lower())
            
            logger.debug(f"🔍 Generated date variations for '{date_str}': {variations[:10]}... (dd.mm.yyyy prioritized)")
        else:
            # If no match, try YYYY-MM-DD format
            date_match_iso = re.match(r'(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})', date_str)
            if date_match_iso:
                year, month, day = date_match_iso.groups()
                day = day.zfill(2)
                month = month.zfill(2)
                
                # Convert to dd.mm.yyyy format and generate variations
                variations = [
                    f"{day}.{month}.{year}",  # PRIMARY: dd.mm.yyyy
                    f"{day}.{month}.{year[-2:]}",
                    f"{int(day)}.{int(month)}.{year}",
                    f"{day}/{month}/{year}",
                    f"{day}-{month}-{year}",
                    date_str  # Keep original
                ]
            else:
                variations = [date_str.lower()]
        
        # Remove duplicates while preserving order (dd.mm.yyyy first)
        seen = set()
        unique_variations = []
        for v in variations:
            v_lower = v.lower()
            if v_lower not in seen:
                seen.add(v_lower)
                unique_variations.append(v)
        
        return unique_variations
    
    def _contains_date_components(self, target_date: str, cell_str: str) -> bool:
        """Check if cell contains components of the target date"""
        import re
        
        # Extract date components from target
        date_match = re.match(r'(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{2,4})', target_date)
        if not date_match:
            return False
        
        day, month, year = date_match.groups()
        
        # Check if cell contains these components in any order
        cell_lower = cell_str.lower()
        
        # Look for day and month together
        day_month_patterns = [
            f"{day}.{month}",
            f"{day}/{month}",
            f"{day}-{month}",
            f"{int(day)}.{int(month)}",
            f"{int(day)}/{int(month)}",
            f"{int(day)}-{int(month)}",
        ]
        
        for pattern in day_month_patterns:
            if pattern in cell_lower:
                return True
        
        return False
    
    def _build_complete_row_data(self, row_data: List[Any], field_info: Dict[str, Any], 
                               db: Session, sheet_id: str, tab_name: str) -> Dict[str, Any]:
        """Build complete row data with field names"""
        try:
            # Get field structure if not cached
            structure = self.cache.get(f"{sheet_id}_{tab_name}_structure")
            if not structure:
                # Simple fallback - use indices
                return {f"column_{i}": value for i, value in enumerate(row_data) if value and str(value).strip()}
            
            complete_data = {}
            headers = structure.get('headers', [])
            
            # CRITICAL FIX: Check if this row_data IS the header row
            # If the row data matches the headers exactly, skip it or use generic names
            if headers and len(row_data) == len(headers):
                is_header_row = True
                for i, (data_val, header_val) in enumerate(zip(row_data, headers)):
                    if str(data_val).strip().lower() != str(header_val).strip().lower():
                        is_header_row = False
                        break
                
                if is_header_row:
                    logger.debug(f"Skipping header row for {tab_name}")
                    # Return generic column names for header row to avoid confusion
                    return {f"column_{i}": value for i, value in enumerate(row_data) if value and str(value).strip()}
            
            # Build complete data using header names for actual data rows
            for field in structure.get('fields', []):
                x = field['x']
                name = field['name']
                if x < len(row_data) and row_data[x] and str(row_data[x]).strip():
                    # Only include non-empty values
                    complete_data[name] = row_data[x]
            
            return complete_data
            
        except Exception as e:
            logger.warning(f"Error building complete row data: {str(e)}")
            return {f"column_{i}": value for i, value in enumerate(row_data) if value and str(value).strip()}
    
    def _detect_field_type(self, db: Session, sheet_id: str, tab_name: str, x: int) -> str:
        """Detect field data type by sampling values"""
        try:
            # Get sample values
            rows = db.query(SheetsData).filter(
                SheetsData.sheet_id == sheet_id,
                SheetsData.tab_name == tab_name
            ).limit(10).all()
            
            values = []
            for row in rows:
                try:
                    row_data = json.loads(row.row_data) if isinstance(row.row_data, str) else row.row_data
                    if row_data and isinstance(row_data, list) and x < len(row_data) and row_data[x]:
                        values.append(str(row_data[x]))
                except:
                    continue
            
            if not values:
                return 'unknown'
            
            # Simple type detection
            numeric_count = sum(1 for v in values if v.replace('.', '').replace('-', '').isdigit())
            date_count = sum(1 for v in values if any(sep in v for sep in ['.', '/', '-']) and any(c.isdigit() for c in v))
            
            if numeric_count > len(values) * 0.7:
                return 'numeric'
            elif date_count > len(values) * 0.5:
                return 'date'
            else:
                return 'text'
                
        except Exception:
            return 'unknown'
    
    def _get_field_samples(self, db: Session, sheet_id: str, tab_name: str, x: int) -> List[str]:
        """Get sample values for a field"""
        try:
            rows = db.query(SheetsData).filter(
                SheetsData.sheet_id == sheet_id,
                SheetsData.tab_name == tab_name
            ).limit(5).all()
            
            samples = []
            for row in rows:
                try:
                    row_data = json.loads(row.row_data) if isinstance(row.row_data, str) else row.row_data
                    if row_data and isinstance(row_data, list) and x < len(row_data) and row_data[x]:
                        samples.append(str(row_data[x]))
                except:
                    continue
            
            return samples[:3]  # Return top 3 samples
            
        except Exception:
            return []
    
    def _rank_results(self, results: List[SearchResult], query: NormalizedQuery) -> List[Dict[str, Any]]:
        """Rank and format results for output"""
        # Sort by match score and relevance
        results.sort(key=lambda r: (r.match_score, -r.row_index), reverse=True)
        
        # Convert to output format
        formatted_results = []
        for result in results:
            formatted_results.append({
                'sheet_id': result.sheet_id,
                'sheet_name': result.sheet_name,
                'tab_name': result.tab_name,
                'field_name': result.field_name,
                'value': result.value,
                'coordinates': result.coordinates,
                'row_data': result.row_data,
                'row_index': result.row_index,
                'match_score': result.match_score,
                'context': result.context
            })
        
        return formatted_results


# Global instance
_universal_database_searcher = None

def get_universal_database_searcher() -> UniversalDatabaseSearcher:
    """Get the global universal database searcher instance"""
    global _universal_database_searcher
    if _universal_database_searcher is None:
        _universal_database_searcher = UniversalDatabaseSearcher()
    return _universal_database_searcher