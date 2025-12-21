"""
Universal Data Service - X-Y Coordinate Based Data Retrieval
Works with any sheet structure using coordinate-based approach
"""

import json
import logging
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy.orm import Session
from services.database import get_db_context, SheetsData, SheetsMetadata
from services.universal_sheet_analyzer import get_universal_analyzer
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class UniversalDataService:
    """
    Universal data service using X-Y coordinate system
    Provides generalized data retrieval for any sheet structure
    """
    
    def __init__(self):
        """Initialize universal data service"""
        self.default_sheet_id = "1MtjJyKiDR7COszXxZF-wYR-UF5LcwYSpTfr_Aa_PEt8"
        self.analyzer = get_universal_analyzer()
        self._sheet_analyses = {}  # Cache for sheet analyses
    
    async def analyze_sheet(self, sheet_id: str = None, tab_name: str = None, 
                          force_refresh: bool = False) -> Dict[str, Any]:
        """
        Analyze sheet structure using universal analyzer with SQLite database
        
        Args:
            sheet_id: Sheet ID (optional, uses default)
            tab_name: Specific tab name (optional)
            force_refresh: Force re-analysis even if cached
            
        Returns:
            Complete sheet analysis with coordinate mapping
        """
        sheet_id = sheet_id or self.default_sheet_id
        cache_key = f"{sheet_id}_{tab_name or 'all'}"
        
        # Return cached analysis if available and not forcing refresh
        if not force_refresh and cache_key in self._sheet_analyses:
            logger.info(f"Using cached analysis for {cache_key}")
            return self._sheet_analyses[cache_key]
        
        try:
            with get_db_context() as db:
                # Get sheet metadata
                sheet_meta = db.query(SheetsMetadata).filter(
                    SheetsMetadata.sheet_id == sheet_id
                ).first()
                
                if not sheet_meta:
                    return {
                        "success": False,
                        "error": f"Sheet {sheet_id} not found in database",
                        "analysis": {}
                    }
                
                # Get sheet data from SQLite using SQLAlchemy ORM
                query = db.query(SheetsData).filter(
                    SheetsData.sheet_id == sheet_id
                )
                
                if tab_name:
                    query = query.filter(SheetsData.tab_name == tab_name)
                
                # Order by tab_name and row_index to maintain structure
                rows = query.order_by(SheetsData.tab_name, SheetsData.row_index).all()
                
                if not rows:
                    return {
                        "success": False,
                        "error": "No data found in database",
                        "analysis": {}
                    }
                
                # Group by tab for analysis
                tabs_data = {}
                for row in rows:
                    tab_name_db = row.tab_name
                    row_index = row.row_index
                    row_data_str = row.row_data
                    synced_at = row.synced_at
                    
                    if tab_name_db not in tabs_data:
                        tabs_data[tab_name_db] = []
                    
                    # Parse row data from JSON string
                    try:
                        row_data = json.loads(row_data_str) if isinstance(row_data_str, str) else row_data_str
                    except (json.JSONDecodeError, TypeError):
                        logger.warning(f"Failed to parse row data for {tab_name_db} row {row_index}")
                        row_data = []
                    
                    tabs_data[tab_name_db].append({
                        'row_index': row_index,
                        'data': row_data,
                        'synced_at': synced_at
                    })
                
                # Analyze each tab
                complete_analysis = {
                    "success": True,
                    "sheet_info": {
                        "sheet_id": sheet_id,
                        "sheet_name": sheet_meta.sheet_name,
                        "last_synced": sheet_meta.last_synced.isoformat() if sheet_meta.last_synced else None
                    },
                    "tabs_analysis": {}
                }
                
                for tab_name_key, tab_rows in tabs_data.items():
                    # Extract raw data for analysis
                    raw_data = [row['data'] for row in tab_rows if row['data']]
                    
                    if not raw_data:
                        logger.warning(f"No valid data found for tab '{tab_name_key}'")
                        continue
                    
                    # Analyze tab structure
                    tab_analysis = self.analyzer.analyze_sheet_structure(raw_data, tab_name_key)
                    
                    # Add database metadata
                    tab_analysis['metadata'] = {
                        'total_db_rows': len(tab_rows),
                        'valid_data_rows': len(raw_data),
                        'last_synced': max((row['synced_at'] for row in tab_rows if row['synced_at']), default=None),
                        'row_indices': [row['row_index'] for row in tab_rows],
                        'data_source': 'sqlite_database'
                    }
                    
                    complete_analysis["tabs_analysis"][tab_name_key] = tab_analysis
                    
                    logger.info(f"Analyzed tab '{tab_name_key}': "
                               f"{len(raw_data)} valid rows, "
                               f"{len(tab_analysis['field_catalog'])} fields")
                
                # Cache the analysis
                self._sheet_analyses[cache_key] = complete_analysis
                
                logger.info(f"✅ Sheet analysis completed: {len(complete_analysis['tabs_analysis'])} tabs analyzed")
                return complete_analysis
                
        except Exception as e:
            logger.error(f"Error analyzing sheet from database: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "analysis": {}
            }
    
    async def get_field_value(self, field_query: str, criteria: Dict[str, Any] = None,
                            sheet_id: str = None, tab_name: str = None) -> Dict[str, Any]:
        """
        Get field value using universal coordinate system with SQLite database
        
        Args:
            field_query: Field name or description (e.g., "ro1&2 feed tank level")
            criteria: Search criteria (e.g., {"date": "26.6.25"})
            sheet_id: Sheet ID (optional)
            tab_name: Tab name (optional)
            
        Returns:
            Field values with coordinates and context
        """
        sheet_id = sheet_id or self.default_sheet_id
        
        # Get sheet analysis from database
        analysis_result = await self.analyze_sheet(sheet_id, tab_name)
        
        if not analysis_result["success"]:
            return {
                "success": False,
                "error": analysis_result["error"],
                "values": []
            }
        
        results = []
        
        # Search across all analyzed tabs
        for tab_name_key, tab_analysis in analysis_result["tabs_analysis"].items():
            # Find field coordinates using analyzer
            field_coords = self.analyzer.find_field_coordinates(tab_analysis, field_query)
            
            if field_coords:
                logger.info(f"Found field '{field_query}' in tab '{tab_name_key}' at coordinates {field_coords}")
                
                # Get field values based on criteria using database
                field_values = await self._get_field_values_from_db(
                    sheet_id, tab_name_key, field_coords, tab_analysis, criteria or {}
                )
                
                for value_info in field_values:
                    results.append({
                        "tab_name": tab_name_key,
                        "field_name": field_query,
                        "value": value_info["value"],
                        "coordinates": value_info["coordinates"],
                        "row_data": value_info["row_data"],
                        "context": self._build_context(tab_analysis, value_info),
                        "data_source": "sqlite_database"
                    })
        
        return {
            "success": True,
            "field_query": field_query,
            "criteria": criteria,
            "values_found": len(results),
            "values": results,
            "data_source": "sqlite_database"
        }
    
    async def get_latest_data(self, sheet_id: str = None, tab_name: str = None) -> Dict[str, Any]:
        """
        Get latest data using intelligent coordinate-based strategy with SQLite database
        
        Args:
            sheet_id: Sheet ID (optional)
            tab_name: Tab name (optional)
            
        Returns:
            Latest data with proper field mapping
        """
        sheet_id = sheet_id or self.default_sheet_id
        
        # Get sheet analysis from database
        analysis_result = await self.analyze_sheet(sheet_id, tab_name)
        
        if not analysis_result["success"]:
            return {
                "success": False,
                "error": analysis_result["error"],
                "latest_data": {}
            }
        
        latest_data = {}
        
        # Get latest data from each tab using database
        for tab_name_key, tab_analysis in analysis_result["tabs_analysis"].items():
            latest_row = await self._get_latest_data_row_from_db(
                sheet_id, tab_name_key, tab_analysis
            )
            
            if latest_row:
                latest_data[tab_name_key] = {
                    "coordinates": latest_row["coordinates"],
                    "fields": latest_row["fields"],
                    "row_index": latest_row.get("row_index"),
                    "context": {
                        "strategy_used": tab_analysis["query_hints"]["latest_data_strategy"]["method"],
                        "field_count": len(latest_row["fields"]),
                        "non_empty_fields": len([v for v in latest_row["fields"].values() 
                                               if not self.analyzer._is_empty_cell(v)]),
                        "data_source": "sqlite_database"
                    }
                }
        
        return {
            "success": True,
            "latest_data": latest_data,
            "tabs_processed": list(latest_data.keys()),
            "data_source": "sqlite_database"
        }
    
    async def _get_latest_data_row_from_db(self, sheet_id: str, tab_name: str, 
                                         tab_analysis: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Get the most recent data row from SQLite database using SQL queries
        """
        try:
            with get_db_context() as db:
                # Use SQL to get the latest non-empty row more efficiently
                sql_query = """
                    SELECT * FROM sheets_data 
                    WHERE sheet_id = :sheet_id 
                    AND tab_name = :tab_name 
                    AND row_index > 0
                    AND row_data != '[]'
                    AND row_data IS NOT NULL
                    AND LENGTH(row_data) > 10
                    ORDER BY row_index DESC
                    LIMIT 10
                """
                
                search_params = {'sheet_id': sheet_id, 'tab_name': tab_name}
                
                logger.info(f"🔍 Getting latest data with SQL for {tab_name}")
                
                result = db.execute(sql_query, search_params)
                recent_rows = result.fetchall()
                
                logger.info(f"✅ Found {len(recent_rows)} recent rows for latest data")
                
                # Process rows to find the best one
                for row_tuple in recent_rows:
                    try:
                        if len(row_tuple) >= 4:
                            row_index = row_tuple[2]
                            row_data_str = row_tuple[3]
                        else:
                            continue
                        
                        row_data = json.loads(row_data_str) if isinstance(row_data_str, str) else row_data_str
                        
                        if not row_data or not isinstance(row_data, list):
                            continue
                        
                        # Check if row has meaningful data
                        has_data = any(
                            value and str(value).strip() and str(value).strip() not in ['', 'null', 'None']
                            for value in row_data
                        )
                        
                        if has_data:
                            return self._build_row_data_response(row_data, row_index, tab_analysis)
                    
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.warning(f"Error processing latest data row: {e}")
                        continue
                
        except Exception as e:
            logger.error(f"Error getting latest data row from database: {str(e)}")
        
        return None
    
    def _build_row_data_response(self, row_data: List[Any], row_index: int, 
                               tab_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build row data response with field mapping
        """
        fields = {}
        
        # Map data to field names
        for field_name, field_info in tab_analysis['field_catalog'].items():
            x = field_info['coordinates']['x']
            if x < len(row_data):
                fields[field_name] = row_data[x]
        
        return {
            'coordinates': {'y': row_index},
            'fields': fields,
            'row_index': row_index
        }
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string to datetime"""
        date_formats = [
            '%d.%m.%y', '%d.%m.%Y', '%d/%m/%y', '%d/%m/%Y',
            '%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y'
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        
        return None
    
    async def search_data(self, search_query: str, sheet_id: str = None, 
                         tab_name: str = None, limit: int = 100) -> Dict[str, Any]:
        """
        Universal search across any sheet structure using SQLite database
        
        Args:
            search_query: Search text or criteria
            sheet_id: Sheet ID (optional)
            tab_name: Tab name (optional)
            limit: Maximum results
            
        Returns:
            Search results with coordinates and context
        """
        sheet_id = sheet_id or self.default_sheet_id
        
        # Get sheet analysis from database
        analysis_result = await self.analyze_sheet(sheet_id, tab_name)
        
        if not analysis_result["success"]:
            return {
                "success": False,
                "error": analysis_result["error"],
                "results": []
            }
        
        all_results = []
        
        # Search across all analyzed tabs using database
        for tab_name_key, tab_analysis in analysis_result["tabs_analysis"].items():
            tab_results = await self._search_tab_data_from_db(
                sheet_id, tab_name_key, tab_analysis, search_query
            )
            all_results.extend(tab_results)
        
        # Sort by match score and limit results
        all_results.sort(key=lambda x: x["match_score"], reverse=True)
        limited_results = all_results[:limit]
        
        return {
            "success": True,
            "search_query": search_query,
            "total_matches": len(all_results),
            "returned_results": len(limited_results),
            "results": limited_results,
            "data_source": "sqlite_database"
        }
    
    async def _search_tab_data_from_db(self, sheet_id: str, tab_name: str, 
                                     tab_analysis: Dict[str, Any], search_query: str) -> List[Dict[str, Any]]:
        """
        Search data in a specific tab using SQLite database with SQL LIKE queries
        """
        results = []
        
        try:
            with get_db_context() as db:
                # Use SQL LIKE query for better matching
                search_params = {
                    'sheet_id': sheet_id, 
                    'tab_name': tab_name,
                    'search_pattern': f'%{search_query}%'
                }
                
                sql_query = """
                    SELECT * FROM sheets_data 
                    WHERE sheet_id = :sheet_id 
                    AND tab_name = :tab_name 
                    AND row_index > 0
                    AND row_data LIKE :search_pattern
                    ORDER BY row_index
                """
                
                logger.info(f"🔍 Searching with SQL: {sql_query} | Pattern: {search_query}")
                
                # Execute raw SQL query
                result = db.execute(sql_query, search_params)
                matching_rows = result.fetchall()
                
                logger.info(f"✅ Found {len(matching_rows)} matching rows for search: {search_query}")
                
                # Process matching rows
                for row_data_tuple in matching_rows:
                    try:
                        if len(row_data_tuple) >= 4:
                            row_index = row_data_tuple[2]
                            row_data_str = row_data_tuple[3]
                        else:
                            continue
                        
                        row_data = json.loads(row_data_str) if isinstance(row_data_str, str) else row_data_str
                        
                        if not row_data or not isinstance(row_data, list):
                            continue
                        
                        # Find which fields match the search query
                        for field_name, field_info in tab_analysis["field_catalog"].items():
                            x = field_info["coordinates"]["x"]
                            
                            if x < len(row_data):
                                field_value = row_data[x]
                                
                                # Check if search query matches this field value
                                if field_value and search_query.lower() in str(field_value).lower():
                                    # Build complete row data
                                    complete_row_data = {}
                                    for fname, finfo in tab_analysis["field_catalog"].items():
                                        fx = finfo["coordinates"]["x"]
                                        if fx < len(row_data):
                                            complete_row_data[fname] = row_data[fx]
                                    
                                    result_item = {
                                        "tab_name": tab_name,
                                        "field_name": field_name,
                                        "value": field_value,
                                        "coordinates": {"x": x, "y": row_index},
                                        "row_data": complete_row_data,
                                        "row_index": row_index
                                    }
                                    
                                    result_item["match_score"] = self._calculate_match_score(search_query, result_item)
                                    result_item["context"] = self._build_context(tab_analysis, result_item)
                                    
                                    results.append(result_item)
                    
                    except (json.JSONDecodeError, TypeError, IndexError) as e:
                        logger.warning(f"Error processing search result: {e}")
                        continue
                
        except Exception as e:
            logger.error(f"Error searching tab data from database: {str(e)}")
        
        return results
    
    async def get_data_by_coordinates(self, x: int, y: int, sheet_id: str = None, 
                                    tab_name: str = None) -> Dict[str, Any]:
        """
        Get data by exact X-Y coordinates using SQLite database
        
        Args:
            x: Column coordinate (0-based)
            y: Row coordinate (0-based)
            sheet_id: Sheet ID (optional)
            tab_name: Tab name (optional)
            
        Returns:
            Cell data with context
        """
        sheet_id = sheet_id or self.default_sheet_id
        
        # Get sheet analysis from database
        analysis_result = await self.analyze_sheet(sheet_id, tab_name)
        
        if not analysis_result["success"]:
            return {
                "success": False,
                "error": analysis_result["error"],
                "cell_data": None
            }
        
        # Find the tab and get cell value from database
        for tab_name_key, tab_analysis in analysis_result["tabs_analysis"].items():
            cell_data = await self._get_cell_data_from_db(sheet_id, tab_name_key, x, y, tab_analysis)
            
            if cell_data:
                return {
                    "success": True,
                    "coordinates": {"x": x, "y": y},
                    "tab_name": tab_name_key,
                    "field_name": cell_data["field_name"],
                    "value": cell_data["value"],
                    "data_type": self.analyzer._detect_data_type(cell_data["value"]),
                    "row_index": cell_data["row_index"],
                    "context": {
                        "is_header": y in tab_analysis["header_analysis"]["detected_headers"],
                        "in_data_region": any(
                            region["start_row"] <= y <= region["end_row"] 
                            for region in tab_analysis["data_regions"]
                        ),
                        "data_source": "sqlite_database"
                    }
                }
        
        return {
            "success": False,
            "error": f"No data found at coordinates ({x}, {y})",
            "cell_data": None
        }
    
    async def _get_cell_data_from_db(self, sheet_id: str, tab_name: str, x: int, y: int, 
                                   tab_analysis: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Get cell data from SQLite database by coordinates
        """
        try:
            with get_db_context() as db:
                # Get the specific row by row_index (y coordinate) using ORM
                row = db.query(SheetsData).filter(
                    SheetsData.sheet_id == sheet_id,
                    SheetsData.tab_name == tab_name,
                    SheetsData.row_index == y
                ).first()
                
                if not row:
                    return None
                
                try:
                    row_data = json.loads(row.row_data) if isinstance(row.row_data, str) else row.row_data
                    
                    if not row_data or not isinstance(row_data, list) or x >= len(row_data):
                        return None
                    
                    cell_value = row_data[x]
                    
                    # Get field name if this is a known field
                    field_name = None
                    for fname, finfo in tab_analysis["field_catalog"].items():
                        if finfo["coordinates"]["x"] == x:
                            field_name = fname
                            break
                    
                    return {
                        "value": cell_value,
                        "field_name": field_name,
                        "row_index": row.row_index
                    }
                
                except (json.JSONDecodeError, TypeError, IndexError):
                    return None
                
        except Exception as e:
            logger.error(f"Error getting cell data from database: {str(e)}")
            return None
    
    async def get_sheet_summary(self, sheet_id: str = None) -> Dict[str, Any]:
        """
        Get comprehensive summary of sheet structure
        
        Args:
            sheet_id: Sheet ID (optional)
            
        Returns:
            Complete sheet summary with all tabs
        """
        sheet_id = sheet_id or self.default_sheet_id
        
        # Get complete sheet analysis
        analysis_result = await self.analyze_sheet(sheet_id)
        
        if not analysis_result["success"]:
            return analysis_result
        
        summary = {
            "success": True,
            "sheet_info": analysis_result["sheet_info"],
            "tabs_summary": {}
        }
        
        for tab_name, tab_analysis in analysis_result["tabs_analysis"].items():
            summary["tabs_summary"][tab_name] = {
                "dimensions": tab_analysis["dimensions"],
                "fields": list(tab_analysis["field_catalog"].keys()),
                "field_count": len(tab_analysis["field_catalog"]),
                "data_regions": len(tab_analysis["data_regions"]),
                "query_hints": tab_analysis["query_hints"],
                "sample_queries": tab_analysis["query_hints"]["common_queries"][:3]
            }
        
        return summary
    
    async def get_keyword_summary(self, keywords: List[str], sheet_id: str = None, 
                                tab_name: str = None, limit: int = 50) -> Dict[str, Any]:
        """
        Get comprehensive summary based on keywords - for generalized queries
        
        Args:
            keywords: List of keywords to search for
            sheet_id: Sheet ID (optional)
            tab_name: Tab name (optional)
            limit: Maximum results per keyword
            
        Returns:
            Comprehensive summary with all related data
        """
        sheet_id = sheet_id or self.default_sheet_id
        
        try:
            # Get sheet analysis
            analysis_result = await self.analyze_sheet(sheet_id, tab_name)
            
            if not analysis_result["success"]:
                return {
                    "success": False,
                    "error": analysis_result["error"],
                    "summary": {}
                }
            
            keyword_results = {}
            all_matches = []
            
            # Search for each keyword
            for keyword in keywords:
                keyword_lower = keyword.lower()
                keyword_matches = []
                
                # Search across all tabs
                for tab_name_key, tab_analysis in analysis_result["tabs_analysis"].items():
                    # Find fields that match the keyword
                    matching_fields = []
                    for field_name, field_info in tab_analysis["field_catalog"].items():
                        if keyword_lower in field_name.lower():
                            matching_fields.append({
                                "field_name": field_name,
                                "coordinates": field_info["coordinates"],
                                "semantic_category": field_info.get("semantic_category"),
                                "data_type": field_info.get("data_type")
                            })
                    
                    # Get sample data for matching fields
                    if matching_fields:
                        tab_data = await self._get_tab_sample_data(
                            sheet_id, tab_name_key, tab_analysis, matching_fields, limit
                        )
                        
                        if tab_data:
                            keyword_matches.append({
                                "tab_name": tab_name_key,
                                "matching_fields": matching_fields,
                                "sample_data": tab_data,
                                "data_count": len(tab_data)
                            })
                
                # Also do general text search for the keyword
                search_results = await self.search_data(keyword, sheet_id, tab_name, limit)
                
                keyword_results[keyword] = {
                    "field_matches": keyword_matches,
                    "text_matches": search_results.get("results", []),
                    "total_field_matches": sum(len(match["matching_fields"]) for match in keyword_matches),
                    "total_text_matches": search_results.get("total_matches", 0)
                }
                
                all_matches.extend(keyword_matches)
            
            # Generate intelligent summary
            summary_text = self._generate_keyword_summary_text(keywords, keyword_results, analysis_result)
            
            return {
                "success": True,
                "keywords": keywords,
                "keyword_results": keyword_results,
                "summary": summary_text,
                "total_tabs_searched": len(analysis_result["tabs_analysis"]),
                "data_source": "sqlite_database"
            }
            
        except Exception as e:
            logger.error(f"Error generating keyword summary: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "summary": {}
            }
    
    async def _get_tab_sample_data(self, sheet_id: str, tab_name: str, 
                                 tab_analysis: Dict[str, Any], matching_fields: List[Dict[str, Any]], 
                                 limit: int) -> List[Dict[str, Any]]:
        """
        Get sample data from a tab for specific fields
        """
        try:
            with get_db_context() as db:
                # Get recent rows from this tab
                rows = db.query(SheetsData).filter(
                    SheetsData.sheet_id == sheet_id,
                    SheetsData.tab_name == tab_name
                ).order_by(SheetsData.row_index.desc()).limit(limit).all()
                
                sample_data = []
                
                for row in rows:
                    try:
                        row_data = json.loads(row.row_data) if isinstance(row.row_data, str) else row.row_data
                        
                        if not row_data or not isinstance(row_data, list):
                            continue
                        
                        # Extract data for matching fields
                        row_sample = {"row_index": row.row_index}
                        has_data = False
                        
                        for field_info in matching_fields:
                            x = field_info["coordinates"]["x"]
                            if x < len(row_data):
                                value = row_data[x]
                                if value and not self.analyzer._is_empty_cell(value):
                                    row_sample[field_info["field_name"]] = value
                                    has_data = True
                        
                        # Also include context fields (date, time, etc.)
                        for field_name, field_info in tab_analysis["field_catalog"].items():
                            if field_info.get("semantic_category") in ["TEMPORAL", "IDENTIFIER"]:
                                x = field_info["coordinates"]["x"]
                                if x < len(row_data) and row_data[x]:
                                    row_sample[f"context_{field_name}"] = row_data[x]
                        
                        if has_data:
                            sample_data.append(row_sample)
                    
                    except (json.JSONDecodeError, TypeError, IndexError):
                        continue
                
                return sample_data
                
        except Exception as e:
            logger.error(f"Error getting tab sample data: {str(e)}")
            return []
    
    def _generate_keyword_summary_text(self, keywords: List[str], keyword_results: Dict[str, Any], 
                                     analysis_result: Dict[str, Any]) -> str:
        """
        Generate intelligent summary text based on keyword results
        """
        summary_parts = []
        
        # Header
        sheet_name = analysis_result["sheet_info"]["sheet_name"]
        summary_parts.append(f"📊 Summary for keywords: {', '.join(keywords)} in '{sheet_name}'")
        
        # Process each keyword
        for keyword, results in keyword_results.items():
            field_matches = results["field_matches"]
            text_matches = results["text_matches"]
            
            if field_matches or text_matches:
                summary_parts.append(f"\n🔍 **{keyword.upper()}**:")
                
                # Field matches
                if field_matches:
                    total_fields = results["total_field_matches"]
                    tabs_with_fields = len(field_matches)
                    summary_parts.append(f"   📋 Found {total_fields} matching fields across {tabs_with_fields} tabs")
                    
                    # Show sample data from each tab
                    for tab_match in field_matches[:3]:  # Limit to 3 tabs
                        tab_name = tab_match["tab_name"]
                        sample_data = tab_match["sample_data"]
                        
                        if sample_data:
                            summary_parts.append(f"   📄 {tab_name}:")
                            
                            # Show recent values
                            for sample in sample_data[:3]:  # Show 3 recent entries
                                values = []
                                for key, value in sample.items():
                                    if not key.startswith("context_") and key != "row_index":
                                        values.append(f"{key}: {value}")
                                
                                if values:
                                    context_info = []
                                    for key, value in sample.items():
                                        if key.startswith("context_"):
                                            context_info.append(f"{key.replace('context_', '')}: {value}")
                                    
                                    context_str = f" ({', '.join(context_info)})" if context_info else ""
                                    summary_parts.append(f"      • {', '.join(values)}{context_str}")
                
                # Text matches
                if text_matches:
                    summary_parts.append(f"   🔎 Found {len(text_matches)} text matches")
                    
                    # Group by tab
                    tab_groups = {}
                    for match in text_matches[:10]:  # Limit to 10 matches
                        tab_name = match["tab_name"]
                        if tab_name not in tab_groups:
                            tab_groups[tab_name] = []
                        tab_groups[tab_name].append(match)
                    
                    for tab_name, matches in tab_groups.items():
                        summary_parts.append(f"      📄 {tab_name}: {len(matches)} matches")
            else:
                summary_parts.append(f"\n❌ **{keyword.upper()}**: No matches found")
        
        # Footer with suggestions
        summary_parts.append(f"\n💡 Searched across {len(analysis_result['tabs_analysis'])} tabs")
        summary_parts.append("   Try more specific keywords or date ranges for better results")
        
        return "\n".join(summary_parts)
    
    async def _get_field_values_from_db(self, sheet_id: str, tab_name: str, 
                                       field_coords: Dict[str, Any], tab_analysis: Dict[str, Any],
                                       criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Get field values from SQLite database based on coordinates and criteria using SQL queries
        """
        results = []
        
        try:
            with get_db_context() as db:
                field_x = field_coords['x']
                
                # Build SQL query with JSON functions for better matching
                if criteria and (criteria.get('date') or criteria.get('search_text')):
                    # Use SQL LIKE queries for better matching
                    search_conditions = []
                    search_params = {'sheet_id': sheet_id, 'tab_name': tab_name}
                    
                    if criteria.get('date'):
                        date_value = str(criteria['date'])
                        # Search across all JSON array elements for date
                        search_conditions.append("""
                            (row_data LIKE :date_pattern1 OR 
                             row_data LIKE :date_pattern2 OR
                             row_data LIKE :date_pattern3)
                        """)
                        search_params['date_pattern1'] = f'%"{date_value}"%'
                        search_params['date_pattern2'] = f'%{date_value}%'
                        # Handle different date formats
                        if '/' in date_value:
                            alt_date = date_value.replace('/', '.')
                            search_params['date_pattern3'] = f'%{alt_date}%'
                        elif '.' in date_value:
                            alt_date = date_value.replace('.', '/')
                            search_params['date_pattern3'] = f'%{alt_date}%'
                        else:
                            search_params['date_pattern3'] = f'%{date_value}%'
                    
                    if criteria.get('search_text'):
                        search_text = str(criteria['search_text'])
                        search_conditions.append("row_data LIKE :search_pattern")
                        search_params['search_pattern'] = f'%{search_text}%'
                    
                    # Build the complete query
                    where_clause = " AND ".join(search_conditions)
                    sql_query = f"""
                        SELECT * FROM sheets_data 
                        WHERE sheet_id = :sheet_id 
                        AND tab_name = :tab_name 
                        AND row_index > 0
                        AND ({where_clause})
                        ORDER BY row_index
                    """
                    
                    logger.info(f"🔍 Using SQL query for criteria matching: {sql_query}")
                    logger.info(f"🔍 Parameters: {search_params}")
                    
                    # Execute raw SQL query
                    result = db.execute(sql_query, search_params)
                    matching_rows = result.fetchall()
                    
                    logger.info(f"✅ Found {len(matching_rows)} matching rows with SQL query")
                    
                else:
                    # No criteria, get all rows
                    rows = db.query(SheetsData).filter(
                        SheetsData.sheet_id == sheet_id,
                        SheetsData.tab_name == tab_name,
                        SheetsData.row_index > 0
                    ).order_by(SheetsData.row_index).all()
                    matching_rows = [(row.sheet_id, row.tab_name, row.row_index, row.row_data, row.synced_at) for row in rows]
                
                # Process matching rows
                for row_data_tuple in matching_rows:
                    try:
                        if len(row_data_tuple) >= 4:
                            row_index = row_data_tuple[2]
                            row_data_str = row_data_tuple[3]
                        else:
                            continue
                        
                        # Parse row data
                        row_data = json.loads(row_data_str) if isinstance(row_data_str, str) else row_data_str
                        
                        if not row_data or not isinstance(row_data, list):
                            continue
                        
                        # Get field value at coordinates
                        if field_x < len(row_data):
                            field_value = row_data[field_x]
                            
                            # Build complete row data with field names
                            complete_row_data = {}
                            for field_name, field_info in tab_analysis['field_catalog'].items():
                                fx = field_info['coordinates']['x']
                                if fx < len(row_data):
                                    complete_row_data[field_name] = row_data[fx]
                            
                            results.append({
                                'coordinates': {'x': field_x, 'y': row_index},
                                'value': field_value,
                                'row_data': complete_row_data,
                                'row_index': row_index
                            })
                    
                    except (json.JSONDecodeError, TypeError, IndexError) as e:
                        logger.warning(f"Error processing row {row_data_tuple}: {e}")
                        continue
                
        except Exception as e:
            logger.error(f"Error getting field values from database: {str(e)}")
        
        return results
    
    async def _search_by_date_sql(self, sheet_id: str, tab_name: str, date_value: str) -> List[Dict[str, Any]]:
        """
        Search for rows containing a specific date using flexible SQL queries
        """
        results = []
        
        try:
            with get_db_context() as db:
                # Create multiple date format patterns
                date_patterns = [
                    f'%"{date_value}"%',  # Exact JSON string match
                    f'%{date_value}%',    # General substring match
                ]
                
                # Handle different date formats
                if '/' in date_value:
                    alt_date = date_value.replace('/', '.')
                    date_patterns.extend([f'%"{alt_date}"%', f'%{alt_date}%'])
                elif '.' in date_value:
                    alt_date = date_value.replace('.', '/')
                    date_patterns.extend([f'%"{alt_date}"%', f'%{alt_date}%'])
                
                # Build OR conditions for all patterns
                or_conditions = []
                search_params = {'sheet_id': sheet_id, 'tab_name': tab_name}
                
                for i, pattern in enumerate(date_patterns):
                    param_name = f'pattern_{i}'
                    or_conditions.append(f'row_data LIKE :{param_name}')
                    search_params[param_name] = pattern
                
                sql_query = f"""
                    SELECT * FROM sheets_data 
                    WHERE sheet_id = :sheet_id 
                    AND tab_name = :tab_name 
                    AND row_index > 0
                    AND ({' OR '.join(or_conditions)})
                    ORDER BY row_index
                """
                
                logger.info(f"🔍 Date search SQL: {sql_query}")
                logger.info(f"🔍 Date patterns: {date_patterns}")
                
                # Execute query
                result = db.execute(sql_query, search_params)
                matching_rows = result.fetchall()
                
                logger.info(f"✅ Found {len(matching_rows)} rows matching date: {date_value}")
                
                # Convert to list of tuples for processing
                for row_tuple in matching_rows:
                    results.append(row_tuple)
                
        except Exception as e:
            logger.error(f"Error in date-based SQL search: {str(e)}")
        
        return results
    
    def _matches_criteria_db(self, row_data: List[Any], tab_analysis: Dict[str, Any], 
                           criteria: Dict[str, Any]) -> bool:
        """
        Check if row matches search criteria using database row data
        """
        if not criteria:
            return True
        
        # Date criteria - Search in ALL fields, not just date fields
        if 'date' in criteria:
            date_value = str(criteria['date']).lower()
            
            # First try date fields if available
            date_fields = tab_analysis.get('query_hints', {}).get('date_fields', [])
            if date_fields:
                for date_field in date_fields:
                    field_info = tab_analysis['field_catalog'].get(date_field)
                    if field_info:
                        x = field_info['coordinates']['x']
                        if x < len(row_data) and row_data[x]:
                            if date_value in str(row_data[x]).lower():
                                return True
            
            # If not found in date fields, search ALL fields for the date
            for value in row_data:
                if value and date_value in str(value).lower():
                    return True
        
        # Text search criteria - Search across all values in the row
        if 'search_text' in criteria:
            search_text = str(criteria['search_text']).lower()
            for value in row_data:
                if value and search_text in str(value).lower():
                    return True
        
        return False

    # Helper methods
    def _parse_search_query(self, search_query: str) -> Dict[str, Any]:
        """Parse search query into criteria"""
        criteria = {}
        
        # Look for date patterns
        import re
        date_pattern = r'\d{1,2}[./\-]\d{1,2}[./\-]\d{2,4}'
        date_match = re.search(date_pattern, search_query)
        if date_match:
            criteria['date'] = date_match.group()
        
        # General text search
        criteria['search_text'] = search_query
        
        return criteria
    
    def _calculate_match_score(self, search_query: str, result: Dict[str, Any]) -> float:
        """Calculate match score for search result"""
        score = 0.0
        search_lower = search_query.lower()
        
        # Exact value match
        if result["value"] and search_lower in str(result["value"]).lower():
            score += 1.0
        
        # Field name match
        if result["field_name"] and search_lower in result["field_name"].lower():
            score += 0.5
        
        # Row data match
        for field_value in result["row_data"].values():
            if field_value and search_lower in str(field_value).lower():
                score += 0.2
                break
        
        return score
    
    def _build_context(self, tab_analysis: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
        """Build context information for result"""
        y = result["coordinates"]["y"]
        
        context = {
            "is_header": y in tab_analysis["header_analysis"]["detected_headers"],
            "data_region": None,
            "row_type": "data"
        }
        
        # Determine data region
        for region in tab_analysis["data_regions"]:
            if region["start_row"] <= y <= region["end_row"]:
                context["data_region"] = region["type"]
                break
        
        # Determine row type
        if context["is_header"]:
            context["row_type"] = "header"
        elif not context["data_region"]:
            context["row_type"] = "unknown"
        
        return context


# Global instance
_universal_data_service = None

def get_universal_data_service() -> UniversalDataService:
    """Get the global universal data service instance"""
    global _universal_data_service
    if _universal_data_service is None:
        _universal_data_service = UniversalDataService()
    return _universal_data_service