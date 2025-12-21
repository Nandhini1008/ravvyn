"""
Universal Sheet Analyzer - X-Y Axis Model for Any Sheet Structure
Provides generalized data retrieval using coordinate-based approach
"""

import logging
import re
from typing import Dict, List, Any, Optional, Tuple, Set
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class UniversalSheetAnalyzer:
    """
    Universal sheet analyzer using X-Y coordinate system
    Works with any sheet structure by analyzing data patterns
    """
    
    def __init__(self):
        """Initialize universal analyzer"""
        
        # Data type patterns for automatic detection
        self.data_patterns = {
            'DATE': [
                r'\d{1,2}[./\-]\d{1,2}[./\-]\d{2,4}',  # DD.MM.YY, DD/MM/YYYY
                r'\d{4}[./\-]\d{1,2}[./\-]\d{1,2}',    # YYYY-MM-DD
                r'\d{1,2}\s+\w+\s+\d{4}',              # DD Month YYYY
            ],
            'TIME': [
                r'\d{1,2}:\d{2}(?::\d{2})?',           # HH:MM or HH:MM:SS
                r'\d{1,2}\.\d{2}',                     # H.MM format
            ],
            'NUMBER': [
                r'^\d+\.?\d*$',                        # Integer or decimal
                r'^\d{1,3}(?:,\d{3})*(?:\.\d+)?$',     # Number with commas
            ],
            'CURRENCY': [
                r'[$€£¥]\s*\d+(?:,\d{3})*(?:\.\d{2})?',  # Currency symbols
                r'\d+(?:,\d{3})*(?:\.\d{2})?\s*[$€£¥]',  # Currency at end
            ],
            'PERCENTAGE': [
                r'\d+(?:\.\d+)?%',                     # Percentage
            ],
            'BOOLEAN': [
                r'^(?:yes|no|true|false|on|off|active|inactive)$',  # Boolean values
            ],
            'EMPTY': [
                r'^$',                                 # Empty cell
                r'^\s*$',                             # Whitespace only
                r'^-$',                               # Dash placeholder
            ]
        }
        
        # Semantic field categories for intelligent grouping
        self.semantic_categories = {
            'TEMPORAL': ['date', 'time', 'timestamp', 'created', 'updated', 'start', 'end', 'duration'],
            'IDENTIFIER': ['id', 'code', 'number', 'ref', 'reference', 'serial'],
            'MEASUREMENT': ['level', 'pressure', 'temperature', 'flow', 'rate', 'volume', 'weight'],
            'FINANCIAL': ['cost', 'price', 'amount', 'total', 'sum', 'budget', 'expense'],
            'STATUS': ['status', 'state', 'condition', 'active', 'enabled', 'running'],
            'LOCATION': ['tank', 'feed', 'supply', 'source', 'destination', 'location'],
            'QUALITY': ['tds', 'purity', 'quality', 'grade', 'efficiency', 'recovery'],
            'OPERATIONAL': ['run', 'operation', 'process', 'cycle', 'batch'],
            'DESCRIPTIVE': ['name', 'description', 'remarks', 'notes', 'comments']
        }
    
    def analyze_sheet_structure(self, sheet_data: List[List[Any]], 
                              sheet_name: str = "") -> Dict[str, Any]:
        """
        Analyze any sheet structure using X-Y coordinate system
        
        Args:
            sheet_data: Raw sheet data as list of rows
            sheet_name: Optional sheet name for context
            
        Returns:
            Complete analysis with coordinate mapping
        """
        if not sheet_data:
            return self._empty_analysis()
        
        analysis = {
            'sheet_name': sheet_name,
            'dimensions': {
                'rows': len(sheet_data),
                'max_columns': max(len(row) for row in sheet_data if row),
                'data_density': self._calculate_data_density(sheet_data)
            },
            'coordinate_map': {},
            'header_analysis': {},
            'data_regions': [],
            'field_catalog': {},
            'query_hints': {}
        }
        
        # Step 1: Create coordinate map (X-Y grid)
        analysis['coordinate_map'] = self._create_coordinate_map(sheet_data)
        
        # Step 2: Detect headers and data regions
        analysis['header_analysis'] = self._analyze_headers(sheet_data)
        analysis['data_regions'] = self._identify_data_regions(sheet_data, analysis['header_analysis'])
        
        # Step 3: Build field catalog with semantic understanding
        analysis['field_catalog'] = self._build_field_catalog(
            sheet_data, analysis['header_analysis'], analysis['data_regions']
        )
        
        # Step 4: Generate query optimization hints
        analysis['query_hints'] = self._generate_query_hints(analysis)
        
        logger.info(f"Analyzed sheet '{sheet_name}': {analysis['dimensions']['rows']} rows, "
                   f"{len(analysis['field_catalog'])} fields, "
                   f"{len(analysis['data_regions'])} data regions")
        
        return analysis
    
    def _create_coordinate_map(self, sheet_data: List[List[Any]]) -> Dict[str, Any]:
        """Create X-Y coordinate mapping of all cells"""
        coordinate_map = {
            'cells': {},
            'row_profiles': {},
            'column_profiles': {}
        }
        
        # Map each cell with coordinates
        for y, row in enumerate(sheet_data):
            coordinate_map['row_profiles'][y] = {
                'length': len(row),
                'empty_cells': 0,
                'data_types': {},
                'semantic_hints': []
            }
            
            for x, cell in enumerate(row):
                # Store cell with X-Y coordinates
                coordinate_map['cells'][f"{x},{y}"] = {
                    'value': cell,
                    'x': x,
                    'y': y,
                    'data_type': self._detect_data_type(cell),
                    'is_empty': self._is_empty_cell(cell)
                }
                
                # Update row profile
                if self._is_empty_cell(cell):
                    coordinate_map['row_profiles'][y]['empty_cells'] += 1
                else:
                    data_type = self._detect_data_type(cell)
                    coordinate_map['row_profiles'][y]['data_types'][data_type] = \
                        coordinate_map['row_profiles'][y]['data_types'].get(data_type, 0) + 1
        
        # Analyze column profiles
        max_cols = max(len(row) for row in sheet_data if row)
        for x in range(max_cols):
            coordinate_map['column_profiles'][x] = {
                'values': [],
                'data_types': {},
                'empty_count': 0,
                'semantic_category': None
            }
            
            for y, row in enumerate(sheet_data):
                if x < len(row):
                    cell = row[x]
                    coordinate_map['column_profiles'][x]['values'].append(cell)
                    
                    if self._is_empty_cell(cell):
                        coordinate_map['column_profiles'][x]['empty_count'] += 1
                    else:
                        data_type = self._detect_data_type(cell)
                        coordinate_map['column_profiles'][x]['data_types'][data_type] = \
                            coordinate_map['column_profiles'][x]['data_types'].get(data_type, 0) + 1
            
            # Determine semantic category for column
            coordinate_map['column_profiles'][x]['semantic_category'] = \
                self._determine_semantic_category(coordinate_map['column_profiles'][x]['values'])
        
        return coordinate_map
    
    def _analyze_headers(self, sheet_data: List[List[Any]]) -> Dict[str, Any]:
        """Analyze potential header rows using multiple strategies"""
        header_analysis = {
            'detected_headers': [],
            'confidence_scores': {},
            'header_coordinates': [],
            'field_names': {}
        }
        
        # Strategy 1: Text density analysis
        for y, row in enumerate(sheet_data[:5]):  # Check first 5 rows
            if not row:
                continue
            
            text_count = sum(1 for cell in row if self._is_text_cell(cell))
            total_cells = len([cell for cell in row if not self._is_empty_cell(cell)])
            
            if total_cells > 0:
                text_ratio = text_count / total_cells
                confidence = text_ratio * 0.7  # Base confidence from text ratio
                
                # Boost confidence for typical header patterns
                if any(self._looks_like_header(str(cell)) for cell in row):
                    confidence += 0.2
                
                # Reduce confidence if too many empty cells
                empty_ratio = sum(1 for cell in row if self._is_empty_cell(cell)) / len(row)
                confidence -= empty_ratio * 0.3
                
                if confidence > 0.5:
                    header_analysis['detected_headers'].append(y)
                    header_analysis['confidence_scores'][y] = confidence
                    header_analysis['field_names'][y] = [
                        self._normalize_field_name(str(cell)) for cell in row
                    ]
        
        # Strategy 2: Pattern consistency analysis
        if len(sheet_data) > 1:
            for y in range(min(3, len(sheet_data) - 1)):
                current_row = sheet_data[y]
                next_row = sheet_data[y + 1]
                
                # Check if current row is text and next row is data
                if (self._is_mostly_text(current_row) and 
                    self._is_mostly_data(next_row)):
                    
                    if y not in header_analysis['detected_headers']:
                        header_analysis['detected_headers'].append(y)
                        header_analysis['confidence_scores'][y] = 0.8
                        header_analysis['field_names'][y] = [
                            self._normalize_field_name(str(cell)) for cell in current_row
                        ]
        
        # Sort headers by confidence
        header_analysis['detected_headers'].sort(
            key=lambda y: header_analysis['confidence_scores'].get(y, 0), 
            reverse=True
        )
        
        return header_analysis
    
    def _identify_data_regions(self, sheet_data: List[List[Any]], 
                             header_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify distinct data regions in the sheet"""
        data_regions = []
        
        # Find primary data region
        primary_header_y = header_analysis['detected_headers'][0] if header_analysis['detected_headers'] else None
        
        if primary_header_y is not None:
            # Primary region starts after the header
            start_y = primary_header_y + 1
            end_y = len(sheet_data) - 1
            
            # Find actual data boundaries
            while start_y <= end_y and self._is_empty_row(sheet_data[start_y]):
                start_y += 1
            
            while end_y >= start_y and self._is_empty_row(sheet_data[end_y]):
                end_y -= 1
            
            if start_y <= end_y:
                data_regions.append({
                    'type': 'primary_data',
                    'header_row': primary_header_y,
                    'start_row': start_y,
                    'end_row': end_y,
                    'columns': self._analyze_region_columns(sheet_data, start_y, end_y),
                    'row_count': end_y - start_y + 1
                })
        
        # Look for additional data regions (tables within tables)
        self._find_secondary_regions(sheet_data, data_regions)
        
        return data_regions
    
    def _build_field_catalog(self, sheet_data: List[List[Any]], 
                           header_analysis: Dict[str, Any], 
                           data_regions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build comprehensive field catalog with semantic understanding"""
        field_catalog = {}
        
        for region in data_regions:
            header_row = region.get('header_row')
            if header_row is not None and header_row in header_analysis['field_names']:
                headers = header_analysis['field_names'][header_row]
                
                for x, field_name in enumerate(headers):
                    if field_name and field_name.strip():
                        # Analyze column data
                        column_data = []
                        for y in range(region['start_row'], region['end_row'] + 1):
                            if y < len(sheet_data) and x < len(sheet_data[y]):
                                column_data.append(sheet_data[y][x])
                        
                        field_catalog[field_name] = {
                            'coordinates': {'x': x, 'header_y': header_row},
                            'data_region': region['type'],
                            'data_type': self._analyze_column_data_type(column_data),
                            'semantic_category': self._determine_semantic_category([field_name] + column_data),
                            'sample_values': [v for v in column_data[:5] if not self._is_empty_cell(v)],
                            'value_count': len([v for v in column_data if not self._is_empty_cell(v)]),
                            'unique_values': len(set(str(v) for v in column_data if not self._is_empty_cell(v))),
                            'aliases': self._generate_field_aliases(field_name),
                            'query_patterns': self._generate_query_patterns(field_name)
                        }
        
        return field_catalog
    
    def _generate_query_hints(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Generate intelligent query optimization hints"""
        hints = {
            'date_fields': [],
            'numeric_fields': [],
            'key_fields': [],
            'searchable_fields': [],
            'latest_data_strategy': {},
            'common_queries': []
        }
        
        for field_name, field_info in analysis['field_catalog'].items():
            # Categorize fields for query optimization
            if field_info['semantic_category'] == 'TEMPORAL':
                hints['date_fields'].append(field_name)
            
            if field_info['data_type'] in ['NUMBER', 'CURRENCY']:
                hints['numeric_fields'].append(field_name)
            
            if field_info['semantic_category'] == 'IDENTIFIER':
                hints['key_fields'].append(field_name)
            
            if field_info['value_count'] > 0:
                hints['searchable_fields'].append(field_name)
        
        # Strategy for finding latest data
        if hints['date_fields']:
            primary_date_field = hints['date_fields'][0]
            hints['latest_data_strategy'] = {
                'method': 'date_based',
                'primary_field': primary_date_field,
                'coordinates': analysis['field_catalog'][primary_date_field]['coordinates']
            }
        else:
            hints['latest_data_strategy'] = {
                'method': 'last_row',
                'fallback': True
            }
        
        # Generate common query patterns
        hints['common_queries'] = self._generate_common_queries(analysis)
        
        return hints
    
    def get_cell_value(self, analysis: Dict[str, Any], x: int, y: int) -> Any:
        """Get cell value using X-Y coordinates"""
        coordinate_key = f"{x},{y}"
        if coordinate_key in analysis['coordinate_map']['cells']:
            return analysis['coordinate_map']['cells'][coordinate_key]['value']
        return None
    
    def find_field_coordinates(self, analysis: Dict[str, Any], field_query: str) -> Optional[Dict[str, Any]]:
        """Find field coordinates using flexible matching"""
        field_query_lower = field_query.lower().strip()
        
        # Direct match
        for field_name, field_info in analysis['field_catalog'].items():
            if field_name.lower() == field_query_lower:
                return field_info['coordinates']
        
        # Alias match
        for field_name, field_info in analysis['field_catalog'].items():
            if field_query_lower in [alias.lower() for alias in field_info.get('aliases', [])]:
                return field_info['coordinates']
        
        # Fuzzy match
        for field_name, field_info in analysis['field_catalog'].items():
            if self._fuzzy_field_match(field_query_lower, field_name.lower()):
                return field_info['coordinates']
        
        return None
    
    def get_field_values_by_criteria(self, analysis: Dict[str, Any], 
                                   field_name: str, criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get field values based on criteria (e.g., date range, conditions)"""
        field_coords = self.find_field_coordinates(analysis, field_name)
        if not field_coords:
            return []
        
        results = []
        x = field_coords['x']
        
        # Find data region
        data_region = None
        for region in analysis['data_regions']:
            if region.get('header_row') == field_coords.get('header_y'):
                data_region = region
                break
        
        if not data_region:
            return []
        
        # Extract values based on criteria
        for y in range(data_region['start_row'], data_region['end_row'] + 1):
            cell_value = self.get_cell_value(analysis, x, y)
            
            # Apply criteria filters
            if self._matches_criteria(analysis, y, criteria):
                row_data = {}
                # Get all fields for this row
                for fname, finfo in analysis['field_catalog'].items():
                    if finfo['data_region'] == data_region['type']:
                        fx = finfo['coordinates']['x']
                        row_data[fname] = self.get_cell_value(analysis, fx, y)
                
                results.append({
                    'coordinates': {'x': x, 'y': y},
                    'value': cell_value,
                    'row_data': row_data
                })
        
        return results
    
    def get_latest_data_row(self, analysis: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Get the most recent data row using intelligent strategy"""
        strategy = analysis['query_hints']['latest_data_strategy']
        
        if strategy['method'] == 'date_based' and not strategy.get('fallback'):
            # Use date field to find latest
            date_field = strategy['primary_field']
            date_coords = analysis['field_catalog'][date_field]['coordinates']
            
            latest_date = None
            latest_y = None
            
            for region in analysis['data_regions']:
                if region.get('header_row') == date_coords.get('header_y'):
                    for y in range(region['start_row'], region['end_row'] + 1):
                        date_value = self.get_cell_value(analysis, date_coords['x'], y)
                        if date_value and not self._is_empty_cell(date_value):
                            # Parse date and compare
                            parsed_date = self._parse_date(str(date_value))
                            if parsed_date and (latest_date is None or parsed_date > latest_date):
                                latest_date = parsed_date
                                latest_y = y
            
            if latest_y is not None:
                return self._get_row_data(analysis, latest_y)
        
        # Fallback: use last non-empty row
        for region in reversed(analysis['data_regions']):
            for y in range(region['end_row'], region['start_row'] - 1, -1):
                if not self._is_empty_row_in_region(analysis, y, region):
                    return self._get_row_data(analysis, y)
        
        return None
    
    # Helper methods
    def _empty_analysis(self) -> Dict[str, Any]:
        """Return empty analysis structure"""
        return {
            'sheet_name': '',
            'dimensions': {'rows': 0, 'max_columns': 0, 'data_density': 0},
            'coordinate_map': {'cells': {}, 'row_profiles': {}, 'column_profiles': {}},
            'header_analysis': {'detected_headers': [], 'confidence_scores': {}, 'field_names': {}},
            'data_regions': [],
            'field_catalog': {},
            'query_hints': {}
        }
    
    def _calculate_data_density(self, sheet_data: List[List[Any]]) -> float:
        """Calculate data density (non-empty cells / total cells)"""
        total_cells = sum(len(row) for row in sheet_data)
        if total_cells == 0:
            return 0.0
        
        non_empty_cells = sum(
            1 for row in sheet_data 
            for cell in row 
            if not self._is_empty_cell(cell)
        )
        
        return non_empty_cells / total_cells
    
    def _detect_data_type(self, cell: Any) -> str:
        """Detect data type of a cell"""
        if self._is_empty_cell(cell):
            return 'EMPTY'
        
        cell_str = str(cell).strip()
        
        for data_type, patterns in self.data_patterns.items():
            for pattern in patterns:
                if re.match(pattern, cell_str, re.IGNORECASE):
                    return data_type
        
        return 'TEXT'
    
    def _is_empty_cell(self, cell: Any) -> bool:
        """Check if cell is empty"""
        if cell is None:
            return True
        
        cell_str = str(cell).strip()
        return cell_str == '' or cell_str == '-' or cell_str.lower() in ['null', 'none', 'n/a']
    
    def _is_text_cell(self, cell: Any) -> bool:
        """Check if cell contains text (not number/date)"""
        if self._is_empty_cell(cell):
            return False
        
        data_type = self._detect_data_type(cell)
        return data_type == 'TEXT'
    
    def _looks_like_header(self, text: str) -> bool:
        """Check if text looks like a header"""
        text = text.strip().upper()
        
        # Common header patterns
        header_indicators = [
            'NAME', 'ID', 'DATE', 'TIME', 'AMOUNT', 'TOTAL', 'LEVEL', 'STATUS',
            'TYPE', 'DESCRIPTION', 'VALUE', 'QUANTITY', 'PRICE', 'COST'
        ]
        
        return any(indicator in text for indicator in header_indicators)
    
    def _normalize_field_name(self, text: str) -> str:
        """Normalize field name"""
        if not text or self._is_empty_cell(text):
            return ""
        
        # Clean and normalize
        normalized = str(text).strip().upper()
        normalized = re.sub(r'[^\w\s&]', '', normalized)
        normalized = re.sub(r'\s+', '_', normalized)
        
        return normalized
    
    def _is_mostly_text(self, row: List[Any]) -> bool:
        """Check if row is mostly text"""
        non_empty = [cell for cell in row if not self._is_empty_cell(cell)]
        if not non_empty:
            return False
        
        text_count = sum(1 for cell in non_empty if self._is_text_cell(cell))
        return text_count / len(non_empty) > 0.6
    
    def _is_mostly_data(self, row: List[Any]) -> bool:
        """Check if row is mostly data (numbers/dates)"""
        non_empty = [cell for cell in row if not self._is_empty_cell(cell)]
        if not non_empty:
            return False
        
        data_count = sum(1 for cell in non_empty if not self._is_text_cell(cell))
        return data_count / len(non_empty) > 0.4
    
    def _is_empty_row(self, row: List[Any]) -> bool:
        """Check if row is empty"""
        return all(self._is_empty_cell(cell) for cell in row)
    
    def _analyze_region_columns(self, sheet_data: List[List[Any]], 
                              start_y: int, end_y: int) -> Dict[str, Any]:
        """Analyze columns in a data region"""
        max_cols = max(len(sheet_data[y]) for y in range(start_y, end_y + 1) if y < len(sheet_data))
        
        columns = {}
        for x in range(max_cols):
            column_values = []
            for y in range(start_y, end_y + 1):
                if y < len(sheet_data) and x < len(sheet_data[y]):
                    column_values.append(sheet_data[y][x])
            
            columns[x] = {
                'data_type': self._analyze_column_data_type(column_values),
                'non_empty_count': len([v for v in column_values if not self._is_empty_cell(v)])
            }
        
        return columns
    
    def _find_secondary_regions(self, sheet_data: List[List[Any]], 
                              data_regions: List[Dict[str, Any]]) -> None:
        """Find additional data regions (placeholder for complex sheets)"""
        # This can be extended for sheets with multiple tables
        pass
    
    def _analyze_column_data_type(self, column_data: List[Any]) -> str:
        """Analyze predominant data type in column"""
        type_counts = {}
        
        for cell in column_data:
            if not self._is_empty_cell(cell):
                data_type = self._detect_data_type(cell)
                type_counts[data_type] = type_counts.get(data_type, 0) + 1
        
        if not type_counts:
            return 'EMPTY'
        
        return max(type_counts.items(), key=lambda x: x[1])[0]
    
    def _determine_semantic_category(self, values: List[Any]) -> Optional[str]:
        """Determine semantic category from values"""
        text_values = [str(v).lower() for v in values if not self._is_empty_cell(v)]
        
        for category, keywords in self.semantic_categories.items():
            if any(keyword in ' '.join(text_values) for keyword in keywords):
                return category
        
        return None
    
    def _generate_field_aliases(self, field_name: str) -> List[str]:
        """Generate aliases for field name"""
        aliases = [field_name.lower()]
        
        # Add variations
        normalized = field_name.replace('_', ' ').lower()
        aliases.append(normalized)
        
        # Add common abbreviations
        abbreviations = {
            'temperature': ['temp'],
            'pressure': ['press'],
            'level': ['lvl'],
            'amount': ['amt'],
            'total': ['tot'],
            'quantity': ['qty']
        }
        
        for full, abbrevs in abbreviations.items():
            if full in normalized:
                for abbrev in abbrevs:
                    aliases.append(normalized.replace(full, abbrev))
        
        return list(set(aliases))
    
    def _generate_query_patterns(self, field_name: str) -> List[str]:
        """Generate query patterns for field"""
        patterns = []
        
        field_lower = field_name.lower()
        
        # Basic patterns
        patterns.extend([
            f"what is the {field_lower}",
            f"show me {field_lower}",
            f"get {field_lower}",
            f"{field_lower} value"
        ])
        
        return patterns
    
    def _generate_common_queries(self, analysis: Dict[str, Any]) -> List[str]:
        """Generate common query patterns for this sheet"""
        queries = []
        
        # Get hints that were already generated
        hints = analysis.get('query_hints', {})
        
        # Date-based queries
        date_fields = hints.get('date_fields', [])
        if date_fields:
            date_field = date_fields[0]
            queries.extend([
                f"show me data for [date]",
                f"what is the latest data",
                f"get data from {date_field.lower()}"
            ])
        
        # Numeric field queries
        numeric_fields = hints.get('numeric_fields', [])
        for field in numeric_fields[:3]:
            queries.append(f"what is the {field.lower()}")
        
        return queries
    
    def _fuzzy_field_match(self, query: str, field_name: str) -> bool:
        """Fuzzy match field names"""
        query_words = set(re.findall(r'\w+', query.lower()))
        field_words = set(re.findall(r'\w+', field_name.lower()))
        
        if not query_words or not field_words:
            return False
        
        overlap = len(query_words & field_words)
        return overlap >= min(len(query_words), len(field_words)) * 0.5
    
    def _matches_criteria(self, analysis: Dict[str, Any], y: int, criteria: Dict[str, Any]) -> bool:
        """Check if row matches search criteria"""
        if not criteria:
            return True
        
        # Date criteria
        if 'date' in criteria:
            date_fields = analysis['query_hints']['date_fields']
            if date_fields:
                date_field = date_fields[0]
                date_coords = analysis['field_catalog'][date_field]['coordinates']
                cell_value = self.get_cell_value(analysis, date_coords['x'], y)
                
                if cell_value and str(criteria['date']).lower() in str(cell_value).lower():
                    return True
        
        # Text search criteria
        if 'search_text' in criteria:
            # Search across all fields in the row
            for field_name, field_info in analysis['field_catalog'].items():
                x = field_info['coordinates']['x']
                cell_value = self.get_cell_value(analysis, x, y)
                
                if cell_value and str(criteria['search_text']).lower() in str(cell_value).lower():
                    return True
        
        return False
    
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
    
    def _get_row_data(self, analysis: Dict[str, Any], y: int) -> Dict[str, Any]:
        """Get complete row data with field mapping"""
        row_data = {'coordinates': {'y': y}, 'fields': {}}
        
        for field_name, field_info in analysis['field_catalog'].items():
            x = field_info['coordinates']['x']
            value = self.get_cell_value(analysis, x, y)
            row_data['fields'][field_name] = value
        
        return row_data
    
    def _is_empty_row_in_region(self, analysis: Dict[str, Any], y: int, region: Dict[str, Any]) -> bool:
        """Check if row is empty within a specific region"""
        for field_name, field_info in analysis['field_catalog'].items():
            if field_info['data_region'] == region['type']:
                x = field_info['coordinates']['x']
                value = self.get_cell_value(analysis, x, y)
                if not self._is_empty_cell(value):
                    return False
        return True


# Global instance
_universal_analyzer = None

def get_universal_analyzer() -> UniversalSheetAnalyzer:
    """Get the global universal analyzer instance"""
    global _universal_analyzer
    if _universal_analyzer is None:
        _universal_analyzer = UniversalSheetAnalyzer()
    return _universal_analyzer