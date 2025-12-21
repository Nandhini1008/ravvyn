#!/usr/bin/env python3
"""
Direct check for 12-12-2025 data in the database
"""

import sys
import os
import json

# Add the backend directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def check_date_data():
    """Check if 12-12-2025 data exists in database"""
    try:
        from services.database import get_db_context, SheetsData
        
        print("🔍 Checking for 12-12-2025 data in database...")
        print("=" * 60)
        
        with get_db_context() as db:
            # Get all rows
            all_rows = db.query(SheetsData).all()
            print(f"📊 Total rows in database: {len(all_rows)}")
            
            # Search for 12-12-2025 patterns
            date_patterns = [
                '12-12-2025', '12.12.2025', '12/12/2025',
                '12-12-25', '12.12.25', '12/12/25',
                '12-12', '12.12', '12/12'
            ]
            
            found_rows = []
            
            for row in all_rows:
                try:
                    row_data = json.loads(row.row_data) if isinstance(row.row_data, str) else row.row_data
                    if not row_data:
                        continue
                    
                    # Convert all cells to string and join
                    row_text = ' '.join(str(cell) for cell in row_data if cell)
                    
                    # Check for any date pattern
                    for pattern in date_patterns:
                        if pattern in row_text:
                            found_rows.append({
                                'sheet_id': row.sheet_id,
                                'tab_name': row.tab_name,
                                'row_index': row.row_index,
                                'row_data': row_data,
                                'matched_pattern': pattern,
                                'row_text': row_text
                            })
                            break
                            
                except Exception as e:
                    continue
            
            print(f"✅ Found {len(found_rows)} rows containing 12-12-2025 data")
            
            if found_rows:
                print("\n📋 Details of found rows:")
                print("-" * 40)
                
                # Group by tab
                by_tab = {}
                for row in found_rows:
                    tab = row['tab_name']
                    if tab not in by_tab:
                        by_tab[tab] = []
                    by_tab[tab].append(row)
                
                for tab_name, tab_rows in by_tab.items():
                    print(f"\n📄 Tab: {tab_name} ({len(tab_rows)} rows)")
                    
                    for i, row in enumerate(tab_rows[:5], 1):  # Show first 5 rows per tab
                        print(f"  {i}. Row {row['row_index']}: Pattern '{row['matched_pattern']}'")
                        print(f"     Data: {row['row_data'][:8]}...")  # First 8 cells
                        print(f"     Full text: {row['row_text'][:100]}...")
                        print()
                    
                    if len(tab_rows) > 5:
                        print(f"     ... and {len(tab_rows) - 5} more rows")
                        print()
            else:
                print("❌ No rows found with 12-12-2025 data")
                
                # Let's check what dates ARE in the database
                print("\n🔍 Checking what dates exist in database...")
                date_samples = set()
                
                for row in all_rows[:1000]:  # Check first 1000 rows
                    try:
                        row_data = json.loads(row.row_data) if isinstance(row.row_data, str) else row.row_data
                        if not row_data:
                            continue
                        
                        for cell in row_data:
                            if cell:
                                cell_str = str(cell)
                                # Look for date-like patterns
                                import re
                                if re.search(r'\d{1,2}[./\-]\d{1,2}[./\-]\d{2,4}', cell_str):
                                    date_samples.add(cell_str)
                                    if len(date_samples) >= 20:
                                        break
                        
                        if len(date_samples) >= 20:
                            break
                            
                    except:
                        continue
                
                print(f"📅 Sample dates found in database:")
                for date_sample in sorted(list(date_samples)[:15]):
                    print(f"  - {date_sample}")
        
        print("\n" + "=" * 60)
        print("✅ Database check completed!")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_date_data()