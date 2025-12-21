#!/usr/bin/env python3
"""
Get Sheet Tabs - Find available tabs/worksheets in your Google Sheet
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add the backend directory to the path
sys.path.insert(0, str(Path(__file__).parent))

from services.sheets import SheetsService

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Your specific sheet ID
YOUR_SHEET_ID = "1MtjJyKiDR7COszXxZF-wYR-UF5LcwYSpTfr_Aa_PEt8"


async def get_sheet_tabs(sheet_id):
    """Get all available tabs/worksheets in a Google Sheet"""
    try:
        sheets_service = SheetsService()
        
        logger.info(f"🔍 Getting tabs for sheet: {sheet_id}")
        
        # Get spreadsheet metadata
        def _get_spreadsheet():
            return sheets_service.sheets_service.spreadsheets().get(
                spreadsheetId=sheet_id
            ).execute()
        
        spreadsheet = await sheets_service._retry_request(_get_spreadsheet)
        
        # Extract sheet information
        sheets = spreadsheet.get('sheets', [])
        
        logger.info(f"📋 Found {len(sheets)} tabs in the spreadsheet:")
        
        tab_info = []
        for i, sheet in enumerate(sheets):
            properties = sheet.get('properties', {})
            tab_name = properties.get('title', f'Sheet{i+1}')
            sheet_id_num = properties.get('sheetId', 0)
            sheet_type = properties.get('sheetType', 'GRID')
            
            tab_info.append({
                'name': tab_name,
                'id': sheet_id_num,
                'type': sheet_type,
                'index': properties.get('index', i)
            })
            
            logger.info(f"  {i+1}. '{tab_name}' (ID: {sheet_id_num}, Type: {sheet_type})")
        
        # Get spreadsheet title
        spreadsheet_title = spreadsheet.get('properties', {}).get('title', 'Unknown')
        logger.info(f"📊 Spreadsheet title: '{spreadsheet_title}'")
        
        return tab_info
        
    except Exception as e:
        logger.error(f"❌ Error getting sheet tabs: {str(e)}")
        return []


async def test_read_with_correct_tab(sheet_id, tab_name):
    """Test reading data with the correct tab name"""
    try:
        sheets_service = SheetsService()
        
        logger.info(f"📖 Testing read with tab: '{tab_name}'")
        
        # Try to read data
        data = await sheets_service.read_sheet(sheet_id, tab_name)
        
        logger.info(f"✅ Successfully read data from '{tab_name}'!")
        logger.info(f"   📊 Total rows: {len(data)}")
        
        if data:
            logger.info("   📋 Data preview:")
            for i, row in enumerate(data[:3]):  # Show first 3 rows
                logger.info(f"      Row {i+1}: {row}")
            if len(data) > 3:
                logger.info(f"      ... and {len(data) - 3} more rows")
        
        return data
        
    except Exception as e:
        logger.error(f"❌ Error reading from tab '{tab_name}': {str(e)}")
        return None


async def main():
    """Main function to get tabs and test reading"""
    logger.info("🔍 Google Sheet Tab Inspector")
    logger.info(f"Sheet ID: {YOUR_SHEET_ID}")
    logger.info("=" * 80)
    
    try:
        # Get available tabs
        tabs = await get_sheet_tabs(YOUR_SHEET_ID)
        
        if not tabs:
            logger.error("❌ No tabs found or error occurred")
            return False
        
        # Try to read from each tab
        logger.info(f"\n📖 Testing data reading from each tab:")
        logger.info("-" * 50)
        
        successful_tabs = []
        
        for tab in tabs:
            tab_name = tab['name']
            logger.info(f"\n🧪 Testing tab: '{tab_name}'")
            
            data = await test_read_with_correct_tab(YOUR_SHEET_ID, tab_name)
            
            if data is not None:
                successful_tabs.append({
                    'name': tab_name,
                    'rows': len(data),
                    'data': data
                })
                logger.info(f"✅ '{tab_name}': {len(data)} rows")
            else:
                logger.error(f"❌ '{tab_name}': Failed to read")
        
        # Summary
        logger.info(f"\n📊 SUMMARY:")
        logger.info("=" * 50)
        logger.info(f"Total tabs found: {len(tabs)}")
        logger.info(f"Successfully readable tabs: {len(successful_tabs)}")
        
        if successful_tabs:
            logger.info(f"\n✅ Recommended tab for hash testing:")
            # Use the tab with the most data
            best_tab = max(successful_tabs, key=lambda x: x['rows'])
            logger.info(f"   Tab name: '{best_tab['name']}'")
            logger.info(f"   Rows: {best_tab['rows']}")
            
            logger.info(f"\n💡 Update your test script to use:")
            logger.info(f"   tab_name = '{best_tab['name']}'")
            
            return best_tab['name']
        else:
            logger.error("❌ No readable tabs found")
            return None
        
    except Exception as e:
        logger.error(f"❌ Main function failed: {str(e)}")
        return None


if __name__ == "__main__":
    result = asyncio.run(main())
    if result:
        print(f"\n🎯 Use this tab name: {result}")
    else:
        print("\n❌ Could not determine correct tab name")
    sys.exit(0 if result else 1)