#!/usr/bin/env python3
"""
Check Database Status - Quick check to see if there's any data
"""

import sqlite3
from pathlib import Path

DB_PATH = "ravvyn.db"

def check_status():
    """Quick status check"""
    print("🔍 Database Status Check")
    print("=" * 40)
    
    # Check if database file exists
    if not Path(DB_PATH).exists():
        print("❌ Database file 'ravvyn.db' not found!")
        print("   Make sure you're in the web/backend directory")
        print("   Run the application or tests first to create the database")
        return
    
    print("✅ Database file found")
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Check tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        
        print(f"📋 Tables found: {len(tables)}")
        for table in tables:
            print(f"   • {table}")
        
        # Check hash tables specifically
        hash_tables = ['file_hashes', 'hash_computation_logs']
        
        for table in hash_tables:
            if table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table};")
                count = cursor.fetchone()[0]
                print(f"📊 {table}: {count} records")
                
                if count > 0 and table == 'file_hashes':
                    # Show which files have hashes
                    cursor.execute("SELECT DISTINCT file_id FROM file_hashes;")
                    file_ids = [row[0] for row in cursor.fetchall()]
                    print(f"   Files with hashes: {len(file_ids)}")
                    for fid in file_ids[:3]:  # Show first 3
                        print(f"     • {fid}")
                    if len(file_ids) > 3:
                        print(f"     • ... and {len(file_ids) - 3} more")
            else:
                print(f"❌ {table}: Table not found")
        
        conn.close()
        
        # Check for your specific sheet
        YOUR_SHEET_ID = "1MtjJyKiDR7COszXxZF-wYR-UF5LcwYSpTfr_Aa_PEt8"
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        if 'file_hashes' in tables:
            cursor.execute("SELECT COUNT(*) FROM file_hashes WHERE file_id = ?;", (YOUR_SHEET_ID,))
            your_sheet_count = cursor.fetchone()[0]
            
            if your_sheet_count > 0:
                print(f"✅ Your sheet ({YOUR_SHEET_ID[:20]}...): {your_sheet_count} hashes found")
            else:
                print(f"❌ Your sheet ({YOUR_SHEET_ID[:20]}...): No hashes found")
                print("   Run the hash tests to generate data for your sheet")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Database error: {e}")

if __name__ == "__main__":
    check_status()