#!/usr/bin/env python3
"""
Check Hash Keys - Simple script to check existing hash key formats
"""

import sqlite3
import sys
import os

# Database path
DB_PATH = "ravvyn.db"
DEFAULT_SHEET_ID = "1MtjJyKiDR7COszXxZF-wYR-UF5LcwYSpTfr_Aa_PEt8"

def check_existing_hashes():
    """Check existing hash records in database"""
    print("🔍 Checking existing hash records...")
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Get all hash records for your sheet
        cursor.execute("""
            SELECT id, file_id, hash_type, content_index, hash_value, created_at
            FROM file_hashes 
            WHERE file_id = ?
            ORDER BY content_index
        """, (DEFAULT_SHEET_ID,))
        
        records = cursor.fetchall()
        
        print(f"📊 Found {len(records)} hash records for sheet {DEFAULT_SHEET_ID}")
        
        if records:
            print("\n🔍 Sample hash records:")
            for i, record in enumerate(records[:10]):  # Show first 10
                id, file_id, hash_type, content_index, hash_value, created_at = record
                key_old_format = f"{content_index}_{hash_type}"
                key_new_format = f"{hash_type}_{content_index}"
                
                print(f"   {i+1}. ID: {id}")
                print(f"      Hash Type: {hash_type}")
                print(f"      Content Index: {content_index}")
                print(f"      Hash Value: {hash_value[:16]}...")
                print(f"      Old Key Format: {key_old_format}")
                print(f"      New Key Format: {key_new_format}")
                print(f"      Created: {created_at}")
                print()
        
        # Check for any invalid records
        cursor.execute("""
            SELECT COUNT(*) FROM file_hashes 
            WHERE file_id = ? AND (hash_type IS NULL OR content_index IS NULL)
        """, (DEFAULT_SHEET_ID,))
        
        invalid_count = cursor.fetchone()[0]
        print(f"⚠️  Invalid records (missing hash_type or content_index): {invalid_count}")
        
        conn.close()
        return len(records), invalid_count
        
    except Exception as e:
        print(f"❌ Error checking hashes: {str(e)}")
        return 0, 0

def main():
    """Main function"""
    print("🚀 Hash Key Format Checker")
    print("=" * 50)
    
    if not os.path.exists(DB_PATH):
        print(f"❌ Database file not found: {DB_PATH}")
        return
    
    total_hashes, invalid_hashes = check_existing_hashes()
    
    print("=" * 50)
    print("📊 SUMMARY")
    print(f"   Total Hash Records: {total_hashes}")
    print(f"   Invalid Records: {invalid_hashes}")
    
    if total_hashes > 0:
        print("\n💡 RECOMMENDATIONS:")
        print("   1. The key format has been standardized to: hash_type_content_index")
        print("   2. Existing hashes should now be properly recognized")
        print("   3. Run a test sync to verify incremental updates work")
    else:
        print("\n💡 INFO:")
        print("   No existing hashes found - fresh start expected")

if __name__ == "__main__":
    main()