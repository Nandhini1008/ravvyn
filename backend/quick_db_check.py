#!/usr/bin/env python3
"""
Quick Database Check - Show key information about hash data
"""

import sqlite3
import sys
from pathlib import Path

DB_PATH = "ravvyn.db"

def quick_check():
    """Quick overview of the database"""
    if not Path(DB_PATH).exists():
        print(f"❌ Database '{DB_PATH}' not found!")
        return
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        print("🗄️  RAVVYN Database Quick Check")
        print("=" * 50)
        
        # Check if hash tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%hash%';")
        hash_tables = cursor.fetchall()
        
        if not hash_tables:
            print("❌ No hash tables found. Hash system may not be initialized.")
            return
        
        print("✅ Hash tables found:")
        for table in hash_tables:
            print(f"  • {table[0]}")
        
        # Check file_hashes table
        try:
            cursor.execute("SELECT COUNT(*) FROM file_hashes;")
            hash_count = cursor.fetchone()[0]
            print(f"\n📊 Total hash records: {hash_count}")
            
            if hash_count > 0:
                # Show files with hashes
                cursor.execute("""
                    SELECT file_id, file_type, COUNT(*) as hashes, 
                           MAX(updated_at) as last_updated
                    FROM file_hashes 
                    GROUP BY file_id, file_type;
                """)
                files = cursor.fetchall()
                
                print(f"📁 Files with hashes: {len(files)}")
                for file_info in files:
                    print(f"  • {file_info[0]} ({file_info[1]}) - {file_info[2]} hashes - {file_info[3]}")
                
                # Show recent activity
                cursor.execute("""
                    SELECT file_id, hash_type, created_at 
                    FROM file_hashes 
                    ORDER BY created_at DESC 
                    LIMIT 5;
                """)
                recent = cursor.fetchall()
                
                print(f"\n🕒 Recent hash activity:")
                for activity in recent:
                    print(f"  • {activity[0]} ({activity[1]}) - {activity[2]}")
        
        except sqlite3.OperationalError as e:
            print(f"❌ Error accessing file_hashes table: {e}")
        
        # Check computation logs
        try:
            cursor.execute("SELECT COUNT(*) FROM hash_computation_logs;")
            log_count = cursor.fetchone()[0]
            print(f"\n📝 Total computation logs: {log_count}")
            
            if log_count > 0:
                # Show success/error stats
                cursor.execute("""
                    SELECT status, COUNT(*) as count 
                    FROM hash_computation_logs 
                    GROUP BY status;
                """)
                stats = cursor.fetchall()
                
                print("📈 Operation statistics:")
                for stat in stats:
                    icon = "✅" if stat[0] == 'success' else "❌"
                    print(f"  {icon} {stat[0]}: {stat[1]}")
        
        except sqlite3.OperationalError as e:
            print(f"❌ Error accessing hash_computation_logs table: {e}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Database error: {e}")

if __name__ == "__main__":
    quick_check()