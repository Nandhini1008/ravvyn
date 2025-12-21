#!/usr/bin/env python3
"""
Show Database Values - Display actual data in the hash tables
"""

import sqlite3
import sys
from pathlib import Path
import json

DB_PATH = "ravvyn.db"

def show_all_data():
    """Show all data in hash-related tables"""
    if not Path(DB_PATH).exists():
        print(f"❌ Database '{DB_PATH}' not found!")
        print("Make sure you're in the web/backend directory.")
        return
    
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        cursor = conn.cursor()
        
        print("🗄️  RAVVYN Database - All Hash Data")
        print("=" * 80)
        
        # Check if tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        all_tables = [row[0] for row in cursor.fetchall()]
        
        print(f"📋 Available tables: {', '.join(all_tables)}")
        print()
        
        # Show file_hashes table
        if 'file_hashes' in all_tables:
            print("🔐 FILE_HASHES TABLE:")
            print("-" * 80)
            
            cursor.execute("SELECT COUNT(*) FROM file_hashes;")
            count = cursor.fetchone()[0]
            print(f"Total records: {count}")
            
            if count > 0:
                cursor.execute("""
                    SELECT id, file_id, file_type, hash_type, hash_value, 
                           content_index, content_metadata, created_at, updated_at
                    FROM file_hashes 
                    ORDER BY file_id, content_index
                    LIMIT 50;
                """)
                
                rows = cursor.fetchall()
                
                print(f"\nShowing first {len(rows)} records:")
                print()
                
                for i, row in enumerate(rows, 1):
                    print(f"Record #{i}:")
                    print(f"  ID: {row['id']}")
                    print(f"  File ID: {row['file_id']}")
                    print(f"  File Type: {row['file_type']}")
                    print(f"  Hash Type: {row['hash_type']}")
                    print(f"  Hash Value: {row['hash_value']}")
                    print(f"  Content Index: {row['content_index']}")
                    
                    # Parse metadata if it exists
                    if row['content_metadata']:
                        try:
                            metadata = json.loads(row['content_metadata'])
                            print(f"  Metadata: {metadata}")
                        except:
                            print(f"  Metadata: {row['content_metadata']}")
                    else:
                        print(f"  Metadata: None")
                    
                    print(f"  Created: {row['created_at']}")
                    print(f"  Updated: {row['updated_at']}")
                    print("-" * 40)
                
                if count > 50:
                    print(f"... and {count - 50} more records")
            else:
                print("No hash records found.")
        else:
            print("❌ file_hashes table not found!")
        
        print("\n" + "=" * 80)
        
        # Show hash_computation_logs table
        if 'hash_computation_logs' in all_tables:
            print("📝 HASH_COMPUTATION_LOGS TABLE:")
            print("-" * 80)
            
            cursor.execute("SELECT COUNT(*) FROM hash_computation_logs;")
            count = cursor.fetchone()[0]
            print(f"Total log records: {count}")
            
            if count > 0:
                cursor.execute("""
                    SELECT id, file_id, operation, status, error_message, 
                           execution_time_ms, created_at
                    FROM hash_computation_logs 
                    ORDER BY created_at DESC
                    LIMIT 20;
                """)
                
                rows = cursor.fetchall()
                
                print(f"\nShowing last {len(rows)} log entries:")
                print()
                
                for i, row in enumerate(rows, 1):
                    status_icon = "✅" if row['status'] == 'success' else "❌"
                    print(f"Log #{i}: {status_icon}")
                    print(f"  ID: {row['id']}")
                    print(f"  File ID: {row['file_id']}")
                    print(f"  Operation: {row['operation']}")
                    print(f"  Status: {row['status']}")
                    if row['error_message']:
                        print(f"  Error: {row['error_message']}")
                    print(f"  Execution Time: {row['execution_time_ms']}ms")
                    print(f"  Created: {row['created_at']}")
                    print("-" * 40)
                
                if count > 20:
                    print(f"... and {count - 20} more log entries")
            else:
                print("No computation logs found.")
        else:
            print("❌ hash_computation_logs table not found!")
        
        # Show summary statistics
        print("\n" + "=" * 80)
        print("📊 SUMMARY STATISTICS:")
        print("-" * 80)
        
        if 'file_hashes' in all_tables:
            # Files with hashes
            cursor.execute("""
                SELECT file_id, file_type, COUNT(*) as hash_count,
                       MIN(created_at) as first_created,
                       MAX(updated_at) as last_updated
                FROM file_hashes 
                GROUP BY file_id, file_type
                ORDER BY hash_count DESC;
            """)
            
            file_stats = cursor.fetchall()
            
            if file_stats:
                print("Files with hash data:")
                for stat in file_stats:
                    print(f"  📁 {stat['file_id']}")
                    print(f"     Type: {stat['file_type']}")
                    print(f"     Hash Count: {stat['hash_count']}")
                    print(f"     First Created: {stat['first_created']}")
                    print(f"     Last Updated: {stat['last_updated']}")
                    print()
        
        if 'hash_computation_logs' in all_tables:
            # Operation statistics
            cursor.execute("""
                SELECT operation, status, COUNT(*) as count,
                       AVG(execution_time_ms) as avg_time
                FROM hash_computation_logs 
                GROUP BY operation, status
                ORDER BY operation, status;
            """)
            
            op_stats = cursor.fetchall()
            
            if op_stats:
                print("Operation statistics:")
                for stat in op_stats:
                    status_icon = "✅" if stat['status'] == 'success' else "❌"
                    print(f"  {status_icon} {stat['operation']} ({stat['status']}): {stat['count']} times, avg {stat['avg_time']:.1f}ms")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error reading database: {e}")
        import traceback
        traceback.print_exc()

def show_specific_file(file_id):
    """Show data for a specific file ID"""
    if not Path(DB_PATH).exists():
        print(f"❌ Database '{DB_PATH}' not found!")
        return
    
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        print(f"🔍 Data for File ID: {file_id}")
        print("=" * 80)
        
        # Get hash data
        cursor.execute("""
            SELECT * FROM file_hashes 
            WHERE file_id = ?
            ORDER BY content_index;
        """, (file_id,))
        
        hash_rows = cursor.fetchall()
        
        print(f"Hash records: {len(hash_rows)}")
        
        if hash_rows:
            print("\nHash details:")
            for i, row in enumerate(hash_rows):
                print(f"  Row {i+1} (Index {row['content_index']}):")
                print(f"    Hash: {row['hash_value']}")
                print(f"    Type: {row['hash_type']}")
                print(f"    Created: {row['created_at']}")
                if i >= 10:  # Limit output
                    print(f"    ... and {len(hash_rows) - 10} more rows")
                    break
        
        # Get log data
        cursor.execute("""
            SELECT * FROM hash_computation_logs 
            WHERE file_id = ?
            ORDER BY created_at DESC;
        """, (file_id,))
        
        log_rows = cursor.fetchall()
        
        print(f"\nLog records: {len(log_rows)}")
        
        if log_rows:
            print("\nRecent logs:")
            for i, row in enumerate(log_rows[:5]):  # Show last 5
                status_icon = "✅" if row['status'] == 'success' else "❌"
                print(f"  {status_icon} {row['operation']} - {row['status']} ({row['execution_time_ms']}ms)")
                if row['error_message']:
                    print(f"    Error: {row['error_message']}")
                print(f"    Time: {row['created_at']}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Show specific file
        file_id = sys.argv[1]
        show_specific_file(file_id)
    else:
        # Show all data
        show_all_data()
        
        print("\n" + "=" * 80)
        print("💡 Usage tips:")
        print("  • To see data for a specific file: python show_db_values.py YOUR_FILE_ID")
        print("  • To see your sheet data: python show_db_values.py 1MtjJyKiDR7COszXxZF-wYR-UF5LcwYSpTfr_Aa_PEt8")
        print("  • Run the hash tests first if you don't see any data")