#!/usr/bin/env python3
"""
Simple SQLite Database Viewer for RAVVYN Hash System
View hash data and logs in a readable format
"""

import sqlite3
import sys
from pathlib import Path
from datetime import datetime
import json

# Database path
DB_PATH = "ravvyn.db"


def connect_db():
    """Connect to the SQLite database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


def show_tables():
    """Show all tables in the database"""
    conn = connect_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        print("📋 Database Tables:")
        print("=" * 50)
        for table in tables:
            print(f"  • {table[0]}")
        print()
        
    except Exception as e:
        print(f"Error showing tables: {e}")
    finally:
        conn.close()


def show_table_schema(table_name):
    """Show schema for a specific table"""
    conn = connect_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        
        print(f"📊 Schema for table '{table_name}':")
        print("=" * 50)
        for col in columns:
            print(f"  {col[1]} ({col[2]}) {'- PRIMARY KEY' if col[5] else ''}")
        print()
        
    except Exception as e:
        print(f"Error showing schema: {e}")
    finally:
        conn.close()


def show_hash_data(limit=10):
    """Show file hash data"""
    conn = connect_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # Get total count
        cursor.execute("SELECT COUNT(*) FROM file_hashes;")
        total_count = cursor.fetchone()[0]
        
        # Get recent data
        cursor.execute("""
            SELECT file_id, file_type, hash_type, hash_value, content_index, 
                   created_at, updated_at
            FROM file_hashes 
            ORDER BY created_at DESC 
            LIMIT ?;
        """, (limit,))
        
        rows = cursor.fetchall()
        
        print(f"🔐 File Hashes (showing {len(rows)} of {total_count} total):")
        print("=" * 80)
        
        if not rows:
            print("  No hash data found.")
            return
        
        for row in rows:
            print(f"File ID: {row['file_id']}")
            print(f"  Type: {row['file_type']} | Hash Type: {row['hash_type']}")
            print(f"  Hash: {row['hash_value'][:32]}...")
            print(f"  Index: {row['content_index']}")
            print(f"  Created: {row['created_at']}")
            print(f"  Updated: {row['updated_at']}")
            print("-" * 40)
        
    except Exception as e:
        print(f"Error showing hash data: {e}")
    finally:
        conn.close()


def show_hash_stats():
    """Show hash statistics by file"""
    conn = connect_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT file_id, file_type, COUNT(*) as hash_count,
                   MIN(created_at) as first_hash,
                   MAX(updated_at) as last_updated
            FROM file_hashes 
            GROUP BY file_id, file_type
            ORDER BY hash_count DESC;
        """)
        
        rows = cursor.fetchall()
        
        print("📈 Hash Statistics by File:")
        print("=" * 80)
        
        if not rows:
            print("  No hash statistics available.")
            return
        
        for row in rows:
            print(f"File: {row['file_id']}")
            print(f"  Type: {row['file_type']} | Hash Count: {row['hash_count']}")
            print(f"  First Hash: {row['first_hash']}")
            print(f"  Last Updated: {row['last_updated']}")
            print("-" * 40)
        
    except Exception as e:
        print(f"Error showing hash stats: {e}")
    finally:
        conn.close()


def show_computation_logs(limit=10):
    """Show hash computation logs"""
    conn = connect_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # Get total count
        cursor.execute("SELECT COUNT(*) FROM hash_computation_logs;")
        total_count = cursor.fetchone()[0]
        
        # Get recent logs
        cursor.execute("""
            SELECT file_id, operation, status, error_message, 
                   execution_time_ms, created_at
            FROM hash_computation_logs 
            ORDER BY created_at DESC 
            LIMIT ?;
        """, (limit,))
        
        rows = cursor.fetchall()
        
        print(f"📝 Computation Logs (showing {len(rows)} of {total_count} total):")
        print("=" * 80)
        
        if not rows:
            print("  No computation logs found.")
            return
        
        for row in rows:
            status_icon = "✅" if row['status'] == 'success' else "❌"
            print(f"{status_icon} {row['file_id']} | {row['operation']}")
            print(f"  Status: {row['status']}")
            if row['error_message']:
                print(f"  Error: {row['error_message']}")
            print(f"  Time: {row['execution_time_ms']}ms")
            print(f"  Created: {row['created_at']}")
            print("-" * 40)
        
    except Exception as e:
        print(f"Error showing computation logs: {e}")
    finally:
        conn.close()


def search_by_file_id(file_id):
    """Search for specific file ID"""
    conn = connect_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # Get hash data for this file
        cursor.execute("""
            SELECT * FROM file_hashes 
            WHERE file_id = ?
            ORDER BY content_index;
        """, (file_id,))
        
        hash_rows = cursor.fetchall()
        
        # Get logs for this file
        cursor.execute("""
            SELECT * FROM hash_computation_logs 
            WHERE file_id = ?
            ORDER BY created_at DESC;
        """, (file_id,))
        
        log_rows = cursor.fetchall()
        
        print(f"🔍 Data for File ID: {file_id}")
        print("=" * 80)
        
        print(f"Hash Records: {len(hash_rows)}")
        if hash_rows:
            print("Sample hashes:")
            for i, row in enumerate(hash_rows[:5]):  # Show first 5
                print(f"  [{row['content_index']}] {row['hash_value'][:32]}...")
        
        print(f"\nLog Records: {len(log_rows)}")
        if log_rows:
            print("Recent logs:")
            for row in log_rows[:3]:  # Show last 3
                status_icon = "✅" if row['status'] == 'success' else "❌"
                print(f"  {status_icon} {row['operation']} - {row['status']} ({row['execution_time_ms']}ms)")
        
    except Exception as e:
        print(f"Error searching for file: {e}")
    finally:
        conn.close()


def main():
    """Main function with menu"""
    if not Path(DB_PATH).exists():
        print(f"❌ Database file '{DB_PATH}' not found!")
        print("Make sure you're in the web/backend directory and the database has been created.")
        return
    
    print("🗄️  RAVVYN Hash System Database Viewer")
    print("=" * 50)
    
    while True:
        print("\nChoose an option:")
        print("1. Show all tables")
        print("2. Show table schemas")
        print("3. Show hash data (recent)")
        print("4. Show hash statistics")
        print("5. Show computation logs")
        print("6. Search by file ID")
        print("7. Exit")
        
        choice = input("\nEnter choice (1-7): ").strip()
        
        if choice == '1':
            show_tables()
        elif choice == '2':
            table = input("Enter table name (file_hashes/hash_computation_logs): ").strip()
            show_table_schema(table)
        elif choice == '3':
            limit = input("How many records to show? (default 10): ").strip()
            limit = int(limit) if limit.isdigit() else 10
            show_hash_data(limit)
        elif choice == '4':
            show_hash_stats()
        elif choice == '5':
            limit = input("How many logs to show? (default 10): ").strip()
            limit = int(limit) if limit.isdigit() else 10
            show_computation_logs(limit)
        elif choice == '6':
            file_id = input("Enter file ID to search: ").strip()
            if file_id:
                search_by_file_id(file_id)
        elif choice == '7':
            print("👋 Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please try again.")


if __name__ == "__main__":
    main()