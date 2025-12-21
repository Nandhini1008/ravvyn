#!/usr/bin/env python3
"""
Database Analysis Script - Analyze SQLite database content and size
"""

import sqlite3
import os
import json
from datetime import datetime
from typing import Dict, Any

def get_database_path():
    """Get the database path"""
    return "ravvyn.db"

def get_file_size(filepath):
    """Get file size in human readable format"""
    if not os.path.exists(filepath):
        return "File not found"
    
    size = os.path.getsize(filepath)
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} TB"

def analyze_database() -> Dict[str, Any]:
    """Analyze the SQLite database and return comprehensive statistics"""
    db_path = get_database_path()
    
    if not os.path.exists(db_path):
        return {"error": f"Database file not found: {db_path}"}
    
    analysis = {
        "database_info": {
            "file_path": db_path,
            "file_size": get_file_size(db_path),
            "analysis_time": datetime.now().isoformat()
        },
        "tables": {},
        "hash_analysis": {},
        "content_summary": {},
        "totals": {}
    }
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get database info
        cursor.execute("PRAGMA page_count")
        page_count = cursor.fetchone()[0]
        cursor.execute("PRAGMA page_size")
        page_size = cursor.fetchone()[0]
        
        analysis["database_info"]["page_count"] = page_count
        analysis["database_info"]["page_size"] = page_size
        analysis["database_info"]["total_pages_size"] = f"{(page_count * page_size) / 1024 / 1024:.2f} MB"
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = [row[0] for row in cursor.fetchall()]
        
        total_records = 0
        
        # Analyze each table
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                analysis["tables"][table] = count
                total_records += count
            except Exception as e:
                analysis["tables"][table] = f"Error: {str(e)}"
        
        analysis["totals"]["total_records"] = total_records
        
        # Detailed hash analysis
        if "file_hashes" in tables:
            # Hash count by file type
            cursor.execute("SELECT file_type, COUNT(*) FROM file_hashes GROUP BY file_type")
            hash_by_type = dict(cursor.fetchall())
            analysis["hash_analysis"]["by_file_type"] = hash_by_type
            
            # Hash count by hash type
            cursor.execute("SELECT hash_type, COUNT(*) FROM file_hashes GROUP BY hash_type")
            hash_by_hash_type = dict(cursor.fetchall())
            analysis["hash_analysis"]["by_hash_type"] = hash_by_hash_type
            
            # Your specific sheet analysis
            your_sheet_id = "1MtjJyKiDR7COszXxZF-wYR-UF5LcwYSpTfr_Aa_PEt8"
            cursor.execute("SELECT COUNT(*) FROM file_hashes WHERE file_id = ?", (your_sheet_id,))
            your_sheet_hashes = cursor.fetchone()[0]
            analysis["hash_analysis"]["your_sheet_hashes"] = your_sheet_hashes
            
            # Recent hash activity
            cursor.execute("""
                SELECT file_id, COUNT(*) as hash_count, MAX(created_at) as last_updated
                FROM file_hashes 
                GROUP BY file_id 
                ORDER BY last_updated DESC 
                LIMIT 5
            """)
            recent_activity = []
            for row in cursor.fetchall():
                recent_activity.append({
                    "file_id": row[0],
                    "hash_count": row[1],
                    "last_updated": row[2]
                })
            analysis["hash_analysis"]["recent_activity"] = recent_activity
        
        # Sheet content analysis
        if "sheets_metadata" in tables and "sheets_content" in tables:
            cursor.execute("""
                SELECT 
                    sm.sheet_name,
                    sm.sheet_id,
                    sc.row_count,
                    sc.synced_at,
                    sm.sync_status
                FROM sheets_metadata sm
                LEFT JOIN sheets_content sc ON sm.sheet_id = sc.sheet_id
                ORDER BY sc.row_count DESC
            """)
            
            sheets_summary = []
            total_rows = 0
            for row in cursor.fetchall():
                sheet_info = {
                    "sheet_name": row[0],
                    "sheet_id": row[1],
                    "row_count": row[2] or 0,
                    "synced_at": row[3],
                    "sync_status": row[4]
                }
                sheets_summary.append(sheet_info)
                total_rows += sheet_info["row_count"]
            
            analysis["content_summary"]["sheets"] = sheets_summary
            analysis["totals"]["total_sheet_rows"] = total_rows
        
        # Chat history analysis
        if "chat_history" in tables:
            cursor.execute("""
                SELECT 
                    user_id,
                    COUNT(*) as message_count,
                    MIN(created_at) as first_message,
                    MAX(created_at) as last_message
                FROM chat_history 
                GROUP BY user_id
            """)
            
            chat_summary = []
            for row in cursor.fetchall():
                chat_summary.append({
                    "user_id": row[0],
                    "message_count": row[1],
                    "first_message": row[2],
                    "last_message": row[3]
                })
            
            analysis["content_summary"]["chat_users"] = chat_summary
        
        conn.close()
        
    except Exception as e:
        analysis["error"] = str(e)
    
    return analysis

def print_analysis(analysis: Dict[str, Any]):
    """Print the analysis in a readable format"""
    print("=" * 80)
    print("🗄️  RAVVYN DATABASE ANALYSIS")
    print("=" * 80)
    
    # Database info
    db_info = analysis.get("database_info", {})
    print(f"\n📁 Database File: {db_info.get('file_path', 'Unknown')}")
    print(f"📊 File Size: {db_info.get('file_size', 'Unknown')}")
    print(f"📄 Pages: {db_info.get('page_count', 'Unknown')} pages × {db_info.get('page_size', 'Unknown')} bytes")
    print(f"🕒 Analysis Time: {db_info.get('analysis_time', 'Unknown')}")
    
    # Tables summary
    tables = analysis.get("tables", {})
    if tables:
        print(f"\n📋 TABLES SUMMARY")
        print("-" * 40)
        for table, count in tables.items():
            print(f"  {table:<25} {count:>10}")
    
    # Totals
    totals = analysis.get("totals", {})
    if totals:
        print(f"\n📈 TOTALS")
        print("-" * 40)
        for key, value in totals.items():
            print(f"  {key.replace('_', ' ').title():<25} {value:>10}")
    
    # Hash analysis
    hash_analysis = analysis.get("hash_analysis", {})
    if hash_analysis:
        print(f"\n🔐 HASH ANALYSIS")
        print("-" * 40)
        
        by_type = hash_analysis.get("by_file_type", {})
        if by_type:
            print("  By File Type:")
            for file_type, count in by_type.items():
                print(f"    {file_type:<20} {count:>8}")
        
        by_hash_type = hash_analysis.get("by_hash_type", {})
        if by_hash_type:
            print("  By Hash Type:")
            for hash_type, count in by_hash_type.items():
                print(f"    {hash_type:<20} {count:>8}")
        
        your_sheet = hash_analysis.get("your_sheet_hashes", 0)
        if your_sheet:
            print(f"  Your Sheet Hashes: {your_sheet}")
    
    # Content summary
    content = analysis.get("content_summary", {})
    if content:
        print(f"\n📊 CONTENT SUMMARY")
        print("-" * 40)
        
        sheets = content.get("sheets", [])
        if sheets:
            print("  Top Sheets by Row Count:")
            for sheet in sheets[:5]:  # Top 5
                name = sheet["sheet_name"][:30] + "..." if len(sheet["sheet_name"]) > 30 else sheet["sheet_name"]
                print(f"    {name:<35} {sheet['row_count']:>8} rows")
        
        chat_users = content.get("chat_users", [])
        if chat_users:
            print("  Chat Activity:")
            for user in chat_users:
                print(f"    {user['user_id']:<20} {user['message_count']:>8} messages")

if __name__ == "__main__":
    print("Analyzing RAVVYN database...")
    analysis = analyze_database()
    
    if "error" in analysis:
        print(f"❌ Error: {analysis['error']}")
    else:
        print_analysis(analysis)
        
        # Save detailed analysis to file
        with open("database_analysis.json", "w") as f:
            json.dump(analysis, f, indent=2)
        print(f"\n💾 Detailed analysis saved to: database_analysis.json")
    
    print("\n" + "=" * 80)