"""
Database optimization script for faster reasoning agent queries
"""

import sqlite3
import time

def optimize_database(db_path="ravvyn.db"):
    """Add indexes and optimize database for faster queries"""
    
    print("🔧 Optimizing database for faster reasoning agent queries...")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check existing indexes
        print("\n📋 Checking existing indexes...")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        existing_indexes = [row[0] for row in cursor.fetchall()]
        print(f"   Found {len(existing_indexes)} existing indexes")
        
        # Optimization 1: Index on sheets_data for time-based queries
        indexes_to_create = [
            {
                "name": "idx_sheets_data_synced_at",
                "sql": "CREATE INDEX IF NOT EXISTS idx_sheets_data_synced_at ON sheets_data(synced_at)",
                "purpose": "Fast time-based filtering on sheets_data"
            },
            {
                "name": "idx_sheets_data_sheet_id",
                "sql": "CREATE INDEX IF NOT EXISTS idx_sheets_data_sheet_id ON sheets_data(sheet_id)",
                "purpose": "Fast joins with sheets_metadata"
            },
            {
                "name": "idx_sheets_data_tab_name",
                "sql": "CREATE INDEX IF NOT EXISTS idx_sheets_data_tab_name ON sheets_data(tab_name)",
                "purpose": "Fast filtering by tab name"
            },
            {
                "name": "idx_sheets_metadata_modified_time",
                "sql": "CREATE INDEX IF NOT EXISTS idx_sheets_metadata_modified_time ON sheets_metadata(modified_time)",
                "purpose": "Fast time-based filtering on metadata"
            },
            {
                "name": "idx_sheets_metadata_sync_status",
                "sql": "CREATE INDEX IF NOT EXISTS idx_sheets_metadata_sync_status ON sheets_metadata(sync_status)",
                "purpose": "Fast sync status queries"
            },
            {
                "name": "idx_chat_history_created_at",
                "sql": "CREATE INDEX IF NOT EXISTS idx_chat_history_created_at ON chat_history(created_at)",
                "purpose": "Fast time-based filtering on chat history"
            },
            {
                "name": "idx_tasks_created_at",
                "sql": "CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at)",
                "purpose": "Fast time-based filtering on tasks"
            },
            {
                "name": "idx_tasks_status",
                "sql": "CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)",
                "purpose": "Fast filtering by task status"
            },
            {
                "name": "idx_sheets_data_composite",
                "sql": "CREATE INDEX IF NOT EXISTS idx_sheets_data_composite ON sheets_data(sheet_id, tab_name, synced_at)",
                "purpose": "Composite index for complex queries"
            }
        ]
        
        print(f"\n🔨 Creating {len(indexes_to_create)} performance indexes...")
        
        created_count = 0
        for index_info in indexes_to_create:
            if index_info["name"] not in existing_indexes:
                try:
                    start_time = time.time()
                    cursor.execute(index_info["sql"])
                    end_time = time.time()
                    duration = (end_time - start_time) * 1000
                    
                    print(f"   ✅ {index_info['name']}: {duration:.2f}ms")
                    print(f"      Purpose: {index_info['purpose']}")
                    created_count += 1
                except Exception as e:
                    print(f"   ❌ Failed to create {index_info['name']}: {e}")
            else:
                print(f"   ℹ️  {index_info['name']}: Already exists")
        
        # Optimization 2: Update database statistics
        print(f"\n📊 Updating database statistics...")
        start_time = time.time()
        cursor.execute("ANALYZE")
        end_time = time.time()
        duration = (end_time - start_time) * 1000
        print(f"   ✅ Statistics updated: {duration:.2f}ms")
        
        # Optimization 3: Vacuum database to optimize storage
        print(f"\n🧹 Optimizing database storage...")
        start_time = time.time()
        cursor.execute("VACUUM")
        end_time = time.time()
        duration = (end_time - start_time) * 1000
        print(f"   ✅ Database vacuumed: {duration:.2f}ms")
        
        # Get database statistics
        print(f"\n📈 Database Statistics:")
        
        # Table sizes
        tables = ['sheets_data', 'sheets_metadata', 'chat_history', 'tasks']
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"   📋 {table}: {count:,} records")
            except:
                print(f"   ❌ {table}: Table not found")
        
        # Database size
        cursor.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
        size_bytes = cursor.fetchone()[0]
        size_mb = size_bytes / (1024 * 1024)
        print(f"   💾 Database size: {size_mb:.2f} MB")
        
        conn.commit()
        conn.close()
        
        print(f"\n🎉 Database optimization completed!")
        print(f"   Created {created_count} new indexes")
        print(f"   Database is now optimized for reasoning agent queries")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Database optimization failed: {e}")
        return False

def test_query_performance(db_path="ravvyn.db"):
    """Test query performance after optimization"""
    
    print("\n🧪 Testing query performance...")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Test queries that the reasoning agent uses
        test_queries = [
            {
                "name": "Count sheets_data",
                "sql": "SELECT COUNT(*) FROM sheets_data"
            },
            {
                "name": "Count with time filter",
                "sql": "SELECT COUNT(*) FROM sheets_data WHERE synced_at >= '2025-12-10 00:00:00'"
            },
            {
                "name": "Join with metadata",
                "sql": """
                SELECT COUNT(*) 
                FROM sheets_data sd 
                INNER JOIN sheets_metadata sm ON sd.sheet_id = sm.sheet_id 
                WHERE sm.sync_status = 'completed'
                """
            },
            {
                "name": "Group by tab_name",
                "sql": "SELECT tab_name, COUNT(*) FROM sheets_data GROUP BY tab_name LIMIT 5"
            },
            {
                "name": "Chat history count",
                "sql": "SELECT COUNT(*) FROM chat_history"
            }
        ]
        
        for query_info in test_queries:
            try:
                start_time = time.time()
                cursor.execute(query_info["sql"])
                result = cursor.fetchall()
                end_time = time.time()
                duration = (end_time - start_time) * 1000
                
                print(f"   ⚡ {query_info['name']}: {duration:.2f}ms")
                if duration > 1000:  # More than 1 second
                    print(f"      ⚠️  Query is slow, consider further optimization")
                elif duration > 100:  # More than 100ms
                    print(f"      ℹ️  Query is moderate speed")
                else:
                    print(f"      ✅ Query is fast")
                    
            except Exception as e:
                print(f"   ❌ {query_info['name']}: Failed - {e}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Performance testing failed: {e}")

if __name__ == "__main__":
    print("🚀 Database Optimization for Reasoning Agent")
    print("=" * 50)
    
    # Optimize database
    success = optimize_database()
    
    if success:
        # Test performance
        test_query_performance()
        
        print(f"\n💡 Recommendations:")
        print(f"   • Run this optimization script periodically")
        print(f"   • Monitor query performance in production")
        print(f"   • Consider partitioning large tables if they grow significantly")
        print(f"   • Use EXPLAIN QUERY PLAN to analyze slow queries")
    else:
        print(f"\n❌ Optimization failed. Check database permissions and integrity.")