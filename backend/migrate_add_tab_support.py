#!/usr/bin/env python3
"""
Database migration script to add tab_name column to file_hashes table
This enables tab-specific hashing for Google Sheets
"""

import sys
import os
import logging

# Add the backend directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from services.database import engine, get_db_context
from sqlalchemy import text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_database():
    """Add tab_name column to file_hashes table"""
    
    try:
        with get_db_context() as db:
            # Check if tab_name column already exists
            result = db.execute(text("PRAGMA table_info(file_hashes)")).fetchall()
            columns = [row[1] for row in result]
            
            if 'tab_name' in columns:
                logger.info("✅ tab_name column already exists in file_hashes table")
                return True
            
            logger.info("🔄 Adding tab_name column to file_hashes table...")
            
            # Add the tab_name column
            db.execute(text("ALTER TABLE file_hashes ADD COLUMN tab_name VARCHAR(255)"))
            
            # Create index for tab_name
            db.execute(text("CREATE INDEX idx_file_tab ON file_hashes (file_id, tab_name)"))
            db.execute(text("CREATE INDEX idx_file_tab_hash ON file_hashes (file_id, tab_name, hash_type)"))
            
            db.commit()
            
            logger.info("✅ Successfully added tab_name column and indexes")
            return True
            
    except Exception as e:
        logger.error(f"❌ Migration failed: {str(e)}")
        return False

def verify_migration():
    """Verify the migration was successful"""
    
    try:
        with get_db_context() as db:
            # Check table structure
            result = db.execute(text("PRAGMA table_info(file_hashes)")).fetchall()
            columns = [row[1] for row in result]
            
            if 'tab_name' not in columns:
                logger.error("❌ tab_name column not found after migration")
                return False
            
            # Check indexes
            result = db.execute(text("PRAGMA index_list(file_hashes)")).fetchall()
            index_names = [row[1] for row in result]
            
            required_indexes = ['idx_file_tab', 'idx_file_tab_hash']
            missing_indexes = [idx for idx in required_indexes if idx not in index_names]
            
            if missing_indexes:
                logger.warning(f"⚠️  Missing indexes: {missing_indexes}")
            
            # Test basic functionality
            db.execute(text("SELECT file_id, tab_name FROM file_hashes LIMIT 1"))
            
            logger.info("✅ Migration verification successful")
            return True
            
    except Exception as e:
        logger.error(f"❌ Migration verification failed: {str(e)}")
        return False

def show_current_data():
    """Show current data structure"""
    
    try:
        with get_db_context() as db:
            # Count total hashes
            total_hashes = db.execute(text("SELECT COUNT(*) FROM file_hashes")).scalar()
            
            # Count by file_id
            file_counts = db.execute(text("""
                SELECT file_id, COUNT(*) as hash_count 
                FROM file_hashes 
                GROUP BY file_id 
                ORDER BY hash_count DESC
            """)).fetchall()
            
            logger.info(f"📊 Current database state:")
            logger.info(f"   Total hashes: {total_hashes}")
            logger.info(f"   Files with hashes: {len(file_counts)}")
            
            for file_id, count in file_counts[:5]:  # Show top 5
                logger.info(f"   {file_id}: {count} hashes")
            
            # Show sample data
            sample_data = db.execute(text("""
                SELECT file_id, file_type, hash_type, content_index, 
                       CASE WHEN tab_name IS NULL THEN 'NULL' ELSE tab_name END as tab_name
                FROM file_hashes 
                LIMIT 5
            """)).fetchall()
            
            logger.info(f"📋 Sample data:")
            for row in sample_data:
                logger.info(f"   {row[0]} | {row[1]} | {row[2]} | idx:{row[3]} | tab:{row[4]}")
            
    except Exception as e:
        logger.error(f"❌ Error showing current data: {str(e)}")

def main():
    """Run the migration"""
    
    logger.info("🚀 Starting database migration for tab-specific hashing")
    logger.info("=" * 60)
    
    # Show current state
    logger.info("📊 Current database state:")
    show_current_data()
    
    # Run migration
    logger.info("\n🔄 Running migration...")
    success = migrate_database()
    
    if not success:
        logger.error("❌ Migration failed")
        return False
    
    # Verify migration
    logger.info("\n🔍 Verifying migration...")
    verified = verify_migration()
    
    if not verified:
        logger.error("❌ Migration verification failed")
        return False
    
    # Show final state
    logger.info("\n📊 Final database state:")
    show_current_data()
    
    logger.info("\n" + "=" * 60)
    logger.info("🎉 Migration completed successfully!")
    logger.info("💡 The system now supports tab-specific hashing for Google Sheets")
    logger.info("💡 Each sheet tab will have separate hashes and can be queried independently")
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\n⏹️  Migration interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ Migration failed with error: {str(e)}")
        sys.exit(1)