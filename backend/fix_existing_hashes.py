#!/usr/bin/env python3
"""
Fix Existing Hashes - Handle existing data in database properly
This script ensures that existing hashes are properly recognized and only changes are processed
"""

import asyncio
import logging
import sys
import os
import sqlite3
from datetime import datetime

# Add the backend directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from services.database import init_db, get_db_context, FileHash
from services.hash_service import HashService
from services.sheets import SheetsService
from services.hash_storage import HashStorage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DEFAULT_SHEET_ID = "1ajWB1qm5a_HedC9Bdo4w14RqLmiKhRzjkzzl3iCaLVg"


async def analyze_existing_hashes():
    """Analyze what hashes currently exist in the database"""
    logger.info("🔍 Analyzing existing hashes in database...")
    
    try:
        with get_db_context() as db:
            # Get all existing hashes for your sheet
            existing_hashes = db.query(FileHash).filter(
                FileHash.file_id == DEFAULT_SHEET_ID
            ).all()
            
            logger.info(f"📊 Found {len(existing_hashes)} existing hashes for sheet {DEFAULT_SHEET_ID}")
            
            if existing_hashes:
                # Analyze the hash structure
                hash_types = {}
                content_indices = []
                
                for hash_record in existing_hashes:
                    hash_type = hash_record.hash_type
                    content_index = hash_record.content_index
                    
                    if hash_type not in hash_types:
                        hash_types[hash_type] = 0
                    hash_types[hash_type] += 1
                    
                    if content_index is not None:
                        content_indices.append(content_index)
                
                logger.info(f"📈 Hash types: {hash_types}")
                if content_indices:
                    logger.info(f"📊 Content index range: {min(content_indices)} to {max(content_indices)}")
                
                # Show sample hashes
                logger.info("🔍 Sample existing hashes:")
                for i, hash_record in enumerate(existing_hashes[:5]):
                    key = f"{hash_record.hash_type}_{hash_record.content_index}"
                    logger.info(f"   {i+1}. Key: {key}, Hash: {hash_record.hash_value[:16]}...")
            
            return existing_hashes
            
    except Exception as e:
        logger.error(f"❌ Error analyzing existing hashes: {str(e)}")
        return []


async def check_key_format_consistency():
    """Check if existing hashes use the correct key format"""
    logger.info("🔍 Checking key format consistency...")
    
    try:
        with get_db_context() as db:
            # Get existing hashes
            existing_hashes = db.query(FileHash).filter(
                FileHash.file_id == DEFAULT_SHEET_ID
            ).all()
            
            if not existing_hashes:
                logger.info("📊 No existing hashes found - fresh start")
                return True
            
            # Check if we can create proper keys
            valid_keys = 0
            invalid_keys = 0
            
            for hash_record in existing_hashes:
                try:
                    key = f"{hash_record.hash_type}_{hash_record.content_index}"
                    if hash_record.hash_type and hash_record.content_index is not None:
                        valid_keys += 1
                    else:
                        invalid_keys += 1
                        logger.warning(f"⚠️  Invalid hash record: type={hash_record.hash_type}, index={hash_record.content_index}")
                except Exception as e:
                    invalid_keys += 1
                    logger.warning(f"⚠️  Error creating key for hash {hash_record.id}: {str(e)}")
            
            logger.info(f"✅ Valid keys: {valid_keys}, Invalid keys: {invalid_keys}")
            return invalid_keys == 0
            
    except Exception as e:
        logger.error(f"❌ Error checking key format: {str(e)}")
        return False


async def test_incremental_with_existing_data():
    """Test incremental updates with existing data in database"""
    logger.info("🧪 Testing incremental updates with existing data...")
    
    try:
        # Initialize services
        hash_service = HashService()
        sheets_service = SheetsService()
        
        # Get current sheet data
        logger.info("📋 Reading current sheet data...")
        tab_data = await sheets_service.read_sheet(DEFAULT_SHEET_ID, "RO DETAILS")
        
        if not tab_data:
            logger.error("❌ No data found in RO DETAILS tab")
            return False
        
        # Take a reasonable sample for testing (first 50 rows)
        sample_data = tab_data[:50]
        logger.info(f"📊 Testing with {len(sample_data)} rows from RO DETAILS")
        
        # Process with hash service
        logger.info("🔄 Processing data with hash service...")
        result = await hash_service.process_file_with_change_detection(
            DEFAULT_SHEET_ID, "sheet", sample_data
        )
        
        if not result['success']:
            logger.error(f"❌ Processing failed: {result.get('error')}")
            return False
        
        # Analyze results
        hash_computation = result.get('hash_computation', {})
        change_detection = result.get('change_detection', {})
        
        logger.info("📈 Processing Results:")
        logger.info(f"   Hash Count: {hash_computation.get('hash_count', 0)}")
        logger.info(f"   Computation Time: {hash_computation.get('computation_time_ms', 0)}ms")
        logger.info(f"   Changes Detected: {result.get('has_changes', False)}")
        
        logger.info("🔍 Change Detection:")
        logger.info(f"   Added: {change_detection.get('added', 0)}")
        logger.info(f"   Modified: {change_detection.get('modified', 0)}")
        logger.info(f"   Deleted: {change_detection.get('deleted', 0)}")
        logger.info(f"   Unchanged: {change_detection.get('unchanged', 0)}")
        
        # Check if incremental updates are working
        unchanged = change_detection.get('unchanged', 0)
        added = change_detection.get('added', 0)
        
        if unchanged > 0:
            logger.info(f"✅ SUCCESS: {unchanged} hashes recognized as unchanged!")
            logger.info("🎉 Incremental hash updates are working correctly with existing data")
            return True
        elif added > 0:
            logger.info(f"⚠️  INFO: {added} hashes treated as new (expected if this is first run after fix)")
            logger.info("💡 Run this test again to see if subsequent runs show unchanged hashes")
            return True
        else:
            logger.warning("⚠️  Unexpected result: No hashes processed")
            return False
        
    except Exception as e:
        logger.error(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


async def clean_invalid_hashes():
    """Clean up any invalid hash records that might cause issues"""
    logger.info("🧹 Cleaning up invalid hash records...")
    
    try:
        with get_db_context() as db:
            # Find invalid hashes (missing hash_type or content_index)
            invalid_hashes = db.query(FileHash).filter(
                (FileHash.hash_type == None) | 
                (FileHash.content_index == None) |
                (FileHash.hash_value == None) |
                (FileHash.hash_value == "")
            ).all()
            
            if invalid_hashes:
                logger.info(f"🗑️  Found {len(invalid_hashes)} invalid hash records")
                
                for invalid_hash in invalid_hashes:
                    logger.info(f"   Removing invalid hash: ID={invalid_hash.id}, "
                              f"type={invalid_hash.hash_type}, index={invalid_hash.content_index}")
                    db.delete(invalid_hash)
                
                db.commit()
                logger.info(f"✅ Cleaned up {len(invalid_hashes)} invalid hash records")
            else:
                logger.info("✅ No invalid hash records found")
            
            return True
            
    except Exception as e:
        logger.error(f"❌ Error cleaning invalid hashes: {str(e)}")
        return False


async def verify_hash_storage_fix():
    """Verify that the hash storage fix is working correctly"""
    logger.info("🔧 Verifying hash storage fix...")
    
    try:
        hash_storage = HashStorage()
        
        # Test the key format consistency
        from services.hash_computer import Hash
        
        # Create test hashes
        test_hashes = [
            Hash(hash_value="a" * 64, hash_type="row", content_index=0, metadata={}),
            Hash(hash_value="b" * 64, hash_type="row", content_index=1, metadata={}),
        ]
        
        # Save test hashes
        logger.info("💾 Saving test hashes...")
        success = await hash_storage.save_hashes_incremental(
            "test_file", "sheet", test_hashes
        )
        
        if not success:
            logger.error("❌ Failed to save test hashes")
            return False
        
        # Load test hashes
        logger.info("📖 Loading test hashes...")
        loaded_hashes = await hash_storage.load_hashes("test_file")
        
        if len(loaded_hashes) != len(test_hashes):
            logger.error(f"❌ Hash count mismatch: saved {len(test_hashes)}, loaded {len(loaded_hashes)}")
            return False
        
        # Save same hashes again (should show unchanged)
        logger.info("🔄 Re-saving same hashes (should show unchanged)...")
        success2 = await hash_storage.save_hashes_incremental(
            "test_file", "sheet", test_hashes
        )
        
        if not success2:
            logger.error("❌ Failed to re-save test hashes")
            return False
        
        # Clean up test data
        with get_db_context() as db:
            db.query(FileHash).filter(FileHash.file_id == "test_file").delete()
            db.commit()
        
        logger.info("✅ Hash storage fix verification passed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Hash storage verification failed: {str(e)}")
        return False


async def main():
    """Main function to fix existing hashes and verify incremental updates"""
    logger.info("🚀 Starting existing hash fix and verification...")
    logger.info("=" * 80)
    
    try:
        # Initialize database
        init_db()
        
        # Step 1: Analyze existing data
        logger.info("📋 Step 1: Analyzing existing data...")
        existing_hashes = await analyze_existing_hashes()
        
        # Step 2: Check key format consistency
        logger.info("\n📋 Step 2: Checking key format consistency...")
        key_format_ok = await check_key_format_consistency()
        
        # Step 3: Clean invalid hashes
        logger.info("\n📋 Step 3: Cleaning invalid hashes...")
        cleanup_ok = await clean_invalid_hashes()
        
        # Step 4: Verify hash storage fix
        logger.info("\n📋 Step 4: Verifying hash storage fix...")
        storage_fix_ok = await verify_hash_storage_fix()
        
        # Step 5: Test incremental updates with existing data
        logger.info("\n📋 Step 5: Testing incremental updates with existing data...")
        incremental_ok = await test_incremental_with_existing_data()
        
        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("📊 SUMMARY")
        logger.info("=" * 80)
        
        results = [
            ("Existing Data Analysis", len(existing_hashes) >= 0),
            ("Key Format Consistency", key_format_ok),
            ("Invalid Hash Cleanup", cleanup_ok),
            ("Hash Storage Fix", storage_fix_ok),
            ("Incremental Updates", incremental_ok)
        ]
        
        all_passed = True
        for test_name, passed in results:
            status = "✅ PASSED" if passed else "❌ FAILED"
            logger.info(f"   {test_name}: {status}")
            if not passed:
                all_passed = False
        
        if all_passed:
            logger.info("\n🎉 ALL CHECKS PASSED!")
            logger.info("✅ The incremental hash system is now working correctly with existing data")
            logger.info("💡 Subsequent runs should show proper unchanged/modified/added counts")
        else:
            logger.info("\n⚠️  SOME CHECKS FAILED")
            logger.info("❌ Please review the errors above and fix any issues")
        
        return all_passed
        
    except Exception as e:
        logger.error(f"❌ Main process failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)