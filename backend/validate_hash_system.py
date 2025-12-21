#!/usr/bin/env python3
"""
Hash System Validation Script
Tests the hash system components to ensure they work correctly
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add the backend directory to the path
sys.path.insert(0, str(Path(__file__).parent))

from services.hash_computer import HashComputer, Hash
from services.hash_validator import HashValidator
from services.hash_service import HashService
from services.database import init_db

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def test_hash_computer():
    """Test hash computer functionality"""
    logger.info("Testing HashComputer...")
    
    computer = HashComputer()
    
    # Test row hashing
    test_rows = [
        ["Name", "Age", "City"],
        ["John", "25", "New York"],
        ["Jane", "30", "Los Angeles"],
        ["", "35", "Chicago"]  # Test empty cell
    ]
    
    row_hashes = computer.compute_row_hashes(test_rows)
    logger.info(f"Computed {len(row_hashes)} row hashes")
    
    # Test block hashing
    test_content = "This is a test document with some content. " * 200  # Make it larger
    block_hashes = computer.compute_block_hashes(test_content)
    logger.info(f"Computed {len(block_hashes)} block hashes for {len(test_content)} characters")
    
    # Test binary hashing
    test_binary = b"This is test binary data" * 1000
    binary_hashes = computer.compute_binary_hashes(test_binary, use_blocks=False)
    logger.info(f"Computed {len(binary_hashes)} binary hashes for {len(test_binary)} bytes")
    
    # Test large binary (should use blocks)
    large_binary = b"Large binary data" * 10000000  # ~170MB
    large_binary_hashes = computer.compute_binary_hashes(large_binary)
    logger.info(f"Computed {len(large_binary_hashes)} hashes for large binary ({len(large_binary)} bytes)")
    
    logger.info("HashComputer tests completed successfully")
    return True


async def test_hash_validator():
    """Test hash validator functionality"""
    logger.info("Testing HashValidator...")
    
    validator = HashValidator()
    
    # Create test hashes
    old_hashes = [
        Hash("hash1", "row", 0, {"test": "data1"}),
        Hash("hash2", "row", 1, {"test": "data2"}),
        Hash("hash3", "row", 2, {"test": "data3"})
    ]
    
    new_hashes = [
        Hash("hash1", "row", 0, {"test": "data1"}),  # Unchanged
        Hash("hash2_modified", "row", 1, {"test": "data2_modified"}),  # Modified
        Hash("hash4", "row", 3, {"test": "data4"})  # Added
        # hash3 is deleted
    ]
    
    # Test change detection
    change_set = validator.compare_hash_sets(old_hashes, new_hashes)
    logger.info(f"Change detection: {len(change_set.added_items)} added, "
               f"{len(change_set.modified_items)} modified, "
               f"{len(change_set.deleted_items)} deleted, "
               f"{change_set.unchanged_count} unchanged")
    
    # Test hash validation
    valid_hash = Hash("a" * 64, "row", 0)
    invalid_hash = Hash("invalid", "row", 0)
    
    assert validator.validate_hash_object(valid_hash), "Valid hash should pass validation"
    assert not validator.validate_hash_object(invalid_hash), "Invalid hash should fail validation"
    
    logger.info("HashValidator tests completed successfully")
    return True


async def test_hash_service():
    """Test hash service functionality"""
    logger.info("Testing HashService...")
    
    hash_service = HashService()
    
    # Test sheet hashing
    test_sheet_data = [
        ["Header1", "Header2", "Header3"],
        ["Value1", "Value2", "Value3"],
        ["Value4", "Value5", "Value6"]
    ]
    
    sheet_result = await hash_service.compute_file_hash("test_sheet_1", "sheet", test_sheet_data)
    logger.info(f"Sheet hashing result: success={sheet_result.success}, "
               f"hashes={len(sheet_result.hashes)}, time={sheet_result.computation_time_ms}ms")
    
    # Test document hashing
    test_doc_content = "This is a test document with multiple paragraphs. " * 100
    
    doc_result = await hash_service.compute_file_hash("test_doc_1", "doc", test_doc_content)
    logger.info(f"Document hashing result: success={doc_result.success}, "
               f"hashes={len(doc_result.hashes)}, time={doc_result.computation_time_ms}ms")
    
    # Test PDF hashing (small)
    test_pdf_data = b"%PDF-1.4\nSmall PDF content" * 1000
    
    pdf_result = await hash_service.compute_file_hash("test_pdf_1", "pdf", test_pdf_data)
    logger.info(f"PDF hashing result: success={pdf_result.success}, "
               f"hashes={len(pdf_result.hashes)}, time={pdf_result.computation_time_ms}ms")
    
    # Test change detection
    # Modify the sheet data slightly
    modified_sheet_data = [
        ["Header1", "Header2", "Header3"],
        ["Value1_modified", "Value2", "Value3"],  # Modified row
        ["Value4", "Value5", "Value6"],
        ["Value7", "Value8", "Value9"]  # Added row
    ]
    
    # Store original hashes
    await hash_service.store_hashes("test_sheet_1", "sheet", sheet_result.hashes)
    
    # Compute new hashes and detect changes
    modified_result = await hash_service.compute_file_hash("test_sheet_1", "sheet", modified_sheet_data)
    change_result = await hash_service.compare_hashes("test_sheet_1", modified_result.hashes)
    
    logger.info(f"Change detection result: has_changes={change_result.has_changes}, "
               f"summary={change_result.change_summary}")
    
    logger.info("HashService tests completed successfully")
    return True


async def test_integration():
    """Test full integration"""
    logger.info("Testing full integration...")
    
    hash_service = HashService()
    
    # Test complete workflow
    test_files = [
        {"file_id": "integration_sheet_1", "file_type": "sheet", "content": [["A", "B"], ["1", "2"]]},
        {"file_id": "integration_doc_1", "file_type": "doc", "content": "Integration test document"},
        {"file_id": "integration_pdf_1", "file_type": "pdf", "content": b"%PDF-1.4\nIntegration test PDF"}
    ]
    
    for test_file in test_files:
        result = await hash_service.process_file_with_change_detection(
            test_file["file_id"], 
            test_file["file_type"], 
            test_file["content"]
        )
        
        logger.info(f"Integration test for {test_file['file_type']}: success={result['success']}, "
                   f"has_changes={result.get('has_changes', 'N/A')}")
    
    # Test service statistics
    stats = await hash_service.get_service_statistics()
    logger.info(f"Service statistics: status={stats.get('service_status', 'unknown')}")
    
    logger.info("Integration tests completed successfully")
    return True


async def main():
    """Run all validation tests"""
    logger.info("Starting hash system validation...")
    
    try:
        # Initialize database
        init_db()
        logger.info("Database initialized")
        
        # Run tests
        tests = [
            test_hash_computer,
            test_hash_validator,
            test_hash_service,
            test_integration
        ]
        
        for test in tests:
            try:
                await test()
            except Exception as e:
                logger.error(f"Test {test.__name__} failed: {str(e)}")
                return False
        
        logger.info("All hash system validation tests passed!")
        return True
        
    except Exception as e:
        logger.error(f"Validation failed: {str(e)}")
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)