"""
Hash Validator Service - Hash integrity validation and comparison logic
Handles change detection through hash comparison and validation
"""

import logging
from typing import List, Set, Dict, Any, Optional
from dataclasses import dataclass
from services.hash_computer import Hash

logger = logging.getLogger(__name__)


@dataclass
class ChangeSet:
    """Represents changes detected between hash sets"""
    added_items: List[Hash]
    modified_items: List[Hash]
    deleted_items: List[Hash]
    unchanged_count: int
    
    @property
    def has_changes(self) -> bool:
        """Check if any changes were detected"""
        return len(self.added_items) > 0 or len(self.modified_items) > 0 or len(self.deleted_items) > 0


@dataclass
class RowChangeSet(ChangeSet):
    """Represents changes detected in spreadsheet rows"""
    pass


@dataclass
class BlockChangeSet(ChangeSet):
    """Represents changes detected in document blocks"""
    pass


@dataclass
class ChangeDetectionResult:
    """Result of change detection comparison"""
    file_id: str
    has_changes: bool
    added_items: List[Hash]
    modified_items: List[Hash]
    deleted_items: List[Hash]
    unchanged_count: int
    change_summary: Dict[str, int]


class HashValidator:
    """
    Hash validation and comparison service.
    Validates hash integrity and detects changes between hash sets.
    """
    
    def __init__(self):
        """Initialize hash validator"""
        pass
    
    def validate_hash_format(self, hash_value: str) -> bool:
        """
        Validate that a hash value is a valid SHA-256 hex string.
        
        Args:
            hash_value: Hash string to validate
            
        Returns:
            True if valid SHA-256 format, False otherwise
        """
        try:
            # SHA-256 produces 64 character hex string
            if not hash_value or len(hash_value) != 64:
                return False
            
            # Check if all characters are valid hex
            int(hash_value, 16)
            return True
            
        except (ValueError, TypeError):
            return False
    
    def validate_hash_object(self, hash_obj: Hash) -> bool:
        """
        Validate a Hash object for integrity.
        
        Args:
            hash_obj: Hash object to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            # Check hash value format
            if not self.validate_hash_format(hash_obj.hash_value):
                return False
            
            # Check hash type
            if hash_obj.hash_type not in ['row', 'block', 'binary']:
                return False
            
            # Check content index (should be non-negative integer or None)
            if hash_obj.content_index is not None and hash_obj.content_index < 0:
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating hash object: {str(e)}")
            return False
    
    def compare_hash_sets(self, old_hashes: List[Hash], new_hashes: List[Hash]) -> ChangeSet:
        """
        Compare two sets of hashes to detect changes.
        
        Args:
            old_hashes: Previous hash set
            new_hashes: New hash set
            
        Returns:
            ChangeSet with detected changes
        """
        try:
            # Create lookup dictionaries for efficient comparison
            old_hash_map = self._create_hash_map(old_hashes)
            new_hash_map = self._create_hash_map(new_hashes)
            
            added_items = []
            modified_items = []
            deleted_items = []
            unchanged_count = 0
            
            # Find added and modified items
            for key, new_hash in new_hash_map.items():
                if key not in old_hash_map:
                    # New item
                    added_items.append(new_hash)
                elif old_hash_map[key].hash_value != new_hash.hash_value:
                    # Modified item
                    modified_items.append(new_hash)
                else:
                    # Unchanged item
                    unchanged_count += 1
            
            # Find deleted items
            for key, old_hash in old_hash_map.items():
                if key not in new_hash_map:
                    deleted_items.append(old_hash)
            
            change_set = ChangeSet(
                added_items=added_items,
                modified_items=modified_items,
                deleted_items=deleted_items,
                unchanged_count=unchanged_count
            )
            
            logger.info(f"Change detection: {len(added_items)} added, {len(modified_items)} modified, "
                       f"{len(deleted_items)} deleted, {unchanged_count} unchanged")
            
            return change_set
            
        except Exception as e:
            logger.error(f"Error comparing hash sets: {str(e)}")
            # Return empty change set on error
            return ChangeSet([], [], [], 0)
    
    def detect_row_changes(self, old_row_hashes: List[Hash], new_row_hashes: List[Hash]) -> RowChangeSet:
        """
        Detect changes in spreadsheet rows.
        
        Args:
            old_row_hashes: Previous row hashes
            new_row_hashes: New row hashes
            
        Returns:
            RowChangeSet with detected row changes
        """
        try:
            # Filter to only row-type hashes
            old_rows = [h for h in old_row_hashes if h.hash_type == 'row']
            new_rows = [h for h in new_row_hashes if h.hash_type == 'row']
            
            change_set = self.compare_hash_sets(old_rows, new_rows)
            
            row_change_set = RowChangeSet(
                added_items=change_set.added_items,
                modified_items=change_set.modified_items,
                deleted_items=change_set.deleted_items,
                unchanged_count=change_set.unchanged_count
            )
            
            logger.debug(f"Row changes detected: {len(row_change_set.added_items)} new rows, "
                        f"{len(row_change_set.modified_items)} modified rows, "
                        f"{len(row_change_set.deleted_items)} deleted rows")
            
            return row_change_set
            
        except Exception as e:
            logger.error(f"Error detecting row changes: {str(e)}")
            return RowChangeSet([], [], [], 0)
    
    def detect_block_changes(self, old_block_hashes: List[Hash], new_block_hashes: List[Hash]) -> BlockChangeSet:
        """
        Detect changes in document blocks.
        
        Args:
            old_block_hashes: Previous block hashes
            new_block_hashes: New block hashes
            
        Returns:
            BlockChangeSet with detected block changes
        """
        try:
            # Filter to only block-type hashes
            old_blocks = [h for h in old_block_hashes if h.hash_type in ['block', 'binary']]
            new_blocks = [h for h in new_block_hashes if h.hash_type in ['block', 'binary']]
            
            change_set = self.compare_hash_sets(old_blocks, new_blocks)
            
            block_change_set = BlockChangeSet(
                added_items=change_set.added_items,
                modified_items=change_set.modified_items,
                deleted_items=change_set.deleted_items,
                unchanged_count=change_set.unchanged_count
            )
            
            logger.debug(f"Block changes detected: {len(block_change_set.added_items)} new blocks, "
                        f"{len(block_change_set.modified_items)} modified blocks, "
                        f"{len(block_change_set.deleted_items)} deleted blocks")
            
            return block_change_set
            
        except Exception as e:
            logger.error(f"Error detecting block changes: {str(e)}")
            return BlockChangeSet([], [], [], 0)
    
    def create_change_detection_result(self, file_id: str, change_set: ChangeSet) -> ChangeDetectionResult:
        """
        Create a comprehensive change detection result.
        
        Args:
            file_id: File identifier
            change_set: ChangeSet with detected changes
            
        Returns:
            ChangeDetectionResult with comprehensive change information
        """
        try:
            change_summary = {
                'added': len(change_set.added_items),
                'modified': len(change_set.modified_items),
                'deleted': len(change_set.deleted_items),
                'unchanged': change_set.unchanged_count,
                'total_changes': len(change_set.added_items) + len(change_set.modified_items) + len(change_set.deleted_items)
            }
            
            result = ChangeDetectionResult(
                file_id=file_id,
                has_changes=change_set.has_changes,
                added_items=change_set.added_items,
                modified_items=change_set.modified_items,
                deleted_items=change_set.deleted_items,
                unchanged_count=change_set.unchanged_count,
                change_summary=change_summary
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error creating change detection result: {str(e)}")
            # Return empty result on error
            return ChangeDetectionResult(
                file_id=file_id,
                has_changes=False,
                added_items=[],
                modified_items=[],
                deleted_items=[],
                unchanged_count=0,
                change_summary={'added': 0, 'modified': 0, 'deleted': 0, 'unchanged': 0, 'total_changes': 0}
            )
    
    def validate_hash_consistency(self, hashes: List[Hash]) -> Dict[str, Any]:
        """
        Validate consistency of a hash set.
        
        Args:
            hashes: List of Hash objects to validate
            
        Returns:
            Dictionary with validation results
        """
        try:
            validation_result = {
                'total_hashes': len(hashes),
                'valid_hashes': 0,
                'invalid_hashes': 0,
                'duplicate_indices': [],
                'missing_indices': [],
                'hash_types': {},
                'errors': []
            }
            
            seen_indices = set()
            expected_indices = set()
            
            for hash_obj in hashes:
                # Validate individual hash
                if self.validate_hash_object(hash_obj):
                    validation_result['valid_hashes'] += 1
                else:
                    validation_result['invalid_hashes'] += 1
                    validation_result['errors'].append(f"Invalid hash object: {hash_obj.hash_value[:16]}...")
                
                # Track hash types
                hash_type = hash_obj.hash_type
                if hash_type not in validation_result['hash_types']:
                    validation_result['hash_types'][hash_type] = 0
                validation_result['hash_types'][hash_type] += 1
                
                # Check for duplicate indices
                if hash_obj.content_index is not None:
                    if hash_obj.content_index in seen_indices:
                        validation_result['duplicate_indices'].append(hash_obj.content_index)
                    else:
                        seen_indices.add(hash_obj.content_index)
                        expected_indices.add(hash_obj.content_index)
            
            # Check for missing indices (gaps in sequence)
            if expected_indices:
                max_index = max(expected_indices)
                for i in range(max_index + 1):
                    if i not in expected_indices:
                        validation_result['missing_indices'].append(i)
            
            validation_result['is_consistent'] = (
                validation_result['invalid_hashes'] == 0 and
                len(validation_result['duplicate_indices']) == 0
            )
            
            return validation_result
            
        except Exception as e:
            logger.error(f"Error validating hash consistency: {str(e)}")
            return {
                'total_hashes': len(hashes) if hashes else 0,
                'valid_hashes': 0,
                'invalid_hashes': 0,
                'duplicate_indices': [],
                'missing_indices': [],
                'hash_types': {},
                'errors': [str(e)],
                'is_consistent': False
            }
    
    def _create_hash_map(self, hashes: List[Hash]) -> Dict[str, Hash]:
        """
        Create a lookup map for hashes based on content index and type.
        
        Args:
            hashes: List of Hash objects
            
        Returns:
            Dictionary mapping keys to Hash objects
        """
        hash_map = {}
        for hash_obj in hashes:
            # Create a unique key based on hash type and content index
            key = f"{hash_obj.hash_type}_{hash_obj.content_index}"
            hash_map[key] = hash_obj
        return hash_map
    
    def get_change_statistics(self, change_sets: List[ChangeSet]) -> Dict[str, Any]:
        """
        Get aggregate statistics from multiple change sets.
        
        Args:
            change_sets: List of ChangeSet objects
            
        Returns:
            Dictionary with aggregate statistics
        """
        try:
            stats = {
                'total_change_sets': len(change_sets),
                'total_added': 0,
                'total_modified': 0,
                'total_deleted': 0,
                'total_unchanged': 0,
                'files_with_changes': 0,
                'files_without_changes': 0
            }
            
            for change_set in change_sets:
                stats['total_added'] += len(change_set.added_items)
                stats['total_modified'] += len(change_set.modified_items)
                stats['total_deleted'] += len(change_set.deleted_items)
                stats['total_unchanged'] += change_set.unchanged_count
                
                if change_set.has_changes:
                    stats['files_with_changes'] += 1
                else:
                    stats['files_without_changes'] += 1
            
            stats['total_changes'] = stats['total_added'] + stats['total_modified'] + stats['total_deleted']
            stats['change_rate'] = (stats['total_changes'] / 
                                  (stats['total_changes'] + stats['total_unchanged'])) if (stats['total_changes'] + stats['total_unchanged']) > 0 else 0
            
            return stats
            
        except Exception as e:
            logger.error(f"Error calculating change statistics: {str(e)}")
            return {
                'total_change_sets': len(change_sets) if change_sets else 0,
                'total_added': 0,
                'total_modified': 0,
                'total_deleted': 0,
                'total_unchanged': 0,
                'files_with_changes': 0,
                'files_without_changes': 0,
                'total_changes': 0,
                'change_rate': 0
            }