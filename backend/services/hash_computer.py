"""
Hash Computer Service - Core hashing algorithms for different content types
Implements SHA-256 hashing for rows, blocks, and binary content
"""

import hashlib
import logging
from typing import List, Any, Union
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class Hash:
    """Represents a single hash value with metadata"""
    hash_value: str
    hash_type: str  # 'row', 'block', 'binary'
    content_index: int = None
    metadata: dict = None


class HashComputer:
    """
    Core hash computation service for different content types.
    Implements canonical string formatting and SHA-256 hashing.
    """
    
    def __init__(self):
        """Initialize hash computer with configuration settings"""
        try:
            from core.config import get_settings
            settings = get_settings()
            
            self.default_block_size = settings.hash_block_size_kb * 1024  # Convert KB to bytes
            self.pdf_large_threshold = settings.hash_pdf_threshold_mb * 1024 * 1024  # Convert MB to bytes
            self.pdf_block_size = settings.hash_pdf_block_size_mb * 1024 * 1024  # Convert MB to bytes
        except Exception as e:
            logger.warning(f"Could not load settings, using defaults: {str(e)}")
            self.default_block_size = 4096  # 4KB blocks for documents
            self.pdf_large_threshold = 100 * 1024 * 1024  # 100MB threshold for PDFs
            self.pdf_block_size = 2 * 1024 * 1024  # 2MB blocks for large PDFs
    
    def compute_row_hash(self, row_data: List[Any]) -> str:
        """
        Compute SHA-256 hash for a spreadsheet row.
        
        Args:
            row_data: List of cell values in the row
            
        Returns:
            SHA-256 hash as hex string
        """
        try:
            canonical_string = self.create_canonical_string(row_data)
            return self._compute_sha256(canonical_string)
        except Exception as e:
            logger.error(f"Error computing row hash: {str(e)}")
            raise
    
    def compute_block_hash(self, content_block: str) -> str:
        """
        Compute SHA-256 hash for a content block.
        
        Args:
            content_block: String content to hash
            
        Returns:
            SHA-256 hash as hex string
        """
        try:
            return self._compute_sha256(content_block)
        except Exception as e:
            logger.error(f"Error computing block hash: {str(e)}")
            raise
    
    def compute_binary_hash(self, binary_data: bytes) -> str:
        """
        Compute SHA-256 hash for binary data.
        
        Args:
            binary_data: Binary content to hash
            
        Returns:
            SHA-256 hash as hex string
        """
        try:
            return hashlib.sha256(binary_data).hexdigest()
        except Exception as e:
            logger.error(f"Error computing binary hash: {str(e)}")
            raise
    
    def create_canonical_string(self, row_data: List[Any]) -> str:
        """
        Create canonical string representation of row data using pipe delimiters.
        
        Args:
            row_data: List of cell values
            
        Returns:
            Canonical string with pipe delimiters
        """
        try:
            # Convert all values to strings and handle None/empty values
            normalized_values = []
            for value in row_data:
                if value is None:
                    normalized_values.append("")
                elif isinstance(value, (int, float)):
                    normalized_values.append(str(value))
                elif isinstance(value, str):
                    # Strip whitespace and normalize
                    normalized_values.append(value.strip())
                else:
                    # Convert other types to string
                    normalized_values.append(str(value).strip())
            
            # Join with pipe delimiter
            canonical_string = "|".join(normalized_values)
            logger.debug(f"Created canonical string: {canonical_string[:100]}...")
            return canonical_string
            
        except Exception as e:
            logger.error(f"Error creating canonical string: {str(e)}")
            raise
    
    def split_into_blocks(self, content: str, block_size: int = None) -> List[str]:
        """
        Split content into fixed-size blocks.
        
        Args:
            content: String content to split
            block_size: Size of each block in characters (uses default if None)
            
        Returns:
            List of content blocks
        """
        try:
            if block_size is None:
                block_size = self.default_block_size
            
            if not content:
                return []
            
            blocks = []
            for i in range(0, len(content), block_size):
                block = content[i:i + block_size]
                blocks.append(block)
            
            logger.debug(f"Split content into {len(blocks)} blocks of size {block_size}")
            return blocks
            
        except Exception as e:
            logger.error(f"Error splitting content into blocks: {str(e)}")
            raise
    
    def split_binary_into_blocks(self, binary_data: bytes, block_size: int = None) -> List[bytes]:
        """
        Split binary data into fixed-size blocks.
        
        Args:
            binary_data: Binary content to split
            block_size: Size of each block in bytes (uses PDF block size if None)
            
        Returns:
            List of binary blocks
        """
        try:
            if block_size is None:
                block_size = self.pdf_block_size
            
            if not binary_data:
                return []
            
            blocks = []
            for i in range(0, len(binary_data), block_size):
                block = binary_data[i:i + block_size]
                blocks.append(block)
            
            logger.debug(f"Split binary data into {len(blocks)} blocks of size {block_size}")
            return blocks
            
        except Exception as e:
            logger.error(f"Error splitting binary data into blocks: {str(e)}")
            raise
    
    def compute_row_hashes(self, rows_data: List[List[Any]]) -> List[Hash]:
        """
        Compute hashes for multiple spreadsheet rows.
        
        Args:
            rows_data: List of rows, each row is a list of cell values
            
        Returns:
            List of Hash objects with row hashes
        """
        try:
            hashes = []
            for row_index, row_data in enumerate(rows_data):
                hash_value = self.compute_row_hash(row_data)
                hash_obj = Hash(
                    hash_value=hash_value,
                    hash_type='row',
                    content_index=row_index,
                    metadata={'row_length': len(row_data)}
                )
                hashes.append(hash_obj)
            
            logger.info(f"Computed hashes for {len(hashes)} rows")
            return hashes
            
        except Exception as e:
            logger.error(f"Error computing row hashes: {str(e)}")
            raise
    
    def compute_block_hashes(self, content: str, block_size: int = None) -> List[Hash]:
        """
        Compute hashes for content blocks.
        
        Args:
            content: String content to hash
            block_size: Size of each block (uses default if None)
            
        Returns:
            List of Hash objects with block hashes
        """
        try:
            blocks = self.split_into_blocks(content, block_size)
            hashes = []
            
            for block_index, block in enumerate(blocks):
                hash_value = self.compute_block_hash(block)
                hash_obj = Hash(
                    hash_value=hash_value,
                    hash_type='block',
                    content_index=block_index,
                    metadata={'block_size': len(block)}
                )
                hashes.append(hash_obj)
            
            logger.info(f"Computed hashes for {len(hashes)} blocks")
            return hashes
            
        except Exception as e:
            logger.error(f"Error computing block hashes: {str(e)}")
            raise
    
    def compute_binary_hashes(self, binary_data: bytes, use_blocks: bool = None) -> List[Hash]:
        """
        Compute hashes for binary data (whole file or blocks).
        
        Args:
            binary_data: Binary content to hash
            use_blocks: Whether to use block-wise hashing (auto-detect if None)
            
        Returns:
            List of Hash objects with binary hashes
        """
        try:
            # Auto-detect whether to use blocks based on size
            if use_blocks is None:
                use_blocks = len(binary_data) >= self.pdf_large_threshold
            
            if use_blocks:
                # Block-wise hashing for large files
                blocks = self.split_binary_into_blocks(binary_data)
                hashes = []
                
                for block_index, block in enumerate(blocks):
                    hash_value = self.compute_binary_hash(block)
                    hash_obj = Hash(
                        hash_value=hash_value,
                        hash_type='block',
                        content_index=block_index,
                        metadata={'block_size': len(block)}
                    )
                    hashes.append(hash_obj)
                
                logger.info(f"Computed block hashes for {len(hashes)} blocks")
                return hashes
            else:
                # Whole-file hashing for small files
                hash_value = self.compute_binary_hash(binary_data)
                hash_obj = Hash(
                    hash_value=hash_value,
                    hash_type='binary',
                    content_index=0,
                    metadata={'file_size': len(binary_data)}
                )
                
                logger.info(f"Computed whole-file hash for {len(binary_data)} bytes")
                return [hash_obj]
                
        except Exception as e:
            logger.error(f"Error computing binary hashes: {str(e)}")
            raise
    
    def _compute_sha256(self, content: str) -> str:
        """
        Compute SHA-256 hash for string content.
        
        Args:
            content: String content to hash
            
        Returns:
            SHA-256 hash as hex string
        """
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
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
            if len(hash_value) != 64:
                return False
            
            # Check if all characters are valid hex
            int(hash_value, 16)
            return True
            
        except (ValueError, TypeError):
            return False