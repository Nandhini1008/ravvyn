"""
Cache Service - In-memory and persistent caching for API responses
Reduces external API calls by caching responses with TTL support
"""

import hashlib
import json
import logging
import time
from typing import Optional, Dict, Any, Callable
from datetime import datetime, timedelta
from functools import wraps
import threading

from core.config import get_settings

logger = logging.getLogger(__name__)


class CacheEntry:
    """Represents a single cache entry with expiration"""
    
    def __init__(self, key: str, value: Any, ttl_seconds: int):
        self.key = key
        self.value = value
        self.created_at = datetime.utcnow()
        self.expires_at = self.created_at + timedelta(seconds=ttl_seconds)
        self.access_count = 0
        self.last_accessed = self.created_at
    
    def is_expired(self) -> bool:
        """Check if cache entry has expired"""
        return datetime.utcnow() > self.expires_at
    
    def access(self) -> Any:
        """Access the cache entry and update statistics"""
        self.access_count += 1
        self.last_accessed = datetime.utcnow()
        return self.value


class CacheService:
    """
    Cache service for storing API responses with TTL support.
    Thread-safe in-memory cache with optional persistence.
    """
    
    def __init__(self):
        """Initialize cache service with configuration"""
        settings = get_settings()
        
        # Cache configuration
        self.enabled = getattr(settings, 'cache_enabled', True)
        self.default_ttl = getattr(settings, 'cache_default_ttl', 3600)  # 1 hour default
        self.max_size = getattr(settings, 'cache_max_size', 1000)  # Max entries
        self.cleanup_interval = getattr(settings, 'cache_cleanup_interval', 300)  # 5 minutes
        
        # Cache storage
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.RLock()  # Reentrant lock for thread safety
        
        # Statistics
        self._stats = {
            'hits': 0,
            'misses': 0,
            'sets': 0,
            'evictions': 0,
            'expired': 0
        }
        
        logger.info(
            f"Cache service initialized: enabled={self.enabled}, "
            f"default_ttl={self.default_ttl}s, max_size={self.max_size}"
        )
    
    def _generate_key(self, prefix: str, *args, **kwargs) -> str:
        """
        Generate a cache key from prefix and arguments.
        Normalizes the input to ensure consistent keys.
        """
        # Normalize arguments
        normalized = {
            'args': [str(arg).lower().strip() if isinstance(arg, str) else arg 
                    for arg in args],
            'kwargs': {k: str(v).lower().strip() if isinstance(v, str) else v 
                      for k, v in sorted(kwargs.items())}
        }
        
        # Create hash
        key_data = f"{prefix}:{json.dumps(normalized, sort_keys=True, default=str)}"
        key_hash = hashlib.sha256(key_data.encode()).hexdigest()
        return f"{prefix}:{key_hash[:16]}"
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get a value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value if found and not expired, None otherwise
        """
        if not self.enabled:
            return None
        
        with self._lock:
            entry = self._cache.get(key)
            
            if entry is None:
                self._stats['misses'] += 1
                return None
            
            if entry.is_expired():
                # Remove expired entry
                del self._cache[key]
                self._stats['expired'] += 1
                self._stats['misses'] += 1
                logger.debug(f"Cache entry expired: {key}")
                return None
            
            # Entry is valid
            self._stats['hits'] += 1
            return entry.access()
    
    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> bool:
        """
        Set a value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl_seconds: Time to live in seconds (uses default if None)
            
        Returns:
            True if set successfully, False otherwise
        """
        if not self.enabled:
            return False
        
        ttl = ttl_seconds if ttl_seconds is not None else self.default_ttl
        
        with self._lock:
            # Check if we need to evict entries
            if len(self._cache) >= self.max_size and key not in self._cache:
                self._evict_oldest()
            
            # Create and store entry
            entry = CacheEntry(key, value, ttl)
            self._cache[key] = entry
            self._stats['sets'] += 1
            
            logger.debug(f"Cache entry set: {key} (TTL: {ttl}s)")
            return True
    
    def delete(self, key: str) -> bool:
        """
        Delete a cache entry.
        
        Args:
            key: Cache key
            
        Returns:
            True if deleted, False if not found
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                logger.debug(f"Cache entry deleted: {key}")
                return True
            return False
    
    def clear(self, prefix: Optional[str] = None) -> int:
        """
        Clear cache entries.
        
        Args:
            prefix: If provided, only clear entries with this prefix
            
        Returns:
            Number of entries cleared
        """
        with self._lock:
            if prefix:
                keys_to_delete = [k for k in self._cache.keys() if k.startswith(prefix)]
                for key in keys_to_delete:
                    del self._cache[key]
                logger.info(f"Cleared {len(keys_to_delete)} cache entries with prefix: {prefix}")
                return len(keys_to_delete)
            else:
                count = len(self._cache)
                self._cache.clear()
                logger.info(f"Cleared all {count} cache entries")
                return count
    
    def invalidate(self, pattern: str) -> int:
        """
        Invalidate cache entries matching a pattern.
        Useful for invalidating related entries (e.g., all entries for a sheet_id).
        
        Args:
            pattern: Pattern to match (simple substring match)
            
        Returns:
            Number of entries invalidated
        """
        with self._lock:
            keys_to_delete = [k for k in self._cache.keys() if pattern in k]
            for key in keys_to_delete:
                del self._cache[key]
            
            if keys_to_delete:
                logger.info(f"Invalidated {len(keys_to_delete)} cache entries matching pattern: {pattern}")
            
            return len(keys_to_delete)
    
    def _evict_oldest(self) -> None:
        """Evict the oldest (least recently accessed) entry"""
        if not self._cache:
            return
        
        # Find oldest entry by last_accessed
        oldest_key = min(
            self._cache.keys(),
            key=lambda k: self._cache[k].last_accessed
        )
        
        del self._cache[oldest_key]
        self._stats['evictions'] += 1
        logger.debug(f"Evicted oldest cache entry: {oldest_key}")
    
    def cleanup_expired(self) -> int:
        """
        Remove all expired entries from cache.
        
        Returns:
            Number of entries removed
        """
        with self._lock:
            expired_keys = [
                key for key, entry in self._cache.items()
                if entry.is_expired()
            ]
            
            for key in expired_keys:
                del self._cache[key]
            
            if expired_keys:
                self._stats['expired'] += len(expired_keys)
                logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")
            
            return len(expired_keys)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        with self._lock:
            total_requests = self._stats['hits'] + self._stats['misses']
            hit_rate = (self._stats['hits'] / total_requests * 100) if total_requests > 0 else 0
            
            return {
                'enabled': self.enabled,
                'size': len(self._cache),
                'max_size': self.max_size,
                'hits': self._stats['hits'],
                'misses': self._stats['misses'],
                'hit_rate': round(hit_rate, 2),
                'sets': self._stats['sets'],
                'evictions': self._stats['evictions'],
                'expired': self._stats['expired']
            }
    
    def get_info(self) -> Dict[str, Any]:
        """
        Get detailed cache information.
        
        Returns:
            Dictionary with cache information
        """
        with self._lock:
            entries_info = []
            for key, entry in list(self._cache.items())[:10]:  # Limit to first 10
                entries_info.append({
                    'key': key,
                    'age_seconds': (datetime.utcnow() - entry.created_at).total_seconds(),
                    'expires_in_seconds': (entry.expires_at - datetime.utcnow()).total_seconds(),
                    'access_count': entry.access_count
                })
            
            return {
                'stats': self.get_stats(),
                'sample_entries': entries_info
            }


# Global cache instance
_cache_service: Optional[CacheService] = None


def get_cache_service() -> CacheService:
    """Get or create the global cache service instance"""
    global _cache_service
    if _cache_service is None:
        _cache_service = CacheService()
    return _cache_service


def cached(prefix: str, ttl_seconds: Optional[int] = None, key_func: Optional[Callable] = None):
    """
    Decorator for caching function results.
    
    Args:
        prefix: Cache key prefix
        ttl_seconds: Time to live in seconds (uses default if None)
        key_func: Optional function to generate custom cache key from function arguments
        
    Example:
        @cached(prefix='ai_chat', ttl_seconds=1800)
        async def chat(message: str, user_id: str):
            ...
    """
    def decorator(func: Callable) -> Callable:
        cache = get_cache_service()
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Generate cache key
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                cache_key = cache._generate_key(prefix, *args, **kwargs)
            
            # Try to get from cache
            cached_value = cache.get(cache_key)
            if cached_value is not None:
                logger.debug(f"Cache hit for {func.__name__}: {cache_key}")
                return cached_value
            
            # Cache miss - call function
            logger.debug(f"Cache miss for {func.__name__}: {cache_key}")
            result = await func(*args, **kwargs)
            
            # Store in cache
            cache.set(cache_key, result, ttl_seconds)
            
            return result
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            # Generate cache key
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                cache_key = cache._generate_key(prefix, *args, **kwargs)
            
            # Try to get from cache
            cached_value = cache.get(cache_key)
            if cached_value is not None:
                logger.debug(f"Cache hit for {func.__name__}: {cache_key}")
                return cached_value
            
            # Cache miss - call function
            logger.debug(f"Cache miss for {func.__name__}: {cache_key}")
            result = func(*args, **kwargs)
            
            # Store in cache
            cache.set(cache_key, result, ttl_seconds)
            
            return result
        
        # Return appropriate wrapper based on whether function is async
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator

