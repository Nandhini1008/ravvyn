"""
Rate Limiter Service - Manages API rate limiting for Google Sheets
Prevents hitting the 60 requests per minute limit
"""

import asyncio
import time
import logging
from typing import Dict, Optional
from datetime import datetime, timedelta
from collections import deque

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Rate limiter for Google Sheets API calls
    Enforces 60 requests per minute limit with buffer
    """
    
    def __init__(self, max_requests: int = 50, time_window: int = 60):
        """
        Initialize rate limiter
        
        Args:
            max_requests: Maximum requests allowed (set to 50 to leave buffer)
            time_window: Time window in seconds (60 for per minute)
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = deque()  # Store timestamps of requests
        self._lock = asyncio.Lock()
    
    async def acquire(self, operation_name: str = "api_call") -> bool:
        """
        Acquire permission to make an API call
        
        Args:
            operation_name: Name of the operation for logging
            
        Returns:
            True when permission is granted (after waiting if necessary)
        """
        async with self._lock:
            now = time.time()
            
            # Remove old requests outside the time window
            while self.requests and self.requests[0] <= now - self.time_window:
                self.requests.popleft()
            
            # Check if we're at the limit
            if len(self.requests) >= self.max_requests:
                # Calculate wait time until oldest request expires
                oldest_request = self.requests[0]
                wait_time = (oldest_request + self.time_window) - now + 1  # +1 second buffer
                
                if wait_time > 0:
                    logger.warning(
                        f"Rate limit reached for {operation_name}. "
                        f"Waiting {wait_time:.1f} seconds. "
                        f"Current requests: {len(self.requests)}/{self.max_requests}"
                    )
                    await asyncio.sleep(wait_time)
                    
                    # Remove expired requests after waiting
                    now = time.time()
                    while self.requests and self.requests[0] <= now - self.time_window:
                        self.requests.popleft()
            
            # Record this request
            self.requests.append(now)
            logger.debug(f"API call permitted for {operation_name}. Current requests: {len(self.requests)}/{self.max_requests}")
            return True
    
    def get_status(self) -> Dict[str, any]:
        """Get current rate limiter status"""
        now = time.time()
        
        # Clean old requests
        while self.requests and self.requests[0] <= now - self.time_window:
            self.requests.popleft()
        
        return {
            "current_requests": len(self.requests),
            "max_requests": self.max_requests,
            "time_window": self.time_window,
            "requests_remaining": self.max_requests - len(self.requests),
            "reset_time": datetime.fromtimestamp(self.requests[0] + self.time_window) if self.requests else None
        }


# Global rate limiter instance
_rate_limiter = None

def get_rate_limiter() -> RateLimiter:
    """Get the global rate limiter instance"""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = RateLimiter()
    return _rate_limiter