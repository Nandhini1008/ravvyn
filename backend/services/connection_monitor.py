"""
Database Connection Pool Monitor
Monitors and manages database connection pool health
"""

import logging
import time
import threading
from typing import Dict, Any
from sqlalchemy.pool import QueuePool
from services.database import engine

logger = logging.getLogger(__name__)


class ConnectionPoolMonitor:
    """Monitor database connection pool health and performance"""
    
    def __init__(self):
        self.stats = {
            'total_connections': 0,
            'active_connections': 0,
            'pool_size': 0,
            'checked_out': 0,
            'overflow': 0,
            'checked_in': 0,
            'last_check': None,
            'pool_timeouts': 0,
            'connection_errors': 0
        }
        self._lock = threading.Lock()
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """Get current connection pool statistics"""
        try:
            with self._lock:
                if hasattr(engine.pool, 'size'):
                    pool = engine.pool
                    self.stats.update({
                        'pool_size': pool.size(),
                        'checked_out': pool.checkedout(),
                        'overflow': pool.overflow(),
                        'checked_in': pool.checkedin(),
                        'last_check': time.time()
                    })
                
                return self.stats.copy()
                
        except Exception as e:
            logger.error(f"Error getting pool stats: {str(e)}")
            return self.stats.copy()
    
    def log_pool_timeout(self):
        """Log a pool timeout event"""
        with self._lock:
            self.stats['pool_timeouts'] += 1
            logger.warning(f"Database pool timeout occurred. Total timeouts: {self.stats['pool_timeouts']}")
    
    def log_connection_error(self, error: str):
        """Log a connection error"""
        with self._lock:
            self.stats['connection_errors'] += 1
            logger.error(f"Database connection error: {error}. Total errors: {self.stats['connection_errors']}")
    
    def is_pool_healthy(self) -> bool:
        """Check if the connection pool is healthy"""
        try:
            stats = self.get_pool_stats()
            
            # Check for warning conditions
            if stats['pool_timeouts'] > 10:
                logger.warning("High number of pool timeouts detected")
                return False
            
            if stats['connection_errors'] > 5:
                logger.warning("High number of connection errors detected")
                return False
            
            # Check pool utilization
            if stats['pool_size'] > 0:
                utilization = stats['checked_out'] / stats['pool_size']
                if utilization > 0.9:
                    logger.warning(f"High pool utilization: {utilization:.1%}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking pool health: {str(e)}")
            return False
    
    def get_health_report(self) -> Dict[str, Any]:
        """Get comprehensive health report"""
        stats = self.get_pool_stats()
        is_healthy = self.is_pool_healthy()
        
        return {
            'healthy': is_healthy,
            'stats': stats,
            'recommendations': self._get_recommendations(stats)
        }
    
    def _get_recommendations(self, stats: Dict[str, Any]) -> List[str]:
        """Get recommendations based on current stats"""
        recommendations = []
        
        if stats['pool_timeouts'] > 5:
            recommendations.append("Consider increasing pool_timeout or pool_size")
        
        if stats['connection_errors'] > 3:
            recommendations.append("Check database connectivity and configuration")
        
        if stats['pool_size'] > 0:
            utilization = stats['checked_out'] / stats['pool_size']
            if utilization > 0.8:
                recommendations.append("Consider increasing pool_size or max_overflow")
        
        if not recommendations:
            recommendations.append("Connection pool is operating normally")
        
        return recommendations


# Global monitor instance
_connection_monitor = None

def get_connection_monitor() -> ConnectionPoolMonitor:
    """Get the global connection monitor instance"""
    global _connection_monitor
    if _connection_monitor is None:
        _connection_monitor = ConnectionPoolMonitor()
    return _connection_monitor