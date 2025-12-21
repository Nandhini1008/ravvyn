"""
Hash Monitoring Service - Performance metrics and health monitoring
Tracks hash service performance, error rates, and system health
"""

import logging
import time
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict, deque
from threading import Lock
from sqlalchemy.orm import Session

from services.database import get_db_context, HashComputationLog
from services.hash_service import HashService

logger = logging.getLogger(__name__)


@dataclass
class MetricPoint:
    """Represents a single metric data point"""
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class HealthCheck:
    """Represents a health check result"""
    name: str
    status: str  # 'healthy', 'warning', 'critical'
    message: str
    timestamp: float
    details: Dict[str, Any] = field(default_factory=dict)


class HashMonitoring:
    """
    Hash service monitoring and metrics collection.
    Tracks performance, errors, and system health.
    """
    
    def __init__(self, hash_service: Optional[HashService] = None):
        """Initialize monitoring service"""
        self.hash_service = hash_service
        
        # Metrics storage (in-memory for now)
        self._metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self._counters: Dict[str, int] = defaultdict(int)
        self._gauges: Dict[str, float] = defaultdict(float)
        self._histograms: Dict[str, List[float]] = defaultdict(list)
        
        # Health checks
        self._health_checks: Dict[str, HealthCheck] = {}
        
        # Thread safety
        self._lock = Lock()
        
        # Configuration
        self.metric_retention_hours = 24
        self.health_check_interval_seconds = 60
        self.error_rate_threshold = 0.1  # 10% error rate threshold
        self.response_time_threshold_ms = 5000  # 5 second response time threshold
        
        logger.info("Hash monitoring service initialized")
    
    def record_hash_computation(self, file_id: str, file_type: str, 
                              computation_time_ms: int, hash_count: int, 
                              success: bool, error: str = None):
        """
        Record hash computation metrics.
        
        Args:
            file_id: File identifier
            file_type: Type of file
            computation_time_ms: Computation time in milliseconds
            hash_count: Number of hashes computed
            success: Whether computation was successful
            error: Error message if failed
        """
        try:
            with self._lock:
                timestamp = time.time()
                
                # Record computation time
                self._metrics['hash_computation_time'].append(
                    MetricPoint(timestamp, computation_time_ms, {'file_type': file_type})
                )
                
                # Record hash count
                self._metrics['hash_count'].append(
                    MetricPoint(timestamp, hash_count, {'file_type': file_type})
                )
                
                # Update counters
                self._counters[f'hash_computations_total_{file_type}'] += 1
                if success:
                    self._counters[f'hash_computations_success_{file_type}'] += 1
                else:
                    self._counters[f'hash_computations_error_{file_type}'] += 1
                
                # Update gauges
                self._gauges['last_computation_time_ms'] = computation_time_ms
                self._gauges['last_hash_count'] = hash_count
                
                # Update histograms
                self._histograms[f'computation_time_{file_type}'].append(computation_time_ms)
                if len(self._histograms[f'computation_time_{file_type}']) > 1000:
                    self._histograms[f'computation_time_{file_type}'] = self._histograms[f'computation_time_{file_type}'][-1000:]
                
                logger.debug(f"Recorded hash computation metrics: {file_type}, {computation_time_ms}ms, {hash_count} hashes, success={success}")
                
        except Exception as e:
            logger.error(f"Error recording hash computation metrics: {str(e)}")
    
    def record_change_detection(self, file_id: str, file_type: str, 
                              has_changes: bool, change_count: int):
        """
        Record change detection metrics.
        
        Args:
            file_id: File identifier
            file_type: Type of file
            has_changes: Whether changes were detected
            change_count: Number of changes detected
        """
        try:
            with self._lock:
                timestamp = time.time()
                
                # Record change detection
                self._metrics['change_detection'].append(
                    MetricPoint(timestamp, 1 if has_changes else 0, {'file_type': file_type})
                )
                
                # Record change count
                if has_changes:
                    self._metrics['change_count'].append(
                        MetricPoint(timestamp, change_count, {'file_type': file_type})
                    )
                
                # Update counters
                self._counters[f'change_detections_total_{file_type}'] += 1
                if has_changes:
                    self._counters[f'changes_detected_{file_type}'] += 1
                
                logger.debug(f"Recorded change detection metrics: {file_type}, changes={has_changes}, count={change_count}")
                
        except Exception as e:
            logger.error(f"Error recording change detection metrics: {str(e)}")
    
    def record_storage_operation(self, operation: str, file_type: str, 
                                success: bool, duration_ms: int):
        """
        Record storage operation metrics.
        
        Args:
            operation: Operation type ('save', 'load', 'delete')
            file_type: Type of file
            success: Whether operation was successful
            duration_ms: Operation duration in milliseconds
        """
        try:
            with self._lock:
                timestamp = time.time()
                
                # Record operation duration
                self._metrics[f'storage_{operation}_time'].append(
                    MetricPoint(timestamp, duration_ms, {'file_type': file_type})
                )
                
                # Update counters
                self._counters[f'storage_{operation}_total_{file_type}'] += 1
                if success:
                    self._counters[f'storage_{operation}_success_{file_type}'] += 1
                else:
                    self._counters[f'storage_{operation}_error_{file_type}'] += 1
                
                logger.debug(f"Recorded storage operation metrics: {operation}, {file_type}, success={success}, {duration_ms}ms")
                
        except Exception as e:
            logger.error(f"Error recording storage operation metrics: {str(e)}")
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive metrics summary.
        
        Returns:
            Dictionary with metrics summary
        """
        try:
            with self._lock:
                # Calculate error rates
                error_rates = {}
                for file_type in ['sheet', 'doc', 'pdf']:
                    total_key = f'hash_computations_total_{file_type}'
                    error_key = f'hash_computations_error_{file_type}'
                    
                    total = self._counters.get(total_key, 0)
                    errors = self._counters.get(error_key, 0)
                    
                    error_rates[file_type] = (errors / total * 100) if total > 0 else 0
                
                # Calculate average response times
                avg_response_times = {}
                for file_type in ['sheet', 'doc', 'pdf']:
                    times = self._histograms.get(f'computation_time_{file_type}', [])
                    avg_response_times[file_type] = sum(times) / len(times) if times else 0
                
                # Get recent metrics (last hour)
                cutoff_time = time.time() - 3600
                recent_computations = sum(
                    1 for metric_list in self._metrics.values()
                    for point in metric_list
                    if point.timestamp > cutoff_time
                )
                
                return {
                    'counters': dict(self._counters),
                    'gauges': dict(self._gauges),
                    'error_rates': error_rates,
                    'average_response_times_ms': avg_response_times,
                    'recent_computations_1h': recent_computations,
                    'total_metrics_points': sum(len(deque_obj) for deque_obj in self._metrics.values()),
                    'timestamp': time.time()
                }
                
        except Exception as e:
            logger.error(f"Error getting metrics summary: {str(e)}")
            return {'error': str(e), 'timestamp': time.time()}
    
    def get_performance_metrics(self, time_range_hours: int = 1) -> Dict[str, Any]:
        """
        Get performance metrics for specified time range.
        
        Args:
            time_range_hours: Time range in hours
            
        Returns:
            Dictionary with performance metrics
        """
        try:
            with self._lock:
                cutoff_time = time.time() - (time_range_hours * 3600)
                
                # Filter metrics by time range
                filtered_metrics = {}
                for metric_name, metric_list in self._metrics.items():
                    filtered_points = [point for point in metric_list if point.timestamp > cutoff_time]
                    if filtered_points:
                        filtered_metrics[metric_name] = filtered_points
                
                # Calculate statistics
                stats = {}
                for metric_name, points in filtered_metrics.items():
                    values = [point.value for point in points]
                    if values:
                        stats[metric_name] = {
                            'count': len(values),
                            'min': min(values),
                            'max': max(values),
                            'avg': sum(values) / len(values),
                            'latest': values[-1] if values else 0
                        }
                
                return {
                    'time_range_hours': time_range_hours,
                    'statistics': stats,
                    'timestamp': time.time()
                }
                
        except Exception as e:
            logger.error(f"Error getting performance metrics: {str(e)}")
            return {'error': str(e), 'timestamp': time.time()}
    
    async def run_health_checks(self) -> Dict[str, HealthCheck]:
        """
        Run comprehensive health checks.
        
        Returns:
            Dictionary of health check results
        """
        try:
            health_checks = {}
            
            # Check hash service availability
            if self.hash_service:
                try:
                    stats = await self.hash_service.get_service_statistics()
                    if stats.get('service_status') == 'active':
                        health_checks['hash_service'] = HealthCheck(
                            name='hash_service',
                            status='healthy',
                            message='Hash service is active and responding',
                            timestamp=time.time(),
                            details=stats
                        )
                    else:
                        health_checks['hash_service'] = HealthCheck(
                            name='hash_service',
                            status='critical',
                            message='Hash service is not active',
                            timestamp=time.time(),
                            details=stats
                        )
                except Exception as e:
                    health_checks['hash_service'] = HealthCheck(
                        name='hash_service',
                        status='critical',
                        message=f'Hash service check failed: {str(e)}',
                        timestamp=time.time()
                    )
            
            # Check error rates
            error_rates = {}
            for file_type in ['sheet', 'doc', 'pdf']:
                total_key = f'hash_computations_total_{file_type}'
                error_key = f'hash_computations_error_{file_type}'
                
                total = self._counters.get(total_key, 0)
                errors = self._counters.get(error_key, 0)
                
                if total > 0:
                    error_rate = errors / total
                    error_rates[file_type] = error_rate
                    
                    if error_rate > self.error_rate_threshold:
                        health_checks[f'error_rate_{file_type}'] = HealthCheck(
                            name=f'error_rate_{file_type}',
                            status='warning',
                            message=f'High error rate for {file_type}: {error_rate:.2%}',
                            timestamp=time.time(),
                            details={'error_rate': error_rate, 'threshold': self.error_rate_threshold}
                        )
                    else:
                        health_checks[f'error_rate_{file_type}'] = HealthCheck(
                            name=f'error_rate_{file_type}',
                            status='healthy',
                            message=f'Error rate for {file_type} is within threshold: {error_rate:.2%}',
                            timestamp=time.time(),
                            details={'error_rate': error_rate, 'threshold': self.error_rate_threshold}
                        )
            
            # Check response times
            for file_type in ['sheet', 'doc', 'pdf']:
                times = self._histograms.get(f'computation_time_{file_type}', [])
                if times:
                    avg_time = sum(times) / len(times)
                    if avg_time > self.response_time_threshold_ms:
                        health_checks[f'response_time_{file_type}'] = HealthCheck(
                            name=f'response_time_{file_type}',
                            status='warning',
                            message=f'High response time for {file_type}: {avg_time:.0f}ms',
                            timestamp=time.time(),
                            details={'avg_response_time_ms': avg_time, 'threshold_ms': self.response_time_threshold_ms}
                        )
                    else:
                        health_checks[f'response_time_{file_type}'] = HealthCheck(
                            name=f'response_time_{file_type}',
                            status='healthy',
                            message=f'Response time for {file_type} is within threshold: {avg_time:.0f}ms',
                            timestamp=time.time(),
                            details={'avg_response_time_ms': avg_time, 'threshold_ms': self.response_time_threshold_ms}
                        )
            
            # Check database connectivity
            try:
                with get_db_context() as db:
                    # Try a simple query
                    db.execute("SELECT 1")
                    health_checks['database'] = HealthCheck(
                        name='database',
                        status='healthy',
                        message='Database is accessible',
                        timestamp=time.time()
                    )
            except Exception as e:
                health_checks['database'] = HealthCheck(
                    name='database',
                    status='critical',
                    message=f'Database connectivity failed: {str(e)}',
                    timestamp=time.time()
                )
            
            # Update stored health checks
            with self._lock:
                self._health_checks.update(health_checks)
            
            return health_checks
            
        except Exception as e:
            logger.error(f"Error running health checks: {str(e)}")
            return {
                'health_check_error': HealthCheck(
                    name='health_check_error',
                    status='critical',
                    message=f'Health check system failed: {str(e)}',
                    timestamp=time.time()
                )
            }
    
    async def get_database_metrics(self) -> Dict[str, Any]:
        """
        Get database-related metrics from hash computation logs.
        
        Returns:
            Dictionary with database metrics
        """
        try:
            with get_db_context() as db:
                from sqlalchemy import func
                
                # Get recent operations (last 24 hours)
                recent_cutoff = datetime.utcnow() - timedelta(hours=24)
                
                # Count operations by status
                operation_counts = db.query(
                    HashComputationLog.operation,
                    HashComputationLog.status,
                    func.count(HashComputationLog.id).label('count')
                ).filter(
                    HashComputationLog.created_at >= recent_cutoff
                ).group_by(
                    HashComputationLog.operation,
                    HashComputationLog.status
                ).all()
                
                # Average execution times
                avg_times = db.query(
                    HashComputationLog.operation,
                    func.avg(HashComputationLog.execution_time_ms).label('avg_time')
                ).filter(
                    HashComputationLog.created_at >= recent_cutoff,
                    HashComputationLog.execution_time_ms.isnot(None)
                ).group_by(
                    HashComputationLog.operation
                ).all()
                
                # Recent error logs
                recent_errors = db.query(HashComputationLog).filter(
                    HashComputationLog.created_at >= recent_cutoff,
                    HashComputationLog.status == 'error'
                ).order_by(HashComputationLog.created_at.desc()).limit(10).all()
                
                return {
                    'operation_counts': [
                        {'operation': op, 'status': status, 'count': count}
                        for op, status, count in operation_counts
                    ],
                    'average_execution_times': [
                        {'operation': op, 'avg_time_ms': float(avg_time) if avg_time else 0}
                        for op, avg_time in avg_times
                    ],
                    'recent_errors': [
                        {
                            'file_id': error.file_id,
                            'operation': error.operation,
                            'error_message': error.error_message,
                            'created_at': error.created_at.isoformat()
                        }
                        for error in recent_errors
                    ],
                    'timestamp': time.time()
                }
                
        except Exception as e:
            logger.error(f"Error getting database metrics: {str(e)}")
            return {'error': str(e), 'timestamp': time.time()}
    
    def cleanup_old_metrics(self):
        """Clean up old metrics data"""
        try:
            with self._lock:
                cutoff_time = time.time() - (self.metric_retention_hours * 3600)
                
                # Clean up metrics
                for metric_name, metric_list in self._metrics.items():
                    # Remove old points
                    while metric_list and metric_list[0].timestamp < cutoff_time:
                        metric_list.popleft()
                
                # Clean up histograms
                for hist_name, hist_list in self._histograms.items():
                    if len(hist_list) > 1000:
                        self._histograms[hist_name] = hist_list[-1000:]
                
                logger.debug("Cleaned up old metrics data")
                
        except Exception as e:
            logger.error(f"Error cleaning up metrics: {str(e)}")
    
    def get_monitoring_status(self) -> Dict[str, Any]:
        """
        Get overall monitoring service status.
        
        Returns:
            Dictionary with monitoring status
        """
        try:
            with self._lock:
                return {
                    'service_status': 'active',
                    'configuration': {
                        'metric_retention_hours': self.metric_retention_hours,
                        'health_check_interval_seconds': self.health_check_interval_seconds,
                        'error_rate_threshold': self.error_rate_threshold,
                        'response_time_threshold_ms': self.response_time_threshold_ms
                    },
                    'metrics_storage': {
                        'total_metric_types': len(self._metrics),
                        'total_data_points': sum(len(deque_obj) for deque_obj in self._metrics.values()),
                        'counter_count': len(self._counters),
                        'gauge_count': len(self._gauges),
                        'histogram_count': len(self._histograms)
                    },
                    'health_checks': {
                        'total_checks': len(self._health_checks),
                        'last_run': max((hc.timestamp for hc in self._health_checks.values()), default=0)
                    },
                    'timestamp': time.time()
                }
                
        except Exception as e:
            logger.error(f"Error getting monitoring status: {str(e)}")
            return {
                'service_status': 'error',
                'error': str(e),
                'timestamp': time.time()
            }