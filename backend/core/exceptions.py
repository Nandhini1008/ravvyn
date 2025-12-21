"""
Custom Exception Hierarchy for RAVVYN
Provides structured error handling with error codes and context
"""

from typing import Optional, Dict, Any


class RAVVYNException(Exception):
    """Base exception for all RAVVYN errors"""
    
    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        status_code: int = 500,
        details: Optional[Dict[str, Any]] = None
    ):
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.status_code = status_code
        self.details = details or {}
        super().__init__(self.message)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for API responses"""
        return {
            'error': self.error_code,
            'message': self.message,
            'details': self.details
        }


class ValidationError(RAVVYNException):
    """Raised when input validation fails"""
    
    def __init__(self, message: str, field: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        error_details = details or {}
        if field:
            error_details['field'] = field
        super().__init__(
            message=message,
            error_code='VALIDATION_ERROR',
            status_code=400,
            details=error_details
        )


class NotFoundError(RAVVYNException):
    """Raised when a requested resource is not found"""
    
    def __init__(self, resource_type: str, resource_id: Optional[str] = None):
        message = f"{resource_type} not found"
        if resource_id:
            message += f": {resource_id}"
        super().__init__(
            message=message,
            error_code='NOT_FOUND',
            status_code=404,
            details={'resource_type': resource_type, 'resource_id': resource_id}
        )


class ServiceError(RAVVYNException):
    """Raised when a service operation fails"""
    
    def __init__(self, message: str, service_name: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        error_details = details or {}
        if service_name:
            error_details['service'] = service_name
        super().__init__(
            message=message,
            error_code='SERVICE_ERROR',
            status_code=500,
            details=error_details
        )


class DatabaseError(RAVVYNException):
    """Raised when a database operation fails"""
    
    def __init__(self, message: str, operation: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        error_details = details or {}
        if operation:
            error_details['operation'] = operation
        super().__init__(
            message=message,
            error_code='DATABASE_ERROR',
            status_code=500,
            details=error_details
        )


class ExternalAPIError(RAVVYNException):
    """Raised when an external API call fails"""
    
    def __init__(
        self,
        message: str,
        api_name: Optional[str] = None,
        status_code: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        error_details = details or {}
        if api_name:
            error_details['api'] = api_name
        if status_code:
            error_details['http_status'] = status_code
        
        # Use provided status code or default to 502 (Bad Gateway)
        http_status = status_code if status_code else 502
        super().__init__(
            message=message,
            error_code='EXTERNAL_API_ERROR',
            status_code=http_status,
            details=error_details
        )

