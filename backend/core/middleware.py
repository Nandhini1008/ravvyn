"""
Middleware for FastAPI application
Includes error handling, request logging, and CORS
"""

import time
import uuid
import logging
from typing import Callable
from fastapi import Request, Response, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from .exceptions import RAVVYNException

logger = logging.getLogger(__name__)


class RequestIDMiddleware(BaseHTTPMiddleware):
    """Add request ID to all requests for tracing"""
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        
        response = await call_next(request)
        response.headers['X-Request-ID'] = request_id
        
        return response


class ErrorHandlingMiddleware(BaseHTTPMiddleware):
    """Global error handling middleware"""
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        try:
            response = await call_next(request)
            return response
        except RAVVYNException as e:
            logger.error(
                f"RAVVYNException: {e.error_code} - {e.message}",
                extra={
                    'request_id': getattr(request.state, 'request_id', None),
                    'error_code': e.error_code,
                    'details': e.details
                }
            )
            return JSONResponse(
                status_code=e.status_code,
                content=e.to_dict()
            )
        except Exception as e:
            request_id = getattr(request.state, 'request_id', None)
            logger.exception(
                f"Unhandled exception: {str(e)}",
                extra={'request_id': request_id}
            )
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={
                    'error': 'INTERNAL_SERVER_ERROR',
                    'message': 'An unexpected error occurred',
                    'details': {
                        'request_id': request_id
                    }
                }
            )


class LoggingMiddleware(BaseHTTPMiddleware):
    """Log all requests and responses"""
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()
        request_id = getattr(request.state, 'request_id', None)
        
        # Log request
        logger.info(
            f"Request: {request.method} {request.url.path}",
            extra={
                'request_id': request_id,
                'method': request.method,
                'path': request.url.path,
                'query_params': dict(request.query_params)
            }
        )
        
        try:
            response = await call_next(request)
            process_time = time.time() - start_time
            
            # Log response
            logger.info(
                f"Response: {request.method} {request.url.path} - {response.status_code}",
                extra={
                    'request_id': request_id,
                    'method': request.method,
                    'path': request.url.path,
                    'status_code': response.status_code,
                    'process_time': process_time
                }
            )
            
            response.headers['X-Process-Time'] = str(process_time)
            return response
        except Exception as e:
            process_time = time.time() - start_time
            logger.error(
                f"Request failed: {request.method} {request.url.path} - {str(e)}",
                extra={
                    'request_id': request_id,
                    'method': request.method,
                    'path': request.url.path,
                    'process_time': process_time
                }
            )
            raise


def setup_cors(app, frontend_url: str):
    """Setup CORS middleware"""
    # Allow common tunnel domains for development
    tunnel_domains = [
        "*.ngrok.io",
        "*.ngrok-free.app",
        "*.trycloudflare.com",
        "*.loca.lt",
        "*.serveo.net",
    ]
    
    # Build origins list with localhost and tunnel domains
    allowed_origins = [
        frontend_url,
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ]
    
    # For development, allow all origins (tunnels use random domains)
    # In production, you should restrict this to specific domains
    import os
    if os.getenv("ENVIRONMENT", "development").lower() == "development":
        # Allow all origins in development (includes all tunnel domains)
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],  # Allow all origins in development
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    else:
        # Production: only allow specific origins
        app.add_middleware(
            CORSMiddleware,
            allow_origins=allowed_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )


def setup_middleware(app):
    """Setup all middleware"""
    # Order matters: first added is last executed
    app.add_middleware(ErrorHandlingMiddleware)
    app.add_middleware(LoggingMiddleware)
    app.add_middleware(RequestIDMiddleware)

