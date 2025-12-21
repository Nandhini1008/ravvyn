"""
Configuration Management for RAVVYN
Validates and provides access to all environment variables
"""

import os
from typing import Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings
from functools import lru_cache
from pathlib import Path


class Settings(BaseSettings):
    """Application settings with validation"""
    
    # AI Provider Configuration
    # Priority: GEMINI > OPENAI > TOGETHER
    ai_provider: str = Field('gemini', env='AI_PROVIDER')  # Options: 'gemini', 'openai', 'together'
    
    # Gemini Configuration (Recommended for cost-effective testing)
    gemini_api_key: Optional[str] = Field(None, env='GEMINI_API_KEY')
    gemini_model: str = Field('gemini-1.5-pro', env='GEMINI_MODEL')  # Upgraded to Pro for ChatGPT-level capabilities
    # Options: 'gemini-1.5-flash' (fast, cheaper), 'gemini-1.5-pro' (best quality), 'gemini-pro'
    
    # OpenAI Configuration
    openai_api_key: Optional[str] = Field(None, env='OPENAI_API_KEY')
    openai_model: str = Field('gpt-4-turbo-preview', env='OPENAI_MODEL')  # Upgraded to GPT-4 for ChatGPT-level capabilities
    # Options: 'gpt-3.5-turbo' (cheaper), 'gpt-4-turbo-preview' (best quality), 'gpt-4'
    
    # Together AI (Optional Fallback)
    together_api_key: Optional[str] = Field(None, env='TOGETHER_API_KEY')
    together_model: str = Field('ServiceNow-AI/Apriel-1.5-15b-Thinker', env='TOGETHER_MODEL')
    
    # Google Credentials
    google_application_credentials: Optional[str] = Field(
        None,
        env='GOOGLE_APPLICATION_CREDENTIALS'
    )
    google_credentials_json: Optional[str] = Field(None, env='GOOGLE_CREDENTIALS_JSON')
    
    # Database
    database_url: str = Field('sqlite:///./ravvyn.db', env='DATABASE_URL')
    
    # Sync Configuration
    sync_interval_minutes: int = Field(3, env='SYNC_INTERVAL_MINUTES', ge=1, le=1440)
    auto_sync_enabled: bool = Field(True, env='AUTO_SYNC_ENABLED')
    
    # Server Configuration
    host: str = Field('0.0.0.0', env='HOST')
    port: int = Field(8000, env='PORT', ge=1, le=65535)
    frontend_url: str = Field('http://localhost:3000', env='FRONTEND_URL')
    
    # Telegram (Optional)
    telegram_bot_token: Optional[str] = Field(None, env='TELEGRAM_BOT_TOKEN')
    telegram_chat_id: Optional[str] = Field(None, env='TELEGRAM_CHAT_ID')
    
    # PDF API (Optional)
    pdf_api_key: Optional[str] = Field(None, env='PDF_API_KEY')
    
    # Logging
    log_level: str = Field('INFO', env='LOG_LEVEL')
    
    # Cache Configuration
    cache_enabled: bool = Field(True, env='CACHE_ENABLED')
    cache_default_ttl: int = Field(3600, env='CACHE_DEFAULT_TTL', ge=60, le=86400)  # 1 hour default, 1 min to 24 hours
    cache_max_size: int = Field(1000, env='CACHE_MAX_SIZE', ge=10, le=10000)  # Max cache entries
    cache_cleanup_interval: int = Field(300, env='CACHE_CLEANUP_INTERVAL', ge=60)  # 5 minutes
    
    # Cache TTLs for specific operations (in seconds)
    cache_ai_ttl: int = Field(1800, env='CACHE_AI_TTL', ge=60)  # 30 minutes for AI responses
    cache_sheets_ttl: int = Field(900, env='CACHE_SHEETS_TTL', ge=60)  # 15 minutes for sheet data
    cache_docs_ttl: int = Field(1800, env='CACHE_DOCS_TTL', ge=60)  # 30 minutes for doc content
    
    # Hash Service Configuration
    hash_enabled: bool = Field(True, env='HASH_ENABLED')  # Enable/disable hash service
    hash_block_size_kb: int = Field(4, env='HASH_BLOCK_SIZE_KB', ge=1, le=64)  # Block size for documents in KB
    hash_pdf_threshold_mb: int = Field(100, env='HASH_PDF_THRESHOLD_MB', ge=1, le=1000)  # PDF size threshold for block-wise hashing in MB
    hash_pdf_block_size_mb: int = Field(2, env='HASH_PDF_BLOCK_SIZE_MB', ge=1, le=10)  # PDF block size in MB
    hash_max_content_size_mb: int = Field(500, env='HASH_MAX_CONTENT_SIZE_MB', ge=1, le=2000)  # Maximum content size in MB
    hash_max_retries: int = Field(3, env='HASH_MAX_RETRIES', ge=1, le=10)  # Maximum retry attempts
    hash_retry_delay_seconds: float = Field(1.0, env='HASH_RETRY_DELAY_SECONDS', ge=0.1, le=60.0)  # Base retry delay
    hash_max_retry_delay_seconds: float = Field(30.0, env='HASH_MAX_RETRY_DELAY_SECONDS', ge=1.0, le=300.0)  # Maximum retry delay
    
    # Content Processing Configuration
    processing_max_concurrent_jobs: int = Field(5, env='PROCESSING_MAX_CONCURRENT_JOBS', ge=1, le=50)  # Max concurrent processing jobs
    processing_job_timeout_seconds: int = Field(300, env='PROCESSING_JOB_TIMEOUT_SECONDS', ge=30, le=3600)  # Job timeout
    processing_batch_size: int = Field(100, env='PROCESSING_BATCH_SIZE', ge=1, le=1000)  # Batch processing size
    processing_cleanup_interval_seconds: int = Field(300, env='PROCESSING_CLEANUP_INTERVAL_SECONDS', ge=60, le=3600)  # Job cleanup interval
    
    @field_validator('google_application_credentials')
    @classmethod
    def validate_google_credentials_path(cls, v):
        """Validate Google credentials file exists if specified"""
        if v and not Path(v).exists():
            # Check relative to backend directory
            backend_dir = Path(__file__).parent.parent
            full_path = backend_dir / v
            if not full_path.exists():
                raise ValueError(f"Google credentials file not found: {v}")
        return v
    
    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, v):
        """Validate log level"""
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of {valid_levels}")
        return v.upper()
    
    def validate_all(self):
        """Validate all required settings"""
        errors = []
        
        # Check AI API key based on provider
        if self.ai_provider == 'gemini':
            if not self.gemini_api_key:
                errors.append("GEMINI_API_KEY must be set when AI_PROVIDER=gemini")
        elif self.ai_provider == 'openai':
            if not self.openai_api_key:
                errors.append("OPENAI_API_KEY must be set when AI_PROVIDER=openai")
        elif self.ai_provider == 'together':
            if not self.together_api_key:
                errors.append("TOGETHER_API_KEY must be set when AI_PROVIDER=together")
        else:
            # Fallback: check if any API key is available
            if not self.gemini_api_key and not self.openai_api_key and not self.together_api_key:
                errors.append("At least one AI API key must be set (GEMINI_API_KEY, OPENAI_API_KEY, or TOGETHER_API_KEY)")
        
        # Check Google credentials
        if not self.google_application_credentials and not self.google_credentials_json:
            # Check if service-account.json exists in credentials folder
            backend_dir = Path(__file__).parent.parent
            creds_path = backend_dir / 'credentials' / 'service-account.json'
            if not creds_path.exists():
                errors.append(
                    "Google credentials not found. Set GOOGLE_APPLICATION_CREDENTIALS, "
                    "GOOGLE_CREDENTIALS_JSON, or place service-account.json in credentials/"
                )
        
        if errors:
            raise ValueError("Configuration errors:\n" + "\n".join(f"  - {e}" for e in errors))
        
        return True
    
    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    settings = Settings()
    settings.validate_all()
    return settings

