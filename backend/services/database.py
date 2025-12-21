"""
Database Service - SQLAlchemy Models and Database Setup
Handles all database operations for sheets, docs, and chat history
"""

from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, JSON, ForeignKey, Index, Boolean, Enum as SQLEnum, event
import uuid
import enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
from datetime import datetime
from typing import Generator
from contextlib import contextmanager
import logging
import threading

logger = logging.getLogger(__name__)

Base = declarative_base()

# Get database URL from settings (with fallback)
try:
    from core.config import get_settings
    settings = get_settings()
    DATABASE_URL = settings.database_url
except Exception as e:
    # Fallback for when settings can't be loaded (e.g., during initial setup)
    import os
    DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///./ravvyn.db')
    logger.warning(f"Could not load settings, using fallback DATABASE_URL: {e}")

# Create engine with proper configuration for SQLite concurrency
if "sqlite" in DATABASE_URL:
    # SQLite-specific configuration optimized for concurrency and connection pooling
    engine = create_engine(
        DATABASE_URL,
        connect_args={
            "check_same_thread": False,
            "timeout": 60,  # Increased SQLite busy timeout
            "isolation_level": None,  # Enable autocommit mode for better concurrency
        },
        echo=False,
        pool_pre_ping=True,
        pool_recycle=1800,  # Longer recycle time for stability
        pool_size=5,  # Increased pool size for concurrent operations
        max_overflow=10,  # Allow overflow connections for peak loads
        pool_timeout=60,  # Increased timeout for getting connection from pool
        pool_reset_on_return='commit'  # Reset connections properly
    )
    
    # Enable WAL mode and configure SQLite for concurrency
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        # Enable WAL mode for better concurrency
        cursor.execute("PRAGMA journal_mode=WAL")
        # Set busy timeout to wait instead of failing immediately
        cursor.execute("PRAGMA busy_timeout=30000")  # 30 seconds
        # Enable foreign key constraints
        cursor.execute("PRAGMA foreign_keys=ON")
        # Optimize for concurrent access
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA cache_size=10000")
        cursor.execute("PRAGMA temp_store=MEMORY")
        cursor.close()
else:
    # PostgreSQL/MySQL configuration
    engine = create_engine(
        DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=5,
        max_overflow=10
    )

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class SheetsMetadata(Base):
    """Metadata for Google Sheets"""
    __tablename__ = 'sheets_metadata'
    
    id = Column(Integer, primary_key=True, index=True)
    sheet_id = Column(String(255), unique=True, nullable=False, index=True)
    sheet_name = Column(String(500), nullable=False)
    created_time = Column(DateTime)
    modified_time = Column(DateTime)
    last_synced = Column(DateTime, default=datetime.utcnow)
    sync_status = Column(String(50), default='pending')  # pending, syncing, completed, error
    error_message = Column(Text, nullable=True)
    
    # Relationship to sheet data
    data_rows = relationship("SheetsData", back_populates="sheet", cascade="all, delete-orphan")


class SheetsData(Base):
    """Individual rows from Google Sheets"""
    __tablename__ = 'sheets_data'
    
    id = Column(Integer, primary_key=True, index=True)
    sheet_id = Column(String(255), ForeignKey('sheets_metadata.sheet_id'), nullable=False, index=True)
    tab_name = Column(String(255), nullable=False, index=True)
    row_index = Column(Integer, nullable=False)  # 0-based row index
    row_data = Column(JSON, nullable=False)  # Store row as JSON array
    synced_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship to metadata
    sheet = relationship("SheetsMetadata", back_populates="data_rows")
    
    # Composite index for efficient queries
    __table_args__ = (
        Index('idx_sheet_tab_row', 'sheet_id', 'tab_name', 'row_index'),
    )


class DocsMetadata(Base):
    """Metadata for Google Docs"""
    __tablename__ = 'docs_metadata'
    
    id = Column(Integer, primary_key=True, index=True)
    doc_id = Column(String(255), unique=True, nullable=False, index=True)
    doc_name = Column(String(500), nullable=False)
    created_time = Column(DateTime)
    modified_time = Column(DateTime)
    last_synced = Column(DateTime, default=datetime.utcnow)
    sync_status = Column(String(50), default='pending')
    error_message = Column(Text, nullable=True)
    
    # Relationship to doc content
    content = relationship("DocsContent", back_populates="doc", uselist=False, cascade="all, delete-orphan")


class DocsContent(Base):
    """Content of Google Docs"""
    __tablename__ = 'docs_content'
    
    id = Column(Integer, primary_key=True, index=True)
    doc_id = Column(String(255), ForeignKey('docs_metadata.doc_id'), unique=True, nullable=False, index=True)
    content = Column(Text, nullable=False)  # Full text content
    content_length = Column(Integer, nullable=False)  # Character count
    synced_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship to metadata
    doc = relationship("DocsMetadata", back_populates="content")


class ChatHistory(Base):
    """Chat conversation history with context"""
    __tablename__ = 'chat_history'
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String(255), nullable=False, index=True)
    conversation_id = Column(String(255), nullable=True, index=True)  # Group related messages
    message = Column(Text, nullable=False)
    response = Column(Text, nullable=False)
    query_type = Column(String(50), nullable=True)  # 'data_query', 'general_chat', 'command'
    context_used = Column(JSON, nullable=True)  # Which sheets/docs were used
    sheet_id = Column(String(255), nullable=True, index=True)
    doc_id = Column(String(255), nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Indexes for efficient queries
    __table_args__ = (
        Index('idx_user_created', 'user_id', 'created_at'),
        Index('idx_conversation', 'conversation_id', 'created_at'),
        Index('idx_user_conversation', 'user_id', 'conversation_id'),
    )


class UserContext(Base):
    """User context - last used sheets/docs"""
    __tablename__ = 'user_context'
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String(255), unique=True, nullable=False, index=True)
    last_sheet_id = Column(String(255), nullable=True)
    last_sheet_name = Column(String(500), nullable=True)
    last_tab_name = Column(String(255), nullable=True)
    last_doc_id = Column(String(255), nullable=True)
    last_doc_name = Column(String(500), nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ConversationContext(Base):
    """Extended conversation context for better RAG"""
    __tablename__ = 'conversation_context'
    
    id = Column(Integer, primary_key=True, index=True)
    conversation_id = Column(String(255), nullable=False, index=True)
    user_id = Column(String(255), nullable=False, index=True)
    active_sheet_id = Column(String(255), nullable=True)
    active_doc_id = Column(String(255), nullable=True)
    active_filters = Column(JSON, nullable=True)  # Current filters applied
    recent_operations = Column(JSON, nullable=True)  # Recent operations performed
    context_summary = Column(Text, nullable=True)  # Summarized context for long conversations
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Indexes
    __table_args__ = (
        Index('idx_conv_user', 'conversation_id', 'user_id'),
        Index('idx_user_updated', 'user_id', 'updated_at'),
    )


class TaskStatus(enum.Enum):
    """Task status enumeration"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    OVERDUE = "overdue"
    CANCELLED = "cancelled"


class TaskPriority(enum.Enum):
    """Task priority enumeration"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    URGENT = "urgent"


class Task(Base):
    """Task management - like Google Tasks"""
    __tablename__ = 'tasks'
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String(255), nullable=False, index=True)
    title = Column(String(500), nullable=False)
    description = Column(Text, nullable=True)
    status = Column(String(50), default='pending', nullable=False, index=True)  # pending, in_progress, completed, overdue, cancelled
    priority = Column(String(50), default='medium', nullable=False)  # low, medium, high, urgent
    due_date = Column(DateTime, nullable=True, index=True)
    completed_at = Column(DateTime, nullable=True)
    reminder_sent = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Indexes for efficient queries
    __table_args__ = (
        Index('idx_user_status', 'user_id', 'status'),
        Index('idx_user_due_date', 'user_id', 'due_date'),
        Index('idx_due_date_status', 'due_date', 'status'),
    )


class FileHash(Base):
    """Stores hash information for files with tab-specific support"""
    __tablename__ = 'file_hashes'
    
    id = Column(Integer, primary_key=True, index=True)
    file_id = Column(String(255), nullable=False, index=True)
    file_type = Column(String(50), nullable=False)  # 'sheet', 'doc', 'pdf'
    tab_name = Column(String(255), nullable=True, index=True)  # Sheet tab name (for sheets only)
    hash_type = Column(String(50), nullable=False)  # 'row', 'block', 'binary'
    hash_value = Column(String(64), nullable=False)  # SHA-256 hash
    content_index = Column(Integer, nullable=True)  # Row index or block index
    content_metadata = Column(JSON, nullable=True)  # Additional metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Indexes for efficient queries
    __table_args__ = (
        Index('idx_file_type', 'file_id', 'file_type'),
        Index('idx_file_tab', 'file_id', 'tab_name'),
        Index('idx_file_hash_type', 'file_id', 'hash_type'),
        Index('idx_file_tab_hash', 'file_id', 'tab_name', 'hash_type'),
        Index('idx_file_content_index', 'file_id', 'content_index'),
        Index('idx_hash_value', 'hash_value'),
    )


class HashComputationLog(Base):
    """Logs hash computation operations for debugging"""
    __tablename__ = 'hash_computation_logs'
    
    id = Column(Integer, primary_key=True, index=True)
    file_id = Column(String(255), nullable=False, index=True)
    operation = Column(String(50), nullable=False)  # 'compute', 'compare', 'store'
    status = Column(String(50), nullable=False)  # 'success', 'error'
    error_message = Column(Text, nullable=True)
    execution_time_ms = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Indexes for efficient queries
    __table_args__ = (
        Index('idx_file_operation', 'file_id', 'operation'),
        Index('idx_status_created', 'status', 'created_at'),
    )


def init_db():
    """Initialize database - create all tables"""
    Base.metadata.create_all(bind=engine)


def get_db() -> Generator[Session, None, None]:
    """
    Dependency injection for FastAPI routes.
    Yields a database session and ensures it's closed after use.
    """
    db = SessionLocal()
    try:
        yield db
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# Global in-memory write lock for SQLite serialization
_sqlite_write_lock = threading.RLock()

@contextmanager
def get_db_context() -> Generator[Session, None, None]:
    """
    Context manager for database sessions with retry logic for SQLite locks.
    Use this for non-FastAPI code that needs a database session.
    """
    import time
    from sqlalchemy.exc import OperationalError
    
    max_retries = 3
    base_delay = 0.1
    
    for attempt in range(max_retries):
        db = SessionLocal()
        try:
            yield db
            db.commit()
            break  # Success, exit retry loop
            
        except OperationalError as e:
            db.rollback()
            if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                # Database lock error - wait and retry
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Database locked, retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(delay)
                db.close()
                continue
            else:
                # Final attempt failed or non-lock error
                db.close()
                raise
                
        except Exception:
            db.rollback()
            db.close()
            raise
            
        finally:
            if db.is_active:
                db.close()

@contextmanager
def get_db_write_context() -> Generator[Session, None, None]:
    """
    Context manager for database write operations with in-memory lock serialization.
    Ensures only one write operation at a time for SQLite.
    """
    with _sqlite_write_lock:
        db = SessionLocal()
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()


def get_db_session() -> Session:
    """
    Get a database session (non-generator version).
    WARNING: Caller is responsible for closing the session.
    Prefer using get_db() or get_db_context() instead.
    """
    logger.warning("get_db_session() called - consider using get_db() or get_db_context() instead")
    return SessionLocal()

