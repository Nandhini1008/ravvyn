"""
API Request/Response Schemas with Validation
Strict validation for all API inputs and outputs
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, List, Dict, Any
import re


# Validation helpers
def validate_sheet_id(value: str) -> str:
    """Validate Google Sheet ID format"""
    if not value or not isinstance(value, str):
        raise ValueError("sheet_id must be a non-empty string")
    if len(value) < 10 or len(value) > 100:
        raise ValueError("sheet_id must be between 10 and 100 characters")
    # Google Sheet IDs are alphanumeric with some special chars
    if not re.match(r'^[a-zA-Z0-9_-]+$', value):
        raise ValueError("sheet_id contains invalid characters")
    return value


def validate_doc_id(value: str) -> str:
    """Validate Google Doc ID format"""
    if not value or not isinstance(value, str):
        raise ValueError("doc_id must be a non-empty string")
    if len(value) < 10 or len(value) > 100:
        raise ValueError("doc_id must be between 10 and 100 characters")
    if not re.match(r'^[a-zA-Z0-9_-]+$', value):
        raise ValueError("doc_id contains invalid characters")
    return value


def validate_tab_name(value: str) -> str:
    """Validate sheet tab name"""
    if not value or not isinstance(value, str):
        raise ValueError("tab_name must be a non-empty string")
    if len(value) > 100:
        raise ValueError("tab_name must be 100 characters or less")
    return value.strip()


def validate_user_id(value: str) -> str:
    """Validate user ID"""
    if not value or not isinstance(value, str):
        raise ValueError("user_id must be a non-empty string")
    if len(value) > 255:
        raise ValueError("user_id must be 255 characters or less")
    return value.strip()


# Chat Schemas
class ChatRequest(BaseModel):
    """Request schema for chat endpoint"""
    message: str = Field(..., min_length=1, max_length=10000, description="User's message")
    user_id: str = Field(default="default", min_length=1, max_length=255, description="User identifier")
    sheet_id: Optional[str] = Field(None, description="Optional sheet ID for context")
    doc_id: Optional[str] = Field(None, description="Optional document ID for context")
    
    @field_validator('message')
    @classmethod
    def validate_message(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("message cannot be empty or whitespace only")
        return v.strip()
    
    @field_validator('sheet_id')
    @classmethod
    def validate_sheet_id(cls, v: Optional[str]) -> Optional[str]:
        if v:
            return validate_sheet_id(v)
        return v
    
    @field_validator('doc_id')
    @classmethod
    def validate_doc_id(cls, v: Optional[str]) -> Optional[str]:
        if v:
            return validate_doc_id(v)
        return v
    
    @field_validator('user_id')
    @classmethod
    def validate_user_id(cls, v: str) -> str:
        return validate_user_id(v)


class ChatResponse(BaseModel):
    """Response schema for chat endpoint"""
    response: str = Field(..., description="AI assistant's response")
    type: str = Field(default="text", description="Response type")


# Sheet Schemas
class SheetRequest(BaseModel):
    """Request schema for sheet operations"""
    action: str = Field(..., description="Action to perform: list, read, write, create")
    sheet_id: Optional[str] = Field(None, description="Sheet ID (required for read, write)")
    tab_name: Optional[str] = Field(None, description="Tab name (required for read, write)")
    data: Optional[List[List[str]]] = Field(None, description="Data to write (required for write)")
    sheet_name: Optional[str] = Field(None, min_length=1, max_length=500, description="Sheet name (required for create)")
    
    @field_validator('action')
    @classmethod
    def validate_action(cls, v: str) -> str:
        valid_actions = ['list', 'read', 'write', 'create']
        if v not in valid_actions:
            raise ValueError(f"action must be one of {valid_actions}")
        return v
    
    @field_validator('sheet_id')
    @classmethod
    def validate_sheet_id(cls, v: Optional[str]) -> Optional[str]:
        if v:
            return validate_sheet_id(v)
        return v
    
    @field_validator('tab_name')
    @classmethod
    def validate_tab_name(cls, v: Optional[str]) -> Optional[str]:
        if v:
            return validate_tab_name(v)
        return v
    
    @field_validator('sheet_name')
    @classmethod
    def validate_sheet_name(cls, v: Optional[str]) -> Optional[str]:
        if v:
            v = v.strip()
            if not v:
                raise ValueError("sheet_name cannot be empty or whitespace only")
            if len(v) > 500:
                raise ValueError("sheet_name must be 500 characters or less")
        return v
    
    @field_validator('data')
    @classmethod
    def validate_data(cls, v: Optional[List[List[str]]]) -> Optional[List[List[str]]]:
        if v is not None:
            if not isinstance(v, list):
                raise ValueError("data must be a list")
            if len(v) == 0:
                raise ValueError("data cannot be empty")
            if len(v) > 10000:
                raise ValueError("data cannot contain more than 10000 rows")
            for i, row in enumerate(v):
                if not isinstance(row, list):
                    raise ValueError(f"data[{i}] must be a list")
                if len(row) > 1000:
                    raise ValueError(f"data[{i}] cannot contain more than 1000 columns")
        return v
    
    @model_validator(mode='after')
    def validate_action_requirements(self):
        """Validate that required fields are present based on action"""
        if self.action == 'read' or self.action == 'write':
            if not self.sheet_id:
                raise ValueError(f"sheet_id is required for action '{self.action}'")
            if not self.tab_name:
                raise ValueError(f"tab_name is required for action '{self.action}'")
        
        if self.action == 'write':
            if not self.data:
                raise ValueError("data is required for action 'write'")
        
        if self.action == 'create':
            if not self.sheet_name:
                raise ValueError("sheet_name is required for action 'create'")
        
        return self


class SheetResponse(BaseModel):
    """Response schema for sheet operations"""
    success: Optional[bool] = Field(None, description="Operation success status")
    sheets: Optional[List[Dict[str, Any]]] = Field(None, description="List of sheets (for list action)")
    data: Optional[List[List[str]]] = Field(None, description="Sheet data (for read action)")
    result: Optional[Dict[str, Any]] = Field(None, description="Operation result")
    sheet: Optional[Dict[str, Any]] = Field(None, description="Created sheet info (for create action)")


# Doc Schemas
class DocRequest(BaseModel):
    """Request schema for doc operations"""
    action: str = Field(..., description="Action to perform: list, read, create, summarize")
    doc_id: Optional[str] = Field(None, description="Document ID (required for read, summarize)")
    doc_name: Optional[str] = Field(None, min_length=1, max_length=500, description="Document name (required for create)")
    
    @field_validator('action')
    @classmethod
    def validate_action(cls, v: str) -> str:
        valid_actions = ['list', 'read', 'create', 'summarize']
        if v not in valid_actions:
            raise ValueError(f"action must be one of {valid_actions}")
        return v
    
    @field_validator('doc_id')
    @classmethod
    def validate_doc_id(cls, v: Optional[str]) -> Optional[str]:
        if v:
            return validate_doc_id(v)
        return v
    
    @field_validator('doc_name')
    @classmethod
    def validate_doc_name(cls, v: Optional[str]) -> Optional[str]:
        if v:
            v = v.strip()
            if not v:
                raise ValueError("doc_name cannot be empty or whitespace only")
            if len(v) > 500:
                raise ValueError("doc_name must be 500 characters or less")
        return v
    
    @model_validator(mode='after')
    def validate_action_requirements(self):
        """Validate that required fields are present based on action"""
        if self.action in ['read', 'summarize']:
            if not self.doc_id:
                raise ValueError(f"doc_id is required for action '{self.action}'")
        
        if self.action == 'create':
            if not self.doc_name:
                raise ValueError("doc_name is required for action 'create'")
        
        return self


class DocResponse(BaseModel):
    """Response schema for doc operations"""
    success: Optional[bool] = Field(None, description="Operation success status")
    docs: Optional[List[Dict[str, Any]]] = Field(None, description="List of docs (for list action)")
    content: Optional[str] = Field(None, description="Document content (for read action)")
    summary: Optional[str] = Field(None, description="Document summary (for summarize action)")
    doc: Optional[Dict[str, Any]] = Field(None, description="Created doc info (for create action)")


# Reminder Schemas
class ReminderRequest(BaseModel):
    """Request schema for reminder operations"""
    action: str = Field(..., description="Action to perform: set, list, delete")
    message: Optional[str] = Field(None, min_length=1, max_length=1000, description="Reminder message (required for set)")
    datetime: Optional[str] = Field(None, description="Reminder datetime in format 'YYYY-MM-DD HH:MM' (required for set)")
    reminder_id: Optional[int] = Field(None, ge=1, description="Reminder ID (required for delete)")
    
    @field_validator('action')
    @classmethod
    def validate_action(cls, v: str) -> str:
        valid_actions = ['set', 'list', 'delete']
        if v not in valid_actions:
            raise ValueError(f"action must be one of {valid_actions}")
        return v
    
    @field_validator('message')
    @classmethod
    def validate_message(cls, v: Optional[str]) -> Optional[str]:
        if v:
            v = v.strip()
            if not v:
                raise ValueError("message cannot be empty or whitespace only")
        return v
    
    @field_validator('datetime')
    @classmethod
    def validate_datetime(cls, v: Optional[str]) -> Optional[str]:
        if v:
            # Validate datetime format: YYYY-MM-DD HH:MM
            datetime_pattern = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$'
            if not re.match(datetime_pattern, v):
                raise ValueError("datetime must be in format 'YYYY-MM-DD HH:MM'")
        return v
    
    @model_validator(mode='after')
    def validate_action_requirements(self):
        """Validate that required fields are present based on action"""
        if self.action == 'set':
            if not self.message:
                raise ValueError("message is required for action 'set'")
            if not self.datetime:
                raise ValueError("datetime is required for action 'set'")
        
        if self.action == 'delete':
            if not self.reminder_id:
                raise ValueError("reminder_id is required for action 'delete'")
        
        return self


class ReminderResponse(BaseModel):
    """Response schema for reminder operations"""
    success: Optional[bool] = Field(None, description="Operation success status")
    reminders: Optional[List[Dict[str, Any]]] = Field(None, description="List of reminders (for list action)")
    reminder: Optional[Dict[str, Any]] = Field(None, description="Created reminder (for set action)")


# Sync Schemas
class SyncRequest(BaseModel):
    """Request schema for sync operations"""
    force: bool = Field(default=False, description="Force sync even if already up to date")


class SyncResponse(BaseModel):
    """Response schema for sync operations"""
    message: str = Field(..., description="Sync status message")
    force: bool = Field(..., description="Whether force sync was requested")


# Sheet CRUD Operation Schemas
class SheetUpdateRequest(BaseModel):
    """Request schema for sheet update operations"""
    sheet_id: str = Field(..., description="Sheet ID")
    tab_name: str = Field(..., description="Tab name")
    row: Optional[int] = Field(None, ge=1, description="Row index (1-based)")
    column: Optional[int] = Field(None, ge=1, description="Column index (1-based, A=1)")
    value: Optional[str] = Field(None, description="Value to set")
    values: Optional[List[List[str]]] = Field(None, description="2D array of values for range update")
    start_row: Optional[int] = Field(None, ge=1, description="Start row for range")
    start_col: Optional[int] = Field(None, ge=1, description="Start column for range")
    end_row: Optional[int] = Field(None, ge=1, description="End row for range")
    end_col: Optional[int] = Field(None, ge=1, description="End column for range")
    
    @field_validator('sheet_id')
    @classmethod
    def validate_sheet_id(cls, v: str) -> str:
        return validate_sheet_id(v)
    
    @field_validator('tab_name')
    @classmethod
    def validate_tab_name(cls, v: str) -> str:
        return validate_tab_name(v)


class SheetDeleteRequest(BaseModel):
    """Request schema for sheet delete operations"""
    sheet_id: str = Field(..., description="Sheet ID")
    tab_name: str = Field(..., description="Tab name")
    start_row: Optional[int] = Field(None, ge=1, description="Start row to delete")
    end_row: Optional[int] = Field(None, ge=1, description="End row to delete")
    start_col: Optional[int] = Field(None, ge=1, description="Start column to delete")
    end_col: Optional[int] = Field(None, ge=1, description="End column to delete")
    
    @field_validator('sheet_id')
    @classmethod
    def validate_sheet_id(cls, v: str) -> str:
        return validate_sheet_id(v)
    
    @field_validator('tab_name')
    @classmethod
    def validate_tab_name(cls, v: str) -> str:
        return validate_tab_name(v)


class SheetInsertRequest(BaseModel):
    """Request schema for sheet insert operations"""
    sheet_id: str = Field(..., description="Sheet ID")
    tab_name: str = Field(..., description="Tab name")
    row_index: int = Field(..., ge=1, description="Row index to insert at (1-based)")
    num_rows: int = Field(1, ge=1, le=1000, description="Number of rows to insert")
    
    @field_validator('sheet_id')
    @classmethod
    def validate_sheet_id(cls, v: str) -> str:
        return validate_sheet_id(v)
    
    @field_validator('tab_name')
    @classmethod
    def validate_tab_name(cls, v: str) -> str:
        return validate_tab_name(v)


# Doc CRUD Operation Schemas
class DocUpdateRequest(BaseModel):
    """Request schema for doc update operations"""
    doc_id: str = Field(..., description="Document ID")
    content: str = Field(..., min_length=1, description="Content to insert/update")
    insert_index: Optional[int] = Field(None, ge=1, description="Index to insert at (if None, appends)")
    
    @field_validator('doc_id')
    @classmethod
    def validate_doc_id(cls, v: str) -> str:
        return validate_doc_id(v)


class DocDeleteRequest(BaseModel):
    """Request schema for doc delete operations"""
    doc_id: str = Field(..., description="Document ID")
    start_index: int = Field(..., ge=1, description="Start character index")
    end_index: int = Field(..., ge=1, description="End character index")
    
    @field_validator('doc_id')
    @classmethod
    def validate_doc_id(cls, v: str) -> str:
        return validate_doc_id(v)
    
    @model_validator(mode='after')
    def validate_indices(self):
        if self.end_index <= self.start_index:
            raise ValueError("end_index must be greater than start_index")
        return self


class DocReplaceRequest(BaseModel):
    """Request schema for doc replace operations"""
    doc_id: str = Field(..., description="Document ID")
    search_text: str = Field(..., min_length=1, description="Text to search for")
    replace_text: str = Field(..., description="Text to replace with")
    
    @field_validator('doc_id')
    @classmethod
    def validate_doc_id(cls, v: str) -> str:
        return validate_doc_id(v)


# Task Schemas
class TaskCreateRequest(BaseModel):
    """Request schema for creating a task"""
    title: str = Field(..., min_length=1, max_length=500, description="Task title")
    description: Optional[str] = Field(None, description="Task description")
    due_date: Optional[str] = Field(None, description="Due date in format 'YYYY-MM-DD HH:MM' or 'YYYY-MM-DD'")
    priority: str = Field('medium', description="Priority: low, medium, high, urgent")
    
    @field_validator('priority')
    @classmethod
    def validate_priority(cls, v: str) -> str:
        valid_priorities = ['low', 'medium', 'high', 'urgent']
        if v not in valid_priorities:
            raise ValueError(f"priority must be one of {valid_priorities}")
        return v


class TaskUpdateRequest(BaseModel):
    """Request schema for updating a task"""
    title: Optional[str] = Field(None, min_length=1, max_length=500, description="Task title")
    description: Optional[str] = Field(None, description="Task description")
    status: Optional[str] = Field(None, description="Status: pending, in_progress, completed, overdue, cancelled")
    priority: Optional[str] = Field(None, description="Priority: low, medium, high, urgent")
    due_date: Optional[str] = Field(None, description="Due date in format 'YYYY-MM-DD HH:MM' or 'YYYY-MM-DD'")
    
    @field_validator('status')
    @classmethod
    def validate_status(cls, v: Optional[str]) -> Optional[str]:
        if v:
            valid_statuses = ['pending', 'in_progress', 'completed', 'overdue', 'cancelled']
            if v not in valid_statuses:
                raise ValueError(f"status must be one of {valid_statuses}")
        return v
    
    @field_validator('priority')
    @classmethod
    def validate_priority(cls, v: Optional[str]) -> Optional[str]:
        if v:
            valid_priorities = ['low', 'medium', 'high', 'urgent']
            if v not in valid_priorities:
                raise ValueError(f"priority must be one of {valid_priorities}")
        return v


class TaskResponse(BaseModel):
    """Response schema for task operations"""
    id: int = Field(..., description="Task ID")
    user_id: str = Field(..., description="User ID")
    title: str = Field(..., description="Task title")
    description: Optional[str] = Field(None, description="Task description")
    status: str = Field(..., description="Task status")
    priority: str = Field(..., description="Task priority")
    due_date: Optional[str] = Field(None, description="Due date")
    completed_at: Optional[str] = Field(None, description="Completion date")
    created_at: str = Field(..., description="Creation date")
    updated_at: str = Field(..., description="Last update date")


class TasksListResponse(BaseModel):
    """Response schema for listing tasks"""
    tasks: List[TaskResponse] = Field(..., description="List of tasks")
    total: int = Field(..., description="Total number of tasks")


# Export Schemas
class ExportToSheetRequest(BaseModel):
    """Request schema for exporting to sheet"""
    data: Any = Field(..., description="Data to export (list, dict, or string)")
    sheet_name: str = Field(..., min_length=1, max_length=500, description="Name for the new sheet")
    tab_name: str = Field('Sheet1', description="Name for the tab")
    sheet_id: Optional[str] = Field(None, description="Existing sheet ID (if exporting to existing sheet)")
    append: bool = Field(False, description="If True, append to existing sheet")
    
    @field_validator('sheet_id')
    @classmethod
    def validate_sheet_id(cls, v: Optional[str]) -> Optional[str]:
        if v:
            return validate_sheet_id(v)
        return v


class ExportToDocRequest(BaseModel):
    """Request schema for exporting to doc"""
    content: str = Field(..., min_length=1, description="Content to export")
    doc_name: str = Field(..., min_length=1, max_length=500, description="Name for the new document")
    doc_id: Optional[str] = Field(None, description="Existing doc ID (if exporting to existing doc)")
    append: bool = Field(True, description="If True, append to existing doc")
    
    @field_validator('doc_id')
    @classmethod
    def validate_doc_id(cls, v: Optional[str]) -> Optional[str]:
        if v:
            return validate_doc_id(v)
        return v


class ExportChatRequest(BaseModel):
    """Request schema for exporting chat conversation"""
    conversation_id: str = Field(..., description="Conversation ID")
    format: str = Field('sheet', description="Export format: sheet or doc")
    name: Optional[str] = Field(None, description="Optional name for the exported file")
    
    @field_validator('format')
    @classmethod
    def validate_format(cls, v: str) -> str:
        if v not in ['sheet', 'doc']:
            raise ValueError("format must be 'sheet' or 'doc'")
        return v


# Health Check Schema
class HealthCheckResponse(BaseModel):
    """Response schema for health check"""
    status: str = Field(..., description="Health status")
    timestamp: str = Field(..., description="Timestamp of health check")
    checks: Dict[str, str] = Field(..., description="Individual service checks")


# Sync Status Schema
class SyncStatusResponse(BaseModel):
    """Response schema for sync status"""
    sheets: List[Dict[str, Any]] = Field(..., description="Sheet sync status")
    docs: List[Dict[str, Any]] = Field(..., description="Doc sync status")


# Hash Service Schemas
class HashComputeRequest(BaseModel):
    """Request schema for hash computation"""
    file_id: str = Field(..., description="File identifier")
    file_type: str = Field(..., description="File type: sheet, doc, or pdf")
    tab_name: Optional[str] = Field(None, description="Tab name for sheets")
    source_type: Optional[str] = Field("url", description="Source type for PDFs: url, file, or bytes")


class HashComputeResponse(BaseModel):
    """Response schema for hash computation"""
    success: bool = Field(..., description="Operation success status")
    file_id: str = Field(..., description="File identifier")
    file_type: str = Field(..., description="File type")
    hash_count: Optional[int] = Field(None, description="Number of hashes computed")
    computation_time_ms: Optional[int] = Field(None, description="Computation time in milliseconds")
    total_size: Optional[int] = Field(None, description="Total content size")
    has_changes: Optional[bool] = Field(None, description="Whether changes were detected")
    change_summary: Optional[Dict[str, Any]] = Field(None, description="Summary of detected changes")
    error: Optional[str] = Field(None, description="Error message if failed")


class HashStatusResponse(BaseModel):
    """Response schema for hash status"""
    file_id: str = Field(..., description="File identifier")
    hash_count: int = Field(..., description="Number of stored hashes")
    last_updated: Optional[str] = Field(None, description="Last update timestamp")
    file_type: Optional[str] = Field(None, description="File type")


class HashStatisticsResponse(BaseModel):
    """Response schema for hash statistics"""
    service_status: str = Field(..., description="Service status")
    configuration: Dict[str, Any] = Field(..., description="Service configuration")
    storage_statistics: Dict[str, Any] = Field(..., description="Storage statistics")


class BatchProcessRequest(BaseModel):
    """Request schema for batch processing"""
    files: List[Dict[str, Any]] = Field(..., description="List of files to process")
    operation: str = Field("hash_and_detect", description="Operation to perform")


class BatchProcessResponse(BaseModel):
    """Response schema for batch processing"""
    success: bool = Field(..., description="Operation success status")
    total_jobs: int = Field(..., description="Total number of jobs")
    completed_jobs: int = Field(..., description="Number of completed jobs")
    failed_jobs: int = Field(..., description="Number of failed jobs")
    total_time_seconds: float = Field(..., description="Total processing time")
    results: List[Dict[str, Any]] = Field(..., description="Processing results")
    errors: List[Dict[str, Any]] = Field(..., description="Processing errors")


class JobStatusResponse(BaseModel):
    """Response schema for job status"""
    job_id: str = Field(..., description="Job identifier")
    file_id: str = Field(..., description="File identifier")
    file_type: str = Field(..., description="File type")
    operation: str = Field(..., description="Operation type")
    status: str = Field(..., description="Job status")
    progress: float = Field(..., description="Progress percentage (0.0 to 1.0)")
    created_at: float = Field(..., description="Creation timestamp")
    started_at: Optional[float] = Field(None, description="Start timestamp")
    completed_at: Optional[float] = Field(None, description="Completion timestamp")
    result: Optional[Dict[str, Any]] = Field(None, description="Job result")
    error: Optional[str] = Field(None, description="Error message if failed")


# Error Response Schema
class ErrorResponse(BaseModel):
    """Standard error response schema"""
    error: str = Field(..., description="Error code")
    message: str = Field(..., description="Error message")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")

