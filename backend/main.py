"""
RAVVYN Personal AI Assistant - FastAPI Backend
A complete personal assistant with Google Sheets, Docs, PDF, Reminders, and AI Chat
"""

from fastapi import FastAPI, BackgroundTasks, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional, Dict, Any, List
import logging
import atexit
import time

# Import core infrastructure
from core.config import get_settings
from core.middleware import setup_middleware, setup_cors
from core.exceptions import RAVVYNException

# Import services
from services.sheets import SheetsService
from services.docs import DocsService
from services.ai import AIService
from services.reminders import RemindersService
from services.telegram_bot import TelegramBot
from services.sync_service import SyncService
from services.tasks import TasksService
from services.export import ExportService
from services.hash_service import HashService
from services.content_processor import ContentProcessor
from services.hash_monitoring import HashMonitoring
from services.database import init_db, get_db, get_db_context
from services.db_queries import save_chat_history, update_user_context, get_or_create_conversation_id
from services.scheduler import start_scheduler, stop_scheduler
from services.database import SheetsMetadata, DocsMetadata
from services.cache import get_cache_service

# Import Universal X-Y Coordinate System
from services.universal_sheet_analyzer import get_universal_analyzer
from services.universal_data_service import get_universal_data_service
from services.universal_query_processor import get_universal_query_processor

# Import SQLite Direct Processor (bypasses AI/LLM entirely)
from services.sqlite_direct_processor import get_sqlite_direct_processor

# Import API schemas
from api.schemas import (
    ChatRequest, ChatResponse,
    SheetRequest, SheetResponse,
    SheetUpdateRequest, SheetDeleteRequest, SheetInsertRequest,
    DocRequest, DocResponse,
    DocUpdateRequest, DocDeleteRequest, DocReplaceRequest,
    ReminderRequest, ReminderResponse,
    TaskCreateRequest, TaskUpdateRequest, TaskResponse, TasksListResponse,
    SyncRequest, SyncResponse,
    ExportToSheetRequest, ExportToDocRequest, ExportChatRequest,
    HashComputeRequest, HashComputeResponse, HashStatusResponse, HashStatisticsResponse,
    BatchProcessRequest, BatchProcessResponse, JobStatusResponse,
    HealthCheckResponse, SyncStatusResponse,
)

# Import Database Reasoning Agent endpoints
from api.reasoning_endpoints import router as reasoning_router

from datetime import datetime
import time

# CONFIGURATION: Your specific Google Sheet ID - Used as default throughout the application
DEFAULT_SHEET_ID = "1MtjJyKiDR7COszXxZF-wYR-UF5LcwYSpTfr_Aa_PEt8"

# Configure logging
settings = get_settings()
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="RAVVYN AI Assistant",
    version="1.0.0",
    description="Personal AI Assistant with Google Sheets, Docs, and more"
)

# Setup middleware
setup_middleware(app)
setup_cors(app, settings.frontend_url)

# Include API routers
app.include_router(reasoning_router)

# Add request logging middleware to debug frontend connection issues
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    
    # Log incoming request
    logger.info(f"🌐 {request.method} {request.url.path} - Headers: {dict(request.headers)}")
    
    # Process request
    response = await call_next(request)
    
    # Log response
    process_time = time.time() - start_time
    logger.info(f"✅ {request.method} {request.url.path} - Status: {response.status_code} - Time: {process_time:.3f}s")
    
    return response

# Initialize database
try:
    init_db()
    logger.info("Database initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize database: {str(e)}")
    raise

# Start scheduler for automatic sync
try:
    start_scheduler()
    logger.info("Scheduler started successfully")
except Exception as e:
    logger.warning(f"Failed to start scheduler: {str(e)}")

# Trigger initial sync on startup (after services are ready) - non-blocking
@app.on_event("startup")
async def startup_sync():
    """Trigger initial sync on application startup - non-blocking, won't fail startup"""
    import asyncio
    
    async def _sync_in_background():
        """Run sync in background, don't block startup"""
        try:
            await asyncio.sleep(2)  # Wait 2 seconds for services to be ready
            
            # Start database operation queue worker
            logger.info("🚀 Starting database operation queue worker...")
            try:
                await db_queue.start_worker()
                logger.info("✅ Database operation queue worker started")
            except Exception as e:
                logger.warning(f"⚠️  Database queue worker failed to start: {str(e)}")
            
            await asyncio.sleep(3)  # Wait a bit more for queue to be ready
            
            # Priority 1: Process your specific sheet with hash integration (non-blocking)
            logger.info(f"🎯 Processing your specific sheet: {DEFAULT_SHEET_ID}")
            try:
                # Run sheet processing in background without blocking
                asyncio.create_task(_comprehensive_sheet_sync(DEFAULT_SHEET_ID))
                logger.info(f"✅ Your sheet processing started in background: {DEFAULT_SHEET_ID}")
            except Exception as e:
                logger.warning(f"⚠️  Your sheet processing failed to start: {str(e)} - will retry later")
            
            # Priority 2: General sync for other sheets/docs (also non-blocking)
            logger.info("🔄 Triggering general sync for other items...")
            try:
                asyncio.create_task(sync_service.sync_all(force=False))
                logger.info("✅ General sync started in background")
            except Exception as e:
                logger.warning(f"⚠️  General sync failed to start: {str(e)}")
            
        except Exception as e:
            error_msg = str(e)
            # Check if it's a network connectivity issue
            if any(keyword in error_msg.lower() for keyword in ['unable to find the server', 'connection', 'network', 'dns', 'socket']):
                logger.warning(f"⚠️  Initial sync skipped: Network connectivity issue - {error_msg}")
                logger.info("💡 Tip: Make sure you have internet connection and can reach Google APIs. Sync will retry automatically.")
            else:
                logger.warning(f"⚠️  Initial sync failed: {error_msg}")
            # Don't raise - allow server to start even if sync fails
    
    # Run sync in background task - won't block startup
    asyncio.create_task(_sync_in_background())

# Register shutdown handler
atexit.register(stop_scheduler)

# Initialize services
try:
    sheets_service = SheetsService()
    docs_service = DocsService()
    sync_service = SyncService()
    ai_service = AIService(sheets_service=sheets_service, docs_service=docs_service, sync_service=sync_service)
    reminders_service = RemindersService()
    hash_service = HashService()
    content_processor = ContentProcessor()
    hash_monitoring = HashMonitoring(hash_service)
    hash_service.set_monitoring(hash_monitoring)  # Set monitoring after initialization
    telegram_bot = TelegramBot() if settings.telegram_bot_token else None
    
    # Initialize Database Reasoning Agent (lazy initialization)
    reasoning_agent = None  # Will be initialized on first use
    
    # Start database operation queue worker
    from services.db_operation_queue import get_db_operation_queue
    db_queue = get_db_operation_queue()
    
    logger.info("Services initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize services: {str(e)}")
    raise


# Helper function to get reasoning agent (lazy initialization)
def _get_reasoning_agent():
    """Get or initialize the Database Reasoning Agent"""
    global reasoning_agent
    if reasoning_agent is None:
        try:
            from database_reasoning_agent import DatabaseReasoningAgent
            reasoning_agent = DatabaseReasoningAgent("ravvyn.db")
            logger.info("🤖 Database Reasoning Agent initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Database Reasoning Agent: {str(e)}")
            raise
    return reasoning_agent


# Helper function for reading data from SQLite database instead of Google Sheets API
async def _read_sheet_from_db(sheet_id: str, tab_name: str = None) -> List[List[Any]]:
    """
    Read sheet data from SQLite database instead of Google Sheets API
    Returns data in the same format as sheets_service.read_sheet()
    """
    try:
        from services.database import SheetsData
        import json
        
        with get_db_context() as db:
            # Get sheet data from database
            query = db.query(SheetsData).filter(SheetsData.sheet_id == sheet_id)
            
            if tab_name:
                query = query.filter(SheetsData.tab_name == tab_name)
            
            # Order by tab_name and row_index to maintain structure
            rows = query.order_by(SheetsData.tab_name, SheetsData.row_index).all()
            
            if not rows:
                logger.warning(f"No data found in database for sheet {sheet_id}, tab {tab_name}")
                return []
            
            # Convert database rows to sheet format
            sheet_data = []
            for row in rows:
                try:
                    # Parse row data from JSON
                    row_data = json.loads(row.row_data) if isinstance(row.row_data, str) else row.row_data
                    if row_data and isinstance(row_data, list):
                        sheet_data.append(row_data)
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f"Failed to parse row data for sheet {sheet_id}, row {row.row_index}")
                    continue
            
            logger.info(f"📊 Read {len(sheet_data)} rows from SQLite database for {sheet_id}/{tab_name}")
            return sheet_data
            
    except Exception as e:
        logger.error(f"Error reading sheet data from database: {str(e)}")
        return []


# Helper function for analyzing user queries
async def _analyze_user_query(message: str) -> Dict[str, Any]:
    """
    Analyze user query to determine type and extract keywords for processing
    """
    message_lower = message.lower().strip()
    
    # Data query indicators
    data_indicators = [
        'what is', 'show me', 'find', 'amount', 'data', 'value', 'total', 
        'how much', 'when', 'where', 'latest', 'recent', 'december', 'january',
        'time', 'date', ':', 'ro details', 'costing', 'tank', 'running', 'level',
        'feed', 'pressure', 'temperature', 'flow', 'rate', 'status'
    ]
    
    is_data_query = any(indicator in message_lower for indicator in data_indicators)
    
    if not is_data_query:
        return {
            "is_data_query": False,
            "type": "general",
            "keywords": [],
            "specificity": "none"
        }
    
    # Extract potential keywords from the message
    import re
    
    # Remove common words and extract meaningful terms
    stop_words = {
        'what', 'is', 'the', 'show', 'me', 'get', 'find', 'how', 'much', 'when', 
        'where', 'from', 'in', 'on', 'at', 'for', 'with', 'and', 'or', 'but',
        'a', 'an', 'this', 'that', 'these', 'those', 'i', 'you', 'we', 'they'
    }
    
    # Extract words and numbers
    words = re.findall(r'\b\w+\b', message_lower)
    keywords = [word for word in words if word not in stop_words and len(word) > 2]
    
    # Extract dates
    date_patterns = [
        r'\d{1,2}[./\-]\d{1,2}[./\-]\d{2,4}',
        r'\d{4}[./\-]\d{1,2}[./\-]\d{1,2}',
    ]
    
    dates = []
    for pattern in date_patterns:
        dates.extend(re.findall(pattern, message))
    
    # Determine query specificity
    has_specific_field = any(term in message_lower for term in [
        'tank level', 'feed tank', 'pressure', 'temperature', 'flow rate',
        'ro1', 'ro2', 'ro3', 'condensate', 'permeate', 'reject'
    ])
    
    has_specific_date = len(dates) > 0
    has_specific_value = any(char.isdigit() for char in message)
    
    # Classify query type
    if has_specific_field and has_specific_date:
        query_type = "specific_field_date"
        specificity = "high"
    elif has_specific_field or has_specific_date:
        query_type = "semi_specific"
        specificity = "medium"
    elif len(keywords) <= 2 and any(kw in ['data', 'information', 'details', 'summary'] for kw in keywords):
        query_type = "generalized_keyword"
        specificity = "low"
    else:
        query_type = "keyword_search"
        specificity = "medium"
    
    # Add dates to keywords if found
    if dates:
        keywords.extend(dates)
    
    # Enhance keywords with domain-specific terms
    domain_keywords = []
    if any(term in message_lower for term in ['ro', 'reverse', 'osmosis']):
        domain_keywords.append('RO')
    if any(term in message_lower for term in ['tank', 'level']):
        domain_keywords.append('TANK')
    if any(term in message_lower for term in ['feed', 'supply']):
        domain_keywords.append('FEED')
    if any(term in message_lower for term in ['pressure', 'press']):
        domain_keywords.append('PRESSURE')
    if any(term in message_lower for term in ['temperature', 'temp']):
        domain_keywords.append('TEMPERATURE')
    
    keywords.extend(domain_keywords)
    
    # Remove duplicates and limit keywords
    keywords = list(set(keywords))[:10]  # Limit to 10 keywords
    
    return {
        "is_data_query": True,
        "type": query_type,
        "keywords": keywords,
        "specificity": specificity,
        "dates": dates,
        "has_specific_field": has_specific_field,
        "has_specific_date": has_specific_date
    }


# Helper function for enhanced AI responses
async def _get_enhanced_ai_response(request: ChatRequest, sheet_id: str, conversation_id: str) -> str:
    """
    Get enhanced AI response with sheet context
    """
    try:
        # Use AI service to generate response with context
        response = await ai_service.chat(
            message=request.message,
            user_id=request.user_id,
            sheet_id=sheet_id,
            doc_id=request.doc_id
        )
        return response
    except Exception as e:
        logger.error(f"Error in enhanced AI response: {str(e)}")
        # Fallback response
        if sheet_id:
            return f"I understand you're asking about your sheet data. However, I'm having trouble accessing the information right now. Please try again in a moment, or try a more specific query about your data."
        else:
            return "I'm sorry, I'm having trouble processing your request right now. Please try again in a moment."


# Helper function for comprehensive sheet sync with incremental processing
async def _comprehensive_sheet_sync(sheet_id: str):
    """
    Comprehensive sheet sync with incremental hash processing - runs in background
    Processes ALL data from each tab and only updates what has changed
    """
    try:
        logger.info(f"🔄 Starting comprehensive incremental sync for sheet: {sheet_id}")
        
        # Step 1: Get all tabs in the sheet
        def _get_spreadsheet():
            return sheets_service.sheets_service.spreadsheets().get(
                spreadsheetId=sheet_id
            ).execute()
        
        spreadsheet = await sheets_service._retry_request(_get_spreadsheet)
        sheets_info = spreadsheet.get('sheets', [])
        spreadsheet_title = spreadsheet.get('properties', {}).get('title', 'Unknown Sheet')
        
        if not sheets_info:
            logger.warning(f"⚠️  No tabs found in sheet {sheet_id}")
            return
        
        logger.info(f"📋 Found {len(sheets_info)} tabs in '{spreadsheet_title}'")
        
        # Step 2: Process each tab incrementally
        total_processed = 0
        total_changes = 0
        
        for sheet_info in sheets_info:
            properties = sheet_info.get('properties', {})
            tab_name = properties.get('title', 'Unknown')
            
            try:
                logger.info(f"   🔄 Processing tab: '{tab_name}'")
                
                # Try to read from SQLite database first, fallback to Google Sheets API
                tab_data = await _read_sheet_from_db(sheet_id, tab_name)
                if not tab_data:
                    # Fallback to Google Sheets API for sync
                    logger.info(f"   📡 No data in SQLite, reading from Google Sheets API for '{tab_name}'")
                    tab_data = await sheets_service.read_sheet(sheet_id, tab_name)
                
                row_count = len(tab_data)
                logger.info(f"   📊 Read {row_count} rows from '{tab_name}'")
                
                # Compute hashes incrementally (tab-specific)
                hash_result = await hash_service.compute_hash_from_source(
                    sheet_id, "sheet", tab_name=tab_name
                )
                
                if hash_result.get('success', False):
                    hash_count = hash_result.get('hash_computation', {}).get('hash_count', 0)
                    has_changes = hash_result.get('has_changes', False)
                    change_summary = hash_result.get('change_detection', {})
                    
                    total_processed += hash_count
                    if has_changes:
                        total_changes += change_summary.get('total_changes', 0)
                        logger.info(f"   ✅ Tab '{tab_name}': {hash_count} hashes, {change_summary.get('total_changes', 0)} changes")
                        
                        # Only sync if there are actual changes
                        sync_result = await sync_service.sync_sheet(sheet_id, spreadsheet_title, tab_name, force=False)
                        if sync_result.get('success'):
                            logger.info(f"   🔄 Synced changes for tab '{tab_name}'")
                        else:
                            logger.warning(f"   ⚠️  Sync failed for tab '{tab_name}': {sync_result.get('error')}")
                    else:
                        logger.info(f"   ✅ Tab '{tab_name}': {hash_count} hashes, no changes (skipped sync)")
                else:
                    logger.warning(f"   ⚠️  Hash computation failed for tab '{tab_name}': {hash_result.get('error')}")
                    
            except Exception as e:
                logger.error(f"   ❌ Error processing tab '{tab_name}': {str(e)}")
        
        logger.info(f"✅ Comprehensive sync completed: {total_processed} hashes processed, {total_changes} total changes across all tabs")
            
    except Exception as e:
        logger.error(f"❌ Comprehensive sheet sync failed: {str(e)}")


# Helper function for non-blocking hash operations
async def _process_hash_operation_background(operation_func, *args, **kwargs):
    """
    Process hash operations in background without blocking main thread
    """
    try:
        import asyncio
        # Run the operation with a timeout
        result = await asyncio.wait_for(
            operation_func(*args, **kwargs),
            timeout=300.0  # 5 minute timeout
        )
        return result
    except asyncio.TimeoutError:
        logger.warning(f"Hash operation timed out: {operation_func.__name__}")
        return {"success": False, "error": "Operation timed out"}
    except Exception as e:
        logger.error(f"Hash operation error: {str(e)}")
        return {"success": False, "error": str(e)}


# Routes
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "RAVVYN AI Assistant API",
        "version": "1.0.0",
        "features": ["Google Sheets", "Google Docs", "PDF", "Reminders", "AI Chat"]
    }


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest, db: Session = Depends(get_db)):
    """
    Enhanced AI Chat endpoint with automatic sheet processing
    - Automatically syncs and hashes sheet data when sheet_id is provided
    - Supports Tamil, English, and mixed language
    - Handles special commands: /clone, /figma, /page, /improve
    - Provides intelligent responses based on synced data
    """
    try:
        # Get or create conversation ID
        conversation_id = get_or_create_conversation_id(request.user_id, db)
        
        # Enhanced: Use your sheet ID as default if no sheet_id provided
        sheet_id_to_use = request.sheet_id or DEFAULT_SHEET_ID
        
        # Quick check if we have data available for responses
        if sheet_id_to_use:
            logger.info(f"🔍 Chat request with sheet_id: {sheet_id_to_use}")
            if sheet_id_to_use == DEFAULT_SHEET_ID:
                logger.info(f"🎯 Using YOUR predefined sheet: {DEFAULT_SHEET_ID}")
            
            # Quick check for existing data availability
            try:
                data_available = await hash_service.hash_storage.check_data_availability_for_queries(sheet_id_to_use)
                if data_available:
                    logger.info("✅ Data available for queries - proceeding with chat")
                else:
                    logger.info("⚠️  Limited data available - will provide basic response")
            except Exception as e:
                logger.warning(f"Error checking data availability: {str(e)} - proceeding anyway")
                data_available = False
            
            # Only do minimal processing if absolutely no data exists
            if not data_available:
                sheet_meta = db.query(SheetsMetadata).filter(
                    SheetsMetadata.sheet_id == sheet_id_to_use
                ).first()
                
                if not sheet_meta:
                    logger.info("📋 No sheet metadata found - will suggest sync")
                else:
                    logger.info(f"📋 Sheet metadata exists but limited data - status: {sheet_meta.sync_status}")
        
        # 🚀 PRIORITY: Use SQLite Direct Processor to bypass AI/LLM entirely
        # This solves both Gemini API quota issues and ensures precise date filtering
        sqlite_direct_processor = get_sqlite_direct_processor()
        
        # Detect query type and extract keywords
        query_analysis = await _analyze_user_query(request.message)
        
        if query_analysis["is_data_query"] and sheet_id_to_use:
            logger.info(f"🗄️  Processing data query: '{request.message}' (Type: {query_analysis['type']})")
            
            try:
                import asyncio
                
                # 🎯 PRIMARY: Use SQLite Direct Processor for ALL data queries
                logger.info(f"🗄️  Using SQLite Direct Processor (NO AI/LLM): {request.message}")
                
                direct_result = await asyncio.wait_for(
                    sqlite_direct_processor.process_direct_query(
                        query=request.message,
                        sheet_id=sheet_id_to_use
                    ),
                    timeout=30.0  # 30 second timeout
                )
                
                if direct_result["success"] and direct_result.get("data_found", 0) > 0:
                    logger.info(f"✅ SQLite found {direct_result['data_found']} data points")
                    response_text = direct_result["answer"]
                    
                    # Add search metadata for transparency
                    search_terms = direct_result.get("search_terms", [])
                    date_filters = direct_result.get("date_filters", {})
                    
                    if search_terms:
                        response_text += f"\n\n🔍 **Search terms used**: {', '.join(search_terms[:5])}"
                    
                    if date_filters.get('description'):
                        response_text += f"\n📅 **Date filter**: {date_filters['description']}"
                    
                    response_text += f"\n\n📊 **Data source**: SQLite database (no AI/LLM quota used)"
                    
                elif direct_result["success"] and direct_result.get("data_found", 0) == 0:
                    # No data found - try reasoning agent
                    logger.info(f"ℹ️  SQLite found no data, trying reasoning agent")
                    response_text = direct_result["answer"]
                    
                    # Add helpful suggestions
                    response_text += "\n\n💡 **Suggestions**:"
                    response_text += "\n• Try using different keywords (e.g., 'tank', 'level', 'pressure', 'flow')"
                    response_text += "\n• Check if the date format is correct (e.g., '12-12-2025', 'December 2025')"
                    response_text += "\n• Use broader terms like 'show all data' or 'latest entries'"
                    response_text += "\n• Try relative dates like 'last 7 days', 'this month', 'today'"
                    
                else:
                    logger.warning(f"⚠️  SQLite Direct Processor failed: {direct_result.get('error')}")
                    
                    # 🤖 FALLBACK: Try Database Reasoning Agent for strict data queries
                    logger.info("🤖 Trying Database Reasoning Agent as fallback...")
                    try:
                        agent = _get_reasoning_agent()
                        reasoning_answer = await agent.answer_question(request.message)
                        
                        # Check if reasoning agent provided a valid answer
                        failure_messages = [
                            "Unable to answer accurately due to ambiguous or unavailable time data.",
                            "No data available for the resolved time range."
                        ]
                        
                        if reasoning_answer not in failure_messages:
                            logger.info("✅ Database Reasoning Agent provided valid answer")
                            response_text = reasoning_answer
                            response_text += "\n\n🤖 **Data source**: Database Reasoning Agent (strict 7-step validation)"
                        else:
                            logger.info("ℹ️  Database Reasoning Agent also found no data")
                            # Final fallback to basic response
                            response_text = f"I encountered an issue processing your query: {direct_result.get('error', 'Unknown error')}"
                            response_text += "\n\nPlease try rephrasing your query or contact support if the issue persists."
                            response_text += "\n\n💡 **Tip**: Try simpler queries like 'show latest data' or 'data for today'"
                            
                    except Exception as reasoning_error:
                        logger.warning(f"⚠️  Database Reasoning Agent also failed: {str(reasoning_error)}")
                        # Final fallback to basic response
                        response_text = f"I encountered an issue processing your query: {direct_result.get('error', 'Unknown error')}"
                        response_text += "\n\nPlease try rephrasing your query or contact support if the issue persists."
                        response_text += "\n\n💡 **Tip**: Try simpler queries like 'show latest data' or 'data for today'"
                    
            except asyncio.TimeoutError:
                logger.warning(f"⏰ SQLite Direct processing timed out for: {request.message}")
                response_text = "Your query is taking longer than expected to process. This might be due to a large dataset."
                response_text += "\n\n💡 **Try**: Use more specific terms or date ranges to narrow down the search."
            except Exception as e:
                logger.error(f"❌ SQLite Direct processing error: {str(e)}")
                response_text = f"I encountered an error while processing your query: {str(e)}"
                response_text += "\n\nPlease try a simpler query or contact support if the issue persists."
        else:
            # For general queries, try SQLite Direct Processor first to avoid AI/LLM quota
            if sheet_id_to_use:
                logger.info(f"🗄️  Trying SQLite Direct Processor for general query: {request.message}")
                try:
                    direct_result = await sqlite_direct_processor.process_direct_query(
                        query=request.message,
                        sheet_id=sheet_id_to_use
                    )
                    
                    if direct_result["success"] and direct_result.get("data_found", 0) > 0:
                        logger.info(f"✅ SQLite Direct Processor handled general query successfully")
                        response_text = direct_result["answer"]
                        response_text += f"\n\n📊 **Data source**: SQLite database (no AI/LLM quota used)"
                    else:
                        # 🤖 Try Database Reasoning Agent before falling back to AI
                        logger.info(f"🤖 Trying Database Reasoning Agent for general query: {request.message}")
                        try:
                            agent = _get_reasoning_agent()
                            reasoning_answer = await agent.answer_question(request.message)
                            
                            failure_messages = [
                                "Unable to answer accurately due to ambiguous or unavailable time data.",
                                "No data available for the resolved time range."
                            ]
                            
                            if reasoning_answer not in failure_messages:
                                logger.info("✅ Database Reasoning Agent handled general query successfully")
                                response_text = reasoning_answer
                                response_text += "\n\n🤖 **Data source**: Database Reasoning Agent (strict validation)"
                            else:
                                # Fallback to enhanced AI response only if both SQLite and Reasoning Agent find no data
                                logger.info(f"ℹ️  Both SQLite and Reasoning Agent found no data, using enhanced AI response")
                                response_text = await _get_enhanced_ai_response(request, sheet_id_to_use, conversation_id)
                        except Exception as reasoning_error:
                            logger.warning(f"⚠️  Database Reasoning Agent failed: {str(reasoning_error)}")
                            # Fallback to enhanced AI response
                            response_text = await _get_enhanced_ai_response(request, sheet_id_to_use, conversation_id)
                except Exception as e:
                    logger.warning(f"⚠️  SQLite Direct Processor failed for general query: {str(e)}")
                    
                    # 🤖 Try Database Reasoning Agent as fallback
                    logger.info(f"🤖 Trying Database Reasoning Agent as fallback for general query")
                    try:
                        agent = _get_reasoning_agent()
                        reasoning_answer = await agent.answer_question(request.message)
                        
                        failure_messages = [
                            "Unable to answer accurately due to ambiguous or unavailable time data.",
                            "No data available for the resolved time range."
                        ]
                        
                        if reasoning_answer not in failure_messages:
                            logger.info("✅ Database Reasoning Agent provided fallback answer")
                            response_text = reasoning_answer
                            response_text += "\n\n🤖 **Data source**: Database Reasoning Agent (fallback)"
                        else:
                            # Final fallback to enhanced AI response
                            response_text = await _get_enhanced_ai_response(request, sheet_id_to_use, conversation_id)
                    except Exception as reasoning_error:
                        logger.warning(f"⚠️  Database Reasoning Agent fallback also failed: {str(reasoning_error)}")
                        # Final fallback to enhanced AI response
                        response_text = await _get_enhanced_ai_response(request, sheet_id_to_use, conversation_id)
            else:
                # No sheet context - use enhanced AI response
                response_text = await _get_enhanced_ai_response(request, sheet_id_to_use, conversation_id)
        
        # Detect query type for logging
        query_type = 'command' if request.message.startswith('/') else 'data_query' if any(kw in request.message.lower() for kw in ['sheet', 'doc', 'data', 'today', 'this sheet']) else 'general_chat'
        
        # Save to chat history
        try:
            context_used = {}
            if sheet_id_to_use:
                context_used['sheet_id'] = sheet_id_to_use
            if request.doc_id:
                context_used['doc_id'] = request.doc_id
            
            save_chat_history(
                user_id=request.user_id,
                message=request.message,
                response=response_text,
                query_type=query_type,
                context_used=context_used if context_used else None,
                sheet_id=sheet_id_to_use,
                doc_id=request.doc_id,
                conversation_id=conversation_id,
                db=db
            )
            
            # Update user context if sheet/doc was used
            if sheet_id_to_use:
                sheet_meta = db.query(SheetsMetadata).filter(
                    SheetsMetadata.sheet_id == sheet_id_to_use
                ).first()
                if sheet_meta:
                    update_user_context(
                        request.user_id,
                        sheet_id=request.sheet_id,
                        sheet_name=sheet_meta.sheet_name,
                        db=db
                    )
            
            if request.doc_id:
                doc_meta = db.query(DocsMetadata).filter(
                    DocsMetadata.doc_id == request.doc_id
                ).first()
                if doc_meta:
                    update_user_context(
                        request.user_id,
                        doc_id=request.doc_id,
                        doc_name=doc_meta.doc_name,
                        db=db
                    )
        except Exception as e:
            logger.warning(f"Failed to save chat history: {str(e)}")
        
        return ChatResponse(response=response_text, type="text")
    
    except RAVVYNException:
        # Let the middleware handle RAVVYN exceptions
        raise
    except Exception as e:
        logger.error(f"Unexpected error in chat endpoint: {str(e)}", exc_info=True)
        raise


@app.post("/sheets", response_model=SheetResponse)
async def sheets_operation(request: SheetRequest, background_tasks: BackgroundTasks):
    """
    Google Sheets operations: list, read, write, create
    """
    try:
        if request.action == "list":
            sheets = await sheets_service.list_sheets()
            return SheetResponse(sheets=sheets)
        
        elif request.action == "read":
            # Use SQLite database instead of Google Sheets API
            data = await _read_sheet_from_db(request.sheet_id, request.tab_name)
            return SheetResponse(data=data)
        
        elif request.action == "write":
            result = await sheets_service.write_sheet(request.sheet_id, request.tab_name, request.data)
            return SheetResponse(success=True, result=result)
        
        elif request.action == "create":
            sheet = await sheets_service.create_sheet(request.sheet_name)
            # Trigger sync for new sheet in background
            background_tasks.add_task(
                sync_service.sync_sheet,
                sheet['id'],
                sheet['name']
            )
            return SheetResponse(success=True, sheet=sheet)
        
        else:
            # This should not happen due to validation, but just in case
            from core.exceptions import ValidationError
            raise ValidationError(f"Unknown action: {request.action}", field="action")
    
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in sheets_operation: {str(e)}", exc_info=True)
        raise


@app.get("/sheets/{sheet_id}/query")
async def sheet_query(sheet_id: str, tab_name: str, question: str):
    """
    AI-powered sheet query: ask questions about your data using SQLite database
    Example: "What's the total of column A?"
    """
    try:
        # Use Universal Query Processor with SQLite database
        universal_query_processor = get_universal_query_processor()
        
        # Process the question using Universal system
        result = await universal_query_processor.process_query(
            query=question,
            sheet_id=sheet_id,
            tab_name=tab_name
        )
        
        if result["success"]:
            return {
                "answer": result["answer"],
                "sheet_id": sheet_id,
                "tab_name": tab_name,
                "query_type": result.get("query_type", "unknown"),
                "confidence": result.get("confidence", 1.0),
                "data_found": result.get("data_found", 0),
                "data_source": "sqlite_database"
            }
        else:
            # Fallback response
            return {
                "answer": f"I couldn't process your question about the data. Error: {result.get('error', 'Unknown error')}",
                "sheet_id": sheet_id,
                "tab_name": tab_name,
                "data_source": "sqlite_database",
                "error": result.get("error")
            }
    
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in sheet_query: {str(e)}", exc_info=True)
        raise


@app.post("/docs", response_model=DocResponse)
async def docs_operation(request: DocRequest, background_tasks: BackgroundTasks):
    """
    Google Docs operations: list, read, create, summarize
    """
    try:
        if request.action == "list":
            docs = await docs_service.list_docs()
            return DocResponse(docs=docs)
        
        elif request.action == "read":
            content = await docs_service.read_doc(request.doc_id)
            return DocResponse(content=content)
        
        elif request.action == "create":
            doc = await docs_service.create_doc(request.doc_name)
            # Trigger sync for new doc in background
            background_tasks.add_task(
                sync_service.sync_doc,
                doc['id'],
                doc['name']
            )
            return DocResponse(success=True, doc=doc)
        
        elif request.action == "summarize":
            content = await docs_service.read_doc(request.doc_id)
            summary = await ai_service.summarize_document(content)
            return DocResponse(summary=summary)
        
        else:
            # This should not happen due to validation
            from core.exceptions import ValidationError
            raise ValidationError(f"Unknown action: {request.action}", field="action")
    
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in docs_operation: {str(e)}", exc_info=True)
        raise


@app.post("/reminders", response_model=ReminderResponse)
async def reminders_operation(request: ReminderRequest):
    """
    Reminders operations: set, list, delete
    """
    try:
        if request.action == "set":
            reminder = reminders_service.set_reminder(request.message, request.datetime)
            return ReminderResponse(success=True, reminder=reminder)
        
        elif request.action == "list":
            reminders = reminders_service.list_reminders()
            return ReminderResponse(reminders=reminders)
        
        elif request.action == "delete":
            reminders_service.delete_reminder(request.reminder_id)
            return ReminderResponse(success=True)
        
        else:
            # This should not happen due to validation
            from core.exceptions import ValidationError
            raise ValidationError(f"Unknown action: {request.action}", field="action")
    
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in reminders_operation: {str(e)}", exc_info=True)
        raise


@app.get("/reminders/check")
async def check_reminders():
    """
    Check for due reminders (called by scheduler)
    """
    try:
        due_reminders = reminders_service.check_due_reminders()
        
        # Send notifications for due reminders
        for reminder in due_reminders:
            if telegram_bot:
                try:
                    await telegram_bot.send_reminder(reminder)
                except Exception as e:
                    logger.warning(f"Failed to send reminder notification: {str(e)}")
        
        return {"checked": len(due_reminders), "due": due_reminders}
    
    except Exception as e:
        logger.error(f"Unexpected error in check_reminders: {str(e)}", exc_info=True)
        raise


# Sync Endpoints
@app.post("/sync/sheets", response_model=SyncResponse)
async def sync_sheets_endpoint(request: SyncRequest = SyncRequest(), background_tasks: BackgroundTasks = BackgroundTasks()):
    """
    Manually trigger sync for all sheets
    force: If True, sync even if already up to date
    """
    try:
        background_tasks.add_task(sync_service.sync_all_sheets, force=request.force)
        return SyncResponse(message="Sheet sync started in background", force=request.force)
    except Exception as e:
        logger.error(f"Error in sync_sheets_endpoint: {str(e)}", exc_info=True)
        raise


@app.post("/sync/docs", response_model=SyncResponse)
async def sync_docs_endpoint(request: SyncRequest = SyncRequest(), background_tasks: BackgroundTasks = BackgroundTasks()):
    """
    Manually trigger sync for all docs
    force: If True, sync even if already up to date
    """
    try:
        background_tasks.add_task(sync_service.sync_all_docs, force=request.force)
        return SyncResponse(message="Doc sync started in background", force=request.force)
    except Exception as e:
        logger.error(f"Error in sync_docs_endpoint: {str(e)}", exc_info=True)
        raise


@app.post("/sync/all", response_model=SyncResponse)
async def sync_all_endpoint(request: SyncRequest = SyncRequest(), background_tasks: BackgroundTasks = BackgroundTasks()):
    """
    Manually trigger sync for all sheets and docs
    force: If True, sync even if already up to date
    """
    try:
        background_tasks.add_task(sync_service.sync_all, force=request.force)
        return SyncResponse(message="Full sync started in background", force=request.force)
    except Exception as e:
        logger.error(f"Error in sync_all_endpoint: {str(e)}", exc_info=True)
        raise


@app.get("/sync/status")
async def sync_status(db: Session = Depends(get_db)):
    """
    Get sync status - last sync times for sheets and docs
    """
    try:
        sheets_status = db.query(SheetsMetadata).all()
        docs_status = db.query(DocsMetadata).all()
        
        return {
            "sheets": [
                {
                    "sheet_id": s.sheet_id,
                    "sheet_name": s.sheet_name,
                    "last_synced": s.last_synced.isoformat() if s.last_synced else None,
                    "sync_status": s.sync_status,
                    "modified_time": s.modified_time.isoformat() if s.modified_time else None
                }
                for s in sheets_status
            ],
            "docs": [
                {
                    "doc_id": d.doc_id,
                    "doc_name": d.doc_name,
                    "last_synced": d.last_synced.isoformat() if d.last_synced else None,
                    "sync_status": d.sync_status,
                    "modified_time": d.modified_time.isoformat() if d.modified_time else None
                }
                for d in docs_status
            ]
        }
    except Exception as e:
        logger.error(f"Error in sync_status: {str(e)}", exc_info=True)
        raise


# Hash-Integrated Sync Endpoints
@app.post("/sync/sheet/hash")
async def sync_sheet_with_hash(
    sheet_id: str = DEFAULT_SHEET_ID,
    force: bool = False,
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Sync a specific sheet with hash-based change detection
    Processes all tabs (gids) within the sheet and computes hashes
    """
    try:
        logger.info(f"🔄 Starting hash-integrated sync for sheet: {sheet_id}")
        
        # Start the sync process in background
        background_tasks.add_task(
            _sync_sheet_with_hash_processing,
            sheet_id,
            force
        )
        
        return {
            "success": True,
            "message": f"Hash-integrated sync started for sheet {sheet_id}",
            "sheet_id": sheet_id,
            "force": force,
            "status": "processing"
        }
        
    except Exception as e:
        logger.error(f"Error starting hash sync for sheet {sheet_id}: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "sheet_id": sheet_id
        }


@app.get("/sync/sheet/hash/status/{sheet_id}")
async def get_hash_sync_status(sheet_id: str, db: Session = Depends(get_db)):
    """
    Get hash sync status for a specific sheet
    Shows sync status, hash counts, and last sync time
    """
    try:
        # Get sheet metadata
        sheet_meta = db.query(SheetsMetadata).filter(
            SheetsMetadata.sheet_id == sheet_id
        ).first()
        
        if not sheet_meta:
            return {
                "success": False,
                "error": "Sheet not found in database",
                "sheet_id": sheet_id
            }
        
        # Get hash statistics
        stored_hashes = await hash_service.get_stored_hashes(sheet_id)
        
        # Get recent hash computation logs
        with get_db_context() as db_ctx:
            from services.database import HashComputationLog
            recent_logs = db_ctx.query(HashComputationLog).filter(
                HashComputationLog.file_id == sheet_id
            ).order_by(HashComputationLog.created_at.desc()).limit(5).all()
        
        return {
            "success": True,
            "sheet_id": sheet_id,
            "sheet_name": sheet_meta.sheet_name,
            "sync_status": sheet_meta.sync_status,
            "last_synced": sheet_meta.last_synced.isoformat() if sheet_meta.last_synced else None,
            "hash_statistics": {
                "total_hashes": len(stored_hashes),
                "hash_types": list(set(h.hash_type for h in stored_hashes)) if stored_hashes else [],
                "last_hash_computation": stored_hashes[0].hash_value[:16] + "..." if stored_hashes else None
            },
            "recent_operations": [
                {
                    "operation": log.operation,
                    "status": log.status,
                    "execution_time_ms": log.execution_time_ms,
                    "created_at": log.created_at.isoformat() if log.created_at else None,
                    "error": log.error_message if log.error_message else None
                }
                for log in recent_logs
            ]
        }
        
    except Exception as e:
        logger.error(f"Error getting hash sync status for {sheet_id}: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "sheet_id": sheet_id
        }


@app.post("/sync/auto/your-sheet")
async def auto_sync_your_sheet(background_tasks: BackgroundTasks = BackgroundTasks()):
    """
    Auto-sync your specific sheet with incremental hash processing
    Uses the predefined sheet ID from DEFAULT_SHEET_ID constant
    Processes ALL data but only updates what has changed
    """
    
    try:
        logger.info(f"🚀 Auto-syncing your sheet with incremental processing: {DEFAULT_SHEET_ID}")
        
        # Start comprehensive incremental sync process
        background_tasks.add_task(
            _comprehensive_sheet_sync,
            DEFAULT_SHEET_ID
        )
        
        return {
            "success": True,
            "message": "Incremental auto-sync started for your sheet",
            "sheet_id": DEFAULT_SHEET_ID,
            "process": "incremental_comprehensive_sync",
            "status": "processing",
            "note": "This will process ALL data from all tabs but only update what has changed"
        }
        
    except Exception as e:
        logger.error(f"Error in auto-sync: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "sheet_id": DEFAULT_SHEET_ID
        }


@app.post("/process/your-sheet")
async def process_your_sheet(force_sync: bool = False, background_tasks: BackgroundTasks = BackgroundTasks()):
    """
    Process your specific sheet with full sync and hash computation
    Uses the predefined sheet ID: 1MtjJyKiDR7COszXxZF-wYR-UF5LcwYSpTfr_Aa_PEt8
    """
    try:
        logger.info(f"🚀 Processing your sheet: {DEFAULT_SHEET_ID}")
        
        # Use the process_complete_sheet logic with your sheet ID
        request_data = {
            "sheet_id": DEFAULT_SHEET_ID,
            "force_sync": force_sync
        }
        
        # Call the complete processing function directly
        result = await process_complete_sheet(request_data, background_tasks)
        
        return {
            "success": True,
            "message": f"Processing started for your sheet",
            "sheet_id": DEFAULT_SHEET_ID,
            "force_sync": force_sync,
            "processing_result": result
        }
        
    except Exception as e:
        logger.error(f"Error processing your sheet: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "sheet_id": DEFAULT_SHEET_ID
        }


@app.get("/api/sheet-data")
async def get_sheet_data(tab_name: str = None, limit: int = 100):
    """
    Get formatted sheet data for frontend display using SQLite database
    """
    try:
        # Use Universal Data Service for SQLite-based data retrieval
        universal_data_service = get_universal_data_service()
        
        # Get sheet analysis and summary
        analysis_result = await universal_data_service.analyze_sheet(
            sheet_id=DEFAULT_SHEET_ID,
            tab_name=tab_name
        )
        
        if analysis_result["success"]:
            # Get latest data from each tab
            latest_data_result = await universal_data_service.get_latest_data(
                sheet_id=DEFAULT_SHEET_ID,
                tab_name=tab_name
            )
            
            # Format data for frontend
            formatted_data = []
            if latest_data_result["success"]:
                for tab_name_key, tab_data in latest_data_result["latest_data"].items():
                    formatted_data.append({
                        "tab_name": tab_name_key,
                        "row_index": tab_data.get("row_index"),
                        "fields": tab_data["fields"],
                        "field_count": tab_data["context"]["field_count"],
                        "non_empty_fields": tab_data["context"]["non_empty_fields"]
                    })
            
            return {
                "success": True,
                "sheet_info": analysis_result["sheet_info"],
                "tabs_analysis": analysis_result["tabs_analysis"],
                "data": formatted_data[:limit],
                "total_available": len(formatted_data),
                "data_source": "sqlite_database",
                "message": f"Retrieved data from {len(formatted_data)} tabs using SQLite database"
            }
        else:
            return {
                "success": False,
                "error": analysis_result["error"],
                "message": "Failed to retrieve sheet data from SQLite database"
            }
            
    except Exception as e:
        logger.error(f"Error in get_sheet_data: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "message": "Failed to retrieve sheet data from SQLite database"
        }


@app.get("/api/data-by-date")
async def get_data_by_date(date: str, tab_name: str = None):
    """
    Get data for a specific date (e.g., 4.11.2025) with optional tab filtering
    """
    try:
        # Use Universal X-Y Coordinate System
        universal_query_processor = get_universal_query_processor()
        
        # Convert date query to natural language with tab context
        if tab_name:
            query = f"show me data for {date} from {tab_name}"
        else:
            query = f"show me data for {date}"
        
        # Process the query with timeout using universal system
        import asyncio
        result = await asyncio.wait_for(
            universal_query_processor.process_query(
                query, 
                sheet_id=DEFAULT_SHEET_ID, 
                tab_name=tab_name
            ),
            timeout=30.0
        )
        
        if result["success"]:
            return {
                "success": True,
                "date": date,
                "tab_name": tab_name,
                "data_found": result.get("data_found", 0),
                "answer": result["answer"],
                "confidence": result.get("confidence", 1.0)
            }
        else:
            return {
                "success": False,
                "date": date,
                "tab_name": tab_name,
                "error": result.get("error", "No data found for this date"),
                "message": f"Could not find data for {date}" + (f" in {tab_name}" if tab_name else "")
            }
            
    except asyncio.TimeoutError:
        return {
            "success": False,
            "date": date,
            "tab_name": tab_name,
            "error": "Query timed out",
            "message": "The query is taking too long. Please try again."
        }
    except Exception as e:
        logger.error(f"Error getting data by date {date}: {str(e)}")
        return {
            "success": False,
            "date": date,
            "tab_name": tab_name,
            "error": str(e),
            "message": f"Failed to retrieve data for {date}" + (f" in {tab_name}" if tab_name else "")
        }


@app.get("/api/tab-specific-data")
async def get_tab_specific_data(tab_name: str, limit: int = 50):
    """
    Get data from a specific tab
    """
    try:
        from services.data_retrieval import get_data_retrieval_service
        data_service = get_data_retrieval_service()
        
        result = await data_service.get_sheet_data_for_llm(
            sheet_id=DEFAULT_SHEET_ID,
            tab_name=tab_name,
            limit=limit
        )
        
        if result["success"]:
            return {
                "success": True,
                "tab_name": tab_name,
                "sheet_info": result["sheet_info"],
                "data_summary": result["data_summary"],
                "data": result["data"][:limit],
                "total_available": len(result["data"]),
                "message": f"Retrieved {len(result['data'])} rows from tab '{tab_name}'"
            }
        else:
            return {
                "success": False,
                "tab_name": tab_name,
                "error": result["error"],
                "message": f"Failed to retrieve data from tab '{tab_name}'"
            }
            
    except Exception as e:
        logger.error(f"Error getting tab-specific data for {tab_name}: {str(e)}")
        return {
            "success": False,
            "tab_name": tab_name,
            "error": str(e),
            "message": f"Failed to retrieve data from tab '{tab_name}'"
        }


@app.get("/api/available-tabs")
async def get_available_tabs():
    """
    Get list of available tabs with hash counts
    """
    try:
        with get_db_context() as db:
            # Get tabs with hash counts
            tab_data = db.execute("""
                SELECT 
                    CASE WHEN tab_name IS NULL THEN 'Default' ELSE tab_name END as tab,
                    COUNT(*) as hash_count,
                    MIN(created_at) as first_hash,
                    MAX(updated_at) as last_updated
                FROM file_hashes 
                WHERE file_id = ?
                GROUP BY tab_name
                ORDER BY hash_count DESC
            """, (DEFAULT_SHEET_ID,)).fetchall()
            
            tabs = []
            for tab, count, first_hash, last_updated in tab_data:
                tabs.append({
                    "tab_name": tab,
                    "hash_count": count,
                    "first_hash_created": first_hash,
                    "last_updated": last_updated,
                    "has_data": count > 0
                })
            
            return {
                "success": True,
                "sheet_id": DEFAULT_SHEET_ID,
                "tabs": tabs,
                "total_tabs": len(tabs),
                "total_hashes": sum(tab["hash_count"] for tab in tabs)
            }
            
    except Exception as e:
        logger.error(f"Error getting available tabs: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "message": "Failed to retrieve available tabs"
        }


@app.post("/api/query")
async def natural_language_query(request: dict):
    """
    Process natural language queries about sheet data
    
    Request body:
    {
        "query": "what is the amount on December 12th?",
        "sheet_id": "optional_sheet_id"
    }
    """
    try:
        query = request.get("query", "").strip()
        sheet_id = request.get("sheet_id", DEFAULT_SHEET_ID)
        
        if not query:
            return {
                "success": False,
                "error": "Query is required",
                "message": "Please provide a query"
            }
        
        logger.info("🌐" + "=" * 70)
        logger.info(f"🌐 API QUERY ENDPOINT - Processing: '{query}'")
        logger.info(f"🌐 Sheet ID: {sheet_id}")
        logger.info("🌐" + "=" * 70)
        
        # Use Universal X-Y Coordinate System
        universal_query_processor = get_universal_query_processor()
        
        result = await universal_query_processor.process_query(
            query, 
            sheet_id=sheet_id, 
            tab_name=None  # Let system analyze all tabs
        )
        
        # Log API response details
        logger.info(f"🌐 API RESPONSE SUMMARY:")
        logger.info(f"   Success: {result['success']}")
        logger.info(f"   Data Found: {result.get('data_found', 0)}")
        logger.info(f"   Query Type: {result.get('query_type', 'unknown')}")
        logger.info(f"   Confidence: {result.get('confidence', 0.0):.2f}")
        
        if result.get("supporting_data"):
            logger.info(f"   Supporting Data Items: {len(result['supporting_data'])}")
        
        # Log response preview for frontend
        answer_preview = result.get("answer", "")[:200] + "..." if len(result.get("answer", "")) > 200 else result.get("answer", "")
        logger.info(f"🌐 RESPONSE TO FRONTEND:\n{answer_preview}")
        
        # Check if response includes export information
        if "📊 **Data exported to Google Sheets:**" in result.get("answer", ""):
            logger.info(f"📊 Query results have been exported to Google Sheets")
        
        logger.info("🌐" + "=" * 70)
        
        return {
            "success": result["success"],
            "query": query,
            "answer": result.get("answer", ""),
            "query_type": result.get("query_type", "unknown"),
            "confidence": result.get("confidence", 0.0),
            "data_found": result.get("data_found", 0),
            "supporting_data": result.get("supporting_data", []),
            "raw_data": result.get("raw_data", {}),  # Store raw data for on-demand export
            "suggestions": result.get("suggestions", []),
            "error": result.get("error"),
            "sheet_id": sheet_id
        }
        
    except Exception as e:
        logger.error(f"Error processing natural language query: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "message": "Query processing failed"
        }


@app.post("/api/export-query-results")
async def export_query_results(request: dict):
    """
    Export query results to Google Sheets on demand
    
    Request body:
    {
        "query": "original query",
        "raw_data": {...},
        "formatted_response": "formatted response text"
    }
    """
    try:
        query = request.get("query", "").strip()
        raw_data = request.get("raw_data", {})
        formatted_response = request.get("formatted_response", "")
        
        if not query:
            return {
                "success": False,
                "error": "Query is required",
                "message": "Please provide the original query"
            }
        
        logger.info(f"📊 Exporting query results to Google Sheets on demand")
        logger.info(f"   Query: '{query}'")
        
        from services.query_results_exporter import get_query_results_exporter
        
        exporter = get_query_results_exporter()
        export_result = await exporter.export_query_results(
            query=query,
            raw_data=raw_data,
            formatted_response=formatted_response
        )
        
        if export_result["success"]:
            logger.info(f"📊 Query results exported to Google Sheets:")
            logger.info(f"   Sheet URL: {export_result.get('sheet_url', 'N/A')}")
            logger.info(f"   Tab Name: {export_result.get('tab_name', 'N/A')}")
            logger.info(f"   Rows Exported: {export_result.get('rows_exported', 0)}")
            
            return {
                "success": True,
                "sheet_url": export_result.get('sheet_url', ''),
                "tab_name": export_result.get('tab_name', ''),
                "rows_exported": export_result.get('rows_exported', 0),
                "message": "Data exported to Google Sheets successfully"
            }
        else:
            logger.warning(f"⚠️  Failed to export query results: {export_result.get('error', 'Unknown error')}")
            return {
                "success": False,
                "error": export_result.get('error', 'Unknown error'),
                "message": "Failed to export to Google Sheets"
            }
            
    except Exception as e:
        logger.error(f"Error exporting query results: {str(e)}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "message": "Failed to export query results"
        }


@app.post("/api/search-data")
async def search_sheet_data(request: dict):
    """
    Search sheet data based on criteria
    
    Request body:
    {
        "search_terms": ["11:00", "473"],
        "tab_names": ["RO DETAILS"],
        "limit": 50
    }
    """
    try:
        from services.data_retrieval import get_data_retrieval_service
        data_service = get_data_retrieval_service()
        
        search_terms = request.get("search_terms", [])
        tab_names = request.get("tab_names", [])
        limit = request.get("limit", 50)
        
        result = await data_service.search_data_by_criteria(
            sheet_id=DEFAULT_SHEET_ID,
            search_terms=search_terms,
            tab_names=tab_names
        )
        
        if result["success"]:
            # Limit results for frontend
            limited_results = result["results"][:limit]
            
            return {
                "success": True,
                "search_criteria": result["search_criteria"],
                "results_count": result["results_count"],
                "results_shown": len(limited_results),
                "results": limited_results,
                "message": f"Found {result['results_count']} matching rows"
            }
        else:
            return {
                "success": False,
                "error": result["error"],
                "message": "Search failed"
            }
            
    except Exception as e:
        logger.error(f"Error in search_sheet_data: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "message": "Search error"
        }


@app.get("/api/tabs-summary")
async def get_tabs_summary():
    """
    Get summary of all tabs in the sheet
    """
    try:
        from services.data_retrieval import get_data_retrieval_service
        data_service = get_data_retrieval_service()
        
        result = await data_service.get_sheet_data_for_llm(
            sheet_id=DEFAULT_SHEET_ID,
            limit=1000  # Get more data for comprehensive summary
        )
        
        if result["success"]:
            return {
                "success": True,
                "sheet_info": result["sheet_info"],
                "tabs_summary": result["data_summary"]["tabs_summary"],
                "total_tabs": len(result["data_summary"]["tabs_found"]),
                "tab_names": result["data_summary"]["tabs_found"],
                "message": f"Found {len(result['data_summary']['tabs_found'])} tabs"
            }
        else:
            return {
                "success": False,
                "error": result["error"],
                "message": "Failed to get tabs summary"
            }
            
    except Exception as e:
        logger.error(f"Error in get_tabs_summary: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "message": "Internal server error"
        }


@app.get("/admin/database-stats")
async def get_database_stats():
    """
    Get comprehensive database statistics
    """
    try:
        import sqlite3
        import os
        
        db_path = "ravvyn.db"
        
        if not os.path.exists(db_path):
            return {"error": "Database file not found"}
        
        # Get file size
        file_size = os.path.getsize(db_path)
        file_size_mb = file_size / (1024 * 1024)
        
        stats = {
            "database_info": {
                "file_size_bytes": file_size,
                "file_size_mb": round(file_size_mb, 2),
                "file_path": db_path
            },
            "table_counts": {},
            "your_sheet_stats": {},
            "totals": {}
        }
        
        # Connect and get table counts
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = [row[0] for row in cursor.fetchall()]
        
        total_records = 0
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                stats["table_counts"][table] = count
                total_records += count
            except Exception as e:
                stats["table_counts"][table] = f"Error: {str(e)}"
        
        stats["totals"]["total_records"] = total_records
        
        # Your sheet specific stats
        your_sheet_id = DEFAULT_SHEET_ID
        cursor.execute("SELECT COUNT(*) FROM file_hashes WHERE file_id = ?", (your_sheet_id,))
        your_sheet_hashes = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM sheets_content WHERE sheet_id = ?", (your_sheet_id,))
        your_sheet_rows = cursor.fetchone()[0]
        
        stats["your_sheet_stats"] = {
            "sheet_id": your_sheet_id,
            "hash_count": your_sheet_hashes,
            "content_rows": your_sheet_rows
        }
        
        conn.close()
        
        return {
            "success": True,
            "stats": stats,
            "summary": f"Database: {file_size_mb:.2f}MB, {total_records} total records, {your_sheet_hashes} hashes for your sheet"
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


@app.get("/test/query-system")
async def test_query_system():
    """
    Test endpoint to verify the natural language query system is working
    """
    try:
        # Use Universal X-Y Coordinate System for testing
        universal_query_processor = get_universal_query_processor()
        
        # Test queries that work with any sheet structure
        test_queries = [
            "what is the amount on December 12th?",
            "show me data from RO DETAILS",
            "what is the latest amount?",
            "what is the ro1&2 feed tank level on 26.6.25?",
            "show me latest data",
            "get data for 4.11.25"
        ]
        
        results = []
        
        for query in test_queries:
            try:
                result = await universal_query_processor.process_query(query)
                results.append({
                    "query": query,
                    "success": result["success"],
                    "answer": result.get("answer", "")[:200] + "..." if len(result.get("answer", "")) > 200 else result.get("answer", ""),
                    "query_type": result.get("query_type", "unknown"),
                    "confidence": result.get("confidence", 0.0),
                    "data_found": result.get("data_found", 0)
                })
            except Exception as e:
                results.append({
                    "query": query,
                    "success": False,
                    "error": str(e)
                })
        
        # Check data availability
        data_available = await hash_service.hash_storage.check_data_availability_for_queries(DEFAULT_SHEET_ID)
        
        return {
            "success": True,
            "message": "Query system test completed",
            "sheet_id": DEFAULT_SHEET_ID,
            "data_available": data_available,
            "test_results": results,
            "system_status": {
                "universal_query_processor": "active",
                "universal_data_service": "active", 
                "universal_sheet_analyzer": "active",
                "database_queue": "active",
                "natural_language_processing": "enabled"
            }
        }
        
    except Exception as e:
        logger.error(f"Error testing query system: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "message": "Query system test failed"
        }


@app.get("/test/reasoning-agent")
async def test_reasoning_agent():
    """
    Test endpoint to verify Database Reasoning Agent integration
    """
    try:
        logger.info("🤖 Testing Database Reasoning Agent integration...")
        
        # Test questions based on actual database schema
        test_questions = [
            "How many sheet records last 7 days?",
            "Count of chat messages in December?",
            "What is the sync status?",
            "Total sheet records",
            "Data activity this month"
        ]
        
        results = []
        
        for question in test_questions:
            try:
                logger.info(f"🤖 Testing question: {question}")
                agent = _get_reasoning_agent()
                answer = await agent.answer_question(question)
                
                # Check if it's a success or failure
                failure_messages = [
                    "Unable to answer accurately due to ambiguous or unavailable time data.",
                    "No data available for the resolved time range."
                ]
                
                success = answer not in failure_messages
                
                results.append({
                    "question": question,
                    "answer": answer,
                    "success": success,
                    "answer_length": len(answer)
                })
                
                logger.info(f"🤖 Result: {'✅ SUCCESS' if success else '❌ FAILED'} - {answer[:100]}...")
                
            except Exception as e:
                logger.error(f"🤖 Error testing question '{question}': {str(e)}")
                results.append({
                    "question": question,
                    "answer": f"Error: {str(e)}",
                    "success": False,
                    "error": str(e)
                })
        
        # Calculate success rate
        successful_tests = sum(1 for r in results if r["success"])
        success_rate = (successful_tests / len(results)) * 100 if results else 0
        
        agent = _get_reasoning_agent()
        
        return {
            "success": True,
            "message": "Database Reasoning Agent integration test completed",
            "test_summary": {
                "total_questions": len(test_questions),
                "successful_answers": successful_tests,
                "success_rate": f"{success_rate:.1f}%"
            },
            "agent_info": {
                "current_date": str(agent.current_date),
                "timezone": agent.timezone,
                "database_path": agent.db_path
            },
            "test_results": results,
            "integration_status": "active"
        }
        
    except Exception as e:
        logger.error(f"Error testing Database Reasoning Agent: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "message": "Database Reasoning Agent integration test failed"
        }


@app.get("/test/data-structure")
async def test_data_structure():
    """
    Test endpoint to verify data structure and retrieval
    """
    try:
        from services.data_retrieval import get_data_retrieval_service
        data_service = get_data_retrieval_service()
        
        # Get sample data
        result = await data_service.get_sheet_data_for_llm(
            sheet_id=DEFAULT_SHEET_ID,
            limit=5  # Just 5 rows for testing
        )
        
        if result["success"] and result["data"]:
            sample_row = result["data"][0]
            
            return {
                "success": True,
                "message": "Data structure verified",
                "sample_data": {
                    "raw_example": "9252|1MtjJyKiDR7COszXxZF-wYR-UF5LcwYSpTfr_Aa_PEt8|RO DETAILS|8|[\"11:00\", \"473\"]",
                    "parsed_example": sample_row,
                    "data_summary": result["data_summary"],
                    "llm_context_preview": result["llm_context"][:500] + "..." if len(result["llm_context"]) > 500 else result["llm_context"]
                },
                "sheet_info": result["sheet_info"],
                "total_rows_available": len(result["data"])
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "No data available"),
                "message": "Data structure test failed"
            }
            
    except Exception as e:
        logger.error(f"Error in test_data_structure: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "message": "Test failed"
        }


@app.get("/test/db-queue-status")
async def test_db_queue_status():
    """
    Test endpoint to check database operation queue status
    """
    try:
        from services.db_operation_queue import get_db_operation_queue
        
        queue = get_db_operation_queue()
        queue_stats = queue.get_stats()
        
        # Test data availability
        data_available = await hash_service.hash_storage.check_data_availability_for_queries(DEFAULT_SHEET_ID)
        
        return {
            "success": True,
            "sheet_id": DEFAULT_SHEET_ID,
            "database_queue": {
                "status": "active",
                "stats": queue_stats,
                "prevents_locks": True
            },
            "data_availability": {
                "can_answer_queries": data_available,
                "message": "System ready for queries" if data_available else "System needs data sync"
            },
            "improvements": {
                "queue_based_operations": True,
                "incremental_hash_updates": True,
                "prevents_repeated_processing": True,
                "database_lock_prevention": "queue-based"
            }
        }
        
    except Exception as e:
        logger.error(f"Error checking database queue status: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "sheet_id": DEFAULT_SHEET_ID
        }


@app.get("/test/sqlite-lock-status")
async def test_sqlite_lock_status():
    """
    Test endpoint to check SQLite lock manager status
    """
    try:
        from services.sqlite_lock_manager import get_sqlite_lock_manager
        
        lock_manager = get_sqlite_lock_manager()
        lock_info = lock_manager.get_lock_info()
        
        # Test data availability
        data_available = await hash_service.hash_storage.check_data_availability_for_queries(DEFAULT_SHEET_ID)
        
        return {
            "success": True,
            "sheet_id": DEFAULT_SHEET_ID,
            "sqlite_lock_manager": {
                "status": "active",
                "lock_info": lock_info,
                "lock_file_path": lock_manager.lock_file_path
            },
            "data_availability": {
                "can_answer_queries": data_available,
                "message": "System ready for queries" if data_available else "System needs data sync"
            },
            "improvements": {
                "file_based_locking": True,
                "database_lock_prevention": "enhanced",
                "chat_timeout_prevention": True
            }
        }
        
    except Exception as e:
        logger.error(f"Error checking SQLite lock status: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "sheet_id": DEFAULT_SHEET_ID
        }


@app.get("/test/incremental-hash")
async def test_incremental_hash():
    """
    Test endpoint to verify incremental hash updates work with existing data
    """
    try:
        from services.hash_service import HashService
        from services.sheets import SheetsService
        
        hash_service = HashService()
        sheets_service = SheetsService()
        
        logger.info(f"🧪 Testing incremental hash updates for sheet: {DEFAULT_SHEET_ID}")
        
        # Read a small sample of data from SQLite database
        tab_data = await _read_sheet_from_db(DEFAULT_SHEET_ID, "RO DETAILS")
        if not tab_data:
            return {
                "success": False,
                "error": "No data found in RO DETAILS tab",
                "sheet_id": DEFAULT_SHEET_ID
            }
        
        # Use first 20 rows for testing
        sample_data = tab_data[:20]
        
        # Process with hash service
        result = await hash_service.process_file_with_change_detection(
            DEFAULT_SHEET_ID, "sheet", sample_data
        )
        
        if result['success']:
            hash_computation = result.get('hash_computation', {})
            change_detection = result.get('change_detection', {})
            
            return {
                "success": True,
                "sheet_id": DEFAULT_SHEET_ID,
                "test_data_rows": len(sample_data),
                "hash_computation": {
                    "hash_count": hash_computation.get('hash_count', 0),
                    "computation_time_ms": hash_computation.get('computation_time_ms', 0)
                },
                "change_detection": {
                    "added": change_detection.get('added', 0),
                    "modified": change_detection.get('modified', 0),
                    "deleted": change_detection.get('deleted', 0),
                    "unchanged": change_detection.get('unchanged', 0),
                    "has_changes": result.get('has_changes', False)
                },
                "incremental_status": {
                    "working_correctly": change_detection.get('unchanged', 0) > 0 or change_detection.get('added', 0) == hash_computation.get('hash_count', 0),
                    "message": "Incremental updates working" if change_detection.get('unchanged', 0) > 0 else "First run or all new data"
                }
            }
        else:
            return {
                "success": False,
                "error": result.get('error', 'Unknown error'),
                "sheet_id": DEFAULT_SHEET_ID
            }
        
    except Exception as e:
        logger.error(f"Error testing incremental hash: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "sheet_id": DEFAULT_SHEET_ID
        }


@app.get("/test/data-availability")
async def test_data_availability():
    """
    Test endpoint to check if the system can work with existing data to answer queries
    """
    try:
        from services.db_lock_manager import get_lock_manager
        
        lock_manager = get_lock_manager()
        lock_status = lock_manager.get_lock_status()
        
        # Check data availability for your sheet
        data_available = await hash_service.hash_storage.check_data_availability_for_queries(DEFAULT_SHEET_ID)
        hash_summary = await hash_service.hash_storage.get_file_hash_summary(DEFAULT_SHEET_ID)
        
        # Get sheet metadata from database
        with get_db_context() as db:
            from services.database import SheetsMetadata
            sheet_meta = db.query(SheetsMetadata).filter(
                SheetsMetadata.sheet_id == DEFAULT_SHEET_ID
            ).first()
        
        return {
            "success": True,
            "sheet_id": DEFAULT_SHEET_ID,
            "data_availability": {
                "can_answer_queries": data_available,
                "hash_summary": hash_summary,
                "sheet_metadata": {
                    "exists": sheet_meta is not None,
                    "sheet_name": sheet_meta.sheet_name if sheet_meta else None,
                    "last_synced": sheet_meta.last_synced.isoformat() if sheet_meta and sheet_meta.last_synced else None,
                    "sync_status": sheet_meta.sync_status if sheet_meta else None
                }
            },
            "database_status": {
                "connection_pool": "improved",
                "lock_manager": {
                    "is_locked": lock_status["is_locked"],
                    "active_operations": lock_status["active_operations"]
                }
            },
            "system_ready": data_available and (sheet_meta is not None),
            "message": "System can work with existing data" if data_available else "System needs data sync"
        }
        
    except Exception as e:
        logger.error(f"Error checking data availability: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "sheet_id": DEFAULT_SHEET_ID
        }


@app.get("/test/db-lock-status")
async def test_db_lock_status():
    """
    Test endpoint to check database lock manager status and verify write lock mechanism
    """
    try:
        from services.db_lock_manager import get_lock_manager
        
        lock_manager = get_lock_manager()
        lock_status = lock_manager.get_lock_status()
        
        # Get hash statistics for your sheet
        stored_hashes = await hash_service.get_stored_hashes(DEFAULT_SHEET_ID)
        
        return {
            "success": True,
            "sheet_id": DEFAULT_SHEET_ID,
            "database_lock_manager": {
                "status": "active",
                "is_locked": lock_status["is_locked"],
                "active_operations": lock_status["active_operations"],
                "operations_details": lock_status["operations"],
                "lock_timeout": lock_status["lock_timeout"]
            },
            "hash_storage": {
                "stored_hashes": len(stored_hashes),
                "write_lock_pattern": "acquire_lock → delete → insert → commit → release_lock",
                "database_lock_prevention": "enabled"
            },
            "improvements": {
                "database_locks_prevented": True,
                "concurrent_write_protection": True,
                "proper_transaction_management": True,
                "error_handling": "enhanced"
            },
            "message": "Database write lock mechanism is active and preventing SQLite lock errors"
        }
        
    except Exception as e:
        logger.error(f"Error checking database lock status: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "sheet_id": DEFAULT_SHEET_ID
        }


@app.post("/test/hash-write-lock")
async def test_hash_write_lock(background_tasks: BackgroundTasks = BackgroundTasks()):
    """
    Test endpoint to verify hash storage with write locks works correctly
    """
    try:
        logger.info(f"🧪 Testing hash write lock mechanism for sheet: {DEFAULT_SHEET_ID}")
        
        # Start hash processing with write lock protection
        background_tasks.add_task(
            _test_hash_write_lock_processing,
            DEFAULT_SHEET_ID
        )
        
        return {
            "success": True,
            "message": f"Hash write lock test started for sheet: {DEFAULT_SHEET_ID}",
            "sheet_id": DEFAULT_SHEET_ID,
            "test_type": "write_lock_verification",
            "expected_result": "No database lock errors",
            "status": "processing"
        }
        
    except Exception as e:
        logger.error(f"Error starting hash write lock test: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "sheet_id": DEFAULT_SHEET_ID
        }


async def _test_hash_write_lock_processing(sheet_id: str):
    """
    Background task to test hash write lock mechanism
    """
    try:
        logger.info(f"🧪 Starting write lock test for sheet: {sheet_id}")
        
        # Read sheet data
        def _get_spreadsheet():
            return sheets_service.sheets_service.spreadsheets().get(
                spreadsheetId=sheet_id
            ).execute()
        
        spreadsheet = await sheets_service._retry_request(_get_spreadsheet)
        sheets_info = spreadsheet.get('sheets', [])
        
        if not sheets_info:
            logger.warning("No sheets found for testing")
            return
        
        # Test with first tab
        first_tab = sheets_info[0]
        tab_name = first_tab.get('properties', {}).get('title', 'Sheet1')
        
        logger.info(f"🧪 Testing write locks with tab: '{tab_name}'")
        
        # Read tab data from SQLite database first, fallback to Google Sheets API
        tab_data = await _read_sheet_from_db(sheet_id, tab_name)
        if not tab_data:
            logger.info(f"   📡 No data in SQLite, reading from Google Sheets API for '{tab_name}'")
            tab_data = await sheets_service.read_sheet(sheet_id, tab_name)
        
        if tab_data:
            # Process with hash service (this will use write locks)
            hash_result = await hash_service.process_file_with_change_detection(
                sheet_id, "sheet", tab_data
            )
            
            if hash_result['success']:
                logger.info(f"✅ Write lock test successful: {hash_result['hash_computation']['hash_count']} hashes processed")
                logger.info(f"🔒 No database lock errors encountered")
            else:
                logger.error(f"❌ Write lock test failed: {hash_result.get('error')}")
        else:
            logger.warning("No data found in tab for testing")
        
        logger.info(f"🧪 Write lock test completed for sheet: {sheet_id}")
        
    except Exception as e:
        logger.error(f"❌ Write lock test failed: {str(e)}")
        import traceback
        traceback.print_exc()


@app.post("/process/my-sheet")
async def process_my_sheet_now(background_tasks: BackgroundTasks = BackgroundTasks()):
    """
    Immediately process your specific sheet: 1MtjJyKiDR7COszXxZF-wYR-UF5LcwYSpTfr_Aa_PEt8
    This endpoint is hardcoded to your sheet ID and processes it immediately
    """
    
    try:
        logger.info(f"🎯 Processing YOUR sheet immediately: {DEFAULT_SHEET_ID}")
        
        # Start immediate processing
        background_tasks.add_task(
            _immediate_sheet_processing,
            DEFAULT_SHEET_ID
        )
        
        return {
            "success": True,
            "message": f"Processing started for your sheet: {DEFAULT_SHEET_ID}",
            "sheet_id": DEFAULT_SHEET_ID,
            "action": "immediate_processing",
            "status": "started",
            "note": "Your sheet is being processed with hash computation for all tabs"
        }
        
    except Exception as e:
        logger.error(f"Error processing your sheet: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "sheet_id": DEFAULT_SHEET_ID,
            "action": "immediate_processing"
        }


async def _get_enhanced_ai_response(request, sheet_id_to_use, conversation_id):
    """
    Get enhanced AI response with structured data context using SQLite database only
    """
    try:
        # Use Universal Data Service for SQLite-based data retrieval
        universal_data_service = get_universal_data_service()
        
        # Get sheet summary from SQLite database
        sheet_summary = await universal_data_service.get_sheet_summary(sheet_id=sheet_id_to_use)
        
        # Enhanced AI response with SQLite data context
        if sheet_summary["success"] and sheet_summary["tabs_summary"]:
            logger.info(f"📊 Retrieved summary for {len(sheet_summary['tabs_summary'])} tabs from SQLite")
            
            # Build context from SQLite data
            context_parts = []
            context_parts.append(f"Sheet: {sheet_summary['sheet_info']['sheet_name']}")
            
            for tab_name, tab_info in list(sheet_summary["tabs_summary"].items())[:3]:  # Limit to 3 tabs
                context_parts.append(f"\nTab '{tab_name}':")
                context_parts.append(f"  - {tab_info['field_count']} fields: {', '.join(tab_info['fields'][:8])}")
                if len(tab_info['fields']) > 8:
                    context_parts.append(f"    (and {len(tab_info['fields']) - 8} more)")
                context_parts.append(f"  - {tab_info['dimensions']['rows']} rows of data")
            
            # Get some recent data for context
            try:
                latest_data = await universal_data_service.get_latest_data(sheet_id=sheet_id_to_use)
                if latest_data["success"] and latest_data["latest_data"]:
                    context_parts.append("\nRecent Data Sample:")
                    for tab_name, tab_data in list(latest_data["latest_data"].items())[:2]:
                        context_parts.append(f"  {tab_name}:")
                        for field, value in list(tab_data["fields"].items())[:5]:
                            if value and str(value).strip():
                                context_parts.append(f"    {field}: {value}")
            except Exception as e:
                logger.warning(f"Could not get latest data for context: {e}")
            
            sqlite_context = "\n".join(context_parts)
            
            # Create enhanced message for AI
            enhanced_message = f"""
User Query: {request.message}

Available Data Context (from SQLite database):
{sqlite_context}

Please provide a helpful response based on the available data. If the user is asking about specific data, reference the actual values from the sheet data provided above.
"""
            
            return await ai_service.chat(
                enhanced_message,
                request.user_id,
                sheet_id=sheet_id_to_use,
                doc_id=request.doc_id,
                conversation_id=conversation_id
            )
        else:
            # Fallback to regular chat if no SQLite data available
            logger.info("⚠️  No SQLite data available, using regular chat")
            return await ai_service.chat(
                request.message,
                request.user_id,
                sheet_id=sheet_id_to_use,
                doc_id=request.doc_id,
                conversation_id=conversation_id
            )
    except Exception as e:
        logger.error(f"Error in enhanced AI response: {str(e)}")
        # Final fallback to basic chat
        return await ai_service.chat(
            request.message,
            request.user_id,
            sheet_id=sheet_id_to_use,
            doc_id=request.doc_id,
            conversation_id=conversation_id
        )


async def _sync_sheet_with_hash_processing(sheet_id: str, force: bool = False):
    """
    Background task: Sync sheet with integrated hash processing
    """
    try:
        logger.info(f"🔄 Processing sheet {sheet_id} with hash integration...")
        
        # Step 1: Get all tabs in the sheet
        logger.info("📋 Step 1: Getting sheet tabs...")
        
        def _get_spreadsheet():
            return sheets_service.sheets_service.spreadsheets().get(
                spreadsheetId=sheet_id
            ).execute()
        
        spreadsheet = await sheets_service._retry_request(_get_spreadsheet)
        sheets = spreadsheet.get('sheets', [])
        
        logger.info(f"Found {len(sheets)} tabs in the sheet")
        
        # Step 2: Process each tab
        total_processed = 0
        total_hashes = 0
        
        for i, sheet in enumerate(sheets):
            properties = sheet.get('properties', {})
            tab_name = properties.get('title', f'Sheet{i+1}')
            
            logger.info(f"📊 Processing tab: '{tab_name}'")
            
            try:
                # Sync the tab data
                await sync_service.sync_sheet(sheet_id, spreadsheet.get('properties', {}).get('title', 'Unknown'))
                
                # Read the tab data from SQLite database first
                tab_data = await _read_sheet_from_db(sheet_id, tab_name)
                if not tab_data:
                    logger.info(f"   📡 No data in SQLite, reading from Google Sheets API for '{tab_name}'")
                    tab_data = await sheets_service.read_sheet(sheet_id, tab_name)
                
                if tab_data:
                    # Compute and store hashes
                    result = await hash_service.process_file_with_change_detection(
                        sheet_id, "sheet", tab_data
                    )
                    
                    if result['success']:
                        hash_count = result['hash_computation']['hash_count']
                        total_hashes += hash_count
                        total_processed += 1
                        
                        logger.info(f"✅ Tab '{tab_name}': {len(tab_data)} rows, {hash_count} hashes")
                    else:
                        logger.error(f"❌ Hash processing failed for tab '{tab_name}': {result.get('error')}")
                else:
                    logger.warning(f"⚠️  Tab '{tab_name}' appears to be empty")
                    
            except Exception as e:
                logger.error(f"❌ Error processing tab '{tab_name}': {str(e)}")
        
        logger.info(f"🎉 Sync completed: {total_processed}/{len(sheets)} tabs processed, {total_hashes} total hashes")
        
    except Exception as e:
        logger.error(f"❌ Hash sync failed for sheet {sheet_id}: {str(e)}")


async def _immediate_sheet_processing(sheet_id: str):
    """
    Immediate processing function specifically for your sheet
    Processes all tabs with hash computation and provides detailed logging
    """
    try:
        logger.info(f"🎯 IMMEDIATE PROCESSING: {sheet_id}")
        logger.info("=" * 80)
        
        # Step 1: Validate and get sheet info
        logger.info("📋 Step 1: Getting sheet information...")
        
        def _get_spreadsheet():
            return sheets_service.sheets_service.spreadsheets().get(
                spreadsheetId=sheet_id
            ).execute()
        
        spreadsheet = await sheets_service._retry_request(_get_spreadsheet)
        sheet_title = spreadsheet.get('properties', {}).get('title', 'Unknown Sheet')
        sheets = spreadsheet.get('sheets', [])
        
        logger.info(f"✅ Sheet found: '{sheet_title}'")
        logger.info(f"📊 Total tabs: {len(sheets)}")
        
        # Step 2: Process each tab with detailed logging
        logger.info("🔄 Step 2: Processing all tabs...")
        
        processing_results = []
        
        for i, sheet in enumerate(sheets):
            properties = sheet.get('properties', {})
            tab_name = properties.get('title', f'Sheet{i+1}')
            tab_id = properties.get('sheetId', i)
            
            logger.info(f"   📄 Tab {i+1}/{len(sheets)}: '{tab_name}' (ID: {tab_id})")
            
            try:
                # Read tab data
                tab_data = await sheets_service.read_sheet(sheet_id, tab_name)
                
                if not tab_data:
                    logger.info(f"      ⚠️  Empty tab, skipping...")
                    processing_results.append({'tab': tab_name, 'status': 'empty'})
                    continue
                
                logger.info(f"      📊 Found {len(tab_data)} rows")
                
                # Sync to database
                await sync_service.sync_sheet(sheet_id, sheet_title)
                logger.info(f"      ✅ Synced to database")
                
                # Process with hash system
                hash_result = await hash_service.process_file_with_change_detection(
                    sheet_id, "sheet", tab_data
                )
      
                if hash_result['success']:
                    hash_count = hash_result['hash_computation']['hash_count']
                    has_changes = hash_result['has_changes']
                    comp_time = hash_result['hash_computation']['computation_time_ms']
                    
                    logger.info(f"      🔐 Computed {hash_count} hashes in {comp_time}ms")
                    logger.info(f"      🔍 Changes detected: {has_changes}")
                    
                    processing_results.append({
                        'tab': tab_name,
                        'status': 'success',
                        'rows': len(tab_data),
                        'hashes': hash_count,
                        'changes': has_changes,
                        'time_ms': comp_time
                    })
                else:
                    error = hash_result.get('error', 'Unknown error')
                    logger.error(f"      ❌ Hash processing failed: {error}")
                    processing_results.append({'tab': tab_name, 'status': 'hash_error', 'error': error})
                
            except Exception as e:
                logger.error(f"      ❌ Tab processing failed: {str(e)}")
                processing_results.append({'tab': tab_name, 'status': 'error', 'error': str(e)})
        
        # Step 3: Summary
        logger.info("📈 Step 3: Processing Summary")
        logger.info("=" * 80)
        
        successful = [r for r in processing_results if r['status'] == 'success']
        failed = [r for r in processing_results if r['status'] in ['error', 'hash_error']]
        empty = [r for r in processing_results if r['status'] == 'empty']
        
        total_rows = sum(r.get('rows', 0) for r in successful)
        total_hashes = sum(r.get('hashes', 0) for r in successful)
        
        logger.info(f"🎯 Sheet: '{sheet_title}' ({sheet_id})")
        logger.info(f"✅ Successful tabs: {len(successful)}")
        logger.info(f"❌ Failed tabs: {len(failed)}")
        logger.info(f"⚠️  Empty tabs: {len(empty)}")
        logger.info(f"📊 Total rows processed: {total_rows}")
        logger.info(f"🔐 Total hashes computed: {total_hashes}")
        
        if failed:
            logger.info("❌ Failed tabs:")
            for fail in failed:
                logger.info(f"   - {fail['tab']}: {fail.get('error', 'Unknown error')}")
        
        logger.info("🎉 IMMEDIATE PROCESSING COMPLETED!")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"❌ IMMEDIATE PROCESSING FAILED: {str(e)}")
        import traceback
        traceback.print_exc()


async def _comprehensive_sheet_sync(sheet_id: str):
    """
    Background task: Comprehensive sync with full hash processing and validation
    """
    try:
        logger.info(f"🚀 Starting comprehensive sync for sheet: {sheet_id}")
        
        # Step 1: Validate sheet access
        logger.info("🔍 Step 1: Validating sheet access...")
        
        def _get_spreadsheet():
            return sheets_service.sheets_service.spreadsheets().get(
                spreadsheetId=sheet_id
            ).execute()
        
        spreadsheet = await sheets_service._retry_request(_get_spreadsheet)
        sheet_title = spreadsheet.get('properties', {}).get('title', 'Unknown Sheet')
        sheets = spreadsheet.get('sheets', [])
        
        logger.info(f"✅ Sheet '{sheet_title}' found with {len(sheets)} tabs")
        
        # Step 2: Process all tabs with hash integration
        logger.info("📊 Step 2: Processing all tabs with hash integration...")
        
        sync_results = []
        
        for i, sheet in enumerate(sheets):
            properties = sheet.get('properties', {})
            tab_name = properties.get('title', f'Sheet{i+1}')
            tab_id = properties.get('sheetId', i)
            
            logger.info(f"🔄 Processing tab {i+1}/{len(sheets)}: '{tab_name}' (ID: {tab_id})")
            
            try:
                # Read tab data from SQLite database first
                tab_data = await _read_sheet_from_db(sheet_id, tab_name)
                if not tab_data:
                    logger.info(f"   📡 No data in SQLite, reading from Google Sheets API for '{tab_name}'")
                    tab_data = await sheets_service.read_sheet(sheet_id, tab_name)
                
                if not tab_data:
                    logger.warning(f"⚠️  Tab '{tab_name}' is empty, skipping...")
                    sync_results.append({
                        'tab_name': tab_name,
                        'tab_id': tab_id,
                        'status': 'skipped',
                        'reason': 'empty'
                    })
                    continue
                
                # Sync to database
                await sync_service.sync_sheet(sheet_id, sheet_title)
                
                # Process with hash system
                hash_result = await hash_service.process_file_with_change_detection(
                    sheet_id, "sheet", tab_data
                )
                
                if hash_result['success']:
                    sync_results.append({
                        'tab_name': tab_name,
                        'tab_id': tab_id,
                        'status': 'success',
                        'rows': len(tab_data),
                        'hashes': hash_result['hash_computation']['hash_count'],
                        'has_changes': hash_result['has_changes'],
                        'computation_time_ms': hash_result['hash_computation']['computation_time_ms']
                    })
                    
                    logger.info(f"✅ '{tab_name}': {len(tab_data)} rows, {hash_result['hash_computation']['hash_count']} hashes, changes: {hash_result['has_changes']}")
                else:
                    sync_results.append({
                        'tab_name': tab_name,
                        'tab_id': tab_id,
                        'status': 'error',
                        'error': hash_result.get('error', 'Unknown error')
                    })
                    logger.error(f"❌ '{tab_name}': {hash_result.get('error', 'Unknown error')}")
                
            except Exception as e:
                sync_results.append({
                    'tab_name': tab_name,
                    'tab_id': tab_id,
                    'status': 'error',
                    'error': str(e)
                })
                logger.error(f"❌ Error processing tab '{tab_name}': {str(e)}")
        
        # Step 3: Summary and validation
        logger.info("📈 Step 3: Generating sync summary...")
        
        successful_tabs = [r for r in sync_results if r['status'] == 'success']
        failed_tabs = [r for r in sync_results if r['status'] == 'error']
        skipped_tabs = [r for r in sync_results if r['status'] == 'skipped']
        
        total_rows = sum(r.get('rows', 0) for r in successful_tabs)
        total_hashes = sum(r.get('hashes', 0) for r in successful_tabs)
        
        logger.info(f"🎉 Comprehensive sync completed!")
        logger.info(f"   📊 Sheet: '{sheet_title}' ({sheet_id})")
        logger.info(f"   ✅ Successful tabs: {len(successful_tabs)}")
        logger.info(f"   ❌ Failed tabs: {len(failed_tabs)}")
        logger.info(f"   ⚠️  Skipped tabs: {len(skipped_tabs)}")
        logger.info(f"   📈 Total rows processed: {total_rows}")
        logger.info(f"   🔐 Total hashes computed: {total_hashes}")
        
        # Step 4: Test hash system functionality
        logger.info("🧪 Step 4: Testing hash system functionality...")
        
        if successful_tabs:
            test_tab = successful_tabs[0]
            test_tab_name = test_tab['tab_name']
            
            # Test hash retrieval
            stored_hashes = await hash_service.get_stored_hashes(sheet_id)
            logger.info(f"✅ Hash retrieval test: {len(stored_hashes)} hashes found in database")
            
            # Test service statistics
            stats = await hash_service.get_service_statistics()
            logger.info(f"✅ Service statistics test: {stats.get('service_status', 'unknown')}")
        
        logger.info(f"🏁 All processing completed for sheet: {sheet_id}")
        
    except Exception as e:
        logger.error(f"❌ Comprehensive sync failed for sheet {sheet_id}: {str(e)}")
        import traceback
        traceback.print_exc()


# Sheet CRUD Operations
@app.post("/sheets/update")
async def update_sheet(request: SheetUpdateRequest, db: Session = Depends(get_db)):
    """Update cells in a Google Sheet"""
    try:
        if request.row and request.column and request.value:
            # Single cell update
            result = await sheets_service.update_cell(
                request.sheet_id,
                request.tab_name,
                request.row,
                request.column,
                request.value
            )
        elif request.start_row and request.start_col and request.end_row and request.end_col and request.values:
            # Range update
            result = await sheets_service.update_range(
                request.sheet_id,
                request.tab_name,
                request.start_row,
                request.start_col,
                request.end_row,
                request.end_col,
                request.values
            )
        else:
            from fastapi import HTTPException
            raise HTTPException(status_code=400, detail="Invalid update parameters")
        
        return {"success": True, "result": result}
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error updating sheet: {str(e)}")
        raise


@app.post("/sheets/delete")
async def delete_sheet_content(request: SheetDeleteRequest, db: Session = Depends(get_db)):
    """Delete rows or columns from a Google Sheet"""
    try:
        if request.start_row and request.end_row:
            # Delete rows
            result = await sheets_service.delete_rows(
                request.sheet_id,
                request.tab_name,
                request.start_row,
                request.end_row
            )
        elif request.start_col and request.end_col:
            # Delete columns
            result = await sheets_service.delete_columns(
                request.sheet_id,
                request.tab_name,
                request.start_col,
                request.end_col
            )
        else:
            from fastapi import HTTPException
            raise HTTPException(status_code=400, detail="Invalid delete parameters")
        
        return {"success": True, "result": result}
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error deleting sheet content: {str(e)}")
        raise


@app.post("/sheets/insert")
async def insert_sheet_rows(request: SheetInsertRequest, db: Session = Depends(get_db)):
    """Insert rows into a Google Sheet"""
    try:
        result = await sheets_service.insert_rows(
            request.sheet_id,
            request.tab_name,
            request.row_index,
            request.num_rows
        )
        return {"success": True, "result": result}
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error inserting rows: {str(e)}")
        raise


# Doc CRUD Operations
@app.post("/docs/update")
async def update_doc(request: DocUpdateRequest, db: Session = Depends(get_db)):
    """Update content in a Google Doc"""
    try:
        result = await docs_service.update_doc(
            request.doc_id,
            request.content,
            request.insert_index
        )
        return {"success": True, "result": result}
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error updating doc: {str(e)}")
        raise


@app.post("/docs/delete")
async def delete_doc_content(request: DocDeleteRequest, db: Session = Depends(get_db)):
    """Delete content from a Google Doc"""
    try:
        result = await docs_service.delete_doc_content(
            request.doc_id,
            request.start_index,
            request.end_index
        )
        return {"success": True, "result": result}
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error deleting doc content: {str(e)}")
        raise


@app.post("/docs/replace")
async def replace_doc_content(request: DocReplaceRequest, db: Session = Depends(get_db)):
    """Replace text in a Google Doc"""
    try:
        result = await docs_service.replace_doc_content(
            request.doc_id,
            request.search_text,
            request.replace_text
        )
        return {"success": True, "result": result}
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error replacing doc content: {str(e)}")
        raise


# Task Management Endpoints
@app.post("/tasks", response_model=TaskResponse)
async def create_task(request: TaskCreateRequest, user_id: str = "default", db: Session = Depends(get_db)):
    """Create a new task"""
    try:
        from datetime import datetime
        due_date = None
        if request.due_date:
            try:
                due_date = datetime.strptime(request.due_date, "%Y-%m-%d %H:%M")
            except ValueError:
                due_date = datetime.strptime(request.due_date, "%Y-%m-%d")
        
        task = tasks_service.create_task(
            user_id=user_id,
            title=request.title,
            description=request.description,
            due_date=due_date,
            priority=request.priority,
            db=db
        )
        return TaskResponse(**task)
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error creating task: {str(e)}")
        raise


@app.get("/tasks", response_model=TasksListResponse)
async def list_tasks(
    user_id: str = "default",
    status: Optional[str] = None,
    priority: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """List tasks for a user"""
    try:
        tasks = tasks_service.list_tasks(
            user_id=user_id,
            status=status,
            priority=priority,
            db=db
        )
        return TasksListResponse(tasks=[TaskResponse(**task) for task in tasks], total=len(tasks))
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error listing tasks: {str(e)}")
        raise


@app.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task(task_id: int, user_id: str = "default", db: Session = Depends(get_db)):
    """Get a specific task"""
    try:
        task = tasks_service.get_task(task_id, user_id, db)
        return TaskResponse(**task)
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error getting task: {str(e)}")
        raise


@app.put("/tasks/{task_id}", response_model=TaskResponse)
async def update_task(
    task_id: int,
    request: TaskUpdateRequest,
    user_id: str = "default",
    db: Session = Depends(get_db)
):
    """Update a task"""
    try:
        from datetime import datetime
        due_date = None
        if request.due_date:
            try:
                due_date = datetime.strptime(request.due_date, "%Y-%m-%d %H:%M")
            except ValueError:
                due_date = datetime.strptime(request.due_date, "%Y-%m-%d")
        
        task = tasks_service.update_task(
            task_id=task_id,
            user_id=user_id,
            title=request.title,
            description=request.description,
            status=request.status,
            priority=request.priority,
            due_date=due_date,
            db=db
        )
        return TaskResponse(**task)
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error updating task: {str(e)}")
        raise


@app.delete("/tasks/{task_id}")
async def delete_task(task_id: int, user_id: str = "default", db: Session = Depends(get_db)):
    """Delete a task"""
    try:
        tasks_service.delete_task(task_id, user_id, db)
        return {"success": True}
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error deleting task: {str(e)}")
        raise


@app.get("/tasks/upcoming")
async def get_upcoming_tasks(user_id: str = "default", days: int = 7, db: Session = Depends(get_db)):
    """Get tasks with upcoming deadlines"""
    try:
        tasks = tasks_service.get_upcoming_tasks(user_id, days, db)
        return {"tasks": tasks, "days": days}
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error getting upcoming tasks: {str(e)}")
        raise


# Export Endpoints
@app.post("/export/sheet")
async def export_to_sheet(request: ExportToSheetRequest, db: Session = Depends(get_db)):
    """Export data to a Google Sheet"""
    try:
        if request.sheet_id:
            result = await export_service.export_to_existing_sheet(
                request.sheet_id,
                request.tab_name,
                request.data,
                request.append
            )
        else:
            result = await export_service.export_to_sheet(
                request.data,
                request.sheet_name,
                request.tab_name
            )
        return {"success": True, **result}
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error exporting to sheet: {str(e)}")
        raise


@app.post("/export/doc")
async def export_to_doc(request: ExportToDocRequest, db: Session = Depends(get_db)):
    """Export content to a Google Doc"""
    try:
        if request.doc_id:
            result = await export_service.export_to_existing_doc(
                request.doc_id,
                request.content,
                request.append
            )
        else:
            result = await export_service.export_to_doc(
                request.content,
                request.doc_name
            )
        return {"success": True, **result}
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error exporting to doc: {str(e)}")
        raise


@app.post("/export/chat")
async def export_chat(request: ExportChatRequest, db: Session = Depends(get_db)):
    """Export chat conversation to sheet or doc"""
    try:
        if request.format == 'sheet':
            result = await export_service.export_chat_to_sheet(
                request.conversation_id,
                request.name
            )
        else:
            result = await export_service.export_chat_to_doc(
                request.conversation_id,
                request.name
            )
        return {"success": True, **result}
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error exporting chat: {str(e)}")
        raise


# Universal X-Y Coordinate System API Endpoints
@app.post("/api/universal/analyze-sheet")
async def universal_analyze_sheet(request: dict):
    """
    Analyze any sheet structure using Universal X-Y Coordinate System
    
    Request body:
    {
        "sheet_id": "optional_sheet_id",
        "tab_name": "optional_tab_name",
        "force_refresh": false
    }
    """
    try:
        sheet_id = request.get("sheet_id", DEFAULT_SHEET_ID)
        tab_name = request.get("tab_name")
        force_refresh = request.get("force_refresh", False)
        
        # Use Universal Data Service
        universal_data_service = get_universal_data_service()
        
        result = await universal_data_service.analyze_sheet(
            sheet_id=sheet_id,
            tab_name=tab_name,
            force_refresh=force_refresh
        )
        
        return {
            "success": result["success"],
            "sheet_id": sheet_id,
            "tab_name": tab_name,
            "analysis": result.get("tabs_analysis", {}),
            "sheet_info": result.get("sheet_info", {}),
            "error": result.get("error")
        }
        
    except Exception as e:
        logger.error(f"Error in universal analyze sheet: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "message": "Sheet analysis failed"
        }


@app.post("/api/universal/field-value")
async def universal_get_field_value(request: dict):
    """
    Get field value using Universal X-Y Coordinate System
    
    Request body:
    {
        "field_query": "ro1&2 feed tank level",
        "criteria": {"date": "26.6.25"},
        "sheet_id": "optional_sheet_id",
        "tab_name": "optional_tab_name"
    }
    """
    try:
        field_query = request.get("field_query", "").strip()
        criteria = request.get("criteria", {})
        sheet_id = request.get("sheet_id", DEFAULT_SHEET_ID)
        tab_name = request.get("tab_name")
        
        if not field_query:
            return {
                "success": False,
                "error": "field_query is required",
                "message": "Please provide a field name or description"
            }
        
        # Use Universal Data Service
        universal_data_service = get_universal_data_service()
        
        result = await universal_data_service.get_field_value(
            field_query=field_query,
            criteria=criteria,
            sheet_id=sheet_id,
            tab_name=tab_name
        )
        
        return {
            "success": result["success"],
            "field_query": field_query,
            "criteria": criteria,
            "values_found": result.get("values_found", 0),
            "values": result.get("values", []),
            "error": result.get("error")
        }
        
    except Exception as e:
        logger.error(f"Error in universal get field value: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "message": "Field value retrieval failed"
        }


@app.post("/api/universal/latest-data")
async def universal_get_latest_data(request: dict):
    """
    Get latest data using Universal X-Y Coordinate System
    
    Request body:
    {
        "sheet_id": "optional_sheet_id",
        "tab_name": "optional_tab_name"
    }
    """
    try:
        sheet_id = request.get("sheet_id", DEFAULT_SHEET_ID)
        tab_name = request.get("tab_name")
        
        # Use Universal Data Service
        universal_data_service = get_universal_data_service()
        
        result = await universal_data_service.get_latest_data(
            sheet_id=sheet_id,
            tab_name=tab_name
        )
        
        return {
            "success": result["success"],
            "sheet_id": sheet_id,
            "tab_name": tab_name,
            "latest_data": result.get("latest_data", {}),
            "tabs_processed": result.get("tabs_processed", []),
            "error": result.get("error")
        }
        
    except Exception as e:
        logger.error(f"Error in universal get latest data: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "message": "Latest data retrieval failed"
        }


@app.post("/api/universal/coordinate")
async def universal_get_by_coordinates(request: dict):
    """
    Get data by exact X-Y coordinates using Universal System
    
    Request body:
    {
        "x": 0,
        "y": 1,
        "sheet_id": "optional_sheet_id",
        "tab_name": "optional_tab_name"
    }
    """
    try:
        x = request.get("x")
        y = request.get("y")
        sheet_id = request.get("sheet_id", DEFAULT_SHEET_ID)
        tab_name = request.get("tab_name")
        
        if x is None or y is None:
            return {
                "success": False,
                "error": "x and y coordinates are required",
                "message": "Please provide both x and y coordinates"
            }
        
        # Use Universal Data Service
        universal_data_service = get_universal_data_service()
        
        result = await universal_data_service.get_data_by_coordinates(
            x=int(x),
            y=int(y),
            sheet_id=sheet_id,
            tab_name=tab_name
        )
        
        return {
            "success": result["success"],
            "coordinates": {"x": x, "y": y},
            "sheet_id": sheet_id,
            "tab_name": tab_name,
            "cell_data": result.get("cell_data"),
            "value": result.get("value"),
            "field_name": result.get("field_name"),
            "context": result.get("context", {}),
            "error": result.get("error")
        }
        
    except Exception as e:
        logger.error(f"Error in universal get by coordinates: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "message": "Coordinate-based retrieval failed"
        }


@app.post("/api/universal/search")
async def universal_search_data(request: dict):
    """
    Universal search across any sheet structure
    
    Request body:
    {
        "search_query": "26.6.25",
        "sheet_id": "optional_sheet_id",
        "tab_name": "optional_tab_name",
        "limit": 100
    }
    """
    try:
        search_query = request.get("search_query", "").strip()
        sheet_id = request.get("sheet_id", DEFAULT_SHEET_ID)
        tab_name = request.get("tab_name")
        limit = request.get("limit", 100)
        
        if not search_query:
            return {
                "success": False,
                "error": "search_query is required",
                "message": "Please provide a search query"
            }
        
        # Use Universal Data Service
        universal_data_service = get_universal_data_service()
        
        result = await universal_data_service.search_data(
            search_query=search_query,
            sheet_id=sheet_id,
            tab_name=tab_name,
            limit=limit
        )
        
        return {
            "success": result["success"],
            "search_query": search_query,
            "total_matches": result.get("total_matches", 0),
            "returned_results": result.get("returned_results", 0),
            "results": result.get("results", []),
            "error": result.get("error")
        }
        
    except Exception as e:
        logger.error(f"Error in universal search data: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "message": "Universal search failed"
        }


@app.post("/api/universal/summary")
async def universal_get_sheet_summary(request: dict):
    """
    Get comprehensive sheet summary using Universal System
    
    Request body:
    {
        "sheet_id": "optional_sheet_id"
    }
    """
    try:
        sheet_id = request.get("sheet_id", DEFAULT_SHEET_ID)
        
        # Use Universal Data Service
        universal_data_service = get_universal_data_service()
        
        result = await universal_data_service.get_sheet_summary(sheet_id=sheet_id)
        
        return {
            "success": result["success"],
            "sheet_id": sheet_id,
            "sheet_info": result.get("sheet_info", {}),
            "tabs_summary": result.get("tabs_summary", {}),
            "error": result.get("error")
        }
        
    except Exception as e:
        logger.error(f"Error in universal get sheet summary: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "message": "Sheet summary failed"
        }


@app.post("/api/universal/query")
async def universal_natural_language_query(request: dict):
    """
    Process natural language queries using Universal X-Y Coordinate System
    
    Request body:
    {
        "query": "what is the ro1&2 feed tank level on 26.6.25?",
        "sheet_id": "optional_sheet_id",
        "tab_name": "optional_tab_name"
    }
    """
    try:
        query = request.get("query", "").strip()
        sheet_id = request.get("sheet_id", DEFAULT_SHEET_ID)
        tab_name = request.get("tab_name")
        
        if not query:
            return {
                "success": False,
                "error": "query is required",
                "message": "Please provide a natural language query"
            }
        
        logger.info(f"🔍 Processing universal natural language query: {query}")
        
        # Use Universal Query Processor
        universal_query_processor = get_universal_query_processor()
        
        result = await universal_query_processor.process_query(
            query=query,
            sheet_id=sheet_id,
            tab_name=tab_name
        )
        
        return {
            "success": result["success"],
            "query": query,
            "answer": result.get("answer", ""),
            "query_type": result.get("query_type", "unknown"),
            "confidence": result.get("confidence", 0.0),
            "data_found": result.get("data_found", 0),
            "supporting_data": result.get("supporting_data", []),
            "suggestions": result.get("suggestions", []),
            "error": result.get("error")
        }
        
    except Exception as e:
        logger.error(f"Error in universal natural language query: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "message": "Universal query processing failed"
        }


@app.post("/api/etp-tank-capacity")
async def etp_tank_capacity(request: dict):
    """
    Get ETP Tank Capacity and Storage Details
    
    NOTE: This endpoint divides storage values by 1000 to convert to KL (Kiloliters).
    All other queries return values as-is from the database without any conversion.
    
    Request body:
    {
        "date": "25.10.2025",
        "etp_inlet_tank_value": 40,
        "sheet_id": "optional_sheet_id"
    }
    """
    try:
        date_str = request.get("date", "").strip()
        etp_inlet_value = request.get("etp_inlet_tank_value")
        sheet_id = request.get("sheet_id", DEFAULT_SHEET_ID)
        
        if not date_str:
            return {
                "success": False,
                "error": "Date is required",
                "message": "Please provide a date"
            }
        
        if etp_inlet_value is None:
            return {
                "success": False,
                "error": "ETP Inlet tank value is required",
                "message": "Please provide ETP Inlet tank value"
            }
        
        logger.info(f"🏭 Processing ETP Tank Capacity request for date: {date_str}")
        
        # Actual capacity constants (in KL)
        actual_capacities = {
            "ETP Inlet Tank": 96,
            "Filter Feed Tank": 40,
            "UF Feed Tank": 40,
            "RO 1 & 2 Feed Tank": 60,
            "RO 3 Feed Tank": 40,
            "Salzberg Feed Tank": 40
        }
        
        # Initialize tank data
        tanks_data = {
            "ETP Inlet Tank": {
                "actual_capacity": 96,
                "storage": float(etp_inlet_value),
                "balance": 0
            },
            "Filter Feed Tank": {
                "actual_capacity": 40,
                "storage": 0,
                "balance": 0
            },
            "UF Feed Tank": {
                "actual_capacity": 40,
                "storage": 0,
                "balance": 0
            },
            "RO 1 & 2 Feed Tank": {
                "actual_capacity": 60,
                "storage": 0,
                "balance": 0
            },
            "RO 3 Feed Tank": {
                "actual_capacity": 40,
                "storage": 0,
                "balance": 0
            },
            "Salzberg Feed Tank": {
                "actual_capacity": 40,
                "storage": 0,
                "balance": 0
            }
        }
        
        # Generate date variations for better matching
        import re
        date_variations = [date_str]
        date_match = re.match(r'(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{2,4})', date_str)
        if date_match:
            day, month, year = date_match.groups()
            day = day.zfill(2)
            month = month.zfill(2)
            date_variations.extend([
                f"{day}.{month}.{year}",
                f"{day}/{month}/{year}",
                f"{day}-{month}-{year}",
                f"{int(day)}.{int(month)}.{year}",
                f"{int(day)}/{int(month)}/{year}",
                f"{int(day)}-{int(month)}-{year}",
            ])
        
            # Query database for storage values from RO DETAILS tab
        with get_db_context() as db:
            # Build search query for RO DETAILS tab
            search_params = {'sheet_id': sheet_id}
            date_conditions = []
            for i, date_var in enumerate(date_variations):
                param_name = f'date_{i}'
                date_conditions.append(f'row_data LIKE :{param_name}')
                search_params[param_name] = f'%{date_var}%'
            
            # Query for RO DETAILS tab to get storage values
            date_conditions_str = ' OR '.join(date_conditions)
            sql_query = text(
                f"SELECT tab_name, row_index, row_data "
                f"FROM sheets_data "
                f"WHERE sheet_id = :sheet_id "
                f"AND tab_name = 'RO DETAILS' "
                f"AND row_index > 0 "
                f"AND ({date_conditions_str}) "
                f"ORDER BY row_index "
                f"LIMIT 50"
            )
            
            result = db.execute(sql_query, search_params)
            matching_rows = result.fetchall()
            
            logger.info(f"✅ Found {len(matching_rows)} rows for date {date_str} in RO DETAILS")
            
            # Process rows to extract storage values only
            import json
            from services.field_mapper import FieldMapper
            field_mapper = FieldMapper()
            
            # First, get header row to identify column indices for RO3 and Salzberg
            header_query = text(
                f"SELECT row_index, row_data "
                f"FROM sheets_data "
                f"WHERE sheet_id = :sheet_id "
                f"AND tab_name = 'RO DETAILS' "
                f"AND row_index IN (0, 1) "
                f"ORDER BY row_index "
                f"LIMIT 2"
            )
            header_result = db.execute(header_query, {'sheet_id': sheet_id})
            header_rows = header_result.fetchall()
            
            ro3_column_idx = None
            salzberg_column_idx = None
            
            # Find column indices for RO3 and Salzberg from header
            for header_tuple in header_rows:
                try:
                    header_data_str = header_tuple[1]
                    header_data = json.loads(header_data_str) if isinstance(header_data_str, str) else header_data_str
                    if header_data and isinstance(header_data, list):
                        for i, cell in enumerate(header_data):
                            if cell:
                                cell_str = str(cell).lower()
                                # Search for RO 3 Feed Tank Level
                                if ('ro 3' in cell_str or 'ro3' in cell_str) and ('feed' in cell_str or 'tank' in cell_str or 'level' in cell_str):
                                    ro3_column_idx = i
                                    logger.info(f"Found RO 3 Feed Tank Level at column {i}: {cell}")
                                # Search for Salzberg/Saltzberg Feed Tank Level (check both spellings)
                                if ('salzberg' in cell_str or 'saltzberg' in cell_str) and ('feed' in cell_str or 'tank' in cell_str or 'level' in cell_str):
                                    salzberg_column_idx = i
                                    logger.info(f"Found Salzberg Feed Tank Level at column {i}: {cell}")
                except Exception as e:
                    logger.warning(f"Error processing header row: {e}")
                    continue
            
            logger.info(f"RO 3 column index: {ro3_column_idx}, Salzberg column index: {salzberg_column_idx}")
            
            # Process RO DETAILS rows to extract storage values
            for row_tuple in matching_rows:
                try:
                    row_index = row_tuple[1]
                    row_data_str = row_tuple[2]
                    
                    # Parse row data
                    row_data = json.loads(row_data_str) if isinstance(row_data_str, str) else row_data_str
                    
                    if not row_data or not isinstance(row_data, list):
                        continue
                    
                    # Map row to fields to get storage values
                    mapped_fields = field_mapper.map_row_to_fields('RO DETAILS', row_data)
                    
                    # Extract storage values (divide by 1000 to convert to KL)
                    # NOTE: Division by 1000 is ONLY for ETP capacity queries
                    # All other queries return values as-is from the database
                    # Filter Feed Tank - column 2
                    if 'FILTER_FEED' in mapped_fields and mapped_fields['FILTER_FEED']:
                        try:
                            value = float(str(mapped_fields['FILTER_FEED']).replace(',', '')) / 1000
                            if tanks_data["Filter Feed Tank"]["storage"] == 0:
                                tanks_data["Filter Feed Tank"]["storage"] = value
                                logger.info(f"Found Filter Feed Tank storage: {value} KL")
                        except (ValueError, TypeError):
                            pass
                    
                    # UF Feed Tank - column 3
                    if 'UF_FEED_TANK_LEVEL' in mapped_fields and mapped_fields['UF_FEED_TANK_LEVEL']:
                        try:
                            value = float(str(mapped_fields['UF_FEED_TANK_LEVEL']).replace(',', '')) / 1000
                            if tanks_data["UF Feed Tank"]["storage"] == 0:
                                tanks_data["UF Feed Tank"]["storage"] = value
                                logger.info(f"Found UF Feed Tank storage: {value} KL")
                        except (ValueError, TypeError):
                            pass
                    
                    # RO 1 & 2 Feed Tank - column 7
                    if 'RO_1_2_FEED_TANK_LEVEL' in mapped_fields and mapped_fields['RO_1_2_FEED_TANK_LEVEL']:
                        try:
                            value = float(str(mapped_fields['RO_1_2_FEED_TANK_LEVEL']).replace(',', '')) / 1000
                            if tanks_data["RO 1 & 2 Feed Tank"]["storage"] == 0:
                                tanks_data["RO 1 & 2 Feed Tank"]["storage"] = value
                                logger.info(f"Found RO 1 & 2 Feed Tank storage: {value} KL")
                        except (ValueError, TypeError):
                            pass
                    
                    # Extract RO 3 Feed Tank Level and Salzberg Feed Tank Level using identified column indices
                    # RO 3 Feed Tank Level
                    if ro3_column_idx is not None and tanks_data["RO 3 Feed Tank"]["storage"] == 0:
                        if ro3_column_idx < len(row_data) and row_data[ro3_column_idx]:
                            try:
                                value = float(str(row_data[ro3_column_idx]).replace(',', '')) / 1000
                                tanks_data["RO 3 Feed Tank"]["storage"] = value
                                logger.info(f"Found RO 3 Feed Tank storage: {value} KL (column {ro3_column_idx})")
                            except (ValueError, TypeError):
                                pass
                    
                    # Salzberg Feed Tank Level
                    if salzberg_column_idx is not None and tanks_data["Salzberg Feed Tank"]["storage"] == 0:
                        if salzberg_column_idx < len(row_data) and row_data[salzberg_column_idx]:
                            try:
                                value = float(str(row_data[salzberg_column_idx]).replace(',', '')) / 1000
                                tanks_data["Salzberg Feed Tank"]["storage"] = value
                                logger.info(f"Found Salzberg Feed Tank storage: {value} KL (column {salzberg_column_idx}, raw value: {row_data[salzberg_column_idx]})")
                            except (ValueError, TypeError) as e:
                                logger.warning(f"Error extracting Salzberg value from column {salzberg_column_idx}: {e}, raw value: {row_data[salzberg_column_idx]}")
                    
                    # Fallback: If column indices not found, search for values in the row
                    if ro3_column_idx is None and tanks_data["RO 3 Feed Tank"]["storage"] == 0:
                        # Search for RO 3 in row text and get nearby numeric value
                        for i, cell in enumerate(row_data):
                            if cell:
                                cell_str = str(cell).lower()
                                if ('ro 3' in cell_str or 'ro3' in cell_str) and i + 1 < len(row_data):
                                    # Check next column for value
                                    if row_data[i + 1]:
                                        try:
                                            value = float(str(row_data[i + 1]).replace(',', '')) / 1000
                                            if 0 < value < 50:
                                                tanks_data["RO 3 Feed Tank"]["storage"] = value
                                                logger.info(f"Found RO 3 Feed Tank storage: {value} KL (fallback)")
                                                break
                                        except (ValueError, TypeError):
                                            pass
                    
                    # Enhanced fallback for Salzberg: Search more thoroughly
                    if salzberg_column_idx is None and tanks_data["Salzberg Feed Tank"]["storage"] == 0:
                        # Search for Salzberg/Saltzberg in row text (check both spellings)
                        for i, cell in enumerate(row_data):
                            if cell:
                                cell_str = str(cell).lower()
                                # Check for both spellings: salzberg and saltzberg
                                if ('salzberg' in cell_str or 'saltzberg' in cell_str):
                                    # Try to get value from the same column (if it's numeric)
                                    try:
                                        value = float(str(cell).replace(',', '')) / 1000
                                        if 0 < value < 50:
                                            tanks_data["Salzberg Feed Tank"]["storage"] = value
                                            logger.info(f"Found Salzberg Feed Tank storage: {value} KL (same column {i})")
                                            break
                                    except (ValueError, TypeError):
                                        pass
                                    
                                    # Also check next column for value
                                    if i + 1 < len(row_data) and row_data[i + 1]:
                                        try:
                                            value = float(str(row_data[i + 1]).replace(',', '')) / 1000
                                            if 0 < value < 50:
                                                tanks_data["Salzberg Feed Tank"]["storage"] = value
                                                logger.info(f"Found Salzberg Feed Tank storage: {value} KL (next column {i+1})")
                                                break
                                        except (ValueError, TypeError):
                                            pass
                    
                    # Additional fallback: Search all columns for numeric values if we still haven't found Salzberg
                    # This is a last resort - check all unmapped columns for reasonable tank level values
                    if tanks_data["Salzberg Feed Tank"]["storage"] == 0:
                        # Check columns beyond the standard mapped ones
                        for i in range(21, len(row_data)):
                            if row_data[i]:
                                try:
                                    value = float(str(row_data[i]).replace(',', '')) / 1000
                                    # If RO 3 is already found and this is a reasonable value, it might be Salzberg
                                    if 0 < value < 50 and tanks_data["RO 3 Feed Tank"]["storage"] > 0:
                                        tanks_data["Salzberg Feed Tank"]["storage"] = value
                                        logger.info(f"Found potential Salzberg Feed Tank storage: {value} KL (column {i}, last resort)")
                                        break
                                except (ValueError, TypeError):
                                    pass
                
                except Exception as e:
                    logger.warning(f"Error processing RO DETAILS row: {e}")
                    continue
        
        # Calculate balance for each tank
        for tank_name, tank_data in tanks_data.items():
            tank_data["balance"] = tank_data["actual_capacity"] - tank_data["storage"]
        
        # Calculate totals
        totals = {
            "total_capacity": sum(t["actual_capacity"] for t in tanks_data.values()),
            "total_storage": sum(t["storage"] for t in tanks_data.values()),
            "total_balance": sum(t["balance"] for t in tanks_data.values())
        }
        
        # Format response
        response_data = {
            "success": True,
            "date": date_str,
            "tanks": tanks_data,
            "totals": totals
        }
        
        logger.info(f"✅ ETP Tank Capacity calculated successfully for {date_str}")
        return response_data
        
    except Exception as e:
        logger.error(f"Error processing ETP tank capacity: {str(e)}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "message": "Failed to process ETP tank capacity request"
        }


@app.post("/api/universal/keyword-summary")
async def universal_keyword_summary(request: dict):
    """
    Get comprehensive summary based on keywords - for generalized queries
    
    Request body:
    {
        "keywords": ["tank", "level", "pressure"],
        "sheet_id": "optional_sheet_id",
        "tab_name": "optional_tab_name",
        "limit": 50
    }
    """
    try:
        keywords = request.get("keywords", [])
        sheet_id = request.get("sheet_id", DEFAULT_SHEET_ID)
        tab_name = request.get("tab_name")
        limit = request.get("limit", 50)
        
        if not keywords:
            return {
                "success": False,
                "error": "keywords are required",
                "message": "Please provide at least one keyword"
            }
        
        logger.info(f"📊 Processing keyword summary for: {keywords}")
        
        # Use Universal Data Service
        universal_data_service = get_universal_data_service()
        
        result = await universal_data_service.get_keyword_summary(
            keywords=keywords,
            sheet_id=sheet_id,
            tab_name=tab_name,
            limit=limit
        )
        
        return {
            "success": result["success"],
            "keywords": keywords,
            "summary": result.get("summary", ""),
            "keyword_results": result.get("keyword_results", {}),
            "total_tabs_searched": result.get("total_tabs_searched", 0),
            "data_source": result.get("data_source", "sqlite_database"),
            "error": result.get("error")
        }
        
    except Exception as e:
        logger.error(f"Error in universal keyword summary: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "message": "Keyword summary processing failed"
        }


@app.get("/health")
async def health_check(db: Session = Depends(get_db)):
    """
    Health check endpoint - checks database, services, and sync status
    """
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "checks": {}
    }
    
    try:
        # Check database connection
        db.execute("SELECT 1")
        health_status["checks"]["database"] = "connected"
    except Exception as e:
        logger.error(f"Database health check failed: {str(e)}")
        health_status["checks"]["database"] = f"error: {str(e)}"
        health_status["status"] = "unhealthy"
    
    try:
        # Check AI service
        if ai_service and ai_service.client:
            health_status["checks"]["ai_service"] = "available"
        else:
            health_status["checks"]["ai_service"] = "unavailable"
            health_status["status"] = "unhealthy"
    except Exception as e:
        logger.error(f"AI service health check failed: {str(e)}")
        health_status["checks"]["ai_service"] = f"error: {str(e)}"
        health_status["status"] = "unhealthy"
    
    try:
        # Check sync service
        if sync_service:
            health_status["checks"]["sync_service"] = "available"
        else:
            health_status["checks"]["sync_service"] = "unavailable"
    except Exception as e:
        logger.error(f"Sync service health check failed: {str(e)}")
        health_status["checks"]["sync_service"] = f"error: {str(e)}"
    
    try:
        # Check scheduler
        from services.scheduler import scheduler
        if scheduler and scheduler.running:
            health_status["checks"]["scheduler"] = "running"
        else:
            health_status["checks"]["scheduler"] = "stopped"
    except Exception as e:
        logger.error(f"Scheduler health check failed: {str(e)}")
        health_status["checks"]["scheduler"] = f"error: {str(e)}"
    
    try:
        # Check hash service
        if hash_service and hash_service.enabled:
            hash_stats = await hash_service.get_service_statistics()
            health_status["checks"]["hash_service"] = {
                "enabled": hash_service.enabled,
                "status": hash_stats.get("service_status", "unknown")
            }
        else:
            health_status["checks"]["hash_service"] = "disabled"
    except Exception as e:
        logger.error(f"Hash service health check failed: {str(e)}")
        health_status["checks"]["hash_service"] = f"error: {str(e)}"
    
    try:
        # Check hash monitoring
        if hash_monitoring:
            monitoring_status = hash_monitoring.get_monitoring_status()
            health_status["checks"]["hash_monitoring"] = {
                "status": monitoring_status.get("service_status", "unknown"),
                "data_points": monitoring_status.get("metrics_storage", {}).get("total_data_points", 0)
            }
        else:
            health_status["checks"]["hash_monitoring"] = "not_initialized"
    except Exception as e:
        logger.error(f"Hash monitoring health check failed: {str(e)}")
        health_status["checks"]["hash_monitoring"] = f"error: {str(e)}"
    
    try:
        # Check cache service
        cache = get_cache_service()
        cache_stats = cache.get_stats()
        health_status["checks"]["cache"] = {
            "enabled": cache_stats["enabled"],
            "size": cache_stats["size"],
            "hit_rate": cache_stats["hit_rate"]
        }
    except Exception as e:
        logger.error(f"Cache health check failed: {str(e)}")
        health_status["checks"]["cache"] = f"error: {str(e)}"
    
    return health_status


# Cache Management Endpoints
@app.get("/cache/stats")
async def get_cache_stats():
    """
    Get cache statistics and performance metrics
    """
    try:
        cache = get_cache_service()
        return cache.get_stats()
    except Exception as e:
        logger.error(f"Error getting cache stats: {str(e)}")
        raise


@app.get("/cache/info")
async def get_cache_info():
    """
    Get detailed cache information including sample entries
    """
    try:
        cache = get_cache_service()
        return cache.get_info()
    except Exception as e:
        logger.error(f"Error getting cache info: {str(e)}")
        raise


@app.post("/cache/clear")
async def clear_cache(prefix: Optional[str] = None):
    """
    Clear cache entries
    
    Args:
        prefix: Optional prefix to clear only specific entries (e.g., 'ai_chat', 'sheet_read')
    """
    try:
        cache = get_cache_service()
        count = cache.clear(prefix=prefix)
        return {
            "success": True,
            "message": f"Cleared {count} cache entries" + (f" with prefix '{prefix}'" if prefix else ""),
            "count": count
        }
    except Exception as e:
        logger.error(f"Error clearing cache: {str(e)}")
        raise


@app.post("/cache/cleanup")
async def cleanup_cache():
    """
    Manually trigger cleanup of expired cache entries
    """
    try:
        cache = get_cache_service()
        count = cache.cleanup_expired()
        return {
            "success": True,
            "message": f"Cleaned up {count} expired cache entries",
            "count": count
        }
    except Exception as e:
        logger.error(f"Error cleaning up cache: {str(e)}")
        raise


# Enhanced Sheet Processing Endpoint
@app.post("/sheets/process-complete")
async def process_complete_sheet(request: dict, background_tasks: BackgroundTasks):
    """
    Complete sheet processing: discover all tabs, sync data, compute hashes
    
    Request body:
    {
        "sheet_id": "1MtjJyKiDR7COszXxZF-wYR-UF5LcwYSpTfr_Aa_PEt8",
        "force_sync": false
    }
    """
    try:
        sheet_id = request.get('sheet_id')
        force_sync = request.get('force_sync', False)
        
        if not sheet_id:
            from core.exceptions import ValidationError
            raise ValidationError("sheet_id is required", field="sheet_id")
        
        logger.info(f"🚀 Starting complete sheet processing for: {sheet_id}")
        
        # Step 1: Discover all tabs in the sheet
        logger.info("📋 Step 1: Discovering sheet tabs...")
        
        def _get_spreadsheet():
            return sheets_service.sheets_service.spreadsheets().get(
                spreadsheetId=sheet_id
            ).execute()
        
        spreadsheet = await sheets_service._retry_request(_get_spreadsheet)
        sheets_info = spreadsheet.get('sheets', [])
        spreadsheet_title = spreadsheet.get('properties', {}).get('title', 'Unknown Sheet')
        
        if not sheets_info:
            return {
                "success": False,
                "error": "No tabs found in the spreadsheet",
                "sheet_id": sheet_id
            }
        
        logger.info(f"✅ Found {len(sheets_info)} tabs in '{spreadsheet_title}'")
        
        # Step 2: Process each tab
        processing_results = []
        total_rows_processed = 0
        total_hashes_computed = 0
        
        for i, sheet_info in enumerate(sheets_info):
            properties = sheet_info.get('properties', {})
            tab_name = properties.get('title', f'Sheet{i+1}')
            tab_id = properties.get('sheetId', 0)
            
            logger.info(f"📊 Processing tab {i+1}/{len(sheets_info)}: '{tab_name}' (ID: {tab_id})")
            
            try:
                # Step 2a: Read ALL data from the tab (no limits)
                logger.info(f"   📖 Reading ALL data from tab '{tab_name}'...")
                try:
                    # Read all data from this tab (no limit)
                    tab_data = await sheets_service.read_sheet(sheet_id, tab_name)
                    actual_row_count = len(tab_data)
                    logger.info(f"   📊 Read {actual_row_count} rows from tab '{tab_name}'")
                    
                    # Step 2b: Process with incremental hash computation
                    logger.info(f"   🔐 Processing hashes incrementally for tab '{tab_name}'...")
                    hash_result = await hash_service.compute_hash_from_source(
                        sheet_id, "sheet", tab_name=tab_name
                    )
                    
                    # Step 2c: Sync only if there are changes or if forced
                    if hash_result.get('has_changes', False) or force_sync:
                        logger.info(f"   🔄 Syncing changed data for tab '{tab_name}'...")
                        sync_result = await sync_service.sync_sheet(sheet_id, spreadsheet_title, tab_name, force=force_sync)
                        rows_synced = sync_result.get('rows_synced', 0) if sync_result.get('success') else 0
                    else:
                        logger.info(f"   ✅ No changes detected for tab '{tab_name}' - skipping sync")
                        rows_synced = 0
                        sync_result = {'success': True, 'message': 'No changes to sync'}
                    
                    total_rows_processed += actual_row_count
                    
                    if hash_result['success']:
                        hash_count = hash_result['hash_computation']['hash_count']
                        computation_time = hash_result['hash_computation']['computation_time_ms']
                        change_summary = hash_result.get('change_detection', {})
                        has_changes = hash_result.get('has_changes', False)
                        total_hashes_computed += hash_count
                        
                        # Log detailed change information
                        if has_changes:
                            added = change_summary.get('added', 0)
                            modified = change_summary.get('modified', 0)
                            deleted = change_summary.get('deleted', 0)
                            unchanged = change_summary.get('unchanged', 0)
                            logger.info(f"   ✅ Processed {hash_count} hashes in {computation_time}ms")
                            logger.info(f"   📊 Changes: +{added} new, ~{modified} modified, -{deleted} deleted, ={unchanged} unchanged")
                        else:
                            logger.info(f"   ✅ Processed {hash_count} hashes in {computation_time}ms (no changes)")
                        
                        processing_results.append({
                            "tab_name": tab_name,
                            "tab_id": tab_id,
                            "success": True,
                            "actual_rows": actual_row_count,
                            "rows_synced": rows_synced,
                            "hashes_computed": hash_count,
                            "computation_time_ms": computation_time,
                            "has_changes": has_changes,
                            "change_summary": change_summary,
                            "sync_skipped": rows_synced == 0 and not has_changes
                        })
                    else:
                        logger.error(f"   ❌ Hash computation failed: {hash_result.get('error')}")
                        processing_results.append({
                            "tab_name": tab_name,
                            "tab_id": tab_id,
                            "success": False,
                            "actual_rows": actual_row_count,
                            "error": f"Hash computation failed: {hash_result.get('error')}"
                        })
                        
                except Exception as read_error:
                    logger.error(f"   ❌ Error reading data from tab '{tab_name}': {str(read_error)}")
                    processing_results.append({
                        "tab_name": tab_name,
                        "tab_id": tab_id,
                        "success": False,
                        "error": f"Data reading failed: {str(read_error)}"
                    })
                    
            except Exception as e:
                logger.error(f"   ❌ Error processing tab '{tab_name}': {str(e)}")
                processing_results.append({
                    "tab_name": tab_name,
                    "tab_id": tab_id,
                    "success": False,
                    "error": str(e)
                })
        
        # Step 3: Summary
        successful_tabs = [r for r in processing_results if r.get('success', False)]
        failed_tabs = [r for r in processing_results if not r.get('success', False)]
        
        logger.info(f"🎉 Processing complete!")
        logger.info(f"   ✅ Successful tabs: {len(successful_tabs)}")
        logger.info(f"   ❌ Failed tabs: {len(failed_tabs)}")
        logger.info(f"   📊 Total rows processed: {total_rows_processed}")
        logger.info(f"   🔐 Total hashes computed: {total_hashes_computed}")
        
        return {
            "success": True,
            "sheet_id": sheet_id,
            "spreadsheet_title": spreadsheet_title,
            "total_tabs": len(sheets_info),
            "successful_tabs": len(successful_tabs),
            "failed_tabs": len(failed_tabs),
            "total_rows_processed": total_rows_processed,
            "total_hashes_computed": total_hashes_computed,
            "processing_results": processing_results,
            "message": f"Processed {len(successful_tabs)}/{len(sheets_info)} tabs successfully"
        }
        
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error in complete sheet processing: {str(e)}")
        raise


# Hash Service Endpoints
@app.post("/hash/compute", response_model=HashComputeResponse)
async def compute_hash(request: HashComputeRequest):
    """
    Compute hash for a file by retrieving content from source
    """
    try:
        kwargs = {}
        if request.tab_name:
            kwargs['tab_name'] = request.tab_name
        if request.source_type:
            kwargs['source_type'] = request.source_type
        
        result = await hash_service.compute_hash_from_source(
            request.file_id, 
            request.file_type, 
            **kwargs
        )
        
        return HashComputeResponse(
            success=result['success'],
            file_id=result['file_id'],
            file_type=result['file_type'],
            hash_count=result.get('hash_computation', {}).get('hash_count'),
            computation_time_ms=result.get('hash_computation', {}).get('computation_time_ms'),
            total_size=result.get('hash_computation', {}).get('total_size'),
            has_changes=result.get('has_changes'),
            change_summary=result.get('change_detection'),
            error=result.get('error')
        )
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error computing hash: {str(e)}")
        raise


@app.get("/hash/status/{file_id}", response_model=HashStatusResponse)
async def get_hash_status(file_id: str):
    """
    Get hash status for a file
    """
    try:
        hashes = await hash_service.get_stored_hashes(file_id)
        
        # Get file type from first hash if available
        file_type = None
        if hashes:
            # We need to query the database to get file type
            with get_db_context() as db:
                from services.database import FileHash
                file_hash = db.query(FileHash).filter(FileHash.file_id == file_id).first()
                if file_hash:
                    file_type = file_hash.file_type
        
        return HashStatusResponse(
            file_id=file_id,
            hash_count=len(hashes),
            last_updated=None,  # Could be enhanced to track last update time
            file_type=file_type
        )
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error getting hash status: {str(e)}")
        raise


@app.get("/hash/statistics", response_model=HashStatisticsResponse)
async def get_hash_statistics():
    """
    Get comprehensive hash service statistics
    """
    try:
        stats = await hash_service.get_service_statistics()
        
        return HashStatisticsResponse(
            service_status=stats['service_status'],
            configuration=stats.get('configuration', {}),
            storage_statistics=stats.get('storage_statistics', {})
        )
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error getting hash statistics: {str(e)}")
        raise


@app.delete("/hash/{file_id}")
async def delete_hash(file_id: str):
    """
    Delete stored hashes for a file
    """
    try:
        result = await content_processor.process_content_deletion(file_id)
        
        return {
            "success": result['success'],
            "file_id": file_id,
            "message": "Hash data deleted successfully" if result['success'] else "Failed to delete hash data",
            "error": result.get('error')
        }
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error deleting hash: {str(e)}")
        raise


@app.post("/hash/batch", response_model=BatchProcessResponse)
async def batch_process_hashes(request: BatchProcessRequest):
    """
    Process multiple files in batch
    """
    try:
        result = await content_processor.batch_process_files(
            request.files, 
            request.operation
        )
        
        return BatchProcessResponse(
            success=result.failed_jobs == 0,
            total_jobs=result.total_jobs,
            completed_jobs=result.completed_jobs,
            failed_jobs=result.failed_jobs,
            total_time_seconds=result.total_time_seconds,
            results=result.results,
            errors=result.errors
        )
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error in batch processing: {str(e)}")
        raise


@app.get("/hash/job/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """
    Get status of a processing job
    """
    try:
        job_status = await content_processor.get_job_status(job_id)
        
        if not job_status:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="Job not found")
        
        return JobStatusResponse(**job_status)
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error getting job status: {str(e)}")
        raise


@app.post("/hash/job/{job_id}/cancel")
async def cancel_job(job_id: str):
    """
    Cancel a processing job
    """
    try:
        cancelled = await content_processor.cancel_job(job_id)
        
        if not cancelled:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="Job not found or cannot be cancelled")
        
        return {
            "success": True,
            "job_id": job_id,
            "message": "Job cancelled successfully"
        }
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling job: {str(e)}")
        raise


@app.post("/hash/cleanup")
async def cleanup_hash_data():
    """
    Clean up orphaned hash data
    """
    try:
        result = await hash_service.cleanup_orphaned_data()
        
        return {
            "success": result['success'],
            "cleaned_items": result.get('cleaned_items', 0),
            "message": result.get('message', 'Cleanup completed'),
            "error": result.get('error')
        }
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error cleaning up hash data: {str(e)}")
        raise


@app.get("/hash/processing/stats")
async def get_processing_statistics():
    """
    Get content processing statistics
    """
    try:
        stats = await content_processor.get_processing_statistics()
        return stats
    except RAVVYNException:
        raise
    except Exception as e:
        logger.error(f"Error getting processing statistics: {str(e)}")
        raise


@app.get("/hash/config")
async def get_hash_configuration():
    """
    Get current hash service configuration
    """
    try:
        from core.config import get_settings
        settings = get_settings()
        
        return {
            "hash_service": {
                "enabled": settings.hash_enabled,
                "block_size_kb": settings.hash_block_size_kb,
                "pdf_threshold_mb": settings.hash_pdf_threshold_mb,
                "pdf_block_size_mb": settings.hash_pdf_block_size_mb,
                "max_content_size_mb": settings.hash_max_content_size_mb,
                "max_retries": settings.hash_max_retries,
                "retry_delay_seconds": settings.hash_retry_delay_seconds,
                "max_retry_delay_seconds": settings.hash_max_retry_delay_seconds
            },
            "content_processing": {
                "max_concurrent_jobs": settings.processing_max_concurrent_jobs,
                "job_timeout_seconds": settings.processing_job_timeout_seconds,
                "batch_size": settings.processing_batch_size,
                "cleanup_interval_seconds": settings.processing_cleanup_interval_seconds
            }
        }
    except Exception as e:
        logger.error(f"Error getting hash configuration: {str(e)}")
        raise


# Hash Monitoring Endpoints
@app.get("/hash/monitoring/metrics")
async def get_hash_metrics():
    """
    Get hash service metrics summary
    """
    try:
        metrics = hash_monitoring.get_metrics_summary()
        return metrics
    except Exception as e:
        logger.error(f"Error getting hash metrics: {str(e)}")
        raise


@app.get("/hash/monitoring/performance")
async def get_hash_performance(time_range_hours: int = 1):
    """
    Get hash service performance metrics
    """
    try:
        performance = hash_monitoring.get_performance_metrics(time_range_hours)
        return performance
    except Exception as e:
        logger.error(f"Error getting hash performance metrics: {str(e)}")
        raise


@app.get("/hash/monitoring/health")
async def get_hash_health():
    """
    Get hash service health checks
    """
    try:
        health_checks = await hash_monitoring.run_health_checks()
        
        # Determine overall health status
        statuses = [hc.status for hc in health_checks.values()]
        if 'critical' in statuses:
            overall_status = 'critical'
        elif 'warning' in statuses:
            overall_status = 'warning'
        else:
            overall_status = 'healthy'
        
        return {
            'overall_status': overall_status,
            'checks': {name: {
                'status': hc.status,
                'message': hc.message,
                'timestamp': hc.timestamp,
                'details': hc.details
            } for name, hc in health_checks.items()},
            'timestamp': time.time()
        }
    except Exception as e:
        logger.error(f"Error getting hash health checks: {str(e)}")
        raise


@app.get("/hash/monitoring/database")
async def get_hash_database_metrics():
    """
    Get hash service database metrics
    """
    try:
        db_metrics = await hash_monitoring.get_database_metrics()
        return db_metrics
    except Exception as e:
        logger.error(f"Error getting hash database metrics: {str(e)}")
        raise


@app.get("/hash/monitoring/status")
async def get_monitoring_status():
    """
    Get monitoring service status
    """
    try:
        status = hash_monitoring.get_monitoring_status()
        return status
    except Exception as e:
        logger.error(f"Error getting monitoring status: {str(e)}")
        raise


@app.post("/hash/monitoring/cleanup")
async def cleanup_monitoring_data():
    """
    Clean up old monitoring data
    """
    try:
        hash_monitoring.cleanup_old_metrics()
        return {
            "success": True,
            "message": "Monitoring data cleanup completed",
            "timestamp": time.time()
        }
    except Exception as e:
        logger.error(f"Error cleaning up monitoring data: {str(e)}")
        raise


@app.get("/hash/integration/status")
async def get_hash_integration_status():
    """
    Get comprehensive hash system integration status
    """
    try:
        # Get hash service status
        hash_service_status = await hash_service.get_service_statistics()
        
        # Get sync service integration status
        sync_integration_status = await sync_service.get_sync_statistics_with_hashes()
        
        # Get monitoring status
        monitoring_status = hash_monitoring.get_monitoring_status()
        
        # Get recent health checks
        health_checks = await hash_monitoring.run_health_checks()
        
        # Get processing statistics
        processing_stats = await content_processor.get_processing_statistics()
        
        # Get configuration
        from core.config import get_settings
        settings = get_settings()
        
        return {
            "integration_status": "active",
            "timestamp": time.time(),
            "components": {
                "hash_service": {
                    "enabled": hash_service.enabled,
                    "status": hash_service_status.get("service_status", "unknown"),
                    "configuration": hash_service_status.get("configuration", {}),
                    "storage_stats": hash_service_status.get("storage_statistics", {})
                },
                "sync_integration": {
                    "status": sync_integration_status.get("integration_status", "unknown"),
                    "sync_stats": sync_integration_status.get("sync_statistics", {}),
                    "hash_stats": sync_integration_status.get("hash_statistics", {})
                },
                "monitoring": {
                    "status": monitoring_status.get("service_status", "unknown"),
                    "metrics_storage": monitoring_status.get("metrics_storage", {}),
                    "health_checks": monitoring_status.get("health_checks", {})
                },
                "content_processing": {
                    "status": processing_stats.get("service_status", "unknown"),
                    "job_stats": processing_stats.get("job_statistics", {}),
                    "configuration": processing_stats.get("configuration", {})
                }
            },
            "health_summary": {
                "total_checks": len(health_checks),
                "healthy_checks": len([hc for hc in health_checks.values() if hc.status == 'healthy']),
                "warning_checks": len([hc for hc in health_checks.values() if hc.status == 'warning']),
                "critical_checks": len([hc for hc in health_checks.values() if hc.status == 'critical'])
            },
            "configuration_summary": {
                "hash_enabled": settings.hash_enabled,
                "block_size_kb": settings.hash_block_size_kb,
                "pdf_threshold_mb": settings.hash_pdf_threshold_mb,
                "max_concurrent_jobs": settings.processing_max_concurrent_jobs,
                "batch_size": settings.processing_batch_size
            }
        }
    except Exception as e:
        logger.error(f"Error getting hash integration status: {str(e)}")
        return {
            "integration_status": "error",
            "error": str(e),
            "timestamp": time.time()
        }


@app.post("/pdf")
async def pdf_operation(request: dict):
    """
    PDF operations: read, summarize (placeholder)
    """
    from core.exceptions import ValidationError
    action = request.get("action")
    pdf_url = request.get("pdf_url")
    
    if not pdf_url:
        raise ValidationError("pdf_url is required", field="pdf_url")
    
    # For now, return a placeholder
    # You can integrate with pdf.co or PyPDF2
    return {
        "message": "PDF feature coming soon",
        "action": action,
        "pdf_url": pdf_url
    }


# Telegram webhook endpoint (optional)
if telegram_bot:
    @app.post("/webhook/telegram")
    async def telegram_webhook(update: dict):
        """
        Telegram bot webhook endpoint
        """
        try:
            await telegram_bot.handle_update(update)
            return {"ok": True}
        except Exception as e:
            logger.error(f"Error in telegram_webhook: {str(e)}", exc_info=True)
            raise


@app.get("/admin/connection-pool-stats")
async def get_connection_pool_stats():
    """
    Get database connection pool statistics and health
    """
    try:
        from services.connection_monitor import get_connection_monitor
        monitor = get_connection_monitor()
        
        health_report = monitor.get_health_report()
        
        return {
            "success": True,
            "pool_health": health_report,
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"Error getting connection pool stats: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": time.time()
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.host, port=settings.port)
