"""
AI Service - Multi-provider AI with RAG (Retrieval Augmented Generation)
Supports: Gemini (recommended), OpenAI, Together AI
Handles AI chat with context from sheets/docs, document summarization, and data analysis
"""

import os
import json
import logging
import asyncio
import hashlib
from typing import Optional, Dict, List, Tuple, Any
from datetime import datetime, date, timedelta

# Conditional imports based on provider
try:
    from openai import OpenAI
    from openai import APIError, RateLimitError, APIConnectionError, APITimeoutError
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

from services.db_queries import (
    find_relevant_sheets, find_relevant_docs, get_sheet_data,
    get_doc_content, get_user_context, get_recent_chat_history,
    get_sheet_tabs, get_or_create_conversation_id, get_conversation_context,
    update_conversation_context, get_conversation_history, get_all_sheets,
    get_tab_metadata, search_sheet_data_by_date, search_sheet_data_by_date_range
)
from services.cache import get_cache_service
from core.exceptions import ExternalAPIError, ValidationError, ServiceError
from core.config import get_settings
import re

logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds


class AIService:
    """Service for interacting with AI APIs (Gemini, OpenAI, Together AI)"""
    
    def __init__(self, sheets_service=None, docs_service=None, sync_service=None):
        """Initialize AI client based on configured provider"""
        try:
            settings = get_settings()
            self.provider = settings.ai_provider.lower()
            self.cache = get_cache_service()
            self.cache_ttl = settings.cache_ai_ttl
            # Store service references for CRUD operations (will be set from main.py)
            self._sheets_service = sheets_service
            self._docs_service = docs_service
            self._sync_service = sync_service
            
            # Initialize based on provider
            if self.provider == 'gemini':
                if not GEMINI_AVAILABLE:
                    raise ServiceError(
                        "google-generativeai package not installed. Run: pip install google-generativeai",
                        service_name="AIService"
                    )
                if not settings.gemini_api_key:
                    raise ServiceError(
                        "GEMINI_API_KEY not found. Set GEMINI_API_KEY environment variable.",
                        service_name="AIService"
                    )
                genai.configure(api_key=settings.gemini_api_key)
                self.model_name = settings.gemini_model
                # Try to initialize with the model, with fallback options
                self.client = self._initialize_gemini_model(self.model_name)
                logger.info(f"Initialized Gemini AI service with model: {self.model_name}")
                
            elif self.provider == 'openai':
                if not OPENAI_AVAILABLE:
                    raise ServiceError(
                        "openai package not installed. Run: pip install openai",
                        service_name="AIService"
                    )
                if not settings.openai_api_key:
                    raise ServiceError(
                        "OPENAI_API_KEY not found. Set OPENAI_API_KEY environment variable.",
                        service_name="AIService"
                    )
                self.client = OpenAI(api_key=settings.openai_api_key)
                self.model_name = settings.openai_model
                logger.info(f"Initialized OpenAI service with model: {self.model_name}")
                
            elif self.provider == 'together':
                if not OPENAI_AVAILABLE:
                    raise ServiceError(
                        "openai package not installed. Run: pip install openai",
                        service_name="AIService"
                    )
                if not settings.together_api_key:
                    raise ServiceError(
                        "TOGETHER_API_KEY not found. Set TOGETHER_API_KEY environment variable.",
                        service_name="AIService"
                    )
                self.client = OpenAI(api_key=settings.together_api_key, base_url="https://api.together.xyz/v1")
                self.model_name = settings.together_model
                logger.info(f"Initialized Together AI service with model: {self.model_name}")
            else:
                # Auto-detect provider
                if settings.gemini_api_key:
                    self.provider = 'gemini'
                    genai.configure(api_key=settings.gemini_api_key)
                    self.model_name = settings.gemini_model
                    self.client = self._initialize_gemini_model(self.model_name)
                elif settings.openai_api_key:
                    self.provider = 'openai'
                    self.client = OpenAI(api_key=settings.openai_api_key)
                    self.model_name = settings.openai_model
                elif settings.together_api_key:
                    self.provider = 'together'
                    self.client = OpenAI(api_key=settings.together_api_key, base_url="https://api.together.xyz/v1")
                    self.model_name = settings.together_model
                else:
                    raise ServiceError(
                        "No AI API key found. Set GEMINI_API_KEY, OPENAI_API_KEY, or TOGETHER_API_KEY.",
                        service_name="AIService"
                    )
                logger.info(f"Auto-detected provider: {self.provider} with model: {self.model_name}")
                
        except ServiceError:
            raise
        except Exception as e:
            logger.error(f"Failed to initialize AIService: {str(e)}")
            raise ServiceError(
                f"Failed to initialize AI service: {str(e)}",
                service_name="AIService"
            )
    
    def _initialize_gemini_model(self, model_name: str):
        """Initialize Gemini model - validation happens on first API call"""
        # Just create the model - validation will happen on first use
        return genai.GenerativeModel(model_name)
    
    def _get_available_gemini_models(self):
        """Get list of available Gemini models"""
        try:
            models = genai.list_models()
            available = [m.name for m in models if 'generateContent' in m.supported_generation_methods]
            # Also try to get just the model name without 'models/' prefix
            simple_names = [m.replace('models/', '') for m in available]
            return available, simple_names
        except Exception as e:
            logger.warning(f"Could not list Gemini models: {str(e)}")
            return [], []
    
    def _try_alternative_gemini_models(self, original_error: Exception):
        """Try to find a working Gemini model"""
        # List of model names to try in order
        fallback_models = [
            "gemini-1.5-flash-latest",
            "models/gemini-1.5-flash-latest",
            "gemini-1.5-pro-latest",
            "models/gemini-1.5-pro-latest",
            "gemini-1.5-flash",
            "models/gemini-1.5-flash",
            "gemini-1.5-pro",
            "models/gemini-1.5-pro",
            "gemini-pro",
            "models/gemini-pro",
        ]
        
        # Try to get available models from API
        available_models, simple_names = self._get_available_gemini_models()
        if available_models:
            # Prepend available models to the fallback list
            fallback_models = available_models + [m for m in fallback_models if m not in available_models]
        
        for model_to_try in fallback_models:
            try:
                logger.info(f"Trying alternative Gemini model: {model_to_try}")
                client = genai.GenerativeModel(model_to_try)
                # Test with a minimal request
                test_response = client.generate_content("Hi")
                if test_response and test_response.text:
                    logger.info(f"Successfully initialized with model: {model_to_try}")
                    self.model_name = model_to_try
                    self.client = client
                    return True
            except Exception as e:
                logger.debug(f"Model '{model_to_try}' failed: {str(e)}")
                continue
        
        return False
    
    def _make_chat_request(self, messages: List[Dict[str, str]], system_prompt: Optional[str] = None) -> str:
        """Make a chat request based on the configured provider"""
        if self.provider == 'gemini':
            return self._make_gemini_request(messages, system_prompt)
        else:
            return self._make_openai_request(messages, system_prompt)
    
    def _make_gemini_request(self, messages: List[Dict[str, str]], system_prompt: Optional[str] = None) -> str:
        """Make a request to Gemini API"""
        try:
            # Build prompt for Gemini
            prompt_parts = []
            
            if system_prompt:
                prompt_parts.append(system_prompt)
            
            # Convert messages to Gemini format
            for msg in messages:
                role = msg.get('role', 'user')
                content = msg.get('content', '')
                
                if role == 'system':
                    if system_prompt is None:
                        prompt_parts.append(content)
                elif role == 'user':
                    prompt_parts.append(f"User: {content}")
                elif role == 'assistant':
                    prompt_parts.append(f"Assistant: {content}")
            
            # Combine all parts
            full_prompt = "\n\n".join(prompt_parts)
            
            # Generate response with enhanced settings for ChatGPT-level responses
            try:
                # Try with generation config (newer Gemini API)
                from google.generativeai.types import GenerationConfig
                generation_config = GenerationConfig(
                    temperature=0.7,  # Balanced creativity and accuracy
                    top_p=0.9,  # Nucleus sampling for better quality
                    top_k=40,  # Consider top 40 tokens
                    max_output_tokens=4096,  # Increased for detailed responses
                )
                response = self.client.generate_content(full_prompt, generation_config=generation_config)
            except (ImportError, TypeError, AttributeError):
                # Fallback for older API versions
                response = self.client.generate_content(full_prompt)
            
            if response and response.text:
                return response.text
            else:
                raise ServiceError("Empty response from Gemini API", service_name="AIService")
                
        except Exception as e:
            error_str = str(e)
            logger.error(f"Gemini API error: {error_str}")
            
            # Check if it's a model not found error (404)
            if "404" in error_str or "not found" in error_str.lower() or "not supported" in error_str.lower():
                logger.warning(f"Model '{self.model_name}' not found, trying alternative models...")
                # Try to find a working model
                if self._try_alternative_gemini_models(e):
                    # Retry the request with the new model
                    try:
                        prompt_parts = []
                        if system_prompt:
                            prompt_parts.append(system_prompt)
                        for msg in messages:
                            role = msg.get('role', 'user')
                            content = msg.get('content', '')
                            if role == 'system':
                                if system_prompt is None:
                                    prompt_parts.append(content)
                            elif role == 'user':
                                prompt_parts.append(f"User: {content}")
                            elif role == 'assistant':
                                prompt_parts.append(f"Assistant: {content}")
                        full_prompt = "\n\n".join(prompt_parts)
                        response = self.client.generate_content(full_prompt)
                        if response and response.text:
                            return response.text
                    except Exception as retry_error:
                        logger.error(f"Retry with alternative model also failed: {str(retry_error)}")
                
                # If we couldn't find a working model, provide helpful error
                available_models, simple_names = self._get_available_gemini_models()
                if available_models:
                    raise ExternalAPIError(
                        f"Gemini model '{self.model_name}' not found. Available models: {', '.join(simple_names[:5])}. "
                        f"Please update GEMINI_MODEL in your .env file.",
                        api_name="Gemini",
                        status_code=404,
                        details={'error': error_str, 'available_models': simple_names}
                    )
                else:
                    raise ExternalAPIError(
                        f"Gemini model '{self.model_name}' not found and could not list available models. "
                        f"Please check your GEMINI_API_KEY is valid. Error: {error_str}",
                        api_name="Gemini",
                        status_code=404,
                        details={'error': error_str}
                    )
            
            raise ExternalAPIError(
                f"Gemini API error: {error_str}",
                api_name="Gemini",
                status_code=500,
                details={'error': error_str}
            )
    
    def _make_openai_request(self, messages: List[Dict[str, str]], system_prompt: Optional[str] = None) -> str:
        """Make a request to OpenAI/Together API"""
        try:
            # Prepare messages
            api_messages = []
            if system_prompt:
                api_messages.append({"role": "system", "content": system_prompt})
            
            for msg in messages:
                role = msg.get('role', 'user')
                if role != 'system':  # Skip system messages if we already added one
                    api_messages.append({"role": role, "content": msg.get('content', '')})
            
            # Make request with enhanced settings for ChatGPT-level responses
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=api_messages,
                temperature=0.7,  # Balanced creativity and accuracy
                max_tokens=4000,  # Increased for detailed, thorough responses
                top_p=0.9,  # Nucleus sampling for better quality
                frequency_penalty=0.1,  # Reduce repetition
                presence_penalty=0.1  # Encourage diverse topics
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"OpenAI/Together API error: {str(e)}")
            raise ExternalAPIError(
                f"AI API error: {str(e)}",
                api_name="OpenAI" if self.provider == 'openai' else "Together AI",
                status_code=500,
                details={'error': str(e)}
            )
    
    async def _retry_ai_request(self, func, *args, **kwargs):
        """Retry an AI API request with exponential backoff"""
        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                # For Gemini, we don't need async, but for OpenAI we do
                if self.provider == 'gemini':
                    return func(*args, **kwargs)
                else:
                    return await asyncio.to_thread(func, *args, **kwargs)
            except Exception as e:
                # Handle rate limits for OpenAI/Together
                if OPENAI_AVAILABLE and isinstance(e, (RateLimitError, APIConnectionError, APITimeoutError)):
                    last_error = e
                    if attempt < MAX_RETRIES - 1:
                        delay = RETRY_DELAY * (2 ** attempt)
                        api_name = "OpenAI" if self.provider == 'openai' else "Together AI"
                        logger.warning(
                            f"{api_name} error (attempt {attempt + 1}/{MAX_RETRIES}), "
                            f"retrying in {delay}s: {str(e)}"
                        )
                        await asyncio.sleep(delay)
                        continue
                    else:
                        api_name = "OpenAI" if self.provider == 'openai' else "Together AI"
                        raise ExternalAPIError(
                            f"{api_name} API error after {MAX_RETRIES} attempts: {str(e)}",
                            api_name=api_name,
                            status_code=429 if isinstance(e, RateLimitError) else 503,
                            details={'error': str(e), 'attempts': MAX_RETRIES}
                        )
                elif OPENAI_AVAILABLE and isinstance(e, APIError):
                    # Don't retry on API errors (4xx)
                    api_name = "OpenAI" if self.provider == 'openai' else "Together AI"
                    raise ExternalAPIError(
                        f"{api_name} API error: {str(e)}",
                        api_name=api_name,
                        status_code=e.status_code if hasattr(e, 'status_code') else 400,
                        details={'error': str(e)}
                    )
                else:
                    # Generic error (including Gemini)
                    last_error = e
                    if attempt < MAX_RETRIES - 1:
                        delay = RETRY_DELAY * (2 ** attempt)
                        api_name = "Gemini" if self.provider == 'gemini' else "AI"
                        logger.warning(
                            f"{api_name} error (attempt {attempt + 1}/{MAX_RETRIES}), "
                            f"retrying in {delay}s: {str(e)}"
                        )
                        await asyncio.sleep(delay)
                        continue
                    else:
                        api_name = "Gemini" if self.provider == 'gemini' else "AI"
                        raise ExternalAPIError(
                            f"{api_name} API error after {MAX_RETRIES} attempts: {str(e)}",
                            api_name=api_name,
                            status_code=500,
                            details={'error': str(e), 'attempts': MAX_RETRIES}
                        )
            except Exception as e:
                last_error = e
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAY * (2 ** attempt)
                    logger.warning(
                        f"Unexpected error in AI API request (attempt {attempt + 1}/{MAX_RETRIES}), "
                        f"retrying in {delay}s: {str(e)}"
                    )
                    await asyncio.sleep(delay)
                else:
                    raise ServiceError(
                        f"Unexpected error in AI API after {MAX_RETRIES} attempts: {str(e)}",
                        service_name="AIService",
                        details={'error': str(e), 'attempts': MAX_RETRIES}
                    )
        
        # Should never reach here
        raise ServiceError(
            f"Failed to complete AI API request: {str(last_error)}",
            service_name="AIService"
        )
    
    def _detect_query_type(self, message: str) -> Tuple[str, Dict]:
        """
        Detect if query needs data (sheets/docs), is CRUD operation, or is general chat
        Returns: (query_type, context_hints)
        query_type: 'data_query', 'crud_operation', 'general_chat', 'command'
        """
        message_lower = message.lower()
        
        # Check for commands
        if message.startswith('/'):
            return 'command', {}
        
        # CRUD operation keywords (high priority - check before data_query)
        crud_keywords = {
            'create': ['create', 'make', 'new', 'add a new', 'add'],
            'update': ['update', 'change', 'modify', 'edit', 'set', 'put', 'write'],
            'delete': ['delete', 'remove', 'clear', 'erase'],
            'insert': ['insert', 'add row', 'add rows', 'append']
        }
        
        # Check for CRUD intent
        for operation, keywords in crud_keywords.items():
            if any(keyword in message_lower for keyword in keywords):
                # Additional check: must mention sheet/doc/tab
                if any(word in message_lower for word in ['sheet', 'spreadsheet', 'doc', 'document', 'cell', 'row', 'column', 'tab', 'worksheet']):
                    return 'crud_operation', {'operation': operation}
        
        # Keywords that suggest data query
        data_keywords = [
            'sheet', 'spreadsheet', 'data', 'row', 'column', 'cell',
            'document', 'doc', 'reading', 'value', 'today', 'yesterday',
            'this sheet', 'that sheet', 'my sheet', 'the sheet',
            'what is', 'show me', 'find', 'search', 'filter',
            'total', 'sum', 'average', 'count', 'analyze', 'aggregate',
            'november', 'december', 'january', 'february', 'march', 'april',
            'may', 'june', 'july', 'august', 'september', 'october'
        ]
        
        # Check for aggregation queries
        aggregation_keywords = ['total', 'sum', 'average', 'avg', 'count', 'aggregate', 'calculate']
        has_aggregation = any(keyword in message_lower for keyword in aggregation_keywords)
        
        # Check if message contains data-related keywords
        has_data_keywords = any(keyword in message_lower for keyword in data_keywords)
        
        # Check for date references
        date_keywords = ['today', 'yesterday', 'this week', 'last week', 'this month', 'last month']
        has_date = any(keyword in message_lower for keyword in date_keywords)
        
        if has_data_keywords or has_date:
            return 'data_query', {'has_date': has_date}
        
        return 'general_chat', {}
    
    def _retrieve_relevant_context(self, query: str, user_id: str, conversation_id: Optional[str] = None) -> Dict:
        """
        Retrieve relevant context from database for RAG
        Returns: dict with sheets_data, docs_data, user_context, conversation_context
        
        Args:
            query: User query
            user_id: User identifier
            conversation_id: Optional conversation ID for context
        
        Raises:
            ServiceError: If context retrieval fails
        """
        context = {
            'sheets': [],
            'docs': [],
            'user_context': {},
            'conversation_context': {},
            'relevant_data': []
        }
        
        try:
            # Get or create conversation ID
            if not conversation_id:
                conversation_id = get_or_create_conversation_id(user_id)
            
            # Get conversation context
            conv_context = get_conversation_context(conversation_id, user_id)
            context['conversation_context'] = conv_context
            context['conversation_id'] = conversation_id
            
            # Get user context (last used sheets/docs)
            user_ctx = get_user_context(user_id)
            context['user_context'] = user_ctx
            
            # Check if we need to trigger a sync (if last sync was more than 2 minutes ago)
            # This ensures fresh data for date-specific queries
            from services.database import SheetsMetadata, get_db_context
            from datetime import datetime, timedelta
            with get_db_context() as db:
                sheets_meta = db.query(SheetsMetadata).filter(
                    SheetsMetadata.sync_status == 'completed'
                ).all()
                
                if sheets_meta:
                    # Check if any sheet hasn't been synced in the last 2 minutes
                    needs_sync = False
                    for sheet_meta in sheets_meta:
                        if sheet_meta.last_synced:
                            time_since_sync = datetime.utcnow() - sheet_meta.last_synced.replace(tzinfo=None)
                            if time_since_sync > timedelta(minutes=2):
                                needs_sync = True
                                break
                        else:
                            needs_sync = True
                            break
                    
                    # If query mentions dates (yesterday, today, specific dates), always sync
                    query_lower = query.lower()
                    date_keywords = ['yesterday', 'today', '27/11', '27.11', 'november 27', 'nov 27']
                    if any(keyword in query_lower for keyword in date_keywords):
                        needs_sync = True
                    
                    if needs_sync and self._sync_service:
                        logger.info(f"🔄 Triggering on-demand sync for fresh data (query: {query[:50]}...)")
                        # Trigger sync in background (non-blocking)
                        import asyncio
                        try:
                            # Create a task to sync without blocking
                            loop = asyncio.get_event_loop()
                            if loop.is_running():
                                # If loop is running, schedule as task
                                asyncio.create_task(self._sync_service.sync_all(force=False))
                            else:
                                # If no loop, run sync
                                asyncio.run(self._sync_service.sync_all(force=False))
                        except Exception as e:
                            logger.warning(f"Could not trigger on-demand sync: {str(e)}")
            
            # Use active sheet/doc from conversation context if available
            active_sheet_id = conv_context.get('active_sheet_id') or user_ctx.get('last_sheet_id')
            active_doc_id = conv_context.get('active_doc_id') or user_ctx.get('last_doc_id')
            
            # Prioritize active sheet/doc from conversation context
            if active_sheet_id:
                try:
                    tabs = get_sheet_tabs(active_sheet_id)
                    if tabs:
                        # STEP 1: Collect metadata for ALL tabs (tab names + headers)
                        all_tabs_metadata = []
                        query_keywords = query.lower().split()
                        relevant_tabs = []
                        
                        for tab in tabs:
                            try:
                                # Get headers for this tab
                                first_row_result = get_sheet_data(active_sheet_id, tab, limit=1)
                                headers = []
                                if first_row_result and len(first_row_result) > 0:
                                    # first_row_result[0] is a dict with 'data', 'row_index', 'tab_name'
                                    headers = first_row_result[0].get('data', [])
                                
                                # Get date range metadata
                                tab_meta = get_tab_metadata(active_sheet_id, tab)
                                
                                tab_metadata = {
                                    'tab_name': tab,
                                    'headers': headers,
                                    'row_count': tab_meta.get('row_count', 0),
                                    'date_range': tab_meta.get('date_range', {})
                                }
                                all_tabs_metadata.append(tab_metadata)
                                
                                # Check if this tab is relevant to the query
                                tab_lower = tab.lower()
                                headers_str = ' '.join([str(cell).lower() for cell in headers])
                                
                                # Score relevance
                                relevance_score = 0
                                for keyword in query_keywords:
                                    if keyword in tab_lower:
                                        relevance_score += 3  # Tab name match is very relevant
                                    if keyword in headers_str:
                                        relevance_score += 2  # Header match is relevant
                                
                                if relevance_score > 0:
                                    relevant_tabs.append((tab, relevance_score))
                            except Exception as e:
                                logger.warning(f"Error getting metadata for tab {tab}: {str(e)}")
                        
                        # Sort tabs by relevance
                        relevant_tabs.sort(key=lambda x: x[1], reverse=True)
                        
                        # STEP 2: Load detailed data from the most relevant tabs (up to 3)
                        tabs_with_data = []
                        if relevant_tabs:
                            # Load top 3 most relevant tabs
                            for tab_name, score in relevant_tabs[:3]:
                                try:
                                    # For date queries, increase limit to get more data
                                    query_lower = query.lower()
                                    has_date_query = any(word in query_lower for word in ['date', 'yesterday', 'today', '27', '28', 'november', 'nov'])
                                    limit = 500 if has_date_query else 100  # More rows for date queries
                                    
                                    sheet_data = get_sheet_data(active_sheet_id, tab_name, limit=limit)
                                    if sheet_data:
                                        tabs_with_data.append({
                                            'tab_name': tab_name,
                                            'data': sheet_data[:200] if has_date_query else sheet_data[:50],  # More rows for date queries
                                            'is_primary': tab_name == relevant_tabs[0][0]
                                        })
                                except Exception as e:
                                    logger.warning(f"Error loading data for tab {tab_name}: {str(e)}")
                        else:
                            # No relevant tabs found, just load first tab
                            try:
                                sheet_data = get_sheet_data(active_sheet_id, tabs[0], limit=100)
                                if sheet_data:
                                    tabs_with_data.append({
                                        'tab_name': tabs[0],
                                        'data': sheet_data[:50],
                                        'is_primary': True
                                    })
                            except Exception as e:
                                logger.warning(f"Error loading data for first tab: {str(e)}")
                        
                        # Add to context with ALL tab metadata
                        context['sheets'].append({
                            'sheet_id': active_sheet_id,
                            'sheet_name': user_ctx.get('last_sheet_name', ''),
                            'all_tabs': all_tabs_metadata,  # Metadata for ALL tabs
                            'tabs_with_data': tabs_with_data,  # Full data for relevant tabs
                            'is_active': True
                        })
                except Exception as e:
                    logger.warning(f"Error retrieving active sheet: {str(e)}")
            
            # Check if user wants to list all sheets
            query_lower = query.lower()
            wants_all_sheets = any(phrase in query_lower for phrase in [
                'list all', 'show all', 'all my sheets', 'all sheets', 
                'list my sheets', 'show my sheets', 'what sheets', 'which sheets',
                'list sheets', 'show sheets', 'list sheet', 'show sheet',
                'what sheet', 'which sheet', 'sheet data', 'sheets data',
                'do you have access to', 'what do you have', 'tell me the sheet',
                'tell me what sheet', 'what are the sheet', 'sheet names'
            ])
            
            # Find relevant sheets
            try:
                if wants_all_sheets:
                    # Get all sheets for listing queries (don't fetch data, just metadata)
                    relevant_sheets = get_all_sheets(limit=50)
                    # For listing queries, just add metadata without data
                    for sheet in relevant_sheets:
                        if sheet['sheet_id'] != active_sheet_id:  # Skip if already added as active
                            context['sheets'].append({
                                'sheet_id': sheet['sheet_id'],
                                'sheet_name': sheet['sheet_name'],
                                'tab_name': None,
                                'data': [],  # No data for listing queries
                                'is_active': False,
                                'modified_time': sheet.get('modified_time')
                            })
                else:
                    relevant_sheets = find_relevant_sheets(query, limit=3)
                    
                    # Get data from relevant sheets (skip if already added as active)
                    for sheet in relevant_sheets:
                        sheet_id = sheet['sheet_id']
                        if sheet_id == active_sheet_id:
                            continue  # Already added
                        
                        try:
                            tabs = get_sheet_tabs(sheet_id)
                            
                            if tabs:
                                # STEP 1: Collect metadata for ALL tabs
                                all_tabs_metadata = []
                                query_keywords = query_lower.split()
                                relevant_tabs = []
                                
                                for tab in tabs:
                                    try:
                                        first_row_result = get_sheet_data(sheet_id, tab, limit=1)
                                        headers = []
                                        if first_row_result and len(first_row_result) > 0:
                                            headers = first_row_result[0].get('data', [])
                                        
                                        tab_metadata = {
                                            'tab_name': tab,
                                            'headers': headers
                                        }
                                        all_tabs_metadata.append(tab_metadata)
                                        
                                        # Check relevance
                                        tab_lower = tab.lower()
                                        headers_str = ' '.join([str(cell).lower() for cell in headers])
                                        
                                        relevance_score = 0
                                        for keyword in query_keywords:
                                            if keyword in tab_lower:
                                                relevance_score += 3
                                            if keyword in headers_str:
                                                relevance_score += 2
                                        
                                        if relevance_score > 0:
                                            relevant_tabs.append((tab, relevance_score))
                                    except Exception as e:
                                        logger.warning(f"Error getting metadata for tab {tab}: {str(e)}")
                                
                                # Sort by relevance
                                relevant_tabs.sort(key=lambda x: x[1], reverse=True)
                                
                                # STEP 2: Load data from most relevant tabs
                                tabs_with_data = []
                                if relevant_tabs:
                                    for tab_name, score in relevant_tabs[:3]:
                                        try:
                                            sheet_data = get_sheet_data(sheet_id, tab_name, limit=100)
                                            if sheet_data:
                                                tabs_with_data.append({
                                                    'tab_name': tab_name,
                                                    'data': sheet_data[:50],
                                                    'is_primary': tab_name == relevant_tabs[0][0]
                                                })
                                        except Exception as e:
                                            logger.warning(f"Error loading data for tab {tab_name}: {str(e)}")
                                else:
                                    # Load first tab if no relevant tabs
                                    try:
                                        sheet_data = get_sheet_data(sheet_id, tabs[0], limit=100)
                                        if sheet_data:
                                            tabs_with_data.append({
                                                'tab_name': tabs[0],
                                                'data': sheet_data[:50],
                                                'is_primary': True
                                            })
                                    except Exception as e:
                                        logger.warning(f"Error loading first tab data: {str(e)}")
                                
                                # Add to context
                                context['sheets'].append({
                                    'sheet_id': sheet_id,
                                    'sheet_name': sheet['sheet_name'],
                                    'all_tabs': all_tabs_metadata,
                                    'tabs_with_data': tabs_with_data
                                })
                        except Exception as e:
                            logger.warning(f"Error retrieving data for sheet {sheet.get('sheet_id')}: {str(e)}")
                            continue
            except Exception as e:
                logger.warning(f"Error finding relevant sheets: {str(e)}")
            
            # Check if user mentioned "this sheet" - use active or last used sheet
            if 'this sheet' in query.lower() or 'the sheet' in query.lower():
                target_sheet_id = active_sheet_id or user_ctx.get('last_sheet_id')
                if target_sheet_id:
                    try:
                        tab_name = user_ctx.get('last_tab_name', 'Sheet1')
                        sheet_data = get_sheet_data(target_sheet_id, tab_name, limit=100)
                        
                        if sheet_data:
                            # Insert at beginning if not already there
                            if not any(s.get('sheet_id') == target_sheet_id for s in context['sheets']):
                                context['sheets'].insert(0, {
                                    'sheet_id': target_sheet_id,
                                    'sheet_name': user_ctx.get('last_sheet_name', ''),
                                    'tab_name': tab_name,
                                    'data': sheet_data[:50],
                                    'is_active': True
                                })
                    except Exception as e:
                        logger.warning(f"Error retrieving last used sheet: {str(e)}")
            
            # Prioritize active doc from conversation context
            if active_doc_id:
                try:
                    content = get_doc_content(active_doc_id)
                    if content:
                        context['docs'].append({
                            'doc_id': active_doc_id,
                            'doc_name': user_ctx.get('last_doc_name', ''),
                            'content': content[:2000],
                            'is_active': True
                        })
                except Exception as e:
                    logger.warning(f"Error retrieving active doc: {str(e)}")
            
            # Find relevant docs
            try:
                relevant_docs = find_relevant_docs(query, limit=2)
                for doc in relevant_docs:
                    doc_id = doc['doc_id']
                    if doc_id == active_doc_id:
                        continue  # Already added
                    
                    try:
                        content = get_doc_content(doc_id)
                        if content:
                            context['docs'].append({
                                'doc_id': doc_id,
                                'doc_name': doc['doc_name'],
                                'content': content[:2000]  # Limit content length
                            })
                    except Exception as e:
                        logger.warning(f"Error retrieving content for doc {doc.get('doc_id')}: {str(e)}")
                        continue
            except Exception as e:
                logger.warning(f"Error finding relevant docs: {str(e)}")
            
            # Get conversation history if available, otherwise recent history
            try:
                if conversation_id:
                    conv_history = get_conversation_history(conversation_id, limit=10)
                    context['chat_history'] = conv_history
                else:
                    recent_history = get_recent_chat_history(user_id, limit=5)
                    context['chat_history'] = recent_history
            except Exception as e:
                logger.warning(f"Error retrieving chat history: {str(e)}")
                context['chat_history'] = []
        
        except Exception as e:
            logger.error(f"Error retrieving context: {str(e)}")
            # Don't fail completely, return partial context
        
        return context
    
    def _build_context_string(self, context: Dict, query: str) -> str:
        """Build context string for AI prompt"""
        context_parts = []
        
        # Check if this is a listing query
        query_lower = query.lower()
        is_listing_query = any(phrase in query_lower for phrase in [
            'list all', 'show all', 'all my sheets', 'all sheets', 
            'list my sheets', 'show my sheets', 'what sheets', 'which sheets',
            'list sheets', 'show sheets', 'list sheet', 'show sheet',
            'what sheet', 'which sheet', 'sheet data', 'sheets data',
            'do you have access to', 'what do you have', 'tell me the sheet',
            'tell me what sheet', 'what are the sheet', 'sheet names',
            'list all docs', 'show all docs', 'all my docs'
        ])
        
        # Add sheets data
        if context.get('sheets'):
            if is_listing_query:
                # For listing queries, just show sheet names
                if len(context['sheets']) > 0:
                    context_parts.append("## User's Google Sheets (CRITICAL: USE ONLY THESE EXACT NAMES - DO NOT INVENT, MODIFY, OR ADD ANY NAMES):\n")
                    for sheet in context['sheets']:
                        sheet_name = sheet.get('sheet_name', 'Unknown')
                        sheet_id = sheet.get('sheet_id', '')
                        modified = sheet.get('modified_time', '')
                        context_parts.append(f"- {sheet_name} (ID: {sheet_id})")
                        if modified:
                            context_parts.append(f"  Last modified: {modified}")
                    context_parts.append("")
                else:
                    context_parts.append("## User's Google Sheets:\n")
                    context_parts.append("NO SHEETS FOUND IN DATABASE. The database is empty. User needs to sync their Google Sheets first. DO NOT INVENT OR MAKE UP SHEET NAMES.\n")
                    context_parts.append("")
            else:
                # For data queries, show sheet data with multi-tab support
                context_parts.append("## Available Sheet Data:\n")
                for sheet in context['sheets']:
                    sheet_name = sheet.get('sheet_name', 'Unknown')
                    context_parts.append(f"### Sheet: {sheet_name}\n")
                    
                    # Show ALL available tabs (metadata)
                    if sheet.get('all_tabs'):
                        context_parts.append(f"📋 This sheet has {len(sheet['all_tabs'])} tabs:")
                        for tab_meta in sheet['all_tabs']:
                            tab_name = tab_meta.get('tab_name', 'Unknown')
                            headers = tab_meta.get('headers', [])
                            row_count = tab_meta.get('row_count', 0)
                            date_range = tab_meta.get('date_range', {})
                            
                            context_parts.append(f"  - Tab: '{tab_name}' ({row_count} rows)")
                            
                            # Show date range if available
                            min_date = date_range.get('min_date')
                            max_date = date_range.get('max_date')
                            if min_date and max_date:
                                context_parts.append(f"    📅 Data available from: {min_date} to {max_date}")
                            elif min_date:
                                context_parts.append(f"    📅 Data from: {min_date}")
                            
                            if headers:
                                # Show first 10 headers
                                header_preview = headers[:10]
                                context_parts.append(f"    Columns: {', '.join([str(h) for h in header_preview if h])}")
                        context_parts.append("")
                    
                    # Show detailed data from relevant tabs
                    if sheet.get('tabs_with_data'):
                        context_parts.append("📊 Detailed data from relevant tabs:\n")
                        for tab_info in sheet['tabs_with_data']:
                            tab_name = tab_info.get('tab_name', 'Unknown')
                            data = tab_info.get('data', [])
                            is_primary = tab_info.get('is_primary', False)
                            
                            primary_marker = " [PRIMARY - Most relevant to query]" if is_primary else ""
                            context_parts.append(f"  Tab: '{tab_name}'{primary_marker}")
                            
                            if data and len(data) > 0:
                                # First row as headers
                                headers = data[0].get('data', []) if isinstance(data[0], dict) else data[0]
                                if headers:
                                    context_parts.append(f"  Headers: {' | '.join([str(h) for h in headers])}")
                                
                                # Sample data rows (skip header row)
                                sample_rows = data[1:min(21, len(data))]  # Up to 20 data rows
                                for row_item in sample_rows:
                                    if isinstance(row_item, dict):
                                        row_data = row_item.get('data', [])
                                        row_idx = row_item.get('row_index', 0)
                                    else:
                                        row_data = row_item
                                        row_idx = sample_rows.index(row_item) + 1
                                    
                                    if row_data:
                                        context_parts.append(f"  Row {row_idx}: {' | '.join([str(cell) for cell in row_data])}")
                            else:
                                context_parts.append("  (No data rows)")
                            context_parts.append("")
                    # Legacy format support (for backward compatibility)
                    elif sheet.get('data'):
                        tab_name = sheet.get('tab_name', 'N/A')
                        context_parts.append(f"Tab: {tab_name}")
                        context_parts.append(f"Data (first {len(sheet['data'])} rows):")
                        
                        headers = sheet['data'][0].get('data', [])
                        if headers:
                            context_parts.append("Headers: " + " | ".join(str(h) for h in headers))
                        
                        for row in sheet['data'][:20]:
                            row_data = row.get('data', [])
                            if row_data:
                                context_parts.append("Row " + str(row.get('row_index', 0)) + ": " + " | ".join(str(cell) for cell in row_data))
                    else:
                        context_parts.append("(Sheet metadata only - no data rows)")
                    
                    context_parts.append("")
        
        # Add docs data
        if context.get('docs'):
            context_parts.append("## Available Document Content:\n")
            for doc in context['docs']:
                context_parts.append(f"Document: {doc['doc_name']}")
                context_parts.append(f"Content: {doc['content'][:1000]}...")  # Truncate
                context_parts.append("")
        
        # Add user context hints - CRITICAL for conversation continuity
        if context.get('user_context'):
            uc = context['user_context']
            if uc.get('last_sheet_name'):
                context_parts.append(f"\n⚠️ IMPORTANT CONTEXT: User's active/last used sheet is '{uc['last_sheet_name']}' (ID: {uc.get('last_sheet_id', 'N/A')})")
                context_parts.append("When user says 'this sheet', 'that sheet', 'the sheet', or mentions a sheet name, they likely mean this sheet.")
            if uc.get('last_doc_name'):
                context_parts.append(f"\n⚠️ IMPORTANT CONTEXT: User's active/last used document is '{uc['last_doc_name']}' (ID: {uc.get('last_doc_id', 'N/A')})")
        
        # Add conversation context
        if context.get('conversation_context'):
            conv_ctx = context['conversation_context']
            if conv_ctx.get('active_sheet_id'):
                context_parts.append(f"\n⚠️ CONVERSATION CONTEXT: Active sheet in this conversation is ID: {conv_ctx['active_sheet_id']}")
            if conv_ctx.get('active_doc_id'):
                context_parts.append(f"\n⚠️ CONVERSATION CONTEXT: Active document in this conversation is ID: {conv_ctx['active_doc_id']}")
        
        return "\n".join(context_parts)
    
    def _extract_date_filters(self, query: str) -> Optional[Dict]:
        """Extract date filters from query - supports specific dates, months, and date ranges"""
        query_lower = query.lower()
        today = date.today()
        
        filters = {}
        
        # First, try to extract specific dates (e.g., 27.11.2025, 27/11/2025)
        import re
        date_patterns = [
            r'\b(\d{1,2}[./-]\d{1,2}[./-]\d{4})\b',  # 27.11.2025, 27/11/2025
            r'\b(\d{1,2}[./-]\d{1,2}[./-]\d{2})\b',  # 27.11.25
        ]
        for pattern in date_patterns:
            matches = re.findall(pattern, query)
            if matches:
                # Use the first date found
                filters['date'] = matches[0]
                filters['date_formats'] = [
                    matches[0],
                    matches[0].replace('/', '.'),
                    matches[0].replace('.', '/'),
                    matches[0].replace('-', '.'),
                ]
                return filters
        
        # Check for month names (e.g., "november", "in november")
        month_map = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4,
            'may': 5, 'june': 6, 'july': 7, 'august': 8,
            'september': 9, 'october': 10, 'november': 11, 'december': 12
        }
        
        for month_name, month_num in month_map.items():
            if month_name in query_lower:
                filters['month'] = month_num
                # Try to extract year (default to current year)
                year_match = re.search(r'\b(20\d{2})\b', query)
                filters['year'] = int(year_match.group(1)) if year_match else today.year
                filters['date_range'] = True
                return filters
        
        # If no specific date, check for relative dates
        if 'today' in query_lower:
            filters['date'] = today.isoformat()
        elif 'yesterday' in query_lower:
            filters['date'] = (today - timedelta(days=1)).isoformat()
        elif 'this week' in query_lower:
            # Start of week (Monday)
            days_since_monday = today.weekday()
            week_start = today - timedelta(days=days_since_monday)
            filters['date'] = week_start.isoformat()
        elif 'last week' in query_lower:
            days_since_monday = today.weekday()
            last_week_start = today - timedelta(days=days_since_monday + 7)
            filters['date'] = last_week_start.isoformat()
        
        return filters if filters else None
    
    async def chat(self, message: str, user_id: str = "default", 
                  sheet_id: Optional[str] = None, doc_id: Optional[str] = None,
                  conversation_id: Optional[str] = None) -> str:
        """
        Main chat method with RAG - intelligently routes queries
        
        Args:
            message: User's message
            user_id: User identifier
            sheet_id: Optional sheet ID for context
            doc_id: Optional document ID for context
            conversation_id: Optional conversation ID for context continuity
        
        Returns:
            AI assistant's response
        
        Raises:
            ValidationError: If message is invalid
            ExternalAPIError: If AI API call fails
            ServiceError: If service operation fails
        """
        if not message or not isinstance(message, str):
            raise ValidationError("message is required and must be a string", field="message")
        
        if not message.strip():
            raise ValidationError("message cannot be empty", field="message")
        
        try:
            # Get or create conversation ID
            if not conversation_id:
                conversation_id = get_or_create_conversation_id(user_id)
            
            # Detect query type
            query_type, hints = self._detect_query_type(message)
            
            # Handle commands (don't cache commands as they may have side effects)
            if query_type == 'command':
                return await self._handle_command(message, user_id)
            
            # Handle CRUD operations
            if query_type == 'crud_operation':
                return await self._handle_crud_operation(message, user_id, hints.get('operation'))
            
            # For general chat without context, try cache first
            # Note: We don't cache data queries as they depend on current data state
            if query_type == 'general_chat' and not sheet_id and not doc_id:
                cache_key = self.cache._generate_key(
                    'ai_chat',
                    message=message.strip().lower(),
                    user_id=user_id,
                    provider=self.provider,
                    model=self.model_name
                )
                cached_response = self.cache.get(cache_key)
                if cached_response is not None:
                    logger.info(f"Cache hit for AI chat: {message[:50]}...")
                    return cached_response
            
            # Retrieve relevant context for data queries
            context = {}
            if query_type == 'data_query' or query_type == 'crud_operation':
                context = self._retrieve_relevant_context(message, user_id, conversation_id)
                
                # Extract sheet name from message and update conversation context
                message_lower = message.lower()
                # Check if message mentions a sheet name
                for sheet in context.get('sheets', []):
                    sheet_name = sheet.get('sheet_name', '').lower()
                    if sheet_name and sheet_name in message_lower:
                        update_conversation_context(
                            conversation_id,
                            user_id,
                            active_sheet_id=sheet.get('sheet_id'),
                            active_doc_id=doc_id or context.get('conversation_context', {}).get('active_doc_id')
                        )
                        break
                
                # Update conversation context with active sheet/doc
                if sheet_id or doc_id:
                    update_conversation_context(
                        conversation_id,
                        user_id,
                        active_sheet_id=sheet_id or context.get('conversation_context', {}).get('active_sheet_id'),
                        active_doc_id=doc_id or context.get('conversation_context', {}).get('active_doc_id')
                    )
                
                # Apply date filters if needed - CRITICAL: Search full dataset for specific dates/date ranges
                date_filters = self._extract_date_filters(message)
                if date_filters and context.get('sheets'):
                    # Check if it's a date range query (month/year)
                    is_date_range = date_filters.get('date_range', False)
                    has_specific_date = any(char.isdigit() for char in date_filters.get('date', ''))
                    
                    if is_date_range:
                        # Date range query (e.g., "November 2025")
                        month = date_filters.get('month')
                        year = date_filters.get('year')
                        logger.info(f"🔍 Date range query detected: month={month}, year={year}, searching database directly (unlimited)...")
                        
                        for sheet in context['sheets']:
                            sheet_id = sheet.get('sheet_id')
                            for tab_info in sheet.get('tabs_with_data', []):
                                tab_name = tab_info.get('tab_name')
                                if sheet_id and tab_name:
                                    # Use database search for date range - searches ALL rows
                                    filtered_data = search_sheet_data_by_date_range(
                                        sheet_id, tab_name, month=month, year=year
                                    )
                                    
                                    if filtered_data:
                                        tab_info['data'] = filtered_data
                                        logger.info(f"   ✅ Found {len(filtered_data)} rows for {month}/{year}")
                                    else:
                                        logger.warning(f"   ⚠️  No rows found for {month}/{year} in {tab_name}")
                    
                    elif has_specific_date:
                        # Specific date query (e.g., "27.11.2025")
                        logger.info(f"🔍 Specific date query detected: {date_filters.get('date')}, searching database directly (unlimited)...")
                        for sheet in context['sheets']:
                            sheet_id = sheet.get('sheet_id')
                            for tab_info in sheet.get('tabs_with_data', []):
                                tab_name = tab_info.get('tab_name')
                                if sheet_id and tab_name:
                                    date_value = date_filters.get('date', '')
                                    logger.info(f"   Searching in {tab_name} for date {date_value} (unlimited search)...")
                                    
                                    # Database-level search - searches ALL rows
                                    filtered_data = search_sheet_data_by_date(sheet_id, tab_name, date_value)
                                    
                                    if filtered_data:
                                        tab_info['data'] = filtered_data
                                        logger.info(f"   ✅ Found {len(filtered_data)} rows matching date {date_value}")
                                    else:
                                        logger.warning(f"   ⚠️  No rows found for date {date_value} in {tab_name}")
                    
                    else:
                        # Relative date filters (today, yesterday) - filter loaded data
                        for sheet in context['sheets']:
                            for tab_info in sheet.get('tabs_with_data', []):
                                if tab_info.get('data'):
                                    filtered_data = []
                                    for row in tab_info.get('data', []):
                                        row_str = ' '.join(str(cell) for cell in row.get('data', [])).lower()
                                        if date_filters.get('date') in row_str:
                                            filtered_data.append(row)
                                    if filtered_data:
                                        tab_info['data'] = filtered_data
            
            # Build system prompt - Enhanced for ChatGPT-level capabilities
            system_prompt = """You are RAVVYN, an advanced AI assistant with ChatGPT-level intelligence and reasoning capabilities. You have DIRECT ACCESS to the user's Google Sheets and Google Docs. 
You can communicate in Tamil, English, or mixed (Tanglish).

=== CORE CAPABILITIES ===
You are a highly capable AI assistant with:
- Advanced reasoning and problem-solving abilities
- Step-by-step analytical thinking
- Code analysis, generation, and debugging capabilities
- Deep understanding of complex questions
- Ability to break down complex problems into manageable steps
- Clear, detailed explanations with examples
- Creative problem-solving approaches
- Multi-step reasoning and planning

=== REASONING APPROACH ===
When answering questions:
1. **Think step-by-step**: Break down complex problems into smaller, manageable parts
2. **Show your work**: For calculations or analysis, explain your reasoning process
3. **Verify your answers**: Double-check calculations and logic before responding
4. **Consider edge cases**: Think about exceptions, special cases, and potential issues
5. **Provide context**: Explain not just what, but why and how
6. **Be thorough**: Give complete, detailed answers rather than brief responses

=== CODE & TECHNICAL CAPABILITIES ===
- Analyze code: Review, debug, and explain code in any programming language
- Generate code: Write clean, well-documented, production-ready code
- Explain concepts: Break down technical concepts in simple terms
- Problem-solving: Approach coding problems systematically
- Best practices: Suggest improvements and follow industry standards

=== DATA & SHEETS RULES ===
CRITICAL RULES:
1. ONLY use information provided in the context below. NEVER make up or invent sheet names, data, or information.
2. If the context shows "No sheets found" or an empty list, tell the user they need to sync their sheets first.
3. If asked to list sheets and the context provides a list, use EXACTLY those sheet names - do not add or modify them.

SMART DATA SEARCH RULES:
4. You can see date ranges for each tab (e.g., "Data from 17.08.2025 to 26.11.2025"). Use this to know what data exists.
5. If the user asks for a specific date that is NOT in the loaded data:
   a) Check the date ranges shown for each tab
   b) If the date falls within a tab's range, tell the user: "I have data for that tab from [min] to [max], but need to load that specific date. Asking user to wait..."
   c) If the date is NOT in any range, suggest the nearest available date: "No data for [requested date], but I have data for [nearest date]. Would you like that?"
6. NEVER say "no data" or "I don't have information" without first:
   a) Checking ALL tabs for relevant information
   b) Checking date ranges to see if data exists but wasn't loaded
   c) Suggesting alternative dates or tabs that might help
7. If asked a follow-up question (like "tell me all temperatures of that day"), refer back to the data you just showed - don't lose context.
8. If no context is provided for sheets/docs, tell the user to run a sync first.

CONVERSATION CONTEXT RULES (CRITICAL):
9. You have access to the PREVIOUS MESSAGES in this conversation above. ALWAYS use them to understand context.
10. When user uses pronouns like "it", "that", "this", "again", "search for it again", etc., refer to the PREVIOUS MESSAGES to understand what they mean.
11. If user says "search for it again" or "could you search for it again", "it" refers to what was discussed in the previous messages - look at the conversation history above.
12. NEVER ask "what do you mean by 'it'?" if the previous conversation makes it clear - use the conversation history to understand.
13. When user mentions a sheet name (e.g., "DAILY REPORT"), remember it for the rest of the conversation.
14. When user says "this sheet", "that sheet", "the sheet", they mean the sheet from the previous conversation or the active sheet.

When answering questions:
- If asked to "list all sheets" or "show my sheets", use ONLY the provided list of sheets in the context
- If the context shows no sheets, say: "I don't see any sheets in your account. Please run a sync first by going to Settings > Sync or use the /sync command."
- If asked about specific data, use ONLY the provided sheet/document data in the context
- If asked about "today" or dates, filter data accordingly
- If asked about "this sheet", use the user's last used sheet
- Be precise with numbers and calculations
- NEVER invent or make up sheet names, data, or information

AGGREGATION AND CALCULATION RULES:
- When user asks for "total", "sum", "average", "count", etc., calculate from the provided data
- For example, if asked "total salt use in kg in november", find all rows for November, identify the salt/kg column, and sum those values
- Look at the column headers to identify which column contains the data you need (e.g., "SALT KG", "SALT", "KG")
- Perform calculations accurately using the actual numeric values from the data
- Show your calculation steps when doing complex aggregations
- If creating a new sheet with filtered data (e.g., "put condensate data for november in a new sheet"), extract the filtered data and create a new sheet with it

=== RESPONSE STYLE ===
- Be conversational, helpful, and engaging
- Provide detailed, thorough answers
- Use examples and analogies when helpful
- Break down complex topics into digestible parts
- Show reasoning and thought process for complex questions
- Be precise and accurate
- Admit when you don't know something (but only use context provided)

=== YOUR CAPABILITIES ===
You help users with:
- Google Sheets operations and data analysis (using ONLY data from context)
- Google Docs management (using ONLY data from context)
- PDF reading and summarization
- Setting reminders
- General questions and conversations
- Code analysis, debugging, and generation
- Problem-solving and analytical thinking
- Explanations of complex topics
- Planning and multi-step reasoning

Respond naturally, helpfully, and in the same language the user uses. Always use ONLY the information provided in the context. Think step-by-step, show your reasoning, and provide thorough, detailed responses."""
            
            # Build messages
            messages = [{"role": "system", "content": system_prompt}]
            
            # CRITICAL: Always include conversation history for context continuity
            # This allows the AI to understand pronouns like "it", "that", "this", "again"
            chat_history = context.get('chat_history', [])
            if chat_history:
                # Add conversation history (oldest first, excluding the current message)
                for chat in chat_history:
                    # Skip if this is the exact same message (avoid duplicates)
                    if chat.get('message') != message:
                        messages.append({"role": "user", "content": chat['message']})
                        messages.append({"role": "assistant", "content": chat['response']})
                logger.debug(f"Added {len(chat_history)} previous messages to context")
            else:
                # Fallback: try to get recent history if not in context
                try:
                    if conversation_id:
                        recent_history = get_conversation_history(conversation_id, limit=5)
                    else:
                        recent_history = get_recent_chat_history(user_id, limit=5)
                    if recent_history:
                        for chat in recent_history:
                            if chat.get('message') != message:
                                messages.append({"role": "user", "content": chat['message']})
                                messages.append({"role": "assistant", "content": chat['response']})
                        logger.debug(f"Added {len(recent_history)} recent messages to context (fallback)")
                except Exception as e:
                    logger.warning(f"Error retrieving chat history: {str(e)}")
            
            # Always build context string (even if empty) so AI knows what data is available
            context_str = self._build_context_string(context, message)
            
            # For data queries, always include context (even if empty)
            if query_type == 'data_query':
                if context_str.strip():
                    messages.append({
                        "role": "user",
                        "content": f"Context:\n{context_str}\n\nUser Question: {message}"
                    })
                else:
                    # No context available - explicitly tell AI
                    messages.append({
                        "role": "user",
                        "content": f"Context: No sheets or docs found in database. User may need to sync first.\n\nUser Question: {message}"
                    })
            else:
                # For general chat, just add the message
                messages.append({"role": "user", "content": message})
            
            # Make API call with retry logic
            def _create_completion():
                return self._make_chat_request(messages, system_prompt)
            
            assistant_message = await self._retry_ai_request(_create_completion)
            
            if not assistant_message:
                raise ServiceError(
                    "AI API returned empty response",
                    service_name="AIService"
                )
            logger.info(f"Successfully generated chat response for user {user_id}, conversation {conversation_id}")
            
            # Cache general chat responses (not data queries or commands)
            if query_type == 'general_chat' and not sheet_id and not doc_id:
                cache_key = self.cache._generate_key(
                    'ai_chat',
                    message=message.strip().lower(),
                    user_id=user_id,
                    provider=self.provider,
                    model=self.model_name
                )
                self.cache.set(cache_key, assistant_message, self.cache_ttl)
                logger.debug(f"Cached AI chat response: {cache_key}")
            
            # Return response with conversation_id for client to use
            return assistant_message
        
        except (ValidationError, ExternalAPIError, ServiceError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error in chat: {str(e)}")
            raise ServiceError(
                f"Failed to process chat message: {str(e)}",
                service_name="AIService",
                details={'user_id': user_id, 'message_length': len(message)}
            )
    
    async def _handle_command(self, message: str, user_id: str) -> str:
        """Handle special commands"""
        if message.startswith('/clone'):
            return await self.handle_clone_command(message, user_id)
        elif message.startswith('/figma'):
            return await self.handle_figma_command(message, user_id)
        elif message.startswith('/page'):
            return await self.handle_page_command(message, user_id)
        elif message.startswith('/improve'):
            return await self.handle_improve_command(message, user_id)
        else:
            return "Unknown command. Available commands: /clone, /figma, /page, /improve"
    
    async def _handle_crud_operation(self, message: str, user_id: str, operation: str) -> str:
        """
        Handle CRUD operations on sheets/docs from natural language
        Efficiently routes to appropriate service methods
        """
        if not self._sheets_service:
            return "CRUD operations require service initialization. Please use the API endpoints directly."
        
        message_lower = message.lower()
        
        try:
            # Determine if it's a sheet or doc operation
            is_sheet = any(word in message_lower for word in ['sheet', 'spreadsheet', 'cell', 'row', 'column'])
            is_doc = any(word in message_lower for word in ['doc', 'document'])
            
            if is_sheet:
                return await self._handle_sheet_crud(message, user_id, operation)
            elif is_doc:
                return await self._handle_doc_crud(message, user_id, operation)
            else:
                return await self._parse_and_execute_crud(message, user_id, operation)
        
        except Exception as e:
            logger.error(f"Error handling CRUD operation: {str(e)}")
            return f"I encountered an error: {str(e)}. Please try rephrasing your request."
    
    async def _handle_sheet_crud(self, message: str, user_id: str, operation: str) -> str:
        """Handle sheet CRUD operations"""
        try:
            if operation == 'create':
                message_lower = message.lower()
                
                # Check if creating a tab
                if any(word in message_lower for word in ['tab', 'worksheet', 'sheet tab']):
                    tab_name = self._extract_tab_name(message)
                    sheet_id, sheet_name = self._get_active_sheet(user_id, message)
                    
                    if not tab_name:
                        return "I couldn't find a tab name. Please specify, e.g., 'create a tab called test' or 'add a tab named test in daily report sheet'"
                    
                    if not sheet_id:
                        return "I couldn't find which sheet to add the tab to. Please specify the sheet name, e.g., 'create a tab called test in daily report sheet'"
                    
                    result = await self._sheets_service.create_tab(sheet_id, tab_name)
                    return f"✅ Created new tab '{tab_name}' in sheet '{sheet_name}'\nSheet ID: {sheet_id}"
                
                # Check if creating a sheet with filtered data (e.g., "put condensate data for november in a new sheet")
                message_lower = message.lower()
                if any(phrase in message_lower for phrase in ['put', 'give', 'create sheet with', 'new sheet with', 'export']):
                    # This is a request to create a sheet with filtered data
                    return await self._create_sheet_with_filtered_data(message, user_id)
                
                # Creating a new empty sheet
                sheet_name = self._extract_sheet_name(message)
                if not sheet_name:
                    return "I couldn't find a sheet name. Please specify, e.g., 'create a sheet called Sales'"
                
                result = await self._sheets_service.create_sheet(sheet_name)
                return f"✅ Created new sheet: '{sheet_name}'\nSheet ID: {result.get('id')}\nURL: {result.get('url', 'N/A')}"
            
            elif operation == 'update':
                params = await self._parse_update_params(message, user_id)
                if not params:
                    return "I couldn't understand the update request. Please specify sheet, cell/range, and value."
                
                if params.get('cell'):
                    result = await self._sheets_service.update_cell(
                        params['sheet_id'], params.get('tab_name', 'Sheet1'),
                        params['row'], params['column'], params['value']
                    )
                    return f"✅ Updated cell {params['cell']} to '{params['value']}' in sheet '{params.get('sheet_name', '')}'"
            
            elif operation == 'delete':
                params = await self._parse_delete_params(message, user_id)
                if not params:
                    return "I couldn't understand the delete request. Please specify what to delete (rows/columns)."
                
                if params.get('rows'):
                    result = await self._sheets_service.delete_rows(
                        params['sheet_id'], params.get('tab_name', 'Sheet1'),
                        params['start_row'], params['end_row']
                    )
                    return f"✅ Deleted rows {params['start_row']+1}-{params['end_row']+1} from sheet '{params.get('sheet_name', '')}'"
                elif params.get('columns'):
                    result = await self._sheets_service.delete_columns(
                        params['sheet_id'], params.get('tab_name', 'Sheet1'),
                        params['start_col'], params['end_col']
                    )
                    return f"✅ Deleted columns from sheet '{params.get('sheet_name', '')}'"
            
            elif operation == 'insert':
                params = await self._parse_insert_params(message, user_id)
                if not params:
                    return "I couldn't understand the insert request. Please specify where to insert rows."
                
                result = await self._sheets_service.insert_rows(
                    params['sheet_id'], params.get('tab_name', 'Sheet1'),
                    params['row_index'], params.get('num_rows', 1)
                )
                return f"✅ Inserted {params.get('num_rows', 1)} row(s) at position {params['row_index']+1} in sheet '{params.get('sheet_name', '')}'"
            
            return "I couldn't determine the operation. Please try again."
        
        except Exception as e:
            logger.error(f"Error in sheet CRUD: {str(e)}")
            return f"Error: {str(e)}"
    
    async def _handle_doc_crud(self, message: str, user_id: str, operation: str) -> str:
        """Handle doc CRUD operations"""
        return "Doc CRUD operations coming soon. Please use the API endpoints for now."
    
    def _extract_sheet_name(self, message: str) -> Optional[str]:
        """Extract sheet name from message"""
        patterns = [
            r'(?:create|make|new).*?sheet.*?(?:called|named|with name|titled)\s+["\']?([^"\']+)["\']?',
            r'(?:create|make|new).*?sheet\s+["\']?([^"\']+)["\']?',
            r'sheet\s+["\']?([^"\']+)["\']?',
        ]
        for pattern in patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return None
    
    def _extract_tab_name(self, message: str) -> Optional[str]:
        """Extract tab name from message"""
        patterns = [
            r'(?:create|make|new|add).*?tab.*?(?:called|named|with name|titled)\s+["\']?([^"\']+)["\']?',
            r'(?:create|make|new|add).*?tab\s+["\']?([^"\']+)["\']?',
            r'tab\s+(?:called|named)\s+["\']?([^"\']+)["\']?',
            r'tab\s+["\']?([^"\']+)["\']?',
        ]
        for pattern in patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return None
    
    def _get_active_sheet(self, user_id: str, message: str) -> Tuple[Optional[str], Optional[str]]:
        """Get active sheet ID and name from context or message"""
        from services.db_queries import get_user_context, find_relevant_sheets
        
        # First, try to get from user context
        user_ctx = get_user_context(user_id)
        if user_ctx.get('last_sheet_id'):
            return user_ctx.get('last_sheet_id'), user_ctx.get('last_sheet_name')
        
        # Try to extract sheet name from message
        message_lower = message.lower()
        patterns = [
            r'(?:in|from|to|at)\s+["\']?([^"\']+?)\s+sheet',
            r'sheet\s+["\']?([^"\']+?)["\']?',
        ]
        for pattern in patterns:
            match = re.search(pattern, message_lower)
            if match:
                sheet_name = match.group(1).strip()
                # Find sheet by name
                sheets = find_relevant_sheets(sheet_name, limit=1)
                if sheets:
                    return sheets[0].get('sheet_id'), sheets[0].get('sheet_name')
        
        return None, None
    
    async def _parse_update_params(self, message: str, user_id: str) -> Optional[Dict]:
        """Parse update parameters from natural language"""
        try:
            user_ctx = get_user_context(user_id)
            active_sheet_id = user_ctx.get('last_sheet_id')
            active_sheet_name = user_ctx.get('last_sheet_name', '')
            
            cell_match = re.search(r'([A-Z]+)(\d+)', message.upper())
            if cell_match:
                col_letter = cell_match.group(1)
                row_num = int(cell_match.group(2))
                col_num = sum((ord(c) - ord('A') + 1) * (26 ** i) for i, c in enumerate(reversed(col_letter)))
                
                value_match = re.search(r'(?:to|as|=|with value)\s+["\']?([^"\']+)["\']?', message, re.IGNORECASE)
                value = value_match.group(1).strip() if value_match else None
                
                if value and active_sheet_id:
                    return {
                        'sheet_id': active_sheet_id, 'sheet_name': active_sheet_name,
                        'cell': cell_match.group(0), 'row': row_num - 1, 'column': col_num - 1, 'value': value
                    }
            return None
        except Exception as e:
            logger.error(f"Error parsing update params: {str(e)}")
            return None
    
    async def _parse_delete_params(self, message: str, user_id: str) -> Optional[Dict]:
        """Parse delete parameters"""
        try:
            user_ctx = get_user_context(user_id)
            active_sheet_id = user_ctx.get('last_sheet_id')
            active_sheet_name = user_ctx.get('last_sheet_name', '')
            
            row_match = re.search(r'row[s]?\s+(\d+)(?:\s*-\s*(\d+))?', message, re.IGNORECASE)
            if row_match and active_sheet_id:
                start_row = int(row_match.group(1)) - 1
                end_row = int(row_match.group(2)) - 1 if row_match.group(2) else start_row
                return {'sheet_id': active_sheet_id, 'sheet_name': active_sheet_name, 'rows': True,
                       'start_row': start_row, 'end_row': end_row}
            
            col_match = re.search(r'column[s]?\s+([A-Z]+)(?:\s*-\s*([A-Z]+))?', message, re.IGNORECASE)
            if col_match and active_sheet_id:
                col1, col2 = col_match.group(1), col_match.group(2) or col_match.group(1)
                start_col = sum((ord(c) - ord('A') + 1) * (26 ** i) for i, c in enumerate(reversed(col1))) - 1
                end_col = sum((ord(c) - ord('A') + 1) * (26 ** i) for i, c in enumerate(reversed(col2))) - 1
                return {'sheet_id': active_sheet_id, 'sheet_name': active_sheet_name, 'columns': True,
                       'start_col': start_col, 'end_col': end_col}
            return None
        except Exception as e:
            logger.error(f"Error parsing delete params: {str(e)}")
            return None
    
    async def _parse_insert_params(self, message: str, user_id: str) -> Optional[Dict]:
        """Parse insert parameters"""
        try:
            user_ctx = get_user_context(user_id)
            active_sheet_id = user_ctx.get('last_sheet_id')
            active_sheet_name = user_ctx.get('last_sheet_name', '')
            
            row_match = re.search(r'row[s]?\s+(?:at|before|after)?\s*(\d+)', message, re.IGNORECASE)
            num_match = re.search(r'(\d+)\s+row[s]?', message, re.IGNORECASE)
            
            if row_match and active_sheet_id:
                row_index = int(row_match.group(1)) - 1
                num_rows = int(num_match.group(1)) if num_match else 1
                return {'sheet_id': active_sheet_id, 'sheet_name': active_sheet_name,
                       'row_index': row_index, 'num_rows': num_rows}
            return None
        except Exception as e:
            logger.error(f"Error parsing insert params: {str(e)}")
            return None
    
    async def _parse_and_execute_crud(self, message: str, user_id: str, operation: str) -> str:
        """Use AI to parse ambiguous CRUD requests"""
        return f"I'm not sure if you want to modify a sheet or document. Please specify, e.g., 'create a sheet called X' or 'update doc Y'."
    
    async def analyze_sheet(self, question: str, data: list, sheet_id: str, tab_name: str) -> str:
        """
        AI-powered sheet analysis (legacy method for backward compatibility)
        
        Args:
            question: Question to analyze
            data: Sheet data as list of rows
            sheet_id: Sheet ID
            tab_name: Tab name
        
        Returns:
            Analysis result as string
        
        Raises:
            ValidationError: If inputs are invalid
            ExternalAPIError: If AI API call fails
        """
        if not question or not isinstance(question, str):
            raise ValidationError("question is required and must be a string", field="question")
        
        if not data or not isinstance(data, list):
            raise ValidationError("data is required and must be a list", field="data")
        
        try:
            # Try cache first (based on question, sheet_id, tab_name, and data hash)
            # Use a hash of the data to detect changes
            data_hash = hashlib.md5(json.dumps(data[:200], sort_keys=True).encode()).hexdigest()[:8]
            cache_key = self.cache._generate_key(
                'ai_analyze_sheet',
                question=question.strip().lower(),
                sheet_id=sheet_id,
                tab_name=tab_name,
                data_hash=data_hash,
                provider=self.provider,
                model=self.model_name
            )
            cached_response = self.cache.get(cache_key)
            if cached_response is not None:
                logger.info(f"Cache hit for sheet analysis: {question[:50]}...")
                return cached_response
            
            data_str = json.dumps(data[:200])
            
            def _analyze():
                return self.client.chat.completions.create(
                    model=self.model_name,
                    messages=[
                        {
                            "role": "system",
                            "content": """You are a data analyst assistant. 
                            You will receive a user's question and rows from a Google Sheet (as JSON). 
                            Analyze the data and answer the question precisely. 
                            If the answer cannot be determined, explain why and suggest what is needed. 
                            Respond concisely."""
                        },
                        {
                            "role": "user",
                            "content": f"""Question: {question}
Sheet ID: {sheet_id}
Tab: {tab_name}
Rows (JSON): {data_str}

Please analyze and answer the question."""
                        }
                    ],
                    temperature=0.2,
                    max_tokens=500
                )
            
            response = await self._retry_ai_request(_analyze)
            
            if not response.choices or not response.choices[0].message.content:
                raise ServiceError("AI API returned empty response", service_name="AIService")
            
            result = response.choices[0].message.content
            
            # Cache the result
            self.cache.set(cache_key, result, self.cache_ttl)
            logger.debug(f"Cached sheet analysis response: {cache_key}")
            
            return result
        
        except (ValidationError, ExternalAPIError, ServiceError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error in analyze_sheet: {str(e)}")
            raise ServiceError(
                f"Failed to analyze sheet: {str(e)}",
                service_name="AIService",
                details={'sheet_id': sheet_id, 'tab_name': tab_name}
            )
    
    async def summarize_document(self, content: str) -> str:
        """
        Summarize a document (Google Docs or PDF)
        
        Args:
            content: Document content to summarize
        
        Returns:
            Summary as string
        
        Raises:
            ValidationError: If content is invalid
            ExternalAPIError: If AI API call fails
        """
        if not content or not isinstance(content, str):
            raise ValidationError("content is required and must be a string", field="content")
        
        try:
            content = content[:4000]  # Limit content length
            
            def _summarize():
                return self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {
                            "role": "system",
                            "content": "You are a helpful assistant. Summarize the following document content concisely."
                        },
                        {
                            "role": "user",
                            "content": f"Summarize this document:\n\n{content}"
                        }
                    ],
                    temperature=0.3,
                    max_tokens=500
                )
            
            response = await self._retry_ai_request(_summarize)
            
            if not response.choices or not response.choices[0].message.content:
                raise ServiceError("AI API returned empty response", service_name="AIService")
            
            return response.choices[0].message.content
        
        except (ValidationError, ExternalAPIError, ServiceError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error in summarize_document: {str(e)}")
            raise ServiceError(
                f"Failed to summarize document: {str(e)}",
                service_name="AIService"
            )
    
    async def handle_clone_command(self, message: str, user_id: str = "default") -> str:
        """Handle /clone command - Generate UI from screenshot"""
        try:
            prompt = message.replace('/clone', '').strip()
            if not prompt:
                return "Please provide a description or upload a screenshot of the UI you want to clone. Example: /clone a modern login page with email and password fields"
            
            def _clone():
                return self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {
                            "role": "system",
                            "content": """You are a UI/UX expert assistant. When users ask to clone a UI, help them by:
                            1. Understanding the design requirements
                            2. Suggesting the best approach (React, HTML/CSS, etc.)
                            3. Providing code structure and components
                            4. Explaining design patterns used
                            
                            Be detailed and practical in your responses."""
                        },
                        {
                            "role": "user",
                            "content": f"Clone UI: {prompt}"
                        }
                    ],
                    temperature=0.7,
                    max_tokens=1500
                )
            
            response = await self._retry_ai_request(_clone)
            return response.choices[0].message.content if response.choices else "No response generated"
        except (ExternalAPIError, ServiceError) as e:
            logger.error(f"Error in handle_clone_command: {str(e)}")
            return f"Sorry, I encountered an error while processing the clone command: {str(e)}"
        except Exception as e:
            logger.error(f"Unexpected error in handle_clone_command: {str(e)}")
            return f"Sorry, I encountered an unexpected error: {str(e)}"
    
    async def handle_figma_command(self, message: str, user_id: str = "default") -> str:
        """Handle /figma command - Import design from Figma"""
        try:
            prompt = message.replace('/figma', '').strip()
            if not prompt:
                return "Please provide a Figma file URL or describe what you want to import. Example: /figma https://figma.com/file/..."
            
            def _figma():
                return self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {
                            "role": "system",
                            "content": """You are a design-to-code expert. When users want to import from Figma, help them by:
                            1. Explaining how to export assets from Figma
                            2. Converting design specs to code
                            3. Suggesting tools and plugins for Figma-to-code conversion
                            4. Providing implementation guidance
                            
                            Be helpful and provide actionable steps."""
                        },
                        {
                            "role": "user",
                            "content": f"Import Figma: {prompt}"
                        }
                    ],
                    temperature=0.7,
                    max_tokens=1500
                )
            
            response = await self._retry_ai_request(_figma)
            return response.choices[0].message.content if response.choices else "No response generated"
        except (ExternalAPIError, ServiceError) as e:
            logger.error(f"Error in handle_figma_command: {str(e)}")
            return f"Sorry, I encountered an error while processing the Figma command: {str(e)}"
        except Exception as e:
            logger.error(f"Unexpected error in handle_figma_command: {str(e)}")
            return f"Sorry, I encountered an unexpected error: {str(e)}"
    
    async def handle_page_command(self, message: str, user_id: str = "default") -> str:
        """Handle /page command - Generate a new web page"""
        try:
            prompt = message.replace('/page', '').strip()
            if not prompt:
                return "Please describe the page you want to create. Example: /page a landing page for a SaaS product with hero section, features, and pricing"
            
            def _page():
                return self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {
                            "role": "system",
                            "content": """You are a web development expert. When users ask to create a page, help them by:
                            1. Understanding the page requirements
                            2. Suggesting the structure and layout
                            3. Providing code examples (React, HTML, CSS)
                            4. Recommending best practices
                            
                            Be comprehensive and provide working code examples."""
                        },
                        {
                            "role": "user",
                            "content": f"Create Page: {prompt}"
                        }
                    ],
                    temperature=0.7,
                    max_tokens=2000
                )
            
            response = await self._retry_ai_request(_page)
            return response.choices[0].message.content if response.choices else "No response generated"
        except (ExternalAPIError, ServiceError) as e:
            logger.error(f"Error in handle_page_command: {str(e)}")
            return f"Sorry, I encountered an error while processing the page command: {str(e)}"
        except Exception as e:
            logger.error(f"Unexpected error in handle_page_command: {str(e)}")
            return f"Sorry, I encountered an unexpected error: {str(e)}"
    
    async def handle_improve_command(self, message: str, user_id: str = "default") -> str:
        """Handle /improve command - Improve existing UI design"""
        try:
            prompt = message.replace('/improve', '').strip()
            if not prompt:
                return "Please describe what UI you want to improve or paste the code. Example: /improve make this form more user-friendly and modern"
            
            def _improve():
                return self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {
                            "role": "system",
                            "content": """You are a UI/UX improvement expert. When users ask to improve UI, help them by:
                            1. Analyzing the current design
                            2. Identifying areas for improvement (UX, accessibility, modern design)
                            3. Providing improved code with explanations
                            4. Suggesting best practices and design patterns
                            
                            Be constructive and provide before/after comparisons when possible."""
                        },
                        {
                            "role": "user",
                            "content": f"Improve UI: {prompt}"
                        }
                    ],
                    temperature=0.7,
                    max_tokens=2000
                )
            
            response = await self._retry_ai_request(_improve)
            return response.choices[0].message.content if response.choices else "No response generated"
        except (ExternalAPIError, ServiceError) as e:
            logger.error(f"Error in handle_improve_command: {str(e)}")
            return f"Sorry, I encountered an error while processing the improve command: {str(e)}"
        except Exception as e:
            logger.error(f"Unexpected error in handle_improve_command: {str(e)}")
            return f"Sorry, I encountered an unexpected error: {str(e)}"


# Import helper function
from services.db_queries import get_sheet_tabs
