const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

// Request timeout in milliseconds
const REQUEST_TIMEOUT = 120000; // 120 seconds (2 minutes) for complex database queries

// Maximum number of retries
const MAX_RETRIES = 3;

// Retry delay in milliseconds
const RETRY_DELAY = 1000; // 1 second

export interface ChatRequest {
  message: string;
  user_id?: string;
  sheet_id?: string;
  doc_id?: string;
}

export interface ChatResponse {
  response: string;
  type: string;
}

export interface ApiError {
  error: string;
  message: string;
  details?: Record<string, unknown>;
}

/**
 * Sleep utility for retry delays
 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Create a timeout promise that rejects after the specified time
 */
function createTimeout(timeout: number): Promise<never> {
  return new Promise((_, reject) => {
    setTimeout(() => reject(new Error('Request timeout')), timeout);
  });
}

/**
 * Parse error response from API
 */
async function parseErrorResponse(response: Response): Promise<ApiError> {
  try {
    const data = await response.json();
    return {
      error: data.error || 'UNKNOWN_ERROR',
      message: data.message || 'An unknown error occurred',
      details: data.details,
    };
  } catch {
    return {
      error: 'HTTP_ERROR',
      message: `HTTP ${response.status}: ${response.statusText}`,
    };
  }
}

/**
 * Make a fetch request with timeout, retry logic, and error handling
 */
async function fetchWithRetry(
  url: string,
  options: RequestInit,
  retries = MAX_RETRIES
): Promise<Response> {
  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      // Create abort controller for timeout
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);

      // Merge abort signal with existing signal
      const signal = options.signal
        ? (() => {
            const combinedController = new AbortController();
            const originalSignal = options.signal!;
            
            originalSignal.addEventListener('abort', () => {
              combinedController.abort();
            });
            
            combinedController.signal.addEventListener('abort', () => {
              controller.abort();
            });
            
            return combinedController.signal;
          })()
        : controller.signal;

      const response = await Promise.race([
        fetch(url, { ...options, signal }),
        createTimeout(REQUEST_TIMEOUT),
      ]);

      clearTimeout(timeoutId);

      // Don't retry on 4xx errors (client errors)
      if (response.status >= 400 && response.status < 500) {
        return response;
      }

      // Retry on 5xx errors or network errors
      if (!response.ok && attempt < retries) {
        const delay = RETRY_DELAY * Math.pow(2, attempt); // Exponential backoff
        console.warn(
          `Request failed (attempt ${attempt + 1}/${retries + 1}), retrying in ${delay}ms...`
        );
        await sleep(delay);
        continue;
      }

      return response;
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));

      // Don't retry on abort (timeout or user cancellation)
      if (lastError.name === 'AbortError') {
        throw new Error('Request timeout');
      }

      // Retry on network errors
      if (attempt < retries) {
        const delay = RETRY_DELAY * Math.pow(2, attempt);
        console.warn(
          `Network error (attempt ${attempt + 1}/${retries + 1}), retrying in ${delay}ms...`,
          lastError
        );
        await sleep(delay);
        continue;
      }
    }
  }

  throw lastError || new Error('Request failed after retries');
}

/**
 * Send a chat message to the API
 */
export async function sendChatMessage(
  message: string,
  user_id: string = 'default'
): Promise<ChatResponse> {
  if (!message || !message.trim()) {
    throw new Error('Message cannot be empty');
  }

  try {
    const response = await fetchWithRetry(`${API_URL}/chat`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        message: message.trim(),
        user_id,
      }),
    });

    if (!response.ok) {
      const error = await parseErrorResponse(response);
      throw new Error(error.message || `HTTP error! status: ${response.status}`);
    }

    const data: ChatResponse = await response.json();
    return data;
  } catch (error) {
    if (error instanceof Error) {
      // Re-throw with more context
      if (error.message === 'Request timeout') {
        throw new Error('Request timed out. Please check your connection and try again.');
      }
      throw error;
    }
    throw new Error('Failed to send message. Please try again.');
  }
}

/**
 * Check if the API is available
 */
export async function checkApiHealth(): Promise<boolean> {
  try {
    const response = await fetch(`${API_URL}/health`, {
      method: 'GET',
      signal: AbortSignal.timeout(5000), // 5 second timeout for health check
    });
    return response.ok;
  } catch {
    return false;
  }
}

// Task Management API
export interface Task {
  id: number;
  user_id: string;
  title: string;
  description?: string;
  status: 'pending' | 'in_progress' | 'completed' | 'overdue' | 'cancelled';
  priority: 'low' | 'medium' | 'high' | 'urgent';
  due_date?: string;
  completed_at?: string;
  created_at: string;
  updated_at: string;
}

export interface TaskCreateRequest {
  title: string;
  description?: string;
  due_date?: string;
  priority?: 'low' | 'medium' | 'high' | 'urgent';
}

export interface TaskUpdateRequest {
  title?: string;
  description?: string;
  status?: 'pending' | 'in_progress' | 'completed' | 'overdue' | 'cancelled';
  priority?: 'low' | 'medium' | 'high' | 'urgent';
  due_date?: string;
}

export async function createTask(task: TaskCreateRequest, user_id: string = 'default'): Promise<Task> {
  const response = await fetchWithRetry(`${API_URL}/tasks`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(task),
  });

  if (!response.ok) {
    const error = await parseErrorResponse(response);
    throw new Error(error.message);
  }

  return response.json();
}

export async function listTasks(
  user_id: string = 'default',
  status?: string,
  priority?: string
): Promise<Task[]> {
  const params = new URLSearchParams({ user_id });
  if (status) params.append('status', status);
  if (priority) params.append('priority', priority);

  const response = await fetchWithRetry(`${API_URL}/tasks?${params}`, {
    method: 'GET',
  });

  if (!response.ok) {
    const error = await parseErrorResponse(response);
    throw new Error(error.message);
  }

  const data = await response.json();
  return data.tasks || [];
}

export async function getTask(task_id: number, user_id: string = 'default'): Promise<Task> {
  const response = await fetchWithRetry(`${API_URL}/tasks/${task_id}?user_id=${user_id}`, {
    method: 'GET',
  });

  if (!response.ok) {
    const error = await parseErrorResponse(response);
    throw new Error(error.message);
  }

  return response.json();
}

export async function updateTask(
  task_id: number,
  task: TaskUpdateRequest,
  user_id: string = 'default'
): Promise<Task> {
  const response = await fetchWithRetry(`${API_URL}/tasks/${task_id}?user_id=${user_id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(task),
  });

  if (!response.ok) {
    const error = await parseErrorResponse(response);
    throw new Error(error.message);
  }

  return response.json();
}

export async function deleteTask(task_id: number, user_id: string = 'default'): Promise<void> {
  const response = await fetchWithRetry(`${API_URL}/tasks/${task_id}?user_id=${user_id}`, {
    method: 'DELETE',
  });

  if (!response.ok) {
    const error = await parseErrorResponse(response);
    throw new Error(error.message);
  }
}

export async function getUpcomingTasks(user_id: string = 'default', days: number = 7): Promise<Task[]> {
  const response = await fetchWithRetry(`${API_URL}/tasks/upcoming?user_id=${user_id}&days=${days}`, {
    method: 'GET',
  });

  if (!response.ok) {
    const error = await parseErrorResponse(response);
    throw new Error(error.message);
  }

  const data = await response.json();
  return data.tasks || [];
}

// Export API
export interface ExportToSheetRequest {
  data: any;
  sheet_name: string;
  tab_name?: string;
  sheet_id?: string;
  append?: boolean;
}

export interface ExportToDocRequest {
  content: string;
  doc_name: string;
  doc_id?: string;
  append?: boolean;
}

export interface ExportChatRequest {
  conversation_id: string;
  format: 'sheet' | 'doc';
  name?: string;
}

export async function exportToSheet(request: ExportToSheetRequest): Promise<any> {
  const response = await fetchWithRetry(`${API_URL}/export/sheet`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(request),
  });

  if (!response.ok) {
    const error = await parseErrorResponse(response);
    throw new Error(error.message);
  }

  return response.json();
}

export async function exportToDoc(request: ExportToDocRequest): Promise<any> {
  const response = await fetchWithRetry(`${API_URL}/export/doc`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(request),
  });

  if (!response.ok) {
    const error = await parseErrorResponse(response);
    throw new Error(error.message);
  }

  return response.json();
}

export async function exportChat(request: ExportChatRequest): Promise<any> {
  const response = await fetchWithRetry(`${API_URL}/export/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(request),
  });

  if (!response.ok) {
    const error = await parseErrorResponse(response);
    throw new Error(error.message);
  }

  return response.json();
}

// Google Docs API
export interface Doc {
  id: string;
  name: string;
  createdTime?: string;
  modifiedTime?: string;
}

export interface DocResponse {
  success?: boolean;
  docs?: Doc[];
  content?: string;
  summary?: string;
  doc?: Doc;
}

/**
 * List all Google Docs
 */
export async function listDocs(): Promise<Doc[]> {
  const response = await fetchWithRetry(`${API_URL}/docs`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action: 'list' }),
  });

  if (!response.ok) {
    const error = await parseErrorResponse(response);
    throw new Error(error.message);
  }

  const data: DocResponse = await response.json();
  return data.docs || [];
}

/**
 * Read a Google Doc by ID
 */
export async function readDoc(docId: string): Promise<string> {
  const response = await fetchWithRetry(`${API_URL}/docs`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action: 'read', doc_id: docId }),
  });

  if (!response.ok) {
    const error = await parseErrorResponse(response);
    throw new Error(error.message);
  }

  const data: DocResponse = await response.json();
  return data.content || '';
}

/**
 * Create a new Google Doc
 */
export async function createDoc(docName: string): Promise<Doc> {
  const response = await fetchWithRetry(`${API_URL}/docs`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action: 'create', doc_name: docName }),
  });

  if (!response.ok) {
    const error = await parseErrorResponse(response);
    throw new Error(error.message);
  }

  const data: DocResponse = await response.json();
  if (!data.doc) {
    throw new Error('Failed to create document');
  }
  return data.doc;
}

/**
 * Summarize a Google Doc
 */
export async function summarizeDoc(docId: string): Promise<string> {
  const response = await fetchWithRetry(`${API_URL}/docs`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action: 'summarize', doc_id: docId }),
  });

  if (!response.ok) {
    const error = await parseErrorResponse(response);
    throw new Error(error.message);
  }

  const data: DocResponse = await response.json();
  return data.summary || '';
}

// Sync API
export interface SyncResponse {
  message: string;
  force: boolean;
}

export interface SyncStatus {
  sheets: Array<{
    sheet_id: string;
    sheet_name: string;
    last_synced: string;
    sync_status: string;
    modified_time?: string;
  }>;
  docs: Array<{
    doc_id: string;
    doc_name: string;
    last_synced: string;
    sync_status: string;
    modified_time?: string;
  }>;
}

/**
 * Trigger a sync of all sheets only
 * @param force - If true, forces a re-sync even if data is up-to-date
 */
export async function syncSheets(force: boolean = true): Promise<SyncResponse> {
  const response = await fetchWithRetry(`${API_URL}/sync/sheets`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ force }),
  });

  if (!response.ok) {
    const error = await parseErrorResponse(response);
    throw new Error(error.message);
  }

  return response.json();
}

/**
 * Trigger a full sync of all sheets and docs
 * @param force - If true, forces a re-sync even if data is up-to-date
 */
export async function syncAll(force: boolean = true): Promise<SyncResponse> {
  const response = await fetchWithRetry(`${API_URL}/sync/all`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ force }),
  });

  if (!response.ok) {
    const error = await parseErrorResponse(response);
    throw new Error(error.message);
  }

  return response.json();
}

/**
 * Get sync status for all sheets and docs
 */
export async function getSyncStatus(): Promise<SyncStatus> {
  const response = await fetchWithRetry(`${API_URL}/sync/status`, {
    method: 'GET',
  });

  if (!response.ok) {
    const error = await parseErrorResponse(response);
    throw new Error(error.message);
  }

  return response.json();
}

// Natural Language Query API
export interface QueryRequest {
  query: string;
  sheet_id?: string;
}

export interface QueryResponse {
  success: boolean;
  query: string;
  answer: string;
  query_type: string;
  confidence: number;
  data_found: number;
  supporting_data: Array<{
    tab_name: string;
    row_index: number;
    data: any[];
    synced_at?: string;
  }>;
  raw_data?: any;  // Store raw data for on-demand export
  suggestions: string[];
  error?: string;
  sheet_id: string;
}

export interface ExportQueryResultsRequest {
  query: string;
  raw_data: any;
  formatted_response: string;
}

export interface ExportQueryResultsResponse {
  success: boolean;
  sheet_url?: string;
  tab_name?: string;
  rows_exported?: number;
  message?: string;
  error?: string;
}

/**
 * Process a natural language query about sheet data
 * @param query - Natural language query like "what is the amount on December 12th?"
 * @param sheet_id - Optional sheet ID (uses default if not provided)
 */
export async function processNaturalLanguageQuery(
  query: string,
  sheet_id?: string
): Promise<QueryResponse> {
  if (!query || !query.trim()) {
    throw new Error('Query cannot be empty');
  }

  try {
    const response = await fetchWithRetry(`${API_URL}/api/query`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query: query.trim(),
        sheet_id,
      }),
    });

    if (!response.ok) {
      const error = await parseErrorResponse(response);
      throw new Error(error.message || `HTTP error! status: ${response.status}`);
    }

    const data: QueryResponse = await response.json();
    return data;
  } catch (error) {
    if (error instanceof Error) {
      // Re-throw with more context
      if (error.message === 'Request timeout') {
        throw new Error('Query timed out. Please check your connection and try again.');
      }
      throw error;
    }
    throw new Error('Failed to process query. Please try again.');
  }
}

/**
 * Get sheet data for display
 */
export async function getSheetData(tab_name?: string, limit: number = 100): Promise<any> {
  const params = new URLSearchParams();
  if (tab_name) params.append('tab_name', tab_name);
  if (limit) params.append('limit', limit.toString());

  const response = await fetchWithRetry(`${API_URL}/api/sheet-data?${params}`, {
    method: 'GET',
  });

  if (!response.ok) {
    const error = await parseErrorResponse(response);
    throw new Error(error.message);
  }

  return response.json();
}

/**
 * Search sheet data by criteria
 */
export async function searchSheetData(searchCriteria: {
  search_terms?: string[];
  tab_names?: string[];
  limit?: number;
}): Promise<any> {
  const response = await fetchWithRetry(`${API_URL}/api/search-data`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(searchCriteria),
  });

  if (!response.ok) {
    const error = await parseErrorResponse(response);
    throw new Error(error.message);
  }

  return response.json();
}

/**
 * Get tabs summary
 */
export async function getTabsSummary(): Promise<any> {
  const response = await fetchWithRetry(`${API_URL}/api/tabs-summary`, {
    method: 'GET',
  });

  if (!response.ok) {
    const error = await parseErrorResponse(response);
    throw new Error(error.message);
  }

  return response.json();
}

/**
 * Export query results to Google Sheets on demand
 */
export async function exportQueryResults(
  request: ExportQueryResultsRequest
): Promise<ExportQueryResultsResponse> {
  if (!request.query || !request.raw_data) {
    throw new Error('Query and raw_data are required');
  }

  try {
    const response = await fetchWithRetry(`${API_URL}/api/export-query-results`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(request),
    });

    if (!response.ok) {
      const error = await parseErrorResponse(response);
      throw new Error(error.message || `HTTP error! status: ${response.status}`);
    }

    const data: ExportQueryResultsResponse = await response.json();
    return data;
  } catch (error) {
    if (error instanceof Error) {
      if (error.message === 'Request timeout') {
        throw new Error('Request timed out. Please check your connection and try again.');
      }
      throw error;
    }
    throw new Error('Failed to export query results. Please try again.');
  }
}

// ETP Tank Capacity API
export interface ETPTankCapacityRequest {
  date: string;
  etp_inlet_tank_value: number;
  sheet_id?: string;
}

export interface ETPTankData {
  actual_capacity: number;
  storage: number;
  balance: number;
}

export interface ETPTankCapacityResponse {
  success: boolean;
  date: string;
  tanks: {
    [key: string]: ETPTankData;
  };
  totals: {
    total_capacity: number;
    total_storage: number;
    total_balance: number;
  };
  error?: string;
  message?: string;
}

/**
 * Get ETP Tank Capacity and Storage Details
 */
export async function getETPTankCapacity(
  request: ETPTankCapacityRequest
): Promise<ETPTankCapacityResponse> {
  if (!request.date || !request.etp_inlet_tank_value) {
    throw new Error('Date and ETP Inlet tank value are required');
  }

  try {
    const response = await fetchWithRetry(`${API_URL}/api/etp-tank-capacity`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(request),
    });

    if (!response.ok) {
      const error = await parseErrorResponse(response);
      throw new Error(error.message || `HTTP error! status: ${response.status}`);
    }

    const data: ETPTankCapacityResponse = await response.json();
    return data;
  } catch (error) {
    if (error instanceof Error) {
      if (error.message === 'Request timeout') {
        throw new Error('Request timed out. Please check your connection and try again.');
      }
      throw error;
    }
    throw new Error('Failed to get ETP tank capacity. Please try again.');
  }
}