# Complete Setup Guide - RAVVYN AI Assistant

## Prerequisites

- Python 3.9+ installed
- Node.js 16+ installed
- Google Cloud Project with APIs enabled
- OpenAI API key

## Step 1: Backend Setup

### 1.1 Install Python Dependencies

```bash
cd web/backend
pip install -r requirements.txt
```

### 1.2 Set Up Environment Variables

1. Copy the example environment file:
```bash
copy env.example.txt .env
```

2. Edit `.env` file and add your credentials:
```env
# OpenAI Configuration (Required)
OPENAI_API_KEY=sk-your-openai-api-key-here
OPENAI_MODEL=gpt-4-turbo-preview

# Google Credentials (Required - choose one)
# Option 1: Service Account (recommended)
GOOGLE_APPLICATION_CREDENTIALS=credentials/service-account.json

# Option 2: OAuth2 JSON
# GOOGLE_CREDENTIALS_JSON={"token": "...", "refresh_token": "..."}

# Database (default SQLite)
DATABASE_URL=sqlite:///./ravvyn.db

# Sync Configuration
SYNC_INTERVAL_MINUTES=15
AUTO_SYNC_ENABLED=true

# Server Configuration
HOST=0.0.0.0
PORT=8000
FRONTEND_URL=http://localhost:3000
```

### 1.3 Set Up Google Credentials

**Option A: Service Account (Recommended)**

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create a project or select existing
3. Enable APIs:
   - Google Sheets API
   - Google Drive API
   - Google Docs API
4. Create Service Account:
   - IAM & Admin → Service Accounts → Create Service Account
   - Download JSON key
5. Save the JSON file as: `web/backend/credentials/service-account.json`

**Option B: OAuth2**

If you already have OAuth2 credentials, add them to `.env`:
```env
GOOGLE_CREDENTIALS_JSON={"token": "your-token", "refresh_token": "your-refresh-token"}
```

### 1.4 Initialize Database

The database will be automatically created when you first run the backend. The schema includes:
- Sheets metadata and data
- Docs metadata and content
- Chat history
- User context

### 1.5 Run Backend

```bash
cd web/backend
python main.py
```

The backend will:
- Initialize the database
- Start the scheduler for automatic sync
- Run on `http://localhost:8000`

You can verify it's working by visiting: `http://localhost:8000/docs`

## Step 2: Frontend Setup

### 2.1 Install Node.js Dependencies

```bash
cd web/frontend
npm install
```

### 2.2 Set Up Environment Variables

1. Create `.env` file:
```bash
copy .env.example .env
```

2. Edit `.env`:
```env
NEXT_PUBLIC_API_URL=http://localhost:8000
```

### 2.3 Run Frontend

```bash
npm run dev
```

The frontend will run on `http://localhost:3000`

## Step 3: Initial Data Sync

### 3.1 Sync All Sheets and Docs

Once the backend is running, trigger the initial sync:

**Option A: Using API (Recommended)**
```bash
# Using curl
curl -X POST http://localhost:8000/sync/all

# Or using PowerShell
Invoke-WebRequest -Uri http://localhost:8000/sync/all -Method POST
```

**Option B: Using FastAPI Docs**
1. Go to `http://localhost:8000/docs`
2. Find `POST /sync/all`
3. Click "Try it out"
4. Click "Execute"

### 3.2 Check Sync Status

```bash
# Check sync status
curl http://localhost:8000/sync/status
```

## Step 4: Test the System

### 4.1 Test Chat with Data

1. Open `http://localhost:3000`
2. Try asking:
   - "What is the reading of today from this sheet?"
   - "Show me data from my sales sheet"
   - "What's in my documents?"

### 4.2 Test General Chat

- "Hello, how are you?"
- "What is Python?"
- "Explain machine learning"

### 4.3 Test Commands

- `/clone a modern login page`
- `/page create a landing page`
- `/improve make this form better`

## Step 5: Verify Everything Works

### 5.1 Health Check

```bash
curl http://localhost:8000/health
```

Should return:
```json
{
  "status": "healthy",
  "checks": {
    "database": "connected",
    "ai_service": "available",
    "sync_service": "available",
    "scheduler": "running"
  }
}
```

### 5.2 Check Database

The database file will be created at: `web/backend/ravvyn.db`

You can inspect it using SQLite:
```bash
sqlite3 web/backend/ravvyn.db
.tables
SELECT COUNT(*) FROM sheets_metadata;
SELECT COUNT(*) FROM docs_metadata;
```

## Troubleshooting

### Backend Issues

**"OPENAI_API_KEY not found"**
- Make sure `.env` file exists in `web/backend/`
- Check that `OPENAI_API_KEY` is set correctly

**"No Google credentials found"**
- Ensure `service-account.json` exists in `credentials/` folder
- Or set `GOOGLE_CREDENTIALS_JSON` in `.env`

**"Database locked"**
- Close any other processes using the database
- Restart the backend

**Scheduler not running**
- Check `AUTO_SYNC_ENABLED=true` in `.env`
- Check logs for scheduler errors

### Frontend Issues

**"Failed to fetch" or CORS errors**
- Make sure backend is running on port 8000
- Check `NEXT_PUBLIC_API_URL` in frontend `.env`
- Verify CORS is enabled in backend (it is by default)

**Build errors**
- Delete `node_modules` and `.next` folders
- Run `npm install` again

### Sync Issues

**Sheets/Docs not syncing**
- Check Google API credentials
- Verify APIs are enabled in Google Cloud Console
- Check sync status: `GET /sync/status`
- Manually trigger sync: `POST /sync/all`

**Sync errors in logs**
- Check Google API quota limits
- Verify service account has proper permissions
- Check network connectivity

## Production Deployment

### Environment Variables for Production

```env
# Use PostgreSQL instead of SQLite
DATABASE_URL=postgresql://user:password@localhost/ravvyn

# Set proper frontend URL
FRONTEND_URL=https://yourdomain.com

# Disable auto-sync if using webhooks
AUTO_SYNC_ENABLED=false
```

### Running with Uvicorn (Production)

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

### Building Frontend for Production

```bash
cd web/frontend
npm run build
npm start
```

## Next Steps

1. ✅ Backend running on port 8000
2. ✅ Frontend running on port 3000
3. ✅ Initial sync completed
4. ✅ Test queries working
5. ✅ Automatic sync every 15 minutes

## API Endpoints Reference

- `POST /chat` - Chat with AI (with RAG)
- `POST /sync/all` - Sync all sheets and docs
- `POST /sync/sheets` - Sync only sheets
- `POST /sync/docs` - Sync only docs
- `GET /sync/status` - Check sync status
- `GET /health` - Health check
- `POST /sheets` - Sheet operations
- `POST /docs` - Doc operations

For full API documentation, visit: `http://localhost:8000/docs`

