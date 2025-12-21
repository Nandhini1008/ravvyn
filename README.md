# RAVVYN Personal AI Assistant - Web Application

A complete personal AI assistant with React frontend and FastAPI backend.

## 🚀 Features

- ✅ **AI Chat** - ChatGPT-like interface (supports Tamil, English, mixed)
- ✅ **Google Sheets** - List, read, write, create, AI-powered queries
- ✅ **Google Docs** - List, read, create, summarize
- ✅ **PDF Operations** - Read and summarize PDFs
- ✅ **Reminders** - Set, list, delete reminders with notifications
- ✅ **Telegram Bot** - (Optional) All features via Telegram
- ✅ **Modern UI** - Clean React interface

## 📁 Project Structure

```
web/
├── backend/              # FastAPI Python backend
│   ├── main.py          # Main FastAPI app
│   ├── services/        # Business logic
│   │   ├── sheets.py    # Google Sheets
│   │   ├── docs.py      # Google Docs
│   │   ├── ai.py        # OpenAI
│   │   ├── reminders.py # Reminders (SQLite)
│   │   └── telegram_bot.py # Telegram bot
│   ├── requirements.txt
│   └── .env             # Configuration
├── frontend/            # React frontend
│   ├── src/
│   │   ├── App.jsx      # Main app
│   │   ├── components/  # UI components
│   │   └── services/    # API clients
│   ├── package.json
│   └── .env
└── docker-compose.yml   # One-command deployment
```

## 🔧 Setup

### Prerequisites

- Python 3.9+
- Node.js 16+
- Google Cloud Project with APIs enabled
- OpenAI API key
- (Optional) Telegram Bot Token

### 1. Backend Setup

```bash
cd web/backend

# Install dependencies
pip install -r requirements.txt

# Create .env file
cp .env.example .env

# Edit .env with your credentials
notepad .env

# Run backend
python main.py
```

Backend will run on: `http://localhost:8000`

### 2. Frontend Setup

```bash
cd web/frontend

# Install dependencies
npm install

# Create .env file
cp .env.example .env

# Run frontend
npm start
```

Frontend will run on: `http://localhost:3000`

### 3. Docker Setup (Easiest)

```bash
# From the web/ directory
docker-compose up -d
```

Everything runs automatically!

## 🔑 Configuration

### Backend `.env` file

```env
# OpenAI
OPENAI_API_KEY=your-openai-key

# Google Credentials (Option 1: Service Account)
GOOGLE_APPLICATION_CREDENTIALS=credentials/service-account.json

# Google Credentials (Option 2: OAuth JSON)
GOOGLE_CREDENTIALS_JSON={"token": "...", "refresh_token": "..."}

# Telegram (Optional)
TELEGRAM_BOT_TOKEN=your-telegram-token
TELEGRAM_CHAT_ID=your-chat-id

# Database
DATABASE_URL=sqlite:///./ravvyn.db

# PDF.co (Optional)
PDF_API_KEY=your-pdf-api-key
```

### Frontend `.env` file

```env
REACT_APP_API_URL=http://localhost:8000
```

## 📱 Usage

### Web Interface

1. Open `http://localhost:3000`
2. Chat with AI in the main interface
3. Use sidebar to access Sheets, Docs, Reminders

### API Endpoints

#### Chat
```bash
POST /chat
{
  "message": "Hello, how are you?"
}
```

#### Google Sheets
```bash
# List sheets
POST /sheets
{
  "action": "list"
}

# Read sheet
POST /sheets
{
  "action": "read",
  "sheet_id": "...",
  "tab_name": "Sheet1"
}

# AI Query
GET /sheets/{sheet_id}/query?tab_name=Sheet1&question=What's the total?
```

#### Reminders
```bash
# Set reminder
POST /reminders
{
  "action": "set",
  "message": "Buy groceries",
  "datetime": "2025-11-20 10:00"
}

# List reminders
POST /reminders
{
  "action": "list"
}
```

## 🎨 Features Demo

### AI Chat
```
You: Hello
Bot: Hello! I'm RAVVYN, your personal AI assistant. How can I help you today?

You: என்ன செய்யலாம்? (Tamil)
Bot: நான் உங்களுக்கு பல விதங்களில் உதவ முடியும்...
```

### Sheet Query
```
You: What's the total revenue in my sales sheet?
Bot: Based on your sheet, the total revenue is $45,230
```

## 💰 Cost Estimation

For single user (light usage):
- **OpenAI API**: $1-5/month (GPT-3.5-turbo)
- **Google APIs**: Free
- **Hosting**: $0 (local) or $5/month (VPS)
- **Total**: **$1-10/month**

## 🔒 Security

- API keys stored in .env (never committed)
- CORS enabled for frontend only
- Google OAuth2 for secure API access
- SQLite database (local)

## 🚀 Deployment

### Local (Development)
```bash
# Backend
cd backend && python main.py

# Frontend
cd frontend && npm start
```

### Docker (Production)
```bash
docker-compose up -d
```

### VPS Deployment
1. Clone repo to VPS
2. Set up .env files
3. Run `docker-compose up -d`
4. Access via VPS IP

## 📝 Next Steps

1. Get Google Cloud credentials
2. Get OpenAI API key
3. Run `python main.py` (backend)
4. Run `npm start` (frontend)
5. Open browser to `localhost:3000`

## 🐛 Troubleshooting

### "No Google credentials found"
- Set up service account JSON
- Or set `GOOGLE_CREDENTIALS_JSON` in .env

### "OpenAI API error"
- Check your API key
- Ensure you have credits

### "CORS error"
- Backend must be running on port 8000
- Frontend on port 3000

## 📚 Documentation

- FastAPI docs: `http://localhost:8000/docs`
- React components: See `frontend/src/components/`

---

**This is much simpler than n8n!** 🎉

All files are being created now. Check the `web/` folder.

