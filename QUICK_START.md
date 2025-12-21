# Quick Start Guide

## Fastest Way to Get Started

### 1. Backend Setup (5 minutes)

```bash
cd web/backend

# Install dependencies
pip install -r requirements.txt

# Copy and edit environment file
copy env.example.txt .env
notepad .env  # Add your OPENAI_API_KEY and Google credentials

# Check setup
python quick_start.py

# Run backend
python main.py
```

### 2. Frontend Setup (2 minutes)

```bash
cd web/frontend

# Install dependencies
npm install

# Create .env file
echo NEXT_PUBLIC_API_URL=http://localhost:8000 > .env

# Run frontend
npm run dev
```

### 3. Initial Sync (1 minute)

Once backend is running, open a new terminal:

```bash
# Option 1: Using the quick start script
cd web/backend
python quick_start.py --sync

# Option 2: Using curl
curl -X POST http://localhost:8000/sync/all

# Option 3: Using browser
# Go to http://localhost:8000/docs
# Find POST /sync/all and click "Execute"
```

### 4. Start Using!

1. Open `http://localhost:3000` in your browser
2. Start chatting with your AI assistant
3. Try asking: "What is the reading of today from this sheet?"

## What You Need

- **OpenAI API Key**: Get from https://platform.openai.com/api-keys
- **Google Credentials**: Service account JSON file from Google Cloud Console

## Troubleshooting

**Backend won't start?**
- Check `.env` file exists and has `OPENAI_API_KEY`
- Run `python quick_start.py` to check setup

**Frontend can't connect?**
- Make sure backend is running on port 8000
- Check `NEXT_PUBLIC_API_URL` in frontend `.env`

**Sync not working?**
- Verify Google credentials are correct
- Check Google APIs are enabled in Cloud Console
- See sync status: `GET http://localhost:8000/sync/status`

For detailed setup, see [SETUP_GUIDE.md](./SETUP_GUIDE.md)

