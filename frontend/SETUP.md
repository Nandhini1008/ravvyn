# Frontend Setup Guide

## Prerequisites

- Node.js 16+ installed
- Backend running on `http://localhost:8000`

## Installation Steps

1. **Install dependencies:**
   ```bash
   cd web/frontend
   npm install
   ```

2. **Create environment file:**
   ```bash
   # Copy the example file
   cp .env.example .env
   ```

3. **Update `.env` file:**
   ```
   NEXT_PUBLIC_API_URL=http://localhost:8000
   ```
   
   If your backend is running on a different port or URL, update this accordingly.

4. **Run the development server:**
   ```bash
   npm run dev
   ```

5. **Open your browser:**
   Navigate to `http://localhost:3000`

## Features

- ✅ Animated chat interface
- ✅ Command palette with shortcuts (/clone, /figma, /page, /improve)
- ✅ Real-time typing indicators
- ✅ File attachment support (UI ready)
- ✅ Responsive design
- ✅ Error handling

## Troubleshooting

### Backend Connection Issues

If you see errors about API connection:
1. Make sure the backend is running: `cd web/backend && python main.py`
2. Check that `NEXT_PUBLIC_API_URL` in `.env` matches your backend URL
3. Verify CORS is enabled in the backend (it should be by default)

### Build Errors

If you encounter build errors:
```bash
# Clear cache and reinstall
rm -rf node_modules .next
npm install
npm run dev
```

### Port Already in Use

If port 3000 is already in use:
```bash
# Use a different port
PORT=3001 npm run dev
```

