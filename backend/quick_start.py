"""
Quick Start Script - Helps verify setup and run initial sync
"""

import os
import sys
import asyncio
from dotenv import load_dotenv

load_dotenv()


def check_setup():
    """Check if everything is set up correctly"""
    print("=" * 60)
    print("  RAVVYN AI Assistant - Quick Setup Check")
    print("=" * 60)
    print()
    
    errors = []
    warnings = []
    
    # Check environment file
    if not os.path.exists('.env'):
        errors.append("❌ .env file not found. Copy env.example.txt to .env")
    else:
        print("✅ .env file found")
    
    # Check OpenAI API key
    openai_key = os.getenv('OPENAI_API_KEY')
    if not openai_key or openai_key == 'sk-your-openai-api-key-here':
        errors.append("❌ OPENAI_API_KEY not set in .env file")
    else:
        print("✅ OPENAI_API_KEY configured")
    
    # Check Google credentials
    google_creds = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    google_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    
    if google_creds:
        if os.path.exists(google_creds):
            print(f"✅ Google credentials found: {google_creds}")
        else:
            errors.append(f"❌ Google credentials file not found: {google_creds}")
    elif google_json:
        print("✅ Google OAuth2 credentials configured")
    else:
        errors.append("❌ Google credentials not configured")
    
    # Check database
    db_url = os.getenv('DATABASE_URL', 'sqlite:///./ravvyn.db')
    print(f"✅ Database URL: {db_url}")
    
    # Check dependencies
    print("\n🔍 Checking Python dependencies...")
    required_packages = [
        'fastapi', 'uvicorn', 'openai', 'sqlalchemy', 
        'google', 'apscheduler', 'python-dotenv'
    ]
    
    missing = []
    for package in required_packages:
        try:
            if package == 'google':
                __import__('googleapiclient')
            else:
                __import__(package)
            print(f"  ✅ {package}")
        except ImportError:
            print(f"  ❌ {package} (missing)")
            missing.append(package)
    
    if missing:
        errors.append(f"❌ Missing packages: {', '.join(missing)}")
        errors.append("   Run: pip install -r requirements.txt")
    
    # Summary
    print("\n" + "=" * 60)
    if errors:
        print("❌ Setup Issues Found:")
        for error in errors:
            print(f"  {error}")
        print("\nPlease fix the issues above before running the server.")
        return False
    else:
        print("✅ All checks passed! You're ready to go!")
        print("\nNext steps:")
        print("  1. Run: python main.py")
        print("  2. Open: http://localhost:8000/docs")
        print("  3. Trigger initial sync: POST /sync/all")
        print("  4. Start frontend: cd ../frontend && npm run dev")
        return True


async def run_initial_sync():
    """Run initial sync of all sheets and docs"""
    print("\n" + "=" * 60)
    print("  Running Initial Sync")
    print("=" * 60)
    print()
    
    try:
        from services.sync_service import SyncService
        
        sync_service = SyncService()
        print("🔄 Syncing all sheets and docs...")
        print("   This may take a few minutes depending on the number of files...")
        print()
        
        stats = await sync_service.sync_all(force=True)
        
        print("✅ Sync completed!")
        print(f"   Sheets: {stats['sheets']['synced']} synced, {stats['sheets']['errors']} errors")
        print(f"   Docs: {stats['docs']['synced']} synced, {stats['docs']['errors']} errors")
        
        if stats['total_errors'] > 0:
            print("\n⚠️  Some errors occurred during sync. Check the logs for details.")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during sync: {str(e)}")
        return False


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--sync':
        # Run sync
        if check_setup():
            asyncio.run(run_initial_sync())
    else:
        # Just check setup
        check_setup()

