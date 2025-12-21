"""
Quick Setup Script for RAVVYN AI Assistant
Run this to check if everything is configured correctly
"""

import os
import sys


def check_dependencies():
    """Check if required packages are installed"""
    print("🔍 Checking dependencies...")
    
    required = ['fastapi', 'uvicorn', 'openai', 'google']
    missing = []
    
    for package in required:
        try:
            __import__(package)
            print(f"  ✅ {package}")
        except ImportError:
            print(f"  ❌ {package} (missing)")
            missing.append(package)
    
    if missing:
        print(f"\n❌ Missing packages: {', '.join(missing)}")
        print("Run: pip install -r requirements.txt")
        return False
    
    print("✅ All dependencies installed!\n")
    return True


def check_env():
    """Check if environment variables are set"""
    print("🔍 Checking environment variables...")
    
    if not os.path.exists('.env'):
        print("  ❌ .env file not found")
        print("  Create .env file from env.example.txt")
        return False
    
    from dotenv import load_dotenv
    load_dotenv()
    
    required_vars = {
        'OPENAI_API_KEY': 'OpenAI API key',
        'GOOGLE_APPLICATION_CREDENTIALS': 'Google credentials path (or GOOGLE_CREDENTIALS_JSON)'
    }
    
    missing = []
    for var, desc in required_vars.items():
        if var == 'GOOGLE_APPLICATION_CREDENTIALS':
            # Check if either service account or OAuth JSON is set
            if not os.getenv(var) and not os.getenv('GOOGLE_CREDENTIALS_JSON'):
                print(f"  ❌ {var}: {desc}")
                missing.append(var)
            else:
                print(f"  ✅ Google credentials configured")
        elif not os.getenv(var):
            print(f"  ❌ {var}: {desc}")
            missing.append(var)
        else:
            print(f"  ✅ {var}")
    
    if missing:
        print(f"\n❌ Missing environment variables. Check your .env file")
        return False
    
    print("✅ All environment variables set!\n")
    return True


def check_google_credentials():
    """Check if Google credentials file exists"""
    print("🔍 Checking Google credentials...")
    
    from dotenv import load_dotenv
    load_dotenv()
    
    creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    
    if creds_path and os.path.exists(creds_path):
        print(f"  ✅ Service account file found: {creds_path}")
        return True
    elif os.getenv('GOOGLE_CREDENTIALS_JSON'):
        print(f"  ✅ OAuth2 credentials JSON set")
        return True
    else:
        print(f"  ❌ Google credentials not found")
        print("  Set up a service account or OAuth2 credentials")
        return False


def main():
    """Run all checks"""
    print("=" * 50)
    print("  RAVVYN AI Assistant - Setup Check")
    print("=" * 50)
    print()
    
    checks = [
        check_dependencies(),
        check_env(),
        check_google_credentials()
    ]
    
    print("=" * 50)
    
    if all(checks):
        print("🎉 All checks passed! You're ready to run:")
        print("   python main.py")
        print()
        print("Then open: http://localhost:8000/docs")
        return 0
    else:
        print("❌ Some checks failed. Please fix the issues above.")
        print()
        print("Need help? Check QUICK_START.md")
        return 1


if __name__ == "__main__":
    sys.exit(main())

