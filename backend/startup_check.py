#!/usr/bin/env python3
"""
Startup check for RAVVYN backend - verifies all imports and configurations
"""

import sys
import os

# Add the backend directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def check_imports():
    """Check if all required modules can be imported"""
    print("🔍 Checking imports...")
    
    try:
        # Core FastAPI imports
        from fastapi import FastAPI, BackgroundTasks, Depends, Request
        from fastapi.responses import JSONResponse
        print("  ✅ FastAPI imports successful")
        
        # Database imports
        from services.database import init_db, get_db, get_db_context
        print("  ✅ Database imports successful")
        
        # Service imports
        from services.sheets import SheetsService
        from services.docs import DocsService
        from services.ai import AIService
        from services.reminders import RemindersService
        from services.hash_service import HashService
        print("  ✅ Service imports successful")
        
        # API schema imports
        from api.schemas import (
            ChatRequest, ChatResponse,
            SheetRequest, SheetResponse,
            SheetUpdateRequest, SheetDeleteRequest, SheetInsertRequest,
            DocRequest, DocResponse,
            DocUpdateRequest, DocDeleteRequest, DocReplaceRequest,
            TaskCreateRequest, TaskUpdateRequest, TaskResponse
        )
        print("  ✅ API schema imports successful")
        
        return True
        
    except ImportError as e:
        print(f"  ❌ Import error: {str(e)}")
        return False
    except Exception as e:
        print(f"  ❌ Unexpected error: {str(e)}")
        return False

def check_configuration():
    """Check configuration settings"""
    print("\n🔧 Checking configuration...")
    
    try:
        from core.config import get_settings
        settings = get_settings()
        
        print(f"  ✅ Settings loaded successfully")
        print(f"  📊 Host: {settings.host}")
        print(f"  📊 Port: {settings.port}")
        print(f"  📊 Database URL: {settings.database_url}")
        print(f"  📊 Hash enabled: {settings.hash_enabled}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Configuration error: {str(e)}")
        return False

def check_database():
    """Check database connectivity"""
    print("\n🗄️  Checking database...")
    
    try:
        from services.database import init_db, get_db_context
        
        # Try to initialize database
        init_db()
        print("  ✅ Database initialization successful")
        
        # Try to connect
        with get_db_context() as db:
            # Simple query to test connection
            result = db.execute("SELECT 1").scalar()
            if result == 1:
                print("  ✅ Database connection successful")
                return True
            else:
                print("  ❌ Database query failed")
                return False
                
    except Exception as e:
        print(f"  ❌ Database error: {str(e)}")
        return False

def check_main_app():
    """Check if main app can be imported and created"""
    print("\n🚀 Checking main application...")
    
    try:
        from main import app
        
        # Count routes
        route_count = len([r for r in app.routes if hasattr(r, 'methods')])
        print(f"  ✅ Main app imported successfully")
        print(f"  📊 Total routes registered: {route_count}")
        
        # Check for essential routes
        essential_paths = ["/", "/chat", "/sheets", "/docs", "/tasks", "/health"]
        registered_paths = [r.path for r in app.routes if hasattr(r, 'path')]
        
        missing_paths = []
        for path in essential_paths:
            if path not in registered_paths:
                missing_paths.append(path)
        
        if missing_paths:
            print(f"  ⚠️  Missing essential routes: {missing_paths}")
        else:
            print("  ✅ All essential routes registered")
        
        return len(missing_paths) == 0
        
    except Exception as e:
        print(f"  ❌ Main app error: {str(e)}")
        return False

def main():
    """Run all startup checks"""
    print("🚀 RAVVYN Backend Startup Check")
    print("=" * 50)
    
    checks = [
        ("Imports", check_imports),
        ("Configuration", check_configuration),
        ("Database", check_database),
        ("Main Application", check_main_app)
    ]
    
    results = []
    for name, check_func in checks:
        try:
            result = check_func()
            results.append((name, result))
        except Exception as e:
            print(f"  ❌ {name} check failed: {str(e)}")
            results.append((name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 STARTUP CHECK SUMMARY")
    print("=" * 50)
    
    passed = 0
    for name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"  {name}: {status}")
        if result:
            passed += 1
    
    total = len(results)
    print(f"\n🎯 Result: {passed}/{total} checks passed ({passed/total:.1%})")
    
    if passed == total:
        print("\n🎉 ALL CHECKS PASSED! Your backend is ready to start.")
        print("💡 Run: python main.py")
        return True
    else:
        print(f"\n❌ {total - passed} checks failed. Fix the issues above before starting.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)