#!/usr/bin/env python3
"""
Verify that all CRUD endpoints are properly registered in main.py
This script checks the FastAPI app routes without starting the server
"""

import sys
import os

# Add the backend directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def verify_endpoints():
    """Verify all endpoints are registered"""
    try:
        # Import the FastAPI app
        from main import app
        
        print("🚀 RAVVYN Backend Endpoint Verification")
        print("=" * 60)
        
        # Get all routes
        routes = []
        for route in app.routes:
            if hasattr(route, 'methods') and hasattr(route, 'path'):
                for method in route.methods:
                    if method != 'HEAD':  # Skip HEAD methods
                        routes.append((method, route.path))
        
        # Sort routes by path
        routes.sort(key=lambda x: x[1])
        
        # Categorize endpoints
        categories = {
            "Core": [],
            "Chat & AI": [],
            "Sheets (CRUD)": [],
            "Docs (CRUD)": [],
            "Tasks (CRUD)": [],
            "Reminders": [],
            "Sync Operations": [],
            "Hash System": [],
            "Export": [],
            "Cache": [],
            "API Data": [],
            "Admin & Test": [],
            "Monitoring": []
        }
        
        # Categorize each route
        for method, path in routes:
            route_str = f"{method:6} {path}"
            
            if path in ["/", "/health"]:
                categories["Core"].append(route_str)
            elif "/chat" in path:
                categories["Chat & AI"].append(route_str)
            elif "/sheets" in path:
                categories["Sheets (CRUD)"].append(route_str)
            elif "/docs" in path:
                categories["Docs (CRUD)"].append(route_str)
            elif "/tasks" in path:
                categories["Tasks (CRUD)"].append(route_str)
            elif "/reminders" in path:
                categories["Reminders"].append(route_str)
            elif "/sync" in path:
                categories["Sync Operations"].append(route_str)
            elif "/hash" in path:
                categories["Hash System"].append(route_str)
            elif "/export" in path:
                categories["Export"].append(route_str)
            elif "/cache" in path:
                categories["Cache"].append(route_str)
            elif "/api" in path:
                categories["API Data"].append(route_str)
            elif "/admin" in path or "/test" in path:
                categories["Admin & Test"].append(route_str)
            elif "/monitoring" in path:
                categories["Monitoring"].append(route_str)
            else:
                categories["Core"].append(route_str)
        
        # Display categorized endpoints
        total_endpoints = 0
        for category, endpoints in categories.items():
            if endpoints:
                print(f"\n📋 {category} ({len(endpoints)} endpoints)")
                print("-" * 40)
                for endpoint in endpoints:
                    print(f"  {endpoint}")
                total_endpoints += len(endpoints)
        
        print("\n" + "=" * 60)
        print(f"✅ VERIFICATION COMPLETE")
        print(f"📊 Total Endpoints: {total_endpoints}")
        print("=" * 60)
        
        # Check for essential CRUD operations
        essential_patterns = [
            ("GET", "/"),
            ("POST", "/chat"),
            ("POST", "/sheets"),
            ("GET", "/sheets/{sheet_id}/query"),
            ("POST", "/sheets/update"),
            ("POST", "/sheets/delete"),
            ("POST", "/sheets/insert"),
            ("POST", "/docs"),
            ("POST", "/docs/update"),
            ("POST", "/docs/delete"),
            ("POST", "/docs/replace"),
            ("POST", "/tasks"),
            ("GET", "/tasks"),
            ("GET", "/tasks/{task_id}"),
            ("PUT", "/tasks/{task_id}"),
            ("DELETE", "/tasks/{task_id}"),
            ("POST", "/hash/compute"),
            ("GET", "/hash/status/{file_id}"),
            ("DELETE", "/hash/{file_id}"),
            ("GET", "/health")
        ]
        
        print("\n🔍 ESSENTIAL CRUD ENDPOINTS CHECK")
        print("-" * 40)
        
        missing_endpoints = []
        for method, path in essential_patterns:
            found = any(r[0] == method and r[1] == path for r in routes)
            status = "✅" if found else "❌"
            print(f"  {status} {method:6} {path}")
            if not found:
                missing_endpoints.append(f"{method} {path}")
        
        if missing_endpoints:
            print(f"\n❌ MISSING ENDPOINTS ({len(missing_endpoints)}):")
            for endpoint in missing_endpoints:
                print(f"  - {endpoint}")
            return False
        else:
            print(f"\n🎉 ALL ESSENTIAL CRUD ENDPOINTS ARE REGISTERED!")
            return True
            
    except ImportError as e:
        print(f"❌ Import Error: {str(e)}")
        print("💡 Make sure you're in the backend directory and dependencies are installed")
        return False
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

if __name__ == "__main__":
    success = verify_endpoints()
    sys.exit(0 if success else 1)