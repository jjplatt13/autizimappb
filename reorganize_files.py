"""
File Structure Reorganization Script
Run this from your AUTIZIM BOT root directory
"""

import os
import shutil
from pathlib import Path

def reorganize_structure():
    """Move analytics and api folders into app/"""
    
    # Get current directory
    root = Path.cwd()
    print(f"Working in: {root}")
    
    # Create app folder if it doesn't exist
    app_dir = root / "app"
    if not app_dir.exists():
        print("Creating app/ folder...")
        app_dir.mkdir()
    else:
        print("app/ folder exists")
    
    # Move analytics folder into app/
    analytics_src = root / "analytics"
    analytics_dest = app_dir / "analytics"
    
    if analytics_src.exists() and analytics_src.is_dir():
        if analytics_dest.exists():
            print(f"Removing old {analytics_dest}...")
            shutil.rmtree(analytics_dest)
        print(f"Moving analytics/ -> app/analytics/")
        shutil.move(str(analytics_src), str(analytics_dest))
    else:
        print("analytics/ folder not found in root, skipping")
    
    # Move api folder into app/
    api_src = root / "api"
    api_dest = app_dir / "api"
    
    if api_src.exists() and api_src.is_dir():
        if api_dest.exists():
            print(f"Removing old {api_dest}...")
            shutil.rmtree(api_dest)
        print(f"Moving api/ -> app/api/")
        shutil.move(str(api_src), str(api_dest))
    else:
        print("api/ folder not found in root, skipping")
    
    # Verify structure
    print("\n=== Verification ===")
    required_paths = [
        app_dir / "api" / "v1",
        app_dir / "analytics"
    ]
    
    for path in required_paths:
        if path.exists():
            print(f"✓ {path} exists")
        else:
            print(f"✗ {path} NOT FOUND")
    
    print("\n=== Done! ===")
    print("Your structure should now be:")
    print("app/")
    print("├── api/")
    print("│   └── v1/")
    print("└── analytics/")

if __name__ == "__main__":
    print("Starting file reorganization...\n")
    reorganize_structure()
