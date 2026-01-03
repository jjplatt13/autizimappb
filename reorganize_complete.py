"""
COMPLETE FILE STRUCTURE REORGANIZATION SCRIPT
Run from: AUTIZIM BOT root directory
"""

import os
import shutil
from pathlib import Path

def reorganize_all():
    """Complete reorganization of project structure"""
    
    root = Path.cwd()
    print(f"Working in: {root}\n")
    
    # ============================================================
    # STEP 1: Move scraper files from root to scrapers/
    # ============================================================
    print("=== STEP 1: Moving scraper files ===")
    scraper_files = [
        "abafinder_playwright.py",
        "bhcoe_scraper.py",
        "google_speech_scraper.py",
        "nationwide_scraper.py",
        "pt_aba_florida_scraper.py",
        "scraper.py",
        "scraper_abafinder.py",
        "scraper_abafinder_api.py",
        "scraper_abafinder_selenium.py",
        "yellowpages_scraper.py"
    ]
    
    scrapers_dir = root / "scrapers"
    scrapers_dir.mkdir(exist_ok=True)
    
    for file in scraper_files:
        src = root / file
        if src.exists():
            dest = scrapers_dir / file
            print(f"  Moving {file} -> scrapers/")
            shutil.move(str(src), str(dest))
    
    # ============================================================
    # STEP 2: Move data files from root to data/raw/
    # ============================================================
    print("\n=== STEP 2: Moving data files ===")
    data_files = [
        "abafinder_ALL_STATES.csv",
        "abafinder_GLIDE_READY.csv",
        "abafinder_GLIDE_READY_with_geo.csv",
        "abafinder_providers.csv",
        "autism_therapy_florida.csv",
        "florida_aba.json",
        "florida_aba_providers.csv",
        "florida_aba_raw.json",
        "florida_speech.json",
        "florida_speech_raw.json",
        "geocode_checkpoint.csv",
        "MASTER_GEOCODE_FINAL_LIST.csv",
        "nationwide_aba_raw.json",
        "nationwide_speech_raw.json",
        "raw_aba.json",
        "raw_speech.json",
        "speech_therapists_FL_all.csv",
        "speech_therapists_serpapi.csv",
        "speech_therapists_USA.csv",
        "test_providers.csv",
        "test_providers_geocoded.csv"
    ]
    
    data_raw_dir = root / "data" / "raw"
    data_raw_dir.mkdir(parents=True, exist_ok=True)
    
    for file in data_files:
        src = root / file
        if src.exists():
            dest = data_raw_dir / file
            if not dest.exists():  # Don't overwrite if already moved
                print(f"  Moving {file} -> data/raw/")
                shutil.move(str(src), str(dest))
    
    # ============================================================
    # STEP 3: Move utility scripts to scripts/
    # ============================================================
    print("\n=== STEP 3: Moving utility scripts ===")
    script_files = [
        "add_geo.py",
        "check_sitemap.py",
        "dump_nationwide_raw.py",
        "fast_geocode.py",
        "geocode_providers.py",
        "load_to_postgres.py",
        "merge_csvs.py",
        "merge_glide_ready.py",
        "scan_site.py",
        "scrape_sitemap_urls.py",
        "yp_florida.py"
    ]
    
    scripts_dir = root / "scripts"
    scripts_dir.mkdir(exist_ok=True)
    
    for file in script_files:
        src = root / file
        if src.exists():
            dest = scripts_dir / file
            print(f"  Moving {file} -> scripts/")
            shutil.move(str(src), str(dest))
    
    # ============================================================
    # STEP 4: Merge root services/ into app/services/
    # ============================================================
    print("\n=== STEP 4: Merging services folders ===")
    root_services = root / "services"
    app_services = root / "app" / "services"
    
    if root_services.exists() and app_services.exists():
        for item in root_services.iterdir():
            if item.name == "__pycache__":
                continue
            dest = app_services / item.name
            if not dest.exists():
                print(f"  Moving {item.name} -> app/services/")
                shutil.move(str(item), str(dest))
        
        # Remove empty services folder
        if not any(root_services.iterdir()):
            print("  Removing empty root services/ folder")
            root_services.rmdir()
    
    # ============================================================
    # STEP 5: Create app/api/v1/__init__.py if missing
    # ============================================================
    print("\n=== STEP 5: Verifying app structure ===")
    app_api_v1 = root / "app" / "api" / "v1"
    init_file = app_api_v1 / "__init__.py"
    if not init_file.exists():
        print("  Creating app/api/v1/__init__.py")
        init_file.touch()
    
    # ============================================================
    # STEP 6: Clean up duplicate folders
    # ============================================================
    print("\n=== STEP 6: Checking for duplicates ===")
    duplicate_folders = ["backend"]
    
    for folder in duplicate_folders:
        folder_path = root / folder
        if folder_path.exists() and folder_path.is_dir():
            # Check if empty
            if not any(folder_path.iterdir()):
                print(f"  Removing empty {folder}/ folder")
                folder_path.rmdir()
            else:
                print(f"  ⚠ {folder}/ exists and has files - please check manually")
    
    # ============================================================
    # VERIFICATION
    # ============================================================
    print("\n=== VERIFICATION ===")
    
    expected_structure = {
        "app/": root / "app",
        "app/main.py": root / "app" / "main.py",
        "app/api/v1/": root / "app" / "api" / "v1",
        "app/core/": root / "app" / "core",
        "app/services/": root / "app" / "services",
        "scrapers/": root / "scrapers",
        "scripts/": root / "scripts",
        "data/raw/": root / "data" / "raw",
    }
    
    for name, path in expected_structure.items():
        if path.exists():
            print(f"✓ {name}")
        else:
            print(f"✗ {name} MISSING")
    
    print("\n=== FINAL STRUCTURE ===")
    print("""
AUTIZIM BOT/
├── .env
├── requirements.txt
├── reorganize_files.py
│
├── app/                      ← Main application
│   ├── main.py
│   ├── api/v1/              ← API endpoints
│   ├── core/                ← Config & database
│   ├── services/            ← Business logic
│   ├── schemas/             ← Pydantic models
│   ├── repositories/        ← Database queries
│   └── utils/               ← Utilities
│
├── data/
│   ├── raw/                 ← All CSV/JSON files
│   ├── processed/
│   └── archive/
│
├── scrapers/                ← All scraper scripts
├── scripts/                 ← Utility scripts
├── utils/                   ← Root utilities (sentry)
└── ARCHIVE_OLD_BACKEND/     ← Backup (don't touch)
    """)
    
    print("\n=== NEXT STEPS ===")
    print("1. Add analytics.py to app/api/v1/")
    print("2. Update app/main.py to import analytics router")
    print("3. Delete root __pycache__ folders if any exist")
    print("4. Test your app to make sure imports still work")
    print("\nDone!")

if __name__ == "__main__":
    print("Starting complete reorganization...\n")
    try:
        reorganize_all()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("Please fix manually or re-run")
