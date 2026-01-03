import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Load environment
load_dotenv()

# CONFIG
CSV_PATH = "test_providers_geocoded.csv"
DB_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
TABLE_NAME = "providers"

def main():
    print(f"Loading CSV: {CSV_PATH}")
    
    try:
        df = pd.read_csv(CSV_PATH)
    except FileNotFoundError:
        print(f"ERROR: File not found: {CSV_PATH}")
        return
    
    print(f"   Found {len(df)} rows, {len(df.columns)} columns")
    
    # Clean
    df = df[~df['name'].str.contains('ERRORS BELOW', na=False)]
    df = df.dropna(subset=['latitude', 'longitude'])
    df = df.replace({pd.NA: None, '': None})
    df = df.drop_duplicates(subset=['name', 'full_address'], keep='first')
    
    print(f"   After cleanup: {len(df)} rows")
    
    # Connect
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            # Get table columns
            result = conn.execute(text(
                f"SELECT column_name FROM information_schema.columns WHERE table_name='{TABLE_NAME}'"
            ))
            db_columns = [row[0] for row in result]
            
            # Match columns
            csv_columns = df.columns.tolist()
            matching_columns = [col for col in csv_columns if col in db_columns]
            missing = [col for col in csv_columns if col not in db_columns]
            
            if missing:
                print(f"   Skipping: {missing}")
            
            df = df[matching_columns]
            
            # Check for duplicates in database
            if 'name' in matching_columns and 'full_address' in matching_columns:
                existing_df = pd.read_sql(
                    f"SELECT name, full_address FROM {TABLE_NAME}",
                    engine
                )
                
                # Filter out duplicates
                before_filter = len(df)
                df = df.merge(
                    existing_df,
                    on=['name', 'full_address'],
                    how='left',
                    indicator=True
                )
                df = df[df['_merge'] == 'left_only'].drop('_merge', axis=1)
                
                duplicates_skipped = before_filter - len(df)
                if duplicates_skipped > 0:
                    print(f"   Skipped {duplicates_skipped} duplicates already in database")
            
            print(f"   Inserting {len(df)} new rows")
    
    except Exception as e:
        print(f"ERROR: {e}")
        return
    
    # Insert
    if len(df) == 0:
        print("No new rows to insert!")
        return
    
    try:
        df.to_sql(TABLE_NAME, engine, if_exists="append", index=False, method='multi')
        print(f"SUCCESS! Inserted {len(df)} rows")
        
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}"))
            total = result.scalar()
            print(f"   Total in database: {total} rows")
    
    except Exception as e:
        print(f"ERROR: {e}")
        return

if __name__ == "__main__":
    main()
