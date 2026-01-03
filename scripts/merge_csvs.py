import os
import pandas as pd

# Folder where all your CSV files exist
folder = r"C:\Users\zubby\AUTIZIM BOT"

# Output file
output_file = os.path.join(folder, "abafinder_ALL_STATES.csv")

print("🔍 Looking for CSV files in:", folder)

# Get all CSVs that match abafinder_XX.csv
csv_files = [
    f for f in os.listdir(folder)
    if f.startswith("abafinder_") and f.endswith(".csv")
]

print(f"📄 Found {len(csv_files)} state CSV files:")
for f in csv_files:
    print("  -", f)

if not csv_files:
    print("❌ No files found. Make sure they are named like 'abafinder_CA.csv'")
    exit()

# Read and merge
df_list = []
for file in csv_files:
    full_path = os.path.join(folder, file)
    print(f"➡️  Merging {file}...")
    df = pd.read_csv(full_path)

    # Add a column for the state (from filename)
    state_code = file.replace("abafinder_", "").replace(".csv", "")
    df["state"] = state_code

    df_list.append(df)

merged_df = pd.concat(df_list, ignore_index=True)

# Save final merged CSV
merged_df.to_csv(output_file, index=False)

print("\n✅ MERGE COMPLETE!")
print(f"📁 Output saved to: {output_file}")
print(f"📊 Total rows: {len(merged_df)}")
