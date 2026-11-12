import pandas as pd
from pathlib import Path


INPUT_FILE = Path("data/online_retail_II.xlsx")
OUTPUT_FILE = Path("data/online_retail_II.csv")


print("=" * 60)
print("ONLINE RETAIL II - DATA PREPARATION")
print("=" * 60)

print(f"Reading: {INPUT_FILE}")

# Read both Excel sheets
sheets = pd.read_excel(
    INPUT_FILE,
    sheet_name=None
)

print(f"Sheets found: {list(sheets.keys())}")

# Combine both years
df = pd.concat(
    sheets.values(),
    ignore_index=True
)

print(f"Total rows: {len(df):,}")
print(f"Total columns: {len(df.columns)}")

print("\nColumns:")
for column in df.columns:
    print(f" - {column}")

print("\nFirst 5 rows:")
print(df.head())

print("\nMissing values:")
print(df.isnull().sum())

# Save consolidated raw dataset
df.to_csv(
    OUTPUT_FILE,
    index=False,
    encoding="utf-8"
)

print("\n" + "=" * 60)
print("CSV CREATED SUCCESSFULLY")
print("=" * 60)
print(f"Output: {OUTPUT_FILE}")
print(f"Size: {OUTPUT_FILE.stat().st_size / (1024 * 1024):.2f} MB")