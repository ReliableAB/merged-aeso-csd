import pandas as pd
import os
import re

def extract_last_update(file_path):
    """Extracts the 'Last Update' timestamp from a CSV file."""
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = [next(f) for _ in range(5)]  # read first 5 lines
    for line in lines:
        if "Last Update :" in line:
            return line.split("Last Update :")[1].strip()
    return None

def process_csv(file_path):
    """Reads AESO CSV, extracts timestamp, and returns DataFrame."""
    timestamp = extract_last_update(file_path)
    try:
        df = pd.read_csv(file_path, skiprows=5)  # skip metadata lines
        df['Timestamp'] = timestamp
        return df
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

def main():
    all_data = []
    for root, _, files in os.walk("."):
        for file in files:
            if file.endswith(".csv") and file != "monthly_data.csv":
                full_path = os.path.join(root, file)
                df = process_csv(full_path)
                if df is not None:
                    all_data.append(df)

    if not all_data:
        print("No data found. monthly_data.csv will not be created.")
        return

    final_df = pd.concat(all_data, ignore_index=True)
    output_file = "monthly_data.csv"  # Save at repo root
    final_df.to_csv(output_file, index=False)
    print(f"✅ Saved combined CSV as {output_file}")

if __name__ == "__main__":
    main()
