import pandas as pd
import os
import re

def extract_last_update(file_path):
    """Extracts the 'Last Update' timestamp from a CSV file."""
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = [next(f) for _ in range(5)]
    for line in lines:
        if "Last Update :" in line:
            return line.split("Last Update :")[1].strip()
    return None

def process_csv(file_path):
    """Reads AESO CSV, extracts timestamp, and returns DataFrame."""
    timestamp = extract_last_update(file_path)
    try:
        df = pd.read_csv(file_path, skiprows=5)
        df['Timestamp'] = timestamp
        df['SourceFile'] = os.path.basename(file_path)
        return df
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

def main():
    output_file = "monthly_data.csv"
    processed_files = set()

    # Load existing data if available
    if os.path.exists(output_file):
        existing_df = pd.read_csv(output_file)
        processed_files = set(existing_df['SourceFile'].unique())
        all_data = [existing_df]
        print(f"Loaded {len(processed_files)} already processed files.")
    else:
        all_data = []

    # Find new CSV files
    new_files_found = False
    for root, _, files in os.walk("."):
        for file in files:
            if file.endswith(".csv") and file != output_file:
                if file not in processed_files:
                    full_path = os.path.join(root, file)
                    df = process_csv(full_path)
                    if df is not None:
                        all_data.append(df)
                        new_files_found = True

    if not new_files_found:
        print("No new CSV files found.")
        return

    # Combine and save
    final_df = pd.concat(all_data, ignore_index=True)
    final_df.to_csv(output_file, index=False)
    print(f"✅ Updated {output_file} with new data.")

if __name__ == "__main__":
    main()
