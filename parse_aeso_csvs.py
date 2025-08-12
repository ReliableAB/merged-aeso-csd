import csv
import os
import re
from datetime import datetime

OUTPUT_FILE = "monthly_data.csv"

def extract_timestamp(file_path):
    """Extract timestamp from row 4 after 'Last Update :' in CSV."""
    try:
        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
            if len(rows) >= 4:
                match = re.search(r"Last Update\s*:\s*(.+)", rows[3][0])
                if match:
                    return match.group(1).strip()
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    return None

def process_files():
    output_rows = []
    for root, _, files in os.walk("."):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                timestamp = extract_timestamp(file_path)
                if timestamp:
                    output_rows.append([timestamp, file_path])
    return output_rows

if __name__ == "__main__":
    rows = process_files()
    if rows:
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Timestamp", "File"])
            writer.writerows(rows)
        print(f"Saved {len(rows)} entries to {OUTPUT_FILE}")
    else:
        print("No valid CSVs found.")
