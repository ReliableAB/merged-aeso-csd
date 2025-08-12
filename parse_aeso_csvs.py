import os
import pandas as pd
import csv
import re
from datetime import datetime

DATA_DIR = "intermittent-aeso-sns-sqs"
OUTPUT_DIR = "monthly_data"

FUEL_TYPE_MAP = {
    "Gas": ("Gas", "Fossil"),
    "Dual Fuel": ("Dual Fuel", "Fossil"),
    "Coal": ("Coal", "Fossil"),
    "Solar": ("Solar", "Renewable"),
    "Wind": ("Wind", "Renewable"),
    "Hydro": ("Hydro", "Renewable"),
    "Energy Storage": ("Energy Storage", "Storage"),
    "Other": ("Other", "Biomass"),
    "Interchange": ("Interchange", "Interchange"),
}

def extract_timestamp_from_csv(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        for i in range(6):  # scan first few lines
            line = f.readline()
            if "Last Update" in line:
                match = re.search(r"Last Update\s*:\s*(.*)", line)
                if match:
                    timestamp_str = match.group(1).strip()
                    try:
                        # Match "May 10, 2024 16:19"
                        return datetime.strptime(timestamp_str, "%b %d, %Y %H:%M")
                    except ValueError:
                        print(f"Could not parse timestamp in {filepath}: {timestamp_str}")
                        return None
    return None

def extract_generation_rows(filepath, timestamp):
    rows = []
    with open(filepath, "r", encoding="utf-8") as f:
        content = list(csv.reader(f))

    # Find where generation data starts (look for "GAS" row)
    start_index = None
    for idx, row in enumerate(content):
        if row and row[0].strip().upper() == "GAS":
            start_index = idx
            break

    if start_index is None:
        return rows  # no generation table found

    df = pd.DataFrame(content[start_index:], columns=["Fuel Type", "MC", "TNG"])
    for _, row in df.iterrows():
        fuel_type = str(row["Fuel Type"]).strip()
        if fuel_type in FUEL_TYPE_MAP:
            mc = pd.to_numeric(row["MC"], errors="coerce")
            tng = pd.to_numeric(row["TNG"], errors="coerce")
            dcr = mc - tng if pd.notnull(mc) and pd.notnull(tng) else None
            rows.append({
                "Fuel Type": FUEL_TYPE_MAP[fuel_type][0],
                "Sub Type": FUEL_TYPE_MAP[fuel_type][1],
                "MC": mc,
                "TNG": tng,
                "DCR": dcr,
                "Timestamp": timestamp
            })
    return rows

def group_and_save_by_month(all_rows):
    if not all_rows:
        print("Parsed 0 rows.")
        return

    df = pd.DataFrame(all_rows)
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df["Month"] = df["Timestamp"].dt.to_period("M")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for period, group in df.groupby("Month"):
        filename = f"{OUTPUT_DIR}/{period.strftime('%Y-%m')}.csv"
        group.drop(columns=["Month"]).to_csv(filename, index=False)

    print(f"Saved monthly CSVs to {OUTPUT_DIR}/")

def main():
    all_rows = []
    for root, _, files in os.walk(DATA_DIR):
        for file in files:
            if file.endswith(".csv"):
                filepath = os.path.join(root, file)
                try:
                    timestamp = extract_timestamp_from_csv(filepath)
                    if timestamp:
                        rows = extract_generation_rows(filepath, timestamp)
                        all_rows.extend(rows)
                except Exception as e:
                    print(f"Error processing {filepath}: {e}")

    group_and_save_by_month(all_rows)

if __name__ == "__main__":
    main()
