import os
import csv
import pandas as pd
from datetime import datetime

# --- SETTINGS ---
INPUT_DIR = "intermittent-aeso-sns-sqs"
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def parse_csv(file_path):
    rows = []
    try:
        with open(file_path, newline='', encoding='utf-8') as f:
            reader = list(csv.reader(f))

            # --- Extract timestamp from row 4 (line 5) ---
            timestamp_line = reader[4][0]
            timestamp_str = timestamp_line.split("Last Update :")[1].strip()
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M")

            # --- Extract rows 28 to 31 ---
            generation_rows = reader[28:32]
            for row in generation_rows:
                if len(row) < 6:
                    continue

                fuel_type = row[0].strip()
                try:
                    mc = float(row[1])
                    tng = float(row[2])
                    dcr = float(row[5])
                except ValueError:
                    continue

                # Classify subtype
                if fuel_type in ["Gas", "Coal", "Dual Fuel"]:
                    sub_type = "Fossil"
                elif fuel_type in ["Solar", "Wind", "Hydro"]:
                    sub_type = "Renewable"
                elif fuel_type == "Energy Storage":
                    sub_type = "Storage"
                elif fuel_type == "Other":
                    sub_type = "Biomass"
                elif "Interchange" in fuel_type:
                    sub_type = "Interchange"
                else:
                    sub_type = "Unknown"

                rows.append({
                    "Timestamp": timestamp,
                    "Type": fuel_type,
                    "Subtype": sub_type,
                    "MC": mc,
                    "TNG": tng,
                    "DCR": dcr,
                })
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
    return rows

def group_and_save_by_month(all_data):
    if not all_data:
        print("No data found.")
        return

    df = pd.DataFrame(all_data)
    df['Month'] = df['Timestamp'].dt.to_period('M')

    for month, group in df.groupby('Month'):
        month_str = month.strftime('%Y-%m')
        output_file = os.path.join(OUTPUT_DIR, f"{month_str}.csv")
        group.drop(columns=["Month"]).to_csv(output_file, index=False)
        print(f"Saved {len(group)} rows to {output_file}")

def main():
    all_data = []
    for root, _, files in os.walk(INPUT_DIR):
        for file in files:
            if file.endswith(".csv"):
                path = os.path.join(root, file)
                all_data.extend(parse_csv(path))
    group_and_save_by_month(all_data)

if __name__ == "__main__":
    main()
