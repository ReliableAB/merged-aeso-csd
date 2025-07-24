import os
import csv
import re
import pandas as pd
from datetime import datetime
from pathlib import Path

# Subtype mapping
SUBTYPE_MAP = {
    'GAS': 'Fossil',
    'DUAL FUEL': 'Fossil',
    'COAL': 'Fossil',
    'SOLAR': 'Renewable',
    'WIND': 'Renewable',
    'HYDRO': 'Renewable',
    'ENERGY STORAGE': 'Storage',
    'OTHER': 'Biomass',
    'INTERCHANGE AB': 'Interchange',
    'INTERCHANGE BC': 'Interchange',
    'INTERCHANGE SK': 'Interchange',
    'INTERCHANGE TOTAL': 'Interchange'
}

# Where to find raw files
INPUT_DIR = 'intermittent-aeso-sns-sqs'
OUTPUT_DIR = 'monthly_data'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Identify CSV files recursively
def find_csv_files(base_dir):
    return list(Path(base_dir).rglob("*.csv"))

def extract_timestamp(row3):
    match = re.search(r"Last Update\s*:\s*(\w+\s+\d{1,2},\s+\d{4}\s+\d{2}:\d{2})", row3)
    if match:
        dt = datetime.strptime(match.group(1), "%b %d, %Y %H:%M")
        return dt.strftime("%Y-%m-%d %H:%M")
    return None

def process_csv(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = list(csv.reader(f))
        if len(reader) < 32:
            return []

        timestamp = extract_timestamp(" ".join(reader[2]))
        if not timestamp:
            return []

        rows = []

        # Interchange: rows 18–21 (index 17–20)
        for row in reader[17:21]:
            if len(row) < 3 or not row[0].strip():
                continue
            inter_type = row[0].strip().upper()
            full_type = f"INTERCHANGE {inter_type}"
            subtype = SUBTYPE_MAP.get(full_type, 'Interchange')
            rows.append({
                'Timestamp': timestamp,
                'Type': full_type,
                'SubType': subtype,
                'MC': '',
                'TNG': row[1].strip(),
                'DCR': ''
            })

        # Generation types: rows 28–31 (index 27–30)
        for row in reader[27:31]:
            if len(row) < 4 or not row[0].strip():
                continue
            gen_type = row[0].strip().upper()
            subtype = SUBTYPE_MAP.get(gen_type, '')
            rows.append({
                'Timestamp': timestamp,
                'Type': gen_type,
                'SubType': subtype,
                'MC': row[1].strip(),
                'TNG': row[2].strip(),
                'DCR': row[3].strip()
            })

        return rows

    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        return []

def group_and_save_by_month(data_rows):
    df = pd.DataFrame(data_rows)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df['Month'] = df['Timestamp'].dt.strftime('%Y-%m')

    for month, group in df.groupby('Month'):
        out_path = os.path.join(OUTPUT_DIR, f"{month}.csv")
        group.drop(columns=['Month']).to_csv(out_path, index=False)

def main():
    all_rows = []
    for file in find_csv_files(INPUT_DIR):
        rows = process_csv(file)
        all_rows.extend(rows)
    print(f"Parsed {len(all_rows)} rows.")
    group_and_save_by_month(all_rows)

if __name__ == "__main__":
    main()
