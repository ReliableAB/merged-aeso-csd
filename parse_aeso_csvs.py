import csv
import os
import pandas as pd
from datetime import datetime

def parse_csv(file_path):
    with open(file_path, newline='', encoding='utf-8') as f:
        reader = list(csv.reader(f))
        try:
            timestamp_line = reader[4][0]  # Line with "Last Update : ..."
            timestamp_str = timestamp_line.split("Last Update :")[1].strip()
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M")
        except Exception as e:
            print(f"Error reading timestamp from {file_path}: {e}")
            return []

        # Generation rows are around lines 28 to 31
        generation_rows = reader[28:32]
        rows = []
        for row in generation_rows:
            if len(row) < 6:
                continue
            fuel_type = row[0].strip()
            mc = float(row[1])
            tng = float(row[2])
            dcr = float(row[5])

            # Classify sub_type
            if fuel_type in ["Gas", "Coal", "Dual Fuel"]:
                sub_type = "Fossil"
            elif fuel_type in ["Solar", "Wind", "Hydro"]:
                sub_type = "Renewable"
            elif fuel_type == "Energy Storage":
                sub_type = "Storage"
            elif fuel_type == "Other":
                sub_type = "Biomass"
            else:
                sub_type = "Interchange" if "Interchange" in fuel_type else "Unknown"

            rows.append({
                "Timestamp": timestamp,
                "Type": fuel_type,
                "Subtype": sub_type,
                "MC": mc,
                "TNG": tng,
                "DCR": dcr,
            })
        return rows
