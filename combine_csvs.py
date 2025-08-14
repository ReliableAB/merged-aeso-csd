import os
import requests
import pandas as pd
import io
from github import Github
import re
import time

def extract_last_update(content):
    """Extracts the 'Last Update' timestamp from CSV content."""
    lines = content.splitlines()[:5]
    for line in lines:
        if "Last Update :" in line:
            return line.split("Last Update :")[1].strip()
    return None

def process_csv(csv_url, file_name):
    """Reads AESO CSV from URL, extracts timestamp, and returns DataFrame."""
    response = requests.get(csv_url)
    response.raise_for_status()
    content = response.text
    timestamp = extract_last_update(content)
    try:
        df = pd.read_csv(io.StringIO(content), skiprows=5)
        df['Timestamp'] = timestamp
        df['SourceFile'] = file_name
        return df
    except Exception as e:
        print(f"Error processing {file_name}: {e}")
        return None

def get_all_files(repo, path, token=None):
    """Recursively fetch all files from a repository path with pagination."""
    files = []
    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    headers = {"Authorization": f"token {token}"} if token else {}
    while url:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        items = response.json()
        for item in items:
            if item["type"] == "file" and item["name"].endswith(".csv"):
                files.append(item)
            elif item["type"] == "dir":
                files.extend(get_all_files(repo, item["path"], token))
        url = response.links.get("next", {}).get("url")
        time.sleep(0.1)  # 100ms delay to avoid rate limits
    return files

def main():
    # Configuration
    SOURCE_REPO = "intermittentnrg/intermittent-aeso-sns-sqs"
    YOUR_REPO = "your_username/merged-aeso-csd"  # Replace with your actual username
    YOUR_PAT = os.getenv("CSV_TOKEN")
    SOURCE_CSV_PATH = "2024-06-23"  # Test with one date folder
    OUTPUT_FILE = "data/monthly_data.csv"

    # Initialize GitHub client
    g = Github(YOUR_PAT)
    your_repo = g.get_repo(YOUR_REPO)

    # Load existing processed files (commented out for testing)
    processed_files = set()
    # try:
    #     output_content = your_repo.get_contents(OUTPUT_FILE)
    #     existing_df = pd.read_csv(io.StringIO(output_content.decoded_content.decode("utf-8")))
    #     processed_files = set(existing_df['SourceFile'].unique())
    #     all_data = [existing_df]
    #     print(f"Loaded {len(processed_files)} already processed files.")
    # except:
    #     all_data = []
    all_data = []
    print("No existing output file found or skipped for testing.")

    # Fetch CSV files recursively from source repository
    files = get_all_files(SOURCE_REPO, SOURCE_CSV_PATH, YOUR_PAT)
    print(f"Found {len(files)} CSV files in {SOURCE_REPO}/{SOURCE_CSV_PATH}")

    # Process new CSV files
    new_files_found = False
    for file in files:
        if file["name"] not in processed_files:
            print(f"Processing {file['path']}")
            df = process_csv(file["download_url"], file["path"])
            if df is not None:
                all_data.append(df)
                new_files_found = True
                # Save intermediate results to manage memory
                if len(all_data) >= 50:  # Process in batches of 50
                    temp_df = pd.concat(all_data, ignore_index=True)
                    temp_df.to_csv(OUTPUT_FILE, index=False)
                    all_data = [temp_df]

    if not new_files_found:
        print("No new CSV files found.")
        return

    # Combine and save to local file
    final_df = pd.concat(all_data, ignore_index=True)
    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved combined data to local {OUTPUT_FILE}")

    # Upload to your repository
    with open(OUTPUT_FILE, "rb") as file:
        content = file.read()
        try:
            output_content = your_repo.get_contents(OUTPUT_FILE)
            your_repo.update_file(
                path=OUTPUT_FILE,
                message="Update monthly_data.csv with new data",
                content=content,
                sha=output_content.sha,
                branch="main"
            )
            print(f"✅ Updated {OUTPUT_FILE} in {YOUR_REPO}")
        except:
            your_repo.create_file(
                path=OUTPUT_FILE,
                message="Add monthly_data.csv",
                content=content,
                branch="main"
            )
            print(f"✅ Created {OUTPUT_FILE} in {YOUR_REPO}")

if __name__ == "__main__":
    main()
