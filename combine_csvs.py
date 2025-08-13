import requests
import pandas as pd
import io
from github import Github
import re

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

def main():
    # Configuration
    SOURCE_REPO = "second_account/repo_name"  # e.g., "user/public-repo"
    YOUR_REPO = "your_account/your_repo_name"
    YOUR_PAT = "your_personal_access_token"  # PAT with repo scope
    SOURCE_CSV_PATH = "path/to/csv_folder"  # Path to CSVs in source repo
    OUTPUT_FILE = "monthly_data.csv"

    # Initialize GitHub client
    g = Github(YOUR_PAT)
    your_repo = g.get_repo(YOUR_REPO)

    # Load existing processed files from output CSV (if it exists)
    processed_files = set()
    try:
        output_content = your_repo.get_contents(OUTPUT_FILE)
        existing_df = pd.read_csv(io.StringIO(output_content.decoded_content.decode("utf-8")))
        processed_files = set(existing_df['SourceFile'].unique())
        all_data = [existing_df]
        print(f"Loaded {len(processed_files)} already processed files.")
    except:
        all_data = []
        print("No existing output file found.")

    # Fetch CSV files from source repository
    api_url = f"https://api.github.com/repos/{SOURCE_REPO}/contents/{SOURCE_CSV_PATH}"
    response = requests.get(api_url)
    response.raise_for_status()
    files = response.json()

    # Process new CSV files
    new_files_found = False
    for file in files:
        if isinstance(file, dict) and file["name"].endswith(".csv") and file["name"] != OUTPUT_FILE:
            if file["name"] not in processed_files:
                df = process_csv(file["download_url"], file["name"])
                if df is not None:
                    all_data.append(df)
                    new_files_found = True

    if not new_files_found:
        print("No new CSV files found.")
        return

    # Combine and save to local file
    final_df = pd.concat(all_data, ignore_index=True)
    final_df.to_csv(OUTPUT_FILE, index=False)

    # Upload to your repository
    with open(OUTPUT_FILE, "rb") as file:
        content = file.read()
        try:
            # Update existing file or create new
            output_content = your_repo.get_contents(OUTPUT_FILE)
            your_repo.update_file(
                path=OUTPUT_FILE,
                message="Update monthly_data.csv with new data",
                content=content,
                sha=output_content.sha,
                branch="main"
            )
        except:
            your_repo.create_file(
                path=OUTPUT_FILE,
                message="Add monthly_data.csv",
                content=content,
                branch="main"
            )
    print(f"✅ Updated {OUTPUT_FILE} in {YOUR_REPO}.")

if __name__ == "__main__":
    main()
