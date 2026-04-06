
import requests
import pandas as pd
import os

RAW_PATH = "data/raw/nyc311_raw.csv"
API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
LIMIT = 10000

def extract():
    print("Extracting data from NYC Open Data API...")
    params = {
        "$limit": LIMIT,
        "$order": "created_date DESC"
    }
    response = requests.get(API_URL, params=params, timeout=60)
    response.raise_for_status()

    os.makedirs("data/raw", exist_ok=True)

    with open(RAW_PATH, "wb") as f:
        f.write(response.content)

    df = pd.read_csv(RAW_PATH)
    print(f"Extracted {len(df)} rows and {len(df.columns)} columns.")
    print(f"Saved to {RAW_PATH}")
    return df

if __name__ == "__main__":
    extract()