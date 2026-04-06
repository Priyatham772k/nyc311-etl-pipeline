import duckdb
import pandas as pd
import os

RAW_PATH = "data/raw/nyc311_raw.csv"
CLEAN_PATH = "data/clean/nyc311_clean.csv"

def run():
    print("Transforming data with DuckDB...")

    os.makedirs("data/clean", exist_ok=True)

    con = duckdb.connect()

    query = """
        SELECT
            unique_key,
            CAST(created_date AS TIMESTAMP)               AS created_date,
            CAST(closed_date  AS TIMESTAMP)               AS closed_date,
            complaint_type,
            descriptor,
            TRIM(borough)                                 AS borough,
            city,
            status,
            agency,
            agency_name,
            ROUND(
                DATEDIFF('day',
                    CAST(created_date AS TIMESTAMP),
                    CAST(closed_date  AS TIMESTAMP)
                ), 2
            )                                             AS days_to_close
        FROM read_csv_auto(?)
        WHERE complaint_type IS NOT NULL
          AND borough        IS NOT NULL
          AND borough        != 'Unspecified'
          AND created_date   IS NOT NULL
    """

    df = con.execute(query, [RAW_PATH]).df()

    df["borough"] = df["borough"].str.title()

    raw_count   = pd.read_csv(RAW_PATH).shape[0]
    clean_count = df.shape[0]

    print(f"Raw rows     : {raw_count}")
    print(f"Clean rows   : {clean_count}")
    print(f"Rows dropped : {raw_count - clean_count}")
    print(f"Columns kept : {df.shape[1]}")
    print(f"Sample days_to_close values:\n{df['days_to_close'].dropna().head()}")

    df.to_csv(CLEAN_PATH, index=False)
    print(f"Saved clean data to {CLEAN_PATH}")
    con.close()
    return df

if __name__ == "__main__":
    run()