import duckdb
import pandas as pd

CLEAN_PATH = "data/clean/nyc311_clean.csv"
DB_PATH    = "data/clean/nyc311.duckdb"

def run():
    print("Loading clean data into DuckDB database...")

    df  = pd.read_csv(CLEAN_PATH)
    con = duckdb.connect(DB_PATH)

    con.execute("DROP TABLE IF EXISTS nyc311_clean")
    con.execute("""
        CREATE TABLE nyc311_clean AS
        SELECT
            unique_key,
            CAST(created_date AS TIMESTAMP) AS created_date,
            CAST(closed_date  AS TIMESTAMP) AS closed_date,
            complaint_type,
            descriptor,
            borough,
            city,
            status,
            agency,
            agency_name,
            days_to_close
        FROM df
    """)

    count = con.execute("SELECT COUNT(*) FROM nyc311_clean").fetchone()[0]
    print(f"Rows loaded into nyc311_clean table: {count}")

    print("\nTop 5 complaint types:")
    print(
        con.execute("""
            SELECT complaint_type, COUNT(*) AS total
            FROM nyc311_clean
            GROUP BY complaint_type
            ORDER BY total DESC
            LIMIT 5
        """).df().to_string(index=False)
    )

    print("\nAvg days to close by borough:")
    print(
        con.execute("""
            SELECT borough, ROUND(AVG(days_to_close), 1) AS avg_days
            FROM nyc311_clean
            WHERE days_to_close IS NOT NULL
              AND days_to_close >= 0
            GROUP BY borough
            ORDER BY avg_days DESC
        """).df().to_string(index=False)
    )

    con.close()
    print(f"\nDatabase saved to {DB_PATH}")

if __name__ == "__main__":
    run()