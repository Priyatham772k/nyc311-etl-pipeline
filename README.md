# NYC 311 ETL Pipeline

An end-to-end ETL pipeline that extracts NYC 311 service request data from the NYC Open Data API, transforms it using DuckDB SQL, loads it into a persistent database, and surfaces insights through an interactive Streamlit dashboard.

## Features

- Extracts 10,000 recent 311 service requests via the NYC Open Data API
- Cleans and transforms raw data using DuckDB SQL (type casting, null filtering, feature derivation)
- Derives `days_to_close` to measure request resolution time per borough
- Loads clean data into a persistent DuckDB table
- Interactive Streamlit UI to trigger the pipeline and explore results

## Tech Stack

- Python 3.11
- DuckDB 0.10.3
- Pandas 2.2.2
- Streamlit 1.35.0
- NYC Open Data API (Socrata)

## Project Structure

```
nyc311-etl-pipeline/
├── data/
│   ├── raw/          # Raw CSV from API (git-ignored)
│   └── clean/        # Cleaned CSV and DuckDB file (git-ignored)
├── src/
│   ├── extract.py    # Pulls data from NYC Open Data API
│   ├── transform.py  # Cleans and transforms with DuckDB SQL
│   ├── load.py       # Loads clean data into DuckDB table
│   └── pipeline.py   # Orchestrates the full ETL run
├── app.py            # Streamlit dashboard
├── requirements.txt
└── README.md
```

## Getting Started

```bash
git clone https://github.com/YOUR_USERNAME/nyc311-etl-pipeline.git
cd nyc311-etl-pipeline

python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # Mac/Linux

pip install -r requirements.txt
streamlit run app.py
```

Then click **Run ETL Now** in the browser to fetch and process the data.

## Pipeline Steps

| Step | Script | Description |
|---|---|---|
| Extract | `extract.py` | Fetches 10,000 rows from NYC Open Data API |
| Transform | `transform.py` | Filters nulls, casts types, derives `days_to_close` |
| Load | `load.py` | Writes clean data to a DuckDB table |
| Orchestrate | `pipeline.py` | Runs all three steps end to end |

## Data Source

NYC 311 Service Requests from [NYC Open Data](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9)