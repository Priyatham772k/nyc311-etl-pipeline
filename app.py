import streamlit as st
import duckdb
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
from pipeline import run_pipeline

DB_PATH = "data/clean/nyc311.duckdb"

st.set_page_config(page_title="NYC 311 ETL Pipeline", layout="wide")
st.title("NYC 311 ETL Pipeline")
st.caption("Extract, transform, and explore 311 service request data from NYC Open Data.")

# ── Run Pipeline ──────────────────────────────────────────────
st.header("Run Pipeline")
if st.button("Run ETL Now", type="primary"):
    with st.spinner("Running pipeline..."):
        log_output = []
        import io
        from contextlib import redirect_stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            run_pipeline()
        log_output = buf.getvalue()
    st.success("Pipeline completed.")
    with st.expander("Pipeline log"):
        st.code(log_output, language="text")

# ── Load Data ─────────────────────────────────────────────────
if not os.path.exists(DB_PATH):
    st.info("No database found. Run the pipeline above to get started.")
    st.stop()

con = duckdb.connect(DB_PATH, read_only=True)

# ── Summary Metrics ───────────────────────────────────────────
st.header("Summary")
total    = con.execute("SELECT COUNT(*) FROM nyc311_clean").fetchone()[0]
boroughs = con.execute("SELECT COUNT(DISTINCT borough) FROM nyc311_clean").fetchone()[0]
types    = con.execute("SELECT COUNT(DISTINCT complaint_type) FROM nyc311_clean").fetchone()[0]
avg_days = con.execute("""
    SELECT ROUND(AVG(days_to_close), 1)
    FROM nyc311_clean
    WHERE days_to_close >= 0
""").fetchone()[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Requests", f"{total:,}")
c2.metric("Boroughs", boroughs)
c3.metric("Complaint Types", types)
c4.metric("Avg Days to Close", avg_days)

# ── Top Complaint Types ───────────────────────────────────────
st.header("Top Complaint Types")
n = st.slider("Show top N types", 5, 20, 10)
top_types = con.execute(f"""
    SELECT complaint_type, COUNT(*) AS total
    FROM nyc311_clean
    GROUP BY complaint_type
    ORDER BY total DESC
    LIMIT {n}
""").df()
st.bar_chart(top_types.set_index("complaint_type"))

# ── By Borough ────────────────────────────────────────────────
st.header("Requests by Borough")
borough_df = con.execute("""
    SELECT borough, COUNT(*) AS total
    FROM nyc311_clean
    GROUP BY borough
    ORDER BY total DESC
""").df()
st.bar_chart(borough_df.set_index("borough"))

# ── Resolution Time ───────────────────────────────────────────
st.header("Avg Days to Close by Borough")
res_df = con.execute("""
    SELECT borough, ROUND(AVG(days_to_close), 1) AS avg_days
    FROM nyc311_clean
    WHERE days_to_close >= 0
    GROUP BY borough
    ORDER BY avg_days DESC
""").df()
st.bar_chart(res_df.set_index("borough"))

# ── Raw Data Explorer ─────────────────────────────────────────
st.header("Explore Clean Data")
selected_borough = st.selectbox(
    "Filter by borough",
    ["All"] + borough_df["borough"].tolist()
)
filter_clause = "" if selected_borough == "All" else f"WHERE borough = '{selected_borough}'"
sample = con.execute(f"""
    SELECT * FROM nyc311_clean {filter_clause}
    LIMIT 500
""").df()
st.dataframe(sample, use_container_width=True)

con.close()