import time
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from extract import extract
from transform import transform
from load import load

def run_pipeline():
    print("=" * 50)
    print("  NYC 311 ETL Pipeline")
    print("=" * 50)
    start = time.time()

    try:
        print("\n[1/3] EXTRACT")
        print("-" * 30)
        extract()

        print("\n[2/3] TRANSFORM")
        print("-" * 30)
        transform()

        print("\n[3/3] LOAD")
        print("-" * 30)
        load()

        elapsed = round(time.time() - start, 2)
        print("\n" + "=" * 50)
        print(f"  Pipeline completed in {elapsed}s")
        print("=" * 50)

    except Exception as e:
        print(f"\nPipeline failed: {e}")
        raise

if __name__ == "__main__":
    run_pipeline()