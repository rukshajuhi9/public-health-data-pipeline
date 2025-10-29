import os, math, time, requests, pandas as pd
from urllib.parse import urlencode
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

CDC_DOMAIN = os.getenv("CDC_DOMAIN", "data.cdc.gov").strip()
DATASET_ID = os.getenv("DATASET_ID", "").strip()
PAGE_SIZE = int(os.getenv("CDC_PAGE_SIZE", "50000"))
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "").strip()

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5433"))
PG_USER = os.getenv("PG_USER", "ph")
PG_PASSWORD = os.getenv("PG_PASSWORD", "ph")
PG_DB = os.getenv("PG_DB", "public_health")

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET", "public_health")
BQ_TABLE = os.getenv("BQ_TABLE", "cdc_state_cases")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

STAGING_TABLE = "stg_cdc_raw"
CLEAN_TABLE = "stg_cdc_clean"

assert DATASET_ID, "Set DATASET_ID in .env"
assert GCP_PROJECT_ID, "Set GCP_PROJECT_ID in .env"

def socrata_count_rows():
    url = f"https://{CDC_DOMAIN}/resource/{DATASET_ID}.json?$select=count(1)"
    headers = {"X-App-Token": SOCRATA_APP_TOKEN} if SOCRATA_APP_TOKEN else {}
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    return int(r.json()[0]["count"])

def fetch_paginated():
    total = socrata_count_rows()
    print(f"Total rows reported by API: {total:,}")
    pages = math.ceil(total / PAGE_SIZE)
    headers = {"X-App-Token": SOCRATA_APP_TOKEN} if SOCRATA_APP_TOKEN else {}
    for i in range(pages):
        offset = i * PAGE_SIZE
        params = {"$limit": PAGE_SIZE, "$offset": offset}
        url = f"https://{CDC_DOMAIN}/resource/{DATASET_ID}.json?{urlencode(params)}"
        print(f"Fetching page {i+1}/{pages} (offset={offset})...")
        r = requests.get(url, headers=headers, timeout=120)
        r.raise_for_status()
        chunk = r.json()
        if not chunk:
            print("No more data returned early; stopping.")
            break
        yield pd.DataFrame(chunk)
        time.sleep(0.5)

def get_pg_engine():
    conn = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(conn, pool_pre_ping=True)

def stage_to_postgres():
    with get_pg_engine().begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {STAGING_TABLE};"))
    total_rows = 0
    for df in fetch_paginated():
        if df.empty: continue
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df.to_sql(STAGING_TABLE, get_pg_engine(), if_exists="append", index=False)
        total_rows += len(df)
        print(f"  -> staged {len(df):,} rows (running total: {total_rows:,})")
    print(f"Staged total rows to Postgres: {total_rows:,}")
    return total_rows

def simple_clean_transform():
    with get_pg_engine().begin() as conn:
        cols = [c[0] for c in conn.execute(text(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{STAGING_TABLE}';"
        )).fetchall()]
        preferred = [c for c in ["submission_date","state","tot_cases","new_case","tot_death","new_death"] if c in cols]
        select_cols = preferred if preferred else cols
        conn.execute(text(f"DROP TABLE IF EXISTS {CLEAN_TABLE};"))
        conn.execute(text(f"CREATE TABLE {CLEAN_TABLE} AS SELECT {', '.join(select_cols)} FROM {STAGING_TABLE};"))
        if "submission_date" in select_cols:
            conn.execute(text(
                f"ALTER TABLE {CLEAN_TABLE} ALTER COLUMN submission_date TYPE date "
                f"USING to_date(submission_date, 'YYYY-MM-DD');"
            ))
        conn.execute(text(
            f"CREATE TABLE tmp AS SELECT DISTINCT * FROM {CLEAN_TABLE};"
            f"DROP TABLE {CLEAN_TABLE};"
            f"ALTER TABLE tmp RENAME TO {CLEAN_TABLE};"
        ))
    print("Created cleaned table in Postgres.")

def load_to_bigquery():
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    try:
        client.get_dataset(f"{GCP_PROJECT_ID}.{BQ_DATASET}")
    except Exception:
        client.create_dataset(bigquery.Dataset(f"{GCP_PROJECT_ID}.{BQ_DATASET}"))
    chunks = pd.read_sql(f"SELECT * FROM {CLEAN_TABLE};", get_pg_engine(), chunksize=100000)
    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    first = True
    for i, df in enumerate(chunks, 1):
        if "submission_date" in df.columns:
            df["submission_date"] = pd.to_datetime(df["submission_date"], errors="coerce").dt.date
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition if first else bigquery.WriteDisposition.WRITE_APPEND
        )
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        tbl = client.get_table(table_id)
        print(f"Loaded chunk {i} to {table_id}. Current rows in BQ: {tbl.num_rows}")
        first = False
    print(f"✅ Finished loading to BigQuery: {table_id}")

def main():
    print("=== Ingest → Postgres staging ===")
    staged = stage_to_postgres()
    if staged == 0:
        print("No rows staged. Exiting.")
        return
    print("=== Clean/Transform in Postgres ===")
    simple_clean_transform()
    print("=== Load to BigQuery ===")
    load_to_bigquery()

if __name__ == "__main__":
    main()
