import os, time, math, requests, pandas as pd
from urllib.parse import urlencode
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

CDC_DOMAIN = os.getenv("CDC_DOMAIN", "data.cdc.gov").strip()
DATASET_ID = os.getenv("DATASET_ID", "").strip()       
PAGE_SIZE  = int(os.getenv("CDC_PAGE_SIZE", "50000"))
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "").strip()

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5433"))
PG_USER = os.getenv("PG_USER", "ph")
PG_PASSWORD = os.getenv("PG_PASSWORD", "ph")
PG_DB = os.getenv("PG_DB", "public_health")

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET", "public_health")
BQ_TABLE   = os.getenv("BQ_TABLE",   "cdc_state_cases")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

STAGING_TABLE = "stg_cdc_raw"
CLEAN_TABLE   = "stg_cdc_clean"

assert DATASET_ID, "Set DATASET_ID in .env (e.g., n8mc-b4w4)"
assert GCP_PROJECT_ID, "Set GCP_PROJECT_ID in .env"

MAX_ROWS = int(os.getenv('MAX_ROWS','0'))

def fetch_paginated():
    """
    Stream the dataset page-by-page until an empty page is returned.
    Avoids $select=count(1) so it works even when count isn't available.
    """
    headers = {}
    if SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = SOCRATA_APP_TOKEN

    offset = 0
    page = 1
    total_rows = 0
    while True:
        params = {"$limit": PAGE_SIZE, "$offset": offset}
        url = f"https://{CDC_DOMAIN}/resource/{DATASET_ID}.json?{urlencode(params)}"
        print(f"Fetching page {page} (offset={offset}) …")
        r = requests.get(url, headers=headers, timeout=120)
        # If the dataset ID is wrong or restricted, raise now with helpful message
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            print(f"HTTP error from Socrata API: {e}\nURL: {url}\nResponse: {r.text[:400]} …")
            raise

        rows = r.json()
        if not rows:
            print("No more data; stopping pagination.")


        df = pd.DataFrame(rows)
        total_rows += len(df)
        print(f"  -> got {len(df):,} rows (running total {total_rows:,})")
        if MAX_ROWS and total_rows >= MAX_ROWS:
            over = total_rows - MAX_ROWS
            if over > 0:
                df = df.iloc[:-over]
            yield df
            print(f'Reached MAX_ROWS={MAX_ROWS}. Stopping pagination.')
            break
        if MAX_ROWS and total_rows >= MAX_ROWS:
            over = total_rows - MAX_ROWS
            if over > 0:
                df = df.iloc[:-over]
            yield df

        yield df

        yield df

        offset += PAGE_SIZE
        page += 1
        time.sleep(0.5)  # be polite

def get_pg_engine():
    conn = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(conn, pool_pre_ping=True)

def stage_to_postgres():
    with get_pg_engine().begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {STAGING_TABLE};"))

    total_rows = 0
    for df in fetch_paginated():
        if df.empty:
            continue
        # normalize column names
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df.to_sql(STAGING_TABLE, get_pg_engine(), if_exists="append", index=False)
        total_rows += len(df)
        print(f"  -> staged {len(df):,} rows (running total: {total_rows:,})")

    print(f"Staged total rows to Postgres: {total_rows:,}")
    return total_rows


def simple_clean_transform():
    with get_pg_engine().begin() as conn:
        # read columns present
        cols = [c[0] for c in conn.execute(text(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{STAGING_TABLE}';"
        )).fetchall()]
        # choose selection columns
        sel = ", ".join(cols)

        conn.execute(text(f"DROP TABLE IF EXISTS {CLEAN_TABLE};"))

        # Create a normalized/validated cleaned table
        # Handle missing columns gracefully with COALESCE(NULL)
        conn.execute(text(f"""
            CREATE TABLE {CLEAN_TABLE} AS
            SELECT
                -- dates: keep YYYY-MM style month fields and drop future months if present
                CASE
                    WHEN EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = '{STAGING_TABLE}' AND column_name='case_month')
                    THEN NULLIF(case_month, '') ELSE NULL END AS case_month,

                -- state: uppercase 2-letter if present
                CASE
                    WHEN EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = '{STAGING_TABLE}' AND column_name='res_state')
                    THEN UPPER(res_state) ELSE NULL END AS res_state,

                -- keep common fields if they exist
                {(", ".join([c for c in ['age_group','sex','current_status','hosp_yn','icu_yn','death_yn','race','ethnicity','res_county','county_fips_code','state_fips_code','case_positive_specimen','process','exposure_yn'] if c in cols]))}
            FROM {STAGING_TABLE}
        """))

        # Remove future months if case_month looks like YYYY-MM
        if 'case_month' in cols:
            conn.execute(text(f"""
                DELETE FROM {CLEAN_TABLE}
                WHERE case_month IS NOT NULL
                  AND case_month ~ '^[0-9]{{4}}-[0-1][0-9]$'
                  AND case_month > to_char(CURRENT_DATE, 'YYYY-MM');
            """))

        # Standardize domains
        if 'sex' in cols:
            conn.execute(text(f"""
                UPDATE {CLEAN_TABLE}
                SET sex = CASE UPPER(COALESCE(sex,''))
                    WHEN 'MALE' THEN 'Male'
                    WHEN 'FEMALE' THEN 'Female'
                    WHEN 'UNKNOWN' THEN 'Unknown'
                    WHEN 'MISSING' THEN 'Missing'
                    ELSE 'Unknown' END;
            """))

        if 'current_status' in cols:
            conn.execute(text(f"""
                UPDATE {CLEAN_TABLE}
                SET current_status = CASE
                    WHEN current_status IN ('Confirmed Case','Probable Case') THEN current_status
                    ELSE 'Probable Case' END;
            """))

        for col in ['hosp_yn','icu_yn','death_yn']:
            if col in cols:
                conn.execute(text(f"""
                    UPDATE {CLEAN_TABLE}
                    SET {col} = CASE UPPER(COALESCE({col}, ''))
                        WHEN 'YES' THEN 'Yes'
                        WHEN 'NO' THEN 'No'
                        WHEN 'UNKNOWN' THEN 'Unknown'
                        WHEN 'MISSING' THEN 'Missing'
                        ELSE 'Unknown' END;
                """))

        # Optionally filter state to valid 2-letter codes if present
        if 'res_state' in cols:
            conn.execute(text(f"""
                DELETE FROM {CLEAN_TABLE}
                WHERE res_state IS NOT NULL AND res_state !~ '^[A-Z]{{2}}$';
            """))

        # De-dupe
        conn.execute(text(
            f"CREATE TABLE tmp AS SELECT DISTINCT * FROM {CLEAN_TABLE};"
            f"DROP TABLE {CLEAN_TABLE};"
            f"ALTER TABLE tmp RENAME TO {CLEAN_TABLE};"
        ))
    print("Created cleaned table in Postgres (normalized for validation).")

def load_to_bigquery():
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    # ensure dataset exists
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
    print(f"Using CDC dataset: {CDC_DOMAIN}/resource/{DATASET_ID}.json  (page size {PAGE_SIZE})")
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
