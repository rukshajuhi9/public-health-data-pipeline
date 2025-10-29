import os
import datetime as dt
from typing import List, Any
from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5433"))
PG_USER = os.getenv("PG_USER", "ph")
PG_PASSWORD = os.getenv("PG_PASSWORD", "ph")
PG_DB = os.getenv("PG_DB", "public_health")
CLEAN_TABLE = "stg_cdc_clean"

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True,
)

app = FastAPI(title="Validation API", version="1.0")

class CheckResult(BaseModel):
    name: str
    passed: bool
    details: str = ""

class ValidationResponse(BaseModel):
    overall_passed: bool
    checks: List[CheckResult]

def fetch_val(query: str) -> Any:
    with engine.begin() as conn:
        return conn.execute(text(query)).scalar()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/validate", response_model=ValidationResponse)
def validate():
    checks: List[CheckResult] = []

    # 1) table exists
    exists = fetch_val(f"""
      SELECT COUNT(*)>0 FROM information_schema.tables
      WHERE table_name='{CLEAN_TABLE}';
    """)
    checks.append(CheckResult(name="table_exists", passed=bool(exists), details=CLEAN_TABLE))

    # 2) row count > 0
    cnt = fetch_val(f"SELECT COUNT(*) FROM {CLEAN_TABLE};") if exists else 0
    checks.append(CheckResult(name="non_empty", passed=(cnt and cnt > 0), details=f"rows={cnt}"))

    # 3) case_month format YYYY-MM (dataset n8mc-b4w4)
    fmt_ok = fetch_val(f"""
      SELECT COUNT(*) FROM {CLEAN_TABLE}
      WHERE case_month IS NOT NULL AND case_month ~ '^[0-9]{{4}}-[0-1][0-9]$';
    """) or 0
    checks.append(CheckResult(name="case_month_format", passed=fmt_ok > 0, details=f"matching_rows={fmt_ok}"))

    # 4) no future months
    yyyy_mm_today = dt.date.today().strftime("%Y-%m")
    future = fetch_val(f"""
      SELECT COUNT(*) FROM {CLEAN_TABLE}
      WHERE case_month > '{yyyy_mm_today}';
    """) or 0
    checks.append(CheckResult(name="no_future_months", passed=future == 0, details=f"future_rows={future}"))

    # 5) res_state looks like 2-letter A–Z
    state_bad = fetch_val(f"""
      SELECT COUNT(*) FROM {CLEAN_TABLE}
      WHERE res_state IS NOT NULL AND res_state !~ '^[A-Z]{{2}}$';
    """) or 0
    checks.append(CheckResult(name="res_state_format", passed=state_bad == 0, details=f"bad_rows={state_bad}"))

    # 6) allowed sex values
    sex_bad = fetch_val(f"""
      SELECT COUNT(*) FROM {CLEAN_TABLE}
      WHERE sex IS NOT NULL AND sex NOT IN ('Male','Female','Unknown','Missing');
    """) or 0
    checks.append(CheckResult(name="sex_domain", passed=sex_bad == 0, details=f"bad_rows={sex_bad}"))

    # 7) allowed current_status values
    status_bad = fetch_val(f"""
      SELECT COUNT(*) FROM {CLEAN_TABLE}
      WHERE current_status IS NOT NULL AND current_status NOT IN ('Confirmed Case','Probable Case');
    """) or 0
    checks.append(CheckResult(name="current_status_domain", passed=status_bad == 0, details=f"bad_rows={status_bad}"))

    # 8–10) Y/N flags & null/state coverage
    def yn_bad(col: str) -> int:
        return fetch_val(f"""
          SELECT COUNT(*) FROM {CLEAN_TABLE}
          WHERE {col} IS NOT NULL AND {col} NOT IN ('Yes','No','Unknown','Missing');
        """) or 0

    hosp_bad = yn_bad("hosp_yn")
    icu_bad  = yn_bad("icu_yn")
    death_bad= yn_bad("death_yn")
    checks.append(CheckResult(name="hosp_yn_domain",  passed=hosp_bad  == 0, details=f"bad_rows={hosp_bad}"))
    checks.append(CheckResult(name="icu_yn_domain",   passed=icu_bad   == 0, details=f"bad_rows={icu_bad}"))
    checks.append(CheckResult(name="death_yn_domain", passed=death_bad == 0, details=f"bad_rows={death_bad}"))

    null_states = fetch_val(f"SELECT COUNT(*) FROM {CLEAN_TABLE} WHERE res_state IS NULL;") or 0
    pct_null = (null_states / cnt * 100.0) if cnt else 100.0
    checks.append(CheckResult(name="res_state_null_rate", passed=(pct_null <= 20.0), details=f"null_pct={pct_null:.2f}%"))

    months = fetch_val(f"SELECT COUNT(DISTINCT case_month) FROM {CLEAN_TABLE};") or 0
    checks.append(CheckResult(name="min_distinct_months", passed=(months >= 2), details=f"months={months}"))

    overall = all(c.passed for c in checks)
    return ValidationResponse(overall_passed=overall, checks=checks)
