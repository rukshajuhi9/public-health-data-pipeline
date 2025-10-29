import os, sys, time, logging, pathlib, requests
from datetime import datetime
from dotenv import load_dotenv
from pipeline import stage_to_postgres, simple_clean_transform, load_to_bigquery

load_dotenv()
LOG_DIR = pathlib.Path("logs"); LOG_DIR.mkdir(exist_ok=True)
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_PATH = LOG_DIR / f"pipeline_{run_id}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler(sys.stdout)],
)

API_URL = os.getenv("VALIDATION_API_URL", "http://127.0.0.1:8000")
VALIDATION_ENDPOINT = f"{API_URL}/validate"

def log_step(step, status="START"):
    logging.info(f"{step} | {status}")

def main():
    SKIP_INGEST = os.getenv('SKIP_INGEST', '0') == '1'
    start = time.time()
    try:
        if not SKIP_INGEST:
            log_step("INGEST")
            staged = stage_to_postgres()
            log_step("INGEST", f"OK (rows={staged})")
        else:
            logging.info("INGEST | SKIPPED")

        log_step("TRANSFORM")
        simple_clean_transform()
        log_step("TRANSFORM", "OK")

        log_step("VALIDATION", "CALL")
        r = requests.get(VALIDATION_ENDPOINT, timeout=180)
        r.raise_for_status()
        res = r.json()
        overall = res.get("overall_passed", False)
        logging.info(f"VALIDATION overall={overall} checks={len(res.get('checks', []))}")
        if not overall:
            (LOG_DIR / f"validation_{run_id}.json").write_text(r.text)
            raise RuntimeError("Validation failed. Aborting load.")

        log_step("LOAD_TO_BQ")
        load_to_bigquery()
        log_step("LOAD_TO_BQ", "OK")

        duration = round(time.time() - start, 2)
        logging.info(f"RUN_STATUS=SUCCESS duration_sec={duration} log={LOG_PATH}")
    except Exception as e:
        duration = round(time.time() - start, 2)
        logging.exception(f"RUN_STATUS=FAIL duration_sec={duration} error={e} log={LOG_PATH}")
        sys.exit(1)

if __name__ == "__main__":
    main()
