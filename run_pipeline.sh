#!/bin/bash
# Robust cron/one-shot runner for the pipeline with validation API

set -u  # fail on unset vars (but don't exit on non-zero commands)

cd /Users/rukshajuhi/Public_health_pipeline

# Ensure logs dir exists
mkdir -p logs

# Activate venv
if [ -f ".venv/bin/activate" ]; then
  source .venv/bin/activate
else
  echo "[runner] venv not found: .venv" | tee -a logs/cron_run_$(date +"%Y%m%d").log
fi

# Load .env into process env (orchestrator also loads it)
if [ -f ".env" ]; then
  export $(grep -E '^[A-Z0-9_]+=' .env | xargs)
fi

# Kill anything on 8000 (validation API port)
lsof -ti:8000 | xargs -I {} kill -9 {} 2>/dev/null || true

# Start Validation API using the venv's python (most reliable)
# Log its output separately
( python -m uvicorn validation_api:app --host 127.0.0.1 --port 8000 > /tmp/validation_api.out 2>&1 ) &

# Wait for /health to be ready (max ~30s)
echo "[runner] Waiting for validation API..." | tee -a logs/cron_run_$(date +"%Y%m%d").log
for i in $(seq 1 60); do
  if curl -s http://127.0.0.1:8000/health > /dev/null; then
    echo "[runner] Validation API is up." | tee -a logs/cron_run_$(date +"%Y%m%d").log
    break
  fi
  sleep 0.5
done

# If still not up, log and continue (or exit 1 if you prefer)
if ! curl -s http://127.0.0.1:8000/health > /dev/null; then
  echo "[runner] WARNING: Validation API not responding; check /tmp/validation_api.out" | tee -a logs/cron_run_$(date +"%Y%m%d").log
fi

# Run the orchestrator and append logs per day
echo "[runner] Starting orchestrator..." | tee -a logs/cron_run_$(date +"%Y%m%d").log
python orchestrator.py >> logs/cron_run_$(date +"%Y%m%d").log 2>&1

echo "[runner] Done." | tee -a logs/cron_run_$(date +"%Y%m%d").log
