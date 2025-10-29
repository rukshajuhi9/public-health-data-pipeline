import os, logging, json, requests
from dotenv import load_dotenv
load_dotenv()

# Config
ALERT_MODE   = os.getenv("ALERT_MODE", "log")   # "log" | "sendgrid"
ALERT_TO     = os.getenv("ALERT_TO", "")
ALERT_FROM   = os.getenv("ALERT_FROM", ALERT_TO or "alerts@example.com")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "")

def _send_sendgrid(subject: str, body: str) -> bool:
    """Send an alert email via SendGrid REST API."""
    if not SENDGRID_API_KEY or not ALERT_TO:
        logging.error("[ALERT] Missing SENDGRID_API_KEY or ALERT_TO")
        return False
    try:
        payload = {
            "personalizations": [{"to": [{"email": ALERT_TO}]}],
            "from": {"email": ALERT_FROM},
            "subject": subject,
            "content": [{"type": "text/plain", "value": body}]
        }
        r = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={
                "Authorization": f"Bearer {SENDGRID_API_KEY}",
                "Content-Type": "application/json"
            },
            data=json.dumps(payload),
            timeout=15
        )
        if r.status_code in (200, 202):
            logging.info(f"[ALERT:SendGrid] {subject} sent to {ALERT_TO}")
            return True
        logging.error(f"[ALERT:SendGrid FAIL] {r.status_code}: {r.text}")
        return False
    except Exception as e:
        logging.exception(f"[ALERT:SendGrid EXC] {e}")
        return False

def alert(subject: str, body: str):
    """Generic alert wrapper."""
    if ALERT_MODE == "sendgrid" and _send_sendgrid(subject, body):
        return
    logging.error(f"[ALERT:LOG] {subject}\n{body}")
