import unittest, requests, time
API = "http://127.0.0.1:8000"

class TestValidationAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        for _ in range(10):
            try:
                r = requests.get(f"{API}/health", timeout=2)
                if r.ok: return
            except Exception:
                time.sleep(0.5)
        raise RuntimeError("Validation API not reachable on /health")

    def test_health(self):
        r = requests.get(f"{API}/health", timeout=5)
        self.assertTrue(r.ok)

    def test_validate_shape(self):
        r = requests.get(f"{API}/validate", timeout=60).json()
        self.assertIn("overall_passed", r)
        self.assertIn("checks", r)
        self.assertGreaterEqual(len(r["checks"]), 10)

    def test_core_checks(self):
        r = requests.get(f"{API}/validate", timeout=60).json()
        need = {"non_empty","res_state_format","case_month_format","no_future_months",
                "sex_domain","current_status_domain","hosp_yn_domain","icu_yn_domain","death_yn_domain"}
        have = {c["name"] for c in r["checks"]}
        self.assertTrue(need.issubset(have))

if __name__ == "__main__":
    unittest.main(verbosity=2)
