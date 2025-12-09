import os

ENRICH_REQUEST_TIMEOUT = 5
ENRICH_MAX_RETRIES = 1
ENRICH_BACKOFF = 0.3
ENRICH_SLEEP_BETWEEN = 0.5
ENRICH_MAX_SEARCH_ATTEMPTS = 1
ENRICH_ONLY_DDG=0

# Thresholds (can be overridden via environment variables TH_AUTO / TH_REVIEW)
TH_AUTO = float(os.getenv("TH_AUTO", "0.90"))
TH_REVIEW = float(os.getenv("TH_REVIEW", "0.60"))