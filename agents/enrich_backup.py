"""
Enrichment Agent - deterministic, explainable scrapers + NPI fallback.

Inputs:
  - data/output/validated.json

Outputs:
  - data/output/enriched.json

Each enriched field contains:
  - value
  - confidence (0..1)
  - source (URL or 'search:<query>' tag)

Behavior:
  - Try to find provider/practice site via DuckDuckGo HTML search and scrape.
  - If no site or fetch fails, call NPI Registry fallback (fetch_provider_by_npi).
  - Keep logic deterministic, explainable and safe (polite throttling + simple caches).
"""
from typing import Dict, List, Optional, Tuple
import json
import os
import re
import time
import urllib.parse
import requests
from bs4 import BeautifulSoup
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
load_dotenv()
# Ensure NPI fallback import is explicit and deterministic
try:
    from services.npi_services import fetch_provider_by_npi
except Exception:
    fetch_provider_by_npi = None  # graceful degrade if service missing

# add import for Google Places helper
try:
    from services.google_maps_services import find_place
except Exception:
    find_place = None

INPUT_JSON = "data/output/validated.json"
OUTPUT_JSON = "data/output/enriched.json"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

# --- Configurable fetch / retry settings (can be overridden via env vars) ---
REQUEST_TIMEOUT = float(os.getenv("ENRICH_REQUEST_TIMEOUT", "5"))          # seconds per request
MAX_RETRIES = int(os.getenv("ENRICH_MAX_RETRIES", "1"))                  # urllib3 retry total
BACKOFF_FACTOR = float(os.getenv("ENRICH_BACKOFF", "0.3"))               # exponential backoff factor
SLEEP_BETWEEN = float(os.getenv("ENRICH_SLEEP_BETWEEN", "0.5"))          # polite throttle between successful fetches
MAX_SEARCH_ATTEMPTS = int(os.getenv("ENRICH_MAX_SEARCH_ATTEMPTS", "1"))  # total different search/fetch attempts per record
# create a configured requests.Session with urllib3 Retry
_SESSION = requests.Session()
_retry_strategy = Retry(
    total=MAX_RETRIES,
    status_forcelist=[429, 500, 502, 503, 504],
    backoff_factor=BACKOFF_FACTOR,
    allowed_methods=frozenset(["GET", "HEAD"])
)
_adapter = HTTPAdapter(max_retries=_retry_strategy)
_SESSION.mount("https://", _adapter)
_SESSION.mount("http://", _adapter)

# adjust the sleep constant (used below) to the configurable value
# override the module-level constant used elsewhere
# ...existing SLEEP_BETWEEN name replaced by the env-configured value above ...

HEADERS = {"User-Agent": USER_AGENT}

SPECIALTY_KEYWORDS = {
    "Cardiology": ["cardio", "cardiology", "heart"],
    "Pediatrics": ["pediatr", "children", "kids"],
    "Dentist": ["dentist", "dental", "dds", "dmd", "orthodont"],
    "Ophthalmology": ["ophthalmology", "ophthalmologist", "lasik", "eye"],
    "Optometry": ["optomet", "optometry"],
    "Pharmacy": ["pharmacy", "pharmac"],
    "Chiropractic": ["chiropractic", "chiropractor"],
    "Gynecology": ["gynecology", "obstetric", "ob/gyn", "women's health"],
    "Surgery": ["surgery", "surgeon", "surgical"],
    "Dermatology": ["dermatology", "dermato", "skin"],
    "Orthopedics": ["orthopedic", "ortho", "bone", "joint"],
    "Neurology": ["neurology", "neuro", "brain"],
    "ENT": ["ear nose throat", "ent", "otolaryngology"],
    "Internal Medicine": ["internal medicine", "internist"],
    "Family Medicine": ["family medicine", "primary care", "general practice"],
    "Psychiatry": ["psychiatry", "psychiatrist", "mental health"],
    "Radiology": ["radiology", "imaging"],
    "Oncology": ["oncology", "oncologist", "cancer"],
}

DEGREE_RE = re.compile(
    r"\b(MD|M\.D\.|DO|D\.O\.|PhD|Ph\.D\.|DDS|DMD|RN|BSN|MSN|NP|APRN|MBA|MPH|PA-C|PA)\b",
    re.I,
)
GRAD_FROM_RE = re.compile(
    r"(?:graduated from|received (?:his|her|their) (?:medical )?degree from|attended|alumnus of|alumna of|studied at)\s+([A-Z][A-Za-z0-9&,\.\- ]{3,140})",
    re.I,
)
TRAINING_RE = re.compile(
    r"(?:residency|fellowship|internship|trained at|completed (?:a |the )?(?:residency|fellowship|internship) at)[:\s\-]+([A-Z][A-Za-z0-9&,\.\- ]{3,140})",
    re.I,
)

SERVICES_SECTION_KEYWORDS = [
    "service",
    "what we offer",
    "treatment",
    "procedures",
    "services include",
    "we specialize in",
    "our services",
    "care we provide",
    "treatments offered",
]

AFFILIATION_PATTERNS = [
    r"(?:affiliated with|affiliated to|affiliates with|affiliation|member of|part of|partners with|associated with)[:\s\-]+([A-Z][A-Za-z0-9 &,\.\-]{3,140})",
    r"(?:staff (?:member|physician|doctor) at|on staff at|practicing at|practices at)[:\s\-]+([A-Z][A-Za-z0-9 &,\.\-]{3,140})",
]

# simple in-memory caches for deterministic runs
_SEARCH_CACHE: Dict[str, Optional[str]] = {}
_FETCH_CACHE: Dict[str, Optional[Tuple[str, str]]] = {}

# new: list of known directory domains to try
KNOWN_DIRECTORIES = [
    "healthgrades.com",
    "vitals.com",
    "webmd.com",
    "zocdoc.com",
    "rateMDs.com",
    "practo.com",
    "docfinder.com",
]

# add module logger
logger = logging.getLogger(__name__)

# Add env flag to force DuckDuckGo-only search
ENRICH_ONLY_DDG = os.getenv("ENRICH_ONLY_DDG", "0") == "1"


def http_get(url: str) -> Tuple[Optional[str], Optional[str]]:
    """Fetch URL with caching; use configured Session + retry/backoff. Returns (text, final_url) or (None, None)."""
    if not url:
        logger.debug("http_get called with empty url")
        return None, None
    if url in _FETCH_CACHE:
        logger.debug("http_get cache hit for %s", url)
        return _FETCH_CACHE[url]  # type: ignore
    try:
        logger.info("Fetching URL: %s (timeout=%s)", url, REQUEST_TIMEOUT)
        r = _SESSION.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        text = r.text
        final = r.url
        _FETCH_CACHE[url] = (text, final)
        # polite throttle only after a successful fetch
        try:
            time.sleep(SLEEP_BETWEEN)
        except Exception:
            pass
        logger.debug("Fetched %s (final: %s)", url, final)
        return text, final
    except Exception as exc:
        logger.warning("http_get failed for %s: %s", url, exc)
        _FETCH_CACHE[url] = (None, None)
        return None, None


def ddg_search_first_site(query: str) -> Optional[str]:
    """Use DuckDuckGo HTML version to deterministically return the first external result URL."""
    q = (query or "").strip()
    if not q:
        return None
    if q in _SEARCH_CACHE:
        return _SEARCH_CACHE[q]
    search_url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote_plus(q)}"
    html, _ = http_get(search_url)
    if html:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http") and "duckduckgo" not in href:
                _SEARCH_CACHE[q] = href
                return href
    _SEARCH_CACHE[q] = None
    return None


def bing_search_first_site(query: str) -> Optional[str]:
    """Deterministic Bing HTML scrape fallback (best-effort)."""
    q = (query or "").strip()
    if not q:
        return None
    key = "bing:" + q
    if key in _SEARCH_CACHE:
        return _SEARCH_CACHE[key]
    search_url = f"https://www.bing.com/search?q={urllib.parse.quote_plus(q)}"
    html, _ = http_get(search_url)
    if not html:
        _SEARCH_CACHE[key] = None
        return None
    soup = BeautifulSoup(html, "html.parser")
    # Bing result links often appear in <li class="b_algo"><h2><a href=...>
    for li in soup.find_all("li", {"class": "b_algo"}):
        a = li.find("a", href=True)
        if a:
            href = a["href"]
            if href.startswith("http"):
                _SEARCH_CACHE[key] = href
                return href
    # fallback to any first http anchor
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("http") and "bing.com" not in href:
            _SEARCH_CACHE[key] = href
            return href
    _SEARCH_CACHE[key] = None
    return None


def _guess_domains_from_name(name: str, city: Optional[str] = None) -> List[str]:
    """Generate likely candidate domains from name and optional city (more variants)."""
    name = (name or "").strip().lower()
    if not name:
        return []
    # remove common stop words (Dr, clinic, physician, llc)
    cleaned = re.sub(r"\b(dr|doctor|clinic|physician|llc|inc|pc|md|pa|pa-c)\b", "", name)
    slug = re.sub(r"[^a-z0-9]+", "-", cleaned).strip("-")
    candidates = []
    if slug:
        candidates.extend([
            f"https://{slug}.com",
            f"https://www.{slug}.com",
            f"https://{slug}.org",
            f"https://www.{slug}.org",
            f"https://{slug}{'.' + (city or '').lower() + '.com' if city else ''}".replace("..", ".")
        ])
    # also try common clinic prefixes/suffixes
    if city:
        cslug = re.sub(r"[^a-z0-9]+", "-", (city or "").strip().lower())
        if slug and cslug:
            candidates.append(f"https://{slug}-{cslug}.com")
            candidates.append(f"https://{slug}{cslug}.com")
    # dedupe and limit
    seen = []
    for c in candidates:
        if c and c not in seen:
            seen.append(c)
    return seen[:8]


def _search_known_directories(name: str, city: str) -> Optional[str]:
    """Try site-specific DuckDuckGo queries for known provider directories."""
    base_query = f"{name} {city}"
    for domain in KNOWN_DIRECTORIES:
        q = f"site:{domain} {base_query}"
        url = ddg_search_first_site(q)
        if url:
            return url
    return None


def _choose_site(name: str, address: str) -> Tuple[Optional[str], str]:
    """
    Determine a site URL for the provider.
    When ENRICH_ONLY_DDG is true, only use DuckDuckGo (then guessed domains).
    If DuckDuckGo yields nothing or is unreachable, try Google Places (if API key present),
    then guessed domain probes (short timeout).
    """
    name = (name or "").strip()
    addr = (address or "").strip()
    city = ""
    if addr:
        parts = [p.strip() for p in addr.split(",") if p.strip()]
        if len(parts) >= 2:
            city = parts[-2]
    query = f"{name} {city} practice website".strip()

    attempts = 0

    # 1) DuckDuckGo first (if enabled)
    if attempts < MAX_SEARCH_ATTEMPTS:
        attempts += 1
        site = ddg_search_first_site(query)
        if site:
            logger.info("Site chosen via DuckDuckGo for '%s': %s", query, site)
            return site, f"ddg:{query}"

    # 2) If DDG failed and Google Places available, try find_place -> website (fast when API key set)
    if find_place and attempts < MAX_SEARCH_ATTEMPTS:
        attempts += 1
        try:
            place = find_place(name, city)
            if place:
                website = place.get("website")
                if website:
                    logger.info("Site chosen via Google Places for '%s': %s", name, website)
                    return website, "google_places"
                # fallback: if place has phone/address, prefer place.url if present
                if place.get("address") or place.get("phone"):
                    # we may still not have a website, but we can prefer place detail page url if returned
                    url = place.get("website")
                    if url:
                        logger.info("Using place.url for '%s': %s", name, url)
                        return url, "google_places"
        except Exception as exc:
            logger.debug("Google Places lookup failed for %s: %s", name, exc)

    # 3) Guessed domains (use short HEAD probes, low timeout)
    for cand in _guess_domains_from_name(name, city):
        if attempts >= MAX_SEARCH_ATTEMPTS:
            break
        attempts += 1
        try:
            logger.debug("Probing guessed domain: %s", cand)
            r = _SESSION.head(cand, headers=HEADERS, timeout=min(REQUEST_TIMEOUT, 2), allow_redirects=True)
            if 200 <= r.status_code < 400:
                logger.info("Guessed domain reachable for '%s': %s", name, cand)
                return cand, f"guessed:{cand}"
        except Exception as exc:
            logger.debug("Guessed domain probe failed for %s: %s", cand, exc)
            continue

    logger.debug("No site found for '%s' -> fallback to search tag (attempts=%d)", query, attempts)
    return None, f"search:{query}"


def _parse_jsonld(soup: BeautifulSoup) -> Dict[str, List[str]]:
    """
    Extract structured data from JSON-LD blocks (schema.org). Returns possible keys:
      - specialties, affiliations, services, education
    """
    out = {"specialties": [], "affiliations": [], "services": [], "education": []}
    scripts = soup.find_all("script", {"type": "application/ld+json"})
    for s in scripts:
        try:
            data = json.loads(s.string or "{}")
        except Exception:
            continue
        # data can be list or dict
        items = data if isinstance(data, list) else [data]
        for it in items:
            # medicalSpecialty or specialty
            ms = it.get("medicalSpecialty") or it.get("specialty")
            if isinstance(ms, str):
                out["specialties"].append(ms)
            elif isinstance(ms, dict):
                v = ms.get("name") or ms.get("value")
                if v:
                    out["specialties"].append(v)
            # affiliation / memberOf / department
            for key in ("affiliation", "memberOf", "department", "workLocation", "hospitalAffiliation"):
                val = it.get(key)
                if isinstance(val, str):
                    out["affiliations"].append(val)
                elif isinstance(val, dict):
                    name = val.get("name")
                    if name:
                        out["affiliations"].append(name)
            # makes/services offered
            if it.get("makesOffer") or it.get("hasOfferCatalog"):
                # try to extract possible service names
                offers = it.get("makesOffer") or it.get("hasOfferCatalog")
                if isinstance(offers, dict):
                    name = offers.get("name")
                    if name:
                        out["services"].append(name)
            # alumni / alumniOf
            if it.get("alumni") or it.get("alumniOf"):
                al = it.get("alumni") or it.get("alumniOf")
                if isinstance(al, str):
                    out["education"].append(al)
                elif isinstance(al, dict):
                    name = al.get("name")
                    if name:
                        out["education"].append(name)
    # dedupe
    for k in out:
        seen = []
        for v in out[k]:
            if v and v not in seen:
                seen.append(v)
        out[k] = seen
    return out


def extract_education(soup: BeautifulSoup, text: str) -> Tuple[Optional[str], float]:
    """
    Deterministic extraction for education/degree.
    Returns (value, confidence).
    """
    # Degree token presence -> high confidence snippet
    m = DEGREE_RE.search(text)
    if m:
        start = max(0, m.start() - 60)
        end = min(len(text), m.end() + 80)
        snippet = text[start:end].strip()
        return snippet, 0.85

    # 'graduated from' patterns -> school name
    m2 = GRAD_FROM_RE.search(text)
    if m2:
        school = m2.group(1).strip()
        return school, 0.8

    # training/residency patterns
    m3 = TRAINING_RE.search(text)
    if m3:
        return m3.group(1).strip(), 0.7

    return None, 0.05


def extract_specialty(soup: BeautifulSoup, text: str, validated_specialty: str) -> Tuple[Optional[str], float]:
    """
    Determine specialty by (priority):
      1) validated_specialty if meaningful
      2) headings and page title
      3) keyword presence in body
    Returns canonical specialty and confidence.
    """
    if validated_specialty and isinstance(validated_specialty, str) and validated_specialty.strip().lower() != "unknown":
        return validated_specialty.strip(), 0.6

    # check title and headings first
    for tag in ("title", "h1", "h2", "h3", "strong", "b"):
        for el in soup.find_all(tag):
            txt = (el.get_text(" ", strip=True) or "").lower()
            for canon, kwlist in SPECIALTY_KEYWORDS.items():
                for kw in kwlist:
                    if kw in txt:
                        return canon, 0.9

    # search body text
    lower = text.lower()
    for canon, kwlist in SPECIALTY_KEYWORDS.items():
        for kw in kwlist:
            if kw in lower:
                return canon, 0.75

    return None, 0.15


def extract_services(soup: BeautifulSoup, text: str) -> Tuple[List[str], float]:
    """
    Try to extract a list of services. Look for section headings then lists or paragraph content.
    Returns (services_list, confidence).
    """
    services: List[str] = []

    # Look for headings indicating services
    for h in soup.find_all(["h2", "h3", "h4"]):
        htxt = (h.get_text(" ", strip=True) or "").lower()
        if any(k in htxt for k in SERVICES_SECTION_KEYWORDS):
            # look for following ul/ol
            sib = h.find_next_sibling()
            if sib:
                if sib.name in ("ul", "ol"):
                    for li in sib.find_all("li"):
                        t = li.get_text(" ", strip=True)
                        if 4 < len(t) < 200:
                            services.append(t)
                else:
                    para = sib.get_text(" ", strip=True)
                    if para:
                        parts = re.split(r"\.|;|\n|\u2022|\u2023|-", para)
                        for p in parts:
                            p = p.strip()
                            if 4 < len(p) < 200:
                                services.append(p)

    # fallback: inline "services include" pattern
    if not services:
        m = re.search(r"services (?:include|offered|provided)[:\s\-]+([A-Za-z0-9\.,; &\-\/]{10,400})", text, re.I)
        if m:
            parts = re.split(r",|;|\.", m.group(1))
            services = [p.strip() for p in parts if 4 < len(p.strip()) < 200]

    # dedupe and limit
    seen = []
    for s in services:
        if s not in seen:
            seen.append(s)
    services = seen[:10]
    conf = 0.8 if services else 0.12
    return services, conf


def _is_likely_hospital(text: str) -> bool:
    """Heuristic to check if page looks like hospital/site page."""
    t = (text or "").lower()
    indicators = ["hospital", "medical center", "clinic", "health system", "medical group", "university hospital"]
    return any(ind in t for ind in indicators)


def extract_affiliations(soup: BeautifulSoup, text: str) -> Tuple[List[str], float]:
    """
    Extract affiliations using known phrases and heuristics.
    Returns (affiliations_list, confidence).
    """
    found = []
    lower = text.lower()

    # simple heuristic: known hospital tokens
    if _is_likely_hospital(text):
        # try to find capitalized organization names via regex patterns
        m = re.findall(r"[A-Z][A-Za-z0-9&\.\- ]+(?:Hospital|Medical Center|Health System|Health|Clinic|University|Center)", text)
        for x in m:
            x = x.strip()
            if len(x) > 4 and x not in found:
                found.append(x)

    # explicit affiliation phrase patterns
    for pat in AFFILIATION_PATTERNS:
        for m in re.finditer(pat, text, re.I):
            grp = m.group(1).strip()
            if grp and grp not in found:
                found.append(grp)

    conf = 0.85 if found else 0.12
    return found[:8], conf


def enrich_record(rec: Dict) -> Dict:
    """
    Enrich a single validated record. Returns record with rec['enrichment'] added.
    Each field: {value, confidence, source}
    """
    name = rec.get("name") or ""
    address = rec.get("address") or ""
    logger.info("Enriching provider: %s | %s", rec.get("provider_id") or "UNKNOWN", name)

    site_url, source_tag = _choose_site(name, address)
    logger.debug("site_url=%s source_tag=%s", site_url, source_tag)

    # initialize low-confidence defaults
    enrichment = {
        "education": {"value": None, "confidence": 0.0, "source": None},
        "specialty": {"value": None, "confidence": 0.0, "source": None},
        "services": {"value": [], "confidence": 0.0, "source": None},
        "affiliations": {"value": [], "confidence": 0.0, "source": None},
    }

    # If no site found, attempt NPI fallback (preferred) before low-confidence defaults
    if not site_url:
        logger.info("No site found; attempting NPI fallback for provider %s", rec.get("provider_id"))
        npi_val = rec.get("npi")
        if npi_val and fetch_provider_by_npi:
            try:
                npi_info = fetch_provider_by_npi(str(npi_val))
            except Exception:
                npi_info = None
            if npi_info:
                # Map taxonomy / basic fields from NPI to enrichment
                # specialty: use taxonomy_description or first tax entry
                npi_spec = npi_info.get("taxonomy_description") or npi_info.get("primary_taxonomy") or rec.get("specialty")
                if npi_spec:
                    enrichment["specialty"] = {"value": npi_spec, "confidence": 0.8, "source": "NPI Registry"}
                else:
                    enrichment["specialty"] = {"value": rec.get("specialty"), "confidence": 0.45, "source": "NPI Registry"}

                # affiliations: map practice addresses -> simple affiliation strings
                addrs = npi_info.get("addresses") or npi_info.get("practice_addresses") or []
                affs = []
                for a in addrs:
                    parts = []
                    if isinstance(a, dict):
                        parts = [a.get(k) for k in ("address_1", "city", "state") if a.get(k)]
                    else:
                        # fallback if addresses are strings
                        parts = [str(a)]
                    aff = ", ".join([p for p in parts if p])
                    if aff and aff not in affs:
                        affs.append(aff)
                enrichment["affiliations"] = {"value": affs[:6], "confidence": 0.7 if affs else 0.12, "source": "NPI Registry"}

                # education: NPI may have basic credential tokens (we use degree token heuristic)
                cred = npi_info.get("credential") or npi_info.get("credentials") or npi_info.get("degree")
                if cred:
                    enrichment["education"] = {"value": cred, "confidence": 0.7, "source": "NPI Registry"}
                else:
                    enrichment["education"] = {"value": None, "confidence": 0.05, "source": "NPI Registry"}

                # services: NPI rarely has services — keep empty with low confidence
                enrichment["services"] = {"value": [], "confidence": 0.12, "source": "NPI Registry"}
                rec["enrichment"] = enrichment
                return rec

        # fallback: no site and no usable NPI -> low-confidence carryforward
        validated_spec = rec.get("specialty")
        if validated_spec and isinstance(validated_spec, str) and validated_spec.strip().lower() != "unknown":
            enrichment["specialty"] = {"value": validated_spec.strip(), "confidence": 0.45, "source": source_tag}
        for k in ("education", "services", "affiliations"):
            enrichment[k] = {"value": enrichment[k]["value"], "confidence": 0.05, "source": source_tag}
        rec["enrichment"] = enrichment
        return rec

    # If we found a site, try to fetch and scrape
    html, final_url = http_get(site_url)
    if not html:
        logger.warning("Failed to fetch site %s for provider %s", site_url, rec.get("provider_id"))
        # site fetch failed -> try NPI fallback as above
        npi_val = rec.get("npi")
        if npi_val and fetch_provider_by_npi:
            try:
                npi_info = fetch_provider_by_npi(str(npi_val))
            except Exception:
                npi_info = None
            if npi_info:
                npi_spec = npi_info.get("taxonomy_description") or npi_info.get("primary_taxonomy") or rec.get("specialty")
                if npi_spec:
                    enrichment["specialty"] = {"value": npi_spec, "confidence": 0.8, "source": "NPI Registry"}
                addrs = npi_info.get("addresses") or npi_info.get("practice_addresses") or []
                affs = []
                for a in addrs:
                    parts = []
                    if isinstance(a, dict):
                        parts = [a.get(k) for k in ("address_1", "city", "state") if a.get(k)]
                    else:
                        parts = [str(a)]
                    aff = ", ".join([p for p in parts if p])
                    if aff and aff not in affs:
                        affs.append(aff)
                enrichment["affiliations"] = {"value": affs[:6], "confidence": 0.7 if affs else 0.12, "source": "NPI Registry"}
                enrichment["education"] = {"value": None, "confidence": 0.05, "source": "NPI Registry"}
                enrichment["services"] = {"value": [], "confidence": 0.12, "source": "NPI Registry"}
                rec["enrichment"] = enrichment
                return rec

        # otherwise low-confidence fallback
        validated_spec = rec.get("specialty")
        if validated_spec and isinstance(validated_spec, str) and validated_spec.strip().lower() != "unknown":
            enrichment["specialty"] = {"value": validated_spec.strip(), "confidence": 0.45, "source": source_tag}
        for k in ("education", "services", "affiliations"):
            enrichment[k] = {"value": enrichment[k]["value"], "confidence": 0.05, "source": source_tag}
        rec["enrichment"] = enrichment
        return rec

    # parse page
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text(" ", strip=True)

    # parse JSON-LD structured data (new)
    jsonld = _parse_jsonld(soup)
    if any(jsonld.values()):
        logger.info("JSON-LD found for %s: specialties=%s affiliations=%s", rec.get("provider_id"), jsonld["specialties"][:2], jsonld["affiliations"][:2])

    # Extract using deterministic functions
    edu_val, edu_conf = extract_education(soup, page_text)
    spec_val, spec_conf = extract_specialty(soup, page_text, rec.get("specialty"))
    services_val, services_conf = extract_services(soup, page_text)
    aff_val, aff_conf = extract_affiliations(soup, page_text)

    # Merge JSON-LD results: if jsonld provides values, prefer and boost confidence
    if jsonld.get("specialties"):
        # pick first specialty and bump confidence
        spec_val = spec_val or jsonld["specialties"][0]
        spec_conf = max(spec_conf, 0.85)
    if jsonld.get("education"):
        edu_val = edu_val or (jsonld["education"][0] if jsonld["education"] else None)
        edu_conf = max(edu_conf, 0.8 if jsonld["education"] else edu_conf)
    if jsonld.get("services"):
        # extend services list and bump confidence
        services_val = list(dict.fromkeys((services_val or []) + jsonld["services"]))[:10]
        services_conf = max(services_conf, 0.85 if services_val else services_conf)
    if jsonld.get("affiliations"):
        aff_val = list(dict.fromkeys((aff_val or []) + jsonld["affiliations"]))[:8]
        aff_conf = max(aff_conf, 0.85 if aff_val else aff_conf)

    # NPI taxonomies: if rec has raw npi taxonomies, use them to boost specialty
    npi_taxonomies = []
    if isinstance(rec.get("npi_raw"), dict):
        # some code paths store raw NPI response; look for taxonomies
        npi_taxonomies = rec["npi_raw"].get("taxonomies") or rec["npi_raw"].get("taxonomies", [])
    elif isinstance(rec.get("taxonomies"), list):
        npi_taxonomies = rec.get("taxonomies", [])

    if npi_taxonomies and not spec_val:
        # pick primary taxonomy description if available
        primary = next((t for t in npi_taxonomies if t.get("primary")), npi_taxonomies[0])
        if primary:
            cand = primary.get("desc") or primary.get("description") or primary.get("specialization")
            if cand:
                spec_val = cand
                spec_conf = max(spec_conf, 0.8)

    enrichment["education"] = {"value": edu_val, "confidence": round(float(edu_conf), 2), "source": final_url}
    enrichment["specialty"] = {"value": spec_val, "confidence": round(float(spec_conf), 2), "source": final_url}
    enrichment["services"] = {"value": services_val, "confidence": round(float(services_conf), 2), "source": final_url}
    enrichment["affiliations"] = {"value": aff_val, "confidence": round(float(aff_conf), 2), "source": final_url}

    # If multiple sources agree (e.g., JSON-LD + HTML + NPI), bump confidences slightly
    agreement_count = 0
    if jsonld.get("specialties") and spec_val and spec_val in jsonld["specialties"]:
        agreement_count += 1
    if jsonld.get("affiliations") and aff_val:
        # check intersection
        if any(a in (aff_val or []) for a in jsonld["affiliations"]):
            agreement_count += 1
    if agreement_count >= 1:
        # small deterministic boost
        for k in ("specialty", "affiliations", "education"):
            if enrichment[k]["confidence"] and enrichment[k]["confidence"] < 0.98:
                enrichment[k]["confidence"] = round(min(1.0, enrichment[k]["confidence"] + 0.05), 2)

    logger.info("Enrichment result for %s -> specialty:%s (%.2f) education:%s (%.2f) services:%d (%.2f) affiliations:%d (%.2f)",
                rec.get("provider_id"),
                enrichment["specialty"]["value"], enrichment["specialty"]["confidence"],
                enrichment["education"]["value"], enrichment["education"]["confidence"],
                len(enrichment["services"]["value"]), enrichment["services"]["confidence"],
                len(enrichment["affiliations"]["value"]), enrichment["affiliations"]["confidence"],
                )
    rec["enrichment"] = enrichment
    return rec


def enrich_all(input_path: str = INPUT_JSON, output_path: str = OUTPUT_JSON):
    logger.info("Starting enrichment run: input=%s output=%s", input_path, output_path)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    try:
        with open(input_path, "r", encoding="utf-8") as f:
            records = json.load(f)
    except FileNotFoundError:
        logger.error("Input file not found: %s", input_path)
        return
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON in input: %s", e)
        return

    enriched: List[Dict] = []
    for rec in records:
        try:
            e = enrich_record(rec)
            enriched.append(e)
        except Exception as exc:
            logger.exception("Enrichment failed for record %s: %s", rec.get("provider_id"), exc)
            rec["enrichment"] = {
                "education": {"value": None, "confidence": 0.0, "source": None},
                "specialty": {"value": rec.get("specialty"), "confidence": 0.4, "source": "fallback"},
                "services": {"value": [], "confidence": 0.0, "source": None},
                "affiliations": {"value": [], "confidence": 0.0, "source": None},
            }
            enriched.append(rec)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(enriched, f, indent=2, ensure_ascii=False)

    logger.info("Enrichment complete: wrote %d records", len(enriched))


if __name__ == "__main__":
    enrich_all()
