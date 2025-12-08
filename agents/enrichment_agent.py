"""
Enrichment Agent - deterministic, explainable scrapers + extractors.

Inputs:
  - data/output/validated.json

Outputs:
  - data/output/enriched.json

Each enriched field contains:
  - value
  - confidence (0..1)
  - source (URL or 'search:<query>' tag)

Strategy:
  - Find practice/hospital site via DuckDuckGo HTML search (deterministic).
  - Fetch page (requests) with polite throttle and basic caching.
  - Extract education, specialty, services, affiliations using
    explainable regex and rule-based heuristics.
"""
from typing import Dict, List, Optional, Tuple
import json
import os
import re
import time
import urllib.parse

import requests
from bs4 import BeautifulSoup

INPUT_JSON = "data/output/validated.json"
OUTPUT_JSON = "data/output/enriched.json"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

REQUEST_TIMEOUT = 8
SLEEP_BETWEEN = 0.8  # polite deterministic throttle
HEADERS = {"User-Agent": USER_AGENT}

# Simple dynamic specialty keywords for deterministic mapping
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


def http_get(url: str) -> Tuple[Optional[str], Optional[str]]:
    """Fetch URL with caching; return (text, final_url) or (None, None)."""
    if not url:
        return None, None
    if url in _FETCH_CACHE:
        return _FETCH_CACHE[url]  # type: ignore
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        text = r.text
        final = r.url
        _FETCH_CACHE[url] = (text, final)
        time.sleep(SLEEP_BETWEEN)
        return text, final
    except Exception:
        _FETCH_CACHE[url] = (None, None)
        return None, None


def ddg_search_first_site(query: str) -> Optional[str]:
    """Use DuckDuckGo HTML version to deterministically return the first external result URL."""
    query = (query or "").strip()
    if not query:
        return None
    if query in _SEARCH_CACHE:
        return _SEARCH_CACHE[query]
    q = urllib.parse.quote_plus(query)
    search_url = f"https://html.duckduckgo.com/html/?q={q}"
    html, final = http_get(search_url)
    if not html:
        _SEARCH_CACHE[query] = None
        return None
    soup = BeautifulSoup(html, "html.parser")
    # look for anchor with class 'result__a' then fallback to first http href
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("http") and "duckduckgo" not in href:
            _SEARCH_CACHE[query] = href
            return href
    _SEARCH_CACHE[query] = None
    return None


def _choose_site(name: str, address: str) -> Tuple[Optional[str], str]:
    """
    Determine a site URL for the provider.
    Returns (site_url or None, source_tag).
    Source_tag is either the URL or 'search:<query>' when no fetchable site found.
    """
    name = (name or "").strip()
    addr = (address or "").strip()
    city = ""
    if addr:
        parts = [p.strip() for p in addr.split(",") if p.strip()]
        if len(parts) >= 2:
            city = parts[-2]
    query = f"{name} {city} practice website".strip()
    site = ddg_search_first_site(query)
    if site:
        return site, site
    return None, f"search:{query}"


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

    site_url, source_tag = _choose_site(name, address)

    # initialize low-confidence defaults
    enrichment = {
        "education": {"value": None, "confidence": 0.0, "source": None},
        "specialty": {"value": None, "confidence": 0.0, "source": None},
        "services": {"value": [], "confidence": 0.0, "source": None},
        "affiliations": {"value": [], "confidence": 0.0, "source": None},
    }

    # if no site found, keep deterministic low-confidence fallback
    if not site_url:
        # If validated specialty exists, carry it forward with modest confidence
        validated_spec = rec.get("specialty")
        if validated_spec and isinstance(validated_spec, str) and validated_spec.strip().lower() != "unknown":
            enrichment["specialty"] = {"value": validated_spec.strip(), "confidence": 0.45, "source": source_tag}
        for k in ("education", "services", "affiliations"):
            enrichment[k] = {"value": enrichment[k]["value"], "confidence": 0.05, "source": source_tag}
        rec["enrichment"] = enrichment
        return rec

    html, final_url = http_get(site_url)
    if not html:
        # site present but fetch failed -> fallback to search tag
        validated_spec = rec.get("specialty")
        if validated_spec and isinstance(validated_spec, str) and validated_spec.strip().lower() != "unknown":
            enrichment["specialty"] = {"value": validated_spec.strip(), "confidence": 0.45, "source": source_tag}
        for k in ("education", "services", "affiliations"):
            enrichment[k] = {"value": enrichment[k]["value"], "confidence": 0.05, "source": source_tag}
        rec["enrichment"] = enrichment
        return rec

    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text(" ", strip=True)

    # Extract using deterministic functions
    edu_val, edu_conf = extract_education(soup, page_text)
    spec_val, spec_conf = extract_specialty(soup, page_text, rec.get("specialty"))
    services_val, services_conf = extract_services(soup, page_text)
    aff_val, aff_conf = extract_affiliations(soup, page_text)

    enrichment["education"] = {"value": edu_val, "confidence": round(float(edu_conf), 2), "source": final_url}
    enrichment["specialty"] = {"value": spec_val, "confidence": round(float(spec_conf), 2), "source": final_url}
    enrichment["services"] = {"value": services_val, "confidence": round(float(services_conf), 2), "source": final_url}
    enrichment["affiliations"] = {"value": aff_val, "confidence": round(float(aff_conf), 2), "source": final_url}

    rec["enrichment"] = enrichment
    return rec


def enrich_all(input_path: str = INPUT_JSON, output_path: str = OUTPUT_JSON):
    """Load validated records, enrich them, and write enriched.json."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    try:
        with open(input_path, "r", encoding="utf-8") as f:
            records = json.load(f)
    except FileNotFoundError:
        print(f"❌ Input file not found: {input_path}")
        return
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in input: {e}")
        return

    enriched: List[Dict] = []
    for rec in records:
        try:
            e = enrich_record(rec)
            enriched.append(e)
        except Exception as exc:
            # deterministic fallback on any error
            rec["enrichment"] = {
                "education": {"value": None, "confidence": 0.0, "source": None},
                "specialty": {"value": rec.get("specialty"), "confidence": 0.4, "source": "fallback"},
                "services": {"value": [], "confidence": 0.0, "source": None},
                "affiliations": {"value": [], "confidence": 0.0, "source": None},
            }
            enriched.append(rec)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(enriched, f, indent=2, ensure_ascii=False)

    print(f"✅ Enrichment complete -> {output_path}")


if __name__ == "__main__":
    enrich_all()
