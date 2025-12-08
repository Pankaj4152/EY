import json
import re
import time
import urllib.parse
from typing import Dict, List, Optional, Tuple, Set

import requests
from bs4 import BeautifulSoup

INPUT_JSON = "data/output/validated.json"
OUTPUT_JSON = "data/output/enriched.json"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

REQUEST_TIMEOUT = 8
SLEEP_BETWEEN = 0.8

# Dynamic hospital/facility indicators instead of fixed list
HOSPITAL_INDICATORS = [
    "hospital", "medical center", "health system", "healthcare system",
    "clinic", "medical group", "health network", "university hospital",
    "regional medical", "community hospital", "memorial hospital",
    "general hospital", "children's hospital", "cancer center",
    "heart center", "health partners", "medical associates"
]

SPECIALTY_KEYWORDS = {
    "cardiology": ["cardio", "cardiology", "heart"],
    "pediatrics": ["pediatr", "children", "kids"],
    "dentist": ["dentist", "dental", "dds", "dmd", "orthodont"],
    "ophthalmology": ["ophthalmology", "ophthalmologist", "lasik", "eye care", "vision"],
    "optometry": ["optomet", "optometry"],
    "pharmacy": ["pharmacy", "pharmac", "drug store"],
    "chiropractic": ["chiropractic", "chiropractor"],
    "gynecology": ["gynecology", "gynecologist", "obstetrics", "ob/gyn", "obgyn", "women's health"],
    "surgery": ["surgery", "surgeon", "surgical"],
    "dermatology": ["dermatology", "dermato", "skin care"],
    "orthopedics": ["orthopedic", "orthopaedic", "ortho", "bone", "joint"],
    "neurology": ["neurology", "neuro", "brain"],
    "ent": ["ear nose throat", "ent", "otolaryngology"],
    "internal medicine": ["internal medicine", "internist"],
    "family medicine": ["family medicine", "family practice", "general practice", "primary care"],
    "psychiatry": ["psychiatry", "psychiatrist", "mental health"],
    "radiology": ["radiology", "radiologist", "imaging"],
    "anesthesiology": ["anesthesiology", "anesthesiologist"],
    "emergency medicine": ["emergency medicine", "emergency room", "er"],
    "oncology": ["oncology", "oncologist", "cancer"],
}

DEGREE_RE = re.compile(
    r"\b(MD|M\.D\.|DO|D\.O\.|PhD|Ph\.D\.|DDS|D\.D\.S\.|DMD|D\.M\.D\.|"
    r"RN|BSN|MSN|NP|APRN|MBA|MPH|PA-C|PA)\b", re.I
)

GRAD_FROM_RE = re.compile(
    r"(?:graduated from|received (?:his|her|their) (?:medical )?degree from|"
    r"alumnus of|alumna of|attended|studied at)\s+"
    r"([A-Z][A-Za-z0-9&,\.\- ]{3,120}?(?:University|College|School|Institute))",
    re.I,
)

TRAINING_RE = re.compile(
    r"(?:residency|fellowship|internship|trained at|training at|completed (?:a |her |his |their )?"
    r"(?:residency|fellowship|internship) at)[:\s\-]+"
    r"([A-Z][A-Za-z0-9&,\.\- ]{3,120})",
    re.I,
)

SERVICES_SECTION_KEYWORDS = [
    "service", "what we offer", "treatment", "procedures", "services include",
    "we specialize in", "our services", "care we provide", "treatments offered"
]

AFFILIATION_PATTERNS = [
    r"(?:affiliated with|affiliated to|affiliates with|affiliation|member of|"
    r"part of|partners with|associated with)[:\s\-]+([A-Z][A-Za-z0-9 &,\.\-]{3,120})",
    r"(?:practicing at|practices at|located at|practice location)[:\s\-]+([A-Z][A-Za-z0-9 &,\.\-]{3,120})",
    r"(?:staff (?:member|physician|doctor) at|on staff at)[:\s\-]+([A-Z][A-Za-z0-9 &,\.\-]{3,120})",
]

HEADERS = {"User-Agent": USER_AGENT}
_SEARCH_CACHE: Dict[str, Optional[str]] = {}


def http_get(url: str) -> Tuple[Optional[str], Optional[str]]:
    """GET a URL; return (text, final_url) or (None, None) on failure."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.text, resp.url
    except Exception:
        return None, None


def ddg_search_first_site(query: str) -> Optional[str]:
    """
    Deterministic DuckDuckGo HTML search -> first external result.
    Uses a simple cache to avoid redundant lookups.
    """
    if not query:
        return None

    if query in _SEARCH_CACHE:
        return _SEARCH_CACHE[query]

    q = urllib.parse.quote_plus(query)
    url = f"https://html.duckduckgo.com/html/?q={q}"
    html, _ = http_get(url)
    time.sleep(SLEEP_BETWEEN)

    if not html:
        _SEARCH_CACHE[query] = None
        return None

    soup = BeautifulSoup(html, "html.parser")

    # prefer external http(s) links that are not DuckDuckGo
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("http") and "duckduckgo" not in href:
            _SEARCH_CACHE[query] = href
            return href

    _SEARCH_CACHE[query] = None
    return None


def _choose_site(name: str, address: str) -> Tuple[Optional[str], str]:
    """
    Return (site_url or None, source_tag).
    source_tag is either a URL or search:query (for traceability).
    """
    city = ""
    parts = [p.strip() for p in (address or "").split(",") if p.strip()]
    if len(parts) >= 2:
        city = parts[-2]

    query = f"{name} {city} practice website".strip()
    site = ddg_search_first_site(query) if name else None

    if site:
        return site, site

    return None, f"search:{query}"


def extract_education(soup: BeautifulSoup, text: str) -> Tuple[Optional[str], float]:
    """Enhanced education extraction with multiple patterns."""
    # First, try to find degree with nearby context
    m = DEGREE_RE.search(text)
    if m:
        span = text[max(0, m.start() - 80) : m.end() + 80].strip()
        return span, 0.9

    # Look for graduation/school patterns
    m2 = GRAD_FROM_RE.search(text)
    if m2:
        school = m2.group(1).strip()
        return school, 0.85

    # Look for training patterns
    m3 = TRAINING_RE.search(text)
    if m3:
        return m3.group(1).strip(), 0.75

    # Check for "Education" or "Background" sections
    for heading in soup.find_all(["h2", "h3", "h4"]):
        heading_text = heading.get_text(" ", strip=True).lower()
        if any(kw in heading_text for kw in ["education", "background", "training", "credentials"]):
            next_elem = heading.find_next_sibling()
            if next_elem:
                edu_text = next_elem.get_text(" ", strip=True)
                if 10 < len(edu_text) < 300:
                    return edu_text, 0.7

    return None, 0.05


def extract_specialty(
    soup: BeautifulSoup, text: str, validated_specialty: str
) -> Tuple[Optional[str], float]:
    """
    Enhanced specialty extraction with more keyword coverage.
    """
    # prefer validated_specialty if meaningful
    if validated_specialty and validated_specialty.strip().lower() != "unknown":
        return validated_specialty, 0.6

    # check headings and prominent text first
    for tag in ("h1", "h2", "h3", "strong", "b", "title"):
        for el in soup.find_all(tag):
            txt = (el.get_text(" ", strip=True) or "").lower()
            for canon, kws in SPECIALTY_KEYWORDS.items():
                for kw in kws:
                    if kw in txt:
                        return canon.title(), 0.9

    # search body text
    lower = text.lower()
    for canon, kws in SPECIALTY_KEYWORDS.items():
        for kw in kws:
            if kw in lower:
                return canon.title(), 0.8

    return None, 0.2


def extract_services(soup: BeautifulSoup, text: str) -> Tuple[List[str], float]:
    """
    Enhanced service extraction with better pattern matching.
    """
    services: List[str] = []

    # search for headings that match our keywords
    for h in soup.find_all(["h2", "h3", "h4", "h5"]):
        htxt = (h.get_text(" ", strip=True) or "").lower()
        if any(k in htxt for k in SERVICES_SECTION_KEYWORDS):
            # look for a following <ul> or <div>
            sib = h.find_next_sibling()
            if not sib:
                continue
            
            if sib.name == "ul":
                for li in sib.find_all("li"):
                    t = li.get_text(" ", strip=True)
                    if 4 < len(t) < 120:
                        services.append(t)
            elif sib.name == "div":
                # Look for lists within the div
                for ul in sib.find_all("ul"):
                    for li in ul.find_all("li"):
                        t = li.get_text(" ", strip=True)
                        if 4 < len(t) < 120:
                            services.append(t)
            else:
                # small paragraph parse
                para = sib.get_text(" ", strip=True)
                if para and len(para) < 1000:
                    parts = re.split(r"\.|\;|\n|\u2022|\u2023|\•", para)
                    for p in parts:
                        p = p.strip()
                        if 4 < len(p) < 120:
                            services.append(p)

    # fallback pattern search
    if not services:
        m = re.search(
            r"services (?:include|offered|provided|available)[:\s\-]+"
            r"([A-Za-z0-9\.,; &\-\/]{10,500})",
            text,
            re.I,
        )
        if m:
            parts = re.split(r"\,|;|\.", m.group(1))
            services = [p.strip() for p in parts if 4 < len(p.strip()) < 120]

    # dedupe and limit
    services = list(dict.fromkeys(services))[:10]
    conf = 0.8 if services else 0.15
    return services, conf


def _is_likely_hospital(text: str) -> bool:
    """Check if text contains hospital/medical facility indicators."""
    lower = text.lower()
    return any(indicator in lower for indicator in HOSPITAL_INDICATORS)


def extract_affiliations(soup: BeautifulSoup, text: str) -> Tuple[List[str], float]:
    """
    Dynamic hospital affiliation extraction using pattern matching and context analysis.
    """
    found: Set[str] = set()

    # Method 1: Use multiple affiliation patterns
    for pattern in AFFILIATION_PATTERNS:
        matches = re.findall(pattern, text, re.I)
        for match in matches:
            match = match.strip()
            if _is_likely_hospital(match) and 3 < len(match) < 100:
                found.add(match)

    # Method 2: Look for "Affiliations" or "Hospital Privileges" sections
    for heading in soup.find_all(["h2", "h3", "h4", "h5"]):
        heading_text = heading.get_text(" ", strip=True).lower()
        if any(kw in heading_text for kw in ["affiliation", "hospital", "privileges", "network"]):
            next_elem = heading.find_next_sibling()
            if next_elem:
                # Check for list items
                if next_elem.name == "ul":
                    for li in next_elem.find_all("li"):
                        aff_text = li.get_text(" ", strip=True)
                        if _is_likely_hospital(aff_text) and 3 < len(aff_text) < 100:
                            found.add(aff_text)
                else:
                    # Parse paragraph
                    aff_text = next_elem.get_text(" ", strip=True)
                    # Split by common delimiters
                    parts = re.split(r"[;\n\u2022\u2023•]", aff_text)
                    for part in parts:
                        part = part.strip()
                        if _is_likely_hospital(part) and 3 < len(part) < 100:
                            found.add(part)

    # Method 3: Extract capitalized phrases that contain hospital indicators
    capitalized_phrases = re.findall(
        r"\b([A-Z][A-Za-z\s&,\.'-]{10,80}(?:Hospital|Medical Center|Health System|"
        r"Healthcare|Clinic|Medical Group))\b",
        text
    )
    for phrase in capitalized_phrases:
        phrase = phrase.strip()
        if 10 < len(phrase) < 100:
            found.add(phrase)

    # Clean up and dedupe
    cleaned = []
    for aff in found:
        # Remove common false positives
        if not any(skip in aff.lower() for skip in ["copyright", "all rights", "privacy policy", "terms"]):
            cleaned.append(aff)

    conf = 0.85 if cleaned else 0.12
    return cleaned[:8], conf  # Limit to 8 affiliations


def enrich_record(rec: Dict) -> Dict:
    """
    Enrich one validated record and return augmented record with deterministic enrichment.
    Each enriched field includes:
      - value
      - confidence (0..1)
      - source (URL or 'search:...' tag)
    """
    name = rec.get("name") or ""
    address = rec.get("address") or ""

    site_url, source_tag = _choose_site(name, address)
    source_used = source_tag

    enriched = {
        "education": {"value": None, "confidence": 0.0, "source": None},
        "specialty": {"value": None, "confidence": 0.0, "source": None},
        "services": {"value": [], "confidence": 0.0, "source": None},
        "affiliations": {"value": [], "confidence": 0.0, "source": None},
    }

    # No site found: fallback to validated specialty if present
    if not site_url:
        validated_spec = rec.get("specialty")
        if validated_spec and validated_spec.lower() != "unknown":
            enriched["specialty"] = {
                "value": validated_spec,
                "confidence": 0.45,
                "source": source_used,
            }

        # leave other fields None/empty with low confidence
        for k in ("education", "services", "affiliations"):
            enriched[k] = {
                "value": enriched[k]["value"],
                "confidence": 0.05,
                "source": source_used,
            }

        return {**rec, "enrichment": enriched}

    # We have a site, try to fetch
    html, final = http_get(site_url)
    time.sleep(SLEEP_BETWEEN)

    # site found but fetch failed
    if not html:
        validated_spec = rec.get("specialty")
        if validated_spec and validated_spec.lower() != "unknown":
            enriched["specialty"] = {
                "value": validated_spec,
                "confidence": 0.45,
                "source": source_used,
            }
        for k in ("education", "services", "affiliations"):
            enriched[k] = {
                "value": enriched[k]["value"],
                "confidence": 0.05,
                "source": source_used,
            }
        return {**rec, "enrichment": enriched}

    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text(" ", strip=True)

    edu_val, edu_conf = extract_education(soup, page_text)
    spec_val, spec_conf = extract_specialty(
        soup, page_text, rec.get("specialty") or ""
    )
    services_val, services_conf = extract_services(soup, page_text)
    aff_val, aff_conf = extract_affiliations(soup, page_text)

    enriched["education"] = {
        "value": edu_val,
        "confidence": round(float(edu_conf), 2),
        "source": final or source_used,
    }
    enriched["specialty"] = {
        "value": spec_val,
        "confidence": round(float(spec_conf), 2),
        "source": final or source_used,
    }
    enriched["services"] = {
        "value": services_val,
        "confidence": round(float(services_conf), 2),
        "source": final or source_used,
    }
    enriched["affiliations"] = {
        "value": aff_val,
        "confidence": round(float(aff_conf), 2),
        "source": final or source_used,
    }

    return {**rec, "enrichment": enriched}


def enrich_all(input_path: str = INPUT_JSON, output_path: str = OUTPUT_JSON) -> None:
    with open(input_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    enriched: List[Dict] = []

    for r in records:
        try:
            e = enrich_record(r)
            enriched.append(e)
        except Exception as ex:
            print(f"⚠️  Error enriching {r.get('provider_id')}: {ex}")
            # deterministic fallback on error
            r["enrichment"] = {
                "education": {"value": None, "confidence": 0.0, "source": None},
                "specialty": {
                    "value": r.get("specialty"),
                    "confidence": 0.4,
                    "source": "fallback",
                },
                "services": {"value": [], "confidence": 0.0, "source": None},
                "affiliations": {"value": [], "confidence": 0.0, "source": None},
            }
            enriched.append(r)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(enriched, f, indent=2, ensure_ascii=False)

    print(f"✅ Enrichment complete -> {output_path}")


if __name__ == "__main__":
    enrich_all()
