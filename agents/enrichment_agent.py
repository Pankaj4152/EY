"""
Enhanced Enrichment Agent V2 - Multi-strategy website discovery + NLP extraction

Key Improvements:
1. Google Places API first (has website field!)
2. Multi-engine search fallback (DuckDuckGo -> Bing -> Provider Directories)
3. HuggingFace NER for entity extraction
4. Semantic similarity for specialty matching
5. Persistent caching + retry logic
6. Better structured data parsing
"""
import os
import time
import sqlite3
import re
import json
import logging
import urllib.parse as urllib_parse
import requests
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()

# Optional advanced NLP imports (graceful degradation if not available)
try:
    from transformers import pipeline, AutoTokenizer, AutoModel
    import torch
    NLP_AVAILABLE = True
except ImportError:
    NLP_AVAILABLE = False
    logging.warning("Transformers not available. Install: pip install transformers torch")

try:
    from sentence_transformers import SentenceTransformer, util as st_util
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False
    logging.warning("Sentence-transformers not available. Install: pip install sentence-transformers")

# Import our services
try:
    from services.npi_services import fetch_provider_by_npi
except:
    fetch_provider_by_npi = None

try:
    from services.google_maps_services import find_place
except:
    find_place = None

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
INPUT_JSON = "data/output/validated.json"
OUTPUT_JSON = "data/output/enriched.json"
CACHE_DB = "data/enrichment_cache.db"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

REQUEST_TIMEOUT = float(os.getenv("ENRICH_REQUEST_TIMEOUT", "5"))
MAX_RETRIES = int(os.getenv("ENRICH_MAX_RETRIES", "3"))
BACKOFF_FACTOR = float(os.getenv("ENRICH_BACKOFF", "0.5"))
SLEEP_BETWEEN = float(os.getenv("ENRICH_SLEEP_BETWEEN", "1.0"))

# Provider directories to search
PROVIDER_DIRECTORIES = [
    "healthgrades.com",
    "vitals.com",
    "webmd.com/providers",
    "zocdoc.com",
    "doximity.com",
]

# Enhanced specialty keywords
SPECIALTY_KEYWORDS = {
    "Cardiology": ["cardio", "heart", "cardiovascular"],
    "Pediatrics": ["pediatr", "children", "kids"],
    "Dentist": ["dentist", "dental", "dds", "dmd", "orthodont"],
    "Ophthalmology": ["ophthalmology", "eye", "vision", "lasik"],
    "Optometry": ["optomet", "optometry"],
    "Pharmacy": ["pharmacy", "pharmac", "pharmacist"],
    "Chiropractic": ["chiropractic", "chiropractor"],
    "Gynecology": ["gynecology", "obstetric", "ob/gyn", "women's health"],
    "Surgery": ["surgery", "surgeon", "surgical"],
    "Dermatology": ["dermatology", "dermato", "skin"],
    "Orthopedics": ["orthopedic", "ortho", "bone", "joint"],
    "Neurology": ["neurology", "neuro", "brain", "neurological"],
    "ENT": ["ear nose throat", "ent", "otolaryngology"],
    "Internal Medicine": ["internal medicine", "internist"],
    "Family Medicine": ["family medicine", "primary care", "general practice"],
    "Psychiatry": ["psychiatry", "psychiatrist", "mental health"],
    "Radiology": ["radiology", "imaging", "radiologist"],
    "Oncology": ["oncology", "oncologist", "cancer"],
}

# Configure HTTP session with retries
def create_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=MAX_RETRIES,
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=BACKOFF_FACTOR,
        allowed_methods=["GET", "HEAD"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

SESSION = create_session()

# Simple SQLite cache for persistent storage

def init_cache():
    """Initialize persistent cache database"""
    os.makedirs(os.path.dirname(CACHE_DB) or "data", exist_ok=True)
    conn = sqlite3.connect(CACHE_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS url_cache (
            url TEXT PRIMARY KEY,
            content TEXT,
            final_url TEXT,
            timestamp REAL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS search_cache (
            query TEXT PRIMARY KEY,
            result_url TEXT,
            timestamp REAL
        )
    """)
    conn.commit()
    conn.close()

def get_cached_url(url: str) -> Optional[Tuple[str, str]]:
    """Get cached URL content"""
    try:
        conn = sqlite3.connect(CACHE_DB)
        cur = conn.cursor()
        cur.execute(
            "SELECT content, final_url FROM url_cache WHERE url = ? AND timestamp > ?",
            (url, time.time() - 7*86400)
        )
        row = cur.fetchone()
        conn.close()
        if row:
            return (row[0], row[1])
    except Exception:
        logging.exception("get_cached_url failed")
    return None

def get_cached_search(query: str) -> Optional[str]:
    """Get cached search result"""
    try:
        conn = sqlite3.connect(CACHE_DB)
        cur = conn.cursor()
        cur.execute(
            "SELECT result_url FROM search_cache WHERE query = ? AND timestamp > ?",
            (query, time.time() - 7*86400)
        )
        row = cur.fetchone()
        conn.close()
        if row:
            return row[0]
    except Exception:
        logging.exception("get_cached_search failed")
    return None

def cache_url(url: str, content: str, final_url: str):
    """Cache URL content"""
    try:
        conn = sqlite3.connect(CACHE_DB)
        conn.execute(
            "INSERT OR REPLACE INTO url_cache VALUES (?, ?, ?, ?)",
            (url, content, final_url, time.time())
        )
        conn.commit()
        conn.close()
    except:
        pass

def cache_search(query: str, result_url: Optional[str]):
    """Cache search result"""
    try:
        conn = sqlite3.connect(CACHE_DB)
        conn.execute(
            "INSERT OR REPLACE INTO search_cache VALUES (?, ?, ?)",
            (query, result_url, time.time())
        )
        conn.commit()
        conn.close()
    except:
        pass

# Initialize cache
init_cache()

# NLP Models (lazy loading)
_ner_pipeline = None
_similarity_model = None

def get_ner_pipeline():
    """Lazy load NER pipeline"""
    global _ner_pipeline
    if _ner_pipeline is None and NLP_AVAILABLE:
        try:
            logger.info("Loading NER model...")
            _ner_pipeline = pipeline(
                "ner",
                model="dslim/bert-base-NER",
                aggregation_strategy="simple"
            )
            logger.info("NER model loaded successfully")
        except Exception as e:
            logger.warning(f"Failed to load NER model: {e}")
    return _ner_pipeline

def get_similarity_model():
    """Lazy load sentence similarity model"""
    global _similarity_model
    if _similarity_model is None and SENTENCE_TRANSFORMERS_AVAILABLE:
        try:
            logger.info("Loading similarity model...")
            _similarity_model = SentenceTransformer('all-MiniLM-L6-v2')
            logger.info("Similarity model loaded successfully")
        except Exception as e:
            logger.warning(f"Failed to load similarity model: {e}")
    return _similarity_model


def http_get(url: str, use_cache: bool = True) -> Tuple[Optional[str], Optional[str]]:
    """Fetch URL with caching and retry logic"""
    if not url:
        return (None, None)
    # Check cache first
    if use_cache:
        cached = get_cached_url(url)
        if cached:
            logger.debug(f"Cache hit: {url}")
            return cached
    headers = {"User-Agent": USER_AGENTS[hash(url) % len(USER_AGENTS)]}
    try:
        logging.info("Fetching: %s", url)
        r = SESSION.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        content = r.text
        final_url = r.url
        cache_url(url, content, final_url)
        time.sleep(SLEEP_BETWEEN)
        return (content, final_url)
    except Exception:
        logging.exception("http_get failed for %s", url)
        return (None, None)

def google_places_search(name: str, city: str) -> Optional[str]:
    """
    Use Google Places API to find provider website
    This is the BEST source - Google has verified websites!
    """
    if not find_place:
        return None
    try:
        logger.info(f"Google Places search: {name}, {city}")
        place = find_place(name, city)
        if place and place.get("website"):
            return place.get("website")
    except Exception:
        logging.exception("google_places_search failed")
    return None

def duckduckgo_search(query: str) -> Optional[str]:
    """Search DuckDuckGo HTML version"""
    cached = get_cached_search(f"ddg:{query}")
    if cached:
        return cached
    try:
        search_url = f"https://html.duckduckgo.com/html/?q={urllib_parse.quote_plus(query)}"
        html, _ = http_get(search_url, use_cache=False)
        if html:
            soup = BeautifulSoup(html, "html.parser")
            a = soup.find("a", href=True)
            if a:
                url = a["href"]
                cache_search(f"ddg:{query}", url)
                return url
    except Exception:
        logging.exception("duckduckgo_search failed")
    cache_search(f"ddg:{query}", None)
    return None

def bing_search(query: str) -> Optional[str]:
    """Search Bing"""
    cached = get_cached_search(f"bing:{query}")
    if cached:
        return cached
    try:
        search_url = f"https://www.bing.com/search?q={urllib_parse.quote_plus(query)}"
        html, _ = http_get(search_url, use_cache=False)
        if html:
            soup = BeautifulSoup(html, "html.parser")
            a = soup.find("a", href=True)
            if a:
                url = a["href"]
                cache_search(f"bing:{query}", url)
                return url
    except Exception:
        logging.exception("bing_search failed")
    cache_search(f"bing:{query}", None)
    return None
def search_provider_directories(name: str, city: str) -> Optional[str]:
    """Search known provider directories"""
    for directory in PROVIDER_DIRECTORIES:
        query = f"site:{directory} {name} {city}"
        
        # Try DuckDuckGo first
        result = duckduckgo_search(query)
        if result:
            logger.info(f" Found on {directory}: {result}")
            return result
    
    return None

def find_provider_website(name: str, address: str) -> Tuple[Optional[str], str]:
    """
    Multi-strategy website discovery
    Priority: Google Places > Provider Directories > General Search
    """
    logger.info(f"Finding website for: {name}")
    
    # Extract city from address
    city = ""
    if address:
        parts = [p.strip() for p in address.split(",")]
        if len(parts) >= 2:
            city = parts[-2]
    
    # Strategy 1: Google Places (BEST - has verified websites!)
    if city:
        website = google_places_search(name, city)
        if website:
            return website, "google_places"
    
    # Strategy 2: Provider Directories (Healthgrades, Vitals, etc.)
    if city:
        website = search_provider_directories(name, city)
        if website:
            return website, "provider_directory"
    
    # Strategy 3: General web search
    queries = [
        f"{name} {city} doctor",
        f"{name} {city} physician",
        f"{name} {city} practice",
        f"{name} healthcare provider"
    ]
    
    for query in queries:
        # Try DuckDuckGo
        result = duckduckgo_search(query)
        if result:
            logger.info(f" Found via DuckDuckGo: {result}")
            return result, f"search:ddg:{query}"
        
        # Try Bing as fallback
        result = bing_search(query)
        if result:
            logger.info(f" Found via Bing: {result}")
            return result, f"search:bing:{query}"
    
    logger.warning(f"❌ No website found for: {name}")
    return None, f"search_failed:{name}"


def extract_with_ner(text: str) -> Dict[str, List[str]]:
    """Extract entities using NER model"""
    ner = get_ner_pipeline()
    if not ner:
        return {"organizations": [], "persons": [], "locations": []}
    
    try:
        # Limit text length for performance
        text_sample = text[:5000]
        entities = ner(text_sample)
        
        result = {"organizations": [], "persons": [], "locations": []}
        
        for entity in entities:
            if entity["entity_group"] == "ORG":
                result["organizations"].append(entity["word"])
            elif entity["entity_group"] == "PER":
                result["persons"].append(entity["word"])
            elif entity["entity_group"] == "LOC":
                result["locations"].append(entity["word"])
        
        return result
    except Exception as e:
        logger.debug(f"NER extraction failed: {e}")
        return {"organizations": [], "persons": [], "locations": []}


def semantic_specialty_match(text: str, validated_specialty: str) -> Tuple[Optional[str], float]:
    """Use semantic similarity to match specialty"""
    model = get_similarity_model()
    if not model:
        return None, 0.0
    
    try:
        # Get embeddings
        text_lower = text.lower()[:1000]  # Sample for performance
        
        # Create candidate list
        candidates = list(SPECIALTY_KEYWORDS.keys())
        if validated_specialty and validated_specialty != "Unknown":
            candidates.insert(0, validated_specialty)
        
        # Compute similarities
        text_embedding = model.encode(text_lower, convert_to_tensor=True)
        candidate_embeddings = model.encode(candidates, convert_to_tensor=True)
        
        similarities = st_util.pytorch_cos_sim(text_embedding, candidate_embeddings)[0]
        
        best_idx = similarities.argmax().item()
        best_score = similarities[best_idx].item()
        
        if best_score > 0.3:  # Threshold
            return candidates[best_idx], min(best_score, 0.95)
    except Exception as e:
        logger.debug(f"Semantic matching failed: {e}")
    
    return None, 0.0


def extract_education(soup: BeautifulSoup, text: str, ner_entities: Dict) -> Tuple[Optional[str], float]:
    """Extract education with NER assistance"""
    # Try NER organizations first (universities)
    if ner_entities.get("organizations"):
        for org in ner_entities["organizations"]:
            if any(word in org.lower() for word in ["university", "college", "school", "institute"]):
                return org, 0.85
    
    # Regex patterns
    degree_match = re.search(
        r"\b(MD|M\.D\.|DO|D\.O\.|PhD|Ph\.D\.|DDS|DMD)\b.*?(?:from|at)\s+([A-Z][A-Za-z\s&,\.]+(?:University|College|School|Institute))",
        text,
        re.I
    )
    if degree_match:
        return degree_match.group(0)[:100], 0.8
    
    grad_match = re.search(
        r"(?:graduated from|received.*?degree from|attended|studied at)\s+([A-Z][A-Za-z0-9&,\.\- ]{3,80})",
        text,
        re.I
    )
    if grad_match:
        return grad_match.group(1).strip(), 0.75
    
    return None, 0.1


def extract_specialty(soup: BeautifulSoup, text: str, validated_specialty: str, ner_entities: Dict) -> Tuple[Optional[str], float]:
    """Extract specialty with semantic matching"""
    # Try semantic matching first
    semantic_spec, semantic_conf = semantic_specialty_match(text, validated_specialty)
    if semantic_spec and semantic_conf > 0.7:
        return semantic_spec, semantic_conf
    
    # Use validated specialty if good
    if validated_specialty and validated_specialty != "Unknown":
        return validated_specialty, 0.6
    
    # Title and heading search
    for tag in ["title", "h1", "h2"]:
        for el in soup.find_all(tag):
            txt = el.get_text(" ", strip=True).lower()
            for canon, keywords in SPECIALTY_KEYWORDS.items():
                if any(kw in txt for kw in keywords):
                    return canon, 0.85
    
    # Body text keyword search
    text_lower = text.lower()
    for canon, keywords in SPECIALTY_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return canon, 0.7
    
    return None, 0.2


def extract_services(soup: BeautifulSoup, text: str) -> Tuple[List[str], float]:
    """Extract services with improved patterns"""
    services = []
    
    # Look for service sections
    service_keywords = ["service", "treatment", "procedure", "we offer", "we provide", "specializ"]
    
    for h in soup.find_all(["h2", "h3", "h4"]):
        h_text = h.get_text(" ", strip=True).lower()
        if any(kw in h_text for kw in service_keywords):
            # Get next sibling list
            sibling = h.find_next_sibling()
            if sibling and sibling.name in ["ul", "ol"]:
                for li in sibling.find_all("li"):
                    service = li.get_text(" ", strip=True)
                    if 5 < len(service) < 150:
                        services.append(service)
    
    # Dedupe
    services = list(dict.fromkeys(services))[:10]
    
    conf = 0.8 if services else 0.15
    return services, conf


def extract_affiliations(soup: BeautifulSoup, text: str, ner_entities: Dict) -> Tuple[List[str], float]:
    """Extract affiliations with NER"""
    affiliations = []
    
    # Use NER organizations
    if ner_entities.get("organizations"):
        for org in ner_entities["organizations"]:
            if any(word in org.lower() for word in ["hospital", "medical", "health", "clinic", "center"]):
                if org not in affiliations:
                    affiliations.append(org)
    
    # Regex patterns
    patterns = [
        r"(?:affiliated with|member of|staff at|practices at)\s+([A-Z][A-Za-z0-9 &\-]{3,80}(?:Hospital|Medical Center|Health|Clinic))",
    ]
    
    for pattern in patterns:
        for match in re.finditer(pattern, text, re.I):
            aff = match.group(1).strip()
            if aff not in affiliations:
                affiliations.append(aff)
    
    affiliations = affiliations[:8]
    conf = 0.85 if affiliations else 0.15
    return affiliations, conf


def enrich_record(rec: Dict) -> Dict:
    """Enrich a single provider record"""
    provider_id = rec.get("provider_id", "UNKNOWN")
    name = rec.get("name", "")
    address = rec.get("address", "")
    
    logger.info(f"Enriching: {provider_id} - {name}")
    
    # Initialize enrichment
    enrichment = {
        "education": {"value": None, "confidence": 0.0, "source": None},
        "specialty": {"value": None, "confidence": 0.0, "source": None},
        "services": {"value": [], "confidence": 0.0, "source": None},
        "affiliations": {"value": [], "confidence": 0.0, "source": None},
    }
    
    # Find website
    website, source_tag = find_provider_website(name, address)
    
    if not website:
        # Fallback to NPI
        logger.info(f"No website found, trying NPI fallback for {provider_id}")
        npi = rec.get("npi")
        if npi and fetch_provider_by_npi:
            try:
                npi_data = fetch_provider_by_npi(str(npi))
                if npi_data:
                    enrichment["specialty"] = {
                        "value": npi_data.get("specialty") or rec.get("specialty"),
                        "confidence": 0.75,
                        "source": "NPI Registry"
                    }
                    enrichment["education"] = {
                        "value": npi_data.get("credential"),
                        "confidence": 0.7 if npi_data.get("credential") else 0.1,
                        "source": "NPI Registry"
                    }
            except Exception as e:
                logger.debug(f"NPI fallback failed: {e}")
        
        # Carry forward validated specialty
        if not enrichment["specialty"]["value"]:
            val_spec = rec.get("specialty")
            if val_spec and val_spec != "Unknown":
                enrichment["specialty"] = {
                    "value": val_spec,
                    "confidence": 0.5,
                    "source": "validated"
                }
        
        rec["enrichment"] = enrichment
        return rec
    
    # Fetch and parse website
    html, final_url = http_get(website)
    if not html:
        logger.warning(f"Failed to fetch {website}")
        rec["enrichment"] = enrichment
        return rec
    
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    
    # Extract with NER
    ner_entities = extract_with_ner(text)
    
    # Extract fields
    edu_val, edu_conf = extract_education(soup, text, ner_entities)
    spec_val, spec_conf = extract_specialty(soup, text, rec.get("specialty", ""), ner_entities)
    serv_val, serv_conf = extract_services(soup, text)
    aff_val, aff_conf = extract_affiliations(soup, text, ner_entities)
    
    enrichment["education"] = {"value": edu_val, "confidence": round(edu_conf, 2), "source": final_url}
    enrichment["specialty"] = {"value": spec_val, "confidence": round(spec_conf, 2), "source": final_url}
    enrichment["services"] = {"value": serv_val, "confidence": round(serv_conf, 2), "source": final_url}
    enrichment["affiliations"] = {"value": aff_val, "confidence": round(aff_conf, 2), "source": final_url}
    
    logger.info(f" Enriched {provider_id}: specialty={spec_val} ({spec_conf:.2f}), education={edu_val} ({edu_conf:.2f})")
    
    rec["enrichment"] = enrichment
    return rec


def enrich_all(input_path: str = INPUT_JSON, output_path: str = OUTPUT_JSON):
    """Main enrichment function"""
    logger.info("="*80)
    logger.info("ENRICHMENT AGENT V2 - Starting")
    logger.info("="*80)
    
    os.makedirs(os.path.dirname(output_path) or "data/output", exist_ok=True)
    
    try:
        with open(input_path, "r", encoding="utf-8") as f:
            records = json.load(f)
    except FileNotFoundError:
        logger.error(f"Input file not found: {input_path}")
        return
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON: {e}")
        return
    
    enriched = []
    success_count = 0
    
    for i, rec in enumerate(records, 1):
        logger.info(f"\n[{i}/{len(records)}] Processing...")
        try:
            enriched_rec = enrich_record(rec)
            enriched.append(enriched_rec)
            
            # Check if enrichment was successful
            if enriched_rec["enrichment"]["specialty"]["confidence"] > 0.5:
                success_count += 1
        except Exception as e:
            logger.exception(f"Failed to enrich record {rec.get('provider_id')}: {e}")
            rec["enrichment"] = {
                "education": {"value": None, "confidence": 0.0, "source": "error"},
                "specialty": {"value": rec.get("specialty"), "confidence": 0.4, "source": "fallback"},
                "services": {"value": [], "confidence": 0.0, "source": "error"},
                "affiliations": {"value": [], "confidence": 0.0, "source": "error"},
            }
            enriched.append(rec)
    
    # Save results
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(enriched, f, indent=2, ensure_ascii=False)
    
    logger.info("\n" + "="*80)
    logger.info("ENRICHMENT COMPLETE")
    logger.info("="*80)
    logger.info(f"Total: {len(records)} providers")
    logger.info(f"Successfully enriched: {success_count} ({success_count/len(records)*100:.1f}%)")
    logger.info(f"Output: {output_path}")
    logger.info("="*80)


if __name__ == "__main__":
    enrich_all()