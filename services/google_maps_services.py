import os
import requests
from dotenv import load_dotenv

# load .env from repo root (d:\EY\.env)
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

GOOGLE_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
PLACES_TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"


def verify_address(address: str):
    """
    Verifies and standardizes an address using Google Geocoding API.
    """
    params = {
        "address": address,
        "key": GOOGLE_API_KEY
    }

    try:
        res = requests.get(GEOCODE_URL, params=params, timeout=10)
        res.raise_for_status()
        data, err = _handle_google_response(res)
        if err:
            print(f"[GOOGLE GEOCODE ERROR] {err}")
            return None

        if not data.get("results"):
            return None

        result = data["results"][0]

        return {
            "formatted_address": result.get("formatted_address"),
            "lat": result["geometry"]["location"]["lat"],
            "lng": result["geometry"]["location"]["lng"],
            "source": "Google Geocoding"
        }

    except requests.exceptions.RequestException as e:
        print(f"[GOOGLE GEOCODE ERROR] {e}")
        return None


def find_place(name: str, city: str):
    """
    Finds a business/place using Google Places Text Search.
    """
    query = f"{name} {city}"

    params = {
        "query": query,
        "key": GOOGLE_API_KEY
    }

    try:
        res = requests.get(PLACES_TEXT_SEARCH_URL, params=params, timeout=10)
        res.raise_for_status()
        data, err = _handle_google_response(res)
        if err:
            print(f"[GOOGLE PLACES ERROR] {err}")
            return None

        if not data.get("results"):
            return None

        place = data["results"][0]

        place_id = place.get("place_id")

        return fetch_place_details(place_id)

    except requests.exceptions.RequestException as e:
        print(f"[GOOGLE PLACES ERROR] {e}")
        return None


def fetch_place_details(place_id: str):
    """
    Fetch full business details including phone and website.
    """
    params = {
        "place_id": place_id,
        # added 'website' and 'url' fields so we can get provider website when available
        "fields": "name,formatted_address,formatted_phone_number,types,website,url",
        "key": GOOGLE_API_KEY
    }

    try:
        res = requests.get(PLACES_DETAILS_URL, params=params, timeout=10)
        res.raise_for_status()
        data, err = _handle_google_response(res)
        if err:
            print(f"[GOOGLE DETAILS ERROR] {err}")
            return None

        result = data.get("result")

        if not result:
            return None

        return {
            "name": result.get("name"),
            "address": result.get("formatted_address"),
            "phone": result.get("formatted_phone_number"),
            "types": result.get("types"),
            "website": result.get("website") or result.get("url"),
            "source": "Google Places"
        }

    except requests.exceptions.RequestException as e:
        print(f"[GOOGLE DETAILS ERROR] {e}")
        return None


def _handle_google_response(res):
    # helper to surface Google's JSON error message if present
    try:
        data = res.json()
    except ValueError:
        return None, f"Invalid JSON response: {res.text[:500]}"
    status = data.get("status")
    if status and status != "OK":
        return data, f"Google API status={status}, error_message={data.get('error_message')}"
    return data, None


if __name__ == "__main__":
    # Example usage
    from services.npi_services import fetch_provider_by_npi, NPI_ID
    
    verified = verify_address(fetch_provider_by_npi(str(NPI_ID))["address"])
    print("Verified Address:", verified)

    place = find_place("10 PARK PHARMACY INC.", "NY")
    print("Place Details:", place)

