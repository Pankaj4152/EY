
# NPI_ID = 1871860924
NPI_ID = 1447268107
import requests

NPI_BASE_URL = "https://npiregistry.cms.hhs.gov/api/"


def fetch_provider_by_npi(npi_id: str):
    """
    Fetch provider details from the official CMS NPI Registry using NPI ID.
    Returns normalized provider data or None if not found.
    """

    params = {
        "number": npi_id,
        "version": "2.1"
    }

    try:
        response = requests.get(NPI_BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("result_count", 0) == 0:
            print(f"[NPI] No provider found for NPI: {npi_id}")
            return None

        provider = data["results"][0]

        # ---- BASIC IDENTIFICATION ----
        basic = provider.get("basic", {})
        name = f"{basic.get('first_name', '')} {basic.get('last_name', '')}".strip()

        # ---- PRACTICE ADDRESS ----
        address_data = None
        for addr in provider.get("addresses", []):
            if addr.get("address_purpose") == "LOCATION":
                address_data = addr
                break

        address = None
        phone = None

        if address_data:
            address = f"{address_data.get('address_1', '')}, {address_data.get('city', '')}, {address_data.get('state', '')} {address_data.get('postal_code', '')}"
            phone = address_data.get("telephone_number")

        # ---- TAXONOMY (SPECIALTY) ----
        taxonomy_data = provider.get("taxonomies", [])
        specialty = None
        license_state = None

        if taxonomy_data:
            primary_taxonomy = next((t for t in taxonomy_data if t.get("primary")), taxonomy_data[0])
            specialty = primary_taxonomy.get("desc")
            license_state = primary_taxonomy.get("state")

        extracted_provider = {
            "npi": npi_id,
            "name": name,
            "address": address,
            "phone": phone,
            "specialty": specialty,
            "license_state": license_state,
            "source": "NPI Registry"
        }

        return extracted_provider

    except requests.exceptions.RequestException as e:
        print(f"[NPI ERROR] Failed to fetch data for NPI {npi_id}: {str(e)}")
        return None


if __name__ == "__main__":
    provider = fetch_provider_by_npi(str(NPI_ID))
    if provider:
        print("Fetched Provider Data:")
        # for key, value in provider.items():
        #     print(f"{key}: {value}")
        print(provider)
    else:
        print("No provider data found.")