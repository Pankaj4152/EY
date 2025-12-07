import requests

# Store NPIs as STRINGS to avoid scientific notation
NPI_ID = "1447268107"  # Changed from int to string

NPI_BASE_URL = "https://npiregistry.cms.hhs.gov/api/"

def fetch_provider_by_npi(npi_id: str):
    """
    Fetch provider details from the official CMS NPI Registry using NPI ID.
    Returns normalized provider data or None if not found.
    """
    
    # Ensure NPI is a string and strip whitespace
    npi_id = str(npi_id).strip()
    
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
        
        # Handle both individual and organization names
        if basic.get("organization_name"):
            name = basic.get("organization_name")
        else:
            first_name = basic.get("first_name", "")
            last_name = basic.get("last_name", "")
            name = f"{first_name} {last_name}".strip()
        
        # ---- PRACTICE ADDRESS ----
        address_data = None
        for addr in provider.get("addresses", []):
            if addr.get("address_purpose") == "LOCATION":
                address_data = addr
                break
        
        # Fallback to first address if no LOCATION found
        if not address_data and provider.get("addresses"):
            address_data = provider["addresses"][0]
        
        address = None
        phone = None
        
        if address_data:
            address_parts = [
                address_data.get("address_1"),
                address_data.get("address_2"),
                address_data.get("city"),
                address_data.get("state"),
                address_data.get("postal_code")
            ]
            address = ", ".join([p for p in address_parts if p])
            phone = address_data.get("telephone_number")
        
        # ---- TAXONOMY (SPECIALTY) ----
        taxonomy_data = provider.get("taxonomies", [])
        specialty = None
        license_state = None
        
        if taxonomy_data:
            primary_taxonomy = next((t for t in taxonomy_data if t.get("primary")), taxonomy_data[0])
            specialty = primary_taxonomy.get("desc")
            license_state = primary_taxonomy.get("state")
        
        # Return data in format expected by validation_agent.py
        extracted_provider = {
            "npi": npi_id,
            "name": name,
            "organization_name": basic.get("organization_name"),  # Added for validation_agent
            "address": address,
            "phone": phone,
            "telephone_number": phone,  # Added for validation_agent compatibility
            "specialty": specialty,
            "license_state": license_state,
            "addresses": provider.get("addresses", []),  # Raw addresses for validation_agent
            "taxonomies": taxonomy_data,  # Raw taxonomies for validation_agent
            "basic": basic,  # Raw basic data for validation_agent
            "source": "NPI Registry"
        }
        
        return extracted_provider
    
    except requests.exceptions.RequestException as e:
        print(f"[NPI ERROR] Failed to fetch data for NPI {npi_id}: {str(e)}")
        return None

if __name__ == "__main__":
    provider = fetch_provider_by_npi(NPI_ID)
    if provider:
        print("Fetched Provider Data:")
        print(provider)
    else:
        print("No provider data found.")
