import csv
import json
import phonenumbers
from services.npi_services import fetch_provider_by_npi
from services.google_maps_services import verify_address, find_place


INPUT_CSV = "data/input/providers.csv"
OUTPUT_JSON = "data/output/validated.json"


def calculate_confidence(npi_val, google_val, csv_val, has_npi=True):
    """
    Simple deterministic confidence scoring.
    """
    if npi_val and google_val and csv_val:
        if npi_val == google_val == csv_val:
            return 1.0
        if npi_val == google_val:
            return 0.9 if has_npi else 0.75
        if google_val == csv_val:
            return 0.85 if has_npi else 0.7
        return 0.6

    if npi_val and google_val:
        return 0.85 if has_npi else 0.7

    if google_val:
        return 0.7

    if npi_val:
        return 0.75

    return 0.4


def normalize_phone(phone: str, default_region: str = "US"):
    if not phone:
        return None
    try:
        pn = phonenumbers.parse(phone, default_region)
        if phonenumbers.is_valid_number(pn):
            return phonenumbers.format_number(pn, phonenumbers.PhoneNumberFormat.E164)
    except phonenumbers.NumberParseException:
        pass
    return phone  # fallback to original


def validate_providers():
    validated_results = []

    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            provider_id = row.get("provider_id")
            name = row.get("full_name")
            phone = row.get("phone")
            address = row.get("address")
            city = row.get("city")
            state = row.get("state")
            npi = row.get("npi")

            print(f"\n[VALIDATING] {name} ({provider_id})")

            # ----------------------------
            # 1. FETCH NPI DATA (IF EXISTS)
            # ----------------------------
            # Replace lines 55-90 with this corrected version:

            npi_data = None
            if npi and npi.strip():
                try:
                    npi_str = str(npi).strip()
                    print(f"[NPI] attempting fetch for: {npi_str!r}")
                    fetched = fetch_provider_by_npi(npi_str)
                    print(f"[NPI] raw fetched: {fetched!r}")
                    
                    if fetched:
                        # Now the data structure matches what we expect
                        npi_data = {
                            "name": fetched.get("name"),
                            "address": fetched.get("address"),
                            "phone": fetched.get("phone"),
                            "specialty": fetched.get("specialty")
                        }
                        print(f"[NPI] Successfully extracted: {npi_data}")
                    else:
                        print(f"[NPI] no data returned for {npi_str!r}")
                except Exception as e:
                    print(f"[NPI ERROR] fetching {npi!r}: {e}")


            has_npi = npi_data is not None

            # ----------------------------
            # 2. GOOGLE ADDRESS VERIFICATION
            # ----------------------------
            google_address_data = verify_address(address)

            # ----------------------------
            # 3. GOOGLE PLACE + PHONE
            # ----------------------------
            google_place_data = find_place(name, city)

            # ----------------------------
            # 4. FIELD RESOLUTION
            # ----------------------------
            final_name = npi_data["name"] if npi_data else name
            final_address = (
                google_address_data["formatted_address"]
                if google_address_data
                else (npi_data["address"] if npi_data else address)
            )

            final_phone_raw = (
                google_place_data["phone"]
                if google_place_data and google_place_data.get("phone")
                else (npi_data["phone"] if npi_data else phone)
            )
            final_phone = normalize_phone(final_phone_raw)

            final_specialty = npi_data["specialty"] if npi_data else "Unknown"

            # ----------------------------
            # 5. CONFIDENCE SCORING
            # ----------------------------
            address_conf = calculate_confidence(
                npi_data["address"] if npi_data else None,
                google_address_data["formatted_address"] if google_address_data else None,
                address,
                has_npi
            )

            phone_conf = calculate_confidence(
                npi_data["phone"] if npi_data else None,
                google_place_data["phone"] if google_place_data else None,
                phone,
                has_npi
            )

            identity_conf = 0.9 if has_npi else 0.6

            # ----------------------------
            # 6. BUILD VALIDATION RECORD
            # ----------------------------

            sources = {
                "npi_provided": bool(npi and npi.strip()),
                "npi_verified": bool(npi_data),
                "google_address": bool(google_address_data),
                "google_place": bool(google_place_data),
            }

            # identity status has three useful states:
            # - NPI_VERIFIED: we fetched and matched NPI Registry data
            # - NPI_PROVIDED_UNVERIFIED: NPI present in CSV but registry fetch failed
            # - NPI_MISSING: no NPI provided in CSV
            if sources["npi_verified"]:
                identity_status = "NPI_VERIFIED"
            elif sources["npi_provided"]:
                identity_status = "NPI_PROVIDED_UNVERIFIED"
            else:
                identity_status = "NPI_MISSING"

            validated_record = {
                "provider_id": provider_id,
                "npi": npi if npi and npi.strip() else None,
                "name": final_name,
                "address": final_address,
                "phone": final_phone,
                "specialty": final_specialty,
                "confidence": {
                    "address": address_conf,
                    "phone": phone_conf,
                    "identity": identity_conf
                },
                "sources": sources,
                "identity_status": identity_status
            }
            validated_results.append(validated_record)
            print("[DONE]", validated_record["identity_status"])

    # ----------------------------
    # 7. SAVE OUTPUT
    # ----------------------------
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(validated_results, f, indent=4)

    print(f"\n✅ VALIDATION COMPLETE → Output saved to {OUTPUT_JSON}")


if __name__ == "__main__":
    validate_providers()
