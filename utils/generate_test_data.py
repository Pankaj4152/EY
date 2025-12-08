"""
Generate realistic test data for provider directory validation
Includes: Real NPIs, missing NPIs, wrong addresses, duplicates, edge cases
"""
import csv
import random
from typing import List, Dict
import os

# Real NPI numbers for testing (publicly available from CMS)
REAL_NPIS = [
    "1447268107",  # 10 PARK PHARMACY INC
    "1013900436",  # Example physician
    "1528070886",  # Example physician
    "1679576722",  # Example physician
    "1790778072",  # Example physician
    "1992755990",  # Example physician
    "1396789543",  # Example physician
    "1164445287",  # Example physician
    "1235123456",  # Example physician
    "1578912345",  # Example physician
]

# Realistic provider names
PROVIDER_NAMES = [
    "Dr. Sarah Johnson", "Dr. Michael Chen", "Dr. Emily Rodriguez",
    "Dr. James Wilson", "Dr. Maria Garcia", "Dr. Robert Taylor",
    "Dr. Jennifer Lee", "Dr. David Martinez", "Dr. Lisa Anderson",
    "Dr. Christopher Brown", "Dr. Amanda White", "Dr. Daniel Kim",
    "Healthcare Clinic Inc", "Family Medical Center", "City Hospital",
    "Urgent Care Associates", "Pediatric Health Group", "Dental Excellence PC"
]

SPECIALTIES = [
    "Family Medicine", "Internal Medicine", "Pediatrics", "Cardiology",
    "Dermatology", "Orthopedics", "Obstetrics & Gynecology", "Psychiatry",
    "Emergency Medicine", "Anesthesiology", "Radiology", "General Surgery",
    "Dentistry", "Ophthalmology", "ENT", "Neurology"
]

CITIES = [
    "New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX",
    "Phoenix, AZ", "Philadelphia, PA", "San Antonio, TX", "San Diego, CA",
    "Dallas, TX", "San Jose, CA", "Austin, TX", "Jacksonville, FL"
]

STREET_NAMES = [
    "Main St", "Medical Center Dr", "Health Plaza", "Hospital Way",
    "Clinic Ave", "Wellness Blvd", "Healthcare Pkwy", "Doctor's Row"
]


class TestDataGenerator:
    """Generate comprehensive test dataset with various scenarios"""
    
    def __init__(self, num_providers: int = 200):
        self.num_providers = num_providers
        self.providers = []
    
    def generate_dataset(self) -> List[Dict]:
        """Generate complete test dataset with various scenarios"""
        
        # Scenario distribution:
        # 60% - Perfect/Near-perfect records (should be AUTO)
        # 20% - Moderate issues (should be REVIEW)
        # 15% - Significant issues (should be HOLD)
        # 5% - Edge cases and duplicates
        
        num_perfect = int(self.num_providers * 0.60)
        num_moderate = int(self.num_providers * 0.20)
        num_problematic = int(self.num_providers * 0.15)
        num_edge = self.num_providers - num_perfect - num_moderate - num_problematic
        
        print(f"Generating {self.num_providers} test providers:")
        print(f"  - {num_perfect} perfect/near-perfect records")
        print(f"  - {num_moderate} moderate issue records")
        print(f"  - {num_problematic} problematic records")
        print(f"  - {num_edge} edge cases")
        
        # Generate each category
        for i in range(num_perfect):
            self.providers.append(self._generate_perfect_record(i))
        
        for i in range(num_moderate):
            self.providers.append(self._generate_moderate_record(i + num_perfect))
        
        for i in range(num_problematic):
            self.providers.append(self._generate_problematic_record(i + num_perfect + num_moderate))
        
        for i in range(num_edge):
            self.providers.append(self._generate_edge_case(i + num_perfect + num_moderate + num_problematic))
        
        return self.providers
    
    def _generate_perfect_record(self, index: int) -> Dict:
        """Generate record with real NPI, correct address, phone"""
        npi = random.choice(REAL_NPIS)
        name = random.choice(PROVIDER_NAMES)
        city_state = random.choice(CITIES)
        
        return {
            "provider_id": f"PROV_{index+1:04d}",
            "full_name": name,
            "npi": npi,
            "phone": self._generate_phone(),
            "address": f"{random.randint(100, 9999)} {random.choice(STREET_NAMES)}",
            "city": city_state.split(",")[0],
            "state": city_state.split(",")[1].strip(),
            "specialty": random.choice(SPECIALTIES),
            "scenario": "PERFECT"
        }
    
    def _generate_moderate_record(self, index: int) -> Dict:
        """Generate record with minor issues (wrong phone, outdated address)"""
        scenarios = [
            "WRONG_PHONE",
            "WRONG_ADDRESS",
            "MISSING_SPECIALTY",
            "OLD_ADDRESS"
        ]
        scenario = random.choice(scenarios)
        
        npi = random.choice(REAL_NPIS)
        name = random.choice(PROVIDER_NAMES)
        city_state = random.choice(CITIES)
        
        provider = {
            "provider_id": f"PROV_{index+1:04d}",
            "full_name": name,
            "npi": npi,
            "address": f"{random.randint(100, 9999)} {random.choice(STREET_NAMES)}",
            "city": city_state.split(",")[0],
            "state": city_state.split(",")[1].strip(),
            "specialty": random.choice(SPECIALTIES),
            "scenario": scenario
        }
        
        # Apply issue based on scenario
        if scenario == "WRONG_PHONE":
            provider["phone"] = "555-0000"  # Obviously wrong
        elif scenario == "WRONG_ADDRESS":
            provider["address"] = "999 Nonexistent St"
        elif scenario == "MISSING_SPECIALTY":
            provider["specialty"] = "Unknown"
        elif scenario == "OLD_ADDRESS":
            provider["phone"] = self._generate_phone()
            provider["address"] = f"OLD: {provider['address']}"
        else:
            provider["phone"] = self._generate_phone()
        
        return provider
    
    def _generate_problematic_record(self, index: int) -> Dict:
        """Generate record with major issues (missing NPI, multiple problems)"""
        scenarios = [
            "NO_NPI",
            "INVALID_NPI",
            "MULTIPLE_ISSUES",
            "MINIMAL_INFO"
        ]
        scenario = random.choice(scenarios)
        
        name = random.choice(PROVIDER_NAMES)
        city_state = random.choice(CITIES)
        
        provider = {
            "provider_id": f"PROV_{index+1:04d}",
            "full_name": name,
            "city": city_state.split(",")[0],
            "state": city_state.split(",")[1].strip(),
            "scenario": scenario
        }
        
        # Apply issues based on scenario
        if scenario == "NO_NPI":
            provider["npi"] = ""  # Missing NPI
            provider["phone"] = self._generate_phone()
            provider["address"] = f"{random.randint(100, 9999)} {random.choice(STREET_NAMES)}"
            provider["specialty"] = random.choice(SPECIALTIES)
        
        elif scenario == "INVALID_NPI":
            provider["npi"] = "1234567890"  # Invalid/fake NPI
            provider["phone"] = self._generate_phone()
            provider["address"] = f"{random.randint(100, 9999)} {random.choice(STREET_NAMES)}"
            provider["specialty"] = "Unknown"
        
        elif scenario == "MULTIPLE_ISSUES":
            provider["npi"] = ""
            provider["phone"] = "555-0000"
            provider["address"] = "Unknown"
            provider["specialty"] = "Unknown"
        
        elif scenario == "MINIMAL_INFO":
            provider["npi"] = ""
            provider["phone"] = ""
            provider["address"] = f"{random.randint(100, 9999)} {random.choice(STREET_NAMES)}"
            provider["specialty"] = ""
        
        return provider
    
    def _generate_edge_case(self, index: int) -> Dict:
        """Generate edge cases (duplicates, special characters, etc.)"""
        scenarios = [
            "DUPLICATE",
            "SPECIAL_CHARS",
            "VERY_LONG_NAME",
            "MULTIPLE_LOCATIONS"
        ]
        scenario = random.choice(scenarios)
        
        city_state = random.choice(CITIES)
        
        if scenario == "DUPLICATE" and len(self.providers) > 0:
            # Duplicate an existing provider with slight variation
            original = random.choice(self.providers)
            provider = original.copy()
            provider["provider_id"] = f"PROV_{index+1:04d}"
            provider["scenario"] = "DUPLICATE"
            provider["phone"] = self._generate_phone()  # Different phone
        
        elif scenario == "SPECIAL_CHARS":
            provider = {
                "provider_id": f"PROV_{index+1:04d}",
                "full_name": "Dr. José María O'Brien-Smith Jr.",
                "npi": random.choice(REAL_NPIS),
                "phone": self._generate_phone(),
                "address": f"{random.randint(100, 9999)} Saint Mary's Blvd.",
                "city": city_state.split(",")[0],
                "state": city_state.split(",")[1].strip(),
                "specialty": random.choice(SPECIALTIES),
                "scenario": scenario
            }
        
        elif scenario == "VERY_LONG_NAME":
            provider = {
                "provider_id": f"PROV_{index+1:04d}",
                "full_name": "Dr. Alexander Bartholomew Christopher Davidson-Wellington III",
                "npi": random.choice(REAL_NPIS),
                "phone": self._generate_phone(),
                "address": f"{random.randint(100, 9999)} {random.choice(STREET_NAMES)}",
                "city": city_state.split(",")[0],
                "state": city_state.split(",")[1].strip(),
                "specialty": random.choice(SPECIALTIES),
                "scenario": scenario
            }
        
        else:  # MULTIPLE_LOCATIONS
            provider = {
                "provider_id": f"PROV_{index+1:04d}",
                "full_name": random.choice(PROVIDER_NAMES),
                "npi": random.choice(REAL_NPIS),
                "phone": f"{self._generate_phone()} ext. 101",
                "address": f"{random.randint(100, 9999)} {random.choice(STREET_NAMES)}, Suite 200",
                "city": city_state.split(",")[0],
                "state": city_state.split(",")[1].strip(),
                "specialty": f"{random.choice(SPECIALTIES)} (Multi-location)",
                "scenario": scenario
            }
        
        return provider
    
    def _generate_phone(self) -> str:
        """Generate realistic phone number"""
        area_codes = ["212", "213", "312", "415", "617", "713", "818", "214", "310", "202"]
        return f"({random.choice(area_codes)}) {random.randint(200, 999)}-{random.randint(1000, 9999)}"
    
    def save_to_csv(self, output_path: str = "data/input/providers.csv"):
        """Save generated data to CSV"""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        fieldnames = ["provider_id", "full_name", "npi", "phone", "address", "city", "state", "specialty", "scenario"]
        
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for provider in self.providers:
                # Ensure all fields exist
                row = {field: provider.get(field, "") for field in fieldnames}
                writer.writerow(row)
        
        print(f"\nGenerated {len(self.providers)} providers")
        print(f"Saved to: {output_path}")
        
        # Print scenario summary
        scenarios = {}
        for p in self.providers:
            scenario = p.get("scenario", "UNKNOWN")
            scenarios[scenario] = scenarios.get(scenario, 0) + 1
        
        print("\nScenario Distribution:")
        for scenario, count in sorted(scenarios.items()):
            print(f"  {scenario:20s}: {count:3d} ({count/len(self.providers)*100:5.1f}%)")
    
    def save_metadata(self, output_path: str = "data/input/test_metadata.json"):
        """Save test data metadata for validation"""
        import json
        
        metadata = {
            "generated_at": "2024-12-08",
            "total_providers": len(self.providers),
            "real_npis_used": REAL_NPIS,
            "scenario_distribution": {},
            "expected_outcomes": {
                "AUTO_target": "60%",
                "REVIEW_target": "20%",
                "HOLD_target": "20%"
            }
        }
        
        for p in self.providers:
            scenario = p.get("scenario", "UNKNOWN")
            metadata["scenario_distribution"][scenario] = metadata["scenario_distribution"].get(scenario, 0) + 1
        
        with open(output_path, "w") as f:
            json.dump(metadata, f, indent=2)
        
        print(f"Metadata saved to: {output_path}")


def main():
    """Generate test dataset"""
    print("="*80)
    print("PROVIDER TEST DATA GENERATOR")
    print("="*80)
    
    generator = TestDataGenerator(num_providers=200)
    generator.generate_dataset()
    generator.save_to_csv()
    generator.save_metadata()
    
    print("\n" + "="*80)
    print("Test data generation complete!")
    print("="*80)
    print("\nNext steps:")
    print("1. Run validation: python agents/validation_agent.py")
    print("2. Or run full pipeline: python pipeline.py")


if __name__ == "__main__":
    main()