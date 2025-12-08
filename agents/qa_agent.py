import json
import csv
import os
from typing import Dict, List, Tuple

INPUT_JSON = "data/output/enriched.json"
OUTPUT_JSON = "data/output/qa.json"
REVIEW_CSV = "data/output/review_queue.csv"
EMAIL_DRAFTS = "data/output/email_drafts.txt"
SUMMARY_REPORT = "data/output/qa_summary.txt"
DETAILED_REPORT = "data/output/qa_detailed.txt"

os.makedirs("data/output", exist_ok=True)

# Thresholds (from spec)
TH_AUTO = 0.90
TH_REVIEW = 0.60


def _get_conf(d: Dict, path: List[str], default: float = 0.0) -> float:
    """
    Safe getter for nested confidence values.
    path e.g. ['confidence','address'] or ['enrichment','education','confidence']
    """
    cur = d
    try:
        for p in path:
            cur = cur.get(p, {})
        if isinstance(cur, (int, float)):
            return float(cur)
    except Exception:
        pass
    return default


def compute_profile_confidence(rec: Dict) -> Tuple[float, Dict[str, float]]:
    """
    Weighted, deterministic, explainable score combining:
      - identity (validation) : 0.40
      - address                : 0.15
      - phone                  : 0.10
      - specialty (validated)  : 0.10
      - enrichment: education  : 0.05
      - enrichment: services   : 0.10
      - enrichment: affiliations:0.10

    Weights sum to 1.0

    Returns (total_score, component_scores) for transparency.
    """
    weights = {
        "identity": 0.40,
        "address": 0.15,
        "phone": 0.10,
        "specialty": 0.10,
        "education": 0.05,
        "services": 0.10,
        "affiliations": 0.10,
    }

    # Validation confidences
    identity = _get_conf(rec, ["confidence", "identity"], 0.0)
    address = _get_conf(rec, ["confidence", "address"], 0.0)
    phone = _get_conf(rec, ["confidence", "phone"], 0.0)

    # Specialty: prefer enriched over validated
    spec_val = rec.get("specialty")
    spec_conf = 0.0
    if spec_val and isinstance(spec_val, str) and spec_val.strip().lower() != "unknown":
        spec_conf = 0.6  # moderate if present in validated data
    spec_conf = max(
        spec_conf, _get_conf(rec, ["enrichment", "specialty", "confidence"], 0.0)
    )

    # Enrichment confidences
    edu_conf = _get_conf(rec, ["enrichment", "education", "confidence"], 0.0)
    services_conf = _get_conf(rec, ["enrichment", "services", "confidence"], 0.0)
    aff_conf = _get_conf(rec, ["enrichment", "affiliations", "confidence"], 0.0)

    components = {
        "identity": round(identity, 3),
        "address": round(address, 3),
        "phone": round(phone, 3),
        "specialty": round(spec_conf, 3),
        "education": round(edu_conf, 3),
        "services": round(services_conf, 3),
        "affiliations": round(aff_conf, 3),
    }

    score = sum(components[k] * weights[k] for k in weights)

    # clamp 0..1 and round to 3 decimals
    return round(max(0.0, min(1.0, score)), 3), components


def generate_reasons(rec: Dict, components: Dict[str, float]) -> List[str]:
    """Generate explainable reasons for low confidence scores."""
    reasons = []

    # Identity checks
    identity_status = rec.get("identity_status", "")
    if identity_status == "NPI_MISSING":
        reasons.append("missing_npi")
    elif identity_status == "NPI_PROVIDED_UNVERIFIED":
        reasons.append("npi_unverified")
    elif components["identity"] < 0.7:
        reasons.append("low_identity_confidence")

    # Address checks
    if components["address"] < 0.6:
        reasons.append("low_address_confidence")

    # Phone checks
    if components["phone"] < 0.5:
        reasons.append("low_phone_confidence")

    # Specialty checks
    spec_val = (rec.get("specialty") or "").lower()
    if spec_val == "unknown" or not spec_val:
        if components["specialty"] < 0.5:
            reasons.append("missing_specialty")

    # Education checks
    if components["education"] < 0.4:
        reasons.append("low_education_info")

    # Services checks
    services = rec.get("enrichment", {}).get("services", {}).get("value", [])
    if not services or len(services) == 0:
        reasons.append("no_services_listed")
    elif components["services"] < 0.5:
        reasons.append("low_services_confidence")

    # Affiliations checks
    affiliations = rec.get("enrichment", {}).get("affiliations", {}).get("value", [])
    if not affiliations and components["affiliations"] < 0.5:
        reasons.append("no_affiliations")

    return reasons


def _short_list(items, limit=3):
    """Helper to truncate lists for readability."""
    if not items:
        return []
    return items[:limit]


def _enrichment_highlights(rec: Dict) -> Dict:
    """Extract key enrichment data for reporting."""
    ent = rec.get("enrichment", {})
    edu = ent.get("education", {}) or {}
    spec = ent.get("specialty", {}) or {}
    serv = ent.get("services", {}) or {}
    aff = ent.get("affiliations", {}) or {}
    return {
        "education": {
            "value": edu.get("value"),
            "confidence": edu.get("confidence"),
            "source": edu.get("source"),
        },
        "specialty": {
            "value": spec.get("value"),
            "confidence": spec.get("confidence"),
            "source": spec.get("source"),
        },
        "services_sample": _short_list(serv.get("value", [])),
        "services_count": len(serv.get("value", [])),
        "services_confidence": serv.get("confidence"),
        "affiliations_sample": _short_list(aff.get("value", [])),
        "affiliations_count": len(aff.get("value", [])),
        "affiliations_confidence": aff.get("confidence"),
    }


def decide(rec: Dict) -> Dict:
    """Attach QA decision, score, reasoning and readable description."""
    score, components = compute_profile_confidence(rec)
    reasons = generate_reasons(rec, components)

    if score >= TH_AUTO:
        decision = "AUTO"
    elif score >= TH_REVIEW:
        decision = "REVIEW"
    else:
        decision = "HOLD"

    # brief human-readable one-line description
    provider_id = rec.get("provider_id", "UNKNOWN")
    name = rec.get("name", "UNKNOWN")
    npi = rec.get("npi") or "NPI_NOT_PROVIDED"
    identity_status = rec.get("identity_status", "UNKNOWN")
    address = (rec.get("address") or "N/A")[:50]  # Truncate long addresses
    phone = rec.get("phone", "N/A")

    description = (
        f"{provider_id} | {name} | decision={decision} | score={score} | "
        f"identity={identity_status} | npi={npi}"
    )

    # explainable summary dictionary
    explainable = {
        "provider_id": provider_id,
        "name": name,
        "npi": npi,
        "identity_status": identity_status,
        "address": rec.get("address", "N/A"),
        "phone": phone,
        "profile_confidence": score,
        "component_scores": components,
        "top_reasons": reasons,
        "enrichment_highlights": _enrichment_highlights(rec),
    }

    rec["qa"] = {
        "decision": decision,
        "profile_confidence": score,
        "component_scores": components,
        "reasons": reasons,
        "description": description,
        "explainable_summary": explainable,
    }
    return rec


def generate_summary(results: List[Dict]) -> str:
    """Generate a human-readable summary report."""
    total = len(results)
    if total == 0:
        return "No providers to analyze.\n"

    auto = sum(1 for r in results if r["qa"]["decision"] == "AUTO")
    review = sum(1 for r in results if r["qa"]["decision"] == "REVIEW")
    hold = sum(1 for r in results if r["qa"]["decision"] == "HOLD")

    avg_score = sum(r["qa"]["profile_confidence"] for r in results) / total

    # Component averages for deeper insight
    component_avgs = {}
    for component in ["identity", "address", "phone", "specialty", "education", "services", "affiliations"]:
        component_avgs[component] = sum(
            r["qa"]["component_scores"].get(component, 0.0) for r in results
        ) / total

    # Reason frequency analysis
    reason_counts = {}
    for r in results:
        for reason in r["qa"]["reasons"]:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

    top_reasons = sorted(reason_counts.items(), key=lambda x: x[1], reverse=True)[:5]

    summary = f"""
QA SUMMARY REPORT
{'='*70}

Total Providers: {total}

DECISIONS:
  AUTO:   {auto:3d} ({auto/total*100:5.1f}%) - Ready for production
  REVIEW: {review:3d} ({review/total*100:5.1f}%) - Manual review needed
  HOLD:   {hold:3d} ({hold/total*100:5.1f}%) - Contact provider required

CONFIDENCE METRICS:
  Average Profile Confidence: {avg_score:.3f}
  
  Component Averages:
    Identity:     {component_avgs['identity']:.3f} (weight: 40%)
    Address:      {component_avgs['address']:.3f} (weight: 15%)
    Phone:        {component_avgs['phone']:.3f} (weight: 10%)
    Specialty:    {component_avgs['specialty']:.3f} (weight: 10%)
    Education:    {component_avgs['education']:.3f} (weight:  5%)
    Services:     {component_avgs['services']:.3f} (weight: 10%)
    Affiliations: {component_avgs['affiliations']:.3f} (weight: 10%)

TOP ISSUES:
"""
    if top_reasons:
        for reason, count in top_reasons:
            summary += f"  - {reason:30s}: {count:3d} providers ({count/total*100:5.1f}%)\n"
    else:
        summary += "  No issues detected.\n"

    summary += f"\n{'='*70}\n"
    return summary


def run(input_path: str = INPUT_JSON):
    """Main QA pipeline execution."""
    try:
        with open(input_path, "r", encoding="utf-8") as f:
            records = json.load(f)
    except FileNotFoundError:
        print(f"❌ Input file not found: {input_path}")
        return
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in input file: {e}")
        return

    if not records:
        print("⚠️  No records to process.")
        return

    results = []
    review_rows = []
    email_texts = []
    detailed_texts = []

    for r in records:
        try:
            out = decide(r)
            results.append(out)

            # append human-readable one-line description for quick scan
            detailed_texts.append(out["qa"]["description"] + "\n")

            # append a short block with enrichment highlights and reasons
            expl = out["qa"]["explainable_summary"]
            enr = expl["enrichment_highlights"]
            
            block = (
                f"Provider: {expl['provider_id']} - {expl['name']}\n"
                f"  NPI: {expl['npi']}  |  Identity: {expl['identity_status']}\n"
                f"  Address: {expl['address']}\n"
                f"  Phone: {expl['phone']}\n"
                f"  Profile Confidence: {expl['profile_confidence']}\n"
                f"  Component Scores:\n"
            )
            for comp, val in expl['component_scores'].items():
                block += f"    - {comp:15s}: {val:.3f}\n"
            
            block += f"  Top Reasons: {', '.join(expl['top_reasons']) if expl['top_reasons'] else 'None'}\n"
            block += f"  Enrichment Highlights:\n"
            block += f"    Education: {enr['education']['value'] or 'N/A'} (conf: {enr['education']['confidence']})\n"
            block += f"    Specialty: {enr['specialty']['value'] or 'N/A'} (conf: {enr['specialty']['confidence']})\n"
            block += f"    Services: {enr['services_count']} found (sample: {enr['services_sample']})\n"
            block += f"    Affiliations: {enr['affiliations_count']} found (sample: {enr['affiliations_sample']})\n"
            block += f"{'-'*70}\n\n"
            
            detailed_texts.append(block)

            if out["qa"]["decision"] == "REVIEW":
                # minimal CSV row for ops
                review_rows.append(
                    {
                        "provider_id": out.get("provider_id"),
                        "name": out.get("name"),
                        "npi": out.get("npi") or "",
                        "profile_confidence": out["qa"]["profile_confidence"],
                        "identity_status": out.get("identity_status", ""),
                        "reasons": "|".join(out["qa"]["reasons"]),
                    }
                )

            if out["qa"]["decision"] == "HOLD":
                subj = f"HOLD - Provider {out.get('provider_id')} requires verification"
                body = (
                    f"Provider: {out.get('name')}\n"
                    f"NPI: {out.get('npi') or 'NOT PROVIDED'}\n"
                    f"Identity Status: {out.get('identity_status', 'UNKNOWN')}\n"
                    f"Profile Confidence: {out['qa']['profile_confidence']}\n"
                    f"Issues: {', '.join(out['qa']['reasons'])}\n\n"
                    f"Action Required:\n"
                    f"Please contact the provider to verify their information and "
                    f"request missing documentation.\n\n"
                    f"Contact Info:\n"
                    f"  Phone: {out.get('phone', 'N/A')}\n"
                    f"  Address: {out.get('address', 'N/A')}\n"
                )
                email_texts.append(f"Subject: {subj}\n\n{body}\n\n{'-'*70}\n\n")
        
        except Exception as e:
            print(f"⚠️  Error processing provider {r.get('provider_id', 'UNKNOWN')}: {e}")
            continue

    # Generate summary report
    summary = generate_summary(results)

    # Write outputs
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    if review_rows:
        with open(REVIEW_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(review_rows[0].keys()))
            writer.writeheader()
            writer.writerows(review_rows)

    if email_texts:
        with open(EMAIL_DRAFTS, "w", encoding="utf-8") as f:
            f.writelines(email_texts)

    # Write summary report
    with open(SUMMARY_REPORT, "w", encoding="utf-8") as f:
        f.write(summary)

    # Write detailed per-provider report for quick human review
    with open(DETAILED_REPORT, "w", encoding="utf-8") as f:
        f.writelines(detailed_texts)

    print(summary)
    print(f"\n✅ QA complete -> {OUTPUT_JSON}")
    if review_rows:
        print(f"✅ Review queue -> {REVIEW_CSV} ({len(review_rows)} providers)")
    if email_texts:
        print(f"✅ Email drafts -> {EMAIL_DRAFTS} ({len(email_texts)} drafts)")
    print(f"✅ Detailed per-provider report -> {DETAILED_REPORT} ({len(results)} providers)")


if __name__ == "__main__":
    run()
