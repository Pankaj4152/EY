from typing import Dict, List, Tuple
import json
import csv
import os
import requests
import logging

# Thresholds (now dynamic: env -> config.py -> defaults)
try:
    import config as cfg  # type: ignore
except Exception:
    cfg = None

TH_AUTO = float(os.getenv("TH_AUTO", str(getattr(cfg, "TH_AUTO", 0.90))))
TH_REVIEW = float(os.getenv("TH_REVIEW", str(getattr(cfg, "TH_REVIEW", 0.60))))

logger = logging.getLogger(__name__)
logger.debug("QA thresholds: TH_AUTO=%s TH_REVIEW=%s", TH_AUTO, TH_REVIEW)

INPUT_JSON = "data/output/enriched.json"
OUTPUT_JSON = "data/output/qa.json"
REVIEW_CSV = "data/output/review_queue.csv"
EMAIL_DRAFTS = "data/output/email_drafts.txt"
SUMMARY_REPORT = "data/output/qa_summary.txt"
DETAILED_REPORT = "data/output/qa_detailed.txt"

os.makedirs("data/output", exist_ok=True)


def _get_conf(d: Dict, path: List[str], default: float = 0.0) -> float:
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
    weights = {
        "identity": 0.40,
        "address": 0.15,
        "phone": 0.10,
        "specialty": 0.10,
        "education": 0.05,
        "services": 0.10,
        "affiliations": 0.10,
    }

    identity = _get_conf(rec, ["confidence", "identity"], 0.0)
    address = _get_conf(rec, ["confidence", "address"], 0.0)
    phone = _get_conf(rec, ["confidence", "phone"], 0.0)

    spec_val = rec.get("specialty")
    spec_conf = 0.0
    if spec_val and isinstance(spec_val, str) and spec_val.strip().lower() != "unknown":
        spec_conf = 0.6
    spec_conf = max(spec_conf, _get_conf(rec, ["enrichment", "specialty", "confidence"], 0.0))

    edu_conf = _get_conf(rec, ["enrichment", "education", "confidence"], 0.0)
    services_conf = _get_conf(rec, ["enrichment", "services", "confidence"], 0.0)
    aff_conf = _get_conf(rec, ["enrichment", "affiliations", "confidence"], 0.0)

    components = {
        "identity": identity,
        "address": address,
        "phone": phone,
        "specialty": spec_conf,
        "education": edu_conf,
        "services": services_conf,
        "affiliations": aff_conf,
    }

    score = sum(components[k] * weights[k] for k in weights)
    return round(max(0.0, min(1.0, score)), 3), components


_S2_MODEL = None
logger = logging.getLogger(__name__)


def _load_sentence_model():
    global _S2_MODEL
    if _S2_MODEL is not None:
        return _S2_MODEL
    try:
        from sentence_transformers import SentenceTransformer, util
        _S2_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
        logger.info("Loaded sentence-transformers model")
        return _S2_MODEL
    except Exception as exc:
        _S2_MODEL = None
        logger.debug("sentence-transformers not available: %s", exc)
        return None


def _semantic_similarity(a: str, b: str) -> float:
    """Return semantic similarity 0..1 using sentence-transformers when available, else 0.0."""
    if not a or not b:
        return 0.0
    model = _load_sentence_model()
    if not model:
        return 0.0
    try:
        from sentence_transformers import util
        emb1 = model.encode(a, convert_to_tensor=True)
        emb2 = model.encode(b, convert_to_tensor=True)
        sim = util.pytorch_cos_sim(emb1, emb2).item()
        # normalize (cosine can be -1..1) -> map to 0..1
        sim01 = max(0.0, (sim + 1.0) / 2.0)
        return float(sim01)
    except Exception:
        return 0.0


def decide(rec: Dict) -> Dict:
    score, components = compute_profile_confidence(rec)
    uplift_applied = None

    # semantic specialty validation: if enrichment.specialty exists and source is URL, fetch page text and compare
    try:
        en_spec = rec.get("enrichment", {}).get("specialty", {}) or {}
        spec_val = en_spec.get("value") if isinstance(en_spec, dict) else en_spec
        spec_src = en_spec.get("source") if isinstance(en_spec, dict) else None
        if spec_val and isinstance(spec_val, str) and spec_src and str(spec_src).startswith("http"):
            # fetch page text (cached by enrichment fetch but we conservatively re-fetch here best-effort)
            page_text = None
            try:
                r = requests.get(str(spec_src), timeout=6, headers={"User-Agent": "Mozilla/5.0"})
                if r.ok:
                    page_text = r.text
            except Exception:
                page_text = None
            if page_text:
                sim = _semantic_similarity(spec_val, page_text)
                if sim > 0.6:
                    boost = min(0.15, (sim - 0.6) * 0.5)
                    components["specialty"] = min(1.0, components["specialty"] + boost)
                    # recompute score
                    score = round(min(1.0, sum(components[k] * w for k, w in {
                        "identity":0.40,"address":0.15,"phone":0.10,"specialty":0.10,"education":0.05,"services":0.10,"affiliations":0.10
                    }.items())), 3)
                    uplift_applied = {"type":"semantic_specialty_boost","value":round(boost,3),"similarity":round(sim,3)}
                    logger.info("Semantic specialty boost applied for %s: sim=%.3f boost=%.3f new_score=%.3f", rec.get("provider_id"), sim, boost, score)
    except Exception as e:
        logger.exception("Error during semantic specialty validation for %s: %s", rec.get("provider_id"), e)

    # existing NPI uplift rule (unchanged)
    if rec.get("identity_status") == "NPI_VERIFIED":
        npi_spec = rec.get("enrichment", {}).get("specialty", {}) or {}
        if npi_spec.get("source") and "NPI" in str(npi_spec.get("source")) and float(npi_spec.get("confidence", 0.0)) >= 0.7:
            uplift = 0.05
            score = round(min(1.0, score + uplift), 3)
            uplift_applied = uplift_applied or {"type":"npi_specialty_uplift","value":uplift,"reason":"NPI-derived specialty present"}
            logger.info("NPI uplift applied for %s: +%.2f -> score=%.3f", rec.get("provider_id"), uplift, score)

    if score >= TH_AUTO:
        decision = "AUTO"
    elif score >= TH_REVIEW:
        decision = "REVIEW"
    else:
        decision = "HOLD"

    # generate reasons (concise)
    reasons = []
    if components["identity"] < 0.7:
        reasons.append("low_identity_confidence")
    if components["address"] < 0.6:
        reasons.append("low_address_confidence")
    if components["education"] < 0.4:
        reasons.append("low_education_info")
    if not rec.get("enrichment", {}).get("services", {}).get("value"):
        reasons.append("no_services_listed")
    if not rec.get("enrichment", {}).get("affiliations", {}).get("value"):
        reasons.append("no_affiliations")
    if uplift_applied:
        reasons.append(uplift_applied["type"])

    # human-readable description
    provider_id = rec.get("provider_id", "UNKNOWN")
    name = rec.get("name", "UNKNOWN")
    npi = rec.get("npi") or "NPI_NOT_PROVIDED"
    identity_status = rec.get("identity_status", "UNKNOWN")
    address = rec.get("address", "N/A")
    phone = rec.get("phone", "N/A")
    description = (
        f"{provider_id} | {name} | decision={decision} | score={score} | "
        f"identity={identity_status} | npi={npi} | address={address} | phone={phone}"
    )

    explainable = {
        "provider_id": provider_id,
        "name": name,
        "npi": npi,
        "identity_status": identity_status,
        "address": address,
        "phone": phone,
        "profile_confidence": score,
        "component_scores": components,
        "top_reasons": reasons,
        "enrichment_highlights": {
            "education": rec.get("enrichment", {}).get("education"),
            "specialty": rec.get("enrichment", {}).get("specialty"),
            "services_sample": (rec.get("enrichment", {}).get("services", {}).get("value") or [])[:3],
            "affiliations_sample": (rec.get("enrichment", {}).get("affiliations", {}).get("value") or [])[:3],
        },
    }
    if uplift_applied:
        explainable["uplift_applied"] = uplift_applied

    rec["qa"] = {
        "decision": decision,
        "profile_confidence": score,
        "component_scores": components,
        "reasons": reasons,
        "description": description,
        "explainable_summary": explainable,
    }
    return rec


def run(input_path: str = INPUT_JSON):
    logger.info("Starting QA run on %s", input_path)
    with open(input_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    results = []
    review_rows = []
    email_texts = []
    detailed_texts = []

    for r in records:
        out = decide(r)
        results.append(out)
        detailed_texts.append(out["qa"]["description"] + "\n")
        expl = out["qa"]["explainable_summary"]
        block = (
            f"Provider: {expl['provider_id']} - {expl['name']}\n"
            f"  NPI: {expl['npi']}  Identity: {expl['identity_status']}\n"
            f"  Address: {expl['address']}\n"
            f"  Phone: {expl['phone']}\n"
            f"  Profile Confidence: {expl['profile_confidence']}\n"
            f"  Component Scores: {expl['component_scores']}\n"
            f"  Top Reasons: {', '.join(expl['top_reasons']) if expl['top_reasons'] else 'None'}\n"
            f"  Enrichment Highlights:\n"
            f"    Education: {expl['enrichment_highlights']['education']}\n"
            f"    Specialty: {expl['enrichment_highlights']['specialty']}\n"
            f"    Services Sample: {expl['enrichment_highlights']['services_sample']}\n"
            f"    Affiliations Sample: {expl['enrichment_highlights']['affiliations_sample']}\n"
            f"{'-'*60}\n\n"
        )
        detailed_texts.append(block)

        if out["qa"]["decision"] == "REVIEW":
            review_rows.append(
                {
                    "provider_id": out.get("provider_id"),
                    "name": out.get("name"),
                    "npi": out.get("npi"),
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
                f"Phone: {out.get('phone', 'N/A')}\n"
                f"Address: {out.get('address', 'N/A')}\n"
            )
            email_texts.append(f"Subject: {subj}\n\n{body}\n\n{'-'*60}\n\n")

    # Generate summary
    total = len(results)
    auto = sum(1 for r in results if r["qa"]["decision"] == "AUTO")
    review = sum(1 for r in results if r["qa"]["decision"] == "REVIEW")
    hold = sum(1 for r in results if r["qa"]["decision"] == "HOLD")
    avg_score = sum(r["qa"]["profile_confidence"] for r in results) / total if total else 0.0

    summary = (
        f"QA SUMMARY REPORT\n{'='*60}\n\n"
        f"Total Providers: {total}\n\n"
        f"DECISIONS:\n  AUTO:   {auto} ({auto/total*100:.1f}%)\n  REVIEW: {review} ({review/total*100:.1f}%)\n  HOLD:   {hold} ({hold/total*100:.1f}%)\n\n"
        f"Average Profile Confidence: {avg_score:.3f}\n\n"
    )

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

    with open(SUMMARY_REPORT, "w", encoding="utf-8") as f:
        f.write(summary)

    with open(DETAILED_REPORT, "w", encoding="utf-8") as f:
        f.writelines(detailed_texts)

    logger.info("QA summary: total=%d auto=%d review=%d hold=%d avg_score=%.3f", total, auto, review, hold, avg_score)
    logger.info("QA complete -> %s", OUTPUT_JSON)
    if review_rows:
        logger.info("Review queue -> %s (%d providers)", REVIEW_CSV, len(review_rows))
    if email_texts:
        logger.info("Email drafts -> %s (%d drafts)", EMAIL_DRAFTS, len(email_texts))
    logger.info("Detailed per-provider report -> %s (%d providers)", DETAILED_REPORT, len(results))


if __name__ == "__main__":
    run()
