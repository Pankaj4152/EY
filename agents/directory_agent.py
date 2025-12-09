import json
import csv
import sqlite3
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging

INPUT_QA = "data/output/qa.json"
DB_PATH = "data/provider_directory.db"
EXPORT_JSON = "data/output/directory.json"
EXPORT_CSV = "data/output/directory.csv"
REVIEW_CSV = "data/output/review_queue.csv"
HOLD_CSV = "data/output/hold_queue.csv"
PDF_FOLDER = "data/output/pdfs"
STATS_JSON = "data/output/directory_stats.json"

os.makedirs(os.path.dirname(EXPORT_JSON), exist_ok=True)
os.makedirs(PDF_FOLDER, exist_ok=True)

logger = logging.getLogger(__name__)


def _connect_db(path: str) -> sqlite3.Connection:
    """Create/connect to SQLite DB with enhanced schema and perform simple migrations."""
    logger.info("Connecting to DB at %s", path)
    conn = sqlite3.connect(path)
    cur = conn.cursor()

    # If providers table does not exist, create full schema
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='providers'")
    if not cur.fetchone():
        logger.info("Creating providers table")
        cur.execute(
            """
            CREATE TABLE providers (
                provider_id TEXT PRIMARY KEY,
                name TEXT,
                npi TEXT,
                identity_status TEXT,
                address TEXT,
                phone TEXT,
                specialty TEXT,
                profile_confidence REAL,
                decision TEXT,
                latest_json TEXT NOT NULL,
                last_updated TEXT NOT NULL
            )
            """
        )
        conn.commit()
    else:
        # Perform idempotent migration: add any missing columns expected by current schema
        expected = {
            "provider_id": "TEXT",
            "name": "TEXT",
            "npi": "TEXT",
            "identity_status": "TEXT",
            "address": "TEXT",
            "phone": "TEXT",
            "specialty": "TEXT",
            "profile_confidence": "REAL",
            "decision": "TEXT",
            "latest_json": "TEXT",
            "last_updated": "TEXT",
        }
        cur.execute("PRAGMA table_info(providers)")
        existing_cols = {row[1] for row in cur.fetchall()}
        for col, ctype in expected.items():
            if col not in existing_cols:
                try:
                    cur.execute(f"ALTER TABLE providers ADD COLUMN {col} {ctype}")
                except sqlite3.OperationalError:
                    # If column addition fails for any reason, continue deterministically
                    pass
        conn.commit()
        logger.info("Providers table exists; performing migrations if needed")
        logger.debug("Finished provider table migrations")

    # Ensure versions table exists and has expected columns (safe migration)
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='versions'")
    if not cur.fetchone():
        logger.info("Creating versions table")
        cur.execute(
            """
            CREATE TABLE versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider_id TEXT NOT NULL,
                version_ts TEXT NOT NULL,
                record_json TEXT NOT NULL,
                change_summary TEXT
            )
            """
        )
        conn.commit()
    else:
        # Check for missing columns in versions and add them if needed
        cur.execute("PRAGMA table_info(versions)")
        existing_versions_cols = {row[1] for row in cur.fetchall()}
        if "change_summary" not in existing_versions_cols:
            try:
                cur.execute("ALTER TABLE versions ADD COLUMN change_summary TEXT")
            except sqlite3.OperationalError:
                pass
        conn.commit()
        logger.debug("Versions table present; ensured columns")

    # Create/support pipeline_runs table if missing
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_ts TEXT NOT NULL,
            total_processed INTEGER,
            auto_approved INTEGER,
            review_needed INTEGER,
            hold_needed INTEGER,
            avg_confidence REAL
        )
        """
    )

    logger.debug("Ensuring indexes")
    # Create indexes (safe after migrations)
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_provider_npi ON providers(npi)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_provider_specialty ON providers(specialty)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_provider_decision ON providers(decision)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_version_provider ON versions(provider_id)")
    except sqlite3.OperationalError:
        logger.warning("Index creation encountered an issue")
    conn.commit()
    logger.info("DB ready")
    return conn


def _get_change_summary(old_rec: Optional[Dict], new_rec: Dict) -> str:
    """Generate human-readable change summary."""
    if not old_rec:
        return "Initial record"
    
    changes = []
    
    # Check confidence change
    old_conf = old_rec.get("qa", {}).get("profile_confidence", 0)
    new_conf = new_rec.get("qa", {}).get("profile_confidence", 0)
    if abs(old_conf - new_conf) > 0.05:
        changes.append(f"confidence {old_conf:.2f}→{new_conf:.2f}")
    
    # Check decision change
    old_dec = old_rec.get("qa", {}).get("decision", "")
    new_dec = new_rec.get("qa", {}).get("decision", "")
    if old_dec != new_dec:
        changes.append(f"decision {old_dec}→{new_dec}")
    
    # Check key field updates
    for field in ["address", "phone", "specialty"]:
        if old_rec.get(field) != new_rec.get(field):
            changes.append(f"{field} updated")
    
    return "; ".join(changes) if changes else "No significant changes"


def _upsert_provider(conn: sqlite3.Connection, provider_id: str, record: Dict):
    logger.info("Upserting provider %s", provider_id)
    try:
        now = datetime.utcnow().isoformat() + "Z"
        rec_json = json.dumps(record, ensure_ascii=False)
        
        # Get old record if exists
        cur = conn.cursor()
        cur.execute("SELECT latest_json FROM providers WHERE provider_id = ?", (provider_id,))
        row = cur.fetchone()
        old_rec = json.loads(row[0]) if row else None
        
        change_summary = _get_change_summary(old_rec, record)
        
        # Extract searchable fields
        name = record.get("name", "")
        npi = record.get("npi", "")
        identity_status = record.get("identity_status", "")
        address = record.get("address", "")
        phone = record.get("phone", "")
        
        # Get specialty from enrichment or validation
        specialty = (
            record.get("enrichment", {}).get("specialty", {}).get("value")
            or record.get("specialty")
            or "Unknown"
        )
        
        profile_confidence = record.get("qa", {}).get("profile_confidence", 0.0)
        decision = record.get("qa", {}).get("decision", "HOLD")
        
        # Upsert providers table
        conn.execute(
            """
            INSERT OR REPLACE INTO providers(
                provider_id, name, npi, identity_status, address, phone, 
                specialty, profile_confidence, decision, latest_json, last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (provider_id, name, npi, identity_status, address, phone, 
             specialty, profile_confidence, decision, rec_json, now),
        )
        
        # Insert version entry
        conn.execute(
            "INSERT INTO versions(provider_id, version_ts, record_json, change_summary) VALUES (?, ?, ?, ?)",
            (provider_id, now, rec_json, change_summary),
        )
        
        conn.commit()
        logger.debug("Upsert complete for %s (change_summary: %s)", provider_id, (change_summary or "")[:120])
    except Exception as exc:
        logger.exception("Failed to upsert provider %s: %s", provider_id, exc)
        raise


def _export_db_to_json_csv(conn: sqlite3.Connection, json_path: str, csv_path: str):
    logger.info("Exporting DB to json=%s csv=%s", json_path, csv_path)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT provider_id, name, npi, identity_status, address, phone, 
               specialty, profile_confidence, decision, latest_json, last_updated 
        FROM providers 
        WHERE decision = 'AUTO'
        ORDER BY name
        """
    )
    rows = cur.fetchall()
    
    records = []
    csv_rows = []
    
    for row in rows:
        provider_id, name, npi, identity_status, address, phone, specialty, profile_confidence, decision, latest_json, last_updated = row
        
        # Full record for JSON
        rec = json.loads(latest_json)
        records.append(rec)
        
        # Flattened record for CSV
        csv_rows.append({
            "provider_id": provider_id,
            "name": name,
            "npi": npi or "",
            "identity_status": identity_status,
            "specialty": specialty,
            "address": address,
            "phone": phone,
            "profile_confidence": profile_confidence,
            "last_updated": last_updated,
        })
    
    # Write JSON
    with open(json_path, "w", encoding="utf-8") as jf:
        json.dump(records, jf, indent=2, ensure_ascii=False)
    
    # Write CSV
    if csv_rows:
        with open(csv_path, "w", newline="", encoding="utf-8") as cf:
            fieldnames = list(csv_rows[0].keys())
            writer = csv.DictWriter(cf, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_rows)
    logger.info("Export complete: wrote %d records to %s", len(records), json_path)


def _generate_stats(conn: sqlite3.Connection, records: List[Dict]) -> Dict:
    """Generate directory statistics."""
    cur = conn.cursor()
    
    # Count by decision
    cur.execute("SELECT decision, COUNT(*) FROM providers GROUP BY decision")
    decision_counts = dict(cur.fetchall())
    
    # Count by specialty (top 10)
    cur.execute(
        """
        SELECT specialty, COUNT(*) as cnt 
        FROM providers 
        WHERE decision = 'AUTO' AND specialty != 'Unknown'
        GROUP BY specialty 
        ORDER BY cnt DESC 
        LIMIT 10
        """
    )
    top_specialties = dict(cur.fetchall())
    
    # Confidence distribution
    cur.execute(
        """
        SELECT 
            COUNT(CASE WHEN profile_confidence >= 0.9 THEN 1 END) as high,
            COUNT(CASE WHEN profile_confidence >= 0.75 AND profile_confidence < 0.9 THEN 1 END) as medium,
            COUNT(CASE WHEN profile_confidence < 0.75 THEN 1 END) as low
        FROM providers
        WHERE decision = 'AUTO'
        """
    )
    conf_dist = cur.fetchone()
    
    # Average confidence by decision
    cur.execute(
        """
        SELECT decision, AVG(profile_confidence) 
        FROM providers 
        GROUP BY decision
        """
    )
    avg_conf_by_decision = dict(cur.fetchall())
    
    # Total records processed
    total = len(records)
    auto = decision_counts.get("AUTO", 0)
    review = decision_counts.get("REVIEW", 0)
    hold = decision_counts.get("HOLD", 0)
    
    stats = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "total_processed": total,
        "by_decision": {
            "AUTO": auto,
            "REVIEW": review,
            "HOLD": hold,
        },
        "by_decision_percentage": {
            "AUTO": round(auto / total * 100, 1) if total > 0 else 0,
            "REVIEW": round(review / total * 100, 1) if total > 0 else 0,
            "HOLD": round(hold / total * 100, 1) if total > 0 else 0,
        },
        "top_specialties": top_specialties,
        "confidence_distribution": {
            "high (>=0.90)": conf_dist[0] if conf_dist else 0,
            "medium (0.75-0.89)": conf_dist[1] if conf_dist else 0,
            "low (<0.75)": conf_dist[2] if conf_dist else 0,
        },
        "avg_confidence_by_decision": {k: round(v, 3) for k, v in avg_conf_by_decision.items()},
    }
    
    return stats


def _record_pipeline_run(conn: sqlite3.Connection, stats: Dict):
    """Record pipeline run metadata."""
    now = datetime.utcnow().isoformat() + "Z"
    conn.execute(
        """
        INSERT INTO pipeline_runs(
            run_ts, total_processed, auto_approved, review_needed, 
            hold_needed, avg_confidence
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            now,
            stats["total_processed"],
            stats["by_decision"]["AUTO"],
            stats["by_decision"]["REVIEW"],
            stats["by_decision"]["HOLD"],
            stats["avg_confidence_by_decision"].get("AUTO", 0.0),
        ),
    )
    conn.commit()


def _generate_simple_pdf(rec: Dict, out_path: str):
    """Generate a simple one-page PDF summary if reportlab is available."""
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
        from reportlab.lib.units import inch

        c = canvas.Canvas(out_path, pagesize=letter)
        
        # Title
        c.setFont("Helvetica-Bold", 14)
        c.drawString(40, 750, "Provider Profile Summary")
        
        # Basic info
        c.setFont("Helvetica-Bold", 11)
        c.drawString(40, 720, "Basic Information")
        c.setFont("Helvetica", 10)
        y = 700
        
        basic_info = [
            ("Provider ID:", rec.get("provider_id")),
            ("Name:", rec.get("name")),
            ("NPI:", rec.get("npi") or "Not Provided"),
            ("Identity Status:", rec.get("identity_status")),
            ("Address:", rec.get("address")),
            ("Phone:", rec.get("phone")),
        ]
        
        for label, value in basic_info:
            c.drawString(40, y, f"{label} {value or 'N/A'}")
            y -= 15
        
        # Quality metrics
        y -= 10
        c.setFont("Helvetica-Bold", 11)
        c.drawString(40, y, "Quality Metrics")
        y -= 15
        
        c.setFont("Helvetica", 10)
        qa = rec.get("qa", {})
        c.drawString(40, y, f"Profile Confidence: {qa.get('profile_confidence', 0):.3f}")
        y -= 15
        c.drawString(40, y, f"Decision: {qa.get('decision', 'UNKNOWN')}")
        y -= 15
        
        reasons = qa.get("reasons", [])
        if reasons:
            c.drawString(40, y, f"Issues: {', '.join(reasons[:3])}")
            y -= 20
        
        # Enrichment highlights
        c.setFont("Helvetica-Bold", 11)
        c.drawString(40, y, "Enrichment Details")
        y -= 15
        
        c.setFont("Helvetica", 10)
        eh = qa.get("explainable_summary", {}).get("enrichment_highlights", {})
        
        specialty = eh.get("specialty", {}).get("value") or "Unknown"
        c.drawString(40, y, f"Specialty: {specialty}")
        y -= 15
        
        education = eh.get("education", {}).get("value") or "Not Available"
        if len(education) > 80:
            education = education[:80] + "..."
        c.drawString(40, y, f"Education: {education}")
        y -= 20
        
        services = eh.get("services_sample", [])
        if services:
            c.drawString(40, y, f"Services ({eh.get('services_count', 0)} total):")
            y -= 15
            for svc in services[:3]:
                if len(svc) > 70:
                    svc = svc[:70] + "..."
                c.drawString(50, y, f"• {svc}")
                y -= 12
        
        y -= 10
        affiliations = eh.get("affiliations_sample", [])
        if affiliations:
            c.drawString(40, y, f"Affiliations ({eh.get('affiliations_count', 0)} total):")
            y -= 15
            for aff in affiliations[:3]:
                if len(aff) > 70:
                    aff = aff[:70] + "..."
                c.drawString(50, y, f"• {aff}")
                y -= 12
        
        # Footer
        c.setFont("Helvetica", 8)
        c.drawString(40, 40, f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        c.showPage()
        c.save()
        return True
    except ImportError:
        print("⚠️  reportlab not installed, skipping PDF generation")
        return False
    except Exception as e:
        print(f"⚠️  PDF generation failed: {e}")
        return False


def run(input_path: str = INPUT_QA):
    logger.info("Directory agent run starting; input=%s", input_path)
    # Load QA output
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

    conn = _connect_db(DB_PATH)

    review_rows = []
    hold_rows = []
    pdf_count = 0

    for rec in records:
        pid = rec.get("provider_id")
        decision = rec.get("qa", {}).get("decision", "HOLD")
        
        # Always upsert to DB for tracking
        _upsert_provider(conn, pid, rec)
        
        # Deterministic handling by decision
        if decision == "AUTO":
            # Generate PDF summary (best-effort)
            pdf_path = os.path.join(PDF_FOLDER, f"{pid}.pdf")
            if _generate_simple_pdf(rec, pdf_path):
                pdf_count += 1
                
        elif decision == "REVIEW":
            review_rows.append({
                "provider_id": pid,
                "name": rec.get("name"),
                "npi": rec.get("npi") or "",
                "identity_status": rec.get("identity_status", ""),
                "profile_confidence": rec.get("qa", {}).get("profile_confidence"),
                "reasons": "|".join(rec.get("qa", {}).get("reasons", [])),
            })
            
        else:  # HOLD
            hold_rows.append({
                "provider_id": pid,
                "name": rec.get("name"),
                "npi": rec.get("npi") or "",
                "identity_status": rec.get("identity_status", ""),
                "profile_confidence": rec.get("qa", {}).get("profile_confidence"),
                "reasons": "|".join(rec.get("qa", {}).get("reasons", [])),
            })

    # Generate and save statistics
    stats = _generate_stats(conn, records)
    with open(STATS_JSON, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)
    
    _record_pipeline_run(conn, stats)

    # Export current directory snapshot (AUTO-approved only)
    _export_db_to_json_csv(conn, EXPORT_JSON, EXPORT_CSV)

    # Write review and hold CSVs
    if review_rows:
        with open(REVIEW_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(review_rows[0].keys()))
            writer.writeheader()
            writer.writerows(review_rows)

    if hold_rows:
        with open(HOLD_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(hold_rows[0].keys()))
            writer.writeheader()
            writer.writerows(hold_rows)

    conn.close()
    
    # Summary output
    print("\n" + "="*70)
    print("DIRECTORY AGENT SUMMARY")
    print("="*70)
    print(f"Total Processed: {stats['total_processed']}")
    print(f"  AUTO:   {stats['by_decision']['AUTO']:3d} ({stats['by_decision_percentage']['AUTO']:5.1f}%)")
    print(f"  REVIEW: {stats['by_decision']['REVIEW']:3d} ({stats['by_decision_percentage']['REVIEW']:5.1f}%)")
    print(f"  HOLD:   {stats['by_decision']['HOLD']:3d} ({stats['by_decision_percentage']['HOLD']:5.1f}%)")
    print(f"\n>= Directory exports:")
    print(f"   - JSON: {EXPORT_JSON}")
    print(f"   - CSV:  {EXPORT_CSV}")
    print(f"   - Stats: {STATS_JSON}")
    
    if review_rows:
        print(f"\n>= Review queue: {REVIEW_CSV} ({len(review_rows)} providers)")
    if hold_rows:
        print(f">= Hold queue: {HOLD_CSV} ({len(hold_rows)} providers)")
    if pdf_count > 0:
        print(f">= PDFs generated: {pdf_count} in {PDF_FOLDER}/")
    
    print(f"\n>= SQLite DB: {DB_PATH}")
    print("="*70 + "\n")


if __name__ == "__main__":
    run()
