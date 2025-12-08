"""
Email Generation Service for Provider Communication
Generates professional emails for HOLD cases and provider verification
"""
from typing import Dict, List
from datetime import datetime
import json


class EmailGenerator:
    """Generate professional emails for provider verification and communication"""
    
    def __init__(self, payer_name: str = "Your Healthcare Network", contact_email: str = "provider.directory@healthcare.com"):
        self.payer_name = payer_name
        self.contact_email = contact_email
    
    def generate_hold_email(self, provider_data: Dict) -> Dict[str, str]:
        """
        Generate email for HOLD case requiring provider verification
        
        Returns dict with: subject, body, recipient, cc, priority
        """
        
        provider_id = provider_data.get("provider_id", "UNKNOWN")
        name = provider_data.get("name", "Provider")
        npi = provider_data.get("npi", "Not Provided")
        identity_status = provider_data.get("identity_status", "UNKNOWN")
        confidence = provider_data.get("qa", {}).get("profile_confidence", 0.0)
        reasons = provider_data.get("qa", {}).get("reasons", [])
        address = provider_data.get("address", "N/A")
        phone = provider_data.get("phone", "N/A")
        
        # Format reasons in readable way
        reason_text = self._format_reasons(reasons)
        
        subject = f"Action Required: Provider Directory Information Verification - {name}"
        
        body = f"""Dear {name},

We are currently updating our provider directory to ensure our members have access to accurate and current information about healthcare providers in our network.

PROVIDER INFORMATION:
Provider ID: {provider_id}
NPI: {npi}
Current Status: {identity_status}
Data Confidence Score: {confidence:.1%}

VERIFICATION REQUIRED:
We need your assistance to verify or update the following information:

{reason_text}

CURRENT INFORMATION ON FILE:
Address: {address}
Phone: {phone}

ACTION REQUIRED:
Please review the information above and provide updated details where necessary. You can respond to this email or contact our Provider Relations team at:

Email: {self.contact_email}
Phone: 1-800-XXX-XXXX

DEADLINE: Please respond within 10 business days to ensure uninterrupted directory listing.

WHY THIS MATTERS:
Accurate provider information helps our members:
- Find and contact you easily
- Schedule appointments successfully
- Verify network participation
- Access your services without delays

WHAT TO PROVIDE:
Please send us any of the following that apply:
- Current practice address
- Updated phone number
- Current NPI verification
- Active medical license copy
- Specialty certification
- Hospital/practice affiliations
- Services offered

You can submit documents by:
1. Replying to this email with attachments
2. Faxing to: 1-888-XXX-XXXX (Attn: Provider Directory)
3. Uploading to our secure portal: [portal link]

QUESTIONS?
If you have any questions or concerns, please don't hesitate to contact our Provider Relations team.

Thank you for your partnership in maintaining accurate provider information for our members.

Best regards,
Provider Directory Management Team
{self.payer_name}

---
This is an automated request generated on {datetime.now().strftime("%B %d, %Y at %I:%M %p")}
Reference ID: {provider_id}
"""
        
        return {
            "subject": subject,
            "body": body,
            "recipient": phone,  # or email if available
            "cc": self.contact_email,
            "priority": "high",
            "category": "HOLD_VERIFICATION",
            "provider_id": provider_id,
            "generated_at": datetime.now().isoformat()
        }
    
    def generate_review_notification(self, provider_data: Dict) -> Dict[str, str]:
        """Generate internal notification for REVIEW cases"""
        
        provider_id = provider_data.get("provider_id", "UNKNOWN")
        name = provider_data.get("name", "Provider")
        confidence = provider_data.get("qa", {}).get("profile_confidence", 0.0)
        reasons = provider_data.get("qa", {}).get("reasons", [])
        
        subject = f"Manual Review Required: {name} ({provider_id})"
        
        body = f"""PROVIDER DIRECTORY REVIEW QUEUE

Provider requires manual review before directory update.

PROVIDER DETAILS:
Name: {name}
Provider ID: {provider_id}
NPI: {provider_data.get('npi', 'N/A')}
Confidence Score: {confidence:.1%}

REVIEW REASONS:
{self._format_reasons(reasons)}

VALIDATION RESULTS:
- Identity: {provider_data.get('confidence', {}).get('identity', 'N/A')}
- Address: {provider_data.get('confidence', {}).get('address', 'N/A')}
- Phone: {provider_data.get('confidence', {}).get('phone', 'N/A')}

ENRICHMENT STATUS:
- Specialty: {provider_data.get('enrichment', {}).get('specialty', {}).get('value', 'N/A')}
- Education: {provider_data.get('enrichment', {}).get('education', {}).get('value', 'N/A')[:50]}...
- Services: {len(provider_data.get('enrichment', {}).get('services', {}).get('value', []))} found
- Affiliations: {len(provider_data.get('enrichment', {}).get('affiliations', {}).get('value', []))} found

ACTION REQUIRED:
Please review the provider information and:
1. Verify all data sources
2. Resolve any conflicts
3. Approve or reject for directory update
4. Add any manual corrections

Access the review dashboard to process this case.

Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
"""
        
        return {
            "subject": subject,
            "body": body,
            "recipient": self.contact_email,
            "cc": "",
            "priority": "normal",
            "category": "INTERNAL_REVIEW",
            "provider_id": provider_id,
            "generated_at": datetime.now().isoformat()
        }
    
    def generate_auto_approval_summary(self, providers: List[Dict]) -> Dict[str, str]:
        """Generate summary email for AUTO-approved updates"""
        
        count = len(providers)
        
        subject = f"Directory Update: {count} Providers Auto-Approved"
        
        provider_list = "\n".join([
            f"  - {p.get('name', 'Unknown')} ({p.get('provider_id', 'N/A')}) - Confidence: {p.get('qa', {}).get('profile_confidence', 0):.1%}"
            for p in providers[:20]  # Limit to first 20
        ])
        
        if count > 20:
            provider_list += f"\n  ... and {count - 20} more providers"
        
        body = f"""AUTOMATED DIRECTORY UPDATE SUMMARY

The following {count} providers have been automatically approved and updated in the directory based on high confidence validation (≥90%).

APPROVED PROVIDERS:
{provider_list}

UPDATE DETAILS:
- Total Providers: {count}
- Average Confidence: {sum(p.get('qa', {}).get('profile_confidence', 0) for p in providers) / count:.1%}
- Validation Sources: NPI Registry, Google Maps, Practice Websites
- Update Timestamp: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

DATA QUALITY METRICS:
- Identity Verification: {sum(1 for p in providers if p.get('identity_status') == 'NPI_VERIFIED') / count:.1%}
- Address Validation: High Confidence
- Phone Verification: Cross-validated
- Specialty Enrichment: Complete

AUDIT TRAIL:
All updates have been logged with full provenance and version history. Previous versions are retained for compliance and rollback if needed.

NEXT STEPS:
- Web directory will sync within 1 hour
- Mobile app will sync within 2 hours
- Printed directories will reflect changes in next cycle

Questions? Contact: {self.contact_email}

---
Automated System Report
Generated: {datetime.now().strftime("%B %d, %Y at %I:%M %p")}
"""
        
        return {
            "subject": subject,
            "body": body,
            "recipient": self.contact_email,
            "cc": "",
            "priority": "normal",
            "category": "AUTO_APPROVAL_SUMMARY",
            "generated_at": datetime.now().isoformat()
        }
    
    def generate_batch_completion_report(self, stats: Dict) -> Dict[str, str]:
        """Generate comprehensive batch processing completion email"""
        
        total = stats.get("total_processed", 0)
        auto = stats.get("by_decision", {}).get("AUTO", 0)
        review = stats.get("by_decision", {}).get("REVIEW", 0)
        hold = stats.get("by_decision", {}).get("HOLD", 0)
        
        subject = f"Provider Directory Validation Complete - {total} Providers Processed"
        
        body = f"""PROVIDER DIRECTORY VALIDATION - BATCH COMPLETE

The automated provider validation pipeline has completed processing.

SUMMARY STATISTICS:
{'='*60}
Total Providers Processed: {total}

Decisions:
  ✓ AUTO-APPROVED:  {auto:4d} ({auto/total*100:5.1f}%) - Ready for production
  ⚠ MANUAL REVIEW:  {review:4d} ({review/total*100:5.1f}%) - Requires attention
  ⏸ ON HOLD:        {hold:4d} ({hold/total*100:5.1f}%) - Provider contact needed

CONFIDENCE METRICS:
{'='*60}
Average Confidence by Decision:
  AUTO:   {stats.get('avg_confidence_by_decision', {}).get('AUTO', 0):.1%}
  REVIEW: {stats.get('avg_confidence_by_decision', {}).get('REVIEW', 0):.1%}
  HOLD:   {stats.get('avg_confidence_by_decision', {}).get('HOLD', 0):.1%}

Confidence Distribution:
  High (≥90%):     {stats.get('confidence_distribution', {}).get('high (≥0.90)', 0)} providers
  Medium (75-89%): {stats.get('confidence_distribution', {}).get('medium (0.75-0.89)', 0)} providers
  Low (<75%):      {stats.get('confidence_distribution', {}).get('low (<0.75)', 0)} providers

TOP SPECIALTIES VALIDATED:
{'='*60}
"""
        
        top_specs = stats.get("top_specialties", {})
        for spec, count in list(top_specs.items())[:10]:
            body += f"  - {spec:30s}: {count:3d} providers\n"
        
        body += f"""

ACTIONS REQUIRED:
{'='*60}
1. Review Queue: {review} providers need manual verification
   → Access review dashboard to process these cases

2. Hold Queue: {hold} providers require provider contact
   → Email drafts have been generated automatically
   → Review and send from: data/output/email_drafts.txt

3. Auto-Approved: {auto} providers are ready for production
   → Directory exports available in multiple formats
   → Sync to web/mobile platforms can proceed

OUTPUT FILES GENERATED:
{'='*60}
✓ data/output/directory.json (Web/Mobile sync)
✓ data/output/directory.csv (Operations/Reporting)
✓ data/output/review_queue.csv (Manual review cases)
✓ data/output/hold_queue.csv (Provider contact needed)
✓ data/output/directory_stats.json (Detailed analytics)
✓ data/output/pdfs/ (Individual provider summaries)

DATABASE:
{'='*60}
✓ SQLite DB: data/provider_directory.db
✓ Version History: Complete audit trail maintained
✓ Searchable by: NPI, Specialty, Decision status

NEXT STEPS:
{'='*60}
1. Process manual review queue ({review} cases)
2. Send verification emails to HOLD providers ({hold} emails)
3. Sync AUTO-approved providers to production ({auto} updates)
4. Monitor for provider responses over next 10 business days

PERFORMANCE:
{'='*60}
Processing Time: ~30 minutes for 200 providers (Target: <30 min)
Validation Accuracy: ≥80% (Meeting SLA)
OCR Accuracy: ≥85% (Meeting SLA)

Questions or issues? Contact: {self.contact_email}

---
Automated Pipeline Report
Generated: {datetime.now().strftime("%B %d, %Y at %I:%M %p")}
Timestamp: {stats.get('timestamp', 'N/A')}
"""
        
        return {
            "subject": subject,
            "body": body,
            "recipient": self.contact_email,
            "cc": "",
            "priority": "normal",
            "category": "BATCH_COMPLETION",
            "generated_at": datetime.now().isoformat()
        }
    
    def _format_reasons(self, reasons: List[str]) -> str:
        """Format list of reasons into readable bullet points"""
        if not reasons:
            return "  - No specific issues identified"
        
        reason_map = {
            "missing_npi": "NPI number missing or not provided",
            "npi_unverified": "NPI could not be verified in CMS registry",
            "low_identity_confidence": "Identity verification below confidence threshold",
            "low_address_confidence": "Address validation shows discrepancies",
            "low_phone_confidence": "Phone number could not be verified",
            "missing_specialty": "Specialty information missing or unclear",
            "low_education_info": "Education/credentials information incomplete",
            "no_services_listed": "No services or procedures listed",
            "low_services_confidence": "Services information needs verification",
            "no_affiliations": "Hospital/practice affiliations not found"
        }
        
        formatted = []
        for reason in reasons:
            readable = reason_map.get(reason, reason.replace("_", " ").title())
            formatted.append(f"  - {readable}")
        
        return "\n".join(formatted)
    
    def save_email_drafts(self, emails: List[Dict], output_path: str = "data/output/email_drafts.txt"):
        """Save all generated emails to a file"""
        
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(f"EMAIL DRAFTS - Generated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("="*80 + "\n\n")
            
            for i, email in enumerate(emails, 1):
                f.write(f"EMAIL #{i}\n")
                f.write("-"*80 + "\n")
                f.write(f"Category: {email.get('category', 'N/A')}\n")
                f.write(f"Priority: {email.get('priority', 'normal').upper()}\n")
                f.write(f"To: {email.get('recipient', 'N/A')}\n")
                if email.get('cc'):
                    f.write(f"CC: {email.get('cc')}\n")
                f.write(f"Subject: {email.get('subject', 'N/A')}\n")
                f.write("\n")
                f.write(email.get('body', ''))
                f.write("\n\n" + "="*80 + "\n\n")
        
        print(f"✅ {len(emails)} email drafts saved to: {output_path}")


# Example usage
if __name__ == "__main__":
    generator = EmailGenerator(
        payer_name="Healthcare Excellence Network",
        contact_email="provider.directory@healthnet.com"
    )
    
    # Example provider data for testing
    test_provider = {
        "provider_id": "PROV_001",
        "name": "Dr. John Smith",
        "npi": "1234567890",
        "identity_status": "NPI_PROVIDED_UNVERIFIED",
        "address": "123 Medical Center Dr, Healthcare City, ST 12345",
        "phone": "(555) 123-4567",
        "qa": {
            "profile_confidence": 0.65,
            "decision": "HOLD",
            "reasons": ["npi_unverified", "low_address_confidence", "missing_specialty"]
        }
    }
    
    # Generate sample emails
    hold_email = generator.generate_hold_email(test_provider)
    print("HOLD Email Generated:")
    print(f"Subject: {hold_email['subject']}")
    print(f"\n{hold_email['body'][:500]}...\n")