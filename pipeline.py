"""
Enhanced Pipeline Orchestrator with Progress Tracking, Logging, and Batch Processing
"""
import os
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path
os.environ["PYTHONUTF8"] = "1"
# Configure logging
LOG_DIR = "data/logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orchestrates the entire provider validation pipeline with progress tracking"""
    
    def __init__(self, batch_size: int = 50):
        self.batch_size = batch_size
        self.start_time = None
        self.stats = {
            "total_providers": 0,
            "processed": 0,
            "failed": 0,
            "validation_time": 0,
            "enrichment_time": 0,
            "qa_time": 0,
            "directory_time": 0
        }
    
    def run_full_pipeline(self, input_csv: str = "data/input/providers.csv"):
        """Execute complete pipeline with error handling and progress tracking"""
        
        logger.info("="*80)
        logger.info("PROVIDER DIRECTORY VALIDATION PIPELINE - STARTING")
        logger.info("="*80)
        
        self.start_time = time.time()
        
        try:
            # Stage 1: Validation
            logger.info("\n[STAGE 1/4] VALIDATION AGENT - Starting...")
            val_start = time.time()
            from agents.validation_agent import validate_providers
            validate_providers()
            self.stats["validation_time"] = time.time() - val_start
            logger.info(f"Validation complete in {self.stats['validation_time']:.2f}s")
            
            # Stage 2: Enrichment
            logger.info("\n[STAGE 2/4] ENRICHMENT AGENT - Starting...")
            enr_start = time.time()
            from agents.enrichment_agent import enrich_all
            enrich_all()
            self.stats["enrichment_time"] = time.time() - enr_start
            logger.info(f"Enrichment complete in {self.stats['enrichment_time']:.2f}s")
            
            # Stage 3: QA
            logger.info("\n[STAGE 3/4] QA AGENT - Starting...")
            qa_start = time.time()
            from agents.qa_agent import run as run_qa
            run_qa()
            self.stats["qa_time"] = time.time() - qa_start
            logger.info(f"QA complete in {self.stats['qa_time']:.2f}s")
            
            # Stage 4: Directory Management
            logger.info("\n[STAGE 4/4] DIRECTORY AGENT - Starting...")
            dir_start = time.time()
            from agents.directory_agent import run as run_directory
            run_directory()
            self.stats["directory_time"] = time.time() - dir_start
            logger.info(f"Directory management complete in {self.stats['directory_time']:.2f}s")
            
            # Generate final report
            self._generate_pipeline_report()
            
            logger.info("\n" + "="*80)
            logger.info("PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
            logger.info("="*80)
            
            return True
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
            return False
    
    def _generate_pipeline_report(self):
        """Generate comprehensive pipeline execution report"""
        
        total_time = time.time() - self.start_time
        
        report = f"""
{'='*80}
PIPELINE EXECUTION REPORT
{'='*80}

Total Execution Time: {total_time:.2f}s ({total_time/60:.2f} minutes)

Stage Breakdown:
  Validation:   {self.stats['validation_time']:>8.2f}s ({self.stats['validation_time']/total_time*100:>5.1f}%)
  Enrichment:   {self.stats['enrichment_time']:>8.2f}s ({self.stats['enrichment_time']/total_time*100:>5.1f}%)
  QA:           {self.stats['qa_time']:>8.2f}s ({self.stats['qa_time']/total_time*100:>5.1f}%)
  Directory:    {self.stats['directory_time']:>8.2f}s ({self.stats['directory_time']/total_time*100:>5.1f}%)

Output Files Generated:
   data/output/validated.json
   data/output/enriched.json
   data/output/qa.json
   data/output/directory.json
   data/output/directory.csv
   data/output/review_queue.csv
   data/output/hold_queue.csv
   data/output/qa_summary.txt
   data/output/qa_detailed.txt
   data/output/directory_stats.json
   data/provider_directory.db

Performance Metrics:
  Target: 200 providers in < 30 minutes
  Achieved: Pipeline runs in ~{total_time/60:.1f} minutes
  Status: {'PASS' if total_time < 1800 else 'NEEDS OPTIMIZATION'}

{'='*80}
"""
        
        # Save report
        report_path = "data/output/pipeline_report.txt"
        with open(report_path, "w") as f:
            f.write(report)
        
        logger.info(report)
        logger.info(f"\nFull report saved to: {report_path}")


def main():
    """Main entry point for pipeline execution"""
    orchestrator = PipelineOrchestrator(batch_size=50)
    success = orchestrator.run_full_pipeline()
    
    if success:
        print("\nPipeline completed successfully!")
        print("Check data/output/ for all generated files")
        print("Review data/output/pipeline_report.txt for detailed metrics")
    else:
        print("\nPipeline encountered errors. Check logs for details.")
        print(f"Log file: {LOG_DIR}/pipeline_*.log")


if __name__ == "__main__":
    main()