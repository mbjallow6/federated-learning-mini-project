#!/usr/bin/env python3
"""
Main entry point for Synthea → OMOP ETL pipeline

Usage:
    python etl_omop_fhir/run_etl.py
    OMOP_DB_URI="postgresql://..." python etl_omop_fhir/run_etl.py
"""

import sys
import os
import logging
from pathlib import Path

# Setup paths
current_dir = Path(__file__).parent.absolute()
project_root = current_dir.parent
sys.path.insert(0, str(current_dir))
sys.path.insert(0, str(project_root))

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Set environment defaults
os.environ.setdefault("OMOP_DB_URI", "postgresql://omop:omop@localhost:5432/omop")


def main():
    """Execute ETL pipeline"""
    try:
        from etl_engine.synthea_omop_mapper import SyntheaOMOPMapper

        db_uri = os.getenv("OMOP_DB_URI")
        synthea_dir = current_dir / "data" / "raw"

        logger.info(f"Starting ETL with database: {db_uri}")
        logger.info(f"Synthea data directory: {synthea_dir}")

        # Verify CSV files exist
        csv_files = list(synthea_dir.glob("*.csv"))
        if not csv_files:
            logger.error(f"No CSV files found in {synthea_dir}")
            return False

        logger.info(f"Found {len(csv_files)} CSV files: {[f.name for f in csv_files]}")

        # Run ETL
        mapper = SyntheaOMOPMapper(db_uri=db_uri, synthea_csv_dir=synthea_dir)

        mapper.run_etl()

        logger.info("=" * 60)
        logger.info("✓ ETL Pipeline Complete Successfully!")
        logger.info("=" * 60)

        return True

    except Exception as e:
        logger.error(f"✗ ETL Pipeline Failed: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
