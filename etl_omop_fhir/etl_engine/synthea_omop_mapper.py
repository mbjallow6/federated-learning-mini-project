"""
Synthea to OMOP CDM v5.4 Mapping Logic
Based on OHDSI ETL-Synthea reference implementation
"""

import pandas as pd
import logging
from pathlib import Path

try:
    from .base_etl import OMOPETLBase
except ImportError:
    from base_etl import OMOPETLBase

logger = logging.getLogger(__name__)


class SyntheaOMOPMapper(OMOPETLBase):
    """Maps Synthea CSV data to OMOP CDM tables"""

    def __init__(self, db_uri: str, synthea_csv_dir: Path):
        """
        Initialize mapper

        Args:
            db_uri: Database connection string
            synthea_csv_dir: Directory containing Synthea CSV files
        """
        super().__init__(db_uri)
        self.synthea_dir = synthea_csv_dir
        self.person_id_map = {}  # Synthea UUID → OMOP person_id

    def run_etl(self) -> None:
        """Execute complete ETL pipeline"""
        logger.info("=" * 60)
        logger.info("Starting Synthea → OMOP ETL")
        logger.info("=" * 60)

        # ETL Pipeline Steps
        self.map_person()
        self.map_observation_period()
        self.map_condition_occurrence()
        self.map_drug_exposure()
        # self.map_measurement()  # If you have labs/vitals
        # self.map_procedure_occurrence()  # If you have procedures

        logger.info("=" * 60)
        logger.info("✓ ETL Complete")
        logger.info("=" * 60)

    def map_person(self) -> None:
        """Map Synthea patients.csv → OMOP person table"""
        logger.info("\n[1/4] Mapping PERSON...")

        # Load Synthea patients
        patients = self.read_csv_to_dataframe(self.synthea_dir / "patients.csv")

        # Transform to OMOP person
        omop_person = pd.DataFrame()

        # Generate sequential person_ids
        omop_person["person_id"] = range(1, len(patients) + 1)

        # Store mapping for later use
        for synthea_id, omop_id in zip(patients["Id"], omop_person["person_id"]):
            self.person_id_map[synthea_id] = omop_id

        # Gender mapping (OMOP concepts: 8507=Female, 8532=Male)
        omop_person["gender_concept_id"] = (
            patients["GENDER"]
            .map({"F": 8532, "M": 8507, "female": 8532, "male": 8507})
            .fillna(0)
        )

        # Birth date components
        patients["BIRTHDATE"] = pd.to_datetime(patients["BIRTHDATE"])
        omop_person["year_of_birth"] = patients["BIRTHDATE"].dt.year
        omop_person["month_of_birth"] = patients["BIRTHDATE"].dt.month
        omop_person["day_of_birth"] = patients["BIRTHDATE"].dt.day
        omop_person["birth_datetime"] = patients["BIRTHDATE"]

        # Race/Ethnicity (simplified - in production use vocab mappings)
        omop_person["race_concept_id"] = (
            patients["RACE"]
            .map({"white": 8527, "black": 8516, "asian": 8515, "other": 8522})
            .fillna(0)
        )

        omop_person["ethnicity_concept_id"] = (
            patients["ETHNICITY"]
            .map({"hispanic": 38003563, "nonhispanic": 38003564})
            .fillna(0)
        )

        # Location/Provider (set to defaults for now)
        omop_person["location_id"] = None
        omop_person["provider_id"] = None
        omop_person["care_site_id"] = None

        # Source values (for traceability)
        omop_person["person_source_value"] = patients["Id"]
        omop_person["gender_source_value"] = patients["GENDER"]
        omop_person["race_source_value"] = patients["RACE"]
        omop_person["ethnicity_source_value"] = patients["ETHNICITY"]

        # Concept source values (for foreign keys)
        omop_person["gender_source_concept_id"] = 0
        omop_person["race_source_concept_id"] = 0
        omop_person["ethnicity_source_concept_id"] = 0

        # Insert into database
        self.bulk_insert("person", omop_person)
        logger.info(f"  ✓ Mapped {len(omop_person)} persons")

    def map_observation_period(self) -> None:
        """Create observation_period for each person"""
        logger.info("\n[2/4] Mapping OBSERVATION_PERIOD...")

        patients = self.read_csv_to_dataframe(self.synthea_dir / "patients.csv")

        obs_period = pd.DataFrame()
        obs_period["observation_period_id"] = range(1, len(patients) + 1)

        # Map to OMOP person_id
        obs_period["person_id"] = patients["Id"].map(self.person_id_map)

        # Observation period = birth date to death date (or now)
        patients["BIRTHDATE"] = pd.to_datetime(patients["BIRTHDATE"])
        obs_period["observation_period_start_date"] = patients["BIRTHDATE"]

        # If death date exists, use it; otherwise use current date
        patients["DEATHDATE"] = pd.to_datetime(patients["DEATHDATE"], errors="coerce")
        obs_period["observation_period_end_date"] = patients["DEATHDATE"].fillna(
            pd.Timestamp.now()
        )

        # Period type concept (44814724 = "Period covering healthcare encounters")
        obs_period["period_type_concept_id"] = 44814724

        self.bulk_insert("observation_period", obs_period)
        logger.info(f"  ✓ Created {len(obs_period)} observation periods")

    def map_condition_occurrence(self) -> None:
        """Map Synthea conditions.csv → OMOP condition_occurrence"""
        logger.info("\n[3/4] Mapping CONDITION_OCCURRENCE...")

        conditions = self.read_csv_to_dataframe(self.synthea_dir / "conditions.csv")

        if len(conditions) == 0:
            logger.warning("  ⚠ No conditions found in CSV")
            return

        omop_cond = pd.DataFrame()
        omop_cond["condition_occurrence_id"] = range(1, len(conditions) + 1)

        # Map patient IDs
        omop_cond["person_id"] = conditions["PATIENT"].map(self.person_id_map)

        # TODO: Map SNOMED codes to OMOP concept_ids
        # For now, use source code as-is (in production, use CONCEPT table)
        omop_cond["condition_concept_id"] = 0  # Placeholder
        omop_cond["condition_source_value"] = conditions["CODE"]
        omop_cond["condition_source_concept_id"] = 0

        # Dates
        conditions["START"] = pd.to_datetime(conditions["START"])
        omop_cond["condition_start_date"] = conditions["START"]
        omop_cond["condition_start_datetime"] = conditions["START"]

        # End date (if available)
        conditions["STOP"] = pd.to_datetime(conditions["STOP"], errors="coerce")
        omop_cond["condition_end_date"] = conditions["STOP"]
        omop_cond["condition_end_datetime"] = conditions["STOP"]

        # Type concept (32020 = "EHR")
        omop_cond["condition_type_concept_id"] = 32020

        # Status (active = still present)
        omop_cond["condition_status_concept_id"] = (
            conditions["STOP"]
            .isna()
            .map({True: 4203942, False: 4230359})  # Active  # Resolved
        )

        # Optional fields (set to NULL for mini-project)
        omop_cond["condition_status_source_value"] = None
        omop_cond["stop_reason"] = None
        omop_cond["provider_id"] = None
        omop_cond["visit_occurrence_id"] = None
        omop_cond["visit_detail_id"] = None

        self.bulk_insert("condition_occurrence", omop_cond)
        logger.info(f"  ✓ Mapped {len(omop_cond)} conditions")

    def map_drug_exposure(self) -> None:
        """Map Synthea medications.csv → OMOP drug_exposure"""
        logger.info("\n[4/4] Mapping DRUG_EXPOSURE...")

        meds = self.read_csv_to_dataframe(self.synthea_dir / "medications.csv")

        if len(meds) == 0:
            logger.warning("  ⚠ No medications found in CSV")
            return

        omop_drug = pd.DataFrame()
        omop_drug["drug_exposure_id"] = range(1, len(meds) + 1)

        # Map patient IDs
        omop_drug["person_id"] = meds["PATIENT"].map(self.person_id_map)

        # Drug concept (placeholder - in production, map RxNorm codes)
        omop_drug["drug_concept_id"] = 0
        omop_drug["drug_source_value"] = meds["CODE"]
        omop_drug["drug_source_concept_id"] = 0

        # Dates
        meds["START"] = pd.to_datetime(meds["START"])
        omop_drug["drug_exposure_start_date"] = meds["START"]
        omop_drug["drug_exposure_start_datetime"] = meds["START"]

        meds["STOP"] = pd.to_datetime(meds["STOP"], errors="coerce")
        omop_drug["drug_exposure_end_date"] = meds["STOP"].fillna(
            meds["START"]  # If no stop date, assume same as start
        )
        omop_drug["drug_exposure_end_datetime"] = omop_drug["drug_exposure_end_date"]

        # Type concept (38000177 = "Prescription written")
        omop_drug["drug_type_concept_id"] = 38000177

        # Optional fields
        omop_drug["stop_reason"] = meds["REASONCODE"]
        omop_drug["refills"] = None
        omop_drug["quantity"] = None
        omop_drug["days_supply"] = (
            omop_drug["drug_exposure_end_date"] - omop_drug["drug_exposure_start_date"]
        ).dt.days
        omop_drug["sig"] = None
        omop_drug["route_concept_id"] = 0
        omop_drug["lot_number"] = None
        omop_drug["provider_id"] = None
        omop_drug["visit_occurrence_id"] = None
        omop_drug["visit_detail_id"] = None
        omop_drug["route_source_value"] = None
        omop_drug["dose_unit_source_value"] = None

        self.bulk_insert("drug_exposure", omop_drug)
        logger.info(f"  ✓ Mapped {len(omop_drug)} drug exposures")


def main():
    """Main ETL execution"""
    import os
    from pathlib import Path

    # Configuration
    DB_URI = os.getenv("OMOP_DB_URI", "postgresql://omop:omop@localhost:5432/omop")

    SYNTHEA_CSV_DIR = Path(__file__).parent.parent / "data" / "raw"

    # Run ETL
    mapper = SyntheaOMOPMapper(db_uri=DB_URI, synthea_csv_dir=SYNTHEA_CSV_DIR)

    mapper.run_etl()

    print("\n" + "=" * 60)
    print("ETL Pipeline Complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
