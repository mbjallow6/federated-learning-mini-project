"""
Base ETL Framework for Synthea → OMOP CDM
Handles database connections, transactions, and logging
"""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Optional
import yaml

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class OMOPETLBase:
    """Base class for OMOP CDM ETL operations"""

    def __init__(self, db_uri: str, config_path: Optional[Path] = None):
        """
        Initialize ETL engine

        Args:
            db_uri: PostgreSQL connection string
                   e.g., "postgresql://omop:omop@localhost:5432/omop"
            config_path: Path to YAML configuration file
        """
        self.engine = create_engine(db_uri, echo=False)
        self.Session = sessionmaker(bind=self.engine)

        # Load configuration
        if config_path and config_path.exists():
            with open(config_path) as f:
                self.config = yaml.safe_load(f)
        else:
            self.config = self._default_config()

        logger.info(f"ETL engine initialized with config: {self.config['etl']['name']}")

    def _default_config(self) -> Dict:
        """Default ETL configuration"""
        return {
            "etl": {
                "name": "Synthea-to-OMOP",
                "version": "1.0",
                "cdm_version": "5.4",
                "batch_size": 1000,
            },
            "schemas": {"cdm": "public", "vocab": "public", "staging": "staging"},
        }

    def execute_sql(self, sql: str, params: Optional[Dict] = None) -> None:
        """Execute SQL statement"""
        with self.engine.connect() as conn:
            conn.execute(text(sql), params or {})
            conn.commit()

    def read_csv_to_dataframe(self, csv_path: Path) -> pd.DataFrame:
        """Read CSV file into pandas DataFrame"""
        logger.info(f"Loading CSV: {csv_path.name}")
        df = pd.read_csv(csv_path, low_memory=False)
        logger.info(f"  Loaded {len(df)} rows, {len(df.columns)} columns")
        return df

    def bulk_insert(
        self, table_name: str, df: pd.DataFrame, schema: str = "public"
    ) -> None:
        """
        Bulk insert DataFrame into database table

        Args:
            table_name: Target table name
            df: DataFrame to insert
            schema: Database schema
        """
        logger.info(f"Inserting {len(df)} rows into {schema}.{table_name}")

        try:
            df.to_sql(
                name=table_name,
                con=self.engine,
                schema=schema,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=self.config["etl"]["batch_size"],
            )
            logger.info("  ✓ Insert complete")
        except Exception as e:
            logger.error(f"  ✗ Insert failed: {e}")
            raise

    def get_next_id(self, table: str, id_column: str, schema: str = "public") -> int:
        """Get next available ID for auto-increment"""
        with self.engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT COALESCE(MAX({id_column}), 0) + 1 FROM {schema}.{table}")
            )
            return result.scalar()

    def validate_counts(
        self, source_count: int, target_table: str, schema: str = "public"
    ) -> bool:
        """Validate row counts after ETL"""
        with self.engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{target_table}"))
            target_count = result.scalar()

        logger.info(f"Validation: {source_count} source → {target_count} target")
        return target_count >= source_count
