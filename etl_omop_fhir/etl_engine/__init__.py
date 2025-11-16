"""
ETL Engine for Synthea to OMOP CDM conversion

This module provides the complete ETL pipeline for transforming
Synthea synthetic patient data into OMOP Common Data Model format.
"""

from .base_etl import OMOPETLBase
from .synthea_omop_mapper import SyntheaOMOPMapper

__version__ = "1.0.0"
__all__ = ["OMOPETLBase", "SyntheaOMOPMapper"]
