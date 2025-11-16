#!/bin/bash

echo "============================================"
echo "ETL Data Verification"
echo "============================================"

# Query all tables
docker exec omop_postgres psql -U omop -d omop -c "
  SELECT 'person' as table_name, COUNT(*) as count FROM person
  UNION ALL
  SELECT 'observation_period', COUNT(*) FROM observation_period
  UNION ALL
  SELECT 'condition_occurrence', COUNT(*) FROM condition_occurrence
  UNION ALL
  SELECT 'drug_exposure', COUNT(*) FROM drug_exposure
  ORDER BY table_name;"

echo ""
echo "Sample Data:"
echo ""

echo "PERSONS:"
docker exec omop_postgres psql -U omop -d omop -c "
  SELECT person_id, gender_concept_id, year_of_birth
  FROM person LIMIT 1;"

echo ""
echo "CONDITIONS (first 5):"
docker exec omop_postgres psql -U omop -d omop -c "
  SELECT condition_occurrence_id, person_id, condition_source_value, condition_start_date
  FROM condition_occurrence LIMIT 5;"

echo ""
echo "MEDICATIONS (first 5):"
docker exec omop_postgres psql -U omop -d omop -c "
  SELECT drug_exposure_id, person_id, drug_source_value, drug_exposure_start_date
  FROM drug_exposure LIMIT 5;"

echo ""
echo "============================================"
