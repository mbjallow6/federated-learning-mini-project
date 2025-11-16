import psycopg2
import pandas as pd
import pickle
import random
import os

# Connect to OMOP
conn = psycopg2.connect(host="localhost", database="omop", user="omop", password="omop")

# Load data
print("Loading OMOP data...")
person = pd.read_sql("SELECT * FROM person", conn)
conditions = pd.read_sql("SELECT * FROM condition_occurrence", conn)
drugs = pd.read_sql("SELECT * FROM drug_exposure", conn)

# De-identify: Remove PII columns
print("De-identifying data...")

# Keep only needed columns
person_clean = person[
    ["person_id", "gender_concept_id", "year_of_birth", "race_concept_id"]
].copy()

conditions_clean = conditions[
    [
        "condition_occurrence_id",
        "person_id",
        "condition_concept_id",
        "condition_start_date",
    ]
].copy()

drugs_clean = drugs[
    ["drug_exposure_id", "person_id", "drug_concept_id", "drug_exposure_start_date"]
].copy()


random.seed(42)  # Reproducible

date_shift = random.randint(1, 30)
conditions_clean["condition_start_date"] = pd.to_datetime(
    conditions_clean["condition_start_date"]
) + pd.Timedelta(days=date_shift)

drugs_clean["drug_exposure_start_date"] = pd.to_datetime(
    drugs_clean["drug_exposure_start_date"]
) + pd.Timedelta(days=date_shift)

print(f"Shifted dates by {date_shift} days")

# Create combined dataset
data = {"person": person_clean, "conditions": conditions_clean, "drugs": drugs_clean}

# Save as RData file
output_file = "data/sample_omop_data.rda"
print(f"Saving to {output_file}...")


os.makedirs("data", exist_ok=True)

with open(output_file.replace(".rda", ".pkl"), "wb") as f:
    pickle.dump(data, f)

print("✓ De-identified data ready")
print(f"  Persons: {len(person_clean)}")
print(f"  Conditions: {len(conditions_clean)}")
print(f"  Drugs: {len(drugs_clean)}")

conn.close()
