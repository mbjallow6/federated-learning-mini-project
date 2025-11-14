# Federated-Learning-Mini-Project

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

## Getting started
```bash
git clone &lt;your-repo&gt;
cd FEDERATED-LEARNING-MINI-PROJECT
python -m venv venv && source venv/bin/activate
pip install -e .[dev]
pre-commit install

| Folder                 | Purpose                            |
| ---------------------- | ---------------------------------- |
| `etl_omop_fhir/`       | Synthea → OMOP → FHIR pipeline     |
| `datashield_vignette/` | DataSHIELD federated analysis demo |
| `governance/`          | EHDS-aligned OPA policies          |
