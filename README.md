# PREDICTCARE AI

**Hệ thống hỗ trợ quyết định lâm sàng đa phương thức dựa trên Big Data cho dự đoán tái nhập viện và tử vong**

A multi-modal Clinical Decision Support System (CDSS) built on Big Data Engineering for predicting 30-day hospital readmission and 12-month post-discharge mortality.

**Team 10** | VNU University of Engineering and Technology

---

## Overview

PREDICTCARE AI is a discharge-planning CDSS that helps clinicians assess post-discharge risks at the point of hospital discharge. The system processes over 56 GB of raw clinical data from three major EHR datasets through a distributed Medallion pipeline (Bronze → Silver → Gold) on HDFS using Apache Spark, producing a Gold analytical dataset of ~383,000 admission records with 236 clinical features.

The system predicts two survival outcomes:
- **30-day Readmission** — probability of hospital readmission within 30 days after discharge
- **12-month Mortality** — probability of death within 12 months after discharge

Predictions are delivered through an interactive dashboard with Macro-level hospital overview, Micro-level patient monitoring, What-If Simulation, and SHAP-based Explainable AI (xAI) panel.

**Live Demo:** http://152.42.220.73/predictcare-ai/

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        OFFLINE TRAINING PIPELINE                        │
│                                                                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────────┐  │
│  │ MIMIC-IV │    │  Bronze  │    │  Silver  │    │       Gold       │  │
│  │  (46 GB) │    │  (Raw)   │    │(Curated) │    │  (Analytical)    │  │
│  │ eICU 9GB │───▶│ Parquet  │───▶│ Cleaned  │───▶│ 383K × 236 feat  │  │
│  │ Note 1GB │    │ +Snappy  │    │ Features │    │ Temporal Split   │  │
│  └──────────┘    └──────────┘    └──────────┘    └────────┬─────────┘  │
│                                                           │            │
│                                    ┌──────────────────────┼───────┐    │
│                                    │                      │       │    │
│                                    ▼                      ▼       │    │
│                            ┌──────────────┐      ┌──────────────┐ │    │
│                            │  Survival    │      │    xAI       │ │    │
│                            │  Models      │      │  Auxiliary   │ │    │
│                            │ (Cox PH,     │      │  XGBoost     │ │    │
│                            │  XGBSE)      │      │  + SHAP      │ │    │
│                            └──────┬───────┘      └──────┬───────┘ │    │
│                                   │                     │         │    │
│                                   ▼                     ▼         │    │
│                            Model & xAI Registry                   │    │
└───────────────────────────────────────────────────────────────────┼────┘
                                                                    │
┌───────────────────────────────────────────────────────────────────┼────┐
│                        DEPLOYMENT PIPELINE                        │    │
│                                                                   │    │
│  ┌──────────┐    ┌───────────────┐    ┌──────────┐    ┌──────────┘    │
│  │ Feature  │    │  Inference    │    │Prediction │                   │
│  │ Table/   │───▶│  Service     │───▶│ + Expl.  │    ┌──────────┐    │
│  │ Gold     │    │  (FastAPI)    │    │  Store   │───▶│ Frontend │    │
│  └──────────┘    └───────────────┘    └──────────┘    │ Dashboard│    │
│                        ▲                               └──────────┘    │
│                        │                                                │
│                   ┌────┴────────┐                                      │
│                   │ xAI Service │                                      │
│                   │ (SHAP)      │                                      │
│                   └─────────────┘                                      │
└───────────────────────────────────────────────────────────────────────┘
```

---

## Datasets

| Source | Description | Type | Size |
|--------|-------------|------|------|
| **MIMIC-IV** | EHR from Beth Israel Deaconess Medical Center, Boston (2008–2019), 391,265 admissions | Structured + Semi-structured | 46 GB |
| **eICU-CRD** | Multi-center ICU data from 208 US hospitals (2014–2015), 200,859 ICU stays | Structured | 9 GB |
| **MIMIC-IV-Note** | Discharge summaries and radiology reports | Unstructured | 1 GB |

**Total:** ~56.6 GB raw CSV → ~20 GB Parquet after ingestion

---

## Pipeline Layers

### Bronze Layer (Raw Data Ingestion)

- Ingests raw CSV from PhysioNet into HDFS as Parquet+Snappy
- Immutable — no filtering, no schema changes
- Explicit schema (`inferSchema=False`) to prevent OOM on large tables
- 12 tables validated across 3 datasets

### Silver Layer (ETL & Feature Engineering)

Six curated Spark jobs produce clean, validated Silver tables:

| Silver Table | Rows | Description |
|---|---|---|
| `admissions` | 391,265 | Base table with survival labels (readmission + mortality) computed from discharge time |
| `chartevents_agg` | 52,588 | Vital signs (SBP, SpO2, HR, Temperature) aggregated from 24h post-admission window |
| `labs_agg` | 4,916,652 | 23 lab features (creatinine, WBC, hemoglobin, lactate, etc.) in long format |
| `diagnoses` | 4,756,326 | ICD-9/ICD-10 codes normalized, grouped into 21 chapters |
| `eicu_harmonized` | 200,859 | eICU schema harmonized to MIMIC-IV for external validation |
| `notes_clean` | 331,793 | Clinical notes: PHI stripped, tokenized, stopwords removed, Word2Vec 128-dim embeddings |

Key preprocessing steps:
- Outlier filtering: SBP ∈ [40,300], SpO2 ∈ [50,100], HR ∈ [20,250], Temp ∈ [25,45]°C
- Adult patients only (age ≥ 18), valid admissions (duration ≥ 1 day)
- Index time = `dischtime` (discharge time) — all features computed before this point to prevent data leakage
- ICD codes grouped into 21 chapters with one-hot encoding
- NLP pipeline: strip PHI → lowercase → tokenize → remove 106 stopwords → Word2Vec (Skip-gram, 128-dim)

### Gold Layer (Analytical Dataset)

Single flat table ready for ML training:

- **383,051 rows** × **236 features** (including 128 note embedding dimensions)
- Temporal split: Train (70%, ≤ year 2171) | Validation (15%, 2172–2183) | Test (15%, ≥ 2184)
- eICU assigned to `test_external` split for cross-center generalization evaluation
- Missing values preserved as NULL (XGBSE handles natively; Cox PH uses median imputation)

**Feature Schema:**

| Group | Example | Type | Count | Missing Rate |
|-------|---------|------|-------|-------------|
| Demographics | age, gender | Int, Binary | 6 | 0% |
| Vitals (24h) | sbp_mean, spo2_mean | Float | 6 | 76% |
| Labs (24h) | creatinine, wbc | Float | 23 | 64% |
| ICD Chapters | icd10_chap_09_circulatory | Binary | 21 | 0.2% |
| Note Embeddings | note_emb_1 … note_emb_128 | Float | 128 | 15% |
| Metadata | split | String | 3 | 0% |

---

## Machine Learning Models

### Survival Analysis

Two survival models predict time-to-event outcomes:

| Task | Label (E) | Time (T) | Horizon | C-index (Test) |
|------|-----------|----------|---------|----------------|
| **12-Month Mortality** | `mortality_event_12m` | days to death or censor at 365 | 365 days | **0.8441** (XGBSE) |
| **30-Day Readmission** | `readmission_event_30d` | days to next admission or censor at 30 | 30 days | **0.7071** (XGBSE) |

**Models:**
- **Cox PH** (baseline): Semi-parametric, `h(t|x) = h₀(t) exp(βᵀx)`, C-index 0.8221 / 0.642
- **XGBSEStackedWeibull** (primary): Two-stage — XGBoost extracts survival embeddings, then Weibull parametric fitting creates continuous S(t). Tuned with Optuna.

**Outputs:** Survival function S(t), hazard function h(t), RMST (Restricted Mean Survival Time), risk score at clinical horizons.

### Explainable AI (xAI)

- Auxiliary XGBoost classifiers trained for horizon-specific risk (30-day readmission, 12-month mortality)
- SHAP TreeExplainer with 500-sample background computes per-patient feature contributions
- Results split into **Top Risk Factors** (red) and **Top Protective Factors** (green)
- Feature names mapped to Vietnamese for clinician readability

---

## Dashboard

The PredictCare AI dashboard provides four integrated views:

### Macro Dashboard
Hospital-level overview with KPI cards (total patients, average LOS), demographic distributions (age, gender, admission type, insurance), and pre-aggregated statistics from Gold Layer cache.

### Micro Patient Monitoring
Patient-level detail view with vital signs (SBP, SpO2, HR, Temperature) with automatic status labeling, lab results (23 features), ICD-10 diagnosis codes, and "Run AI Analysis" button.

### What-If Simulation (Sandbox)
Counterfactual discharge planning tool that allows clinicians to:
- Compare discharge options: **Home**, **Home Health Care**, **Skilled Nursing Facility (SNF)**
- Adjust clinical parameters via sliders (temperature, hemoglobin, PT, bilirubin)
- View branching survival curves S(t) and cumulative risk comparison
- Quantify ΔRisk and ΔRMST between scenarios

### xAI Explanation Panel
SHAP-based explainability showing per-patient risk and protective factors for both readmission (30-day) and mortality (12-month) tasks, with Vietnamese labels and clinical explanations.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Processing | Apache Spark 3.4.1 (PySpark), HDFS |
| Storage | Parquet + Snappy compression |
| ML Framework | XGBoost, XGBSE, scikit-learn, lifelines |
| Hyperparameter Tuning | Optuna |
| xAI | SHAP (TreeExplainer) |
| NLP | Word2Vec (PySpark MLlib, Skip-gram, 128-dim) |
| Backend | FastAPI (async), SQLite, Pydantic |
| Frontend | ReactJS, Recharts |
| Containerization | Docker, Docker Compose |
| Cloud | GCP (Hadoop/YARN cluster, VM: master10/bigdata2) |
| API Docs | Swagger UI (OpenAPI) |

---

## Project Structure

```
├── data/
│   ├── raw/                          # Sample CSV files for local dev
│   └── processed/                    # Processed data output
├── docs/
│   ├── BRONZE_LAYER.md               # Bronze layer completion report
│   ├── SILVER_LAYER.md               # Silver layer plan & production results
│   ├── GOLD_LAYER_AND_BEYOND.md      # Gold layer guide & downstream integration
│   ├── XAI_INTEGRATION_PLAN.md       # SHAP-based xAI architecture plan
│   ├── xai_shap_what_if_chat.md      # xAI implementation notes
│   ├── DATASET_SAMPLE_CMDS.md        # Dataset sampling commands
│   ├── final_report.pdf              # IEEE-style final report (Vietnamese)
│   └── specification.pdf             # Design specification
├── model/
│   └── mortality_model/
│       └── mortality_models/         # Trained artifacts (.joblib)
├── src/
│   ├── ingestion/                    # Bronze: CSV → Parquet+Snappy
│   │   ├── ingest_mimic.py           # MIMIC-IV ingestion (7 tables)
│   │   ├── ingest_eicu.py            # eICU ingestion (4 tables)
│   │   ├── ingest_notes.py           # MIMIC-IV-Note ingestion (multiline CSV)
│   │   ├── ingest_chartevents.py     # Chartevents-specific ingestion
│   │   ├── ingest_chartevents.ipynb  # Chartevents notebook
│   │   └── validate_bronze.py        # Bronze validation (row count, schema, _SUCCESS)
│   ├── etl/                          # Silver + Gold ETL
│   │   ├── silver_admissions.py      # Base table + survival labels
│   │   ├── silver_vitals_mimic.py    # Vital signs 24h aggregation
│   │   ├── silver_labs.py            # Lab features (long format)
│   │   ├── silver_diagnoses.py       # ICD normalization + primary diagnosis
│   │   ├── silver_eicu_harmonized.py # eICU → MIMIC-IV schema harmonization
│   │   ├── build_gold_dataset.py     # Gold: join all Silver → analytical dataset
│   │   ├── validate_gold.py          # Gold validation
│   │   ├── validate_silver.py        # Silver validation
│   │   ├── inspect_gold.py           # Gold inspection utilities
│   │   └── inspect_silver_inputs.py  # Silver input inspection
│   ├── nlp/
│   │   └── notes_clean.py            # Clinical note cleaning + tokenization
│   ├── ml/
│   │   ├── train_xai_models.py       # Auxiliary XGBoost + SHAP explainer training
│   │   └── test.ipynb                # ML experimentation notebook
│   ├── api/                          # FastAPI backend (planned)
│   └── frontend/                     # ReactJS dashboard (planned)
├── docker-compose.yml                # PySpark dev container (spark-3.4.1)
├── requirements.txt                  # Python dependencies
├── predictcareai_postdischarge_data_training_guide.md  # Data processing & training guide
└── README.md
```

---

## Getting Started

### Prerequisites

- Python 3.9+
- Docker & Docker Compose
- HDFS cluster (for production) or local sample data

### Local Development

```bash
# Start PySpark container
docker compose up -d

# Bronze ingestion (sample data)
sudo docker exec predictcare-spark-dev spark-submit /home/jovyan/src/ingestion/ingest_mimic.py local
sudo docker exec predictcare-spark-dev spark-submit /home/jovyan/src/ingestion/ingest_eicu.py local

# Silver ETL
sudo docker exec predictcare-spark-dev spark-submit /home/jovyan/src/etl/silver_admissions.py local
sudo docker exec predictcare-spark-dev spark-submit /home/jovyan/src/etl/silver_vitals_mimic.py local
sudo docker exec predictcare-spark-dev spark-submit /home/jovyan/src/etl/silver_diagnoses.py local
sudo docker exec predictcare-spark-dev spark-submit /home/jovyan/src/etl/silver_labs.py local
sudo docker exec predictcare-spark-dev spark-submit /home/jovyan/src/etl/silver_eicu_harmonized.py local

# NLP
sudo docker exec predictcare-spark-dev spark-submit /home/jovyan/src/nlp/notes_clean.py local

# Gold dataset
sudo docker exec predictcare-spark-dev spark-submit /home/jovyan/src/etl/build_gold_dataset.py local
```

### Production (HDFS)

```bash
# Bronze ingestion
spark-submit src/ingestion/ingest_mimic.py hdfs
spark-submit src/ingestion/ingest_eicu.py hdfs
spark-submit src/ingestion/ingest_notes.py hdfs

# Silver ETL
spark-submit src/etl/silver_admissions.py hdfs
spark-submit src/etl/silver_vitals_mimic.py hdfs
spark-submit src/etl/silver_diagnoses.py hdfs
spark-submit --driver-memory 6g --conf spark.sql.shuffle.partitions=400 src/etl/silver_labs.py hdfs
spark-submit src/etl/silver_eicu_harmonized.py hdfs
spark-submit --driver-memory 6g --conf spark.sql.shuffle.partitions=200 src/nlp/notes_clean.py hdfs

# Gold dataset (MIMIC + eICU)
spark-submit --driver-memory 6g --conf spark.sql.shuffle.partitions=200 src/etl/build_gold_dataset.py hdfs --include-eicu

# Validation
spark-submit src/ingestion/validate_bronze.py hdfs
spark-submit src/etl/validate_gold.py hdfs
```

All scripts accept `local` or `hdfs` as the first argument to switch between local Docker and HDFS cluster execution.

---

## Branching Strategy

- **main**: Stable, production-ready code
- **dev**: Integration branch for testing
- **feature/\<name\>**: Feature branches (e.g., `feature/setup-pyspark`, `feature/ingest-mimic`)

---

## Team

| Name | Role |
|------|------|
| Trần Tuấn Anh | Team Lead |
| Nguyễn Trung Kiên | Data Engineering |
| Trần Đăng Duật | ML Engineering |
| Nguyễn Văn Vượng | Backend Development |
| Nguyễn Anh Khang | Frontend Development |

**Institution:** VNU University of Engineering and Technology, Hanoi, Vietnam

---

## References

1. A. Johnson et al., "MIMIC-IV," PhysioNet, 2024, v3.1
2. A. E. W. Johnson et al., "MIMIC-IV, a freely accessible electronic health record dataset," Scientific Data, 2023
3. A. Johnson et al., "MIMIC-IV-Note: Deidentified free-text clinical notes," PhysioNet, 2023, v2.2
4. T. Pollard et al., "eICU collaborative research database," PhysioNet, 2019, v2.0
5. T. J. Pollard et al., "The eICU collaborative research database," Scientific Data, 2018
6. A. L. Goldberger et al., "PhysioBank, PhysioToolkit, and PhysioNet," Circulation, 2000
7. D. R. Cox, "Regression models and life-tables," JRSS-B, 1972
8. E. L. Kaplan and P. Meier, "Nonparametric estimation from incomplete observations," JASA, 1958
9. S. Pölsterl, "scikit-survival: A library for time-to-event analysis," JMLR, 2020
10. T. Chen and C. Guestrin, "XGBoost: A scalable tree boosting system," KDD, 2016
11. J. D. Raffa et al., "XGBSE: XGBoost Survival Embeddings," GitHub
12. S. M. Lundberg and S.-I. Lee, "A unified approach to interpreting model predictions," NeurIPS, 2017
13. Apache Spark documentation, https://spark.apache.org/
14. WHO, International Classification of Diseases (ICD)
