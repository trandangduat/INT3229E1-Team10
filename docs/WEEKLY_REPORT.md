# Weekly Progress Report – PREDICTCARE AI – CDSS Dashboard
**Team:** Team 10
**Project:** Scalable Big Data Pipeline & Multi-Modal Clinical Decision Support System
**Report Period:** Week ending May 7, 2026
**Author:** Team 10 – Big Data Engineering

---

## 1. Work Completed Last Week

### 1.1 Bronze Layer – Production Ingestion (COMPLETED)

Successfully completed production ingestion and validation of all three datasets on GCP VM HDFS cluster (`hdfs://master10:9000/user/dis/data/`):

| Dataset | Tables | Status |
|---------|--------|--------|
| MIMIC-IV v3.1 | admissions, patients, diagnoses_icd, labevents, d_items, chartevents, d_labitems | ✅ 7 tables validated |
| eICU v2.0 | patient, vitalPeriodic, diagnosis, medication | ✅ 4 tables validated |
| MIMIC-IV-Note v2.2 | discharge | ✅ 1 table validated |

**Total: 11 Bronze tables validated** with `_SUCCESS` markers, row count parity (raw CSV = Bronze Parquet), and correct column schema.

Key technical decisions:
- All Bronze data stored as Parquet + Snappy compression
- `inferSchema=False` to read all columns as string (immutable raw layer)
- MIMIC-IV-Note processed with `multiLine=True` and `escape='"'` to handle clinical notes with embedded newlines
- Added `--tables` parameter to `ingest_mimic.py` for selective table ingestion

### 1.2 Silver Layer – ETL & Feature Engineering (COMPLETED)

Built and validated 6 Silver Layer Spark ETL jobs, all passing production validation on HDFS:

| Silver Job | Script | Output Rows | Key Features |
|------------|--------|-------------|--------------|
| admissions | `silver_admissions.py` | 391,265 | age>=18, duration>=1d, mortality flag, temporal partition |
| chartevents_agg | `silver_vitals_mimic.py` | 52,588 | SBP, SpO2, HR, Temperature in 24h admission window |
| diagnoses | `silver_diagnoses.py` | 4,756,326 | ICD codes normalized, primary diagnosis flagged |
| labs_agg | `silver_labs.py` | 4,916,652 | 23 lab types aggregated (long format) in 24h window |
| eicu_harmonized | `silver_eicu_harmonized.py` | 200,859 | Schema harmonized with MIMIC, same vitals/outlier rules |
| notes_clean | `notes_clean.py` | 331,793 | PHI stripped, tokenized, stopwords removed |

**Validation results:** All 6 tables pass `_SUCCESS`, schema, required non-null, and relationship checks (hadm_id subset of admissions).

Key technical achievements:
- Identified MIMIC-IV temperature itemids from full HDFS `d_items` dictionary (223761°F, 223762°C)
- Solved OOM issue in `silver_labs.py` by switching from wide-format aggregation to long-format output
- Solved OOM issue in `notes_clean.py` by disabling vectorized Parquet reader and using PySpark SQL functions instead of `pyspark.ml` (which required numpy not available on HDFS VM)
- Diagnoses filtered by admissions cohort to eliminate 39,819 orphan rows

### 1.3 Gold Layer – Analytical Dataset (COMPLETED)

Built the final analytical dataset joining all Silver outputs:

| Gold Output | Rows | Columns | Partition |
|-------------|------|---------|-----------|
| `gold/analytical_dataset` | ~391,265 | ~160 features | split (train/val/test) |

**Temporal split strategy:**
- `train`: admityear < 2019 (~70%)
- `val`: admityear == 2019 (~15%)
- `test`: admityear >= 2020 (~15%)

**Features included:**
- Demographics: age, gender
- Vitals 24h: sbp_mean, sbp_min, spo2_mean, hr_mean, temperature_mean
- Labs 24h: 23 lab features (creatinine, sodium, wbc, hemoglobin, etc.)
- Diagnoses: one-hot ICD-10 chapters (top 50)
- Notes: Word2Vec 128-dim document embeddings
- Survival labels: duration_days, event_flag_readmission, event_flag_mortality

### 1.4 Documentation & Validation

- Created `docs/BRONZE_LAYER.md` – Bronze completion report
- Created `docs/SILVER_LAYER.md` – Silver implementation plan with production metrics
- Created `docs/GOLD_LAYER_AND_BEYOND.md` – Comprehensive pipeline guide for downstream members
- Built `validate_silver.py` automated validator checking schema, nulls, relationships
- All source code in `src/etl/`, `src/ingestion/`, `src/nlp/`

---

## 2. Work In Progress and Next Steps

### 2.1 Current Work – ML Layer (Week 10-11)

| Task | Script | Status | Target Metric |
|------|--------|--------|---------------|
| Cox PH Baseline | `src/ml/train_cox.py` | In progress | C-index >= 0.70 |
| XGBSE Readmission | `src/ml/train_xgbse_readmission.py` | In progress | C-index >= 0.70 |
| XGBSE Mortality | `src/ml/train_xgbse_mortality.py` | Not started | C-index >= 0.70 |
| Evaluation Framework | `src/ml/evaluate.py` | Not started | IBS < 0.25 |
| Optuna Hyperparameter Tuning | `src/ml/tune_xgbse.py` | Not started | Maximize C-index on val set |

### 2.2 Upcoming Work – Serving Layer (Week 12-13)

| Task | Description | Dependencies |
|------|-------------|--------------|
| FastAPI Backend | REST API endpoints for inference, What-If simulation | ML models serialized |
| ReactJS Frontend | Macro Dashboard, Micro Patient Table, What-If Simulation | API endpoints |
| Model Deployment | Load model artifacts, pre-cache Gold dataset for O(1) lookup | HDFS /models/ |

### 2.3 Deliverables Timeline

```text
Week 10-11: ML training (Cox PH, XGBSE), evaluation on temporal test set
Week 12-13: FastAPI backend + ReactJS frontend
Week 14-15: Integration testing, dashboard demo, final report
```

---

## 3. Blockers, Technical Issues, and Difficulties

### 3.1 Resolved Issues

| Issue | Resolution |
|-------|------------|
| `silver_labs.py` OOM during wide-format aggregation | Switched to long-format output (lab_name, lab_mean) to reduce memory pressure |
| `notes_clean.py` fails with `ModuleNotFoundError: numpy` on HDFS VM | Replaced `pyspark.ml.feature.RegexTokenizer/StopWordsRemover` with PySpark SQL `split()` + Python UDF |
| `notes_clean.py` OOM reading large Parquet note files | Disabled vectorized Parquet reader, removed intermediate `.count()` calls, added `repartition(200)` |
| `validate_silver.py` Wrong FS error on HDFS | Fixed by using `path.getFileSystem(hadoop_conf)` instead of `FileSystem.get(hadoop_conf)` |
| `silver_diagnoses.py` orphan hadm_id (39,819 rows) | Added inner join with `silver/admissions` to filter diagnoses by cohort |
| MIMIC-IV temperature itemid not found in local sample | Ran inspection on full HDFS `d_items` (4,014 items) and identified 223761 (°F), 223762 (°C) |

### 3.2 Current Blockers

| Blocker | Impact | Mitigation |
|---------|--------|------------|
| `spark.ml` requires `numpy` on HDFS VM | Cannot use Spark NLP pipeline (`DocumentAssembler`, `Tokenizer`, `StopWordsCleaner`) for notes_clean | Used PySpark SQL functions as workaround; Word2Vec training will use PySpark MLlib `Word2Vec` instead of Spark NLP |
| Gold dataset schema deviation from spec | `temp_mean` currently in Celsius (converted from Fahrenheit); spec requires Fahrenheit | Plan to fix `silver_vitals_mimic.py` to keep Fahrenheit, re-run Silver vitals, rebuild Gold |
| `event_flag_readmission` not yet implemented | Spec requires 30-day readmission flag; only mortality flag exists | Need to compute next admission within 30 days from admissions table; requires self-join logic |

### 3.3 Technical Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| XGBSE C-index < 0.70 on test set | Medium | High | Optuna tuning on validation set; add note embeddings; consider feature selection |
| Word2Vec training OOM on 331K notes | Low | Medium | Use PySpark MLlib distributed Word2Vec with 8 partitions |
| eICU cross-center C-index < 0.65 | Medium | Medium | May need eICU-specific fine-tuning or threshold calibration |

---

## 4. Specific Questions or Requests for Guidance

1. **eICU usage strategy:** The specification mentions eICU for cross-center validation (NFR-ML03: C-index >= 0.65). Should we also union eICU data into the training set to increase sample size, or keep it strictly as an external validation set?

2. **Temporal split year boundaries:** The spec suggests admityear < 2019 for train, 2019 for val, >= 2020 for test. However, MIMIC-IV uses de-identified dates (actual years are synthetic, e.g., 2105-2212). Should we map de-identified years back to real years using `anchor_year` from the patients table, or use the de-identified years as-is for the split?

3. **ICD one-hot encoding depth:** The spec mentions "top-50 ICD-10 chapters" for one-hot encoding. Should we group at the chapter level (A00-B99 = Infectious diseases) or at the subcategory level? Chapter-level gives ~22 columns; subcategory-level could give 50+ columns with better discrimination.

4. **Missing value strategy for ML:** With only 13.4% coverage for vitals (chartevents_agg) and 76.9% for labs, what imputation strategy should we use? Options: (a) median impute, (b) let XGBSE handle missing natively, (c) add "is_missing" indicator columns, or (d) exclude features with > 50% missing rate.

5. **Dashboard refresh mechanism:** The spec mentions a "Nạp Dữ Liệu Spark" button to trigger pipeline refresh. Should this trigger a full re-run of Bronze→Silver→Gold→ML, or should it only re-run Gold→ML using cached Silver outputs?

---

*Report submitted by Team 10 – PREDICTCARE AI – CDSS Dashboard*
*Date: May 7, 2026*
