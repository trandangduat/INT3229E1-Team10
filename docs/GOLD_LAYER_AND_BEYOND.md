# HƯỚNG DẪN PIPELINE: TỪ SILVER ĐẾN DASHBOARD
**Dự án:** PREDICTCARE AI – CDSS Dashboard (Team 10)

**Đối tượng đọc:** Tất cả thành viên, đặc biệt ML Engineer và Fullstack Developer

**Mục đích:** Hiểu rõ dữ liệu đang có, dữ liệu cần tạo, và cách từng phần kết nối với nhau

**Cập nhật kỹ thuật mới nhất (2026-05-19):**
- Gold đọc trực tiếp `readmission_event_30d` và `mortality_event_12m` từ `silver/admissions`, không tự overwrite lại từ alias cũ.
- `validate_gold.py` và `inspect_gold.py` đã hỗ trợ `--dataset-name` để validate/inspect dataset versioned (ví dụ `analytical_dataset_postdischarge_v2`).
- `build_gold_dataset.py` có warning runtime khi dùng `--include-eicu` cho survival mortality 12 tháng; chỉ nên dùng cho external validation nếu chưa harmonize follow-up label tương ứng.

---

## PHẦN 1: DỮ LIỆU SAU BƯỚC SILVER – Ý NGHĨA VÀ TÁC ĐỘNG

### 1.1 Tóm tắt Pipeline Tổng Thể

Để hiểu Silver Layer quan trọng thế nào, hãy nhìn vào bức tranh lớn:

```text
[57 GB CSV thô]                    [~20 GB Parquet]                [~500 MB Parquet]           [Model + API]
   MIMIC-IV                                Silver                         Gold                    Dashboard
   eICU          ──── Bronze ────>    (đã sạch, đã         ────>  (mỗi hàng =       ────>   S(t), h(t)
   MIMIC-Note                         chuẩn hóa)                  1 bệnh nhân +              RMST, rủi ro
                                                                  ~160 features)
```

Silver Layer là tầng "đã nấu chín" – dữ liệu đã được:
- Cast đúng kiểu dữ liệu (string → number, timestamp)
- Loại bỏ bản ghi lỗi (thiếu ID, tuổi < 18, nhập viện < 1 ngày)
- Tính toán features từ dữ liệu thô (trung bình sinh hiệu 24h, mã ICD chuẩn hóa)
- Chuẩn hóa schema giữa MIMIC-IV và eICU

### 1.2 Bảng Silver và Vai Trò Trong Hệ Thống Dự Đoán

Dưới đây là 6 bảng Silver hiện có, giải thích từng bảng dùng để làm gì trong hệ thống CDSS:

---

#### BẢNG 1: `silver/admissions` – NỀN TẢNG CỦA MỌI THỨ

**Đường dẫn:** `hdfs://master10:9000/user/dis/data/silver/admissions/`
**Số dòng:** 391,265 (mỗi dòng = 1 lần nhập viện)
**Partition:** `admityear`

**Cột chính:**
| Cột | Ý nghĩa | Dùng cho |
|-----|---------|----------|
| `hadm_id` | Mã lần nhập viện (primary key) | Join với mọi bảng khác |
| `subject_id` | Mã bệnh nhân | Nhóm bệnh nhân |
| `admittime` | Thời điểm nhập viện | Tính thời gian, temporal split |
| `dischtime` | Thời điểm xuất viện | Mốc dự đoán, `index_time` |
| `duration_days` | Số ngày nằm viện | **Feature** về length of stay |
| `readmission_time_days` | Số ngày từ xuất viện đến nhập viện kế tiếp (censor at 30d) | Survival label T cho Task 1 |
| `readmission_event_30d` | 1 = tái nhập trong 30 ngày | Survival label E cho Task 1 |
| `mortality_time_days` | Số ngày từ xuất viện đến tử vong (censor at 365d) | Survival label T cho Task 2 |
| `mortality_event_12m` | 1 = tử vong trong 12 tháng sau xuất viện | Survival label E cho Task 2 |
| `event_flag_mortality` | Alias tương thích cho `mortality_event_12m` | Downstream compatibility |
| `age` | Tuổi tại thời điểm nhập viện | Feature nhân khẩu học |
| `gender` | Giới tính | Feature nhân khẩu học |
| `admityear` | Năm nhập viện (de-identified) | Temporal split: train/val/test |

**Tác động đến hệ thống:**
- Đây là **base table** – mọi bảng Silver khác đều join vào đây qua `hadm_id`
- `index_time = dischtime` là mốc cốt lõi cho cả hai bài toán; `duration_days` chỉ là feature, còn survival label phải dùng các cột `readmission_*` và `mortality_*` theo discharge time
- `admityear` quyết định temporal split: năm nào dùng train, năm nào dùng test
- Nếu bảng này có 391,265 dòng thì Gold dataset cũng sẽ có tối đa 391,265 dòng (trước khi filter)

---

#### BẢNG 2: `silver/chartevents_agg` – SINH HIỆU 24H ĐẦU NHẬP VIỆN

**Đường dẫn:** `hdfs://master10:9000/user/dis/data/silver/chartevents_agg/`
**Số dòng:** 52,588 (mỗi dòng = 1 lần nhập viện có đo sinh hiệu)
**Partition:** `admityear`

**Cột chính:**
| Cột | Ý nghĩa | Dùng cho |
|-----|---------|----------|
| `hadm_id` | Mã lần nhập viện | Join với admissions |
| `sbp_mean` | Huyết áp tâm thu trung bình (mmHg) | Feature sinh hiệu |
| `sbp_min` | Huyết áp tâm thu thấp nhất | Feature sinh hiệu |
| `spo2_mean` | Độ bão hòa oxy trung bình (%) | Feature sinh hiệu |
| `hr_mean` | Nhịp tim trung bình (bpm) | Feature sinh hiệu |
| `temperature_mean` | Nhiệt độ trung bình (°F) | Feature sinh hiệu |

**Tác động đến hệ thống:**
- 4 chỉ số sinh hiệu này là **input chính cho NEWS Score** (National Early Warning Score) trên dashboard Micro-Level
- Bác sĩ nhìn SBP, SpO2, HR trên bảng bệnh nhân để đánh giá nhanh tình trạng
- Nếu `hadm_id` nào không có trong bảng này → bệnh nhân đó thiếu dữ liệu sinh hiệu → Gold sẽ có NULL ở các cột SBP/SpO2/HR/Temp
- **Chỉ 52,588 / 391,265 admissions có sinh hiệu** (~13.4%) – điều này bình thường vì chartevents chỉ ghi cho bệnh nhân ICU có monitor

**Outlier filtering đã áp dụng:**
- SBP: 40–300 mmHg
- SpO2: 50–100%
- HR: 20–250 bpm
- Temperature: 25–45°C

---

#### BẢNG 3: `silver/labs_agg` – KẾT QUẢ XÉT NGHIỆM 24H ĐẦU

**Đường dẫn:** `hdfs://master10:9000/user/dis/data/silver/labs_agg/`
**Số dòng:** 4,916,652 (long format – mỗi dòng = 1 admission + 1 loại xét nghiệm)
**Distinct admissions:** 301,193
**Partition:** `admityear`

**Cột chính:**
| Cột | Ý nghĩa | Dùng cho |
|-----|---------|----------|
| `hadm_id` | Mã lần nhập viện | Join với admissions |
| `lab_name` | Tên xét nghiệm (creatinine, sodium, wbc...) | Pivot thành cột riêng |
| `lab_mean` | Giá trị trung bình | Feature xét nghiệm |
| `lab_min` | Giá trị nhỏ nhất | Feature xét nghiệm |
| `lab_max` | Giá trị lớn nhất | Feature xét nghiệm |

**Danh sách 23 loại xét nghiệm:**
hematocrit, hemoglobin, platelet, wbc, creatinine, bun, sodium, potassium, chloride, bicarbonate, anion_gap, glucose, calcium, magnesium, phosphate, inr, pt, ptt, alt, ast, bilirubin_total, albumin, lactate

**Tác động đến hệ thống:**
- **Output dạng long format** – cần **pivot** thành wide format trước khi đưa vào Gold (mỗi lab_name thành 1 cột riêng)
- Xét nghiệm là feature quan trọng cho mô hình: creatinine → chức năng thận, wbc → nhiễm trùng, lactate → sốc
- Nếu admission không có xét nghiệm nào trong 24h đầu → Gold sẽ có NULL ở tất cả cột lab

---

#### BẢNG 4: `silver/diagnoses` – MÃ CHẨN ĐOÁN ICD

**Đường dẫn:** `hdfs://master10:9000/user/dis/data/silver/diagnoses/`
**Số dòng:** 4,756,326 (mỗi dòng = 1 mã chẩn đoán cho 1 lần nhập viện)
**Distinct admissions:** 391,265 (đã filter theo admissions cohort)

**Cột chính:**
| Cột | Ý nghĩa | Dùng cho |
|-----|---------|----------|
| `hadm_id` | Mã lần nhập viện | Join với admissions |
| `icd_code` | Mã ICD (đã uppercase, trim) | One-hot encoding ở Gold |
| `icd_version` | 9 hoặc 10 | Phân biệt ICD-9 vs ICD-10 |
| `seq_num` | Thứ tự chẩn đoán | Xác định chẩn đoán chính |
| `is_primary_diagnosis` | 1 = chẩn đoán chính (seq_num=1) | Feature riêng |
| `primary_icd_code` | Mã ICD chính | Feature riêng |

**Tác động đến hệ thống:**
- Mỗi admission có **trung bình ~12 mã ICD** → khi one-hot sẽ tạo ra hàng trăm cột
- Specification yêu cầu one-hot **top 50 ICD-10 chapters** ở Gold → cần nhóm ICD code thành chapter trước khi one-hot
- Mã ICD quyết định **"Bệnh nhân bị gì?"** – đây là feature có giá trị dự đoán cao nhất
- Dashboard Macro-Level hiển thị "Tỉ Lệ Tử Vong Theo ICD" → cần aggregate từ bảng này

---

#### BẢNG 5: `silver/eicu_harmonized` – DỮ LIỆU eICU ĐÃ CHUẨN HÓA

**Đường dẫn:** `hdfs://master10:9000/user/dis/data/silver/eicu_harmonized/`
**Số dòng:** 200,859 (mỗi dòng = 1 lần nhập ICU)
**Partition:** `hospitalid`

**Cột chính:**
| Cột | Ý nghĩa | Dùng cho |
|-----|---------|----------|
| `stay_id_eicu` | Mã lần nhập ICU (thay cho hadm_id) | Join nội bộ eICU |
| `hospitalid` | Mã bệnh viện | Phân tích theo bệnh viện |
| `event_flag_mortality` | 1 = tử vong | Survival label E (eICU external validation) |
| `sbp_mean`, `spo2_mean`, `hr_mean`, `temperature_mean` | Sinh hiệu 24h đầu | Feature sinh hiệu |
| `age`, `gender` | Nhân khẩu học | Feature nhân khẩu học |

**Tác động đến hệ thống:**
- eICU dùng làm **external validation set** – kiểm tra mô hình train trên MIMIC có generalize tốt không
- Hoặc **union vào training set** để tăng kích thước dữ liệu huấn luyện
- Schema đã harmonized với MIMIC → có thể union trực tiếp sau khi đổi tên cột
- Cột `hospitalid` giúp phân tích hiệu suất mô hình theo từng bệnh viện

---

#### BẢNG 6: `silver/notes_clean` – GHI CHÚ LÂM SÀNG ĐÃ LÀM SẠCH

**Đường dẫn:** `hdfs://master10:9000/user/dis/data/silver/notes_clean/`
**Số dòng:** 331,793 (mỗi dòng = 1 ghi chú discharge)
**Distinct admissions:** 331,793

**Cột chính:**
| Cột | Ý nghĩa | Dùng cho |
|-----|---------|----------|
| `hadm_id` | Mã lần nhập viện | Join với admissions |
| `note_text_clean` | Text đã strip PHI, lowercase | Input cho Word2Vec |
| `tokens` | Mảng từ đã tokenize, remove stopwords | Input cho Word2Vec |
| `token_count` | Số lượng từ | Chất lượng dữ liệu |

**Tác động đến hệ thống:**
- Bảng này **chưa sẵn sàng cho Gold** – cần thêm bước **Word2Vec training** để tạo `note_embedding[128]`
- Sau khi có embedding, mỗi admission sẽ có vector 128 chiều biểu diễn nội dung ghi chú lâm sàng
- Ghi chú chứa thông tin mà bảng structured không có: diễn biến bệnh, phản ứng thuốc, ghi chú bác sĩ
- Đây là **modal dữ liệu thứ 3** (cùng với structured + vitals) giúp mô hình dự đoán chính xác hơn

---

### 1.3 Ma Trận Coverage – Bao Nhiêu Bệnh Nhân Có Dữ Liệu?

```text
Bảng                    Số admissions    Tỷ lệ / 391,265
─────────────────────────────────────────────────────────
admissions              391,265          100%   ← Base
diagnoses               391,265          100%   ← Ai cũng có chẩn đoán
chartevents_agg          52,588           13.4% ← Chỉ bệnh nhân ICU có monitor
labs_agg                301,193           76.9% ← Phần lớn có xét nghiệm
notes_clean             331,793           84.8% ← Phần lớn có ghi chú
eicu_harmonized        200,859           (riêng eICU, không join MIMIC)
```

**Hệ quả cho Gold:** Khi left join từ admissions, nhiều bệnh nhân sẽ có NULL ở SBP/SpO2/HR/Temp/lab. Mô hình ML phải xử lý được missing values (XGBSE xử lý tự động, Cox PH cần impute). Gold cần giữ `index_time = dischtime` và các cột `readmission_*` / `mortality_*` để train đúng bài toán discharge-planning.

---

## PHẦN 2: KẾ HOẠCH THỰC HIỆN GOLD LAYER

### 2.1 Mục Tiêu Gold Layer

Biến đổi 6 bảng Silver thành **1 bảng duy nhất** – Gold Analytical Dataset:
- Mỗi hàng = 1 bệnh nhân (hadm_id)
- Mỗi cột = 1 feature (~160 cột)
- Sẵn sàng cho ML training mà không cần thêm preprocessing phức tạp

### 2.2 Script Chính: `src/etl/build_gold_dataset.py`

**Lệnh chạy production:**
```bash
# MIMIC only:
spark-submit --driver-memory 6g --conf spark.sql.shuffle.partitions=200 src/etl/build_gold_dataset.py hdfs

# MIMIC + eICU (external validation only):
spark-submit --driver-memory 6g --conf spark.sql.shuffle.partitions=200 src/etl/build_gold_dataset.py hdfs --include-eicu

# Validate default dataset:
spark-submit src/etl/validate_gold.py hdfs

# Validate dataset có suffix:
spark-submit src/etl/validate_gold.py hdfs --dataset-name analytical_dataset_postdischarge_v2

# Inspect dataset có suffix:
spark-submit src/etl/inspect_gold.py hdfs --dataset-name analytical_dataset_postdischarge_v2
```

**Input:**
```text
/data/silver/admissions/          → Base table
/data/silver/chartevents_agg/     → Left join ON hadm_id
/data/silver/labs_agg/            → Pivot + left join ON hadm_id
/data/silver/diagnoses/           → One-hot + left join ON hadm_id
/data/silver/eicu_harmonized/     → Union (optional, --include-eicu)
/data/silver/note_embeddings/     → Left join (optional, --include-notes)
```

**Output (Production):**
```text
/data/gold/analytical_dataset/split=train/          277,735 rows
/data/gold/analytical_dataset/split=val/             58,693 rows
/data/gold/analytical_dataset/split=test/            54,837 rows
/data/gold/analytical_dataset/split=test_external/  200,859 rows (eICU)
```

### 2.3 Các Bước Transform Chi Tiết

#### Bước 1: Load admissions cohort
```python
df_base = spark.read.parquet("silver/admissions")
# 391,265 rows × hadm_id, subject_id, age, gender, duration_days, index_time, readmission_time_days, readmission_event_30d, mortality_time_days, mortality_event_12m, admityear
```

#### Bước 2: Left join vitals
```python
df_vitals = spark.read.parquet("silver/chartevents_agg")
df = df_base.join(df_vitals.select("hadm_id", "sbp_mean", "sbp_min", "spo2_mean", "hr_mean", "temperature_mean"), on="hadm_id", how="left")
# 391,265 rows, thêm 5 cột vitals (NULL nếu không có)
```

#### Bước 3: Pivot labs từ long → wide → left join
```python
df_labs = spark.read.parquet("silver/labs_agg")
# Pivot: mỗi lab_name → 1 cột lab_mean
df_labs_wide = df_labs.groupBy("hadm_id").pivot("lab_name").agg(first("lab_mean"))
df = df.join(df_labs_wide, on="hadm_id", how="left")
# 391,265 rows, thêm 23 cột lab (NULL nếu không có)
```

#### Bước 4: One-hot ICD chapters → left join
```python
df_diag = spark.read.parquet("silver/diagnoses")
# Nhóm ICD-10 code thành chapter (A00-B99 = Chapter 1, C00-D49 = Chapter 2, ...)
# One-hot top 50 chapters → 50 cột icd10_chap_01 ... icd10_chap_50
df = df.join(df_icd_onehot, on="hadm_id", how="left")
# 391,265 rows, thêm 50 cột ICD
```

#### Bước 5: Left join note embeddings (sau khi có Word2Vec)
```python
df_notes = spark.read.parquet("silver/note_embeddings")  # chưa có
df = df.join(df_notes.select("hadm_id", *[f"note_emb_{i}" for i in range(1,129)]), on="hadm_id", how="left")
# 391,265 rows, thêm 128 cột embedding (NULL nếu không có note)
```

#### Bước 6: Thêm cột temporal split
```python
from pyspark.sql.functions import when

# Dùng percentile-based split cho de-identified years
year_bounds = df.filter(col("admityear").isNotNull()).approxQuantile(
    "admityear", [0.70, 0.85], 0.001
)
train_max = int(year_bounds[0])  # 2171
val_max = int(year_bounds[1])    # 2183

df = df.withColumn("split",
    when(col("admityear").isNull(), "test_external")
    .when(col("admityear") <= train_max, "train")
    .when(col("admityear") <= val_max, "val")
    .otherwise("test")
)
# train: 277,735 rows (71%), val: 58,693 (15%), test: 54,837 (14%)
```

#### Bước 7: Union với eICU (optional)
```python
df_eicu = spark.read.parquet("silver/eicu_harmonized")
# Rename cột cho khớp schema Gold, thêm split="test_external"
df = df.unionByName(df_eicu_gold, allowMissingColumns=True)
```

#### Bước 8: Write partitioned by split
```python
df.write.mode("overwrite").partitionBy("split").option("compression", "snappy").parquet("gold/analytical_dataset")
```

### 2.4 Thứ Tự Ưu Tiên Thực Hiện

| Thứ tự | Task | Script | Trạng thái |
|--------|------|--------|-----------|
| 1 | Gold skeleton (admissions + vitals + labs + diagnoses) | `build_gold_dataset.py` | ✅ Hoàn thành |
| 2 | Tính `readmission_event_30d` và `mortality_event_12m` | `silver_admissions.py` | ✅ Hoàn thành |
| 3 | Tạo `silver/note_embeddings` | `train_word2vec.py` + `note_embeddings.py` | ✅ Hoàn thành |
| 4 | Append note embeddings vào Gold | `build_gold_dataset.py --include-notes` | ✅ Hoàn thành |
| 5 | Union eICU vào Gold | `build_gold_dataset.py --include-eicu` | ✅ Hoàn thành |
| 6 | Sửa `temp_mean` giữ nguyên Fahrenheit | `silver_vitals_mimic.py` | ⏳ Cần sửa |

### 2.5 Validation Metrics cho Gold (Production Results)

Sau khi chạy `build_gold_dataset.py` trên production:

```text
- Số dòng: 592,124 (391,265 MIMIC + 200,859 eICU)
- Số cột: 187 (8 base + 6 vitals + 23 labs + 21 ICD chapters + 128 note embeddings + 1 split)
- Tỷ lệ readmission 30 ngày: 19.28% (MIMIC)
- Tỷ lệ mortality 12 tháng: tính theo `patients.dod` từ discharge
- Missing rate vitals: 76.43% (chỉ ICU có monitor)
- Missing rate labs: 63.90%
- Không có duplicate hadm_id
- Temporal split (percentile-based): train 71%, val 15%, test 14%
```

---

## PHẦN 3: DỮ LIỆU SAU BƯỚC GOLD – Ý NGHĨA

### 3.1 Gold Analytical Dataset – Mỗi Hàng Là Gì?

Sau Gold Layer, mỗi hàng trong dataset biểu diễn **1 lần nhập viện** với đầy đủ thông tin:

```text
hadm_id: 10575854
subject_id: 12345
age: 69
gender: 1 (Male)
index_time: dischtime         ← Mốc dự đoán tại thời điểm xuất viện
duration_days: 7              ← Số ngày nằm viện (Feature)
readmission_time_days: 30     ← Censor at 30 days nếu không nhập viện lại
readmission_event_30d: 0      ← Không tái nhập trong 30 ngày
mortality_time_days: 365      ← Censor at 365 ngày nếu không tử vong
mortality_event_12m: 0        ← Không tử vong trong 12 tháng sau xuất viện

── Vitals 24h đầu ──
sbp_mean: 125.3 mmHg
sbp_min: 98.0 mmHg
spo2_mean: 97.2 %
hr_mean: 82.1 bpm
temperature_mean: 98.6 °F

── Labs 24h đầu ──
creatinine: 1.2 mg/dL
sodium: 139.0 mEq/L
wbc: 8.5 K/uL
hemoglobin: 12.3 g/dL
... (23 loại xét nghiệm)

── Chẩn đoán ──
icd10_chap_01: 0   ← Chapter 1: Bệnh nhiễm trùng
icd10_chap_05: 1   ← Chapter 5: Bệnh tim mạch (có)
icd10_chap_11: 0   ← Chapter 11: Bệnh tiêu hóa
... (top 50 chapters)

── Ghi chú lâm sàng ──
note_emb_1: 0.0234
note_emb_2: -0.1456
... (128 chiều)

── Metadata ──
split: train              ← Dùng cho temporal split
```

### 3.2 Gold Dataset Dùng Cho Việc Gì?

| Thành phần downstream | Đọc từ Gold | Output |
|---|---|---|
| **ML Training** | split=train | Mô hình XGBSE, Cox PH |
| **ML Validation** | split=val | Hyperparameter tuning (Optuna) |
| **ML Test** | split=test | C-index, IBS, AUC evaluation |
| **FastAPI Backend** | split=test (hoặc toàn bộ) | Feature vector cho inference |
| **EDA** | Toàn bộ | Statistics, distributions |

### 3.3 Output Của ML Layer (Sau Gold)

ML Layer đọc Gold dataset và tạo ra:

| Artifact | Đường dẫn | Nội dung |
|---|---|---|
| XGBSE Readmission model | `/models/xgbse_readmission_v1/` | Mô hình dự đoán tái nhập 30 ngày |
| XGBSE Mortality model | `/models/xgbse_mortality_v1/` | Mô hình dự đoán tử vong |
| Cox PH model | `/models/cox_readmission_v1/` | Baseline model |
| Evaluation report | `/models/evaluation_report.json` | C-index, IBS, AUC trên test set |

### 3.4 Output Của API Layer (Sau ML)

FastAPI đọc model artifact và Gold dataset cache, trả về:

| Endpoint | Input | Output |
|---|---|---|
| `/api/patients/{hadm_id}` | hadm_id | S(t)[], h(t)[], RMST |
| `/api/predict/whatif` | feature_vector + discharge_location | S(t) mới, delta_RMST |
| `/api/macro/summary` | - | KPI cards, drug demand, ICD mortality |

### 3.5 Output Của Frontend (Sau API)

ReactJS render 3 màn hình:

**Macro Dashboard:**
- 4 KPI cards: Bệnh nhân nội trú, Tỉ lệ tử vong, Báo động thuốc, Dự báo tái nhập
- Biểu đồ Nhu cầu thuốc (top 5)
- Bảng Tỉ lệ tử vong theo ICD

**Micro Dashboard:**
- Bảng bệnh nhân: HADM_ID, SBP, SpO2, PR, điểm rủi ro (màu sắc)
- Nút "Mô Phỏng AI" cho mỗi bệnh nhân

**What-If Simulation:**
- Survival Curve S(t) – đường cong xác suất sống sót
- Hazard Curve h(t) – đường cong rủi ro tức thời
- RMST Score – số ngày an toàn kỳ vọng
- Toggle Tái Nhập Viện (đỏ) / Tử Vong (tím)
- Chọn Discharge Location: Home / Home Health Care / SNF

---

## PHẦN 4: LƯU Ý CHO CÁC THÀNH VIÊN SAU GOLD

### 4.1 Cho ML Engineer (`src/ml/`)

**Hiểu rõ dữ liệu đầu vào:**
- Gold dataset có ~391K rows × ~160 features (ước tính sau khi đầy đủ)
- Nhiều feature sẽ NULL (vitals chỉ có 13.4%, labs 76.9%)
- XGBSE xử lý missing tự động; Cox PH cần impute (khuyến nghị median impute)

**Temporal split đã được quyết định bởi Gold:**
- `split=train` → admityear < 2019
- `split=val` → admityear == 2019
- `split=test` → admityear >= 2020
- **KHÔNG được random split** – phải dùng temporal split để tránh data leakage

**Evaluation bắt buộc:**
- C-index >= 0.70 trên test set (readmission)
- IBS < 0.25 (calibration)
- C-index >= 0.65 trên eICU (cross-center generalizability)

**Lưu model artifacts đúng cấu trúc:**
```text
/models/xgbse_readmission_v1/
    model.pkl
    metadata.json (features list, hyperparams, metrics)
```

**Đọc Gold dataset bằng:**
```python
# Chỉ đọc split=test để evaluate
df_test = spark.read.parquet("gold/analytical_dataset").filter(col("split") == "test")

# Đọc toàn bộ để train
df_train = spark.read.parquet("gold/analytical_dataset").filter(col("split") == "train")
df_val = spark.read.parquet("gold/analytical_dataset").filter(col("split") == "val")
```

### 4.2 Cho Backend Developer (`src/api/`)

**Cần hiểu rõ schema Gold để tạo Pydantic models:**
- Mỗi endpoint cần Pydantic BaseModel validate input
- Feature vector phải đúng thứ tự và kiểu dữ liệu mà model expect
- Nếu thiếu feature → dùng NULL/None, KHÔNG dùng 0 (vì 0 có ý nghĩa lâm sàng)

**Cache strategy:**
- Load model 1 lần khi FastAPI khởi động (startup event)
- Pre-load Gold dataset vào pandas DataFrame, index bởi hadm_id
- S(t), h(t) cache với TTL 5 phút

**Response format chuẩn:**
```json
{
    "status": "success",
    "data": {
        "hadm_id": "10575854",
        "survival_function": [1.0, 0.99, 0.97, ...],
        "hazard_function": [0.0, 0.01, 0.02, ...],
        "rmst": 24.5,
        "risk_score": 35
    },
    "message": null,
    "timestamp": "2026-05-07T12:00:00Z"
}
```

### 4.3 Cho Frontend Developer (`src/frontend/`)

**Cần hiểu rõ API response để render đúng:**
- S(t) là array 30 phần tử (ngày 0→30 cho readmission, tháng 0→12 cho mortality)
- h(t) là array 30 phần tử (derivative của S(t))
- RMST là scalar (số ngày an toàn)

**Color coding cho rủi ro:**
- Đỏ (#E53E3E): rủi ro cao >= 70%
- Vàng (#F59E0B): trung bình 40–69%
- Xanh lá (#10B981): ổn định < 40%

**Dual-Path Toggle logic:**
- Khi toggle sang "Tử Vong" → gọi lại API với mortality model
- Trục X đổi từ 0–30 ngày sang 0–12 tháng
- Màu đường cong đổi từ đỏ sang tím

**Biểu đồ dùng Recharts:**
```jsx
// Ví dụ Survival Curve
<LineChart data={survivalData}>
    <Line type="monotone" dataKey="st" stroke="#E53E3E" />
    <XAxis dataKey="day" label="Ngày" />
    <YAxis label="Xác suất sống sót" domain={[0, 1]} />
</LineChart>
```

### 4.4 Cho Tất Cả Thành Viên

**Quy tắc vàng khi làm việc với dữ liệu pipeline:**
1. **Đọc tài liệu trước khi code** – hiểu schema, không đoán
2. **Không hard-code đường dẫn** – dùng base_path từ config/argparse
3. **Kiểm tra null trước khi dùng** – nhiều feature sẽ NULL
4. **Log đủ metrics** – row count, null count, distribution
5. **Validate output sau mỗi bước** – dùng `validate_gold.py` (sẽ tạo)

---

## PHẦN 5: CHECKLIST NGHIỆM THU

### Bronze Layer ✅
- [x] Ingest MIMIC-IV, eICU, MIMIC-IV-Note vào HDFS
- [x] Parquet + Snappy format
- [x] Immutable, không filter
- [x] Validate 11 bảng PASS

### Silver Layer ✅
- [x] `silver/admissions` – 391,265 rows
- [x] `silver/chartevents_agg` – 52,588 rows
- [x] `silver/diagnoses` – 4,756,326 rows
- [x] `silver/labs_agg` – 4,916,652 rows
- [x] `silver/eicu_harmonized` – 200,859 rows
- [x] `silver/notes_clean` – 331,793 rows
- [x] Relationship checks PASS

### Silver Layer – Đã sửa
- [ ] `temp_mean` giữ nguyên Fahrenheit (hiện đang convert sang Celsius)
- [x] Tính `readmission_event_30d` và `mortality_event_12m` theo discharge time – Production: readmission 19.28%
- [x] Tạo `silver/note_embeddings` (Word2Vec 128-dim, vocab_size 35660)

### Gold Layer ✅
- [x] `build_gold_dataset.py` – skeleton (admissions + vitals + labs + diagnoses)
- [x] Pivot labs long → wide (23 lab features)
- [x] One-hot ICD chapters (21 chapters, ICD-9 + ICD-10)
- [x] Temporal split (train/val/test) – percentile-based 70/15/15
- [x] Union eICU (`--include-eicu` flag)
- [x] Append note embeddings (`--include-notes` flag, output tại `analytical_dataset_with_notes`)
- [x] Validate Gold dataset – production PASS

### Gold Layer Production Results
| Metric | Value |
|--------|-------|
| Total rows | 592,124 |
| MIMIC rows | 391,265 |
| eICU rows | 200,859 |
| Total columns | 187 (đã kèm 128 cột note embeddings) |
| Distinct hadm_id | 592,124 (no duplicates) |

**Temporal Split (percentile-based 70/15/15):**
| Split | Rows | Admityear | Mortality | Readmission |
|-------|------|-----------|-----------|-------------|
| train | 277,735 | 2105-2171 | 2.12% | 19.17% |
| val | 58,693 | 2172-2183 | 2.11% | 19.31% |
| test | 54,837 | 2184-2212 | 1.71% | 19.81% |
| test_external | 200,859 | N/A (eICU) | 5.43% | 0% |

**Feature Coverage:**
| Feature Group | Columns | Missing Rate |
|--------------|---------|-------------|
| Demographics | 6 | 0% (MIMIC) |
| Vitals 24h | 6 | 76.43% (ICU patients only) |
| Labs 24h | 23 | 63.90% |
| ICD Chapters | 21 | 0.17% |

### ML Layer – Chưa bắt đầu
- [ ] Cox PH baseline
- [ ] XGBSE readmission model
- [ ] XGBSE mortality model
- [ ] Evaluation: C-index, IBS, AUC
- [ ] Model serialization

### Serving Layer – Chưa bắt đầu
- [ ] FastAPI endpoints
- [ ] What-If Simulation logic
- [ ] Macro Dashboard aggregation
- [ ] ReactJS frontend

---

## PHẦN 6: TÀI LIỆU THAM KHẢO

| Tài liệu | Nội dung | Đường dẫn |
|---|---|---|
| Design Specification | Đặc tả đầy đủ hệ thống | `docs/specification.pdf` |
| Bronze Layer Report | Báo cáo hoàn thành Bronze | `docs/BRONZE_LAYER.md` |
| Silver Layer Plan & Status | Kế hoạch và kết quả Silver | `docs/SILVER_LAYER.md` |
| Dataset Sample Commands | Lệnh lấy mẫu dữ liệu | `docs/DATASET_SAMPLE_CMDS.md` |
| Source Code – Ingestion | Scripts ingestion Bronze | `src/ingestion/` |
| Source Code – ETL | Scripts Silver + Gold | `src/etl/` |
| Source Code – NLP | Scripts NLP pipeline | `src/nlp/` |

---

*Tài liệu được tạo tự động từ kết quả pipeline thực tế. Cập nhật lần cuối: 2026-05-11 (Gold Layer với Note Embeddings production completed).*
