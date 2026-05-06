# KẾ HOẠCH TRIỂN KHAI SILVER LAYER
**Dự án:** PREDICTCARE AI - CDSS Dashboard (Team 10)
**Vai trò:** Big Data Engineering - Distributed ETL & Feature Engineering
**Phạm vi:** Bronze -> Silver transformation sau khi production ingestion hoàn tất
**Trạng thái hiện tại:** MIMIC-IV và eICU đã hoàn tất production Bronze ingestion + validation trên VM/HDFS. MIMIC-IV-Note đang pending.

---

## 1. MỤC TIÊU SILVER LAYER

Silver Layer là tầng dữ liệu đã được làm sạch, chuẩn hóa schema, validate kiểu dữ liệu và tạo các feature nền tảng cho Gold Analytical Dataset.

Khác với Bronze Layer, Silver Layer được phép thực hiện:

*   Cast kiểu dữ liệu từ string sang `Integer`, `Double`, `Timestamp`, `Date`.
*   Rename và harmonize schema giữa MIMIC-IV và eICU.
*   Filter các bản ghi không hợp lệ theo rule lâm sàng.
*   Join các bảng cần thiết để tạo entity-level dataset.
*   Aggregate dữ liệu event-level thành admission-level hoặc patient-level features.
*   Tạo label cho survival analysis như `duration_days`, `event_flag_readmission`, `event_flag_mortality`.

Nguyên tắc quan trọng:

*   Bronze không bị sửa đổi.
*   Mọi logic làm sạch phải nằm ở Silver và có thể trace về Bronze.
*   Silver jobs phải chạy bằng Spark trên HDFS, không dùng pandas cho dữ liệu lớn.
*   Mỗi job nên có input/output rõ ràng, log đầy đủ, và validation metrics.

---

## 2. INPUT VÀ OUTPUT DỰ KIẾN

### 2.1. Input Bronze

Sau production ingestion, các input chính đã được validate nằm tại:

```text
hdfs://master10:9000/user/dis/data/bronze/mimic_iv/admissions/
hdfs://master10:9000/user/dis/data/bronze/mimic_iv/patients/
hdfs://master10:9000/user/dis/data/bronze/mimic_iv/diagnoses_icd/
hdfs://master10:9000/user/dis/data/bronze/mimic_iv/labevents/
hdfs://master10:9000/user/dis/data/bronze/mimic_iv/d_items/
hdfs://master10:9000/user/dis/data/bronze/mimic_iv/chartevents/
hdfs://master10:9000/user/dis/data/bronze/eicu/patient/
hdfs://master10:9000/user/dis/data/bronze/eicu/vitalPeriodic/
hdfs://master10:9000/user/dis/data/bronze/eicu/diagnosis/
hdfs://master10:9000/user/dis/data/bronze/eicu/medication/
```

MIMIC-IV-Note sẽ bổ sung sau khi xử lý `discharge.csv`:

```text
hdfs://master10:9000/user/dis/data/bronze/mimic_iv_note/discharge/
```

### 2.2. Output Silver

Output Silver dự kiến:

```text
hdfs://master10:9000/user/dis/data/silver/admissions/
hdfs://master10:9000/user/dis/data/silver/chartevents_agg/
hdfs://master10:9000/user/dis/data/silver/labs_agg/
hdfs://master10:9000/user/dis/data/silver/diagnoses/
hdfs://master10:9000/user/dis/data/silver/eicu_harmonized/
hdfs://master10:9000/user/dis/data/silver/notes_clean/
hdfs://master10:9000/user/dis/data/silver/note_embeddings/
```

---

## 3. THỨ TỰ TRIỂN KHAI ĐỀ XUẤT

### Phase 0: Bronze Validation

Mục tiêu: xác nhận production Bronze đã chạy đúng trước khi transform sang Silver.

Trạng thái hiện tại: Đã hoàn thành cho MIMIC-IV và eICU. Còn pending MIMIC-IV-Note.

Script đã tạo:

```text
src/ingestion/validate_bronze.py
```

Validation cần có:

*   Kiểm tra path tồn tại trên HDFS.
*   Kiểm tra mỗi output có `_SUCCESS`.
*   Đọc thử Parquet bằng Spark.
*   In schema của từng bảng.
*   Tính row count từng bảng.
*   Ghi report ra `docs/BRONZE_VALIDATION_RESULT.md` hoặc log file.

Kết quả đã đạt:

*   MIMIC-IV production Bronze validation: PASS cho các bảng đã ingest, bao gồm `chartevents`.
*   eICU production Bronze validation: PASS cho `patient`, `vitalPeriodic`, `diagnosis`, `medication`.
*   MIMIC-IV-Note validation: pending do chưa cập nhật dữ liệu `discharge.csv`.

Lý do cần làm trước Silver:

*   Nếu Bronze thiếu bảng hoặc ghi lỗi, Silver sẽ fail hoặc tạo feature sai.
*   Row count là bằng chứng nghiệm thu FR-01, FR-02, FR-03.

---

### Phase 1: `silver_admissions.py`

Mục tiêu: tạo bảng admission-level nền tảng cho toàn bộ pipeline.

Script đề xuất:

```text
src/etl/silver_admissions.py
```

Input:

```text
/data/bronze/mimic_iv/admissions/
/data/bronze/mimic_iv/patients/
```

Transformation logic:

*   Cast các cột định danh:
    *   `subject_id` -> integer/long
    *   `hadm_id` -> integer/long
*   Cast thời gian:
    *   `admittime` -> timestamp
    *   `dischtime` -> timestamp
    *   `deathtime` -> timestamp nếu có
*   Join `admissions` với `patients` theo `subject_id`.
*   Tính `duration_days = datediff(dischtime, admittime)`.
*   Tính `admityear = year(admittime)` để partition.
*   Tính `event_flag_mortality` từ `hospital_expire_flag`.
*   Tính tuổi tại thời điểm nhập viện bằng `anchor_age`, `anchor_year`, `admittime`.
*   Filter theo specification:
    *   Adult patients: `age >= 18`.
    *   Admission duration hợp lệ: `duration_days >= 1` nếu cần dùng cho survival task.

Output:

```text
/data/silver/admissions/
```

Partition đề xuất:

```text
partitionBy("admityear")
```

Validation metrics:

*   Số dòng input admissions.
*   Số dòng sau join patients.
*   Số dòng sau filter adult.
*   Tỉ lệ mortality.
*   Min/max/avg `duration_days`.
*   Phân phối theo `admityear`.

---

### Phase 2: `silver_vitals_mimic.py`

Mục tiêu: tạo feature sinh hiệu trong 24h đầu nhập viện từ MIMIC-IV `chartevents`.

Script đề xuất:

```text
src/etl/silver_vitals_mimic.py
```

Input:

```text
/data/bronze/mimic_iv/chartevents/
/data/bronze/mimic_iv/d_items/
/data/silver/admissions/
```

Feature mục tiêu theo specification:

*   SBP.
*   SpO2.
*   HR hoặc PR.
*   Temperature.

Transformation logic:

*   Cast `subject_id`, `hadm_id`, `stay_id`, `itemid`.
*   Cast `charttime` sang timestamp.
*   Cast `valuenum` sang double.
*   Filter `itemid` liên quan đến vital signs.
*   Join với `silver/admissions` để lấy `admittime`, `dischtime`, `admityear`.
*   Chỉ lấy event trong 24h đầu:
    ```text
    charttime >= admittime AND charttime < admittime + interval 24 hours
    ```
*   Filter outlier ở Silver:
    *   SBP: 40-300.
    *   SpO2: 50-100.
    *   HR/PR: cần xác nhận threshold, ví dụ 20-250.
    *   Temperature: cần xác nhận unit và threshold.
*   Aggregate theo `hadm_id`:
    *   mean.
    *   min.
    *   max.
    *   count.

Output:

```text
/data/silver/chartevents_agg/
```

Partition đề xuất:

```text
partitionBy("admityear")
```

Spark optimization:

*   Filter `itemid` càng sớm càng tốt trước khi join.
*   Repartition theo `hadm_id` trước aggregate nếu shuffle quá lớn.
*   Cache `silver/admissions` nếu dùng nhiều lần.
*   Không `.collect()` trên dữ liệu lớn.

Validation metrics:

*   Row count chartevents input.
*   Row count sau filter vital itemids.
*   Row count sau window 24h.
*   Số admission có đủ vitals.
*   Missing rate từng vital feature.
*   Min/max sau outlier filtering.

Điểm cần xác nhận trước khi code:

*   Danh sách `itemid` cuối cùng cho SBP, SpO2, HR/PR, Temperature trong MIMIC-IV v3.1.
*   Có lấy cả invasive/non-invasive SBP hay chỉ một loại.

---

### Phase 3: `silver_labs.py`

Mục tiêu: aggregate kết quả xét nghiệm từ `labevents` thành admission-level lab features.

Script đề xuất:

```text
src/etl/silver_labs.py
```

Input:

```text
/data/bronze/mimic_iv/labevents/
/data/silver/admissions/
```

Transformation logic:

*   Cast `subject_id`, `hadm_id`, `itemid`.
*   Cast `charttime`/`storetime` sang timestamp nếu có.
*   Cast `valuenum` sang double.
*   Chọn danh sách lab itemid cần dùng cho model.
*   Join với `silver/admissions` để lấy admission window.
*   Aggregate trong 24h đầu hoặc trong toàn admission, cần thống nhất theo model spec.
*   Tạo feature mean/min/max/latest/count cho các lab quan trọng.

Output:

```text
/data/silver/labs_agg/
```

Validation metrics:

*   Số dòng labevents input.
*   Số dòng sau filter lab itemids.
*   Số admission có lab feature.
*   Missing rate từng lab.

Điểm cần xác nhận trước khi code:

*   Danh sách lab itemid cần dùng.
*   Window feature: 24h đầu hay toàn admission.

---

### Phase 4: `silver_diagnoses.py`

Mục tiêu: chuẩn hóa ICD diagnosis để phục vụ feature engineering ở Gold.

Script đề xuất:

```text
src/etl/silver_diagnoses.py
```

Input:

```text
/data/bronze/mimic_iv/diagnoses_icd/
```

Transformation logic:

*   Cast `subject_id`, `hadm_id`, `seq_num`, `icd_version`.
*   Giữ `icd_code` dạng string.
*   Chuẩn hóa casing và trimming cho `icd_code`.
*   Tạo feature phụ:
    *   `primary_icd_code` nếu `seq_num = 1`.
    *   ICD chapter hoặc ICD prefix nếu có mapping.
*   Không one-hot quá sớm nếu chưa cần. One-hot có thể làm ở Gold để kiểm soát số lượng feature.

Output:

```text
/data/silver/diagnoses/
```

Validation metrics:

*   Row count diagnoses input.
*   Số distinct `hadm_id`.
*   Số distinct `icd_code`.
*   Phân phối ICD version.
*   Tỉ lệ admission có primary diagnosis.

---

### Phase 5: `silver_eicu_harmonized.py`

Mục tiêu: chuẩn hóa eICU về schema feature tương thích với MIMIC-IV ở tầng Silver.

Script đề xuất:

```text
src/etl/silver_eicu_harmonized.py
```

Input:

```text
/data/bronze/eicu/patient/
/data/bronze/eicu/vitalPeriodic/
/data/bronze/eicu/diagnosis/
/data/bronze/eicu/medication/
```

Transformation logic:

*   Cast key columns:
    *   `patientunitstayid`.
    *   `uniquepid` nếu cần patient-level grouping.
    *   `hospitalid`.
*   Rename ở Silver, không ở Bronze:
    *   `patientunitstayid` -> `stay_id_eicu` hoặc `hadm_id_eicu`.
    *   `systemicSystolic` -> `sbp`.
    *   `sao2` -> `spo2`.
    *   Pulse/heart rate column cần xác nhận từ actual eICU schema.
*   Cast vital columns sang double.
*   Dùng `observationoffset` để lấy 24h đầu ICU stay.
*   Filter outlier bằng cùng rule với MIMIC khi tương thích.
*   Aggregate vital features theo `patientunitstayid`.
*   Giữ thêm `hospitalid` để partition/debug theo hospital.

Output:

```text
/data/silver/eicu_harmonized/
```

Partition đề xuất:

```text
partitionBy("hospitalid")
```

Validation metrics:

*   Row count patient input.
*   Row count vitalPeriodic input.
*   Số ICU stays có vitals.
*   Missing rate SBP/SpO2/HR/Temp.
*   Phân phối theo `hospitalid`.

Điểm cần xác nhận trước khi code:

*   eICU sẽ dùng như external validation dataset hay union vào train set.
*   Mapping cuối cùng cho vitals eICU.
*   Cách định nghĩa event/readmission/mortality với eICU.

---

### Phase 6: MIMIC-IV-Note Silver NLP

Trạng thái: pending vì dữ liệu note sẽ được cập nhật sau.

Script đề xuất sau khi có data:

```text
src/nlp/notes_clean.py
src/nlp/train_word2vec.py
src/nlp/note_embeddings.py
```

Input:

```text
/data/bronze/mimic_iv_note/discharge/
```

Transformation logic dự kiến:

*   Strip PHI placeholders dạng `[**...**]`.
*   Lowercase.
*   Tokenize bằng Spark NLP hoặc PySpark tokenizer.
*   Remove stopwords thường và medical stopwords.
*   Train Word2Vec CBOW với `vectorSize=128`.
*   Sinh document embedding theo `hadm_id`.

Output:

```text
/data/silver/notes_clean/
/data/silver/note_embeddings/
```

Validation metrics:

*   Số note input.
*   Số note sau clean.
*   Token count distribution.
*   Số admission có embedding.
*   Vector dimension = 128.

---

## 4. GOLD LAYER CHUẨN BỊ SAU SILVER

Sau khi các Silver jobs ổn định, tạo Gold Analytical Dataset.

Script đề xuất:

```text
src/etl/build_gold_dataset.py
```

Input:

```text
/data/silver/admissions/
/data/silver/chartevents_agg/
/data/silver/labs_agg/
/data/silver/diagnoses/
/data/silver/eicu_harmonized/
/data/silver/note_embeddings/
```

Gold logic:

*   Base table là `silver/admissions`.
*   Left join vitals, labs, diagnoses, notes embeddings.
*   Tạo ICD one-hot hoặc ICD chapter features.
*   Tạo train/val/test split theo thời gian để tránh leakage.
*   Partition output theo `split`.

Output dự kiến:

```text
/data/gold/analytical_dataset/split=train/
/data/gold/analytical_dataset/split=val/
/data/gold/analytical_dataset/split=test/
```

---

## 5. CẤU TRÚC CODE ĐỀ XUẤT

```text
src/
  ingestion/
    ingest_mimic.py
    ingest_eicu.py
    ingest_notes.py
    validate_bronze.py
  etl/
    silver_admissions.py
    silver_vitals_mimic.py
    silver_labs.py
    silver_diagnoses.py
    silver_eicu_harmonized.py
    build_gold_dataset.py
  nlp/
    notes_clean.py
    train_word2vec.py
    note_embeddings.py
```

---

## 6. QUY TẮC TRIỂN KHAI SILVER JOBS

*   Luôn chạy bằng `spark-submit`, không chạy bằng `python` trực tiếp.
*   Không dùng pandas cho bảng lớn.
*   Không dùng `.collect()` trừ khi dữ liệu rất nhỏ và đã limit rõ ràng.
*   Không hard-code local path trong logic chính. Dùng tham số `local`/`hdfs` hoặc `argparse`.
*   Log input path, output path, row count và thời gian chạy từng job.
*   Ghi output bằng `Parquet + Snappy`.
*   Với bảng lớn như `chartevents`, filter column/itemid càng sớm càng tốt.
*   Với output lớn, dùng partition hợp lý (`admityear`, `hospitalid`, hoặc `split`).
*   Mỗi job cần có validation metrics sau write.

---

## 7. THỨ TỰ ƯU TIÊN SAU PRODUCTION BRONZE

1.  Tạo `silver_admissions.py`.
2.  Tạo `silver_vitals_mimic.py`.
3.  Tạo `silver_diagnoses.py`.
4.  Tạo `silver_labs.py`.
5.  Tạo `silver_eicu_harmonized.py`.
6.  Bổ sung MIMIC-IV-Note Bronze/Silver NLP sau khi có `discharge.csv`.
7.  Chạy lại `validate_bronze.py` cho MIMIC-IV-Note sau khi ingestion notes hoàn tất.
8.  Tạo `build_gold_dataset.py`.

---

## 8. CÂU HỎI CẦN XÁC NHẬN TRƯỚC KHI CODE SILVER

1.  Danh sách `itemid` MIMIC-IV cuối cùng cho SBP, SpO2, HR/PR, Temperature là gì?
2.  Lab features nào sẽ dùng cho mô hình và dashboard?
3.  Feature window là 24h đầu nhập viện hay toàn bộ admission?
4.  eICU dùng để train chung với MIMIC hay chỉ dùng làm external validation?
5.  Survival task chính là readmission 30 ngày, mortality, hay cả hai như specification?
6.  Train/val/test split sẽ theo năm nào? Specification gợi ý temporal split nhưng cần xác nhận mốc năm theo data thực tế.

---

## 9. DELIVERABLES CỦA SILVER LAYER

Khi hoàn tất Silver Layer, cần có:

*   Source code các Spark ETL jobs trong `src/etl/`.
*   Output Parquet+Snappy trong `/data/silver/` trên HDFS.
*   Validation report gồm row count, missing rate, event rate, và feature distribution.
*   Tài liệu cập nhật cách chạy từng job bằng `spark-submit`.
*   Checklist mapping Bronze -> Silver -> Gold để phục vụ nghiệm thu Big Data Engineering.
