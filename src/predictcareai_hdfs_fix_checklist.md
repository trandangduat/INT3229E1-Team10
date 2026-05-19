# PredictCareAI – Checklist các vấn đề cần sửa trước khi chạy lại HDFS

## Mục tiêu

Tài liệu này tổng hợp các vấn đề cần sửa trong pipeline Silver/Gold sau khi nhóm chuyển bài toán về đúng hướng **post-discharge prediction**:

```text
Task 1: 30-day readmission
→ Dự đoán nguy cơ tái nhập viện trong 30 ngày sau khi xuất viện.

Task 2: 12-month mortality
→ Dự đoán nguy cơ tử vong trong 12 tháng sau khi xuất viện.
```

Mốc dự đoán chung cho cả hai bài toán:

```text
index_time = dischtime
```

Vì vậy, `duration_days` không còn là survival time label cho mortality/readmission nữa. Nó chỉ nên được giữ lại như một **feature** biểu diễn length of stay.

---

## 1. Kết luận nhanh

Code hiện tại đã đi đúng hướng, nhưng **chưa nên overwrite HDFS production ngay**.

Cần sửa tối thiểu các điểm sau trước khi chạy lại:

```text
1. silver_admissions.py phải tạo rõ hai alias:
   - readmission_event_30d
   - mortality_event_12m

2. Phải loại bệnh nhân tử vong trong viện khỏi cohort post-discharge.

3. Nên cải thiện cách tính readmission để không miss same-day readmission.

4. build_gold_dataset.py nên đọc label chuẩn từ Silver thay vì tự overwrite lại.

5. Không nên dùng --include-eicu cho training survival mortality 12 tháng nếu eICU không có time-to-death 12 tháng.

6. validate_gold.py và inspect_gold.py cần hỗ trợ output suffix nếu dùng dataset version mới.
```

---

## 2. Vấn đề 1 – `silver_admissions.py` thiếu alias label chuẩn

### Hiện trạng

Trong `silver_admissions.py`, bạn đã tạo các cột:

```text
event_flag_readmission
event_flag_mortality
readmission_time_days
mortality_time_days
mortality_time_months
```

Nhưng trong validator và Gold schema, bạn lại yêu cầu thêm:

```text
readmission_event_30d
mortality_event_12m
```

Nếu `silver/admissions` không có hai cột này, `validate_silver.py` sẽ fail, hoặc `build_gold_dataset.py` sẽ fail khi `.select()`.

### Cách sửa

Trong `silver_admissions.py`, sau khi tạo `event_flag_readmission` và `event_flag_mortality`, thêm:

```python
.withColumn("readmission_event_30d", col("event_flag_readmission"))
.withColumn("mortality_event_12m", col("event_flag_mortality"))
```

Đoạn cuối phần tạo label nên có dạng:

```python
.withColumn(
    "event_flag_readmission",
    when(
        col("days_to_readmission").isNotNull()
        & (col("days_to_readmission") > 0)
        & (col("days_to_readmission") <= 30),
        1,
    ).otherwise(0),
)
.withColumn(
    "readmission_time_days",
    when(
        col("event_flag_readmission") == 1,
        col("days_to_readmission"),
    ).otherwise(30),
)
.withColumn(
    "days_to_death_after_discharge",
    datediff(col("dod"), col("dischtime")),
)
.withColumn(
    "event_flag_mortality",
    when(
        col("days_to_death_after_discharge").isNotNull()
        & (col("days_to_death_after_discharge") > 0)
        & (col("days_to_death_after_discharge") <= 365),
        1,
    ).otherwise(0),
)
.withColumn(
    "mortality_time_days",
    when(
        col("event_flag_mortality") == 1,
        col("days_to_death_after_discharge"),
    ).otherwise(365),
)
.withColumn(
    "mortality_time_months",
    col("mortality_time_days") / 30.4375,
)
.withColumn("readmission_event_30d", col("event_flag_readmission"))
.withColumn("mortality_event_12m", col("event_flag_mortality"))
```

### Vì sao cần sửa?

Để Gold dataset có schema rõ ràng:

```text
readmission_time_days      = T cho readmission
readmission_event_30d      = E cho readmission
mortality_time_days        = T cho mortality 12 tháng
mortality_time_months      = T theo tháng cho visualization
mortality_event_12m        = E cho mortality 12 tháng
```

Các cột `event_flag_readmission` và `event_flag_mortality` có thể giữ lại làm alias để tương thích với code cũ, nhưng downstream nên dùng tên mới rõ nghĩa hơn.

---

## 3. Vấn đề 2 – Cần loại bệnh nhân tử vong trong viện khỏi cohort post-discharge

### Hiện trạng

Bài toán của nhóm là dự đoán sau xuất viện:

```text
readmission 30 ngày sau xuất viện
mortality 12 tháng sau xuất viện
```

Do đó, cohort đúng phải là:

```text
bệnh nhân sống đến lúc xuất viện
```

Nếu một bệnh nhân đã tử vong trong viện, họ không còn là đối tượng của bài toán discharge planning.

Hiện tại code có thể gặp lỗi logic như sau:

```text
Bệnh nhân tử vong trong viện
→ dod trùng hoặc gần dischtime
→ datediff(dod, dischtime) = 0
→ event_flag_mortality = 0 vì điều kiện đang yêu cầu > 0
→ mortality_time_days = 365
```

Như vậy bệnh nhân tử vong trong viện có thể bị gán nhầm là “không tử vong trong 12 tháng sau xuất viện”. Đây là sai về mặt cohort.

### Cách sửa

Import thêm:

```python
from pyspark.sql.functions import lower
```

Sau đó trong filter cohort, thêm điều kiện loại in-hospital death:

```python
& col("deathtime").isNull()
& (
    col("discharge_location").isNull()
    | (
        ~lower(col("discharge_location")).contains("expire")
        & ~lower(col("discharge_location")).contains("deceased")
    )
)
```

Đoạn filter nên có dạng:

```python
df_filtered = df_transformed.filter(
    col("subject_id").isNotNull()
    & col("hadm_id").isNotNull()
    & col("admittime").isNotNull()
    & col("dischtime").isNotNull()
    & col("admityear").isNotNull()
    & (col("age") >= 18)
    & (col("duration_days") >= 1)
    & col("deathtime").isNull()
    & (
        col("discharge_location").isNull()
        | (
            ~lower(col("discharge_location")).contains("expire")
            & ~lower(col("discharge_location")).contains("deceased")
        )
    )
)
```

### Ghi chú

Sau khi filter in-hospital death, số dòng trong `silver/admissions` sẽ giảm so với bản cũ. Điều này là bình thường và đúng với bài toán mới.

---

## 4. Vấn đề 3 – Same-day readmission có thể bị bỏ sót

### Hiện trạng

Bạn đang tính:

```python
days_to_readmission = datediff(next_admittime, dischtime)
```

và check:

```python
days_to_readmission > 0
```

Vấn đề: nếu bệnh nhân xuất viện buổi sáng và tái nhập viện buổi tối cùng ngày, `datediff` có thể bằng `0`. Khi đó bệnh nhân bị gán nhầm là không tái nhập viện.

### Có bắt buộc sửa ngay không?

Không bắt buộc nếu deadline gấp. Với demo, bản hiện tại vẫn chạy được.

Nhưng nếu muốn label chính xác hơn, nên sửa sang tính theo giờ.

### Cách sửa tốt hơn

Import thêm:

```python
from pyspark.sql.functions import unix_timestamp, ceil, greatest, lit
```

Thay logic `datediff` bằng:

```python
.withColumn(
    "hours_to_readmission",
    (unix_timestamp(col("next_admittime")) - unix_timestamp(col("dischtime"))) / 3600.0,
)
.withColumn(
    "event_flag_readmission",
    when(
        col("hours_to_readmission").isNotNull()
        & (col("hours_to_readmission") > 0)
        & (col("hours_to_readmission") <= 30 * 24),
        1,
    ).otherwise(0),
)
.withColumn(
    "readmission_time_days",
    when(
        col("event_flag_readmission") == 1,
        greatest(ceil(col("hours_to_readmission") / 24.0), lit(1)),
    ).otherwise(30),
)
```

Sau đó drop thêm:

```python
.drop(
    "next_admittime",
    "days_to_readmission",
    "hours_to_readmission",
    "days_to_death_after_discharge",
)
```

---

## 5. Vấn đề 4 – `build_gold_dataset.py` đang tạo lại label alias không cần thiết

### Hiện trạng

Trong `build_gold_dataset.py`, bạn đang đọc label từ Silver:

```python
col("readmission_time_days").cast("int").alias("readmission_time_days")
col("mortality_time_days").cast("int").alias("mortality_time_days")
col("mortality_time_months").cast("double").alias("mortality_time_months")
col("mortality_event_12m").cast("int").alias("mortality_event_12m")
```

Sau đó lại tạo lại:

```python
df = df.withColumn("readmission_event_30d", col("event_flag_readmission"))
df = df.withColumn("mortality_event_12m", col("event_flag_mortality"))
```

Điều này không sai nếu hai alias giống nhau, nhưng code bị dư và dễ gây nhầm.

### Cách sửa khuyến nghị

Trong `.select()` của `df_base`, đọc rõ cả hai alias:

```python
col("readmission_event_30d").cast("int").alias("readmission_event_30d"),
col("mortality_event_12m").cast("int").alias("mortality_event_12m"),
```

Sau đó bỏ hai dòng:

```python
df = df.withColumn("readmission_event_30d", col("event_flag_readmission"))
df = df.withColumn("mortality_event_12m", col("event_flag_mortality"))
```

### Nguyên tắc nên giữ

```text
Silver tạo label chuẩn.
Gold chỉ join feature và giữ nguyên label.
ML layer chỉ đọc label từ Gold.
```

Gold không nên tự tạo lại survival label nếu không cần thiết.

---

## 6. Vấn đề 5 – Không dùng eICU cho survival mortality 12 tháng nếu thiếu follow-up 12 tháng

### Hiện trạng

Trong `build_gold_dataset.py`, eICU được union tùy chọn bằng flag:

```bash
--include-eicu
```

Nhưng eICU harmonized hiện chủ yếu có hospital-level mortality/vitals, không chắc có `dod` hoặc follow-up đủ 12 tháng.

Nếu eICU không có:

```text
mortality_time_days
mortality_event_12m
```

thì không nên gộp eICU vào training survival mortality 12 tháng.

### Khuyến nghị

Khi build Gold cho MIMIC survival models, chạy:

```bash
spark-submit src/etl/build_gold_dataset.py hdfs
```

Không thêm:

```bash
--include-eicu
```

Chỉ dùng eICU cho một trong các mục sau:

```text
1. External validation dạng classification nếu chỉ có hospital mortality.
2. Domain generalization demo.
3. Future work nếu đã harmonize được label post-discharge 12 tháng.
```

---

## 7. Vấn đề 6 – `validate_gold.py` và `inspect_gold.py` chưa hỗ trợ output suffix

### Hiện trạng

`build_gold_dataset.py` có tham số:

```bash
--output-suffix
```

Ví dụ:

```bash
spark-submit src/etl/build_gold_dataset.py hdfs --output-suffix _postdischarge_v2
```

Output sẽ nằm ở:

```text
/user/dis/data/gold/analytical_dataset_postdischarge_v2
```

Nhưng `validate_gold.py` và `inspect_gold.py` hiện đang đọc cố định:

```text
/user/dis/data/gold/analytical_dataset
```

Vì vậy nếu dùng suffix để test dataset mới, validator vẫn có thể đang kiểm tra dataset cũ.

### Cách sửa nhanh

Thêm argument vào `validate_gold.py` và `inspect_gold.py`:

```python
parser.add_argument(
    "--dataset-name",
    default="analytical_dataset",
    help="Gold dataset directory name, e.g. analytical_dataset_postdischarge_v2",
)
```

Sau đó sửa path:

```python
gold_path = f"{base_path}/gold/{args.dataset_name}"
```

Lệnh chạy:

```bash
spark-submit src/etl/validate_gold.py hdfs --dataset-name analytical_dataset_postdischarge_v2
spark-submit src/etl/inspect_gold.py hdfs --dataset-name analytical_dataset_postdischarge_v2
```

### Nếu chưa muốn sửa validator

Có thể inspect thủ công bằng Spark shell hoặc tạm ghi đè final path sau khi đã backup. Nhưng cách an toàn hơn là sửa validator để đọc được dataset version mới.

---

## 8. Logic label đúng sau khi sửa

### 8.1. Readmission 30 ngày

Với mỗi admission:

```text
index_time = dischtime
```

Tìm lần nhập viện kế tiếp của cùng `subject_id`:

```text
next_admittime = admission kế tiếp sau dischtime
```

Label:

```text
readmission_event_30d = 1 nếu 0 < next_admittime - dischtime <= 30 ngày
readmission_event_30d = 0 nếu không có readmission trong 30 ngày

readmission_time_days = số ngày đến readmission nếu event = 1
readmission_time_days = 30 nếu event = 0
```

### 8.2. Mortality 12 tháng

Với mỗi admission mà bệnh nhân sống đến lúc xuất viện:

```text
index_time = dischtime
```

Dựa vào `patients.dod`:

```text
mortality_event_12m = 1 nếu 0 < dod - dischtime <= 365 ngày
mortality_event_12m = 0 nếu không thấy tử vong trong 365 ngày

mortality_time_days = số ngày đến death nếu event = 1
mortality_time_days = 365 nếu event = 0
mortality_time_months = mortality_time_days / 30.4375
```

### 8.3. `duration_days`

`duration_days` chỉ còn là feature:

```text
duration_days = dischtime - admittime
```

Không dùng `duration_days` làm survival time `T` cho readmission hoặc mortality 12 tháng.

---

## 9. Checklist sửa code

### 9.1. `silver_admissions.py`

Cần sửa:

- [ ] Import `lower` nếu filter in-hospital death bằng discharge location.
- [ ] Import `unix_timestamp`, `ceil`, `greatest`, `lit` nếu muốn sửa same-day readmission.
- [ ] Thêm filter loại bệnh nhân tử vong trong viện.
- [ ] Tạo `readmission_event_30d` alias từ `event_flag_readmission`.
- [ ] Tạo `mortality_event_12m` alias từ `event_flag_mortality`.
- [ ] Kiểm tra `readmission_time_days` nằm trong `[1, 30]` nếu event = 1.
- [ ] Kiểm tra `mortality_time_days` nằm trong `[1, 365]` nếu event = 1.
- [ ] Đảm bảo `duration_days` vẫn được giữ như feature.

### 9.2. `build_gold_dataset.py`

Cần sửa:

- [ ] Đọc `readmission_event_30d` trực tiếp từ `silver/admissions`.
- [ ] Đọc `mortality_event_12m` trực tiếp từ `silver/admissions`.
- [ ] Bỏ hoặc không dùng lại hai dòng overwrite alias từ `event_flag_*`.
- [ ] Không chạy `--include-eicu` cho training survival mortality 12 tháng nếu eICU thiếu label follow-up.
- [ ] Nếu chạy test dataset mới, dùng `--output-suffix _postdischarge_v2`.

### 9.3. `validate_silver.py`

Cần kiểm tra:

- [ ] Required columns của `silver/admissions` có đủ:
  - `index_time`
  - `readmission_time_days`
  - `readmission_event_30d`
  - `event_flag_readmission`
  - `mortality_time_days`
  - `mortality_time_months`
  - `mortality_event_12m`
  - `event_flag_mortality`
- [ ] Required non-null có đủ:
  - `readmission_time_days`
  - `mortality_time_days`
  - `event_flag_readmission`
  - `event_flag_mortality`
- [ ] Nên bổ sung validation logic range cho label.

### 9.4. `validate_gold.py`

Cần sửa/cải thiện:

- [ ] Thêm `--dataset-name` nếu dùng output suffix.
- [ ] Check required columns của Gold.
- [ ] Check duplicate `hadm_id`.
- [ ] Check split distribution.
- [ ] Check label range.
- [ ] Check event rate theo split.

### 9.5. `inspect_gold.py`

Cần sửa/cải thiện:

- [ ] Thêm `--dataset-name` nếu dùng output suffix.
- [ ] In thống kê:
  - `readmission_time_days`
  - `readmission_event_30d`
  - `mortality_time_days`
  - `mortality_time_months`
  - `mortality_event_12m`
- [ ] In split distribution và event rate.

---

## 10. Validation nên bổ sung

### 10.1. Silver label validation

Nên thêm vào `validate_silver.py` hoặc chạy query riêng:

```python
adm = spark.read.parquet(f"{base_path}/silver/admissions")

adm.filter(
    (col("readmission_event_30d") == 1)
    & ~((col("readmission_time_days") >= 1) & (col("readmission_time_days") <= 30))
).count()

adm.filter(
    (col("readmission_event_30d") == 0)
    & (col("readmission_time_days") != 30)
).count()

adm.filter(
    (col("mortality_event_12m") == 1)
    & ~((col("mortality_time_days") >= 1) & (col("mortality_time_days") <= 365))
).count()

adm.filter(
    (col("mortality_event_12m") == 0)
    & (col("mortality_time_days") != 365)
).count()
```

Tất cả các count trên nên bằng `0`.

### 10.2. Gold label validation

Sau khi build Gold, kiểm tra:

```text
readmission_event_30d = 1  → readmission_time_days in [1, 30]
readmission_event_30d = 0  → readmission_time_days = 30
mortality_event_12m = 1    → mortality_time_days in [1, 365]
mortality_event_12m = 0    → mortality_time_days = 365
```

### 10.3. Split validation

Kiểm tra event rate theo split:

```python
df.groupBy("split").agg(
    count("*").alias("rows"),
    avg(col("readmission_event_30d")).alias("readmission_rate"),
    avg(col("mortality_event_12m")).alias("mortality_rate_12m"),
    min(col("admityear")).alias("min_year"),
    max(col("admityear")).alias("max_year"),
).show(truncate=False)
```

Nếu event rate ở train/val/test quá lệch, cần xem lại temporal split hoặc data filtering.

---

## 11. Thứ tự chạy an toàn trên HDFS

### Bước 1 – Check syntax

```bash
python3 -m py_compile src/etl/silver_admissions.py
python3 -m py_compile src/etl/build_gold_dataset.py
python3 -m py_compile src/etl/validate_silver.py
python3 -m py_compile src/etl/validate_gold.py
python3 -m py_compile src/etl/inspect_gold.py
```

### Bước 2 – Backup output cũ

```bash
hdfs dfs -cp /user/dis/data/silver/admissions /user/dis/data/silver/admissions_backup_before_postdischarge
hdfs dfs -cp /user/dis/data/gold/analytical_dataset /user/dis/data/gold/analytical_dataset_backup_before_postdischarge
```

Nếu path backup đã tồn tại, dùng tên có timestamp:

```bash
hdfs dfs -cp /user/dis/data/silver/admissions /user/dis/data/silver/admissions_backup_$(date +%Y%m%d_%H%M)
hdfs dfs -cp /user/dis/data/gold/analytical_dataset /user/dis/data/gold/analytical_dataset_backup_$(date +%Y%m%d_%H%M)
```

### Bước 3 – Chạy lại Silver admissions

```bash
spark-submit src/etl/silver_admissions.py hdfs
```

### Bước 4 – Validate Silver

```bash
spark-submit src/etl/validate_silver.py hdfs
```

Chỉ chạy tiếp nếu Silver PASS.

### Bước 5 – Build Gold version mới

Khuyến nghị ban đầu ghi ra dataset mới:

```bash
spark-submit src/etl/build_gold_dataset.py hdfs --output-suffix _postdischarge_v2
```

Nếu `validate_gold.py` đã hỗ trợ `--dataset-name`, chạy:

```bash
spark-submit src/etl/validate_gold.py hdfs --dataset-name analytical_dataset_postdischarge_v2
spark-submit src/etl/inspect_gold.py hdfs --dataset-name analytical_dataset_postdischarge_v2
```

Nếu chưa hỗ trợ `--dataset-name`, inspect thủ công hoặc tạm sửa validator trước.

### Bước 6 – Ghi đè final dataset sau khi kiểm tra ổn

```bash
spark-submit src/etl/build_gold_dataset.py hdfs
spark-submit src/etl/validate_gold.py hdfs
spark-submit src/etl/inspect_gold.py hdfs
```

---

## 12. Lệnh chạy production đề xuất

### MIMIC-only Gold, không eICU, không notes

```bash
spark-submit \
  --driver-memory 6g \
  --conf spark.sql.shuffle.partitions=200 \
  src/etl/build_gold_dataset.py hdfs
```

### MIMIC-only Gold version test

```bash
spark-submit \
  --driver-memory 6g \
  --conf spark.sql.shuffle.partitions=200 \
  src/etl/build_gold_dataset.py hdfs \
  --output-suffix _postdischarge_v2
```

### Gold có notes nếu đã có `silver/note_embeddings`

```bash
spark-submit \
  --driver-memory 6g \
  --conf spark.sql.shuffle.partitions=200 \
  src/etl/build_gold_dataset.py hdfs \
  --include-notes
```

Không khuyến nghị:

```bash
spark-submit src/etl/build_gold_dataset.py hdfs --include-eicu
```

cho survival mortality 12 tháng nếu eICU chưa có label follow-up tương ứng.

---

## 13. Checklist cuối trước khi train model

Sau khi Gold mới đã build xong, phải đảm bảo:

- [ ] Gold có `index_time = dischtime`.
- [ ] Gold có `readmission_time_days`.
- [ ] Gold có `readmission_event_30d`.
- [ ] Gold có `mortality_time_days`.
- [ ] Gold có `mortality_time_months`.
- [ ] Gold có `mortality_event_12m`.
- [ ] `duration_days` chỉ dùng làm feature, không dùng làm survival label.
- [ ] Không có bệnh nhân tử vong trong viện trong cohort post-discharge.
- [ ] Không có label null ở train/val/test.
- [ ] Split train/val/test đúng theo `admityear`.
- [ ] Event rate readmission/mortality theo split hợp lý.
- [ ] Nếu dùng notes, đảm bảo không leakage từ dữ liệu sau `index_time`.
- [ ] Nếu dùng discharge_location cho what-if, ghi rõ đây là simulation/counterfactual approximation, không phải causal proof.

---

## 14. Gợi ý chỉnh code training sau khi Gold đã sửa

### Readmission model

```python
TASK = "readmission"
time_col = "readmission_time_days"
event_col = "readmission_event_30d"
time_bins = np.arange(1, 31)
```

### Mortality model

```python
TASK = "mortality"
time_col = "mortality_time_days"
event_col = "mortality_event_12m"
time_bins = np.arange(30, 366, 30)
```

### Exclude columns khỏi feature matrix

```python
exclude_cols = [
    "hadm_id",
    "subject_id",
    "admittime",
    "dischtime",
    "index_time",
    "split",
    "admityear",
    "event_flag_readmission",
    "event_flag_mortality",
    "readmission_event_30d",
    "readmission_time_days",
    "mortality_event_12m",
    "mortality_time_days",
    "mortality_time_months",
    "dod",
    "next_admittime",
]
```

`duration_days` có thể giữ lại trong features vì nó là length of stay của admission hiện tại, đã biết tại thời điểm xuất viện.

---

## 15. Tóm tắt cuối

Bản sửa hiện tại đã đúng hướng về mặt thiết kế:

```text
index_time = dischtime
readmission 30 ngày sau xuất viện
mortality 12 tháng sau xuất viện
```

Nhưng cần sửa trước khi chạy production:

```text
Bắt buộc:
1. Thêm readmission_event_30d và mortality_event_12m trong silver_admissions.py.
2. Loại in-hospital death khỏi cohort post-discharge.
3. Đảm bảo build_gold_dataset.py đọc label chuẩn từ Silver.

Khuyến nghị:
4. Sửa same-day readmission bằng cách tính theo giờ.
5. Thêm --dataset-name cho validate_gold.py và inspect_gold.py.
6. Không include eICU cho survival mortality 12 tháng nếu thiếu follow-up label.
```

Sau khi sửa, chạy theo thứ tự:

```text
silver_admissions.py
→ validate_silver.py
→ build_gold_dataset.py với suffix test
→ validate_gold.py / inspect_gold.py
→ build final Gold
→ train readmission model
→ train mortality model
```
