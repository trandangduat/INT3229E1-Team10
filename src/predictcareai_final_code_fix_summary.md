# PredictCareAI – Tóm tắt các sửa đổi cuối cùng trước khi chạy lại HDFS

## 1. Kết luận ngắn

Code hiện tại đã đi đúng hướng cho bài toán **discharge-planning**:

```text
index_time = dischtime
readmission = tái nhập viện trong 30 ngày sau xuất viện
mortality = tử vong trong 12 tháng sau xuất viện
```

Các điểm đã sửa đúng:

```text
✓ Có readmission_event_30d
✓ Có mortality_event_12m
✓ Có readmission_time_days
✓ Có mortality_time_days
✓ Có mortality_time_months
✓ duration_days chỉ còn là feature length-of-stay
✓ index_time = dischtime
✓ đã loại bệnh nhân tử vong trong viện bằng deathtime.isNull()
✓ đã tính readmission theo giờ thay vì datediff ngày
✓ validate_gold.py và inspect_gold.py đã hỗ trợ --dataset-name
```

Tuy nhiên, trước khi chạy overwrite production HDFS, nên sửa thêm các điểm dưới đây.

---

# 2. Vấn đề bắt buộc sửa: `next_admittime` đang tính sau khi filter cohort

## 2.1. Vấn đề hiện tại

Trong `silver_admissions.py`, logic hiện tại đang làm theo thứ tự:

```python
df_filtered = df_transformed.filter(...)

patient_window = Window.partitionBy("subject_id").orderBy("admittime")

df_with_next = df_filtered.withColumn(
    "next_admittime",
    lead("admittime", 1).over(patient_window)
)
```

Điều này có nghĩa là `next_admittime` được tính **sau khi đã filter cohort**.

Cohort filter hiện tại loại các admission như:

```text
- bệnh nhân dưới 18 tuổi
- duration_days < 1
- admission có deathtime
- discharge_location chứa expire/deceased
```

Vấn đề: nếu lần nhập viện kế tiếp của bệnh nhân bị filter ra, thì `lead()` sẽ bỏ qua lần nhập viện đó.

Ví dụ:

| Admission | Thời điểm | Trạng thái | Có trong df_filtered? |
|---|---|---|---|
| A1 | Jan 1–Jan 5 | xuất viện sống | có |
| A2 | Jan 15–Jan 20 | tái nhập viện, tử vong trong viện | bị loại |
| A3 | Apr 1–Apr 5 | xuất viện sống | có |

Nếu tính `lead()` sau filter:

```text
A1 → A3
```

Như vậy A1 bị coi là **không tái nhập viện trong 30 ngày**, dù thực tế A2 là readmission trong 10 ngày.

## 2.2. Cách sửa đúng

Cần tính `next_admittime` từ **toàn bộ admissions hợp lệ về thời gian**, trước khi filter cohort discharge-planning.

Thêm đoạn này **sau khi có `df_transformed`**, trước `df_filtered`:

```python
patient_window = Window.partitionBy("subject_id").orderBy("admittime", "hadm_id")

# Lookup admission kế tiếp từ toàn bộ admissions hợp lệ về mặt thời gian.
# Không dùng df_filtered ở đây, vì readmission phải xét mọi lần nhập viện kế tiếp.
df_readmission_lookup = (
    df_transformed
    .filter(
        col("subject_id").isNotNull()
        & col("hadm_id").isNotNull()
        & col("admittime").isNotNull()
    )
    .withColumn(
        "next_admittime",
        lead("admittime", 1).over(patient_window)
    )
    .select("hadm_id", "next_admittime")
)
```

Sau đó giữ `df_filtered` như cohort chính:

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
            & ~lower(col("discharge_location")).contains("died")
        )
    )
)
```

Sau đó join lookup vào cohort:

```python
df_filtered = df_filtered.join(
    df_readmission_lookup,
    on="hadm_id",
    how="left"
)
```

Cuối cùng, trong `df_with_next`, **bỏ dòng tạo `next_admittime` bằng `lead()`**, vì cột này đã có sẵn từ lookup.

Đoạn tính readmission giữ như sau:

```python
df_with_next = (
    df_filtered
    .withColumn(
        "hours_to_readmission",
        (unix_timestamp(col("next_admittime")) - unix_timestamp(col("dischtime")))
        / 3600.0,
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
)
```

## 2.3. Vì sao cách này xử lý được bệnh nhân nhập viện nhiều lần?

Nếu một bệnh nhân có nhiều admission:

```text
A1 → A2 → A3 → A4
```

`lead()` trên toàn bộ admission timeline sẽ tạo:

```text
A1.next = A2
A2.next = A3
A3.next = A4
A4.next = null
```

Sau đó cohort filter có thể giữ hoặc bỏ một số dòng, nhưng `next_admittime` của mỗi admission đã được tính từ timeline đầy đủ, nên không bỏ sót readmission thật sự.

---

# 3. Nên thêm `died` vào filter `discharge_location`

Hiện tại bạn đã filter:

```python
~lower(col("discharge_location")).contains("expire")
& ~lower(col("discharge_location")).contains("deceased")
```

Nên thêm:

```python
& ~lower(col("discharge_location")).contains("died")
```

Đoạn đầy đủ:

```python
& (
    col("discharge_location").isNull()
    | (
        ~lower(col("discharge_location")).contains("expire")
        & ~lower(col("discharge_location")).contains("deceased")
        & ~lower(col("discharge_location")).contains("died")
    )
)
```

Lý do: một số discharge status có thể dùng từ `DIED`, `EXPIRED`, hoặc `DECEASED`. Dù `deathtime.isNull()` đã xử lý phần lớn, thêm `died` giúp filter chắc hơn.

---

# 4. Cân nhắc sửa same-day mortality sau xuất viện

## 4.1. Vấn đề

Hiện tại mortality được tính bằng:

```python
col("days_to_death_after_discharge") > 0
```

Nếu bệnh nhân xuất viện và tử vong cùng ngày, `datediff(dod, dischtime)` có thể bằng `0`. Khi đó bệnh nhân sẽ bị gán:

```text
mortality_event_12m = 0
mortality_time_days = 365
```

Điều này có thể làm bỏ sót một số ca tử vong rất sớm sau xuất viện.

## 4.2. Cách sửa nếu muốn tính same-day death là event

Đổi điều kiện thành `>= 0` và ép thời gian event tối thiểu là 1 ngày:

```python
.withColumn(
    "event_flag_mortality",
    when(
        col("days_to_death_after_discharge").isNotNull()
        & (col("days_to_death_after_discharge") >= 0)
        & (col("days_to_death_after_discharge") <= 365),
        1,
    ).otherwise(0),
)
.withColumn(
    "mortality_time_days",
    when(
        col("event_flag_mortality") == 1,
        greatest(col("days_to_death_after_discharge"), lit(1)),
    ).otherwise(365),
)
```

## 4.3. Có bắt buộc không?

Không bắt buộc nếu muốn tránh mơ hồ vì `dod` trong MIMIC có thể chỉ ở mức ngày, không có timestamp chính xác. Nhưng nếu mục tiêu là không bỏ sót death sớm sau discharge, nên dùng cách `>= 0` và `max(days, 1)`.

---

# 5. Nên sửa `validate_relationships()` để lỗi orphan làm fail thật

## 5.1. Vấn đề

Trong `validate_silver.py`, nếu phát hiện orphan `hadm_id`, code hiện tại chỉ in:

```text
[FAIL] table has orphan hadm_id
```

nhưng không làm script fail thật sự.

## 5.2. Nên sửa

Cho `validate_relationships()` trả về boolean:

```python
def validate_relationships(spark, base_path):
    print("\n=== relationships ===")
    all_passed = True

    admissions_path = f"{base_path}/silver/admissions"
    admissions = spark.read.parquet(admissions_path).select("hadm_id").distinct()
    admissions_count = admissions.count()
    print(f"[METRIC] Silver admissions distinct hadm_id: {admissions_count}")

    for table_name in ["chartevents_agg", "diagnoses", "labs_agg", "notes_clean"]:
        path = f"{base_path}/silver/{table_name}"
        if not hdfs_path_exists(spark, path):
            print(f"[WARN] Skipping relationship check for missing table: {table_name}")
            continue

        table = spark.read.parquet(path).select("hadm_id").distinct()
        orphan_count = table.join(admissions, on="hadm_id", how="left_anti").count()
        print(f"[METRIC] {table_name} hadm_id not found in admissions: {orphan_count}")

        if orphan_count == 0:
            print(f"[PASS] {table_name} hadm_id subset of admissions")
        else:
            print(f"[FAIL] {table_name} has orphan hadm_id")
            all_passed = False

    return all_passed
```

Trong `main()` sửa:

```python
relationships_passed = True
if any(table == "admissions" and passed for table, passed in results):
    relationships_passed = validate_relationships(spark, base_path)

all_passed = relationships_passed
for table_name, passed in results:
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {table_name}")
    all_passed = all_passed and passed
```

---

# 6. Không dùng `--include-eicu` cho training survival 12-month mortality

Trong `build_gold_dataset.py`, bạn đã thêm warning cho `--include-eicu`. Điều này đúng.

Khuyến nghị:

```bash
# Không dùng eICU khi build Gold để train 12-month mortality survival
spark-submit src/etl/build_gold_dataset.py hdfs --output-suffix _postdischarge_v2
```

Không chạy:

```bash
spark-submit src/etl/build_gold_dataset.py hdfs --include-eicu
```

trừ khi bạn chỉ muốn tạo external validation hoặc đã harmonize được follow-up label cho eICU.

---

# 7. Thứ tự chạy sau khi sửa

## 7.1. Compile trước

```bash
python3 -m py_compile src/etl/silver_admissions.py
python3 -m py_compile src/etl/build_gold_dataset.py
python3 -m py_compile src/etl/validate_silver.py
python3 -m py_compile src/etl/validate_gold.py
python3 -m py_compile src/etl/inspect_gold.py
```

## 7.2. Backup dữ liệu cũ

```bash
hdfs dfs -cp /user/dis/data/silver/admissions /user/dis/data/silver/admissions_backup_before_postdischarge_v2
hdfs dfs -cp /user/dis/data/gold/analytical_dataset /user/dis/data/gold/analytical_dataset_backup_before_postdischarge_v2
```

## 7.3. Chạy lại Silver admissions

```bash
spark-submit src/etl/silver_admissions.py hdfs
```

## 7.4. Validate Silver

```bash
spark-submit src/etl/validate_silver.py hdfs
```

## 7.5. Build Gold ra dataset suffix trước

```bash
spark-submit src/etl/build_gold_dataset.py hdfs --output-suffix _postdischarge_v2
```

## 7.6. Validate Gold suffix

```bash
spark-submit src/etl/validate_gold.py hdfs --dataset-name analytical_dataset_postdischarge_v2
```

## 7.7. Inspect Gold suffix

```bash
spark-submit src/etl/inspect_gold.py hdfs --dataset-name analytical_dataset_postdischarge_v2
```

Nếu mọi thứ pass, mới overwrite dataset final:

```bash
spark-submit src/etl/build_gold_dataset.py hdfs
```

---

# 8. Checklist kiểm tra cuối cùng

Sau khi chạy xong, kiểm tra các điều kiện sau.

## 8.1. Schema bắt buộc

Gold phải có các cột:

```text
hadm_id
subject_id
index_time
duration_days
readmission_event_30d
readmission_time_days
mortality_event_12m
mortality_time_days
mortality_time_months
event_flag_readmission
event_flag_mortality
split
```

## 8.2. Logic label readmission

```text
readmission_event_30d = 1  → readmission_time_days nằm trong [1, 30]
readmission_event_30d = 0  → readmission_time_days = 30
```

## 8.3. Logic label mortality

```text
mortality_event_12m = 1  → mortality_time_days nằm trong [1, 365]
mortality_event_12m = 0  → mortality_time_days = 365
```

## 8.4. Ý nghĩa của `duration_days`

```text
duration_days không còn là survival label T.
duration_days chỉ là feature length-of-stay của admission hiện tại.
```

## 8.5. Ý nghĩa của model training sau này

Readmission model:

```text
X = features tại thời điểm discharge
T = readmission_time_days
E = readmission_event_30d
horizon = 30 ngày
```

Mortality model:

```text
X = features tại thời điểm discharge
T = mortality_time_days hoặc mortality_time_months
E = mortality_event_12m
horizon = 365 ngày / 12 tháng
```

---

# 9. Tóm tắt các sửa đổi cần làm

Bắt buộc:

```text
1. Tính next_admittime từ toàn bộ admissions trước khi filter cohort.
2. Join next_admittime vào df_filtered theo hadm_id.
3. Bỏ lead() trong df_with_next hiện tại.
4. Thêm contains("died") vào filter discharge_location.
```

Nên làm:

```text
5. Cân nhắc same-day mortality: dùng >= 0 và max(days, 1).
6. Sửa validate_relationships() để orphan hadm_id làm fail script thật.
```

Không làm:

```text
7. Không dùng --include-eicu để train survival mortality 12 tháng nếu chưa có follow-up label tương ứng.
```

Sau các sửa đổi trên, pipeline Silver → Gold sẽ phù hợp hơn với mục tiêu thật của PredictCareAI: hỗ trợ bác sĩ ở thời điểm discharge planning, dự đoán 30-day readmission và 12-month mortality sau xuất viện.
