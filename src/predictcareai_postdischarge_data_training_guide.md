# PredictCareAI: Hướng dẫn chỉnh lại Data Processing, Label Survival, Training và Workflow Bác sĩ

**Dự án:** PREDICTCARE AI – Clinical Decision Support System  
**Mục tiêu hệ thống:** hỗ trợ bác sĩ trong quyết định chăm sóc sau xuất viện bằng cách dự đoán:

1. **30-day readmission**: nguy cơ tái nhập viện trong 30 ngày sau khi bệnh nhân được xuất viện.
2. **12-month mortality**: nguy cơ tử vong trong 12 tháng sau khi bệnh nhân được xuất viện.

---

## 1. Tóm tắt vấn đề hiện tại

Trong một số tài liệu/code cũ, survival label đang được hiểu theo dạng:

```text
T = duration_days
E = event_flag_mortality
```

Cách này chỉ phù hợp nếu bài toán là:

```text
Dự đoán tử vong trong viện, hay in-hospital mortality.
```

Nhưng bài toán thật của nhóm là:

```text
1. Readmission sau 30 ngày kể từ lúc xuất viện.
2. Mortality dài hạn trong 12 tháng kể từ lúc xuất viện.
```

Vì vậy, cần sửa lại cách định nghĩa label survival. `duration_days` không nên được dùng làm survival time cho 12-month mortality nữa. Nó chỉ nên được giữ lại như **một feature**, tức là độ dài lần nằm viện hiện tại.

---

## 2. Định vị hệ thống PredictCareAI trong workflow lâm sàng

PredictCareAI nên được mô tả là một hệ thống:

```text
Discharge-planning Clinical Decision Support System
```

Tức là hệ thống hỗ trợ bác sĩ ở thời điểm **bệnh nhân chuẩn bị xuất viện**.

### 2.1. Câu hỏi hệ thống trả lời

Tại thời điểm xuất viện, bác sĩ muốn biết:

```text
Bệnh nhân này có nguy cơ tái nhập viện trong 30 ngày tới không?
Bệnh nhân này có nguy cơ tử vong trong 12 tháng tới không?
Nếu chuyển bệnh nhân về Home, Home Health Care, hoặc SNF thì rủi ro thay đổi thế nào?
Có cần follow-up sớm, gọi điện sau xuất viện, chăm sóc tại nhà, hoặc chuyển viện dưỡng chuyên sâu không?
```

### 2.2. Workflow bác sĩ sử dụng hệ thống

Workflow đề xuất:

```text
1. Bệnh nhân đang ở giai đoạn chuẩn bị xuất viện.
2. Bác sĩ mở dashboard PredictCareAI.
3. Dashboard hiển thị danh sách bệnh nhân hiện tại hoặc bệnh nhân sắp xuất viện.
4. Bác sĩ chọn một bệnh nhân.
5. Hệ thống lấy dữ liệu admission hiện tại của bệnh nhân.
6. Backend tạo feature vector tại thời điểm xuất viện.
7. Readmission model dự đoán nguy cơ tái nhập viện 30 ngày.
8. Mortality model dự đoán nguy cơ tử vong 12 tháng.
9. Dashboard hiển thị:
   - Risk score
   - Survival curve S(t)
   - Hazard curve h(t)
   - RMST
   - Risk category: Low / Medium / High
10. Bác sĩ thử What-if Simulation:
   - Home
   - Home Health Care
   - Skilled Nursing Facility, SNF
11. Hệ thống re-predict với discharge option mới.
12. Bác sĩ dùng kết quả như một bằng chứng hỗ trợ ra quyết định discharge plan.
```

### 2.3. Hệ thống không thay thế bác sĩ

Nên ghi rõ trong báo cáo:

```text
PredictCareAI is a clinical decision support system, not an autonomous decision maker.
The system prioritizes high-risk patients and provides risk estimates to support,
not replace, clinician judgment.
```

---

## 3. Khái niệm quan trọng: index time

Đối với cả hai bài toán của nhóm, nên chọn:

```text
index_time = dischtime
```

Trong đó:

```text
dischtime = thời điểm bệnh nhân được xuất viện.
```

Lý do:

```text
Readmission 30 ngày được tính từ lúc xuất viện.
Mortality 12 tháng cũng được tính từ lúc xuất viện.
Các quyết định Home / Home Health Care / SNF cũng được đưa ra ở thời điểm xuất viện.
```

Vì vậy, mỗi dòng dữ liệu training nên đại diện cho:

```text
Một lần nhập viện đã kết thúc, tại thời điểm xuất viện.
```

---

## 4. Định nghĩa lại hai bài toán ML

## 4.1. Bài toán 1: 30-day readmission

### Mục tiêu

Dự đoán xác suất bệnh nhân sẽ bị tái nhập viện trong vòng 30 ngày sau khi xuất viện.

### Input

Feature vector tại thời điểm xuất viện:

```text
X_i = thông tin của bệnh nhân i trong admission hiện tại, chỉ dùng dữ liệu biết trước hoặc tại dischtime.
```

### Label survival

Với mỗi admission `i`:

```text
index_time_i = dischtime_i
next_admittime_i = thời điểm nhập viện kế tiếp của cùng subject_id sau dischtime_i
```

Tính:

```text
days_to_readmission_i = next_admittime_i - dischtime_i
```

Label:

```text
readmission_event_30d = 1 nếu có lần nhập viện kế tiếp trong vòng 30 ngày.
readmission_event_30d = 0 nếu không có lần nhập viện kế tiếp trong vòng 30 ngày.

readmission_time_days = days_to_readmission nếu event = 1.
readmission_time_days = 30 nếu event = 0.
```

### Ví dụ

| Case | Xuất viện | Nhập viện tiếp theo | `readmission_time_days` | `readmission_event_30d` |
|---|---:|---:|---:|---:|
| A | ngày 0 | ngày 12 | 12 | 1 |
| B | ngày 0 | không có | 30 | 0 |
| C | ngày 0 | ngày 45 | 30 | 0 |
| D | ngày 0 | ngày 3 | 3 | 1 |

Bệnh nhân C có tái nhập viện sau 45 ngày, nhưng vì bài toán chỉ xét 30 ngày nên trong horizon 30 ngày:

```text
T = 30
E = 0
```

---

## 4.2. Bài toán 2: 12-month mortality

### Mục tiêu

Dự đoán xác suất bệnh nhân tử vong trong vòng 12 tháng sau khi xuất viện.

### Input

Feature vector tại thời điểm xuất viện:

```text
X_i = thông tin của bệnh nhân i trong admission hiện tại, chỉ dùng dữ liệu biết trước hoặc tại dischtime.
```

### Label survival

Với mỗi admission `i`:

```text
index_time_i = dischtime_i
dod_i = ngày tử vong của bệnh nhân, nếu có
```

Tính:

```text
days_to_death_i = dod_i - dischtime_i
```

Label:

```text
mortality_event_12m = 1 nếu tử vong trong vòng 365 ngày sau xuất viện.
mortality_event_12m = 0 nếu không thấy tử vong trong vòng 365 ngày sau xuất viện.

mortality_time_days = days_to_death nếu event = 1.
mortality_time_days = 365 nếu event = 0.

mortality_time_months = mortality_time_days / 30.4375.
```

### Ví dụ

| Case | Xuất viện | Tử vong | `mortality_time_days` | `mortality_event_12m` |
|---|---:|---:|---:|---:|
| A | ngày 0 | ngày 80 | 80 | 1 |
| B | ngày 0 | không tử vong trong 12 tháng | 365 | 0 |
| C | ngày 0 | ngày 500 | 365 | 0 |
| D | ngày 0 | ngày 20 | 20 | 1 |

Bệnh nhân C tử vong sau 500 ngày, nhưng vì bài toán chỉ xét 12 tháng nên:

```text
T = 365
E = 0
```

---

## 5. Cấu trúc Gold Dataset mới nên có

Gold Dataset nên có mỗi hàng là một admission đã kết thúc:

```text
one row = one hospital admission = one hadm_id
```

### 5.1. Cột định danh

```text
subject_id
hadm_id
admittime
dischtime
index_time
```

Trong đó:

```text
index_time = dischtime
```

### 5.2. Features

Các cột dùng làm input cho model.

#### Demographics

```text
age
gender
```

#### Admission context

```text
admission_type
insurance
marital_status
race
length_of_stay_days = duration_days
```

Lưu ý:

```text
duration_days lúc này là feature, không phải survival label cho 12-month mortality.
```

#### Vitals 24h đầu hoặc trong admission

```text
sbp_mean
sbp_min
spo2_mean
spo2_min
hr_mean
hr_max
temperature_mean
resp_rate_mean
```

#### Labs 24h đầu hoặc trong admission

```text
creatinine_mean
bun_mean
sodium_mean
potassium_mean
wbc_mean
hemoglobin_mean
platelet_mean
lactate_mean
albumin_mean
bilirubin_total_mean
...
```

#### Diagnoses

```text
icd_chapter_01
icd_chapter_02
...
icd_chapter_21
primary_diagnosis_group
comorbidity features nếu có
```

#### Discharge planning feature

```text
discharge_location
```

Cột này rất quan trọng cho What-if Simulation.

#### Optional text features

```text
note_emb_001
note_emb_002
...
note_emb_128
```

Nhưng phải cẩn thận leakage, xem phần 8.

### 5.3. Labels

Nên có đầy đủ các label sau:

```text
readmission_time_days
readmission_event_30d

mortality_time_days
mortality_time_months
mortality_event_12m
```

### 5.4. Metadata

```text
split
admityear
source_dataset
```

Ví dụ:

```text
split = train / val / test / test_external
source_dataset = mimic / eicu
```

---

## 6. Những cột tuyệt đối không đưa vào feature X

Khi train model, cần loại các cột sau khỏi `X`:

```text
subject_id
hadm_id
admittime
dischtime
index_time
split
admityear
source_dataset

readmission_time_days
readmission_event_30d
mortality_time_days
mortality_time_months
mortality_event_12m

next_admittime
days_to_next_admission
dod
days_to_death
```

Lý do:

```text
Các cột này là ID, metadata, hoặc label/outcome.
Nếu đưa vào X sẽ gây data leakage.
```

---

## 7. Quy trình data processing đề xuất

## Step 1: Build admission index table

Input:

```text
admissions
patients
```

Output:

```text
admission_index
```

Schema:

```text
subject_id
hadm_id
admittime
dischtime
index_time
age
gender
duration_days
admityear
dod
```

Filter:

```text
age >= 18
dischtime is not null
duration_days >= 1
```

Pseudo-code:

```python
admission_index = admissions.join(patients, on="subject_id", how="left")
admission_index["index_time"] = admission_index["dischtime"]
admission_index = admission_index[admission_index["duration_days"] >= 1]
admission_index = admission_index[admission_index["age"] >= 18]
```

---

## Step 2: Build readmission label

Input:

```text
admission_index
```

Logic:

1. Sort admissions theo `subject_id`, `admittime`.
2. Với mỗi admission, tìm admission kế tiếp của cùng bệnh nhân.
3. Tính số ngày từ `dischtime` đến `next_admittime`.
4. Gán event/time cho horizon 30 ngày.

Pseudo-code pandas:

```python
import pandas as pd

adm = admission_index.sort_values(["subject_id", "admittime"]).copy()

adm["next_admittime"] = adm.groupby("subject_id")["admittime"].shift(-1)

adm["days_to_next_admission"] = (
    pd.to_datetime(adm["next_admittime"]) - pd.to_datetime(adm["dischtime"])
).dt.total_seconds() / 86400.0

adm["readmission_event_30d"] = (
    (adm["days_to_next_admission"] > 0) &
    (adm["days_to_next_admission"] <= 30)
).astype(int)

adm["readmission_time_days"] = adm["days_to_next_admission"].where(
    adm["readmission_event_30d"] == 1,
    30
)

adm["readmission_time_days"] = adm["readmission_time_days"].clip(lower=1, upper=30)
```

Pseudo-code PySpark:

```python
from pyspark.sql import Window
from pyspark.sql import functions as F

w = Window.partitionBy("subject_id").orderBy("admittime")

adm = admission_index.withColumn(
    "next_admittime",
    F.lead("admittime").over(w)
)

adm = adm.withColumn(
    "days_to_next_admission",
    (F.unix_timestamp("next_admittime") - F.unix_timestamp("dischtime")) / 86400.0
)

adm = adm.withColumn(
    "readmission_event_30d",
    F.when(
        (F.col("days_to_next_admission") > 0) &
        (F.col("days_to_next_admission") <= 30),
        1
    ).otherwise(0)
)

adm = adm.withColumn(
    "readmission_time_days",
    F.when(
        F.col("readmission_event_30d") == 1,
        F.col("days_to_next_admission")
    ).otherwise(F.lit(30.0))
)
```

---

## Step 3: Build 12-month mortality label

Input:

```text
admission_index
```

Logic:

1. Lấy `dod` từ patients.
2. Tính số ngày từ `dischtime` đến `dod`.
3. Nếu tử vong trong 365 ngày thì event = 1.
4. Nếu không thì event = 0 và time = 365.

Pseudo-code pandas:

```python
adm["days_to_death_after_discharge"] = (
    pd.to_datetime(adm["dod"]) - pd.to_datetime(adm["dischtime"])
).dt.total_seconds() / 86400.0

adm["mortality_event_12m"] = (
    (adm["days_to_death_after_discharge"] > 0) &
    (adm["days_to_death_after_discharge"] <= 365)
).astype(int)

adm["mortality_time_days"] = adm["days_to_death_after_discharge"].where(
    adm["mortality_event_12m"] == 1,
    365
)

adm["mortality_time_days"] = adm["mortality_time_days"].clip(lower=1, upper=365)
adm["mortality_time_months"] = adm["mortality_time_days"] / 30.4375
```

Pseudo-code PySpark:

```python
adm = adm.withColumn(
    "days_to_death_after_discharge",
    (F.unix_timestamp("dod") - F.unix_timestamp("dischtime")) / 86400.0
)

adm = adm.withColumn(
    "mortality_event_12m",
    F.when(
        (F.col("days_to_death_after_discharge") > 0) &
        (F.col("days_to_death_after_discharge") <= 365),
        1
    ).otherwise(0)
)

adm = adm.withColumn(
    "mortality_time_days",
    F.when(
        F.col("mortality_event_12m") == 1,
        F.col("days_to_death_after_discharge")
    ).otherwise(F.lit(365.0))
)

adm = adm.withColumn(
    "mortality_time_months",
    F.col("mortality_time_days") / F.lit(30.4375)
)
```

---

## Step 4: Build feature tables

### 4.1. Vitals features

Input:

```text
chartevents
admission_index
```

Chỉ dùng dữ liệu trước hoặc tại `index_time`.

Hai lựa chọn hợp lý:

```text
Option A: 24h đầu sau nhập viện
Option B: toàn bộ admission trước discharge
```

Với hệ thống discharge-planning, Option B có ý nghĩa hơn vì bác sĩ dự đoán tại thời điểm xuất viện. Nhưng để tránh leakage và giữ pipeline đơn giản, Option A cũng chấp nhận được nếu nhóm nói rõ.

Các feature ví dụ:

```text
sbp_mean_24h
sbp_min_24h
spo2_mean_24h
spo2_min_24h
hr_mean_24h
hr_max_24h
temperature_mean_24h
resp_rate_mean_24h
```

### 4.2. Lab features

Input:

```text
labevents
admission_index
```

Feature:

```text
creatinine_mean
creatinine_max
bun_mean
sodium_mean
wbc_mean
hemoglobin_mean
platelet_mean
lactate_mean
albumin_mean
...
```

### 4.3. Diagnoses features

Input:

```text
diagnoses_icd
```

Feature:

```text
icd_chapter_01
icd_chapter_02
...
icd_chapter_21
primary_diagnosis_group
```

### 4.4. Discharge features

Input:

```text
admissions
```

Feature:

```text
discharge_location
```

Cột này giúp dashboard chạy What-if Simulation.

---

## Step 5: Join thành Gold Dataset

Join theo `hadm_id`:

```text
admission_index_with_labels
LEFT JOIN vitals_features
LEFT JOIN lab_features
LEFT JOIN diagnosis_features
LEFT JOIN note_features nếu dùng
```

Output:

```text
gold_analytical_dataset
```

Partition:

```text
split=train
split=val
split=test
```

---

## 8. Cảnh báo quan trọng về data leakage

## 8.1. Không dùng dữ liệu sau index_time

Vì model dự đoán tại thời điểm xuất viện, không được dùng thông tin xảy ra sau `dischtime`.

Không dùng:

```text
future admissions
future labs
future vitals
future diagnoses
days_to_death
days_to_next_admission
label columns
```

## 8.2. Cẩn thận với discharge summary notes

Nếu dùng note embeddings từ discharge summary, cần cẩn thận vì discharge summary có thể chứa thông tin rất gần outcome hoặc được viết sau khi toàn bộ quá trình điều trị đã kết thúc.

Ba lựa chọn:

```text
Option 1: Không dùng note embeddings trong bản demo real-time.
Option 2: Chỉ dùng notes được tạo trước hoặc tại index_time.
Option 3: Dùng note embeddings cho retrospective experiment, nhưng ghi rõ không claim real-time deployment.
```

Khuyến nghị cho nhóm:

```text
Train structured-only model trước: demographics + vitals + labs + ICD + discharge_location.
Sau đó note embeddings để extension hoặc ablation study.
```

---

## 9. Train model như thế nào?

Nên train **hai model riêng**.

```text
Model 1: Readmission Survival Model
Model 2: Mortality Survival Model
```

Lý do:

```text
Readmission và mortality là hai event khác nhau.
Hai event có cơ chế lâm sàng khác nhau.
Hai horizon khác nhau: 30 ngày vs 365 ngày.
Output dashboard cũng khác nhau.
```

---

## 9.1. Model readmission 30 ngày

### Label

```text
T = readmission_time_days
E = readmission_event_30d
```

### Time bins

```python
time_bins = np.arange(1, 31, 1)
```

### Output

```text
S_readmit(t) với t = 1..30 ngày
risk_30d = 1 - S_readmit(30)
RMST_30d = area under S(t) từ 0 đến 30
```

---

## 9.2. Model mortality 12 tháng

### Label

```text
T = mortality_time_days
E = mortality_event_12m
```

### Time bins

Có thể dùng theo tháng:

```python
time_bins = np.arange(30, 366, 30)
```

Hoặc dùng ngày:

```python
time_bins = np.arange(1, 366, 1)
```

Khuyến nghị cho dashboard:

```text
Dùng 12 điểm theo tháng để dễ hiển thị.
```

### Output

```text
S_mortality(t) với t = 1..12 tháng
risk_12m = 1 - S_mortality(12 tháng)
RMST_12m = area under S(t) từ 0 đến 12 tháng
```

---

## 10. Code template train model survival

```python
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from xgbse import XGBSEStackedWeibull
from xgbse.metrics import concordance_index


def make_survival_y(T, E):
    y = np.empty(dtype=[("event", bool), ("duration", float)], shape=len(T))
    y["event"] = E.astype(bool)
    y["duration"] = T.astype(float)
    return y


def get_task_config(task):
    if task == "readmission":
        return {
            "time_col": "readmission_time_days",
            "event_col": "readmission_event_30d",
            "time_bins": np.arange(1, 31, 1),
            "horizon_name": "30d",
        }

    if task == "mortality":
        return {
            "time_col": "mortality_time_days",
            "event_col": "mortality_event_12m",
            "time_bins": np.arange(30, 366, 30),
            "horizon_name": "12m",
        }

    raise ValueError(f"Unknown task: {task}")


def build_xy(df_train, df_val, df_test, task):
    cfg = get_task_config(task)

    exclude_cols = {
        "subject_id", "hadm_id",
        "admittime", "dischtime", "index_time",
        "split", "admityear", "source_dataset",
        "readmission_time_days", "readmission_event_30d",
        "mortality_time_days", "mortality_time_months", "mortality_event_12m",
        "next_admittime", "days_to_next_admission",
        "dod", "days_to_death_after_discharge",
    }

    feature_cols = [c for c in df_train.columns if c not in exclude_cols]

    X_train_raw = df_train[feature_cols]
    X_val_raw = df_val[feature_cols]
    X_test_raw = df_test[feature_cols]

    T_train = df_train[cfg["time_col"]]
    E_train = df_train[cfg["event_col"]]
    T_val = df_val[cfg["time_col"]]
    E_val = df_val[cfg["event_col"]]
    T_test = df_test[cfg["time_col"]]
    E_test = df_test[cfg["event_col"]]

    imputer = SimpleImputer(strategy="median")
    X_train = pd.DataFrame(imputer.fit_transform(X_train_raw), columns=feature_cols)
    X_val = pd.DataFrame(imputer.transform(X_val_raw), columns=feature_cols)
    X_test = pd.DataFrame(imputer.transform(X_test_raw), columns=feature_cols)

    y_train = make_survival_y(T_train, E_train)
    y_val = make_survival_y(T_val, E_val)
    y_test = make_survival_y(T_test, E_test)

    return X_train, y_train, X_val, y_val, X_test, y_test, imputer, cfg, feature_cols


def train_xgbse(df_train, df_val, df_test, task):
    X_train, y_train, X_val, y_val, X_test, y_test, imputer, cfg, feature_cols = build_xy(
        df_train, df_val, df_test, task
    )

    xgb_params = {
        "objective": "survival:cox",
        "eval_metric": "cox-nloglik",
        "tree_method": "hist",
        "device": "cuda",
        "learning_rate": 0.05,
        "max_depth": 4,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "seed": 42,
    }

    model = XGBSEStackedWeibull(xgb_params=xgb_params)

    model.fit(
        X_train,
        y_train,
        validation_data=(X_val, y_val),
        early_stopping_rounds=20,
        time_bins=cfg["time_bins"],
    )

    train_preds = model.predict(X_train)
    val_preds = model.predict(X_val)
    test_preds = model.predict(X_test)

    metrics = {
        "train_c_index": concordance_index(y_train, train_preds),
        "val_c_index": concordance_index(y_val, val_preds),
        "test_c_index": concordance_index(y_test, test_preds),
    }

    return model, imputer, feature_cols, cfg, metrics, test_preds
```

---

## 11. Evaluation metrics nên báo cáo

## 11.1. Survival metrics chính

```text
C-index
Integrated Brier Score, IBS
Calibration curve
```

## 11.2. Horizon-specific metrics

Cho readmission:

```text
AUC at 30 days
Risk calibration at 30 days
Sensitivity/Specificity ở threshold clinical
```

Cho mortality:

```text
AUC at 12 months
Risk calibration at 12 months
Sensitivity/Specificity ở threshold clinical
```

## 11.3. Business/clinical metrics

```text
Số bệnh nhân high-risk được flag
Tỉ lệ high-risk trong ward
Số bệnh nhân cần follow-up sớm
Số bệnh nhân được đề xuất Home Health Care/SNF
```

---

## 12. Backend API nên thiết kế như thế nào?

Nên có hai endpoint riêng:

```text
POST /predict/readmission
POST /predict/mortality
```

Hoặc một endpoint chung:

```text
POST /predict?task=readmission
POST /predict?task=mortality
```

### 12.1. Request input

```json
{
  "patient_id": "10575854",
  "hadm_id": "123456",
  "age": 69,
  "gender": "M",
  "admission_type": "EMERGENCY",
  "duration_days": 7.2,
  "discharge_location": "HOME",
  "sbp_mean": 97,
  "spo2_mean": 90,
  "hr_mean": 80,
  "creatinine_mean": 1.4,
  "wbc_mean": 12.1,
  "icd_chapter_09": 1,
  "icd_chapter_10": 0
}
```

### 12.2. Readmission response

```json
{
  "task": "readmission",
  "horizon": "30d",
  "risk_30d": 0.28,
  "risk_percent": 28.0,
  "risk_level": "medium",
  "survival_function": [
    {"day": 0, "s_t": 1.00},
    {"day": 1, "s_t": 0.99},
    {"day": 30, "s_t": 0.72}
  ],
  "hazard_function": [
    {"day": 1, "h_t": 0.01},
    {"day": 30, "h_t": 0.03}
  ],
  "rmst_30d": 25.4
}
```

### 12.3. Mortality response

```json
{
  "task": "mortality",
  "horizon": "12m",
  "risk_12m": 0.37,
  "risk_percent": 37.0,
  "risk_level": "high",
  "survival_function": [
    {"month": 0, "s_t": 1.00},
    {"month": 1, "s_t": 0.98},
    {"month": 12, "s_t": 0.63}
  ],
  "hazard_function": [
    {"month": 1, "h_t": 0.02},
    {"month": 12, "h_t": 0.05}
  ],
  "rmst_12m": 9.8
}
```

---

## 13. What-if Simulation nên làm như thế nào?

Prototype hiện tại có các lựa chọn:

```text
Home
Home Health Care
SNF
```

Cách triển khai demo đơn giản:

```text
1. Lấy feature vector gốc của bệnh nhân.
2. Thay đổi giá trị discharge_location.
3. Chạy lại model.
4. So sánh risk và RMST giữa các phương án.
```

Ví dụ:

```python
def run_what_if(patient_features, options, task):
    results = []

    for option in options:
        x = patient_features.copy()
        x["discharge_location"] = option

        pred = model.predict(x)
        risk = 1 - pred.iloc[0, -1]
        rmst = pred.iloc[0].sum()

        results.append({
            "option": option,
            "risk": risk,
            "rmst": rmst,
        })

    return results
```

Cần ghi rõ trong báo cáo:

```text
The What-if module is a counterfactual simulation for decision support.
It estimates how predicted risk changes when discharge disposition is modified,
but it does not establish a causal treatment effect.
```

---

## 14. Frontend dashboard nên hiển thị gì?

## 14.1. Macro Dashboard

Mục tiêu: giúp trưởng khoa/quản lý thấy toàn cảnh.

Hiển thị:

```text
Tổng số bệnh nhân nội trú
Số bệnh nhân high-risk readmission
Số bệnh nhân high-risk mortality
Tỉ lệ tử vong thực tế
Tỉ lệ tái nhập viện dự kiến
Phân bố rủi ro theo khoa/phòng
Drug demand hoặc resource demand nếu có
```

## 14.2. Micro Dashboard

Mục tiêu: giúp bác sĩ xử lý từng bệnh nhân.

Hiển thị:

```text
Danh sách bệnh nhân hiện tại hoặc sắp xuất viện
Risk readmission 30 ngày
Risk mortality 12 tháng
Sinh hiệu chính: SBP, SpO2, HR, Temperature
Tình trạng: Stable / Checking / Intervention
Nút “Mô phỏng AI”
```

## 14.3. What-if Simulation

Hiển thị:

```text
Toggle task:
    Readmission 30d
    Mortality 12m

Intervention options:
    Home
    Home Health Care
    SNF

Charts:
    Survival curve S(t)
    Hazard curve h(t)
    RMST
    Risk change
```

---

## 15. Chỉnh lại phần báo cáo như thế nào?

### 15.1. Đoạn mô tả hệ thống

Có thể viết:

```text
PredictCareAI is designed as a discharge-planning clinical decision support system.
At the point of discharge, the system transforms the patient's current admission data
into a structured feature vector and estimates two post-discharge risks:
30-day hospital readmission and 12-month mortality. The goal is not to replace
clinician judgment, but to prioritize high-risk patients and support comparison
between discharge options such as Home, Home Health Care, and Skilled Nursing Facility.
```

### 15.2. Đoạn mô tả readmission label

```text
For the 30-day readmission task, the index time is defined as the discharge time
of the current admission. The event indicator E is set to 1 if the patient has
a subsequent hospital admission within 30 days after discharge, and 0 otherwise.
The survival time T is the number of days from discharge to the next admission
if the event occurs within 30 days, and is administratively censored at 30 days otherwise.
```

### 15.3. Đoạn mô tả mortality label

```text
For the 12-month mortality task, the index time is also defined as the discharge time.
The event indicator E is set to 1 if the patient dies within 365 days after discharge,
and 0 otherwise. The survival time T is the number of days from discharge to death
if death occurs within 12 months, and is administratively censored at 365 days otherwise.
```

### 15.4. Đoạn cảnh báo sửa lại từ tài liệu cũ

```text
In the earlier implementation, duration_days and event_flag_mortality were used as
the survival label, which corresponds to an in-hospital mortality formulation. Since
our final system targets post-discharge 12-month mortality, duration_days is retained
only as a feature representing length of stay, while the mortality survival label is
redefined from discharge time to death or censoring at 365 days.
```

---

## 16. Checklist cần làm lại

### Data processing

- [ ] Tạo `index_time = dischtime`.
- [ ] Tạo `readmission_time_days`.
- [ ] Tạo `readmission_event_30d`.
- [ ] Tạo `mortality_time_days`.
- [ ] Tạo `mortality_time_months`.
- [ ] Tạo `mortality_event_12m`.
- [ ] Giữ `duration_days` như feature, không dùng làm mortality survival T.
- [ ] Không dùng dữ liệu sau `dischtime` làm feature.
- [ ] Kiểm tra leakage từ discharge notes.
- [ ] Partition Gold theo `split=train/val/test`.

### Training

- [ ] Train model riêng cho readmission.
- [ ] Train model riêng cho mortality.
- [ ] Readmission dùng `T=readmission_time_days`, `E=readmission_event_30d`.
- [ ] Mortality dùng `T=mortality_time_days`, `E=mortality_event_12m`.
- [ ] Readmission time bins: `1..30` ngày.
- [ ] Mortality time bins: 12 mốc tháng hoặc `1..365` ngày.
- [ ] Evaluate bằng C-index, IBS, AUC tại horizon, calibration.

### Backend/API

- [ ] Có endpoint predict readmission.
- [ ] Có endpoint predict mortality.
- [ ] API trả về risk score, S(t), h(t), RMST.
- [ ] API hỗ trợ What-if Simulation bằng cách thay đổi `discharge_location`.

### Frontend

- [ ] Dashboard hiển thị danh sách bệnh nhân hiện tại/sắp xuất viện.
- [ ] Hiển thị risk readmission 30 ngày.
- [ ] Hiển thị risk mortality 12 tháng.
- [ ] Có toggle giữa Readmission và Mortality.
- [ ] Có survival curve, hazard curve, RMST.
- [ ] Có What-if Home / Home Health Care / SNF.

---

## 17. Kết luận thiết kế mới

Thiết kế phù hợp nhất với ý định của nhóm là:

```text
PredictCareAI = hệ thống hỗ trợ discharge planning.
```

Tại thời điểm xuất viện, hệ thống dùng dữ liệu admission hiện tại để dự đoán hai outcome sau xuất viện:

```text
1. Tái nhập viện trong 30 ngày.
2. Tử vong trong 12 tháng.
```

Do đó, data processing và survival label phải được xây dựng quanh:

```text
index_time = dischtime
```

Không nên dùng:

```text
duration_days + event_flag_mortality
```

làm label cho bài toán 12-month mortality, vì cặp đó chỉ biểu diễn in-hospital mortality. Trong thiết kế mới, `duration_days` là feature, còn label mortality dài hạn phải là:

```text
T = mortality_time_days từ discharge đến death hoặc censoring 365 ngày
E = mortality_event_12m
```

Tương tự, label readmission phải là:

```text
T = readmission_time_days từ discharge đến next admission hoặc censoring 30 ngày
E = readmission_event_30d
```

Đây là chỉnh sửa quan trọng nhất để mô hình, backend, dashboard và workflow bác sĩ khớp với nhau.
