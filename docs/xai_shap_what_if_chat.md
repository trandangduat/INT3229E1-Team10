# Gợi ý triển khai xAI đơn giản cho mô hình survival / What-If Simulation

Đúng, vậy mình khuyên **không làm xAI quá phức tạp nữa**. Làm mức **vừa đủ, đẹp, dễ demo, dễ viết báo cáo**:

```text
xAI = SHAP explanation cho risk tại horizon chính
Readmission: giải thích risk_30d
Mortality: giải thích risk_12m
What-if: giải thích delta risk khi đổi Home → SNF / Home Health
```

Không cần làm time-dependent SHAP, uncertainty, causal layer, similar patient support nữa.

---

# 1. Mức xAI nên làm

Với thời gian ít, làm 3 thứ thôi:

```text
1. Global SHAP
→ model nhìn chung dựa vào feature nào nhiều nhất.

2. Local SHAP
→ với một bệnh nhân cụ thể, feature nào làm tăng/giảm risk.

3. What-if delta explanation
→ khi chọn SNF, risk/RMST thay đổi bao nhiêu và feature discharge_location đóng góp thế nào.
```

Trong dashboard, thêm một panel:

```text
Why this prediction?
```

Panel này hiển thị:

```text
Top factors increasing risk
Top factors decreasing risk
Effect of selected intervention
```

Vậy là đủ đẹp và hợp với thiết kế CDSS/What-If Simulation hiện tại của nhóm, vì hệ thống đã có `S(t)`, `h(t)`, RMST và What-If Simulation.

---

# 2. Cách làm đơn giản nhất

Vì survival model/XGBSE đôi khi khó giải thích trực tiếp, cách dễ nhất là:

```text
Train thêm 2 model phụ chỉ để xAI:
    1. XGBoostClassifier cho readmission_event_30d
    2. XGBoostClassifier cho mortality_event_12m
```

Hai model này không thay survival model chính. Survival model chính vẫn dùng để vẽ:

```text
S(t), h(t), RMST
```

Còn xAI model phụ chỉ dùng để giải thích:

```text
risk_30d
risk_12m
```

Kiến trúc:

```text
Gold Data
   ↓
Main Survival Model
   → S(t), h(t), RMST, risk

Gold Data
   ↓
XAI Horizon Model
   → SHAP explanation cho risk_30d / risk_12m
```

Cách này **rất thực tế**, dễ làm, dễ debug, dễ viết báo cáo.

---

# 3. Vì sao không cần giải thích toàn bộ survival curve?

Vì bác sĩ thường cần câu trả lời:

```text
Vì sao bệnh nhân này có nguy cơ tái nhập viện 30 ngày cao?
Vì sao bệnh nhân này có nguy cơ tử vong 12 tháng cao?
```

Chứ không nhất thiết cần giải thích từng điểm trên đường cong.

Do đó, mình khuyên:

```text
Readmission:
    giải thích risk tại ngày 30

Mortality:
    giải thích risk tại tháng 12
```

Còn đường cong `S(t)` và `h(t)` vẫn hiển thị như cũ.

---

# 4. Step-by-step triển khai

## Step 1 — Chuẩn bị feature và label

Từ Gold Data, tạo:

```python
TASK = "readmission"  # hoặc "mortality"

if TASK == "readmission":
    label_col = "readmission_event_30d"

elif TASK == "mortality":
    label_col = "mortality_event_12m"
```

Loại các cột không được đưa vào feature:

```python
exclude_cols = [
    "subject_id",
    "hadm_id",
    "admittime",
    "dischtime",
    "index_time",
    "split",
    "admityear",

    "readmission_time_days",
    "readmission_event_30d",
    "mortality_time_days",
    "mortality_time_months",
    "mortality_event_12m",

    "next_admittime",
    "days_to_next_admission",
    "dod",
    "days_to_death_after_discharge"
]

feature_cols = [c for c in df_train.columns if c not in exclude_cols]
```

---

## Step 2 — Train XGBoostClassifier cho xAI

Ví dụ cho readmission:

```python
import xgboost as xgb
from sklearn.impute import SimpleImputer
from sklearn.metrics import roc_auc_score
import joblib
import json
import pandas as pd

label_col = "readmission_event_30d"

X_train_raw = df_train[feature_cols]
y_train = df_train[label_col]

X_val_raw = df_val[feature_cols]
y_val = df_val[label_col]

X_test_raw = df_test[feature_cols]
y_test = df_test[label_col]

imputer = SimpleImputer(strategy="median")

X_train = pd.DataFrame(
    imputer.fit_transform(X_train_raw),
    columns=feature_cols
)

X_val = pd.DataFrame(
    imputer.transform(X_val_raw),
    columns=feature_cols
)

X_test = pd.DataFrame(
    imputer.transform(X_test_raw),
    columns=feature_cols
)

xai_model = xgb.XGBClassifier(
    n_estimators=300,
    max_depth=4,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    objective="binary:logistic",
    eval_metric="auc",
    tree_method="hist",
    random_state=42
)

xai_model.fit(
    X_train,
    y_train,
    eval_set=[(X_val, y_val)],
    verbose=False
)

pred_test = xai_model.predict_proba(X_test)[:, 1]
auc = roc_auc_score(y_test, pred_test)

print("XAI model AUC:", auc)

joblib.dump(xai_model, "xai_readmission_model.pkl")
joblib.dump(imputer, "xai_readmission_imputer.pkl")

with open("xai_feature_cols.json", "w") as f:
    json.dump(feature_cols, f)
```

Làm tương tự cho mortality:

```python
label_col = "mortality_event_12m"
```

---

## Step 3 — Tạo SHAP explainer

```python
import shap
import joblib
import pandas as pd

xai_model = joblib.load("xai_readmission_model.pkl")
imputer = joblib.load("xai_readmission_imputer.pkl")

background = X_train.sample(n=min(1000, len(X_train)), random_state=42)

explainer = shap.TreeExplainer(xai_model, background)

joblib.dump(explainer, "xai_readmission_explainer.pkl")
```

Nếu chạy chậm, giảm background xuống:

```python
background = X_train.sample(n=300, random_state=42)
```

---

## Step 4 — Local explanation cho một bệnh nhân

```python
import numpy as np

def explain_patient(patient_df, model, imputer, explainer, feature_cols, top_k=5):
    X_raw = patient_df[feature_cols]

    X = pd.DataFrame(
        imputer.transform(X_raw),
        columns=feature_cols
    )

    risk = model.predict_proba(X)[0, 1]

    shap_values = explainer.shap_values(X)

    if isinstance(shap_values, list):
        shap_values = shap_values[1]

    shap_row = shap_values[0]

    rows = []
    for feature, value, shap_value in zip(feature_cols, X.iloc[0], shap_row):
        rows.append({
            "feature": feature,
            "value": float(value),
            "shap_value": float(shap_value),
            "direction": "increase_risk" if shap_value > 0 else "decrease_risk"
        })

    rows = sorted(rows, key=lambda x: abs(x["shap_value"]), reverse=True)

    top_risk = [r for r in rows if r["shap_value"] > 0][:top_k]
    top_protective = [r for r in rows if r["shap_value"] < 0][:top_k]

    return {
        "risk": float(risk),
        "top_risk_factors": top_risk,
        "top_protective_factors": top_protective
    }
```

---

# 5. What-if explanation đơn giản

Khi bác sĩ chọn `SNF`, bạn làm:

```text
1. Predict baseline, ví dụ HOME.
2. Predict scenario, ví dụ SNF.
3. Tính delta risk.
4. Tính SHAP cho baseline và SNF.
5. Tính delta SHAP.
```

Code ý tưởng:

```python
def apply_discharge_option(patient_df, option):
    x = patient_df.copy()

    # Nếu discharge_location là one-hot
    for col in [
        "discharge_location_HOME",
        "discharge_location_HOME_HEALTH",
        "discharge_location_SNF"
    ]:
        if col in x.columns:
            x[col] = 0

    if option == "HOME":
        x["discharge_location_HOME"] = 1
    elif option == "HOME_HEALTH":
        x["discharge_location_HOME_HEALTH"] = 1
    elif option == "SNF":
        x["discharge_location_SNF"] = 1

    return x
```

```python
def explain_whatif(patient_df, baseline_option, scenario_option,
                   model, imputer, explainer, feature_cols):

    base_df = apply_discharge_option(patient_df, baseline_option)
    scenario_df = apply_discharge_option(patient_df, scenario_option)

    base_exp = explain_patient(base_df, model, imputer, explainer, feature_cols)
    scenario_exp = explain_patient(scenario_df, model, imputer, explainer, feature_cols)

    delta_risk = scenario_exp["risk"] - base_exp["risk"]

    return {
        "baseline_option": baseline_option,
        "scenario_option": scenario_option,
        "baseline_risk": base_exp["risk"],
        "scenario_risk": scenario_exp["risk"],
        "delta_risk": delta_risk,
        "scenario_top_risk_factors": scenario_exp["top_risk_factors"],
        "scenario_top_protective_factors": scenario_exp["top_protective_factors"],
        "text": f"Kịch bản {scenario_option} làm risk thay đổi {delta_risk * 100:.1f}% so với {baseline_option}."
    }
```

---

# 6. Response API nên trả về đơn giản như này

```json
{
  "task": "readmission",
  "intervention": "SNF",

  "prediction": {
    "risk_30d": 0.51,
    "rmst": 23.8,
    "survival_function": [],
    "hazard_function": []
  },

  "xai": {
    "method": "SHAP",
    "target": "30-day readmission risk",

    "top_risk_factors": [
      {
        "feature": "spo2_mean",
        "display_name": "Độ bão hòa oxy trung bình",
        "value": 90,
        "direction": "increase_risk",
        "text": "SpO2 thấp làm tăng nguy cơ tái nhập viện."
      },
      {
        "feature": "age",
        "display_name": "Tuổi",
        "value": 80,
        "direction": "increase_risk",
        "text": "Tuổi cao làm tăng nguy cơ sau xuất viện."
      }
    ],

    "top_protective_factors": [
      {
        "feature": "sbp_mean",
        "display_name": "Huyết áp tâm thu trung bình",
        "value": 120,
        "direction": "decrease_risk",
        "text": "Huyết áp ổn định làm giảm nguy cơ."
      }
    ],

    "whatif_summary": {
      "baseline": "HOME",
      "scenario": "SNF",
      "baseline_risk": 0.68,
      "scenario_risk": 0.51,
      "delta_risk": -0.17,
      "text": "Mô hình dự đoán SNF làm giảm risk 17% so với HOME."
    },

    "disclaimer": "Đây là giải thích của mô hình dự đoán, không phải bằng chứng nhân quả."
  }
}
```

---

# 7. Frontend chỉ cần thêm panel này

Trong What-If Simulation, thêm block:

```text
Why this prediction?
```

Nội dung:

```text
Main risk drivers
- SpO2 thấp
- Tuổi cao
- Creatinine cao

Protective factors
- Huyết áp ổn định
- Nhịp tim bình thường

Effect of selected option
- SNF làm predicted risk giảm 17% so với HOME

Clinical note
- Đây là giải thích của mô hình, không chứng minh quan hệ nhân quả.
```

Vậy là đủ tốt để demo.

---

# 8. Viết báo cáo thế nào cho gọn?

Bạn có thể viết:

```text
To improve interpretability, we add a lightweight SHAP-based explainability
module to the What-If Simulation. Since the survival model produces
time-dependent outputs, the xAI module focuses on clinically meaningful
horizon-level risks: 30-day readmission risk and 12-month mortality risk.
For each task, an auxiliary XGBoost classifier is trained on the same Gold
features to approximate the event risk at the target horizon. SHAP values are
then computed to identify the top factors that increase or decrease the
patient-specific risk.

In the What-If module, the system compares the baseline discharge option with
the selected scenario, such as Home versus SNF. The dashboard reports the
change in predicted risk and displays the most influential clinical factors.
The explanation is intended to support clinician understanding of model
behavior and does not imply causal treatment effects.
```

---

# 9. Checklist làm nhanh

```text
[ ] Train xAI_readmission_model.pkl
[ ] Train xAI_mortality_model.pkl
[ ] Save imputer + feature_cols
[ ] Build SHAP explainer
[ ] Viết explain_patient()
[ ] Viết explain_whatif()
[ ] API trả top_risk_factors, top_protective_factors, whatif_summary
[ ] Frontend thêm panel “Why this prediction?”
[ ] Báo cáo ghi rõ: lightweight SHAP-based xAI, không causal
```

Kết luận: **làm xAI đơn giản bằng SHAP cho risk_30d và risk_12m là hợp lý nhất**. Không cần giải thích toàn bộ đường cong, chỉ cần giải thích risk tại horizon chính và delta khi chọn SNF/Home Health/Home.
