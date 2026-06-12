# KẾ HOẠCH TÍCH HỢP XAI (EXPLAINABLE AI) VÀO PREDICTCARE AI - CẬP NHẬT (WHAT-IF STRATEGY)

**Dự án:** PREDICTCARE AI – CDSS Dashboard (Team 10)
**Mục tiêu:** Cung cấp lý giải rủi ro (Risk Explanation) và phân tích kịch bản (What-If Analysis) cho bác sĩ một cách nhẹ nhàng, dễ hiểu và dễ triển khai, thay vì cố gắng giải thích toàn bộ mô hình survival phức tạp.

---

## 1. PHƯƠNG PHÁP XAI LỰA CHỌN: SHAP VỚI MÔ HÌNH PHỤ (AUXILIARY MODEL)
Thay vì cố gắng giải thích trực tiếp mô hình survival (XGBSE/Cox PH) vốn tạo ra đầu ra phụ thuộc thời gian (time-dependent) phức tạp, chúng ta sẽ áp dụng chiến lược **Horizon-Specific Explanation**:

1.  **Train 2 mô hình phụ (Auxiliary Models):** Sử dụng `XGBoostClassifier` tiêu chuẩn để dự đoán rủi ro tại một mốc thời gian lâm sàng quan trọng:
    *   Mô hình 1: Dự đoán tái nhập viện trong 30 ngày (`readmission_event_30d`).
    *   Mô hình 2: Dự đoán tử vong trong 12 tháng (`mortality_event_12m`).
2.  **Sử dụng `shap.TreeExplainer`** trên các mô hình phụ này để giải thích rủi ro cục bộ (Local SHAP) cho bệnh nhân.
3.  **Tập trung vào What-If Delta:** Giải thích sự thay đổi rủi ro (delta risk) khi thay đổi địa điểm xuất viện (ví dụ: `HOME` vs `SNF`).

**Ưu điểm:** Đơn giản, tính toán nhanh, dễ dàng tích hợp vào API và Frontend, bác sĩ dễ hiểu (chỉ tập trung vào rủi ro tại mốc thời gian cụ thể thay vì cả đường cong).

---

## 2. NHỮNG THAY ĐỔI CẦN THIẾT TRÊN CÁC LAYER

### 2.1. ML Layer (`src/ml/`)
**Quy trình mới:**
1.  Bên cạnh việc train Main Survival Model, chuẩn bị script để train 2 mô hình phụ bằng `xgboost.XGBClassifier` sử dụng cùng Gold Dataset.
2.  Xử lý missing value bằng `SimpleImputer(strategy="median")` trước khi train mô hình phụ.
3.  Lưu các artifact sau vào thư mục model:
    *   `xai_readmission_model.pkl` & `xai_mortality_model.pkl`
    *   `xai_readmission_imputer.pkl` & `xai_mortality_imputer.pkl`
    *   `xai_feature_cols.json`
4.  Tạo và lưu `shap.TreeExplainer` cho từng mô hình phụ với background dataset nhỏ (ví dụ 300 samples) để làm base value:
    *   `xai_readmission_explainer.pkl` & `xai_mortality_explainer.pkl`

### 2.2. API Layer (`src/api/`)
**Tích hợp XAI vào Endpoint What-If Simulation:**
- Khi Frontend gọi API What-If (truyền vào `baseline_option` và `scenario_option`), API sẽ thực hiện:
  1.  Tính dự đoán `S(t)`, `h(t)`, RMST từ **Main Survival Model** (Giữ nguyên logic cũ).
  2.  Load **Auxiliary Model**, **Imputer**, và **Explainer** tương ứng với task.
  3.  Tính toán rủi ro dự đoán và SHAP values cho bệnh nhân ở trạng thái `baseline_option`.
  4.  Tính toán rủi ro dự đoán và SHAP values cho bệnh nhân ở trạng thái `scenario_option`.
  5.  Tổng hợp và trả về kết quả XAI dưới dạng JSON bao gồm: Top Risk Factors, Top Protective Factors và What-If Summary (chênh lệch rủi ro).

**Format Response API (Phần XAI):**
```json
{
  "task": "readmission",
  "intervention": "SNF",
  "prediction": { ... },
  "xai": {
    "method": "SHAP",
    "target": "30-day readmission risk",
    "top_risk_factors": [
      {
        "feature": "spo2_mean",
        "value": 90,
        "direction": "increase_risk"
      }
    ],
    "top_protective_factors": [ ... ],
    "whatif_summary": {
      "baseline": "HOME",
      "scenario": "SNF",
      "delta_risk": -0.17,
      "text": "Mô hình dự đoán SNF làm giảm risk 17% so với HOME."
    },
    "disclaimer": "Đây là giải thích của mô hình dự đoán, không phải bằng chứng nhân quả."
  }
}
```

### 2.3. Frontend Layer (`src/frontend/`)
**Cập nhật giao diện Dashboard (Chỉ cần 1 thay đổi nhỏ nhưng hiệu quả):**
- **What-If Simulation Panel:**
  - Thêm một khối UI mới mang tên **"Why this prediction?"**.
  - Hiển thị danh sách (bullet points) đơn giản thay vì biểu đồ phức tạp:
    - **Main risk drivers (Top yếu tố tăng rủi ro):** Text màu đỏ (ví dụ: SpO2 thấp, Tuổi cao).
    - **Protective factors (Top yếu tố giảm rủi ro):** Text màu xanh (ví dụ: Huyết áp ổn định).
    - **Effect of selected option:** Hiển thị phần `text` từ `whatif_summary`.
    - **Clinical Note:** Hiển thị phần `disclaimer`.

---

## 3. LỘ TRÌNH THỰC HIỆN (ROADMAP)

| Phase | Task | Người phụ trách | Output |
|---|---|---|---|
| **Phase 1: R&D trong Notebook** | Chạy thử nghiệm mô phỏng dữ liệu, train Auxiliary Model, tạo explainer, và viết các hàm tính delta SHAP cho kịch bản What-if. | ML Engineer | `model/mortality_model/xai_shap_experiment_what_if.ipynb` |
| **Phase 2: ML Pipeline (Aux Models)** | Viết script train 2 mô hình XAI (Readmission & Mortality) từ dữ liệu Gold thực tế và xuất `.pkl`. | ML Engineer | `src/ml/train_xai_models.py` |
| **Phase 3: API Integration** | Cập nhật logic của `/api/predict/whatif` để gộp thêm block `"xai"` vào response sử dụng các file `.pkl`. | Backend Dev | Cập nhật mã nguồn API. |
| **Phase 4: Frontend UI** | Cập nhật component What-if, parse JSON và render bảng "Why this prediction?". Ánh xạ tên biến (`feature_name_mapping`). | Frontend Dev | Giao diện hiển thị lý giải XAI trên Web. |
| **Phase 5: Báo cáo** | Cập nhật báo cáo thiết kế theo hướng tiếp cận "Lightweight SHAP-based xAI". | Documentation Team | Báo cáo hoàn thiện. |

---

## 4. BÁO CÁO THUYẾT MINH DỰ KIẾN (Tham khảo để viết Document)
*"To improve interpretability, we add a lightweight SHAP-based explainability module to the What-If Simulation. Since the survival model produces time-dependent outputs, the xAI module focuses on clinically meaningful horizon-level risks: 30-day readmission risk and 12-month mortality risk. For each task, an auxiliary XGBoost classifier is trained on the same Gold features to approximate the event risk at the target horizon. SHAP values are then computed to identify the top factors that increase or decrease the patient-specific risk. In the What-If module, the system compares the baseline discharge option with the selected scenario... The explanation is intended to support clinician understanding of model behavior and does not imply causal treatment effects."*
