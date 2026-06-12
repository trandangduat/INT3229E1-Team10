# 🚀 Hướng Dẫn Thực Thi Quy Trình Phân Tích Sinh Tồn (12-Month Mortality Pipeline)

### 1. Nạp dữ liệu (Data Loading)
- Hệ thống tải bộ dữ liệu từ Kaggle Storage vào RAM thông qua định dạng file `.parquet` nhằm tối ưu hóa tốc độ đọc ghi và tiết kiệm bộ nhớ. 
- Quá trình nạp được chia thành 3 tập độc lập với cấu trúc dữ liệu cụ thể: Tập huấn luyện (`Train`: 271,739 mẫu, 248 cột), Tập tối ưu (`Validation`: 57,437 mẫu, 248 cột), và Tập kiểm thử cuối cùng (`Test`: 53,875 mẫu, 248 cột).

### 2. Tiền xử lý dữ liệu (Data Preprocessing)
Xây dựng hàm `preprocess_data` nhằm đóng gói toàn bộ các bước làm sạch dữ liệu, ngăn chặn triệt để hiện tượng rò rỉ dữ liệu (Data Leakage):
- **Loại bỏ đặc trưng rò rỉ:** Loại bỏ hoàn toàn các trường dữ liệu mang tính chất định danh (`subject_id`, `hadm_id`), các mốc thời gian hành chính (`admittime`, `dischtime`), dữ liệu tương lai (`dod`, `days_to_death_after_discharge`) cùng các nhãn thuộc bài toán Tái nhập viện (Readmission).
- **Định hình mục tiêu bài toán (Target Formulation):** Trích xuất hai thuộc tính nền tảng của bài toán Survival Analysis bao gồm: Nhãn thời gian theo dõi liên tục T (`mortality_time_days` - được chặn tối đa ở mốc 365 ngày) và Nhãn biến cố đích E (`mortality_event_12m`).
- **Xử lý đặc trưng dạng phân loại (Categorical):** Hệ thống tự động phát hiện 6 cột dạng chuỗi ký tự bao gồm `['gender', 'admission_type', 'insurance', 'marital_status', 'race', 'discharge_location']`. Toàn bộ các cột này ngay lập tức được chuyển đổi sang dạng số nguyên thông qua lớp `OrdinalEncoder`.
- **Xử lý giá trị khuyết thiếu (Imputation):** Sử dụng thuật toán `SimpleImputer` với chiến lược lấy giá trị trung vị (`median`) để điền khuyết cho toàn bộ các cột số còn lại (như huyết áp, nhịp tim, creatinine, WBC... và 128 chiều Note Embeddings).
- **Hoàn tất tiền xử lý:** Sau khi đi qua toàn bộ luồng làm sạch, bộ dữ liệu chuẩn hóa cuối cùng ghi nhận có **236 Features (cột)** được đưa vào mô hình.

### 3. Huấn luyện Mô hình Cơ sở (Baseline Model - Cox PH)
- Khởi tạo và huấn luyện mô hình thống kê truyền thống **Cox Proportional Hazards (Cox PH)** thông qua thư viện `lifelines`, kết hợp tham số phạt `penalizer=0.1` để kiểm soát đa cộng tuyến.
- Quá trình huấn luyện ghi nhận hiệu năng tổng quát rất tốt thông qua chỉ số **Concordance Index (C-index)**: Mô hình đạt `0.8594` trên cả tập Train và Validation, đồng thời giữ vững mức `0.8321` trên tập Test độc lập.

### 4. Dự báo và Trực quan hóa Biểu đồ Baseline
- Sử dụng mô hình Cox PH đã huấn luyện ở bước trước để dự báo Hàm sinh tồn S(t) cho 18 bệnh nhân mẫu từ tập dữ liệu Test.
- Vẽ đồ thị biểu diễn xác suất sống sót liên tục của các bệnh nhân kéo dài từ ngày 0 đến ngày 365.
- Trích xuất điểm rủi ro tử vong tích lũy (Cumulative Risk) tại mốc thời gian giới hạn cuối cùng nhằm đưa ra đánh giá sơ bộ về mức độ nguy hiểm của ca bệnh.

### 5. Định dạng cấu trúc nhãn cho XGBSE (Structured Array Formatter)
- Thư viện `xgbse` yêu cầu cấu hình nhãn đầu vào theo định dạng mảng cấu trúc đặc biệt của thư viện `lifelines` / `scikit-survival`.
- Tiến hành đóng gói thành công cặp nhãn hành trình (E, T) của cả 3 tập dữ liệu thành dạng mảng bản ghi NumPy (Structured Array) chứa hai trường thuộc tính độc lập là `event` (Biến cố) và `time` (Thời gian), dữ liệu lúc này đã sẵn sàng để đưa vào Optuna.

### 6. Tối ưu hóa Siêu tham số với Optuna
- Thiết lập các mốc thời gian đánh giá (`TIME_BINS`) chạy liên tục theo từng khoảng từ ngày 30 đến ngày 365 (khoảng cách bước nhảy 30 ngày).
- Định nghĩa hàm mục tiêu `objective` cho framework **Optuna** và tiến hành rà quét 10 vòng thử nghiệm (trials) để dò tìm không gian tham số tối ưu cho mô hình nâng cao **XGBSEStackedWeibull**.
- **Kết quả tối ưu:** Quá trình chạy ghi nhận mức C-index tốt nhất đạt `0.8508` với bộ tham số vàng được xác định là: `{'learning_rate': 0.0839, 'max_depth': 6, 'min_child_weight': 22, 'subsample': 0.7739, 'colsample_bytree': 0.8611, 'lambda': 0.0512, 'alpha': 0.0126}`.

### 7. Huấn luyện Mô hình Chính và Đánh giá Đồng thời (Final Model Training)
- Trích xuất bộ siêu tham số tốt nhất từ Optuna, cấu hình lại hàm mất mát dạng sinh tồn (`objective: 'survival:cox'`) trên phần cứng GPU (`device: 'cuda'`).
- Tiến hành huấn luyện mô hình **XGBSE Final** trên toàn bộ tập Train, áp dụng cơ chế dừng sớm `early_stopping_rounds=10` để triệt tiêu Overfitting.
- **Đánh giá hiệu năng:** C-index của mô hình XGBSE đạt `0.8552` (Train) và `0.8509` (Validation). Đặc biệt trên tập Test, mô hình đạt `0.8441`, hoàn thành kì vọng >= 0.70 của dự án.

### 8. Mô phỏng Can thiệp Xuất viện What-if (What-if Simulation Module)
Xây dựng module giả lập lâm sàng nhằm phục vụ trực tiếp cho tính năng tương tác trên Dashboard của bác sĩ:
- Xây dựng hàm ánh xạ chính xác từ bộ `fitted_encoder` để đổi chuỗi ký tự sang mã số: `Về Nhà (HOME)` ánh xạ thành `5`, `Home Health Care` thành `6`, và `Viện Điều Dưỡng (SNF)` thành `12`.
- Chọn một bệnh nhân mẫu từ tập Test, giữ nguyên 100% hồ sơ bệnh lý nền, lần lượt vặn nút giả lập mã số của 3 kịch bản xuất viện trên để mô hình XGBSE dự đoán ra 3 hàm sinh tồn S(t) rẽ nhánh.
- **Tính toán chỉ số phái sinh:**
  1. **Risk Score 12 tháng:** Kết quả mô phỏng cho thấy rủi ro tử vong khi về nhà (`HOME`) là `9.1%`, có hỗ trợ điều dưỡng (`Home Health Care`) là `9.6%`, và chuyển vào viện dưỡng lão (`SNF`) là `12.8%`.
  2. **RMST (Restricted Mean Survival Time):** Thông qua tích phân xấp xỉ diện tích `np.trapz`, số ngày sống khỏe tích lũy đạt `369.9 ngày` (HOME), `368.8 ngày` (Home Health Care), và `361.2 ngày` (SNF).
- Cuối cùng, vẽ đồ thị 3 đường cong trực quan và hiển thị bảng đối chiếu kết quả mô phỏng.

### 9. Đóng gói và Lưu trữ Bộ Mô hình (Model Exporting Pipeline)
- Khởi tạo thư mục `mortality_models` trên ổ đĩa và dùng thư viện `joblib` đóng gói hoàn tất các cấu phần cốt lõi bao gồm: Bộ điền khuyết (`fitted_imputer.joblib`), Mô hình cơ sở (`baseline_cox_model.joblib`), và Mô hình đầu não chính (`final_xgbse_model.joblib`).
- Kích hoạt lệnh nén hệ thống để xuất file `survival_models_export.zip`. File này chứa trọn vẹn "tri thức" của mô hình, sẵn sàng được tải về từ nền tảng Kaggle để đội ngũ Backend triển khai trực tiếp lên môi trường Production của ứng dụng CDSS.