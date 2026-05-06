# BÁO CÁO TIẾN ĐỘ & KẾ HOẠCH LÀM VIỆC
**Vai trò:** Data Ingestion Engineer
**Dự án:** PREDICTCARE AI - CDSS Dashboard (Team 10)
**Mục tiêu Tầng:** Bronze Layer (Raw Data to Parquet+Snappy)

---

## 1. BỐI CẢNH DỰ ÁN & VAI TRÒ
*   **Kiến trúc:** Medallion Architecture (Bronze -> Silver -> Gold).
*   **Nhiệm vụ Data Ingestion:** Đọc dữ liệu thô (CSV) từ MIMIC-IV (~50GB), eICU (~7GB), và MIMIC-IV-Note (~4GB). Chuyển đổi định dạng sang `Parquet` + nén `Snappy` và lưu vào HDFS tại `Bronze layer`.
*   **Nguyên tắc cốt lõi:** Lớp Bronze là "Immutable" (bất biến). Tuyệt đối KHÔNG filter, drop null hay làm sạch dữ liệu tại bước này.

---

## 2. NHỮNG CÔNG VIỆC ĐÃ HOÀN THÀNH 

### 2.1. Xác thực Kiến trúc & Cấu trúc Dữ liệu
*   Đã đối chiếu Design Specification với thực tế (Ground Truth) của MIMIC-IV v3.1, eICU v2.0 và MIMIC-Note v2.2. Các bảng, schema và logic kết nối (hadm_id, patientunitstayid, d_items) đều chính xác.

### 2.2. Trích xuất Dữ liệu Mẫu (Data Sampling)
*   **Vấn đề:** Tránh lỗi OOM khi test trên máy local với file `chartevents.csv` (30GB+).
*   **Giải quyết:** Đã thực thi lệnh `hdfs dfs -cat ... | head -n 10001` trên GCP VM (`master10`/`bigdata2`) để cắt 10.000 dòng đầu tiên.
*   **Kết quả:** Đã tải thành công file `chartevents_sample.csv` (khoảng 1MB) về máy local bằng `gcloud compute scp`.

### 2.3. Thiết lập Môi trường Phát triển (Local Dev Environment)
*   **Cấu trúc thư mục:** Chuẩn hóa theo Yêu cầu Phi chức năng (NFR-M03).
    *   Code lưu tại: `src/ingestion/`
    *   Dữ liệu raw lưu tại: `data/raw/`
    *   Dữ liệu output lưu tại: `data/bronze/`
*   **Docker:** Khởi tạo thành công Docker container sử dụng image `jupyter/pyspark-notebook:spark-3.4.1`.
*   **Đồng bộ version:** Phiên bản Spark (3.4.1) tại local giống hệt 100% với phiên bản cài đặt trên cụm Hadoop/YARN thực tế (GCP VM).
*   **Bug fix:** Đã xử lý triệt để lỗi phân quyền Linux (File Permission Denied) bằng lệnh `sudo chmod -R 777 data/` để Docker có thể ghi đè file.

### 2.4. Xây dựng Script Ingestion Đầu tiên
*   Đã viết và chạy thành công kịch bản PySpark đọc file mẫu `chartevents` trên Jupyter Notebook.
*   **Tối ưu:** Đã áp dụng `StructType` cứng (explicit schema) thay vì `inferSchema=True` để ngăn chặn OOM khi chạy file lớn.
*   **Output:** Đã xuất thành công file định dạng `.snappy.parquet` vào thư mục `data/bronze/mimic_iv/chartevents/`.

---

## 3. KẾ HOẠCH LÀM VIỆC TIẾP THEO (NEXT STEPS CHO AI AGENT)

Dưới đây là các tác vụ mà AI Agent và tôi cần tiếp tục triển khai:

### TÁC VỤ 1: Hoàn thiện Script Ingestion cho toàn bộ bảng MIMIC-IV
*   **Mục tiêu:** Chuyển đổi notebook hiện tại thành file Python chuẩn (`src/ingestion/ingest_mimic.py`).
*   **Nhiệm vụ:**
    *   Định nghĩa `StructType` cứng cho các bảng còn lại: `admissions`, `patients`, `diagnoses_icd`, `labevents`, `d_items`.
    *   Viết logic vòng lặp hoặc cấu trúc hàm để đọc từng file CSV tương ứng trong `data/raw/` và ghi ra `data/bronze/mimic_iv/<table_name>/`.

### TÁC VỤ 2: Xây dựng Script Ingestion cho eICU (Có ánh xạ Schema)
*   **Mục tiêu:** Tạo script `src/ingestion/ingest_eicu.py`.
*   **Nhiệm vụ:**
    *   Đọc các bảng eICU (`patient`, `vitalPeriodic`, `diagnosis`, `medication`).
    *   **Lưu ý:** Design Spec yêu cầu đổi tên cột (Rename) và ép kiểu để tương thích với MIMIC-IV ngay từ khâu lưu vào Bronze (ví dụ: `systemicSystolic` -> `sbp`). Tuy nhiên, cần cân nhắc kỹ: Nguyên tắc Medallion thường đẩy việc rename này sang Silver Layer. *Cần review lại logic này.*

### TÁC VỤ 3: Xử lý file Text đa dòng của MIMIC-IV-Note
*   **Mục tiêu:** Tạo script `src/ingestion/ingest_notes.py` để xử lý file `discharge.csv`.
*   **Nhiệm vụ cốt lõi:** Cấu hình PySpark CSV Reader với tùy chọn `multiLine=True` và `escape="\""` để không bị vỡ dòng do các ký tự `\n` nằm lẫn bên trong ghi chú lâm sàng của bác sĩ.

### TÁC VỤ 4: Scale Up lên GCP Production (Máy ảo `bigdata2`)
*   **Mục tiêu:** Triển khai code đã test lên cluster thật.
*   **Nhiệm vụ:**
    1. Đổi toàn bộ đường dẫn I/O trong các file `.py` từ Local File System (`/home/jovyan/data/...`) sang HDFS URI (`hdfs://master10:9000/data/raw_data/...`).
    2. Gỡ bỏ dòng `.master("local[*]")` trong phần cấu hình SparkSession.
    3. Đưa file `.py` lên máy ảo và chạy bằng lệnh `spark-submit`.

---

## 4. QUY TẮC CỐT LÕI KHI GENERATE CODE (CHO AI AGENT)
1. **Tuyệt đối không dùng `inferSchema=True`** cho các bảng lớn (đặc biệt là chartevents và vitalPeriodic).
2. **Không filter dữ liệu:** Mọi hàm `filter()`, `dropna()`, `where()` đều bị cấm ở thư mục `src/ingestion/`.
3. **Định dạng:** Luôn sử dụng `df.write.parquet(path, mode="overwrite", compression="snappy")`.
