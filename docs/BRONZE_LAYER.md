# BÁO CÁO BRONZE LAYER
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

### 2.5. Hoàn thiện Script Ingestion Local cho MIMIC-IV và eICU
*   **Script đã tạo:**
    *   `src/ingestion/ingest_mimic.py`
    *   `src/ingestion/ingest_eicu.py`
    *   `src/ingestion/ingest_notes.py`
*   **Quyết định kiến trúc:** Bronze layer giữ dữ liệu hoàn toàn raw. Không rename cột, không cast kiểu dữ liệu, không filter, không drop null. Việc chuẩn hóa schema giữa MIMIC-IV và eICU sẽ chuyển sang Silver layer.
*   **Cách đọc CSV:** Dùng `header=True` và `inferSchema=False`. PySpark sẽ lấy tên cột từ header của file CSV và đọc toàn bộ giá trị dưới dạng string, tránh việc Spark scan file lớn để đoán schema.
*   **Cách ghi output:** Luôn ghi `Parquet` với nén `Snappy` bằng `df.write.parquet(path, mode="overwrite", compression="snappy")`.
*   **Hỗ trợ môi trường:** Các script nhận tham số dòng lệnh `local` hoặc `hdfs`:
    *   `local`: chạy trong Docker với dữ liệu sample tại `/home/jovyan/data/raw`.
    *   `hdfs`: chạy production trên cluster với dữ liệu tại `hdfs://master10:9000/user/dis/data/raw_data/...`.

### 2.6. Local Validation bằng Docker Spark
*   **Môi trường chạy:** Docker container `predictcare-spark-dev`, image `jupyter/pyspark-notebook:spark-3.4.1`.
*   **Lệnh chạy đúng:** Phải dùng `spark-submit`, không dùng `python` trực tiếp vì `python` trong container báo lỗi `ModuleNotFoundError: No module named 'pyspark'`.
*   **Lệnh đã dùng để chạy MIMIC-IV:**
    ```bash
    sudo docker exec predictcare-spark-dev spark-submit /home/jovyan/src/ingestion/ingest_mimic.py local
    ```
*   **Kết quả MIMIC-IV:** Chạy thành công với dữ liệu sample, output đã được ghi vào `data/bronze/mimic_iv/`.
*   **Lệnh đã dùng để chạy eICU:**
    ```bash
    sudo docker exec predictcare-spark-dev spark-submit /home/jovyan/src/ingestion/ingest_eicu.py local
    ```
*   **Kết quả eICU:** Chạy thành công với dữ liệu sample, output đã được ghi vào:
    *   `data/bronze/eicu/patient/`
    *   `data/bronze/eicu/vitalPeriodic/`
    *   `data/bronze/eicu/diagnosis/`
    *   `data/bronze/eicu/medication/`
*   **Lưu ý permission:** Nếu output được tạo bởi user `root` khi dùng `docker exec`, có thể cần chạy `sudo chmod -R 777 data/` trước khi ghi đè lại dữ liệu.

### 2.7. Production Ingestion và Validation trên VM/HDFS
*   **Môi trường chạy:** VM instance `bigdata2` kết nối HDFS `hdfs://master10:9000/user/dis/data`.
*   **Script validation đã tạo:** `src/ingestion/validate_bronze.py`.
*   **Mục tiêu validation:** So sánh raw CSV với Bronze Parquet để đảm bảo ingestion không mất dữ liệu.
*   **Validation checks:**
    *   Raw CSV path tồn tại.
    *   Bronze Parquet path tồn tại.
    *   Output có `_SUCCESS`.
    *   `raw_rows == bronze_rows`.
    *   Header columns raw CSV khớp với columns trong Bronze Parquet.
    *   Bronze columns đều là `string`, đúng nguyên tắc raw Bronze.
*   **Kết quả tổng thể:** Đã chạy đầy đủ Bronze ingestion trên VM instance/HDFS cho 3 dataset: MIMIC-IV, eICU và MIMIC-IV-Note.
*   **Bronze validation:** `validate_bronze.py` đã validate thành công 11 bảng Bronze trên HDFS.
*   **Kết quả MIMIC-IV:** Đã chạy production ingestion và validate thành công trên HDFS cho các bảng MIMIC-IV đã xử lý, bao gồm cả bảng lớn `chartevents`.
*   **Kết quả eICU:** Đã chạy production ingestion và validate thành công trên HDFS cho các bảng `patient`, `vitalPeriodic`, `diagnosis`, `medication`.
*   **Kết quả MIMIC-IV-Note:** Đã chạy production ingestion và validate thành công trên HDFS cho dữ liệu note, bao gồm `discharge.csv`.
*   **Base path chuẩn:** Tất cả production data dùng base path `hdfs://master10:9000/user/dis/data`.

---

## 3. TRẠNG THÁI HOÀN THÀNH VÀ BƯỚC TIẾP THEO

Dưới đây là trạng thái các tác vụ Bronze Layer sau khi chạy production trên VM/HDFS:

### TÁC VỤ 1: Hoàn thiện Script Ingestion cho toàn bộ bảng MIMIC-IV
*   **Trạng thái:** Đã hoàn thành production ingestion và validation trên VM/HDFS.
*   **Kết quả:** Đã tạo `src/ingestion/ingest_mimic.py`, chạy thành công bằng Docker Spark với sample data, sau đó chạy production trên VM và validate thành công output Parquet+Snappy tại `hdfs://master10:9000/user/dis/data/bronze/mimic_iv/`.
*   **Ghi chú:** Script không dùng `StructType` ép kiểu dữ liệu tại Bronze. Dữ liệu được giữ raw bằng cách đọc toàn bộ cột dưới dạng string với `inferSchema=False`.

### TÁC VỤ 2: Xây dựng Script Ingestion cho eICU
*   **Trạng thái:** Đã hoàn thành production ingestion và validation trên VM/HDFS.
*   **Kết quả:** Đã tạo `src/ingestion/ingest_eicu.py`, chạy thành công bằng Docker Spark với sample data, sau đó chạy production trên VM và validate thành công output Parquet+Snappy tại `hdfs://master10:9000/user/dis/data/bronze/eicu/`.
*   **Quyết định:** Không ánh xạ schema, không rename, không cast tại Bronze. Các thao tác như `systemicSystolic` -> `sbp` sẽ thực hiện ở Silver layer.

### TÁC VỤ 3: Xử lý file Text đa dòng của MIMIC-IV-Note
*   **Trạng thái:** Đã hoàn thành production ingestion và validation trên VM/HDFS.
*   **Kết quả:** Đã tạo `src/ingestion/ingest_notes.py` với cấu hình PySpark CSV Reader `multiLine=True` và `escape='"'` để tránh vỡ dòng khi đọc ghi chú lâm sàng nhiều dòng; output Parquet+Snappy đã được ghi và validate thành công tại `hdfs://master10:9000/user/dis/data/bronze/mimic_iv_note/discharge/`.

### TÁC VỤ 4: Scale Up lên GCP Production (Máy ảo `bigdata2`)
*   **Trạng thái:** Đã hoàn thành cho MIMIC-IV, eICU và MIMIC-IV-Note.
*   **Kết quả:** Production Bronze output đã được ghi và validate tại `hdfs://master10:9000/user/dis/data/bronze/`.
*   **Tổng kết:** Bronze Layer đã ingest đầy đủ 3 dataset và validate thành công 11 bảng trên HDFS. Có thể chuyển sang triển khai Silver Layer.

---

## 4. QUY TẮC CỐT LÕI KHI GENERATE CODE (CHO AI AGENT)
1. **Tuyệt đối không dùng `inferSchema=True`** cho các bảng lớn (đặc biệt là chartevents và vitalPeriodic).
2. **Không filter dữ liệu:** Mọi hàm `filter()`, `dropna()`, `where()` đều bị cấm ở thư mục `src/ingestion/`.
3. **Định dạng:** Luôn sử dụng `df.write.parquet(path, mode="overwrite", compression="snappy")`.
