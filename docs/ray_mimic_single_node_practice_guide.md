# Hướng dẫn thực hành Ray cho pipeline MIMIC-IV trên 1 máy PC

## 0. Mục tiêu bài thực hành

Bài thực hành này hướng dẫn triển khai một pipeline Ray chạy trên **một máy PC Ubuntu** để xử lý dữ liệu MIMIC-IV và huấn luyện mô hình XGBoost Survival.

Pipeline tổng quát:

```text
Local Data trên PC
    ↓
Ray Single-node Runtime
    ↓
Ray Data
    - đọc patients
    - đọc admissions
    - đọc chartevents
    - join / filter / aggregation
    - tạo Gold Data
    ↓
Ray Train
    - huấn luyện XGBoost Survival
    - dùng GPU nếu có
    ↓
Model artifact + metrics + log thực nghiệm
```

Trong báo cáo, nên mô tả setup này là:

```text
single-node Ray deployment
```

hoặc:

```text
local Ray runtime with CPU/GPU resource scheduling
```

Không nên viết là đã chạy multi-node cluster nếu thực tế chỉ chạy trên một máy.

---

## 1. Giả định ban đầu

Máy PC đang dùng:

- Ubuntu Linux.
- Có dữ liệu MIMIC-IV lưu local trên PC.
- Có Python 3.
- Có GPU NVIDIA nếu muốn chạy training trên GPU.
- Không cần HDFS nữa trong bản thực hành này.
- Không cần Tailscale nữa nếu chỉ chạy một máy.

Kiến trúc thực nghiệm cuối:

```text
PC Ubuntu
├── Local disk
│   └── MIMIC-IV raw data
├── Ray Runtime
│   ├── Ray Data workers dùng CPU cores
│   └── Ray Train worker dùng GPU
└── Output
    ├── label table
    ├── feature table
    ├── Gold Data
    ├── trained model
    └── metrics/logs
```

---

## 2. Chuẩn bị thư mục project

Mở terminal trên PC:

```bash
mkdir -p ~/ray-mimic
cd ~/ray-mimic
```

Tạo cấu trúc thư mục:

```bash
mkdir -p data outputs scripts logs ray_results
```

Sau bước này, cấu trúc thư mục nên là:

```text
~/ray-mimic/
├── data/
├── outputs/
├── scripts/
├── logs/
└── ray_results/
```

---

## 3. Tạo Python virtual environment

```bash
cd ~/ray-mimic
python3 -m venv ~/ray-env
source ~/ray-env/bin/activate
```

Nâng cấp `pip`:

```bash
pip install -U pip
```

Cài các package cần thiết:

```bash
pip install -U "ray[data,train,tune,serve]" pandas pyarrow scikit-learn xgboost psutil
```

Kiểm tra cài đặt:

```bash
python -c "import ray; print('Ray:', ray.__version__)"
python -c "import pandas as pd; print('Pandas:', pd.__version__)"
python -c "import pyarrow as pa; print('PyArrow:', pa.__version__)"
python -c "import xgboost as xgb; print('XGBoost:', xgb.__version__)"
```

Nếu có GPU NVIDIA, kiểm tra thêm:

```bash
nvidia-smi
```

Nếu `nvidia-smi` chạy được thì driver GPU đã sẵn sàng.

---

## 4. Chuẩn bị dữ liệu local

Nên đặt dữ liệu trong thư mục:

```text
~/ray-mimic/data/
```

### 4.1. Trường hợp dữ liệu là Parquet

Cấu trúc khuyến nghị:

```text
~/ray-mimic/data/
├── patients/
│   └── *.parquet
├── admissions/
│   └── *.parquet
└── chartevents/
    └── *.parquet
```

### 4.2. Trường hợp dữ liệu là CSV

Có thể dùng cấu trúc:

```text
~/ray-mimic/data/
├── patients.csv
├── admissions.csv
└── chartevents.csv
```

Nếu `chartevents` lớn, nên chuyển sang Parquet để đọc nhanh hơn và chọn cột tốt hơn.

---

## 5. Khởi động Ray local runtime

Có hai cách chạy Ray trên một máy.

### Cách 1: Đơn giản nhất

Không cần chạy lệnh `ray start`. Trong code chỉ cần:

```python
ray.init()
```

Ray sẽ tự tạo local runtime.

### Cách 2: Khuyến nghị vì có Ray Dashboard

Chạy:

```bash
source ~/ray-env/bin/activate
ray stop
ray start --head \
  --port=6379 \
  --dashboard-host=127.0.0.1 \
  --dashboard-port=8265
```

Mở trình duyệt:

```text
http://127.0.0.1:8265
```

Trong các script Python, dùng:

```python
ray.init(address="auto")
```

Nếu Ray không nhận GPU, start lại rõ số GPU:

```bash
ray stop
ray start --head \
  --port=6379 \
  --dashboard-host=127.0.0.1 \
  --dashboard-port=8265 \
  --num-gpus=1
```

---

## 6. Test Ray nhận CPU/GPU

Tạo file:

```bash
nano ~/ray-mimic/scripts/test_ray.py
```

Nội dung:

```python
import ray
import socket
import os

ray.init(address="auto")

@ray.remote
def cpu_task():
    return {
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "resources": ray.get_runtime_context().get_assigned_resources(),
    }

@ray.remote(num_gpus=1)
def gpu_task():
    return {
        "host": socket.gethostname(),
        "cuda_visible_devices": os.environ.get("CUDA_VISIBLE_DEVICES"),
        "resources": ray.get_runtime_context().get_assigned_resources(),
    }

print("Cluster resources:")
print(ray.cluster_resources())

print("\nCPU task:")
print(ray.get(cpu_task.remote()))

print("\nGPU task:")
print(ray.get(gpu_task.remote()))
```

Chạy:

```bash
cd ~/ray-mimic
source ~/ray-env/bin/activate
python scripts/test_ray.py
```

Kết quả mong muốn có dòng kiểu:

```text
'GPU': 1.0
```

Nếu máy không có GPU, có thể bỏ phần `gpu_task` và chạy CPU-only.

---

## 7. Test đọc dữ liệu bằng Ray Data

### 7.1. Nếu dữ liệu là Parquet

Tạo file:

```bash
nano ~/ray-mimic/scripts/test_read_data.py
```

Nội dung:

```python
import ray
import ray.data as rd
from pathlib import Path

ray.init(address="auto")

BASE = Path.home() / "ray-mimic" / "data"

patients_path = f"local://{BASE}/patients"
admissions_path = f"local://{BASE}/admissions"

patients = rd.read_parquet(patients_path)
admissions = rd.read_parquet(admissions_path)

print("Patients schema:")
print(patients.schema())
print(patients.take(3))

print("Admissions schema:")
print(admissions.schema())
print(admissions.take(3))
```

Chạy:

```bash
python scripts/test_read_data.py
```

### 7.2. Nếu dữ liệu là CSV

Sửa nội dung thành:

```python
import ray
import ray.data as rd
from pathlib import Path

ray.init(address="auto")

BASE = Path.home() / "ray-mimic" / "data"

patients = rd.read_csv(f"local://{BASE}/patients.csv")
admissions = rd.read_csv(f"local://{BASE}/admissions.csv")

print("Patients schema:")
print(patients.schema())
print(patients.take(3))

print("Admissions schema:")
print(admissions.schema())
print(admissions.take(3))
```

---

## 8. Bước 1 của pipeline: tạo label table từ patients + admissions

Mục tiêu:

```text
patients + admissions → label table
```

Label table gồm các thông tin:

- `subject_id`
- `hadm_id`
- `gender`
- `anchor_age`
- `admission_type`
- `event`
- `time_to_event_hours`

Tạo file:

```bash
nano ~/ray-mimic/scripts/01_make_label.py
```

Nội dung:

```python
import ray
import ray.data as rd
from pathlib import Path
import pandas as pd

ray.init(address="auto")

BASE = Path.home() / "ray-mimic"
DATA = BASE / "data"
OUT = BASE / "outputs"
OUT.mkdir(exist_ok=True)

# Nếu dữ liệu là Parquet
patients = rd.read_parquet(f"local://{DATA}/patients")
admissions = rd.read_parquet(f"local://{DATA}/admissions")

# Nếu dữ liệu là CSV, dùng thay thế:
# patients = rd.read_csv(f"local://{DATA}/patients.csv")
# admissions = rd.read_csv(f"local://{DATA}/admissions.csv")

patients = patients.select_columns([
    "subject_id",
    "gender",
    "anchor_age",
    "dod",
])

admissions = admissions.select_columns([
    "subject_id",
    "hadm_id",
    "admittime",
    "dischtime",
    "deathtime",
    "admission_type",
])

label = admissions.join(
    patients,
    on="subject_id",
    how="left",
)

def make_survival_label(batch: pd.DataFrame) -> pd.DataFrame:
    batch["admittime"] = pd.to_datetime(batch["admittime"], errors="coerce")
    batch["dischtime"] = pd.to_datetime(batch["dischtime"], errors="coerce")
    batch["deathtime"] = pd.to_datetime(batch["deathtime"], errors="coerce")

    # event = 1 nếu bệnh nhân tử vong trong admission
    batch["event"] = batch["deathtime"].notna().astype(int)

    # Nếu có deathtime: time_to_event = deathtime - admittime
    # Nếu không có deathtime: censoring time = dischtime - admittime
    end_time = batch["deathtime"].fillna(batch["dischtime"])
    batch["time_to_event_hours"] = (
        end_time - batch["admittime"]
    ).dt.total_seconds() / 3600.0

    batch = batch[batch["time_to_event_hours"].notna()]
    batch = batch[batch["time_to_event_hours"] > 0]

    return batch[[
        "subject_id",
        "hadm_id",
        "gender",
        "anchor_age",
        "admission_type",
        "event",
        "time_to_event_hours",
    ]]

label = label.map_batches(
    make_survival_label,
    batch_format="pandas",
)

print(label.schema())
print(label.take(5))
print("Label rows:", label.count())

label.write_parquet(f"local://{OUT}/label_table")
```

Chạy:

```bash
cd ~/ray-mimic
source ~/ray-env/bin/activate
python scripts/01_make_label.py
```

Kiểm tra output:

```bash
ls -lh ~/ray-mimic/outputs/label_table
```

---

## 9. Bước 2 của pipeline: lọc chartevents sample

Mục tiêu:

```text
chartevents raw → vitals_sample
```

Ở bước đầu, chỉ nên chạy sample nhỏ để kiểm tra logic.

Tạo file:

```bash
nano ~/ray-mimic/scripts/02_make_features_sample.py
```

Nội dung:

```python
import ray
import ray.data as rd
from pathlib import Path
import pandas as pd

ray.init(address="auto")

BASE = Path.home() / "ray-mimic"
DATA = BASE / "data"
OUT = BASE / "outputs"
OUT.mkdir(exist_ok=True)

CHARTEVENTS_PATH = f"local://{DATA}/chartevents"

# Ví dụ itemid trong MIMIC-IV.
# Cần kiểm tra lại itemid thực tế trong dữ liệu của nhóm.
VITAL_ITEMIDS = {
    220045: "heart_rate",
    220179: "sbp",
    220180: "dbp",
    220210: "resp_rate",
    220277: "spo2",
}

# Nếu dữ liệu là Parquet
chartevents = rd.read_parquet(
    CHARTEVENTS_PATH,
    columns=[
        "subject_id",
        "hadm_id",
        "charttime",
        "itemid",
        "valuenum",
    ],
)

# Nếu dữ liệu là CSV, dùng thay thế:
# chartevents = rd.read_csv(f"local://{DATA}/chartevents.csv")
# chartevents = chartevents.select_columns([
#     "subject_id", "hadm_id", "charttime", "itemid", "valuenum"
# ])

# Sample trước để test logic.
# Sau khi chạy ổn, có thể tăng lên 10_000_000, 50_000_000 hoặc bỏ limit.
chartevents = chartevents.limit(2_000_000)

def filter_and_prepare(batch: pd.DataFrame) -> pd.DataFrame:
    batch = batch[batch["itemid"].isin(VITAL_ITEMIDS.keys())]
    batch = batch.dropna(subset=["valuenum", "hadm_id"])
    batch["charttime"] = pd.to_datetime(batch["charttime"], errors="coerce")
    batch = batch.dropna(subset=["charttime"])
    batch["feature_name"] = batch["itemid"].map(VITAL_ITEMIDS)

    return batch[[
        "subject_id",
        "hadm_id",
        "charttime",
        "feature_name",
        "valuenum",
    ]]

vitals = chartevents.map_batches(
    filter_and_prepare,
    batch_format="pandas",
)

print(vitals.schema())
print(vitals.take(5))
print("Filtered rows:", vitals.count())

vitals.write_parquet(f"local://{OUT}/vitals_sample")
```

Chạy:

```bash
python scripts/02_make_features_sample.py
```

Kiểm tra output:

```bash
ls -lh ~/ray-mimic/outputs/vitals_sample
```

---

## 10. Bước 3 của pipeline: aggregate feature table sample

Mục tiêu:

```text
vitals_sample → feature_table_sample
```

Ở bản sample, có thể dùng pandas sau khi dữ liệu đã được lọc mạnh. Không dùng `to_pandas()` trên full `chartevents`.

Tạo file:

```bash
nano ~/ray-mimic/scripts/03_aggregate_features_sample.py
```

Nội dung:

```python
import ray
import ray.data as rd
from pathlib import Path
import pandas as pd

ray.init(address="auto")

BASE = Path.home() / "ray-mimic"
OUT = BASE / "outputs"

vitals = rd.read_parquet(f"local://{OUT}/vitals_sample")

# Chỉ dùng cách này cho sample hoặc dữ liệu đã lọc mạnh.
df = vitals.to_pandas()

agg = (
    df.groupby(["subject_id", "hadm_id", "feature_name"])["valuenum"]
      .agg(["mean", "min", "max"])
      .reset_index()
)

wide = agg.pivot_table(
    index=["subject_id", "hadm_id"],
    columns="feature_name",
    values=["mean", "min", "max"],
)

wide.columns = [f"{stat}_{feat}" for stat, feat in wide.columns]
wide = wide.reset_index()

print(wide.head())
print("Feature table shape:", wide.shape)

feature_ds = ray.data.from_pandas(wide)
feature_ds.write_parquet(f"local://{OUT}/feature_table_sample")
```

Chạy:

```bash
python scripts/03_aggregate_features_sample.py
```

Kiểm tra output:

```bash
ls -lh ~/ray-mimic/outputs/feature_table_sample
```

---

## 11. Bước 4 của pipeline: tạo Gold Data sample

Mục tiêu:

```text
label_table + feature_table_sample → gold_data_sample
```

Tạo file:

```bash
nano ~/ray-mimic/scripts/04_make_gold_sample.py
```

Nội dung:

```python
import ray
import ray.data as rd
from pathlib import Path

ray.init(address="auto")

BASE = Path.home() / "ray-mimic"
OUT = BASE / "outputs"

label = rd.read_parquet(f"local://{OUT}/label_table")
features = rd.read_parquet(f"local://{OUT}/feature_table_sample")

gold = label.join(
    features,
    on=["subject_id", "hadm_id"],
    how="inner",
)

print(gold.schema())
print(gold.take(5))
print("Gold rows:", gold.count())

gold.write_parquet(f"local://{OUT}/gold_data_sample")
```

Chạy:

```bash
python scripts/04_make_gold_sample.py
```

Kiểm tra output:

```bash
ls -lh ~/ray-mimic/outputs/gold_data_sample
```

---

## 12. Bước 5 của pipeline: train XGBoost Survival bằng Ray Train

Mục tiêu:

```text
gold_data_sample → XGBoost Survival model
```

Tạo file:

```bash
nano ~/ray-mimic/scripts/05_train_xgb_survival.py
```

Nội dung:

```python
import ray
import ray.data as rd
from pathlib import Path

from ray.train import ScalingConfig, RunConfig
from ray.train.xgboost import XGBoostTrainer

ray.init(address="auto")

BASE = Path.home() / "ray-mimic"
OUT = BASE / "outputs"
RESULTS = BASE / "ray_results"

gold = rd.read_parquet(f"local://{OUT}/gold_data_sample")

drop_cols = {
    "subject_id",
    "hadm_id",
    "event",
    "time_to_event_hours",
}

schema_names = gold.schema().names
feature_cols = [c for c in schema_names if c not in drop_cols]

# Bản demo dùng time_to_event_hours làm label cho survival:cox.
# Nếu cần xử lý censoring đầy đủ hơn, cần chuẩn hóa lại format label/event.
train_ds = gold.select_columns(feature_cols + ["time_to_event_hours"])

trainer = XGBoostTrainer(
    label_column="time_to_event_hours",
    params={
        "objective": "survival:cox",
        "eval_metric": "cox-nloglik",
        "tree_method": "hist",
        "device": "cuda",
        "max_depth": 4,
        "eta": 0.05,
    },
    datasets={
        "train": train_ds,
    },
    scaling_config=ScalingConfig(
        num_workers=1,
        use_gpu=True,
        resources_per_worker={
            "CPU": 4,
            "GPU": 1,
        },
    ),
    run_config=RunConfig(
        name="xgb_survival_single_node_ray",
        storage_path=str(RESULTS),
    ),
)

result = trainer.fit()

print("Training result:")
print(result)
print("Metrics:")
print(result.metrics)
print("Checkpoint:")
print(result.checkpoint)
```

Chạy:

```bash
python scripts/05_train_xgb_survival.py
```

Trong lúc chạy, mở Ray Dashboard:

```text
http://127.0.0.1:8265
```

Nên chụp màn hình dashboard để đưa vào báo cáo.

Nếu máy không có GPU hoặc XGBoost CUDA lỗi, đổi phần training thành CPU-only:

```python
params={
    "objective": "survival:cox",
    "eval_metric": "cox-nloglik",
    "tree_method": "hist",
    "max_depth": 4,
    "eta": 0.05,
},
scaling_config=ScalingConfig(
    num_workers=1,
    use_gpu=False,
    resources_per_worker={"CPU": 4},
),
```

---

## 13. Script chạy toàn bộ pipeline theo thứ tự

Tạo file:

```bash
nano ~/ray-mimic/run_all.sh
```

Nội dung:

```bash
#!/usr/bin/env bash
set -e

source ~/ray-env/bin/activate
cd ~/ray-mimic

mkdir -p logs

python scripts/01_make_label.py | tee logs/01_make_label.log
python scripts/02_make_features_sample.py | tee logs/02_make_features_sample.log
python scripts/03_aggregate_features_sample.py | tee logs/03_aggregate_features_sample.log
python scripts/04_make_gold_sample.py | tee logs/04_make_gold_sample.log
python scripts/05_train_xgb_survival.py | tee logs/05_train_xgb_survival.log
```

Cấp quyền chạy:

```bash
chmod +x ~/ray-mimic/run_all.sh
```

Chạy toàn bộ:

```bash
./run_all.sh
```

---

## 14. Scale dần lên dữ liệu lớn hơn

Không chạy full `chartevents` ngay từ đầu.

Trong file:

```text
scripts/02_make_features_sample.py
```

Ban đầu:

```python
chartevents = chartevents.limit(2_000_000)
```

Sau khi chạy ổn, tăng dần:

```python
chartevents = chartevents.limit(10_000_000)
```

rồi:

```python
chartevents = chartevents.limit(50_000_000)
```

Cuối cùng bỏ dòng `limit` để chạy full:

```python
# chartevents = chartevents.limit(50_000_000)
```

Lưu ý quan trọng:

```python
# Không làm thế này với dữ liệu lớn
chartevents.to_pandas()
```

`to_pandas()` chỉ nên dùng sau khi dữ liệu đã được lọc/nén đủ nhỏ.

---

## 15. Ghi log thời gian và RAM

Tạo file tiện ích:

```bash
nano ~/ray-mimic/scripts/utils.py
```

Nội dung:

```python
import time
import psutil


def start_timer():
    return time.time()


def log_stage(name: str, start_time: float):
    elapsed = time.time() - start_time
    mem = psutil.virtual_memory()
    print(
        f"[{name}] elapsed={elapsed:.2f}s, "
        f"RAM used={mem.used / 1e9:.2f}GB, "
        f"RAM percent={mem.percent:.1f}%"
    )
```

Ví dụ dùng trong script:

```python
from utils import start_timer, log_stage

t = start_timer()
# chạy một bước xử lý
log_stage("make_label", t)
```

Các số liệu nên ghi lại cho báo cáo:

```text
1. Thời gian đọc patients/admissions
2. Thời gian tạo label table
3. Thời gian lọc chartevents
4. Thời gian aggregate feature
5. Thời gian tạo Gold Data
6. Kích thước raw data
7. Kích thước Gold Data
8. Training time
9. Peak RAM
10. GPU utilization screenshot
```

Kiểm tra kích thước dữ liệu:

```bash
du -sh ~/ray-mimic/data/*
du -sh ~/ray-mimic/outputs/*
du -sh ~/ray-mimic/ray_results/*
```

---

## 16. Những lỗi thường gặp và cách xử lý

### 16.1. Ray không start được

Chạy:

```bash
ray stop
ray start --head --dashboard-host=127.0.0.1 --dashboard-port=8265
```

Nếu vẫn lỗi, kiểm tra process cũ:

```bash
ps aux | grep ray
```

### 16.2. Không mở được Ray Dashboard

Kiểm tra Ray đang nghe port 8265:

```bash
ss -lntp | grep 8265
```

Mở trình duyệt:

```text
http://127.0.0.1:8265
```

### 16.3. Ray không nhận GPU

Kiểm tra:

```bash
nvidia-smi
```

Start lại Ray:

```bash
ray stop
ray start --head \
  --dashboard-host=127.0.0.1 \
  --dashboard-port=8265 \
  --num-gpus=1
```

Chạy lại:

```bash
python scripts/test_ray.py
```

### 16.4. Lỗi thiếu cột trong dữ liệu

Ví dụ lỗi:

```text
Column 'anchor_age' does not exist
```

Cách xử lý:

1. In schema bằng `test_read_data.py`.
2. Kiểm tra tên cột thật.
3. Sửa danh sách `select_columns` trong script.

### 16.5. Lỗi OOM khi xử lý chartevents

Không dùng:

```python
df = chartevents.to_pandas()
```

Nên dùng:

```python
select_columns()
limit()
map_batches()
filter sớm theo itemid
write checkpoint ra Parquet
```

### 16.6. XGBoost GPU lỗi

Nếu lỗi liên quan CUDA, chạy CPU trước để chứng minh pipeline:

```python
"tree_method": "hist"
```

và bỏ:

```python
"device": "cuda"
```

đồng thời đổi:

```python
use_gpu=False
```

---

## 17. Nội dung nên chụp màn hình cho báo cáo

Nên chuẩn bị các hình sau:

1. Ray Dashboard lúc pipeline đang chạy.
2. CPU utilization khi Ray Data xử lý ETL.
3. GPU utilization khi Ray Train chạy XGBoost.
4. Terminal log tạo Gold Data.
5. Terminal log training result.
6. Kích thước thư mục raw data và Gold Data.

Lệnh lấy kích thước:

```bash
du -sh ~/ray-mimic/data/*
du -sh ~/ray-mimic/outputs/gold_data_sample
```

Lệnh kiểm tra GPU trong lúc train:

```bash
watch -n 1 nvidia-smi
```

---

## 18. Cách mô tả trung thực trong báo cáo

Đoạn tiếng Anh có thể dùng:

```text
Due to the expiration of the previously configured cloud infrastructure,
the final experiments were executed on a single-node Ray deployment.
This setup preserves the same Ray programming abstraction used in a
multi-node cluster, including the Ray runtime, object store, Ray Data,
Ray Train, and CPU/GPU resource scheduling. Therefore, the experiment
focuses on validating the end-to-end pipeline design and measuring
single-machine performance, while multi-node scaling is left as future work.
```

Đoạn tiếng Việt tương ứng:

```text
Do hạ tầng cloud đã gần hết hạn, thực nghiệm cuối cùng được triển khai trên
single-node Ray deployment. Mặc dù toàn bộ thành phần chạy trên một máy vật lý,
thiết lập này vẫn giữ nguyên các abstraction chính của Ray như runtime, object
store, Ray Data, Ray Train và cơ chế lập lịch tài nguyên CPU/GPU. Vì vậy, phần
thực nghiệm tập trung kiểm chứng pipeline end-to-end và đo hiệu năng trên một
máy, trong khi mở rộng multi-node được xem là hướng phát triển tiếp theo.
```

---

## 19. Kiến trúc nên vẽ trong báo cáo

Sơ đồ đề xuất:

```text
Local SSD / HDD
    ↓
Ray Single-node Runtime
    ├── Ray Data workers on CPU cores
    │       patients + admissions + chartevents
    │       join + filter + aggregation
    │       Gold Data
    │
    └── Ray Train worker on GPU
            XGBoost survival:cox
```

Caption tiếng Anh:

```text
Single-node Ray deployment used in the final experiment. Although all
components run on one physical machine, Ray still exposes the same runtime
abstractions for data processing, object storage, and GPU training as in a
multi-node deployment.
```

---

## 20. Checklist hoàn thành bài thực hành

- [ ] Tạo được `~/ray-mimic`.
- [ ] Cài được `~/ray-env`.
- [ ] Cài được Ray, pandas, pyarrow, xgboost.
- [ ] Start được Ray local runtime.
- [ ] Mở được Ray Dashboard.
- [ ] Chạy được `test_ray.py`.
- [ ] Đọc được patients/admissions bằng Ray Data.
- [ ] Tạo được `label_table`.
- [ ] Lọc được `vitals_sample` từ chartevents.
- [ ] Tạo được `feature_table_sample`.
- [ ] Join được thành `gold_data_sample`.
- [ ] Train được XGBoost Survival bằng Ray Train.
- [ ] Có log runtime.
- [ ] Có screenshot Ray Dashboard.
- [ ] Có số liệu raw data size và Gold Data size.
- [ ] Có đoạn mô tả trung thực về single-node Ray deployment trong báo cáo.

---

## 21. Thứ tự chạy nhanh

Mỗi lần mở terminal mới:

```bash
source ~/ray-env/bin/activate
cd ~/ray-mimic
```

Start Ray:

```bash
ray stop
ray start --head --dashboard-host=127.0.0.1 --dashboard-port=8265 --num-gpus=1
```

Chạy test:

```bash
python scripts/test_ray.py
python scripts/test_read_data.py
```

Chạy pipeline:

```bash
python scripts/01_make_label.py
python scripts/02_make_features_sample.py
python scripts/03_aggregate_features_sample.py
python scripts/04_make_gold_sample.py
python scripts/05_train_xgb_survival.py
```

Hoặc chạy toàn bộ:

```bash
./run_all.sh
```

Mở dashboard:

```text
http://127.0.0.1:8265
```

Theo dõi GPU:

```bash
watch -n 1 nvidia-smi
```
