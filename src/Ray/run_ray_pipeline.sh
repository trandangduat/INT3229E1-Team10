#!/usr/bin/env bash
# run_ray_pipeline.sh — Run the full Ray MIMIC-IV pipeline
# Usage: bash src/Ray/run_ray_pipeline.sh [--skip-env]
#
# Steps:
#   0. Create & activate conda env (ray-mimic)
#   1. 01_make_label.py         — patients + admissions → label_table
#   2. 02_make_vitals.py        — chartevents → vitals_agg
#   3. 03_make_labs.py          — labevents  → labs_agg
#   4. 04_make_diagnoses.py     — diagnoses  → diagnoses_onehot
#   5. 05_build_gold.py         — join all   → gold_dataset
#   6. 06_train_readmission.py  — train XGBoost readmission model
#
# All output is saved under <project_root>/outputs/ and logs under <project_root>/logs/

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
LOGS_DIR="${PROJECT_DIR}/logs"
mkdir -p "${LOGS_DIR}"

CONDA_ENV="ray-mimic"

# ─── Helper ─────────────────────────────────────────────────────────────────
run_step() {
    local step_name="$1"
    local script="$2"
    local log_file="${LOGS_DIR}/${step_name}.log"
    echo ""
    echo "=========================================="
    echo " Running: ${step_name}"
    echo "=========================================="
    python "${SCRIPT_DIR}/${script}" 2>&1 | tee "${log_file}"
    echo "[DONE] ${step_name} — log saved to ${log_file}"
}

# ─── Activate conda env ──────────────────────────────────────────────────────
if [[ "$1" != "--skip-env" ]]; then
    source "$(conda info --base)/etc/profile.d/conda.sh"
    conda activate "${CONDA_ENV}" || {
        echo "[ERROR] Could not activate conda env '${CONDA_ENV}'."
        echo "        Run: bash src/Ray/setup_env.sh"
        exit 1
    }
fi

echo ""
echo "=========================================="
echo " Ray MIMIC-IV Pipeline"
echo " Project: ${PROJECT_DIR}"
echo " Conda env: ${CONDA_ENV}"
echo "=========================================="

# ─── Start Ray (single-node) ─────────────────────────────────────────────────
echo ""
echo "[INFO] Starting Ray (single-node) via Python ray.init() ..."

# Stop any stale Ray processes first (best-effort)
python -c "import ray; ray.shutdown()" 2>/dev/null || true

# Start Ray head node via Python — avoids the ray CLI deepcopy bug
python - <<'PYEOF'
import ray, socket, os, sys

# Detect GPU count
try:
    import subprocess
    gpu_count = int(subprocess.check_output(
        ["nvidia-smi", "-L"], stderr=subprocess.DEVNULL
    ).decode().strip().count("\n")) + 1
except Exception:
    gpu_count = 0

ray.init(
    ignore_reinit_error=True,
    num_gpus=gpu_count,
    dashboard_host="127.0.0.1",
    dashboard_port=8265,
    include_dashboard=True,
)

resources = ray.cluster_resources()
print(f"[INFO] Ray started on host: {socket.gethostname()}")
print(f"[INFO] Cluster resources: {resources}")
print(f"[INFO] Ray Dashboard: http://127.0.0.1:8265")
ray.shutdown()
PYEOF

echo ""

# ─── Run pipeline steps ─────────────────────────────────────────────────────
run_step "01_make_label"        "01_make_label.py"
run_step "02_make_vitals"       "02_make_vitals.py"
run_step "03_make_labs"         "03_make_labs.py"
run_step "04_make_diagnoses"    "04_make_diagnoses.py"
run_step "05_build_gold"        "05_build_gold.py"
run_step "06_train_readmission" "06_train_readmission.py"

# ─── Data sizes ─────────────────────────────────────────────────────────────
echo ""
echo "=========================================="
echo " Output sizes"
echo "=========================================="
du -sh "${PROJECT_DIR}/outputs/"* 2>/dev/null || true

echo ""
echo "=========================================="
echo " PIPELINE COMPLETE"
echo "=========================================="
echo " Metrics → ${LOGS_DIR}/readmission_metrics.json"
echo " Model   → ${PROJECT_DIR}/outputs/xgb_readmission_model.json"
