#!/usr/bin/env bash
# bootstrap.sh — One-shot bootstrap: setup env + run pipeline
#
# Run from project root:
#   bash src/Ray/bootstrap.sh
#
# This script:
#   1. Creates the conda env "ray-mimic" (Python 3.10 + all deps)
#   2. Creates output directories
#   3. Runs the full Ray MIMIC-IV pipeline
#   4. Git commits results

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

echo "=========================================="
echo " Bootstrap: Ray MIMIC-IV Pipeline"
echo " Project: ${PROJECT_DIR}"
echo "=========================================="

# ─── Step 0: Create output dirs ──────────────────────────────────────────────
mkdir -p "${PROJECT_DIR}/outputs"
mkdir -p "${PROJECT_DIR}/logs"
chmod +x "${SCRIPT_DIR}/setup_env.sh"
chmod +x "${SCRIPT_DIR}/run_ray_pipeline.sh"
echo "[OK] Output directories ready."

# ─── Step 1: Setup conda env ────────────────────────────────────────────────
source "$(conda info --base)/etc/profile.d/conda.sh"

CONDA_ENV="ray-mimic"
if ! conda env list | grep -q "^${CONDA_ENV}"; then
    echo "[INFO] Creating conda env '${CONDA_ENV}' ..."
    conda create -n "${CONDA_ENV}" python=3.10 -y
fi

conda activate "${CONDA_ENV}"

# Install packages
echo "[INFO] Installing Python packages ..."
pip install -q --upgrade pip
pip install -q \
    "ray[data,train,tune,serve]==2.10.0" \
    pandas \
    pyarrow \
    scikit-learn \
    xgboost \
    psutil \
    lifelines \
    matplotlib \
    numpy

echo "[OK] Packages installed."

# ─── Step 2: Run full pipeline ───────────────────────────────────────────────
bash "${SCRIPT_DIR}/run_ray_pipeline.sh" --skip-env

# ─── Step 3: Git commit ──────────────────────────────────────────────────────
cd "${PROJECT_DIR}"
git add -A
git commit -m "feat: Ray pipeline results - single-node ETL + XGBoost readmission" || true
git push || echo "[WARN] git push failed (check remote). Commit is local."

echo ""
echo "=========================================="
echo " BOOTSTRAP COMPLETE"
echo "=========================================="
