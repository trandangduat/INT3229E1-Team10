#!/usr/bin/env bash
# setup_env.sh — Create conda environment for the Ray MIMIC-IV pipeline
#
# Usage: bash src/Ray/setup_env.sh

set -e

CONDA_ENV="ray-mimic"
PYTHON_VERSION="3.10"

echo "=========================================="
echo " Setting up conda env: ${CONDA_ENV}"
echo " Python: ${PYTHON_VERSION}"
echo "=========================================="

# Source conda
source "$(conda info --base)/etc/profile.d/conda.sh"

# Remove existing env if present
if conda env list | grep -q "^${CONDA_ENV}"; then
    echo "[INFO] Removing existing env '${CONDA_ENV}' ..."
    conda env remove -n "${CONDA_ENV}" -y
fi

# Create fresh env
echo "[INFO] Creating conda env '${CONDA_ENV}' ..."
conda create -n "${CONDA_ENV}" python="${PYTHON_VERSION}" -y

# Activate
conda activate "${CONDA_ENV}"

# Upgrade pip
pip install --upgrade pip

# Install packages
echo "[INFO] Installing packages ..."
pip install \
    "ray[data,train,tune,serve]==2.10.0" \
    pandas \
    pyarrow \
    scikit-learn \
    xgboost \
    psutil \
    lifelines \
    matplotlib \
    numpy

echo ""
echo "=========================================="
echo " Verifying installs ..."
echo "=========================================="
python -c "import ray; print('Ray:', ray.__version__)"
python -c "import pandas as pd; print('Pandas:', pd.__version__)"
python -c "import pyarrow as pa; print('PyArrow:', pa.__version__)"
python -c "import xgboost as xgb; print('XGBoost:', xgb.__version__)"
python -c "import sklearn; print('Sklearn:', sklearn.__version__)"

echo ""
echo "[DONE] Conda env '${CONDA_ENV}' is ready."
echo "       Activate with: conda activate ${CONDA_ENV}"
