import os
import pandas as pd
import numpy as np
import xgboost as xgb
import shap
import joblib
import json
from sklearn.metrics import roc_auc_score

def train_xai_mortality(base_data_path, model_export_dir):
    """
    Huấn luyện mô hình phụ (Auxiliary Model) cho XAI: 12-Month Mortality
    """
    print("=== BẮT ĐẦU HUẤN LUYỆN XAI MODEL: 12-MONTH MORTALITY ===")
    
    # 1. Load Data
    print(f"1. Loading data từ: {base_data_path}")
    df_train = pd.read_parquet(os.path.join(base_data_path, "split=train"))
    df_test = pd.read_parquet(os.path.join(base_data_path, "split=test"))
    
    label_col = "mortality_event_12m"
    exclude_cols = [
        'subject_id', 'hadm_id', 'admittime', 'dischtime', 'index_time',
        'split', 'admityear', 'source_dataset',
        'readmission_time_days', 'readmission_event_30d',
        'mortality_time_days', 'mortality_time_months', 'mortality_event_12m',
        'next_admittime', 'days_to_next_admission',
        'dod', 'days_to_death_after_discharge',
        'event_flag_mortality', 'event_flag_readmission',
        'discharge_location_enc'
    ]
    
    feature_cols = [c for c in df_train.columns if c not in exclude_cols]
    
    X_train_raw = df_train[feature_cols].copy()
    y_train = df_train[label_col]
    
    X_test_raw = df_test[feature_cols].copy()
    y_test = df_test[label_col]
    
    # Encode categorical columns if needed (giả sử đã được encode giống model chính)
    cat_cols = X_train_raw.select_dtypes(include=['object', 'string', 'category']).columns.tolist()
    if len(cat_cols) > 0:
        print(f"Lưu ý: Dữ liệu cần được Ordinal Encode trước cho các cột {cat_cols}")
        # Trong thực tế, bạn sẽ load fitted_encoder.joblib để transform ở đây
    
    # 2. Impute missing values (sử dụng imputer của model chính)
    imputer_path = os.path.join(model_export_dir, "fitted_imputer.joblib")
    if os.path.exists(imputer_path):
        print(f"2. Đang sử dụng fitted_imputer từ {imputer_path}")
        imputer = joblib.load(imputer_path)
        # Transform data
        X_train = pd.DataFrame(imputer.transform(X_train_raw), columns=feature_cols)
        X_test = pd.DataFrame(imputer.transform(X_test_raw), columns=feature_cols)
    else:
        print("Không tìm thấy imputer của model chính! Đang tự tạo SimpleImputer mới...")
        from sklearn.impute import SimpleImputer
        imputer = SimpleImputer(strategy="median")
        X_train = pd.DataFrame(imputer.fit_transform(X_train_raw), columns=feature_cols)
        X_test = pd.DataFrame(imputer.transform(X_test_raw), columns=feature_cols)
        joblib.dump(imputer, os.path.join(model_export_dir, "xai_mortality_imputer.joblib"))

    # 3. Train Auxiliary Model
    print("3. Đang huấn luyện XGBoost Classifier cho xAI...")
    xai_model = xgb.XGBClassifier(
        n_estimators=150,
        max_depth=5,
        learning_rate=0.05,
        objective="binary:logistic",
        eval_metric="auc",
        random_state=42,
        tree_method="hist"
    )
    
    xai_model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
    
    pred_test = xai_model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, pred_test)
    print(f"-> AUC trên tập test của mô hình phụ xAI: {auc:.4f}")
    
    # 4. Create SHAP Explainer
    print("4. Đang tạo SHAP TreeExplainer...")
    # Dùng một tập background nhỏ để tính toán nhanh
    background = X_train.sample(n=min(500, len(X_train)), random_state=42)
    explainer = shap.TreeExplainer(xai_model, background)
    
    # 5. Export XAI Artifacts
    print(f"5. Xuất các file artifacts vào {model_export_dir}")
    joblib.dump(xai_model, os.path.join(model_export_dir, "xai_mortality_model.joblib"))
    joblib.dump(explainer, os.path.join(model_export_dir, "xai_mortality_explainer.joblib"))
    
    with open(os.path.join(model_export_dir, "xai_feature_cols.json"), "w") as f:
        json.dump(feature_cols, f)
        
    print("=== HOÀN THÀNH HUẤN LUYỆN XAI MODEL ===")

if __name__ == "__main__":
    # Thay đổi đường dẫn này trỏ tới thư mục chứa dữ liệu Gold Dataset (Parquet files)
    # Ví dụ: base_data_path = "/kaggle/input/datasets/anhkhang/bich-data/analytical_dataset_with_notes_2"
    base_data_path = "path/to/your/gold_dataset" 
    
    # Thư mục chứa model hiện tại
    model_export_dir = "../../model/mortality_model/mortality_models"
    
    if os.path.exists(base_data_path):
        train_xai_mortality(base_data_path, model_export_dir)
    else:
        print(f"Vui lòng cập nhật biến 'base_data_path' trỏ tới Gold Dataset. Đường dẫn hiện tại '{base_data_path}' không tồn tại.")
